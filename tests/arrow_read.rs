//! Live Arrow reads. Set TEST_DB_PASSWORD and optionally TEST_DB_HOST/PORT/USER/NAME.

#![cfg(feature = "arrow")]

use std::time::Duration;

use arrow_array::{
    Array, BooleanArray, Date32Array, Decimal128Array, FixedSizeListArray, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray,
    RecordBatch, StringArray, StructArray, Time64NanosecondArray, UInt32Array, UInt8Array,
};
use arrow_schema::DataType;
use futures_util::{StreamExt, TryStreamExt};
use mssql_tiberius_bridge::{
    ArrowBatch, ArrowOptions, AuthMethod, Client, Config, Error, TdsManager,
};

fn test_config() -> Option<Config> {
    let Ok(password) = std::env::var("TEST_DB_PASSWORD") else {
        eprintln!("skipping live Arrow read test: TEST_DB_PASSWORD is not set");
        return None;
    };
    let mut config = Config::new();
    config
        .host(std::env::var("TEST_DB_HOST").unwrap_or_else(|_| "localhost".into()))
        .port(
            std::env::var("TEST_DB_PORT")
                .map(|port| port.parse().expect("valid TEST_DB_PORT"))
                .unwrap_or(1433),
        )
        .database(std::env::var("TEST_DB_NAME").unwrap_or_else(|_| "master".into()))
        .authentication(AuthMethod::sql_server(
            std::env::var("TEST_DB_USER").unwrap_or_else(|_| "sa".into()),
            password,
        ))
        .trust_cert();
    Some(config)
}

fn array<'a, A: Array + 'static>(batch: &'a RecordBatch, name: &str) -> &'a A {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap()
}

fn child<'a, A: Array + 'static>(value: &'a StructArray, name: &str) -> &'a A {
    value
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap()
}

#[tokio::test]
async fn parameters_batches_and_retained_values() {
    let Some(config) = test_config() else { return };
    let mut client = Client::connect(&config).await.unwrap();
    let options = ArrowOptions {
        batch_size: 2,
        ..ArrowOptions::default()
    };
    let batches: Vec<ArrowBatch> = client
        .query_arrow_with_options(
            "SELECT n AS id, CASE WHEN n = 3 THEN NULL ELSE @P1 END AS text, \
         CASE WHEN n = 4 THEN NULL ELSE @P2 END AS bytes \
         FROM (VALUES (1), (2), (3), (4), (5)) v(n) ORDER BY n",
            &[&"caf\u{e9}_\u{4e2d}\u{1f600}", &vec![0u8, 255, 13]],
            options,
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        batches
            .iter()
            .map(|v| v.batch.num_rows())
            .collect::<Vec<_>>(),
        [2, 2, 1]
    );
    for result in &batches {
        assert_eq!(result.result_index, 0);
    }
    assert_eq!(array::<Int32Array>(&batches[2].batch, "id").value(0), 5);
    assert_eq!(
        array::<LargeStringArray>(&batches[0].batch, "text").value(0),
        "caf\u{e9}_\u{4e2d}\u{1f600}"
    );
    assert!(array::<LargeStringArray>(&batches[1].batch, "text").is_null(0));
    assert_eq!(
        array::<LargeBinaryArray>(&batches[0].batch, "bytes").value(1),
        [0, 255, 13]
    );
    assert!(array::<LargeBinaryArray>(&batches[1].batch, "bytes").is_null(1));
    client.ping().await.unwrap();
    assert!(!client.is_connection_dead());
    // Completed batches remain usable after subsequent network operations.
    assert_eq!(
        array::<Int32Array>(&batches[0].batch, "id").values(),
        &[1, 2]
    );
}

#[tokio::test]
async fn parameter_encoding_configuration_is_preserved() {
    let Some(mut config) = test_config() else {
        return;
    };
    config.send_string_parameters_as_unicode(false);
    let mut client = Client::connect(&config).await.unwrap();
    let results: Vec<ArrowBatch> = client.query_arrow(
        "SELECT CONVERT(varchar(30), SQL_VARIANT_PROPERTY(@P1,'BaseType')) AS base_type, @P1 AS text",
        &[&"hello"],
    ).try_collect().await.unwrap();
    assert_eq!(
        array::<LargeStringArray>(&results[0].batch, "base_type").value(0),
        "varchar"
    );
    assert_eq!(
        array::<LargeStringArray>(&results[0].batch, "text").value(0),
        "hello"
    );
}

#[tokio::test]
async fn empty_and_multiple_results_preserve_metadata_and_indexes() {
    let Some(config) = test_config() else { return };
    let mut client = Client::connect(&config).await.unwrap();
    let batches: Vec<ArrowBatch> = client
        .simple_query_arrow(
            "SET NOCOUNT ON; PRINT 'before'; \
         SELECT CAST(NULL AS decimal(12,4)) AS empty_amount WHERE 1=0; \
         SELECT 1 AS n; PRINT 'between'; \
         SELECT CAST(NULL AS nvarchar(20)) AS empty_text WHERE 1=0; \
         SELECT N'last' AS text; SELECT N'next' AS text",
        )
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        batches
            .iter()
            .map(|v| (v.result_index, v.batch.num_rows()))
            .collect::<Vec<_>>(),
        [(0, 0), (1, 1), (2, 0), (3, 1), (4, 1)]
    );
    assert_eq!(
        batches[0].batch.schema().field(0).data_type(),
        &DataType::Decimal128(12, 4)
    );
    assert_eq!(
        batches[2].batch.schema().field(0).data_type(),
        &DataType::LargeUtf8
    );
    assert_eq!(
        array::<LargeStringArray>(&batches[3].batch, "text").value(0),
        "last"
    );
    assert_eq!(batches[3].batch.schema(), batches[4].batch.schema());
    assert_eq!(
        array::<LargeStringArray>(&batches[4].batch, "text").value(0),
        "next"
    );
    let no_rows: Vec<ArrowBatch> = client
        .simple_query_arrow("SET NOCOUNT OFF; PRINT 'no rows'")
        .try_collect()
        .await
        .unwrap();
    assert!(no_rows.is_empty());
    client.ping().await.unwrap();
}

#[tokio::test]
async fn sql_types_are_lossless_and_nulls_are_preserved() {
    let Some(config) = test_config() else { return };
    let mut client = Client::connect(&config).await.unwrap();
    client
        .simple_query(
            "CREATE TABLE #arrow_types (
          seq int IDENTITY, b bit, tiny tinyint, small smallint, n int, big bigint,
          r real, f float, decval decimal(38,7), num numeric(9,2),
          m money, sm smallmoney, guid uniqueidentifier, d date, t time(7),
          dt datetime, sdt smalldatetime, dt2 datetime2(7), dto datetimeoffset(7),
          text nvarchar(max), narrow varchar(max), bytes varbinary(max), x xml);
         INSERT #arrow_types VALUES
          (1,255,-32768,-2147483648,-9223372036854775808,1.5,2.5,
           1234567890123456789012345678901.1234567,-12345.67,
           -922337203685477.5808,-214748.3648,
           '00112233-4455-6677-8899-aabbccddeeff',
           '0001-01-01','23:59:59.9999999',
           '1900-01-01T00:00:00.003','2079-06-06T23:59:00',
           '9999-12-31T23:59:59.9999999','2000-01-02T03:04:05.1234567+05:30',
           N'hello', 'ascii', 0x00FF, '<root>text</root>');
         INSERT #arrow_types DEFAULT VALUES;",
        )
        .await
        .unwrap();
    let results: Vec<ArrowBatch> = client
        .simple_query_arrow("SELECT * FROM #arrow_types ORDER BY seq")
        .try_collect()
        .await
        .unwrap();
    let batch = &results[0].batch;
    assert_eq!(batch.num_rows(), 2);
    for col in &batch.columns()[1..] {
        assert!(col.is_null(1), "null not preserved: {:?}", col.data_type());
    }
    assert!(array::<BooleanArray>(batch, "b").value(0));
    assert_eq!(array::<UInt8Array>(batch, "tiny").value(0), 255);
    assert_eq!(array::<Int16Array>(batch, "small").value(0), i16::MIN);
    assert_eq!(array::<Int32Array>(batch, "n").value(0), i32::MIN);
    assert_eq!(array::<Int64Array>(batch, "big").value(0), i64::MIN);
    assert_eq!(array::<Float32Array>(batch, "r").value(0), 1.5);
    assert_eq!(array::<Float64Array>(batch, "f").value(0), 2.5);
    assert_eq!(
        array::<Decimal128Array>(batch, "decval").value(0),
        "12345678901234567890123456789011234567"
            .parse::<i128>()
            .unwrap()
    );
    assert_eq!(array::<Decimal128Array>(batch, "num").value(0), -1234567);
    assert_eq!(
        array::<Decimal128Array>(batch, "m").value(0),
        i128::from(i64::MIN)
    );
    assert_eq!(
        array::<Decimal128Array>(batch, "sm").value(0),
        i128::from(i32::MIN)
    );
    assert_eq!(
        array::<StringArray>(batch, "guid").value(0),
        "00112233-4455-6677-8899-aabbccddeeff"
    );
    assert_eq!(array::<Date32Array>(batch, "d").value(0), -719162);
    assert_eq!(
        array::<Time64NanosecondArray>(batch, "t").value(0),
        86_399_999_999_900
    );
    let dt = array::<StructArray>(batch, "dt");
    assert_eq!(child::<Date32Array>(dt, "date").value(0), -25567);
    assert_eq!(child::<UInt32Array>(dt, "ticks_300").value(0), 1);
    let dt2 = array::<StructArray>(batch, "dt2");
    assert_eq!(child::<Date32Array>(dt2, "date").value(0), 3652058 - 719162);
    assert_eq!(
        child::<Time64NanosecondArray>(dt2, "time").value(0),
        86_399_999_999_900
    );
    let sdt = array::<StructArray>(batch, "sdt");
    assert_eq!(
        child::<Time64NanosecondArray>(sdt, "time").value(0),
        86_340_000_000_000
    );
    let dto = array::<StructArray>(batch, "dto");
    assert_eq!(child::<Int16Array>(dto, "offset_minutes").value(0), 330);
    // DATETIMEOFFSET's TDS payload carries the UTC date/time and the original offset.
    assert_eq!(child::<Date32Array>(dto, "date").value(0), 10957); // 2000-01-01
    assert_eq!(
        child::<Time64NanosecondArray>(dto, "time").value(0),
        77_645_123_456_700
    );
    assert_eq!(array::<LargeStringArray>(batch, "text").value(0), "hello");
    assert_eq!(array::<LargeStringArray>(batch, "narrow").value(0), "ascii");
    assert_eq!(array::<LargeBinaryArray>(batch, "bytes").value(0), [0, 255]);
    assert!(array::<LargeStringArray>(batch, "x")
        .value(0)
        .contains("<root>text</root>"));
}

#[tokio::test]
async fn source_metadata_duplicate_names_and_nullable_widths() {
    let Some(config) = test_config() else { return };
    let mut client = Client::connect(&config).await.unwrap();
    let results: Vec<ArrowBatch> = client
        .simple_query_arrow(
            "SELECT CAST(NULL AS tinyint) AS same, CAST(NULL AS smallint) AS same,
         CAST(NULL AS bigint) AS big, CAST(NULL AS real) AS r,
         CAST(NULL AS smalldatetime) AS dt, CAST(NULL AS smallmoney) AS money,
         CAST(NULL AS varchar(20)) COLLATE Latin1_General_100_CI_AS AS text WHERE 1=0",
        )
        .try_collect()
        .await
        .unwrap();
    let schema = results[0].batch.schema();
    assert_eq!(schema.field(0).name(), "same");
    assert_eq!(schema.field(1).name(), "same");
    assert_eq!(schema.field(0).data_type(), &DataType::UInt8);
    assert_eq!(schema.field(1).data_type(), &DataType::Int16);
    assert_eq!(schema.field(2).data_type(), &DataType::Int64);
    assert_eq!(schema.field(3).data_type(), &DataType::Float32);
    assert!(matches!(schema.field(4).data_type(), DataType::Struct(_)));
    assert_eq!(schema.field(5).data_type(), &DataType::Decimal128(10, 4));
    for field in schema.fields() {
        assert!(field.is_nullable());
        assert!(field.metadata().contains_key("mssql.type"));
    }
}

#[tokio::test]
async fn unicode_codepages_empty_and_max_values() {
    let Some(config) = test_config() else { return };
    let mut client = Client::connect(&config).await.unwrap();
    let results: Vec<ArrowBatch> = client.simple_query_arrow(
        "SELECT CONVERT(varchar(20), N'caf' + NCHAR(233)) COLLATE Latin1_General_100_CI_AS AS cp,
         CONVERT(varchar(50), (NCHAR(20013)+NCHAR(25991)) COLLATE Latin1_General_100_CI_AS_SC_UTF8) AS utf8,
         CAST(N'' AS nvarchar(max)) AS empty_text, CAST(0x AS varbinary(max)) AS empty_bytes,
         REPLICATE(CAST(NCHAR(20013) AS nvarchar(max)), 100000) AS long_text"
    ).try_collect().await.unwrap();
    let batch = &results[0].batch;
    assert_eq!(array::<LargeStringArray>(batch, "cp").value(0), "caf\u{e9}");
    assert_eq!(
        array::<LargeStringArray>(batch, "utf8").value(0),
        "\u{4e2d}\u{6587}"
    );
    assert_eq!(array::<LargeStringArray>(batch, "empty_text").value(0), "");
    assert!(array::<LargeBinaryArray>(batch, "empty_bytes")
        .value(0)
        .is_empty());
    assert_eq!(
        array::<LargeStringArray>(batch, "long_text").value(0),
        "\u{4e2d}".repeat(100000)
    );
}

#[tokio::test]
async fn spatial_bytes_preserve_native_serialization() {
    let Some(config) = test_config() else { return };
    let mut client = Client::connect(&config).await.unwrap();
    let results: Vec<ArrowBatch> = client
        .simple_query_arrow(
            "SELECT geography::Point(47.6,-122.3,4326) AS g,
         geography::Point(47.6,-122.3,4326).Serialize() AS expected,
         CAST(NULL AS geometry) AS null_geometry",
        )
        .try_collect()
        .await
        .unwrap();
    let batch = &results[0].batch;
    assert_eq!(
        array::<LargeBinaryArray>(batch, "g").value(0),
        array::<LargeBinaryArray>(batch, "expected").value(0)
    );
    assert!(array::<LargeBinaryArray>(batch, "null_geometry").is_null(0));
}

#[tokio::test]
async fn sql2025_json_and_vectors() {
    let Some(config) = test_config() else {
        return;
    };
    let mut client = Client::connect(&config).await.unwrap();
    let version = client
        .simple_query("SELECT CAST(SERVERPROPERTY('ProductMajorVersion') AS int)")
        .await
        .unwrap()
        .into_first_result()[0]
        .get::<i32, _>(0)
        .unwrap();
    if version < 17 {
        eprintln!("skipping native JSON/vector read: SQL Server 2025 is required");
        return;
    }
    let results: Vec<ArrowBatch> = client
        .simple_query_arrow(
            "SELECT CAST('[1,2,3]' AS vector(3)) AS v, CAST('{\"a\":1}' AS json) AS j
         UNION ALL SELECT CAST(NULL AS vector(3)), CAST(NULL AS json)",
        )
        .try_collect()
        .await
        .unwrap();
    let batch = &results[0].batch;
    let vectors = array::<FixedSizeListArray>(batch, "v");
    assert_eq!(vectors.value_length(), 3);
    assert!(vectors.is_null(1));
    let value = vectors.value(0);
    assert_eq!(
        value
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .values(),
        &[1.0, 2.0, 3.0]
    );
    let json = array::<LargeStringArray>(batch, "j");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(json.value(0)).unwrap(),
        serde_json::json!({"a": 1})
    );
    assert!(json.is_null(1));
}

#[tokio::test]
async fn oversized_row_is_separate_and_byte_target_is_soft() {
    let Some(config) = test_config() else { return };
    let mut client = Client::connect(&config).await.unwrap();
    let options = ArrowOptions {
        batch_bytes: 1024,
        ..ArrowOptions::default()
    };
    let results: Vec<ArrowBatch> = client.simple_query_arrow_with_options(
        "SELECT n AS id, CASE WHEN n=2 THEN REPLICATE(CAST(N'x' AS nvarchar(max)),2000) ELSE N'a' END AS text
         FROM (VALUES(1),(2),(3)) v(n) ORDER BY n",
        options,
    ).try_collect().await.unwrap();
    assert_eq!(
        results
            .iter()
            .map(|v| v.batch.num_rows())
            .collect::<Vec<_>>(),
        [1, 1, 1]
    );
    assert_eq!(
        array::<LargeStringArray>(&results[1].batch, "text")
            .value(0)
            .len(),
        2000
    );
}

#[tokio::test]
async fn defaults_accept_a_value_larger_than_eight_mib() {
    let Some(config) = test_config() else { return };
    let mut client = Client::connect(&config).await.unwrap();
    let batches: Vec<ArrowBatch> = client
        .simple_query_arrow("SELECT REPLICATE(CAST('x' AS varchar(max)), 8388609) AS large_value")
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].batch.num_rows(), 1);
    let value = array::<LargeStringArray>(&batches[0].batch, "large_value").value(0);
    assert_eq!(value.len(), 8 * 1024 * 1024 + 1);
    assert!(value.bytes().all(|byte| byte == b'x'));
    client.ping().await.unwrap();
}

#[tokio::test]
async fn opt_in_limits_return_errors_without_partial_batches() {
    let Some(config) = test_config() else { return };
    for options in [
        ArrowOptions {
            max_value_bytes: Some(32),
            ..ArrowOptions::default()
        },
        ArrowOptions {
            max_row_bytes: Some(32),
            ..ArrowOptions::default()
        },
    ] {
        let mut client = Client::connect(&config).await.unwrap();
        let mut stream = client.simple_query_arrow_with_options(
            "SELECT CAST(N'first' AS nvarchar(max)) AS text UNION ALL
             SELECT REPLICATE(CAST(N'x' AS nvarchar(max)),1000)",
            options,
        );
        assert!(matches!(
            stream.next().await.unwrap(),
            Err(Error::Conversion(_))
        ));
        assert!(stream.next().await.is_none());
        drop(stream);
        assert!(client.is_connection_dead());
    }
}

#[tokio::test]
async fn invalid_options_do_not_execute_or_poison_connection() {
    let Some(config) = test_config() else { return };
    let mut client = Client::connect(&config).await.unwrap();
    for options in [
        ArrowOptions {
            batch_size: 0,
            ..ArrowOptions::default()
        },
        ArrowOptions {
            batch_bytes: 0,
            ..ArrowOptions::default()
        },
        ArrowOptions {
            max_value_bytes: Some(0),
            ..ArrowOptions::default()
        },
        ArrowOptions {
            max_row_bytes: Some(0),
            ..ArrowOptions::default()
        },
    ] {
        let mut stream =
            client.simple_query_arrow_with_options("CREATE TABLE #not_executed(n int)", options);
        assert!(matches!(
            stream.next().await.unwrap(),
            Err(Error::Conversion(_))
        ));
        drop(stream);
        assert!(!client.is_connection_dead());
    }
    let rows = client
        .simple_query("SELECT OBJECT_ID('tempdb..#not_executed') AS object_id")
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(rows[0].get::<i32, _>(0), None);
}

#[tokio::test]
async fn unsupported_schema_is_rejected_even_without_rows() {
    let Some(config) = test_config() else { return };
    let mut client = Client::connect(&config).await.unwrap();
    let mut stream =
        client.simple_query_arrow("SELECT CAST(NULL AS sql_variant) AS value WHERE 1=0");
    let error = stream.next().await.unwrap().unwrap_err();
    assert!(matches!(error, Error::Conversion(_)));
    assert!(error.to_string().to_lowercase().contains("variant"));
    drop(stream);
    assert!(client.is_connection_dead());
}

#[tokio::test]
async fn dropping_unpolled_is_safe_but_started_streams_are_not_recycled() {
    let Some(config) = test_config() else { return };
    let pool = TdsManager::create_pool(config, 1).unwrap();
    let mut client = pool.get().await.unwrap();
    drop(client.simple_query_arrow("SELECT 1"));
    client.ping().await.unwrap();
    {
        let mut stream = client.simple_query_arrow_with_options(
            "SELECT 1 AS n UNION ALL SELECT 2",
            ArrowOptions {
                batch_size: 1,
                ..ArrowOptions::default()
            },
        );
        assert_eq!(stream.next().await.unwrap().unwrap().batch.num_rows(), 1);
    }
    assert!(client.is_connection_dead());
    drop(client);
    let mut replacement = pool.get().await.unwrap();
    assert!(!replacement.is_connection_dead());
    replacement.ping().await.unwrap();
}

#[tokio::test]
async fn cancellation_during_io_marks_connection_dead() {
    let Some(config) = test_config() else { return };
    let mut client = Client::connect(&config).await.unwrap();
    {
        let mut stream = client.simple_query_arrow("WAITFOR DELAY '00:00:03'; SELECT 1 AS n");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), stream.next())
                .await
                .is_err()
        );
    }
    assert!(client.is_connection_dead());
}

#[tokio::test]
async fn errors_after_a_published_batch_are_not_hidden() {
    let Some(config) = test_config() else { return };
    let mut client = Client::connect(&config).await.unwrap();
    let mut stream = client.simple_query_arrow_with_options(
        "SELECT 42 AS n; RAISERROR('arrow failure',16,1)",
        ArrowOptions {
            batch_size: 1,
            ..ArrowOptions::default()
        },
    );
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(array::<Int32Array>(&first.batch, "n").value(0), 42);
    assert!(matches!(stream.next().await.unwrap(), Err(Error::Tds(_))));
    assert!(stream.next().await.is_none());
    drop(stream);
    assert!(client.is_connection_dead());
    assert_eq!(array::<Int32Array>(&first.batch, "n").value(0), 42);
}
