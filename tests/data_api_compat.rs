//! Tiberius public data API contract from saurabh500/tiberius@e01b6a4.
//!
//! Connection construction is intentionally bridge-native. Every other live
//! assertion states data behavior from `tests/data_api.rs`, `tests/query.rs`,
//! or `tests/bulk.rs` at that commit.

use async_trait::async_trait;
use futures_util::TryStreamExt;
use mssql_tds::core::TdsResult;
use mssql_tds::datatypes::column_values::ColumnValues;
use mssql_tds::datatypes::sql_string::SqlString;
use mssql_tds::message::bulk_load::StreamingBulkLoadWriter;
use mssql_tiberius_bridge::bulk::BulkLoadRow;
use mssql_tiberius_bridge::{AuthMethod, Client, ColumnType, Config, Error, FromSql, Row, ToSql};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Status {
    UnchangedPass,
    AdaptedPass,
    CompileGap,
    BehavioralGap,
    IntentionalImprovement,
}

struct Trace {
    behavior: &'static str,
    source: &'static str,
    status: Status,
    evidence: &'static str,
}

const TRACEABILITY: &[Trace] = &[
    Trace { behavior: "primitive value and NULL conversions", source: "data_api.rs:public_value_conversions_cover_values_nulls_and_errors", status: Status::AdaptedPass, evidence: "tiberius_compat::{bool_type,u8_token,i16_token,i32_token,i64_token,f32_token,f64_token}" },
    Trace { behavior: "owned and borrowed strings", source: "data_api.rs:public_value_conversions_cover_values_nulls_and_errors", status: Status::AdaptedPass, evidence: "tiberius_compat::{string_roundtrip,str_borrow_from_row,str_borrow_null}" },
    Trace { behavior: "owned and borrowed binary", source: "data_api.rs:public_value_conversions_cover_values_nulls_and_errors", status: Status::AdaptedPass, evidence: "tiberius_compat::{varbinary_roundtrip,vec_u8_param_roundtrip}" },
    Trace { behavior: "UUID conversion", source: "data_api.rs:public_value_conversions_cover_values_nulls_and_errors", status: Status::AdaptedPass, evidence: "tiberius_compat::uuid_roundtrip" },
    Trace { behavior: "numeric conversion", source: "data_api.rs:public_value_conversions_cover_values_nulls_and_errors", status: Status::AdaptedPass, evidence: "tiberius_compat::{decimal_roundtrip,numeric_large}" },
    Trace { behavior: "XML conversion", source: "data_api.rs:public_value_conversions_cover_values_nulls_and_errors", status: Status::AdaptedPass, evidence: "tiberius_compat::xml_type" },
    Trace { behavior: "chrono conversions", source: "data_api.rs:chrono_values_round_trip_through_public_conversions", status: Status::AdaptedPass, evidence: "tiberius_compat::{naive_date_time,naive_date,naive_time,datetime_offset}" },
    Trace { behavior: "time crate conversions", source: "data_api.rs:time_values_round_trip_through_public_conversions", status: Status::AdaptedPass, evidence: "query::tests::time_temporals_roundtrip_with_fractional_seconds_and_offset" },
    Trace { behavior: "typed NULL parameter preservation", source: "data_api.rs:execute_dynamic_query_and_bulk_behavior", status: Status::AdaptedPass, evidence: "typed_null_parameter_preserves_column_type" },
    Trace { behavior: "FromSqlOwned conversions", source: "data_api.rs:public_value_conversions_cover_values_nulls_and_errors", status: Status::CompileGap, evidence: "compile_fail/data_api_conversions.rs (#127)" },
    Trace { behavior: "FromSql conversion error channel", source: "data_api.rs:public_value_conversions_cover_values_nulls_and_errors", status: Status::CompileGap, evidence: "compile_fail/data_api_conversion_errors.rs (#127)" },
    Trace { behavior: "ColumnData and IntoSql", source: "data_api.rs:public_value_conversions_cover_values_nulls_and_errors", status: Status::CompileGap, evidence: "compile_fail/data_api_conversions.rs (#127)" },
    Trace { behavior: "Column name and type metadata", source: "data_api.rs:public_numeric_time_xml_and_token_row_helpers", status: Status::UnchangedPass, evidence: "row_metadata_access_and_errors" },
    Trace { behavior: "Row named access", source: "data_api.rs:row_metadata_stream_and_collection_behavior", status: Status::UnchangedPass, evidence: "row_metadata_access_and_errors" },
    Trace { behavior: "Row numeric index access", source: "data_api.rs:row_metadata_stream_and_collection_behavior", status: Status::UnchangedPass, evidence: "row_metadata_access_and_errors" },
    Trace { behavior: "duplicate names select first", source: "data_api.rs:row_metadata_stream_and_collection_behavior", status: Status::UnchangedPass, evidence: "row_metadata_access_and_errors" },
    Trace { behavior: "missing named try_get is recoverable", source: "data_api.rs:row_metadata_stream_and_collection_behavior", status: Status::UnchangedPass, evidence: "row_metadata_access_and_errors" },
    Trace { behavior: "out-of-range numeric try_get is recoverable", source: "DATA_API_COVERAGE.md:Compatibility observations", status: Status::IntentionalImprovement, evidence: "row_metadata_access_and_errors" },
    Trace { behavior: "NULL row retrieval", source: "data_api.rs:row_metadata_stream_and_collection_behavior", status: Status::UnchangedPass, evidence: "row_metadata_access_and_errors" },
    Trace { behavior: "wrong-type retrieval reports conversion error", source: "data_api.rs:row_metadata_stream_and_collection_behavior", status: Status::BehavioralGap, evidence: "bridge FromSql currently represents mismatch as None" },
    Trace { behavior: "Row cells and consuming iterator", source: "data_api.rs:row_metadata_stream_and_collection_behavior", status: Status::CompileGap, evidence: "compile_fail/data_api_row_iteration.rs (#128)" },
    Trace { behavior: "Row result_index", source: "data_api.rs:row_metadata_stream_and_collection_behavior", status: Status::CompileGap, evidence: "compile_fail/data_api_query_stream.rs (#125)" },
    Trace { behavior: "QueryItem metadata and row variants", source: "data_api.rs:row_metadata_stream_and_collection_behavior", status: Status::CompileGap, evidence: "compile_fail/data_api_query_stream.rs (#125)" },
    Trace { behavior: "QueryStream columns metadata", source: "data_api.rs:row_metadata_stream_and_collection_behavior", status: Status::CompileGap, evidence: "compile_fail/data_api_query_stream.rs (#125)" },
    Trace { behavior: "into_row_stream collection", source: "data_api.rs:row_metadata_stream_and_collection_behavior", status: Status::AdaptedPass, evidence: "row_stream_collects_all_sets" },
    Trace { behavior: "into_results non-empty sets", source: "data_api.rs:row_metadata_stream_and_collection_behavior", status: Status::AdaptedPass, evidence: "collected_results_preserve_empty_middle_set" },
    Trace { behavior: "empty result-set boundaries", source: "DATA_API_COVERAGE.md:Compatibility observations", status: Status::IntentionalImprovement, evidence: "collected_results_preserve_empty_middle_set" },
    Trace { behavior: "into_first_result", source: "data_api.rs:row_metadata_stream_and_collection_behavior", status: Status::AdaptedPass, evidence: "tiberius_compat::multiple_result_sets" },
    Trace { behavior: "async into_row helper", source: "data_api.rs:row_metadata_stream_and_collection_behavior", status: Status::CompileGap, evidence: "compile_fail/data_api_query_stream_collectors.rs (#125)" },
    Trace { behavior: "ExecuteResult total", source: "tests/query.rs:execute_multiple_count_total", status: Status::AdaptedPass, evidence: "execute_counts_iterate_and_total" },
    Trace { behavior: "ExecuteResult inherent into_iter", source: "data_api.rs:execute_dynamic_query_and_bulk_behavior", status: Status::UnchangedPass, evidence: "execute_counts_iterate_and_total" },
    Trace { behavior: "ExecuteResult rows_affected", source: "data_api.rs:execute_dynamic_query_and_bulk_behavior", status: Status::CompileGap, evidence: "compile_fail/data_api_execute_rows_affected.rs (#130)" },
    Trace { behavior: "ExecuteResult IntoIterator trait", source: "data_api.rs:execute_dynamic_query_and_bulk_behavior", status: Status::CompileGap, evidence: "compile_fail/data_api_execute_into_iterator.rs (#130)" },
    Trace { behavior: "dynamic Query binding", source: "data_api.rs:execute_dynamic_query_and_bulk_behavior", status: Status::CompileGap, evidence: "compile_fail/data_api_query_builder.rs (#129)" },
    Trace { behavior: "dynamic NULL binding through Client::query", source: "data_api.rs:execute_dynamic_query_and_bulk_behavior", status: Status::AdaptedPass, evidence: "typed_null_parameter_preserves_column_type" },
    Trace { behavior: "bulk nullable-row success and count", source: "data_api.rs:execute_dynamic_query_and_bulk_behavior", status: Status::AdaptedPass, evidence: "bulk_nullable_rows_succeed" },
    Trace { behavior: "TokenRow helpers", source: "data_api.rs:public_numeric_time_xml_and_token_row_helpers", status: Status::CompileGap, evidence: "compile_fail/data_api_bulk_row.rs (#131)" },
    Trace { behavior: "IntoRow tuple arities 1 through 10", source: "data_api.rs:public_numeric_time_xml_and_token_row_helpers", status: Status::CompileGap, evidence: "compile_fail/data_api_bulk_row.rs (#131)" },
    Trace { behavior: "bulk send/finalize lifecycle", source: "data_api.rs:execute_dynamic_query_and_bulk_behavior", status: Status::CompileGap, evidence: "compile_fail/data_api_bulk_lifecycle.rs (#131)" },
    Trace { behavior: "bulk oversized value returns BulkInput", source: "data_api.rs:execute_dynamic_query_and_bulk_behavior", status: Status::CompileGap, evidence: "current batch BulkInsert API has no Tiberius lifecycle/error type" },
];

fn config() -> Option<Config> {
    let password = std::env::var("TEST_DB_PASSWORD").ok()?;
    let mut config = Config::new();
    config
        .host(std::env::var("TEST_DB_HOST").unwrap_or_else(|_| "localhost".to_string()))
        .port(
            std::env::var("TEST_DB_PORT")
                .ok()
                .and_then(|port| port.parse().ok())
                .unwrap_or(1433),
        )
        .database(std::env::var("TEST_DB_NAME").unwrap_or_else(|_| "master".to_string()))
        .authentication(AuthMethod::sql_server(
            std::env::var("TEST_DB_USER").unwrap_or_else(|_| "sa".to_string()),
            password,
        ))
        .trust_cert();
    Some(config)
}

async fn connect() -> Option<Client> {
    let config = config()?;
    Some(
        Client::connect(&config)
            .await
            .expect("connect to compatibility SQL Server"),
    )
}

#[test]
fn phase_1_traceability_is_complete_and_stable() {
    assert_eq!(TRACEABILITY.len(), 40);
    assert_eq!(
        TRACEABILITY
            .iter()
            .filter(|trace| trace.status == Status::UnchangedPass)
            .count(),
        7
    );
    assert_eq!(
        TRACEABILITY
            .iter()
            .filter(|trace| trace.status == Status::AdaptedPass)
            .count(),
        15
    );
    assert_eq!(
        TRACEABILITY
            .iter()
            .filter(|trace| trace.status == Status::CompileGap)
            .count(),
        15
    );
    assert_eq!(
        TRACEABILITY
            .iter()
            .filter(|trace| trace.status == Status::BehavioralGap)
            .count(),
        1
    );
    assert_eq!(
        TRACEABILITY
            .iter()
            .filter(|trace| trace.status == Status::IntentionalImprovement)
            .count(),
        2
    );
    for trace in TRACEABILITY {
        assert!(!trace.behavior.is_empty());
        assert!(!trace.source.is_empty());
        assert!(!trace.evidence.is_empty());
    }
}

#[tokio::test]
async fn row_metadata_access_and_errors() {
    let Some(mut client) = connect().await else {
        return;
    };
    let rows = client
        .simple_query(
            "SELECT CAST(7 AS int) AS duplicate, CAST(NULL AS int) AS nullable, \
             CAST(8 AS int) AS duplicate, CAST(9 AS int) AS unique_name",
        )
        .await
        .expect("query row contract")
        .into_first_result();
    let row = rows.first().expect("one result row");
    let first_column = row.columns().first().expect("first column metadata");

    assert_eq!(first_column.name(), "duplicate");
    assert_eq!(first_column.column_type(), ColumnType::Int4);
    assert_eq!(row.get::<i32, _>(0usize), Some(7));
    assert_eq!(row.get::<i32, _>("duplicate"), Some(7));
    assert_eq!(row.get::<i32, _>("unique_name"), Some(9));
    assert_eq!(row.get::<i32, _>("nullable"), None);
    assert_eq!(row.get::<Option<i32>, _>("nullable"), Some(None));
    assert!(matches!(
        row.try_get::<i32, _>("missing"),
        Err(Error::ColumnNotFound(name)) if name == "missing"
    ));
    assert!(matches!(
        row.try_get::<i32, _>(99usize),
        Err(Error::ColumnIndexOutOfBounds {
            index: 99,
            count: 4
        })
    ));
}

#[tokio::test]
async fn collected_results_preserve_empty_middle_set() {
    let Some(mut client) = connect().await else {
        return;
    };
    let result = client
        .simple_query(
            "SELECT CAST(7 AS int) AS first_value; \
             SELECT CAST(1 AS int) AS empty_value WHERE 1 = 0; \
             SELECT CAST(9 AS int) AS final_value",
        )
        .await
        .expect("query three result sets");
    assert_eq!(result.result_set_count(), 3);
    let results = result.into_results();
    assert_eq!(
        results.iter().map(Vec::len).collect::<Vec<_>>(),
        vec![1, 0, 1]
    );
    let first = results
        .first()
        .and_then(|rows| rows.first())
        .expect("first result row");
    let final_row = results
        .get(2)
        .and_then(|rows| rows.first())
        .expect("final result row");
    assert_eq!(first.get::<i32, _>("first_value"), Some(7));
    assert_eq!(final_row.get::<i32, _>("final_value"), Some(9));
}

#[tokio::test]
async fn row_stream_collects_all_sets() {
    let Some(mut client) = connect().await else {
        return;
    };
    let rows: Vec<Row> = client
        .simple_query("SELECT 1 AS value; SELECT 2 AS value")
        .await
        .expect("query stream source")
        .into_row_stream()
        .try_collect()
        .await
        .expect("collect row stream");
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows.first()
            .expect("first streamed row")
            .get::<i32, _>("value"),
        Some(1)
    );
    assert_eq!(
        rows.get(1)
            .expect("second streamed row")
            .get::<i32, _>("value"),
        Some(2)
    );
}

#[tokio::test]
async fn execute_counts_iterate_and_total() {
    let Some(mut client) = connect().await else {
        return;
    };
    client
        .simple_query("CREATE TABLE #compat_counts (id int)")
        .await
        .expect("create count table");
    let result = client
        .execute(
            "INSERT INTO #compat_counts VALUES (1), (2); \
             UPDATE #compat_counts SET id = id + 1; \
             DELETE FROM #compat_counts WHERE id = 99",
            &[],
        )
        .await
        .expect("execute count batch");
    assert_eq!(result.total(), 4);
    assert_eq!(result.into_iter().collect::<Vec<_>>(), vec![2, 2, 0]);
}

#[tokio::test]
async fn typed_null_parameter_preserves_column_type() {
    let Some(mut client) = connect().await else {
        return;
    };
    let value = None::<i32>;
    let rows = client
        .query("SELECT @P1 AS nullable", &[&value])
        .await
        .expect("query typed NULL")
        .into_first_result();
    let row = rows.first().expect("typed NULL row");
    assert_eq!(
        row.columns()
            .first()
            .expect("typed NULL column metadata")
            .column_type(),
        ColumnType::Int4
    );
    assert_eq!(row.get::<Option<i32>, _>("nullable"), Some(None));
}

#[derive(Clone)]
struct NullableBulkRow {
    id: i32,
    value: Option<String>,
}

#[async_trait]
impl BulkLoadRow for NullableBulkRow {
    async fn write_to_packet(
        &self,
        writer: &mut StreamingBulkLoadWriter<'_>,
        column_index: &mut usize,
    ) -> TdsResult<()> {
        writer
            .write_column_value(*column_index, &ColumnValues::Int(self.id))
            .await?;
        *column_index += 1;
        let value = self.value.as_ref().map_or(ColumnValues::Null, |value| {
            ColumnValues::String(SqlString::from_utf8_string(value.clone()))
        });
        writer.write_column_value(*column_index, &value).await?;
        *column_index += 1;
        Ok(())
    }
}

#[tokio::test]
async fn bulk_nullable_rows_succeed() {
    let Some(mut client) = connect().await else {
        return;
    };
    client
        .simple_query("CREATE TABLE #compat_bulk (id int NOT NULL, value nvarchar(8) NULL)")
        .await
        .expect("create bulk table");
    let result = client
        .bulk_insert("#compat_bulk")
        .send([
            NullableBulkRow {
                id: 1,
                value: Some("one".to_string()),
            },
            NullableBulkRow { id: 2, value: None },
        ])
        .await
        .expect("bulk insert nullable rows");
    assert_eq!(result.rows_affected, 2);

    let rows = client
        .simple_query("SELECT id, value FROM #compat_bulk ORDER BY id")
        .await
        .expect("read bulk rows")
        .into_first_result();
    assert_eq!(
        rows.first()
            .expect("first bulk row")
            .get::<&str, _>("value"),
        Some("one")
    );
    assert_eq!(
        rows.get(1)
            .expect("second bulk row")
            .get::<Option<&str>, _>("value"),
        Some(None)
    );
}

#[test]
fn local_conversion_contract_uses_bridge_value_types() {
    assert!(matches!(
        None::<i32>.to_sql(),
        mssql_tds::datatypes::sqltypes::SqlType::Int(None)
    ));
    assert_eq!(<i32 as FromSql>::from_sql(&ColumnValues::Int(7)), Some(7));
    assert_eq!(
        <&[u8] as FromSql>::from_sql(&ColumnValues::Bytes(vec![1, 2, 3])),
        Some(&[1, 2, 3][..])
    );
}
