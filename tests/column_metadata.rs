//! Result-column metadata regressions for issue #63, using TEST_DB_* settings.

use futures_util::TryStreamExt;
use mssql_tds::connection::tds_client::ResultSet;
use mssql_tiberius_bridge::{AuthMethod, Client, Column, ColumnType, Config, Row};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn env_or(name: &str, default: &str) -> Result<String, std::env::VarError> {
    match std::env::var(name) {
        Err(std::env::VarError::NotPresent) => Ok(default.to_owned()),
        value => value,
    }
}

async fn connect() -> TestResult<Option<Client>> {
    let password = match std::env::var("TEST_DB_PASSWORD") {
        Ok(password) => password,
        Err(std::env::VarError::NotPresent) => {
            eprintln!("TEST_DB_PASSWORD not set, skipping column metadata integration tests");
            return Ok(None);
        }
        Err(error) => return Err(error.into()),
    };
    let mut config = Config::new();
    config
        .host(env_or("TEST_DB_HOST", "localhost")?)
        .port(env_or("TEST_DB_PORT", "1433")?.parse()?)
        .database(env_or("TEST_DB_NAME", "master")?)
        .authentication(AuthMethod::sql_server(
            env_or("TEST_DB_USER", "sa")?,
            password,
        ))
        .trust_cert();
    Ok(Some(Client::connect(&config).await?))
}

fn column<'a>(columns: &'a [Column], name: &str) -> &'a Column {
    columns
        .iter()
        .find(|column| column.name() == name)
        .expect("expected named column metadata")
}

const SETUP: &str = "
    SET NOCOUNT ON;
    CREATE TABLE #bridge_metadata (
        id int IDENTITY(1,1) NOT NULL,
        optional_value int NULL,
        name nvarchar(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
        fixed_name nchar(255) COLLATE Latin1_General_100_BIN2 NULL,
        ansi_name varchar(255) COLLATE Latin1_General_100_CI_AS NOT NULL,
        fixed_ansi char(255) COLLATE Latin1_General_100_CI_AS NULL,
        amount decimal(18,4) NOT NULL,
        quantity numeric(38,0) NULL,
        small_money smallmoney NULL,
        money_amount money NULL,
        time_value time(7) NULL,
        datetime_value datetime2(3) NULL,
        offset_value datetimeoffset(4) NULL,
        bytes binary(16) NULL,
        variable_bytes varbinary(255) NULL,
        max_name nvarchar(max) COLLATE Latin1_General_100_BIN2 NULL,
        max_ansi varchar(max) COLLATE Latin1_General_100_CI_AS NULL,
        max_bytes varbinary(max) NULL,
        calculated AS (id + 10)
    );
    INSERT INTO #bridge_metadata (name, ansi_name, amount, max_name, max_ansi)
    VALUES (N'first', 'first', 12.3456, N'large', 'large'),
           (N'second', 'second', 98.7654, NULL, NULL);
";

const SELECT_COLUMNS: &str = "
    SELECT id, optional_value, name, fixed_name, ansi_name, fixed_ansi,
           amount, quantity, small_money, money_amount, time_value,
           datetime_value, offset_value, bytes, variable_bytes,
           max_name, max_ansi, max_bytes, calculated
    FROM #bridge_metadata
";

fn assert_metadata(columns: &[Column]) {
    assert_eq!(columns.len(), 19);
    assert!(column(columns, "id").is_identity());
    assert!(!column(columns, "id").nullable());
    assert!(column(columns, "optional_value").nullable());
    assert!(!column(columns, "optional_value").is_identity());
    assert!(column(columns, "calculated").is_computed());
    assert!(!column(columns, "amount").is_computed());

    for (name, column_type, bytes) in [
        ("name", ColumnType::NVarchar, 510),
        ("fixed_name", ColumnType::NChar, 510),
        ("ansi_name", ColumnType::Varchar, 255),
        ("fixed_ansi", ColumnType::Char, 255),
    ] {
        let column = column(columns, name);
        assert_eq!(column.column_type(), column_type, "{name}");
        assert_eq!(column.byte_length(), bytes, "{name}");
        assert_eq!(column.char_length(), Some(255), "{name}");
        assert!(!column.is_plp(), "{name}");
        assert_eq!(column.precision(), None, "{name}");
        assert_eq!(column.scale(), None, "{name}");
        assert_eq!(
            column
                .collation()
                .expect("string collation")
                .lcid_language_id,
            1033,
            "{name}"
        );
    }
    assert!(column(columns, "name").is_case_sensitive());
    assert!(!column(columns, "ansi_name").is_case_sensitive());

    for (name, precision, scale) in [("amount", 18, 4), ("quantity", 38, 0)] {
        let column = column(columns, name);
        assert!(matches!(
            column.column_type(),
            ColumnType::Decimaln | ColumnType::Numericn
        ));
        assert_eq!(column.precision(), Some(precision), "{name}");
        assert_eq!(column.scale(), Some(scale), "{name}");
        assert_eq!(column.char_length(), None, "{name}");
        assert_eq!(column.collation(), None, "{name}");
    }
    for (name, column_type, precision) in [
        ("small_money", ColumnType::Money4, 10),
        ("money_amount", ColumnType::Money, 19),
    ] {
        assert_eq!(column(columns, name).column_type(), column_type, "{name}");
        assert_eq!(column(columns, name).precision(), Some(precision), "{name}");
        assert_eq!(column(columns, name).scale(), None, "{name}");
    }
    for (name, scale) in [
        ("time_value", 7),
        ("datetime_value", 3),
        ("offset_value", 4),
    ] {
        assert_eq!(column(columns, name).scale(), Some(scale), "{name}");
        assert_eq!(column(columns, name).precision(), None, "{name}");
    }
    for (name, column_type, length) in [
        ("bytes", ColumnType::Binary, 16),
        ("variable_bytes", ColumnType::VarBinary, 255),
    ] {
        assert_eq!(column(columns, name).column_type(), column_type, "{name}");
        assert_eq!(column(columns, name).byte_length(), length, "{name}");
        assert_eq!(column(columns, name).char_length(), None, "{name}");
        assert_eq!(column(columns, name).collation(), None, "{name}");
    }
    for (name, column_type) in [
        ("max_name", ColumnType::NVarchar),
        ("max_ansi", ColumnType::Varchar),
        ("max_bytes", ColumnType::VarBinary),
    ] {
        let column = column(columns, name);
        assert_eq!(column.column_type(), column_type, "{name}");
        assert!(column.is_plp(), "{name}");
        assert_eq!(column.byte_length(), usize::from(u16::MAX), "{name}");
        assert_eq!(column.char_length(), None, "{name}");
    }
    assert_eq!(
        column(columns, "max_name").collation(),
        column(columns, "name").collation()
    );
    assert_eq!(
        column(columns, "max_ansi").collation(),
        column(columns, "ansi_name").collation()
    );
    assert!(columns
        .iter()
        .all(|column| column.multi_part_name().is_none()));
}

#[tokio::test]
async fn metadata_is_shared_by_buffered_and_streamed_rows() {
    let Some(mut client) = connect().await.expect("connect to SQL Server") else {
        return;
    };
    client
        .simple_query(SETUP)
        .await
        .expect("create metadata fixtures");
    let sql = format!("{SELECT_COLUMNS} ORDER BY id");
    let result = client
        .simple_query(sql.clone())
        .await
        .expect("query metadata");
    assert_metadata(result.columns().expect("result schema"));
    let columns = result.columns().expect("result schema").as_ptr();
    let buffered = result.into_first_result();
    assert_eq!(buffered.len(), 2);
    for row in &buffered {
        assert_eq!(row.columns().as_ptr(), columns);
    }

    let streamed: Vec<Row> = client
        .simple_query_streamed(sql.clone())
        .try_collect()
        .await
        .expect("stream metadata rows");
    let parameterized: Vec<Row> = client
        .query_streamed(
            format!("{SELECT_COLUMNS} WHERE id > @P1 ORDER BY id"),
            &[&0i32],
        )
        .try_collect()
        .await
        .expect("stream parameterized metadata rows");
    let buffered_stream: Vec<Row> = client
        .simple_query(sql)
        .await
        .expect("query buffered metadata rows")
        .into_row_stream()
        .try_collect()
        .await
        .expect("consume buffered metadata stream");
    assert_eq!(buffered, streamed);
    assert_eq!(buffered, parameterized);
    assert_eq!(buffered, buffered_stream);
    for rows in [&buffered, &streamed, &parameterized, &buffered_stream] {
        for row in rows {
            assert_metadata(row.columns());
        }
    }
}

#[tokio::test]
async fn metadata_survives_empty_parameterized_and_prepared_results() {
    let Some(mut client) = connect().await.expect("connect to SQL Server") else {
        return;
    };
    client
        .simple_query(SETUP)
        .await
        .expect("create metadata fixtures");
    let sql = format!("{SELECT_COLUMNS} WHERE id = @P1");
    let prepared = client
        .prepare(sql.clone(), &[&0i32])
        .await
        .expect("prepare metadata query");
    for id in [0i32, 1i32] {
        for result in [
            client
                .query(sql.clone(), &[&id])
                .await
                .expect("query parameterized metadata"),
            prepared
                .query(&mut client, &[&id])
                .await
                .expect("query prepared metadata"),
        ] {
            assert_eq!(result.result_set_count(), 1);
            assert_metadata(result.columns().expect("schema exists without rows"));
            assert!(result.result_set_columns(1).is_none());
            let rows = result.into_first_result();
            assert_eq!(
                rows.len(),
                usize::try_from(id).expect("nonnegative test id")
            );
            for row in &rows {
                assert_metadata(row.columns());
                assert_eq!(row.get::<i32, _>("id"), Some(id));
            }
        }
    }
    prepared
        .close(&mut client)
        .await
        .expect("close prepared metadata query");
    let result = client
        .simple_query(format!("{SELECT_COLUMNS} WHERE 1 = 0"))
        .await
        .expect("query empty result metadata");
    assert_metadata(result.columns().expect("empty simple-query schema"));
    assert!(result.into_first_result().is_empty());
}

#[tokio::test]
async fn metadata_indexes_include_empty_sets_but_not_dml_statements() {
    let Some(mut client) = connect().await.expect("connect to SQL Server") else {
        return;
    };
    let sql = "
        SET NOCOUNT OFF;
        DECLARE @values TABLE (n int);
        INSERT INTO @values VALUES (1), (2);
        SELECT CAST(NULL AS nvarchar(12)) AS first_empty WHERE 1 = 0;
        SELECT n AS present FROM @values ORDER BY n;
        SELECT CAST(NULL AS decimal(12,3)) AS middle_empty WHERE 1 = 0;
        SELECT CAST(3 AS bigint) AS last_value;
        SELECT CAST(NULL AS varbinary(9)) AS final_empty WHERE 1 = 0;
    ";
    let result = client
        .simple_query(sql)
        .await
        .expect("query mixed result sets");
    assert_eq!(result.result_set_count(), 5);
    for (index, name) in [
        "first_empty",
        "present",
        "middle_empty",
        "last_value",
        "final_empty",
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            result
                .result_set_columns(index)
                .and_then(|columns| columns.first())
                .map(Column::name),
            Some(name)
        );
    }
    assert_eq!(
        column(result.columns().expect("first schema"), "first_empty").char_length(),
        Some(12)
    );
    let numeric = column(
        result.result_set_columns(2).expect("middle schema"),
        "middle_empty",
    );
    assert_eq!(numeric.precision(), Some(12));
    assert_eq!(numeric.scale(), Some(3));
    assert_eq!(
        column(
            result.result_set_columns(4).expect("last schema"),
            "final_empty"
        )
        .byte_length(),
        9
    );
    assert!(result.result_set_columns(5).is_none());
    let sets = result.into_results();
    assert_eq!(
        sets.iter().map(Vec::len).collect::<Vec<_>>(),
        vec![0, 2, 0, 1, 0]
    );
    assert!(client
        .simple_query(sql)
        .await
        .expect("query an empty first result set")
        .into_first_result()
        .is_empty());
    let rows: Vec<Row> = client
        .simple_query(sql)
        .await
        .expect("query mixed result sets for streaming")
        .into_row_stream()
        .try_collect()
        .await
        .expect("stream mixed result sets");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows.first().expect("first row").get::<i32, _>(0), Some(1));
    assert_eq!(rows.get(1).expect("second row").get::<i32, _>(0), Some(2));
    assert_eq!(rows.get(2).expect("third row").get::<i64, _>(0), Some(3));

    let no_rows = client
        .simple_query("DECLARE @n int = 1; SET @n = 2;")
        .await
        .expect("execute statements without row results");
    assert_eq!(no_rows.result_set_count(), 0);
    assert!(no_rows.columns().is_none());
}

#[tokio::test]
async fn metadata_sparse_column_sets_are_not_ordinary_sparse_or_guid_columns() {
    let Some(mut client) = connect().await.expect("connect to SQL Server") else {
        return;
    };
    let result = client
        .simple_query(
            "
        CREATE TABLE #bridge_metadata_sparse (
            sparse_value int SPARSE NULL,
            row_guid uniqueidentifier ROWGUIDCOL NOT NULL,
            ordinary_guid uniqueidentifier NULL,
            sparse_set xml COLUMN_SET FOR ALL_SPARSE_COLUMNS
        );
        INSERT INTO #bridge_metadata_sparse (sparse_value, row_guid, ordinary_guid)
        VALUES (1, NEWID(), NEWID());
        SELECT sparse_value, row_guid, ordinary_guid, sparse_set
        FROM #bridge_metadata_sparse;
    ",
        )
        .await
        .expect("query sparse and GUID metadata");
    let columns = result.columns().expect("sparse result schema");
    assert!(!column(columns, "sparse_value").is_sparse_column_set());
    let sparse_set = column(columns, "sparse_set");
    assert!(sparse_set.is_sparse_column_set());
    assert_eq!(sparse_set.column_type(), ColumnType::Xml);
    assert!(sparse_set.is_plp());
    for name in ["row_guid", "ordinary_guid"] {
        let column = column(columns, name);
        assert_eq!(column.column_type(), ColumnType::Guid);
        assert!(!column.is_sparse_column_set());
        assert!(!column.is_identity());
    }
}

#[tokio::test]
async fn metadata_preserves_populated_legacy_lob_source_names() {
    let Some(mut client) = connect().await.expect("connect to SQL Server") else {
        return;
    };
    client
        .simple_query(
            "
        CREATE TABLE #bridge_metadata_lob (
            narrow_text text NULL,
            wide_text ntext NULL,
            image_value image NULL
        );
        INSERT INTO #bridge_metadata_lob VALUES ('narrow', N'wide', 0x0102);
    ",
        )
        .await
        .expect("create LOB metadata fixtures");
    let sql = "SELECT narrow_text, wide_text, image_value FROM #bridge_metadata_lob";
    client
        .inner_mut()
        .execute(sql.to_owned(), ())
        .await
        .expect("read native LOB metadata");
    let expected: Vec<_> = client
        .inner_mut()
        .get_metadata()
        .iter()
        .map(|column| column.multi_part_name.clone())
        .collect();
    let result = client
        .simple_query(sql)
        .await
        .expect("query bridged LOB metadata");
    let columns = result.columns().expect("LOB schema");
    assert_eq!(columns.len(), 3);
    assert_eq!(expected.len(), columns.len());
    for (column, expected) in columns.iter().zip(&expected) {
        let expected = expected
            .as_ref()
            .expect("server supplies a LOB source name");
        let actual = column
            .multi_part_name()
            .expect("bridge retains the source name");
        assert_eq!(actual.server_name.as_deref(), expected.server_name());
        assert_eq!(actual.catalog_name.as_deref(), expected.catalog_name());
        assert_eq!(actual.schema_name.as_deref(), expected.schema_name());
        assert_eq!(actual.table_name, expected.table_name());
        assert!(actual.table_name.starts_with("#bridge_metadata_lob"));
    }
    let rows = result.into_first_result();
    let row = rows.first().expect("LOB row");
    assert_eq!(row.get::<&str, _>("narrow_text"), Some("narrow"));
    assert_eq!(row.get::<&str, _>("wide_text"), Some("wide"));
    assert_eq!(row.get::<&[u8], _>("image_value"), Some(&[1, 2][..]));
}
