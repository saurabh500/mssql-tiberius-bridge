//! Integration tests requiring a live SQL Server instance.
//!
//! Set these env vars to run:
//!   TEST_DB_HOST (default: localhost)
//!   TEST_DB_PORT (default: 1433)
//!   TEST_DB_USER (default: sa)
//!   TEST_DB_PASSWORD (required)
//!   TEST_DB_NAME (default: master)

use mssql_tiberius_bridge::{AuthMethod, Client, Config, TdsManager};

fn test_config() -> Config {
    let password = match std::env::var("TEST_DB_PASSWORD") {
        Ok(p) => p,
        Err(_) => {
            eprintln!("TEST_DB_PASSWORD not set, skipping integration tests");
            std::process::exit(0);
        }
    };

    let mut cfg = Config::new();
    cfg.host(std::env::var("TEST_DB_HOST").unwrap_or("localhost".into()))
        .port(
            std::env::var("TEST_DB_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(1433),
        )
        .database(std::env::var("TEST_DB_NAME").unwrap_or("master".into()))
        .authentication(AuthMethod::sql_server(
            std::env::var("TEST_DB_USER").unwrap_or("sa".into()),
            password,
        ))
        .trust_cert();
    cfg
}

#[tokio::test]
async fn connect_and_select_one() {
    let cfg = test_config();
    let mut client = Client::connect(&cfg).await.expect("connect failed");

    let rows = client
        .simple_query("SELECT 1 AS value")
        .await
        .expect("query failed")
        .into_first_result();

    assert_eq!(rows.len(), 1);
    let val: i32 = rows[0].get("value").expect("column 'value' not found");
    assert_eq!(val, 1);
}

#[tokio::test]
async fn select_multiple_types() {
    let cfg = test_config();
    let mut client = Client::connect(&cfg).await.expect("connect failed");

    let rows = client
        .simple_query(
            "SELECT \
                CAST(42 AS int) AS int_col, \
                CAST(1 AS bit) AS bit_col, \
                CAST(3.14 AS float) AS float_col, \
                CAST('hello' AS nvarchar(50)) AS str_col, \
                NEWID() AS guid_col",
        )
        .await
        .expect("query failed")
        .into_first_result();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i32, _>("int_col"), Some(42));
    assert_eq!(rows[0].get::<bool, _>("bit_col"), Some(true));
    assert!(rows[0].get::<f64, _>("float_col").is_some());
    assert!(rows[0].get::<String, _>("str_col").is_some());
    assert!(rows[0].get::<uuid::Uuid, _>("guid_col").is_some());
}

#[tokio::test]
async fn parameterized_query() {
    let cfg = test_config();
    let mut client = Client::connect(&cfg).await.expect("connect failed");

    let rows = client
        .query("SELECT @P1 AS a, @P2 AS b", &[&42i32, &"world"])
        .await
        .expect("query failed")
        .into_first_result();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i32, _>("a"), Some(42));
    assert_eq!(rows[0].get::<String, _>("b"), Some("world".into()));
}

#[tokio::test]
async fn null_handling() {
    let cfg = test_config();
    let mut client = Client::connect(&cfg).await.expect("connect failed");

    let rows = client
        .simple_query("SELECT CAST(NULL AS int) AS nullable_col")
        .await
        .expect("query failed")
        .into_first_result();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i32, _>("nullable_col"), None);
    assert_eq!(rows[0].get::<Option<i32>, _>("nullable_col"), Some(None));
}

#[tokio::test]
async fn multiple_rows() {
    let cfg = test_config();
    let mut client = Client::connect(&cfg).await.expect("connect failed");

    let rows = client
        .simple_query("SELECT name FROM sys.databases WHERE database_id <= 4 ORDER BY database_id")
        .await
        .expect("query failed")
        .into_first_result();

    // System DBs: master, tempdb, model, msdb
    assert!(rows.len() >= 4);
    assert_eq!(rows[0].get::<String, _>("name"), Some("master".into()));
}

#[tokio::test]
async fn get_by_index() {
    let cfg = test_config();
    let mut client = Client::connect(&cfg).await.expect("connect failed");

    let rows = client
        .simple_query("SELECT 99 AS val")
        .await
        .expect("query failed")
        .into_first_result();

    assert_eq!(rows[0].get::<i32, _>(0usize), Some(99));
}

#[tokio::test]
async fn datetime_types() {
    let cfg = test_config();
    let mut client = Client::connect(&cfg).await.expect("connect failed");

    let rows = client
        .simple_query(
            "SELECT \
                CAST('2026-05-08' AS date) AS d, \
                CAST('13:30:00' AS time) AS t, \
                CAST('2026-05-08 13:30:00' AS datetime2) AS dt2",
        )
        .await
        .expect("query failed")
        .into_first_result();

    assert_eq!(rows.len(), 1);
    let d: chrono::NaiveDate = rows[0].get("d").expect("date");
    assert_eq!(d, chrono::NaiveDate::from_ymd_opt(2026, 5, 8).unwrap());

    let t: chrono::NaiveTime = rows[0].get("t").expect("time");
    assert_eq!(t, chrono::NaiveTime::from_hms_opt(13, 30, 0).unwrap());

    let dt: chrono::NaiveDateTime = rows[0].get("dt2").expect("datetime2");
    assert_eq!(
        dt.date(),
        chrono::NaiveDate::from_ymd_opt(2026, 5, 8).unwrap()
    );
}

#[tokio::test]
async fn decimal_type() {
    let cfg = test_config();
    let mut client = Client::connect(&cfg).await.expect("connect failed");

    let rows = client
        .simple_query("SELECT CAST(123.45 AS decimal(10,2)) AS dec_col")
        .await
        .expect("query failed")
        .into_first_result();

    let d: rust_decimal::Decimal = rows[0].get("dec_col").expect("decimal");
    assert_eq!(d.to_string(), "123.45");
}

#[tokio::test]
async fn client_ping() {
    let cfg = test_config();
    let mut client = Client::connect(&cfg).await.expect("connect failed");

    client.ping().await.expect("ping failed");

    let rows = client
        .simple_query("SELECT 1 AS value")
        .await
        .expect("query after ping failed")
        .into_first_result();
    assert_eq!(rows[0].get::<i32, _>("value"), Some(1));
}

#[tokio::test]
#[ignore = "requires SQL Server reachable at 10.0.0.21:1434 and TEST_DB_PASSWORD"]
async fn client_ping_against_known_sql_server() {
    let password = match std::env::var("TEST_DB_PASSWORD") {
        Ok(p) => p,
        Err(_) => {
            eprintln!("TEST_DB_PASSWORD not set, skipping");
            return;
        }
    };
    let mut cfg = Config::new();
    cfg.host("10.0.0.21")
        .port(1434)
        .database("master")
        .authentication(AuthMethod::sql_server(
            std::env::var("TEST_DB_USER").unwrap_or("sa".into()),
            password,
        ))
        .trust_cert();

    let mut client = Client::connect(&cfg).await.expect("connect failed");
    client.ping().await.expect("ping failed");
}

#[tokio::test]
async fn connection_pool() {
    let cfg = test_config();
    let pool = TdsManager::create_pool(cfg, 4).expect("pool creation failed");

    let mut conn = pool.get().await.expect("pool checkout failed");

    let rows = conn
        .simple_query("SELECT 1 AS value")
        .await
        .expect("query failed")
        .into_first_result();
    assert_eq!(rows[0].get::<i32, _>("value"), Some(1));
}

#[tokio::test]
async fn binary_data() {
    let cfg = test_config();
    let mut client = Client::connect(&cfg).await.expect("connect failed");

    let rows = client
        .simple_query("SELECT CAST(0xDEADBEEF AS varbinary(4)) AS bin_col")
        .await
        .expect("query failed")
        .into_first_result();

    let bytes: Vec<u8> = rows[0].get("bin_col").expect("binary");
    assert_eq!(bytes, vec![0xDE, 0xAD, 0xBE, 0xEF]);
}

// TODO: Enable when mssql-tds implements TDS wire encoding for SqlType::Numeric.
// Currently the ToSql impl correctly converts Decimal to SqlType::Numeric, but
// the downstream TDS parameter encoding is not yet implemented in the mssql-tds
// crate. Unit tests in src/query.rs validate the roundtrip conversion logic.
#[tokio::test]
#[ignore = "blocked on mssql-tds SqlType::Numeric TDS encoding implementation"]
async fn decimal_parameter_roundtrip() {
    use rust_decimal::Decimal;

    let cfg = test_config();
    let mut client = Client::connect(&cfg).await.expect("connect failed");

    // Create a temporary table
    let decimal_val = Decimal::new(12345, 2); // 123.45
    client
        .simple_query("CREATE TABLE #temp_decimal_test (id INT, amount NUMERIC(10, 4))")
        .await
        .expect("create table failed");

    // Insert with decimal parameter
    client
        .query(
            "INSERT INTO #temp_decimal_test (id, amount) VALUES (@P1, @P2)",
            &[&1i32, &decimal_val],
        )
        .await
        .expect("insert failed");

    // Read back the decimal value
    let rows = client
        .simple_query("SELECT amount FROM #temp_decimal_test WHERE id = 1")
        .await
        .expect("select failed")
        .into_first_result();

    assert_eq!(rows.len(), 1);
    let read_val: Option<Decimal> = rows[0].get("amount");
    assert_eq!(read_val, Some(decimal_val));

    // Clean up
    client
        .simple_query("DROP TABLE #temp_decimal_test")
        .await
        .expect("drop table failed");
}
