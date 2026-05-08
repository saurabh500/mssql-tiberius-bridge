use chrono::TimeZone;
use mssql_tiberius_bridge::{AuthMethod, Client, Config};

fn test_config() -> Config {
    let password = match std::env::var("TEST_DB_PASSWORD") {
        Ok(p) => p,
        Err(_) => {
            eprintln!("TEST_DB_PASSWORD not set, skipping");
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

async fn connect() -> Client {
    Client::connect(&test_config())
        .await
        .expect("connect failed")
}

// =============================================================================
// 1. Type round-trips via parameterized queries
// =============================================================================

#[tokio::test]
async fn bool_type() {
    let mut client = connect().await;
    let row = client
        .query("SELECT @P1", &[&true])
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(row[0].get::<bool, _>(0usize), Some(true));

    let row = client
        .query("SELECT @P1", &[&false])
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(row[0].get::<bool, _>(0usize), Some(false));
}

#[tokio::test]
async fn u8_token() {
    let mut client = connect().await;
    let row = client
        .query("SELECT @P1", &[&255u8])
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(row[0].get::<u8, _>(0usize), Some(255u8));
}

#[tokio::test]
async fn i16_token() {
    let mut client = connect().await;
    let row = client
        .query("SELECT @P1", &[&i16::MIN])
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(row[0].get::<i16, _>(0usize), Some(i16::MIN));

    let row = client
        .query("SELECT @P1", &[&i16::MAX])
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(row[0].get::<i16, _>(0usize), Some(i16::MAX));
}

#[tokio::test]
async fn i32_token() {
    let mut client = connect().await;
    let row = client
        .query("SELECT @P1", &[&i32::MIN])
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(row[0].get::<i32, _>(0usize), Some(i32::MIN));

    let row = client
        .query("SELECT @P1", &[&i32::MAX])
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(row[0].get::<i32, _>(0usize), Some(i32::MAX));
}

#[tokio::test]
async fn i64_token() {
    let mut client = connect().await;
    let row = client
        .query("SELECT @P1", &[&i64::MIN])
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(row[0].get::<i64, _>(0usize), Some(i64::MIN));

    let row = client
        .query("SELECT @P1", &[&i64::MAX])
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(row[0].get::<i64, _>(0usize), Some(i64::MAX));
}

#[tokio::test]
async fn f32_token() {
    let mut client = connect().await;
    let row = client
        .query("SELECT @P1", &[&1.23f32])
        .await
        .unwrap()
        .into_first_result();
    let val = row[0].get::<f32, _>(0usize).unwrap();
    assert!((val - 1.23f32).abs() < f32::EPSILON);
}

#[tokio::test]
async fn f64_token() {
    let mut client = connect().await;
    let row = client
        .query("SELECT @P1", &[&1.23456789f64])
        .await
        .unwrap()
        .into_first_result();
    let val = row[0].get::<f64, _>(0usize).unwrap();
    assert!((val - 1.23456789f64).abs() < f64::EPSILON);
}

#[tokio::test]
async fn string_roundtrip() {
    let mut client = connect().await;
    let input = "hello world";
    let row = client
        .query("SELECT @P1", &[&input])
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(
        row[0].get::<String, _>(0usize),
        Some("hello world".to_string())
    );
}

#[tokio::test]
async fn uuid_roundtrip() {
    let mut client = connect().await;
    let id = uuid::Uuid::parse_str("936da01f-9abd-4d9d-80c7-02af85c822a8").unwrap();
    let row = client
        .query("SELECT @P1", &[&id])
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(row[0].get::<uuid::Uuid, _>(0usize), Some(id));
}

#[tokio::test]
async fn decimal_roundtrip() {
    let mut client = connect().await;
    let row = client
        .query("SELECT CAST(123.456 AS decimal(10,3))", &[])
        .await
        .unwrap()
        .into_first_result();
    let val = row[0].get::<rust_decimal::Decimal, _>(0usize).unwrap();
    let expected: rust_decimal::Decimal = "123.456".parse().unwrap();
    assert_eq!(val, expected);
}

// =============================================================================
// 2. Nullable types
// =============================================================================

#[tokio::test]
async fn nullable_i32_some() {
    let mut client = connect().await;
    let row = client
        .query("SELECT @P1", &[&42i32])
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(row[0].get::<Option<i32>, _>(0usize), Some(Some(42)));
}

#[tokio::test]
async fn nullable_i32_none() {
    let mut client = connect().await;
    let row = client
        .query("SELECT CAST(NULL AS int)", &[])
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(row[0].get::<Option<i32>, _>(0usize), Some(None));
}

#[tokio::test]
async fn nullable_string_some() {
    let mut client = connect().await;
    let row = client
        .query("SELECT @P1", &[&"hello"])
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(
        row[0].get::<Option<String>, _>(0usize),
        Some(Some("hello".to_string()))
    );
}

#[tokio::test]
async fn nullable_string_none() {
    let mut client = connect().await;
    let row = client
        .query("SELECT CAST(NULL AS nvarchar(50))", &[])
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(row[0].get::<Option<String>, _>(0usize), Some(None));
}

// =============================================================================
// 3. String encoding tests
// =============================================================================

#[tokio::test]
async fn kanji_nvarchar() {
    let mut client = connect().await;
    let text = "につい765765t";
    client
        .simple_query("CREATE TABLE #kanji_test (val nvarchar(100))")
        .await
        .unwrap();
    client
        .execute("INSERT INTO #kanji_test (val) VALUES (@P1)", &[&text])
        .await
        .unwrap();
    let row = client
        .query("SELECT val FROM #kanji_test", &[])
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(row[0].get::<String, _>(0usize), Some(text.to_string()));
}

#[tokio::test]
async fn finnish_varchar() {
    let mut client = connect().await;
    let text = "Ä is for Ansen";
    client
        .simple_query("CREATE TABLE #finnish_test (val nvarchar(100))")
        .await
        .unwrap();
    client
        .execute("INSERT INTO #finnish_test (val) VALUES (@P1)", &[&text])
        .await
        .unwrap();
    let row = client
        .query("SELECT val FROM #finnish_test", &[])
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(row[0].get::<String, _>(0usize), Some(text.to_string()));
}

#[tokio::test]
async fn empty_string() {
    let mut client = connect().await;
    let row = client
        .query("SELECT CAST('' AS varchar(10))", &[])
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(
        row[0].get::<Option<String>, _>(0usize),
        Some(Some(String::new()))
    );
}

// =============================================================================
// 4. Date/time types
// =============================================================================

#[tokio::test]
async fn naive_date_time() {
    let mut client = connect().await;
    let row = client
        .query("SELECT CAST('2020-04-20 16:20:00' AS datetime2)", &[])
        .await
        .unwrap()
        .into_first_result();
    let val = row[0].get::<chrono::NaiveDateTime, _>(0usize).unwrap();
    let expected = chrono::NaiveDate::from_ymd_opt(2020, 4, 20)
        .unwrap()
        .and_hms_opt(16, 20, 0)
        .unwrap();
    assert_eq!(val, expected);
}

#[tokio::test]
async fn naive_date() {
    let mut client = connect().await;
    let row = client
        .query("SELECT CAST('2020-04-20' AS date)", &[])
        .await
        .unwrap()
        .into_first_result();
    let val = row[0].get::<chrono::NaiveDate, _>(0usize).unwrap();
    assert_eq!(val, chrono::NaiveDate::from_ymd_opt(2020, 4, 20).unwrap());
}

#[tokio::test]
async fn naive_time() {
    let mut client = connect().await;
    let row = client
        .query("SELECT CAST('16:20:00' AS time)", &[])
        .await
        .unwrap()
        .into_first_result();
    let val = row[0].get::<chrono::NaiveTime, _>(0usize).unwrap();
    assert_eq!(val, chrono::NaiveTime::from_hms_opt(16, 20, 0).unwrap());
}

#[tokio::test]
async fn datetime_offset() {
    let mut client = connect().await;
    let row = client
        .query(
            "SELECT CAST('2020-04-20 16:20:00 +02:00' AS datetimeoffset)",
            &[],
        )
        .await
        .unwrap()
        .into_first_result();
    let val = row[0]
        .get::<chrono::DateTime<chrono::FixedOffset>, _>(0usize)
        .unwrap();
    let expected = chrono::FixedOffset::east_opt(2 * 3600)
        .unwrap()
        .from_local_datetime(
            &chrono::NaiveDate::from_ymd_opt(2020, 4, 20)
                .unwrap()
                .and_hms_opt(16, 20, 0)
                .unwrap(),
        )
        .unwrap();
    assert_eq!(val, expected);
}

// =============================================================================
// 5. Binary types
// =============================================================================

#[tokio::test]
async fn varbinary_roundtrip() {
    let mut client = connect().await;
    let row = client
        .query("SELECT CAST(0xDEADBEEF AS varbinary(4))", &[])
        .await
        .unwrap()
        .into_first_result();
    let val = row[0].get::<Vec<u8>, _>(0usize).unwrap();
    assert_eq!(val, vec![0xDE, 0xAD, 0xBE, 0xEF]);
}

#[tokio::test]
async fn varbinary_empty() {
    let mut client = connect().await;
    let row = client
        .query("SELECT CAST(0x AS varbinary(1))", &[])
        .await
        .unwrap()
        .into_first_result();
    let val = row[0].get::<Vec<u8>, _>(0usize).unwrap();
    assert!(val.is_empty());
}

#[tokio::test]
async fn binary_type() {
    let mut client = connect().await;
    let row = client
        .query("SELECT CAST(0x0102030405 AS binary(5))", &[])
        .await
        .unwrap()
        .into_first_result();
    let val = row[0].get::<Vec<u8>, _>(0usize).unwrap();
    assert_eq!(val, vec![0x01, 0x02, 0x03, 0x04, 0x05]);
}

// =============================================================================
// 6. Multiple rows and result sets
// =============================================================================

#[tokio::test]
async fn multiple_rows() {
    let mut client = connect().await;
    let rows = client
        .query("SELECT value FROM (VALUES (1),(2),(3)) AS t(value)", &[])
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<i32, _>(0usize), Some(1));
    assert_eq!(rows[1].get::<i32, _>(0usize), Some(2));
    assert_eq!(rows[2].get::<i32, _>(0usize), Some(3));
}

#[tokio::test]
async fn multiple_result_sets() {
    let mut client = connect().await;
    let results = client
        .query("SELECT 1; SELECT 'hello'", &[])
        .await
        .unwrap()
        .into_results();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0][0].get::<i32, _>(0usize), Some(1));
    assert_eq!(
        results[1][0].get::<String, _>(0usize),
        Some("hello".to_string())
    );
}

#[tokio::test]
async fn empty_result_set() {
    let mut client = connect().await;
    let rows = client
        .query("SELECT 1 WHERE 1=0", &[])
        .await
        .unwrap()
        .into_first_result();
    assert!(rows.is_empty());
}

// =============================================================================
// 7. DML execution
// =============================================================================

#[tokio::test]
async fn execute_insert_update_delete() {
    let mut client = connect().await;
    client
        .simple_query("CREATE TABLE #dml_test (id int, name nvarchar(50))")
        .await
        .unwrap();

    let result = client
        .execute(
            "INSERT INTO #dml_test (id, name) VALUES (@P1, @P2)",
            &[&1i32, &"test"],
        )
        .await
        .unwrap();
    assert_eq!(result.total(), 1);

    let result = client
        .execute(
            "UPDATE #dml_test SET name = @P1 WHERE id = @P2",
            &[&"updated", &1i32],
        )
        .await
        .unwrap();
    assert_eq!(result.total(), 1);

    let result = client
        .execute("DELETE FROM #dml_test WHERE id = @P1", &[&1i32])
        .await
        .unwrap();
    assert_eq!(result.total(), 1);
}

#[tokio::test]
async fn simple_query_ddl() {
    let mut client = connect().await;
    client
        .simple_query("CREATE TABLE #ddl_test (id int)")
        .await
        .unwrap();
    client.simple_query("DROP TABLE #ddl_test").await.unwrap();
}

// =============================================================================
// 8. Transactions
// =============================================================================

#[tokio::test]
async fn transaction_commit() {
    let mut client = connect().await;
    client
        .simple_query("CREATE TABLE #tx_commit (id int)")
        .await
        .unwrap();
    client.simple_query("BEGIN TRAN").await.unwrap();
    client
        .simple_query("INSERT INTO #tx_commit VALUES (1)")
        .await
        .unwrap();
    client.simple_query("COMMIT").await.unwrap();

    let rows = client
        .query("SELECT id FROM #tx_commit", &[])
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i32, _>(0usize), Some(1));
}

#[tokio::test]
async fn transaction_rollback() {
    let mut client = connect().await;
    client
        .simple_query("CREATE TABLE #tx_rollback (id int)")
        .await
        .unwrap();
    client.simple_query("BEGIN TRAN").await.unwrap();
    client
        .simple_query("INSERT INTO #tx_rollback VALUES (1)")
        .await
        .unwrap();
    client.simple_query("ROLLBACK").await.unwrap();

    let rows = client
        .query("SELECT id FROM #tx_rollback", &[])
        .await
        .unwrap()
        .into_first_result();
    assert!(rows.is_empty());
}

// =============================================================================
// 9. Numeric/Decimal edge cases
// =============================================================================

#[tokio::test]
async fn numeric_large() {
    let mut client = connect().await;
    let row = client
        .query(
            "SELECT CAST(99999999999999999999.999999 AS numeric(38,6))",
            &[],
        )
        .await
        .unwrap()
        .into_first_result();
    let val = row[0].get::<rust_decimal::Decimal, _>(0usize).unwrap();
    let expected: rust_decimal::Decimal = "99999999999999999999.999999".parse().unwrap();
    assert_eq!(val, expected);
}

#[tokio::test]
async fn money_type() {
    let mut client = connect().await;
    let row = client
        .query("SELECT CAST(1234.5678 AS money)", &[])
        .await
        .unwrap()
        .into_first_result();
    let val = row[0].get::<rust_decimal::Decimal, _>(0usize).unwrap();
    let expected: rust_decimal::Decimal = "1234.5678".parse().unwrap();
    assert_eq!(val, expected);
}

// =============================================================================
// 10. XML type
// =============================================================================

#[tokio::test]
async fn xml_type() {
    let mut client = connect().await;
    let row = client
        .query("SELECT CAST('<root><item>test</item></root>' AS xml)", &[])
        .await
        .unwrap()
        .into_first_result();
    let val = row[0].get::<String, _>(0usize).unwrap();
    assert_eq!(val, "<root><item>test</item></root>");
}

// =============================================================================
// 11. Column access patterns
// =============================================================================

#[tokio::test]
async fn get_by_name() {
    let mut client = connect().await;
    let rows = client
        .query("SELECT 42 AS answer, 'hello' AS greeting", &[])
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(rows[0].get::<i32, _>("answer"), Some(42));
    assert_eq!(
        rows[0].get::<String, _>("greeting"),
        Some("hello".to_string())
    );
}

#[tokio::test]
async fn get_by_index() {
    let mut client = connect().await;
    let rows = client
        .query("SELECT 42 AS answer, 'hello' AS greeting", &[])
        .await
        .unwrap()
        .into_first_result();
    assert_eq!(rows[0].get::<i32, _>(0usize), Some(42));
    assert_eq!(rows[0].get::<String, _>(1usize), Some("hello".to_string()));
}

#[tokio::test]
async fn column_metadata() {
    let mut client = connect().await;
    let rows = client
        .query("SELECT 42 AS answer, 'hello' AS greeting", &[])
        .await
        .unwrap()
        .into_first_result();
    let cols = rows[0].columns();
    assert_eq!(cols[0].name, "answer");
    assert_eq!(cols[1].name, "greeting");
}

// --- &str borrowing (tiberius compat) ---

#[tokio::test]
async fn str_borrow_from_row() {
    let mut client = connect().await;
    let rows = client
        .query("SELECT @P1 AS name", &[&"hello world"])
        .await
        .expect("query failed")
        .into_first_result();
    let name: &str = rows[0].get("name").expect("should borrow &str");
    assert_eq!(name, "hello world");
}

#[tokio::test]
async fn str_borrow_null() {
    let mut client = connect().await;
    let rows = client
        .simple_query("SELECT CAST(NULL AS nvarchar(50)) AS val")
        .await
        .expect("query failed")
        .into_first_result();
    let val: Option<&str> = rows[0].get("val").expect("option should work");
    assert_eq!(val, None);
}
