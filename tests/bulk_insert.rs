//! Integration test for `Client::bulk_insert` (issue #53).
//!
//! Mirrors `mssql-tds`'s bulk_copy integration coverage but exercises the
//! bridge wrapper. Skipped unless `TEST_DB_PASSWORD` is set (consistent with
//! the other DB-required tests in this directory).

use async_trait::async_trait;
use mssql_tds::core::TdsResult;
use mssql_tds::datatypes::column_values::ColumnValues;
use mssql_tds::datatypes::sql_string::SqlString;
use mssql_tds::message::bulk_load::StreamingBulkLoadWriter;
use mssql_tiberius_bridge::bulk::BulkLoadRow;
use mssql_tiberius_bridge::{AuthMethod, Client, Config};

fn test_config() -> Config {
    let password = match std::env::var("TEST_DB_PASSWORD") {
        Ok(p) => p,
        Err(_) => {
            eprintln!("TEST_DB_PASSWORD not set, skipping bulk_insert integration test");
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

#[derive(Clone)]
struct Person {
    id: i32,
    name: String,
}

#[async_trait]
impl BulkLoadRow for Person {
    async fn write_to_packet(
        &self,
        writer: &mut StreamingBulkLoadWriter<'_>,
        column_index: &mut usize,
    ) -> TdsResult<()> {
        writer
            .write_column_value(*column_index, &ColumnValues::Int(self.id))
            .await?;
        *column_index += 1;
        writer
            .write_column_value(
                *column_index,
                &ColumnValues::String(SqlString::from_utf8_string(self.name.clone())),
            )
            .await?;
        *column_index += 1;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_insert_thousand_rows() {
    let cfg = test_config();
    let mut client = Client::connect(&cfg).await.expect("connect failed");

    client
        .execute(
            "CREATE TABLE #BridgeBulkPeople (id INT NOT NULL, name NVARCHAR(100) NOT NULL)",
            &[],
        )
        .await
        .expect("create temp table failed");

    let rows: Vec<Person> = (0..1000)
        .map(|i| Person {
            id: i,
            name: format!("user-{i}"),
        })
        .collect();

    let result = client
        .bulk_insert("#BridgeBulkPeople")
        .batch_size(500)
        .table_lock(true)
        .send(rows)
        .await
        .expect("bulk insert failed");

    assert_eq!(result.rows_affected, 1000);

    let count_rows = client
        .simple_query("SELECT COUNT(*) AS n FROM #BridgeBulkPeople")
        .await
        .expect("select count failed")
        .into_first_result();
    let n: i32 = count_rows[0].get("n").expect("missing count");
    assert_eq!(n, 1000);
}
