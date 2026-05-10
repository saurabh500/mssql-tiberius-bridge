//! Integration test for `BulkInsert::send_arrow` (issue #85).
//!
//! Skipped unless `TEST_DB_PASSWORD` is set (consistent with other DB tests).

#![cfg(feature = "arrow")]

use std::sync::Arc;

use arrow_array::{
    BooleanArray, Decimal128Array, Float64Array, Int32Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use mssql_tiberius_bridge::{AuthMethod, Client, Config};

fn test_config() -> Config {
    let password = match std::env::var("TEST_DB_PASSWORD") {
        Ok(p) => p,
        Err(_) => {
            eprintln!("TEST_DB_PASSWORD not set, skipping arrow bulk_insert test");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_insert_send_arrow_mixed_types() {
    let cfg = test_config();
    let mut client = Client::connect(&cfg).await.expect("connect failed");

    client
        .execute(
            "CREATE TABLE #BridgeBulkArrow (
                id INT NOT NULL,
                name NVARCHAR(100) NOT NULL,
                price DECIMAL(10, 2) NOT NULL,
                rating FLOAT NOT NULL,
                active BIT NOT NULL
            )",
            &[],
        )
        .await
        .expect("create table failed");

    let n = 1000_i32;
    let ids: Int32Array = (0..n).collect();
    let names = StringArray::from((0..n).map(|i| format!("user-{i}")).collect::<Vec<_>>());
    let prices = Decimal128Array::from((0..n).map(|i| (i as i128) * 100 + 99).collect::<Vec<_>>())
        .with_precision_and_scale(10, 2)
        .unwrap();
    let ratings = Float64Array::from((0..n).map(|i| i as f64 / 10.0).collect::<Vec<_>>());
    let actives = BooleanArray::from((0..n).map(|i| i % 2 == 0).collect::<Vec<_>>());

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("price", DataType::Decimal128(10, 2), false),
        Field::new("rating", DataType::Float64, false),
        Field::new("active", DataType::Boolean, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ids),
            Arc::new(names),
            Arc::new(prices),
            Arc::new(ratings),
            Arc::new(actives),
        ],
    )
    .expect("RecordBatch::try_new failed");

    let result = client
        .bulk_insert("#BridgeBulkArrow")
        .batch_size(500)
        .table_lock(true)
        .send_arrow(batch)
        .await
        .expect("send_arrow failed");

    assert_eq!(result.rows_affected, n as u64);

    let count_rows = client
        .simple_query("SELECT COUNT(*) AS n FROM #BridgeBulkArrow")
        .await
        .expect("select count failed")
        .into_first_result();
    let count: i32 = count_rows[0].get("n").expect("missing count");
    assert_eq!(count, n);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_insert_send_arrow_batches() {
    let cfg = test_config();
    let mut client = Client::connect(&cfg).await.expect("connect failed");

    client
        .execute(
            "CREATE TABLE #BridgeBulkArrowBatches (id INT NOT NULL, name NVARCHAR(100) NOT NULL)",
            &[],
        )
        .await
        .expect("create table failed");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batches: Vec<RecordBatch> = (0..3)
        .map(|b| {
            let ids: Int32Array = (b * 100..(b + 1) * 100).collect();
            let names = StringArray::from(
                (b * 100..(b + 1) * 100)
                    .map(|i| format!("u{i}"))
                    .collect::<Vec<_>>(),
            );
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(names)]).unwrap()
        })
        .collect();

    let result = client
        .bulk_insert("#BridgeBulkArrowBatches")
        .send_arrow_batches(batches)
        .await
        .expect("send_arrow_batches failed");

    assert_eq!(result.rows_affected, 300);
}
