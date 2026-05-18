//! Benchmark: streaming row throughput comparing next_row vs next_row_into paths.

use criterion::{criterion_group, criterion_main, Criterion};
use futures_util::StreamExt;
use mssql_tiberius_bridge::{AuthMethod, Client, Config};
use std::time::Instant;
use tokio::runtime::Runtime;

fn test_config() -> Config {
    let mut cfg = Config::new();
    cfg.host("localhost")
        .port(11433)
        .database("master")
        .authentication(AuthMethod::sql_server("sa", "YourStrong@Passw0rd"))
        .trust_cert();
    cfg
}

const DROP_SQL: &str = "IF OBJECT_ID('dbo.bench_rows', 'U') IS NOT NULL DROP TABLE dbo.bench_rows;";

const CREATE_SQL: &str = "CREATE TABLE dbo.bench_rows (
    id INT,
    name NVARCHAR(100),
    value FLOAT,
    created_at DATETIME2
);";

const INSERT_SQL: &str = "
INSERT INTO dbo.bench_rows (id, name, value, created_at)
SELECT TOP 10000
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)),
    CONCAT(N'row_name_', ROW_NUMBER() OVER (ORDER BY (SELECT NULL))),
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) * 1.5,
    GETDATE()
FROM sys.all_objects a CROSS JOIN sys.all_objects b;
";

fn bench_streaming_rows(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    rt.block_on(async {
        let cfg = test_config();
        let mut client = Client::connect(&cfg).await.expect("connect failed");
        client
            .simple_query(DROP_SQL)
            .await
            .expect("drop failed")
            .into_first_result();
        client
            .simple_query(CREATE_SQL)
            .await
            .expect("create failed")
            .into_first_result();
        client
            .simple_query(INSERT_SQL)
            .await
            .expect("insert failed")
            .into_first_result();
    });

    c.bench_function("stream_10k_rows", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let cfg = test_config();
                let mut client = Client::connect(&cfg).await.unwrap();
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let start = Instant::now();
                    let mut stream = client.simple_query_streamed(
                        "SELECT id, name, value, created_at FROM dbo.bench_rows",
                    );
                    let mut count = 0u64;
                    while let Some(row) = stream.next().await {
                        let _ = row.unwrap();
                        count += 1;
                    }
                    total += start.elapsed();
                    assert_eq!(count, 10000);
                }
                total
            })
        });
    });
}

fn bench_buffered_rows(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    rt.block_on(async {
        let cfg = test_config();
        let mut client = Client::connect(&cfg).await.expect("connect failed");
        client
            .simple_query(DROP_SQL)
            .await
            .expect("drop failed")
            .into_first_result();
        client
            .simple_query(CREATE_SQL)
            .await
            .expect("create failed")
            .into_first_result();
        client
            .simple_query(INSERT_SQL)
            .await
            .expect("insert failed")
            .into_first_result();
    });

    c.bench_function("buffered_10k_rows", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let cfg = test_config();
                let mut client = Client::connect(&cfg).await.unwrap();
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let start = Instant::now();
                    let result = client
                        .simple_query("SELECT id, name, value, created_at FROM dbo.bench_rows")
                        .await
                        .unwrap();
                    let rows = result.into_first_result();
                    total += start.elapsed();
                    assert_eq!(rows.len(), 10000);
                }
                total
            })
        });
    });
}

criterion_group!(benches, bench_streaming_rows, bench_buffered_rows);
criterion_main!(benches);
