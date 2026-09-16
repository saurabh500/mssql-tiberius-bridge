use mssql_tiberius_bridge::bulk::{BulkCopyResult, BulkLoadRow};
use mssql_tiberius_bridge::{Client, Result};

async fn native_batch<R>(client: &mut Client, rows: Vec<R>) -> Result<BulkCopyResult>
where
    R: BulkLoadRow,
{
    client
        .bulk_insert("#items")
        .batch_size(100)
        .send(rows)
        .await
}

fn main() {}
