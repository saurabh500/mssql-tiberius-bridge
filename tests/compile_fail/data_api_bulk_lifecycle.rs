// Compatibility issue: https://github.com/saurabh500/mssql-tiberius-bridge/issues/131
use mssql_tiberius_bridge::{Client, IntoRow, Result};

async fn send(client: &mut Client) -> Result<()> {
    let mut bulk = client.bulk_insert("#items").await?;
    bulk.send((1i32, Option::<&str>::None).into_row()).await?;
    let _ = bulk.finalize().await?;
    Ok(())
}

fn main() {}
