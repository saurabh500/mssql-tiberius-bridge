// Compatibility issue: https://github.com/saurabh500/mssql-tiberius-bridge/issues/125
use mssql_tiberius_bridge::{QueryStream, Result};

async fn all_results(stream: QueryStream<'_>) -> Result<()> {
    let _ = stream.into_results().await?;
    Ok(())
}

async fn first_result(stream: QueryStream<'_>) -> Result<()> {
    let _ = stream.into_first_result().await?;
    Ok(())
}

async fn one_row(stream: QueryStream<'_>) -> Result<()> {
    let _ = stream.into_row().await?;
    Ok(())
}

fn main() {}
