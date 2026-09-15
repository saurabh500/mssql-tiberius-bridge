// Compatibility issue: https://github.com/saurabh500/mssql-tiberius-bridge/issues/127
use mssql_tiberius_bridge::{query::Query, Client, Result};

async fn execute(client: &mut Client) -> Result<()> {
    let mut query = Query::new("SELECT @P1");
    query.bind(Option::<i32>::None);
    let _ = query.execute(client).await?;

    let mut query = Query::new("SELECT @P1");
    query.bind(1i32);
    let _ = query.query(client).await?;
    Ok(())
}

fn main() {}
