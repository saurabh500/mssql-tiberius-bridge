//! Compile-only Tiberius 0.7 query patterns with the bridge as dependency alias.
use futures_util::TryStreamExt;
use tiberius::{Client, QueryItem, QueryStream, Row};

pub async fn query<'a>(client: &'a mut Client) -> tiberius::Result<QueryStream<'a>> {
    let sql = String::from("SELECT @P1 AS n");
    let number = 42;
    client.query(sql.as_str(), &[&number]).await
}

pub async fn consume(client: &mut Client) -> tiberius::Result<()> {
    let mut stream = query(client).await?;
    let columns = stream.columns().await?.expect("SELECT metadata");
    assert_eq!(columns[0].name(), "n");
    while let Some(item) = stream.try_next().await? {
        match item {
            QueryItem::Metadata(meta) => {
                assert_eq!(meta.result_index(), 0);
            }
            QueryItem::Row(row) => {
                assert_eq!(row.result_index(), 0);
                let _: Option<i32> = row.get("n");
            }
        }
    }
    drop(stream);
    let _: Option<Row> = client.query("SELECT 1", &[]).await?.into_row().await?;
    let _: Vec<Row> = client
        .simple_query("SELECT 1")
        .await?
        .into_first_result()
        .await?;
    let _: Vec<Vec<Row>> = client
        .simple_query("SELECT 1; SELECT 2")
        .await?
        .into_results()
        .await?;
    let mut rows = client.query("SELECT 1", &[]).await?.into_row_stream();
    while let Some(row) = rows.try_next().await? {
        let _: Option<i32> = row.get(0);
    }
    Ok(())
}
