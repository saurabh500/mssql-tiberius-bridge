// Compatibility issue: https://github.com/saurabh500/mssql-tiberius-bridge/issues/127
use mssql_tiberius_bridge::{ColumnData, FromSqlOwned, IntoSql};

fn main() {
    let data = String::from("owned").into_sql();
    let _ = String::from_sql_owned(data);
    let _: Option<ColumnData<'static>> = None;
}
