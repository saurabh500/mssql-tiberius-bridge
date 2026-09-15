// Compatibility issue: https://github.com/saurabh500/mssql-tiberius-bridge/issues/129
use mssql_tiberius_bridge::Query;

fn main() {
    let mut query = Query::new("SELECT @P1");
    query.bind(Option::<i32>::None);
}
