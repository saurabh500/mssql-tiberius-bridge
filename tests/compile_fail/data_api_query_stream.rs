// Compatibility issue: https://github.com/saurabh500/mssql-tiberius-bridge/issues/125
use mssql_tiberius_bridge::{QueryItem, QueryStream};

fn inspect(_: QueryStream<'_>, item: QueryItem) {
    let _ = item.as_metadata();
    let _ = item.as_row();
}

fn main() {}
