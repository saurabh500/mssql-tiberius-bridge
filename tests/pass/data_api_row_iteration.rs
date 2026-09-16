// Compatibility issue: https://github.com/saurabh500/mssql-tiberius-bridge/issues/130
use mssql_tiberius_bridge::{Column, ColumnData, Row};

fn inspect(row: Row) {
    let _ = row.cells().count();
    let _ = row.into_iter().count();
}

fn cells(row: &Row) -> impl Iterator<Item = (&Column, &ColumnData<'static>)> {
    row.cells()
}

fn main() {}
