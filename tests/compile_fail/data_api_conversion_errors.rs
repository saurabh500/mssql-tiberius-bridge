// Compatibility issue: https://github.com/saurabh500/mssql-tiberius-bridge/issues/128
use mssql_tiberius_bridge::{ColumnValues, FromSql, Result};

fn convert(value: &ColumnValues) -> Result<Option<i32>> {
    i32::from_sql(value)
}

fn main() {}
