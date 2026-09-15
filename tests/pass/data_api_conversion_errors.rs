// Compatibility issue: https://github.com/saurabh500/mssql-tiberius-bridge/issues/128
use mssql_tiberius_bridge::{compat::FromSql, ColumnData, Result};

struct Custom(i32);

impl<'a> FromSql<'a> for Custom {
    fn from_sql(value: &'a ColumnData<'static>) -> Result<Option<Self>> {
        i32::from_sql(value).map(|value| value.map(Custom))
    }
}

fn convert(value: &ColumnData<'static>) -> Result<Option<i32>> {
    i32::from_sql(value)
}

fn main() {}
