// Compatibility issue: https://github.com/saurabh500/mssql-tiberius-bridge/issues/128
use mssql_tiberius_bridge::{compat::FromSql, ColumnData, FromSqlOwned, IntoSql};

fn conversions<T, O>(value: T, borrowed: &ColumnData<'_>)
where
    T: IntoSql<'static>,
    O: FromSqlOwned,
{
    let data = <T as IntoSql>::into_sql(value);
    let _ = <O as FromSqlOwned>::from_sql_owned(data);
    let _ = <&str as FromSql>::from_sql(borrowed);
}

fn main() {}
