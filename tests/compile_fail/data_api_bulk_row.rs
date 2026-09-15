// Compatibility issue: https://github.com/saurabh500/mssql-tiberius-bridge/issues/131
use mssql_tiberius_bridge::{IntoRow, IntoSql, TokenRow};

fn main() {
    let mut row = TokenRow::with_capacity(2);
    row.push(1i32.into_sql());
    let _ = row.get(0);
    let _ = row.iter().count();
    let _ = row.clone().into_iter().count();
    row.clear();
    let _ = (1i32, 2i32).into_row();
}
