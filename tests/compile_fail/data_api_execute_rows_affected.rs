// Compatibility issue: https://github.com/saurabh500/mssql-tiberius-bridge/issues/131
use mssql_tiberius_bridge::ExecuteResult;

fn counts(result: &ExecuteResult) -> &[u64] {
    result.rows_affected()
}

fn main() {}
