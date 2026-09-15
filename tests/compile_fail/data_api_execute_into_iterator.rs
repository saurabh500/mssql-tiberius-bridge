// Compatibility issue: https://github.com/saurabh500/mssql-tiberius-bridge/issues/130
use mssql_tiberius_bridge::ExecuteResult;

fn consume<I: IntoIterator<Item = u64>>(_: I) {}

fn probe(result: ExecuteResult) {
    consume(result);
}

fn main() {}
