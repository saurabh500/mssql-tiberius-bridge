//! Client wrapper around mssql-tds TdsClient with tiberius-style API.

use mssql_tds::connection::tds_client::{ResultSet, ResultSetClient, TdsClient};
use mssql_tds::connection_provider::tds_connection_provider::TdsConnectionProvider;
use mssql_tds::datatypes::column_values::ColumnValues;
use mssql_tds::query::metadata::ColumnMetadata;

use crate::config::Config;
use crate::error::{Error, Result};
use crate::query::{build_params, QueryResult, ToSql};

/// Ergonomic wrapper around `TdsClient` with tiberius-style query methods.
pub struct Client {
    inner: TdsClient,
}

impl Client {
    /// Connect to SQL Server using the given config.
    pub async fn connect(config: &Config) -> Result<Self> {
        let ctx = config.to_client_context();
        let datasource = config.datasource_string();
        let provider = TdsConnectionProvider {};
        let client = provider
            .create_client(ctx, &datasource, None)
            .await
            .map_err(Error::Tds)?;
        Ok(Client { inner: client })
    }

    /// Execute a raw SQL query without parameters and collect all results.
    /// Mirrors tiberius' `simple_query`.
    pub async fn simple_query(&mut self, sql: impl Into<String>) -> Result<QueryResult> {
        let sql = sql.into();
        self.inner
            .execute(sql, None, None)
            .await
            .map_err(Error::Tds)?;

        self.collect_results().await
    }

    /// Execute a parameterized query using positional @P1, @P2, ... params.
    /// Mirrors tiberius' `query`.
    pub async fn query(
        &mut self,
        sql: impl Into<String>,
        params: &[&dyn ToSql],
    ) -> Result<QueryResult> {
        let sql = sql.into();

        if params.is_empty() {
            return self.simple_query(sql).await;
        }

        let rpc_params = build_params(params);
        self.inner
            .execute_sp_executesql(sql, rpc_params, None, None)
            .await
            .map_err(Error::Tds)?;

        self.collect_results().await
    }

    /// Execute a statement and return the number of affected rows.
    pub async fn execute(
        &mut self,
        sql: impl Into<String>,
        params: &[&dyn ToSql],
    ) -> Result<u64> {
        let sql = sql.into();

        if params.is_empty() {
            self.inner
                .execute(sql, None, None)
                .await
                .map_err(Error::Tds)?;
        } else {
            let rpc_params = build_params(params);
            self.inner
                .execute_sp_executesql(sql, rpc_params, None, None)
                .await
                .map_err(Error::Tds)?;
        }

        // Drain any result sets to get to the done token with row count.
        // mssql-tds doesn't expose affected rows directly in a simple way,
        // so we drain and count.
        let mut total = 0u64;
        loop {
            if let Some(rs) = self.inner.get_current_resultset() {
                while let Some(_row) = rs.next_row().await.map_err(Error::Tds)? {
                    total += 1;
                }
                if !self.inner.move_to_next().await.map_err(Error::Tds)? {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(total)
    }

    /// Access the underlying TdsClient for advanced operations.
    pub fn inner_mut(&mut self) -> &mut TdsClient {
        &mut self.inner
    }

    /// Collect all result sets from the current execution into a QueryResult.
    async fn collect_results(&mut self) -> Result<QueryResult> {
        let mut result_sets: Vec<(Vec<ColumnMetadata>, Vec<Vec<ColumnValues>>)> = Vec::new();

        loop {
            if let Some(rs) = self.inner.get_current_resultset() {
                let metadata = rs.get_metadata().clone();
                let mut rows: Vec<Vec<ColumnValues>> = Vec::new();

                while let Some(row) = rs.next_row().await.map_err(Error::Tds)? {
                    rows.push(row);
                }

                result_sets.push((metadata, rows));

                if !self.inner.move_to_next().await.map_err(Error::Tds)? {
                    break;
                }
            } else {
                break;
            }
        }

        Ok(QueryResult { result_sets })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_to_datasource() {
        let mut cfg = Config::new();
        cfg.host("myserver").port(1433).database("testdb");
        assert_eq!(cfg.datasource_string(), "tcp:myserver,1433");
    }
}
