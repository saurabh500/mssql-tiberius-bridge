//! SQL Server client with tiberius-compatible query methods.
//!
//! The [`Client`] wraps `mssql-tds`'s `TdsClient` and provides the familiar
//! tiberius API: [`simple_query`](Client::simple_query),
//! [`query`](Client::query) with positional parameters, and
//! [`execute`](Client::execute) for DML.

use mssql_tds::connection::tds_client::{ResultSet, ResultSetClient, TdsClient};
use mssql_tds::connection_provider::tds_connection_provider::TdsConnectionProvider;
use mssql_tds::datatypes::column_values::ColumnValues;
use mssql_tds::query::metadata::ColumnMetadata;

use crate::config::Config;
use crate::error::{Error, Result};
use crate::query::{build_params, ExecuteResult, QueryResult, ToSql};

/// An async SQL Server client with tiberius-style query methods.
///
/// `Client` owns a single TCP connection to SQL Server. It is **not** `Clone`
/// or `Sync` — for concurrent access, use a connection pool via [`TdsManager`](crate::TdsManager).
///
/// # Example
///
/// ```rust,no_run
/// use mssql_tiberius_bridge::{Client, Config, AuthMethod};
///
/// # async fn example() -> mssql_tiberius_bridge::Result<()> {
/// let mut cfg = Config::new();
/// cfg.host("localhost").authentication(AuthMethod::sql_server("sa", "pass")).trust_cert();
///
/// let mut client = Client::connect(&cfg).await?;
/// let rows = client.simple_query("SELECT 1 AS n").await?.into_first_result();
/// assert_eq!(rows[0].get::<i32, _>("n"), Some(1));
/// # Ok(())
/// # }
/// ```
pub struct Client {
    inner: TdsClient,
}

impl Client {
    /// Connect to SQL Server using the given [`Config`].
    ///
    /// Establishes a TCP connection, performs TLS negotiation (if configured),
    /// and authenticates. The TCP transport is managed internally — unlike
    /// tiberius, you don't need to create a `TcpStream` yourself.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Tds`] if the connection fails (DNS resolution,
    /// TCP timeout, TLS handshake, authentication failure, etc.).
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

    /// Execute a raw SQL query without parameters.
    ///
    /// Mirrors tiberius' `simple_query`. The SQL is sent as a TDS SQL Batch
    /// (not parameterized). Use [`query`](Self::query) for parameterized queries.
    ///
    /// Returns a [`QueryResult`] that can be consumed with
    /// [`into_first_result()`](QueryResult::into_first_result) or
    /// [`into_results()`](QueryResult::into_results).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # async fn example(client: &mut mssql_tiberius_bridge::Client) -> mssql_tiberius_bridge::Result<()> {
    /// let rows = client
    ///     .simple_query("SELECT name FROM sys.databases")
    ///     .await?
    ///     .into_first_result();
    /// for row in &rows {
    ///     println!("{}", row.get::<&str, _>("name").unwrap());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::Tds`] on SQL errors or connection issues.
    pub async fn simple_query(&mut self, sql: impl Into<String>) -> Result<QueryResult> {
        let sql = sql.into();
        self.inner
            .execute(sql, None, None)
            .await
            .map_err(Error::Tds)?;

        self.collect_results().await
    }

    /// Execute a parameterized query with positional `@P1, @P2, ...` parameters.
    ///
    /// Mirrors tiberius' `query`. Parameters are bound via `sp_executesql`,
    /// which provides plan caching and SQL injection protection.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # async fn example(client: &mut mssql_tiberius_bridge::Client) -> mssql_tiberius_bridge::Result<()> {
    /// let rows = client
    ///     .query("SELECT @P1 AS a, @P2 AS b", &[&42i32, &"hello"])
    ///     .await?
    ///     .into_first_result();
    /// assert_eq!(rows[0].get::<i32, _>("a"), Some(42));
    /// assert_eq!(rows[0].get::<&str, _>("b"), Some("hello"));
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::Tds`] on SQL errors, parameter binding failures,
    /// or connection issues.
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

    /// Execute a DML statement and return row counts.
    ///
    /// Use for INSERT, UPDATE, DELETE, or any statement where you need
    /// the affected row count rather than result rows.
    ///
    /// # Known Limitation
    ///
    /// Currently returns 0 for DML statements because `mssql-tds` doesn't
    /// expose DONE token row counts through its public API.
    /// See [issue #1](https://github.com/saurabh500/mssql-tiberius-bridge/issues/1).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Tds`] on SQL errors or connection issues.
    pub async fn execute(
        &mut self,
        sql: impl Into<String>,
        params: &[&dyn ToSql],
    ) -> Result<ExecuteResult> {
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

        // Drain result sets, counting rows in each.
        let mut counts: Vec<u64> = Vec::new();
        while let Some(rs) = self.inner.get_current_resultset() {
            let mut count = 0u64;
            while let Some(_row) = rs.next_row().await.map_err(Error::Tds)? {
                count += 1;
            }
            counts.push(count);
            if !self.inner.move_to_next().await.map_err(Error::Tds)? {
                break;
            }
        }
        Ok(ExecuteResult { counts })
    }

    /// Access the underlying `mssql-tds` [`TdsClient`] for advanced operations.
    ///
    /// Use this escape hatch when you need functionality not yet exposed
    /// by the bridge API (e.g., bulk copy, stored procedure output parameters).
    pub fn inner_mut(&mut self) -> &mut TdsClient {
        &mut self.inner
    }

    /// Collect all result sets from the current execution into a [`QueryResult`].
    async fn collect_results(&mut self) -> Result<QueryResult> {
        let mut result_sets: Vec<(Vec<ColumnMetadata>, Vec<Vec<ColumnValues>>)> = Vec::new();

        while let Some(rs) = self.inner.get_current_resultset() {
            let metadata = rs.get_metadata().clone();
            let mut rows: Vec<Vec<ColumnValues>> = Vec::new();

            while let Some(row) = rs.next_row().await.map_err(Error::Tds)? {
                rows.push(row);
            }

            result_sets.push((metadata, rows));

            if !self.inner.move_to_next().await.map_err(Error::Tds)? {
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
