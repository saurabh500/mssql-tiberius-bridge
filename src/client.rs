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
use crate::query::{build_params_with_string_encoding, ExecuteResult, QueryResult, ToSql};

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
    send_string_parameters_as_unicode: bool,
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
        Ok(Client {
            inner: client,
            send_string_parameters_as_unicode: config.string_parameters_as_unicode(),
        })
    }

    /// Check whether the connection is alive and responsive.
    ///
    /// This sends a tiny `SELECT 1` batch and drains the result so the client is
    /// ready for the next request. Connection pools can use this as a cheap
    /// validation step before handing out an existing connection.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Tds`] if the batch fails or the connection cannot be used.
    pub async fn ping(&mut self) -> Result<()> {
        let _ = self.simple_query("SELECT 1").await?.into_first_result();
        Ok(())
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
        self.inner.close_query().await.map_err(Error::Tds)?;
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

        self.inner.close_query().await.map_err(Error::Tds)?;
        let rpc_params =
            build_params_with_string_encoding(params, self.send_string_parameters_as_unicode);
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
        self.inner.close_query().await.map_err(Error::Tds)?;

        if params.is_empty() {
            self.inner
                .execute(sql, None, None)
                .await
                .map_err(Error::Tds)?;
        } else {
            let rpc_params =
                build_params_with_string_encoding(params, self.send_string_parameters_as_unicode);
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

    /// Execute a parameterized query and return rows as a true wire-level
    /// stream — each `.next().await` pulls the next row from the network
    /// without buffering the rest of the result set.
    ///
    /// Mirrors tiberius' `Client::query(...).await?.into_row_stream()`
    /// behavior; rows from multiple result sets are flattened in order.
    ///
    /// Use this for memory-bounded processing of large result sets
    /// (e.g., the windmill MSSQL → S3 export path). For small result
    /// sets, [`query`](Self::query) is more ergonomic.
    ///
    /// # Lifetime
    ///
    /// The returned stream borrows `&mut self` for its lifetime. You must
    /// fully consume the stream (or drop it) before issuing another query
    /// on the same `Client`.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use mssql_tiberius_bridge::Client;
    /// use futures_util::StreamExt;
    ///
    /// # async fn ex(client: &mut Client) -> mssql_tiberius_bridge::Result<()> {
    /// let mut s = client.query_streamed("SELECT @P1 AS n", &[&1i32]);
    /// while let Some(row) = s.next().await {
    ///     println!("{:?}", row?.get::<i32, _>("n"));
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_streamed<'a>(
        &'a mut self,
        sql: impl Into<String>,
        params: &[&dyn ToSql],
    ) -> std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<crate::row::Row>> + Send + 'a>>
    {
        let sql = sql.into();
        let rpc_params = if params.is_empty() {
            None
        } else {
            Some(build_params_with_string_encoding(
                params,
                self.send_string_parameters_as_unicode,
            ))
        };
        Box::pin(async_stream::try_stream! {
            // Drain any leftover state from a prior query / dropped stream
            // so we don't hit "open batch" errors when re-using the Client.
            self.inner.close_query().await.map_err(Error::Tds)?;

            // Initiate the query inside the stream so the &mut self borrow
            // lives for the entire row-pull duration.
            match rpc_params {
                None => self.inner.execute(sql, None, None).await.map_err(Error::Tds)?,
                Some(p) => self.inner.execute_sp_executesql(sql, p, None, None).await.map_err(Error::Tds)?,
            }

            while let Some(schema) = self
                .inner
                .get_current_resultset()
                .map(|rs| crate::row::RowSchema::from_metadata(rs.get_metadata()))
            {
                loop {
                    let next = match self.inner.get_current_resultset() {
                        Some(rs) => rs.next_row().await.map_err(Error::Tds)?,
                        None => None,
                    };
                    match next {
                        Some(values) => {
                            yield crate::row::Row::from_schema(schema.clone(), values)
                        }
                        None => break,
                    }
                }
                if !self.inner.move_to_next().await.map_err(Error::Tds)? {
                    break;
                }
            }
        })
    }

    /// Streaming counterpart of [`simple_query`](Self::simple_query) — see
    /// [`query_streamed`](Self::query_streamed) for semantics and the
    /// borrow contract.
    pub fn simple_query_streamed<'a>(
        &'a mut self,
        sql: impl Into<String>,
    ) -> std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<crate::row::Row>> + Send + 'a>>
    {
        self.query_streamed(sql, &[])
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

    #[test]
    fn client_is_send_for_pooling() {
        fn assert_send<T: Send>() {}
        assert_send::<Client>();
    }
}
