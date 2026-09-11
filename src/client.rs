//! SQL Server client with tiberius-compatible query methods.
//!
//! The [`Client`] wraps `mssql-tds`'s `TdsClient` and provides the familiar
//! tiberius API: [`simple_query`](Client::simple_query),
//! [`query`](Client::query) with positional parameters, and
//! [`execute`](Client::execute) for DML.

use mssql_tds::connection::tds_client::{ResultSet, StatementResult, TdsClient};
use mssql_tds::connection_provider::tds_connection_provider::TdsConnectionProvider;
use std::sync::Arc;

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
    prepared_session: Arc<()>,
}

// An interrupted reset must never leave a reusable, partially cleaned session.
struct SessionReset<'a> {
    inner: &'a mut TdsClient,
    complete: bool,
}

impl Drop for SessionReset<'_> {
    fn drop(&mut self) {
        if !self.complete {
            self.inner.mark_connection_dead();
        }
    }
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
            prepared_session: Arc::new(()),
        })
    }

    /// Return whether the driver has observed the connection to be dead.
    ///
    /// This performs no I/O. A `false` result does not prove that an idle
    /// connection is still responsive; use [`ping`](Self::ping) or
    /// [`reset_session`](Self::reset_session) to validate it.
    pub fn is_connection_dead(&self) -> bool {
        self.inner.is_connection_dead()
    }

    /// Reset and validate this session without closing its physical connection.
    ///
    /// Drains any outstanding query, then sends the native TDS `RESETCONNECTION`
    /// flag on a batch that restores `READ COMMITTED` transaction isolation.
    /// The reset rolls back open transactions and clears temporary tables and
    /// session settings; the driver verifies the server's acknowledgement.
    /// Isolation is set explicitly because SQL Server reset does not restore it.
    ///
    /// Prepared statements created before this call become invalid and must be
    /// prepared again. The carrying batch produces no rows and replaces a
    /// separate ping, so normal recycling needs only one round trip.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Tds`] if the connection is known dead, draining fails,
    /// or the reset/baseline batch fails. A failed or cancelled reset marks the
    /// connection dead so it cannot be recycled in an unknown session state.
    pub async fn reset_session(&mut self) -> Result<()> {
        if self.is_connection_dead() {
            return Err(mssql_tds::error::Error::ConnectionClosed(
                "cannot reset a connection that is known dead".into(),
            )
            .into());
        }

        self.prepared_session = Arc::new(());
        let mut reset = SessionReset {
            inner: &mut self.inner,
            complete: false,
        };
        reset.inner.close_query().await?;
        reset.inner.prepare_reset_connection(false);
        reset
            .inner
            .execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED".into(), ())
            .await?;
        reset.inner.close_query().await?;
        reset.complete = true;
        Ok(())
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
        self.inner.execute(sql, ()).await.map_err(Error::Tds)?;

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
            .execute_sp_executesql(sql, rpc_params, ())
            .await
            .map_err(Error::Tds)?;

        self.collect_results().await
    }

    /// Execute a DML statement and return row counts.
    ///
    /// Use for INSERT, UPDATE, DELETE, or any statement where you need
    /// the affected row count rather than result rows.
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

        let result = if params.is_empty() {
            self.inner.execute(sql, ()).await.map_err(Error::Tds)?
        } else {
            let rpc_params =
                build_params_with_string_encoding(params, self.send_string_parameters_as_unicode);
            self.inner
                .execute_sp_executesql(sql, rpc_params, ())
                .await
                .map_err(Error::Tds)?
        };
        self.collect_execute_results(result).await
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
                None => self.inner.execute(sql, ()).await.map_err(Error::Tds)?,
                Some(p) => self.inner.execute_sp_executesql(sql, p, ()).await.map_err(Error::Tds)?,
            };

            while self.inner.on_rows()
                || self.inner.advance_to_rows().await.map_err(Error::Tds)?
            {
                let schema = crate::row::RowSchema::from_metadata(self.inner.get_metadata());
                let mut writer = crate::row::BridgeRowWriter::new(schema);
                while self.inner.next_row_into(&mut writer).await.map_err(Error::Tds)? {
                    yield writer.take_row();
                }
                if !self.inner.advance_to_rows().await.map_err(Error::Tds)? {
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

    /// Begin a bulk insert into `table`.
    ///
    /// Returns a [`BulkInsert`](crate::bulk::BulkInsert) builder. Configure it
    /// with options ([`batch_size`](crate::bulk::BulkInsert::batch_size),
    /// [`table_lock`](crate::bulk::BulkInsert::table_lock),
    /// [`keep_identity`](crate::bulk::BulkInsert::keep_identity), …) then call
    /// [`send`](crate::bulk::BulkInsert::send) to stream the rows.
    ///
    /// See the [`bulk`](crate::bulk) module for an end-to-end example.
    pub fn bulk_insert<'a>(&'a mut self, table: impl Into<String>) -> crate::bulk::BulkInsert<'a> {
        crate::bulk::BulkInsert::new(&mut self.inner, table)
    }

    /// Begin a bulk insert into `table` with explicit destination column names.
    ///
    /// Equivalent to calling [`Client::bulk_insert`] and then
    /// [`map_column_by_ordinal`](crate::bulk::BulkInsert::map_column_by_ordinal)
    /// for each column in order, so source row column 0 lands in `columns[0]`,
    /// row column 1 in `columns[1]`, etc.
    pub fn bulk_insert_with_columns<'a>(
        &'a mut self,
        table: impl Into<String>,
        columns: &[&str],
    ) -> crate::bulk::BulkInsert<'a> {
        let mut bi = crate::bulk::BulkInsert::new(&mut self.inner, table);
        for (ordinal, dest) in columns.iter().enumerate() {
            bi = bi.map_column_by_ordinal(ordinal, *dest);
        }
        bi
    }

    /// Access the underlying `mssql-tds` [`TdsClient`] for advanced operations.
    ///
    /// Use this escape hatch when you need functionality not yet exposed
    /// by the bridge API (e.g., bulk copy, stored procedure output parameters).
    /// Use [`reset_session`](Self::reset_session) for resets so the bridge can
    /// also invalidate its prepared-statement handles.
    pub fn inner_mut(&mut self) -> &mut TdsClient {
        &mut self.inner
    }

    /// Prepare a parameterized statement server-side via `sp_prepare`.
    ///
    /// Returns a [`PreparedStatement`](crate::prepared::PreparedStatement)
    /// handle that can be executed many times via
    /// [`PreparedStatement::query`](crate::prepared::PreparedStatement::query)
    /// or [`PreparedStatement::execute`](crate::prepared::PreparedStatement::execute)
    /// without re-parsing or re-compiling the plan on the server.
    ///
    /// The `param_types` slice supplies **sample values** whose
    /// [`ToSql`] mapping is used to derive the parameter declaration string
    /// (e.g., `@P1 INT, @P2 NVARCHAR(MAX)`). Their *runtime* values are
    /// not bound — pass placeholders like `&0i32`, `&""`, etc.
    ///
    /// Closes [#56](https://github.com/saurabh500/mssql-tiberius-bridge/issues/56).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use mssql_tiberius_bridge::{Client, Config, AuthMethod};
    /// # async fn run() -> mssql_tiberius_bridge::Result<()> {
    /// # let mut cfg = Config::new();
    /// # cfg.authentication(AuthMethod::sql_server("sa", "p"));
    /// # let mut client = Client::connect(&cfg).await?;
    /// let stmt = client.prepare("SELECT @P1 * @P2 AS p", &[&0i32, &0i32]).await?;
    /// for (a, b) in [(2i32, 3i32), (5, 7)] {
    ///     let rows = stmt.query(&mut client, &[&a, &b]).await?.into_first_result();
    ///     let p: i32 = rows[0].get("p").unwrap();
    ///     println!("{a}*{b} = {p}");
    /// }
    /// stmt.close(&mut client).await?;
    /// # Ok(()) }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::Tds`] on SQL errors, connection issues, or if the
    /// server returns an unexpected response from `sp_prepare`.
    pub async fn prepare(
        &mut self,
        sql: impl Into<String>,
        param_types: &[&dyn ToSql],
    ) -> Result<crate::prepared::PreparedStatement> {
        let sql = sql.into();
        self.inner.close_query().await.map_err(Error::Tds)?;
        crate::prepared::PreparedStatement::prepare(
            &mut self.inner,
            sql,
            param_types,
            self.send_string_parameters_as_unicode,
            Arc::clone(&self.prepared_session),
        )
        .await
    }

    /// Execute a previously prepared statement and collect all result sets.
    ///
    /// The `params` slice must match (in count and type) the `param_types`
    /// supplied to [`Client::prepare`]. Usually called via
    /// [`PreparedStatement::query`](crate::prepared::PreparedStatement::query).
    pub async fn query_prepared(
        &mut self,
        stmt: &crate::prepared::PreparedStatement,
        params: &[&dyn ToSql],
    ) -> Result<QueryResult> {
        stmt.validate_session(&self.prepared_session)?;
        self.inner.close_query().await.map_err(Error::Tds)?;
        let rpc_params = if params.is_empty() {
            None
        } else {
            Some(build_params_with_string_encoding(
                params,
                self.send_string_parameters_as_unicode,
            ))
        };
        self.inner
            .execute_stored_procedure(
                "sp_execute".into(),
                Some(stmt.handle_parameter()),
                rpc_params,
                (),
            )
            .await
            .map_err(Error::Tds)?;
        self.collect_results().await
    }

    /// Execute a previously prepared DML statement and return row counts.
    ///
    /// Uses the same affected-row counting as [`Client::execute`].
    /// Usually called via
    /// [`PreparedStatement::execute`](crate::prepared::PreparedStatement::execute).
    pub async fn execute_prepared(
        &mut self,
        stmt: &crate::prepared::PreparedStatement,
        params: &[&dyn ToSql],
    ) -> Result<ExecuteResult> {
        stmt.validate_session(&self.prepared_session)?;
        self.inner.close_query().await.map_err(Error::Tds)?;
        let rpc_params = if params.is_empty() {
            None
        } else {
            Some(build_params_with_string_encoding(
                params,
                self.send_string_parameters_as_unicode,
            ))
        };
        let result = self
            .inner
            .execute_stored_procedure(
                "sp_execute".into(),
                Some(stmt.handle_parameter()),
                rpc_params,
                (),
            )
            .await
            .map_err(Error::Tds)?;
        self.collect_execute_results(result).await
    }

    /// Release a prepared-statement handle via `sp_unprepare`.
    ///
    /// Consumes the [`PreparedStatement`](crate::prepared::PreparedStatement).
    /// Usually called via
    /// [`PreparedStatement::close`](crate::prepared::PreparedStatement::close).
    ///
    /// Dropping the [`Client`] also releases all prepared handles for the
    /// connection, so calling this is optional unless you want to free
    /// server-side memory while keeping the connection alive.
    pub async fn unprepare(&mut self, stmt: crate::prepared::PreparedStatement) -> Result<()> {
        stmt.validate_session(&self.prepared_session)?;
        self.inner.close_query().await.map_err(Error::Tds)?;
        self.inner
            .execute_stored_procedure(
                "sp_unprepare".into(),
                Some(stmt.handle_parameter()),
                None,
                (),
            )
            .await
            .map_err(Error::Tds)?;
        self.inner.close_query().await.map_err(Error::Tds)
    }

    /// Collect all result sets from the current execution into a [`QueryResult`].
    async fn collect_results(&mut self) -> Result<QueryResult> {
        let mut result_sets: Vec<Vec<crate::row::Row>> = Vec::new();

        while self.inner.on_rows() || self.inner.advance_to_rows().await.map_err(Error::Tds)? {
            let schema = crate::row::RowSchema::from_metadata(self.inner.get_metadata());
            let mut writer = crate::row::BridgeRowWriter::new(schema);
            let mut rows: Vec<crate::row::Row> = Vec::new();

            while self
                .inner
                .next_row_into(&mut writer)
                .await
                .map_err(Error::Tds)?
            {
                rows.push(writer.take_row());
            }

            result_sets.push(rows);

            if !self.inner.advance_to_rows().await.map_err(Error::Tds)? {
                break;
            }
        }

        Ok(QueryResult { result_sets })
    }

    async fn collect_execute_results(
        &mut self,
        mut result: StatementResult,
    ) -> Result<ExecuteResult> {
        let mut counts = Vec::new();
        loop {
            match result {
                StatementResult::Rows => {
                    let mut count = 0;
                    while self.inner.next_row().await.map_err(Error::Tds)?.is_some() {
                        count += 1;
                    }
                    counts.push(count);
                }
                StatementResult::NoRows { rows_affected } => {
                    if let Some(count) = rows_affected {
                        counts.push(count);
                    }
                }
                StatementResult::End => break,
            }
            result = self.inner.advance().await.map_err(Error::Tds)?;
        }
        Ok(ExecuteResult { counts })
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
