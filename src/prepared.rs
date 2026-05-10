//! Server-side prepared statements via `sp_prepare` / `sp_execute` /
//! `sp_unprepare`.
//!
//! Closes [#56](https://github.com/saurabh500/mssql-tiberius-bridge/issues/56)
//! (mirrors upstream tiberius [#30](https://github.com/prisma/tiberius/issues/30)).
//!
//! # Why
//!
//! Sending a parameterized query through [`Client::query`](crate::Client::query)
//! uses `sp_executesql`: SQL Server parses + plan-compiles the statement on
//! every call. For a hot loop that executes the same statement with different
//! values, paying that cost N times is wasteful.
//!
//! [`Client::prepare`](crate::Client::prepare) compiles the statement *once*
//! server-side via `sp_prepare`, returning a [`PreparedStatement`] handle.
//! Subsequent invocations through [`PreparedStatement::query`] /
//! [`PreparedStatement::execute`] use `sp_execute` with that handle and skip
//! parse + plan-compile.
//!
//! # Example
//!
//! ```rust,no_run
//! # use mssql_tiberius_bridge::{Client, Config, AuthMethod};
//! # async fn run() -> mssql_tiberius_bridge::Result<()> {
//! # let mut cfg = Config::new();
//! # cfg.authentication(AuthMethod::sql_server("sa", "pwd"));
//! # let mut client = Client::connect(&cfg).await?;
//! // The parameter types are derived from these "sample" values.
//! let stmt = client
//!     .prepare("SELECT @P1 + @P2 AS sum", &[&0i32, &0i32])
//!     .await?;
//!
//! for (a, b) in [(1i32, 2i32), (10, 20), (100, 200)] {
//!     let rows = stmt.query(&mut client, &[&a, &b]).await?.into_first_result();
//!     let sum: i32 = rows[0].get("sum").unwrap();
//!     println!("{a} + {b} = {sum}");
//! }
//!
//! // Free server-side resources.
//! stmt.close(&mut client).await?;
//! # Ok(()) }
//! ```
//!
//! # Resource lifecycle
//!
//! [`PreparedStatement`] holds an `i32` server-side handle. To release it
//! either:
//!
//! - Call [`PreparedStatement::close`] (or [`Client::unprepare`]) explicitly,
//!   **or**
//! - Drop the [`Client`](crate::Client) — closing the TDS connection frees
//!   all of its prepared handles.
//!
//! Dropping the [`PreparedStatement`] alone does **not** release the handle
//! (Rust [`Drop`] cannot run async code). The type is marked
//! `#[must_use]` to nudge callers toward explicit cleanup.
//!
//! # Type inference for parameters
//!
//! [`Client::prepare`] takes the *same* `&[&dyn ToSql]` slice as
//! [`Client::query`]. Only the **types** of those values matter — their
//! actual contents are used to build the parameter declaration string sent
//! to `sp_prepare`. Passing `0i32` to declare an `INT` parameter is the
//! common idiom.
//!
//! At execution time, the values passed to
//! [`PreparedStatement::query`] / [`PreparedStatement::execute`] **must**
//! match the prepared types — SQL Server will reject mismatched types
//! with an RPC error.

use crate::error::Result;
use crate::query::{ExecuteResult, QueryResult, ToSql};
use crate::Client;

/// A server-side prepared statement.
///
/// Created via [`Client::prepare`]. Holds an `i32` handle returned by
/// `sp_prepare` plus the original SQL (for debugging). See the
/// [module docs](self) for lifecycle details.
#[derive(Debug)]
#[must_use = "PreparedStatement holds a server-side handle; call `close()` to release it (or drop the Client)"]
pub struct PreparedStatement {
    handle: i32,
    sql: String,
}

impl PreparedStatement {
    pub(crate) fn new(handle: i32, sql: String) -> Self {
        Self { handle, sql }
    }

    /// The server-side `sp_prepare` handle.
    pub fn handle(&self) -> i32 {
        self.handle
    }

    /// The original SQL text that was prepared.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Execute the prepared statement and collect all result sets.
    ///
    /// Delegates to [`Client::query_prepared`].
    pub async fn query(&self, client: &mut Client, params: &[&dyn ToSql]) -> Result<QueryResult> {
        client.query_prepared(self, params).await
    }

    /// Execute the prepared statement and return affected-row counts only.
    ///
    /// Delegates to [`Client::execute_prepared`].
    pub async fn execute(
        &self,
        client: &mut Client,
        params: &[&dyn ToSql],
    ) -> Result<ExecuteResult> {
        client.execute_prepared(self, params).await
    }

    /// Release the server-side handle via `sp_unprepare`.
    ///
    /// Consumes `self`. Delegates to [`Client::unprepare`].
    pub async fn close(self, client: &mut Client) -> Result<()> {
        client.unprepare(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepared_statement_handle_and_sql_accessors() {
        let stmt = PreparedStatement::new(42, "SELECT @P1".to_string());
        assert_eq!(stmt.handle(), 42);
        assert_eq!(stmt.sql(), "SELECT @P1");
    }

    #[test]
    fn prepared_statement_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<PreparedStatement>();
    }

    #[test]
    fn prepared_statement_debug_includes_handle() {
        let stmt = PreparedStatement::new(7, "SELECT 1".to_string());
        let s = format!("{stmt:?}");
        assert!(s.contains("7"), "debug should include handle: {s}");
    }
}
