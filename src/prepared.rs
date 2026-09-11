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

use mssql_tds::connection::tds_client::TdsClient;
use mssql_tds::datatypes::column_values::{ColumnValues, DEFAULT_VARTIME_SCALE};
use mssql_tds::datatypes::sql_string::SqlString;
use mssql_tds::datatypes::sqldatatypes::{TdsDataType, VectorBaseType};
use mssql_tds::datatypes::sqltypes::SqlType;
use mssql_tds::message::parameters::rpc_parameters::{RpcParameter, StatusFlags};

use crate::error::{Error, Result};
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

    pub(crate) async fn prepare(
        client: &mut TdsClient,
        sql: String,
        param_types: &[&dyn ToSql],
        unicode: bool,
    ) -> Result<Self> {
        let declaration = Self::parameter_declaration(param_types, unicode)?;
        let parameters = vec![
            RpcParameter::new(None, StatusFlags::BY_REF_VALUE, SqlType::Int(None)),
            RpcParameter::new(
                None,
                StatusFlags::NONE,
                SqlType::NVarcharMax(Some(SqlString::from_utf8_string(declaration))),
            ),
            RpcParameter::new(
                None,
                StatusFlags::NONE,
                SqlType::NVarcharMax(Some(SqlString::from_utf8_string(sql.clone()))),
            ),
        ];
        // The managed driver API prepares lazily and hides the server handle.
        // A named RPC preserves the bridge's eager prepare and raw-handle API.
        client
            .execute_stored_procedure("sp_prepare".into(), Some(parameters), None, ())
            .await
            .map_err(Error::Tds)?;
        while client.advance_to_rows().await.map_err(Error::Tds)? {}
        let values = client.get_return_values();
        match values.as_slice() {
            [value] => match value.value {
                ColumnValues::Int(handle) => Ok(Self::new(handle, sql)),
                _ => Err(Error::Tds(mssql_tds::error::Error::ProtocolError(
                    "sp_prepare did not return an integer handle".into(),
                ))),
            },
            _ => Err(Error::Tds(mssql_tds::error::Error::ProtocolError(
                "sp_prepare did not return exactly one output parameter".into(),
            ))),
        }
    }

    pub(crate) fn handle_parameter(&self) -> Vec<RpcParameter> {
        vec![RpcParameter::new(
            None,
            StatusFlags::NONE,
            SqlType::Int(Some(self.handle)),
        )]
    }

    /// The server-side `sp_prepare` handle.
    pub fn handle(&self) -> i32 {
        self.handle
    }

    fn parameter_declaration(params: &[&dyn ToSql], unicode: bool) -> Result<String> {
        params
            .iter()
            .enumerate()
            .map(|(index, param)| {
                let value = match (param.to_sql(), unicode) {
                    (SqlType::NVarchar(value, len), false) => SqlType::Varchar(value, len),
                    (SqlType::NVarcharMax(value), false) => SqlType::VarcharMax(value),
                    (value, _) => value,
                };
                Ok(format!(
                    "@P{} {}",
                    index + 1,
                    Self::sql_declaration(&value)?
                ))
            })
            .collect::<Result<Vec<_>>>()
            .map(|declarations| declarations.join(", "))
    }

    fn sql_declaration(value: &SqlType) -> Result<String> {
        if let SqlType::Table(name, _) = value {
            let schema = name
                .schema_name
                .as_deref()
                .unwrap_or("dbo")
                .replace(']', "]]");
            let name = name.type_name.replace(']', "]]");
            return Ok(format!("[{schema}].[{name}] READONLY"));
        }
        let name = TdsDataType::from(value)
            .get_meta_type_name()
            .map_err(Error::Tds)?;
        let suffix = match value {
            SqlType::NVarcharMax(_) | SqlType::VarcharMax(_) | SqlType::VarBinaryMax(_) => {
                "MAX".into()
            }
            SqlType::NVarchar(_, len) if *len > 4000 => "MAX".into(),
            SqlType::Varchar(_, len) | SqlType::VarBinary(_, len) if *len > 8000 => "MAX".into(),
            SqlType::NVarchar(_, len)
            | SqlType::Varchar(_, len)
            | SqlType::VarBinary(_, len)
            | SqlType::Binary(_, len)
            | SqlType::Char(_, len)
            | SqlType::NChar(_, len) => len.to_string(),
            SqlType::Decimal(parts) | SqlType::Numeric(parts) => parts
                .as_ref()
                .map(|parts| format!("{},{}", parts.precision, parts.scale))
                .unwrap_or_else(|| "18,10".into()),
            SqlType::Time(value) => value
                .as_ref()
                .map(|value| value.scale)
                .unwrap_or(DEFAULT_VARTIME_SCALE)
                .to_string(),
            SqlType::DateTime2(value) => value
                .as_ref()
                .map(|value| value.time.scale)
                .unwrap_or(DEFAULT_VARTIME_SCALE)
                .to_string(),
            SqlType::DateTimeOffset(value) => value
                .as_ref()
                .map(|value| value.datetime2.time.scale)
                .unwrap_or(DEFAULT_VARTIME_SCALE)
                .to_string(),
            SqlType::Vector(_, dimensions, VectorBaseType::Float32) => dimensions.to_string(),
            SqlType::Vector(_, dimensions, VectorBaseType::Float16) => {
                format!("{dimensions}, float16")
            }
            _ => return Ok(name.into()),
        };
        Ok(format!("{name}({suffix})"))
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
    fn prepare_declarations_preserve_parameter_types_and_encoding() {
        let params: &[&dyn ToSql] = &[&0i32, &"", &false];
        assert_eq!(
            PreparedStatement::parameter_declaration(params, true).unwrap(),
            "@P1 int, @P2 nvarchar(4000), @P3 bit"
        );
        assert_eq!(
            PreparedStatement::parameter_declaration(params, false).unwrap(),
            "@P1 int, @P2 varchar(4000), @P3 bit"
        );
        assert_eq!(
            PreparedStatement::parameter_declaration(&[], true).unwrap(),
            ""
        );
    }

    #[test]
    fn prepare_declarations_preserve_lengths_precision_and_scale() {
        use mssql_tds::datatypes::column_values::SqlTime;
        use mssql_tds::datatypes::decoder::DecimalParts;

        let cases = [
            (SqlType::NVarchar(None, 42), "nvarchar(42)"),
            (SqlType::NVarchar(None, 4001), "nvarchar(MAX)"),
            (SqlType::Varchar(None, 8001), "varchar(MAX)"),
            (SqlType::VarBinary(None, 8001), "varbinary(MAX)"),
            (SqlType::Binary(None, 16), "binary(16)"),
            (SqlType::Char(None, 12), "char(12)"),
            (SqlType::NChar(None, 12), "nchar(12)"),
            (SqlType::Numeric(None), "numeric(18,10)"),
            (
                SqlType::Decimal(Some(DecimalParts::from_string("1.25", 8, 2).unwrap())),
                "decimal(8,2)",
            ),
            (SqlType::Time(None), "time(7)"),
            (
                SqlType::Time(Some(SqlTime {
                    time_nanoseconds: 0,
                    scale: 3,
                })),
                "time(3)",
            ),
            (SqlType::DateTime2(None), "datetime2(7)"),
            (SqlType::DateTimeOffset(None), "datetimeoffset(7)"),
        ];
        for (value, expected) in cases {
            assert_eq!(
                PreparedStatement::sql_declaration(&value).unwrap(),
                expected
            );
        }
    }

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
