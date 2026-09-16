//! # mssql-tiberius-bridge
//!
//! A tiberius-compatible API bridge over Microsoft's [`mssql-tds`](https://crates.io/crates/mssql-tds) crate.
//! Migrate from tiberius with minimal code changes.
//!
//! This crate wraps `mssql-tds` (Microsoft's official Rust TDS protocol implementation)
//! and provides the familiar tiberius-style API: named column access via `row.get::<T, _>("col")`,
//! `into_first_result()`, positional `@P1, @P2` parameter binding, and a fluent [`Config`] builder.
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use mssql_tiberius_bridge::{Config, AuthMethod, Client};
//!
//! #[tokio::main]
//! async fn main() -> mssql_tiberius_bridge::Result<()> {
//!     let mut cfg = Config::new();
//!     cfg.host("localhost")
//!        .port(1433)
//!        .database("master")
//!        .authentication(AuthMethod::sql_server("sa", "password"))
//!        .trust_cert();
//!
//!     let mut client = Client::connect(&cfg).await?;
//!
//!     // Simple query
//!     let rows = client
//!         .simple_query("SELECT name FROM sys.databases")
//!         .await?
//!         .into_first_result();
//!
//!     for row in &rows {
//!         let name: &str = row.get("name").unwrap();
//!         println!("{name}");
//!     }
//!
//!     // Parameterized query
//!     let rows = client
//!         .query("SELECT @P1 AS greeting, @P2 AS number", &[&"hello", &42i32])
//!         .await?
//!         .into_first_result();
//!
//!     let greeting: &str = rows[0].get("greeting").unwrap();
//!     let number: i32 = rows[0].get("number").unwrap();
//!     println!("{greeting} {number}");
//!
//!     Ok(())
//! }
//! ```
//!
//! # Feature Flags
//!
//! | Flag | Default | Description |
//! |------|---------|-------------|
//! | `json` | off | Enables [`serde_json::Value`] support for [`FromSql`] and [`ToSql`] |
//! | `time` | off | Enables `time` crate support for [`FromSql`] and [`ToSql`] |
//! | `jiff` | off | Enables `jiff` crate support for [`FromSql`] and [`ToSql`] |
//! | `serde` | off | Enables `serde::Deserialize` for [`Row`] (see [`serde_de`]) |
//! | `arrow` | off | Enables [`BulkInsert::send_arrow`](crate::bulk::BulkInsert::send_arrow) for Apache Arrow `RecordBatch` input (see [`bulk_arrow`]) |
//! | `bb8` | off | Enables [`bb8`][bb8] connection pooling via [`TdsManager`] |
//!
//! # Modules
//!
//! - [`client`] — [`Client`] wrapper with `simple_query`, `query`, `execute`
//! - [`config`] — [`Config`] builder, [`AuthMethod`], [`EncryptionLevel`]
//! - [`row`] — [`Row`] with named/indexed access, [`FromSql`] trait
//! - [`query`] — [`QueryResult`], [`ToSql`] trait, [`ExecuteResult`]
//! - [`column`] — [`Column`] metadata, [`ColumnType`] enum
//! - [`pool`] — Connection pooling via [`deadpool`] and optional [`bb8`][bb8]
//! - [`prepared`] — [`PreparedStatement`] for server-side `sp_prepare`/`sp_execute`
//! - [`error`] — [`Error`] and [`Result`] types
//!
//! # Migration from tiberius
//!
//! See the [README](https://github.com/saurabh500/mssql-tiberius-bridge#migration-from-tiberius)
//! for a full migration table. Key differences:
//!
//! - TCP transport is handled internally — no `TcpStream` boilerplate
//! - `row.get::<&str, _>("col")` works (strings are pre-decoded from UTF-16)
//! - Connection pooling via [`TdsManager`] + [`deadpool`] or optional [`bb8`][bb8] with native session resets
//!
//! [bb8]: https://docs.rs/bb8/0.9.1/bb8/

pub mod bulk;
#[cfg(feature = "arrow")]
pub mod bulk_arrow;
pub mod client;
pub mod column;
pub mod compat;
pub mod config;
pub mod error;
mod operation;
pub mod pool;
pub mod prepared;
pub mod query;
pub mod row;
#[cfg(feature = "serde")]
pub mod serde_de;

// Re-exports for ergonomic top-level access.
pub use bulk::{
    BulkInsert, BulkLoadRequest, BulkLoadRow, ColumnMapping, ColumnMappingSource, IntoRow, TokenRow,
};
pub use client::Client;
pub use column::{Collation, Column, ColumnType, MultiPartName};
pub use compat::{
    ColumnData, FromSqlOwned, IntoSql, Query, QueryItem, QueryStream, ResultMetadata,
};
pub use config::{AuthMethod, Config, EncryptionLevel, Transport};
pub use error::{Error, Result};
#[cfg(feature = "bb8")]
pub use pool::{Bb8Pool, Bb8PooledConnection};
pub use pool::{Pool, PooledConnection, RecyclingMethod, TdsManager};
pub use prepared::PreparedStatement;
pub use query::{DebugParams, ExecuteResult, QueryResult, ToSql};
pub use row::{ColumnIndex, FromSql, Row};

// Re-export mssql-tds types that consumers might need for advanced use.
/// The underlying mssql-tds client type, exposed for advanced operations
/// via [`Client::inner_mut()`].
pub use mssql_tds::connection::tds_client::TdsClient;
/// Raw column values from mssql-tds, exposed for low-level access
/// via [`Row::raw_value()`].
/// Spatial values remain `ColumnValues::Bytes`; their identity is exposed
/// through [`ColumnType::Geography`] and [`ColumnType::Geometry`].
pub use mssql_tds::datatypes::column_values::ColumnValues;
/// Decimal/Numeric value representation from mssql-tds.
/// Returned inside `ColumnValues::Decimal` and `ColumnValues::Numeric`.
pub use mssql_tds::datatypes::decoder::DecimalParts;
