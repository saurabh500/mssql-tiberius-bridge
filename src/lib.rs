//! # mssql-tiberius-bridge
//!
//! A tiberius-compatible API bridge over Microsoft's `mssql-tds` crate.
//! Migrate from tiberius with minimal code changes.
//!
//! Provides familiar APIs like `row.get::<T, _>("column_name")`,
//! `stream.into_first_result()`, and `conn.query(sql, &[&params])` on top
//! of Microsoft's official TDS protocol implementation.
//!
//! # Quick start
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
//!     let rows = client
//!         .simple_query("SELECT name FROM sys.databases")
//!         .await?
//!         .into_first_result();
//!
//!     for row in rows {
//!         let name: &str = row.get("name").unwrap();
//!         println!("{name}");
//!     }
//!     Ok(())
//! }
//! ```

pub mod client;
pub mod column;
pub mod config;
pub mod error;
pub mod pool;
pub mod query;
pub mod row;
pub mod vector;

// Re-exports for ergonomic top-level access.
pub use client::Client;
pub use column::{Column, ColumnType};
pub use config::{AuthMethod, Config, EncryptionLevel};
pub use error::{Error, Result};
pub use pool::{Pool, PooledConnection, TdsManager};
pub use query::{ExecuteResult, QueryResult, ToSql};
pub use row::{ColumnIndex, FromSql, Row};
pub use vector::VectorValue;

// Re-export mssql-tds types that consumers might need for advanced use.
pub use mssql_tds::connection::tds_client::TdsClient;
pub use mssql_tds::datatypes::column_values::ColumnValues;
