//! Bulk insert (BCP) — high-throughput row loading via the TDS BulkLoadBCP token.
//!
//! Mirrors tiberius' `Client::bulk_insert` API on top of mssql-tds'
//! [`mssql_tds::connection::bulk_copy::BulkCopy`] surface. Bulk insert is
//! typically **10–100×** faster than per-row `INSERT` statements for
//! loads of more than a few hundred rows because it streams rows in their
//! TDS wire format and bypasses the query optimizer.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use std::time::Duration;
//! use async_trait::async_trait;
//! use mssql_tiberius_bridge::{Client, Config, AuthMethod, Result};
//! use mssql_tiberius_bridge::bulk::{BulkLoadRow, ColumnMapping};
//! use mssql_tds::core::TdsResult;
//! use mssql_tds::datatypes::column_values::ColumnValues;
//! use mssql_tds::datatypes::sql_string::SqlString;
//! use mssql_tds::message::bulk_load::StreamingBulkLoadWriter;
//!
//! struct User { id: i32, name: String }
//!
//! #[async_trait]
//! impl BulkLoadRow for User {
//!     async fn write_to_packet(
//!         &self,
//!         writer: &mut StreamingBulkLoadWriter<'_>,
//!         column_index: &mut usize,
//!     ) -> TdsResult<()> {
//!         writer.write_column_value(*column_index, &ColumnValues::Int(self.id)).await?;
//!         *column_index += 1;
//!         writer.write_column_value(
//!             *column_index,
//!             &ColumnValues::String(SqlString::from_utf8_string(self.name.clone())),
//!         ).await?;
//!         *column_index += 1;
//!         Ok(())
//!     }
//! }
//!
//! # async fn run(client: &mut Client) -> Result<()> {
//! let users = vec![
//!     User { id: 1, name: "Ada".into() },
//!     User { id: 2, name: "Grace".into() },
//! ];
//! let result = client
//!     .bulk_insert("Users")
//!     .batch_size(5000)
//!     .timeout(Duration::from_secs(60))
//!     .table_lock(true)
//!     .send(users)
//!     .await?;
//! println!("loaded {} rows in {:?}", result.rows_affected, result.elapsed);
//! # Ok(()) }
//! ```
//!
//! # Column mapping
//!
//! By default, source row columns map to the destination table by ordinal
//! (skipping identity columns unless [`BulkInsert::keep_identity`] is set).
//! For named mapping, use [`BulkInsert::add_column_mapping`] /
//! [`BulkInsert::map_column`] / [`BulkInsert::map_column_by_ordinal`], or pass
//! the explicit destination column list to
//! [`Client::bulk_insert_with_columns`].
//!
//! # Options
//!
//! All `SqlBulkCopyOptions`-equivalent flags are exposed as builder methods:
//! [`keep_identity`](BulkInsert::keep_identity),
//! [`keep_nulls`](BulkInsert::keep_nulls),
//! [`table_lock`](BulkInsert::table_lock),
//! [`check_constraints`](BulkInsert::check_constraints),
//! [`fire_triggers`](BulkInsert::fire_triggers),
//! [`use_internal_transaction`](BulkInsert::use_internal_transaction),
//! [`batch_size`](BulkInsert::batch_size),
//! [`timeout`](BulkInsert::timeout),
//! [`notification_interval`](BulkInsert::notification_interval).

use std::time::Duration;

use mssql_tds::connection::bulk_copy::BulkCopy as TdsBulkCopy;
use mssql_tds::connection::tds_client::TdsClient;

use crate::error::{Error, Result};

// Re-export upstream types that callers will use directly.
pub use mssql_tds::connection::bulk_copy::{
    BulkCopyOptions, BulkCopyProgress, BulkCopyResult, BulkLoadRow, ColumnMapping,
    ColumnMappingSource,
};

/// Builder + executor for a single bulk insert into one destination table.
///
/// Created via [`Client::bulk_insert`](crate::Client::bulk_insert) or
/// [`Client::bulk_insert_with_columns`](crate::Client::bulk_insert_with_columns).
/// Configure it with the option setters, then call [`send`](Self::send) to
/// stream rows.
pub struct BulkInsert<'a> {
    inner: TdsBulkCopy<'a>,
}

impl<'a> BulkInsert<'a> {
    /// Construct a `BulkInsert` for the given destination table.
    ///
    /// Prefer [`Client::bulk_insert`](crate::Client::bulk_insert) — this is the
    /// low-level entry point for callers that already hold a
    /// [`TdsClient`](mssql_tds::connection::tds_client::TdsClient) reference
    /// (e.g., via [`Client::inner_mut`](crate::Client::inner_mut)).
    pub fn new(client: &'a mut TdsClient, table_name: impl Into<String>) -> Self {
        Self {
            inner: TdsBulkCopy::new(client, table_name),
        }
    }

    /// Number of rows per server-side batch. Default 0 = single batch.
    pub fn batch_size(mut self, n: usize) -> Self {
        self.inner = self.inner.batch_size(n);
        self
    }

    /// Per-operation timeout. Default 30 seconds. Pass `Duration::ZERO` for no timeout.
    pub fn timeout(mut self, t: Duration) -> Self {
        self.inner = self.inner.timeout(t);
        self
    }

    /// Enforce CHECK constraints on the destination table during the load. Default off.
    pub fn check_constraints(mut self, enabled: bool) -> Self {
        self.inner = self.inner.check_constraints(enabled);
        self
    }

    /// Fire INSERT triggers for every loaded row. Default off.
    pub fn fire_triggers(mut self, enabled: bool) -> Self {
        self.inner = self.inner.fire_triggers(enabled);
        self
    }

    /// Preserve source identity column values. Default off (server auto-generates).
    pub fn keep_identity(mut self, enabled: bool) -> Self {
        self.inner = self.inner.keep_identity(enabled);
        self
    }

    /// Preserve source NULLs even when destination has a DEFAULT. Default off.
    pub fn keep_nulls(mut self, enabled: bool) -> Self {
        self.inner = self.inner.keep_nulls(enabled);
        self
    }

    /// Acquire a bulk-update (TABLOCK) lock for the duration of the load. Default off.
    pub fn table_lock(mut self, enabled: bool) -> Self {
        self.inner = self.inner.table_lock(enabled);
        self
    }

    /// Wrap each batch in its own server-side transaction. Default off.
    ///
    /// **Cannot be combined with an active client-level transaction.**
    pub fn use_internal_transaction(mut self, enabled: bool) -> Self {
        self.inner = self.inner.use_internal_transaction(enabled);
        self
    }

    /// Rows between progress callback invocations. Default 0 = no callbacks.
    pub fn notification_interval(mut self, n: usize) -> Self {
        self.inner = self.inner.notification_interval(n);
        self
    }

    /// Add an explicit source → destination column mapping.
    ///
    /// When any mapping is added, ordinal auto-mapping is disabled and only
    /// the listed mappings apply.
    pub fn add_column_mapping(mut self, mapping: ColumnMapping) -> Self {
        self.inner = self.inner.add_column_mapping(mapping);
        self
    }

    /// Convenience: map source column `source_name` to destination column `dest_name`.
    pub fn map_column(self, source_name: impl Into<String>, dest_name: impl Into<String>) -> Self {
        self.add_column_mapping(ColumnMapping::by_name(source_name, dest_name))
    }

    /// Convenience: map source ordinal `source_ord` (0-based) to destination column `dest_name`.
    pub fn map_column_by_ordinal(self, source_ord: usize, dest_name: impl Into<String>) -> Self {
        self.add_column_mapping(ColumnMapping::by_ordinal(source_ord, dest_name))
    }

    /// Stream the row iterator to the server.
    ///
    /// Each `R: BulkLoadRow` writes its columns directly into the streaming
    /// TDS packet — no intermediate buffering, no allocation per row beyond
    /// what the row itself owns.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Tds`] for any failure: column type mismatch, network
    /// error, server-side constraint violation, timeout, etc. On error the
    /// connection state is recovered automatically by mssql-tds.
    pub async fn send<I, R>(mut self, rows: I) -> Result<BulkCopyResult>
    where
        I: IntoIterator<Item = R>,
        R: BulkLoadRow,
    {
        self.inner
            .write_to_server_zerocopy(rows)
            .await
            .map_err(Error::Tds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // No DB-required tests live here; the BulkCopy state is internal to
    // the upstream crate, so we can't construct a BulkInsert<'a> without
    // a TdsClient. We just sanity-check that the re-exported types resolve
    // and that the convenience mapping helpers produce the expected variants.

    #[test]
    fn map_column_by_name_creates_named_mapping() {
        let m = ColumnMapping::by_name("src", "dst");
        match m.source {
            ColumnMappingSource::Name(n) => assert_eq!(n, "src"),
            _ => panic!("expected Name mapping"),
        }
        assert_eq!(m.destination, "dst");
    }

    #[test]
    fn map_column_by_ordinal_creates_ordinal_mapping() {
        let m = ColumnMapping::by_ordinal(3, "dst");
        match m.source {
            ColumnMappingSource::Ordinal(n) => assert_eq!(n, 3),
            _ => panic!("expected Ordinal mapping"),
        }
        assert_eq!(m.destination, "dst");
    }

    #[test]
    fn bulk_copy_options_defaults_match_dotnet_sqlbulkcopy() {
        let o = BulkCopyOptions::default();
        assert_eq!(o.batch_size, 0);
        assert_eq!(o.timeout_sec, 30);
        assert!(!o.check_constraints);
        assert!(!o.fire_triggers);
        assert!(!o.keep_identity);
        assert!(!o.keep_nulls);
        assert!(!o.table_lock);
        assert!(!o.use_internal_transaction);
    }

    #[test]
    fn bulk_copy_result_computes_throughput() {
        let r = BulkCopyResult::new(10_000, Duration::from_secs(2));
        assert_eq!(r.rows_affected, 10_000);
        assert_eq!(r.elapsed, Duration::from_secs(2));
        assert!((r.rows_per_second - 5_000.0).abs() < 1.0);
    }

    #[test]
    fn bulk_copy_result_zero_elapsed_yields_zero_throughput() {
        let r = BulkCopyResult::new(100, Duration::ZERO);
        assert_eq!(r.rows_per_second, 0.0);
    }
}
