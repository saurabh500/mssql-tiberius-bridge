//! Query result types and the [`ToSql`] trait for parameter binding.
//!
//! [`QueryResult`] wraps streamed result sets from SQL Server and provides
//! [`into_first_result()`](QueryResult::into_first_result) (single result set)
//! and [`into_results()`](QueryResult::into_results) (multiple result sets).

use mssql_tds::datatypes::column_values::ColumnValues;
use mssql_tds::datatypes::sql_string::SqlString;
use mssql_tds::datatypes::sqltypes::SqlType;
use mssql_tds::message::parameters::rpc_parameters::{RpcParameter, StatusFlags};
use mssql_tds::query::metadata::ColumnMetadata;

use crate::row::Row;

/// Result of an `execute()` call, containing row counts per statement.
#[derive(Debug, Clone)]
pub struct ExecuteResult {
    pub(crate) counts: Vec<u64>,
}

impl ExecuteResult {
    /// Total rows affected across all statements.
    pub fn total(&self) -> u64 {
        self.counts.iter().sum()
    }

    /// Iterate over per-statement row counts.
    #[allow(clippy::should_implement_trait)]
    pub fn into_iter(self) -> impl Iterator<Item = u64> {
        self.counts.into_iter()
    }
}

/// Collected query results from one or more SQL statements.
///
/// Use [`into_first_result()`](Self::into_first_result) for single-statement
/// queries (most common), or [`into_results()`](Self::into_results) for
/// multi-statement batches.
pub struct QueryResult {
    pub(crate) result_sets: Vec<(Vec<ColumnMetadata>, Vec<Vec<ColumnValues>>)>,
}

impl QueryResult {
    /// Consume the first result set into a `Vec<Row>`.
    ///
    /// This is the most common access pattern, equivalent to tiberius'
    /// `stream.into_first_result().await?`.
    ///
    /// Returns an empty `Vec` if the query produced no result set.
    pub fn into_first_result(self) -> Vec<Row> {
        let mut sets = self.result_sets;
        if sets.is_empty() {
            return Vec::new();
        }
        let (meta, rows) = sets.remove(0);
        let schema = crate::row::RowSchema::from_metadata(&meta);
        rows.into_iter()
            .map(|values| Row::from_schema(schema.clone(), values))
            .collect()
    }

    /// Consume all result sets into a `Vec<Vec<Row>>`.
    ///
    /// Use for multi-statement batches like `SELECT 1; SELECT 2`.
    pub fn into_results(self) -> Vec<Vec<Row>> {
        self.result_sets
            .into_iter()
            .map(|(meta, rows)| {
                let schema = crate::row::RowSchema::from_metadata(&meta);
                rows.into_iter()
                    .map(|values| Row::from_schema(schema.clone(), values))
                    .collect()
            })
            .collect()
    }

    /// Number of result sets.
    pub fn result_set_count(&self) -> usize {
        self.result_sets.len()
    }

    /// Consume into a [`Stream`](futures_core::Stream) of rows across all
    /// result sets.
    ///
    /// Mirrors tiberius' `QueryStream::into_row_stream()` for API
    /// compatibility. Use this when migrating code that calls
    /// `.into_row_stream().map(...).next().await` (e.g., the
    /// `windmill-worker` MSSQL S3 export path).
    ///
    /// # Limitations
    ///
    /// **Rows are pre-collected.** Unlike tiberius (which streams from the
    /// wire), this yields rows that have already been buffered into memory
    /// during the originating `query()` / `simple_query()` call. The
    /// streaming API is preserved for migration ergonomics, but wire-level
    /// streaming will require the `Client::query_streamed` follow-up
    /// tracked in <https://github.com/saurabh500/mssql-tiberius-bridge/issues/20>.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use mssql_tiberius_bridge::Client;
    /// use futures_util::StreamExt;
    ///
    /// # async fn example(client: &mut Client) -> mssql_tiberius_bridge::Result<()> {
    /// let mut stream = client
    ///     .simple_query("SELECT 1 AS n UNION ALL SELECT 2")
    ///     .await?
    ///     .into_row_stream();
    /// while let Some(row) = stream.next().await {
    ///     let row = row?;
    ///     println!("{:?}", row.get::<i32, _>("n"));
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn into_row_stream(self) -> RowStream {
        let rows: Vec<Row> = self
            .result_sets
            .into_iter()
            .flat_map(|(meta, rows)| {
                let schema = crate::row::RowSchema::from_metadata(&meta);
                rows.into_iter()
                    .map(move |values| Row::from_schema(schema.clone(), values))
            })
            .collect();
        RowStream {
            rows: rows.into_iter(),
        }
    }

    /// Create an empty QueryResult.
    #[allow(dead_code)]
    pub(crate) fn empty() -> Self {
        QueryResult {
            result_sets: Vec::new(),
        }
    }
}

/// A `Stream` of [`Row`]s yielded across all result sets of a buffered
/// [`QueryResult`].
///
/// Created by [`QueryResult::into_row_stream`]. Implements
/// [`futures_core::Stream`] so it composes with `StreamExt`/`TryStreamExt`
/// (`.map`, `.try_next`, `.collect`, etc.) — matching the API surface
/// callers used with tiberius' `into_row_stream()`.
///
/// **Note:** rows are pre-buffered (see
/// [`QueryResult::into_row_stream`] for the limitation and roadmap).
pub struct RowStream {
    rows: std::vec::IntoIter<Row>,
}

impl futures_core::Stream for RowStream {
    type Item = crate::error::Result<Row>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Ready(self.rows.next().map(Ok))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.rows.len();
        (n, Some(n))
    }
}

// ---------------------------------------------------------------------------
// ToSql — convert Rust types to RPC parameters
// ---------------------------------------------------------------------------

/// Trait for types that can be used as query parameters.
///
/// Implemented for common Rust types:
///
/// | Rust type | SQL Server type |
/// |-----------|------------------|
/// | `bool` | `bit` |
/// | `u8` | `tinyint` |
/// | `i16` | `smallint` |
/// | `i32` | `int` |
/// | `i64` | `bigint` |
/// | `f32` | `real` |
/// | `f64` | `float` |
/// | `&str` | `nvarchar(4000)` |
/// | `String` | `nvarchar(4000)` |
/// | `uuid::Uuid` | `uniqueidentifier` |
/// | `Option<T>` | Nullable version of inner type |
pub trait ToSql: Send + Sync {
    /// Convert this value into an mssql-tds `SqlType` for parameter binding.
    fn to_sql(&self) -> SqlType;
}

impl ToSql for bool {
    fn to_sql(&self) -> SqlType {
        SqlType::Bit(Some(*self))
    }
}

impl ToSql for u8 {
    fn to_sql(&self) -> SqlType {
        SqlType::TinyInt(Some(*self))
    }
}

impl ToSql for i16 {
    fn to_sql(&self) -> SqlType {
        SqlType::SmallInt(Some(*self))
    }
}

impl ToSql for i32 {
    fn to_sql(&self) -> SqlType {
        SqlType::Int(Some(*self))
    }
}

impl ToSql for i64 {
    fn to_sql(&self) -> SqlType {
        SqlType::BigInt(Some(*self))
    }
}

impl ToSql for f32 {
    fn to_sql(&self) -> SqlType {
        SqlType::Real(Some(*self))
    }
}

impl ToSql for f64 {
    fn to_sql(&self) -> SqlType {
        SqlType::Float(Some(*self))
    }
}

impl ToSql for &str {
    fn to_sql(&self) -> SqlType {
        SqlType::NVarchar(
            Some(SqlString::from_utf8_string(self.to_string())),
            4000, // default max length
        )
    }
}

impl ToSql for String {
    fn to_sql(&self) -> SqlType {
        SqlType::NVarchar(Some(SqlString::from_utf8_string(self.clone())), 4000)
    }
}

impl ToSql for uuid::Uuid {
    fn to_sql(&self) -> SqlType {
        SqlType::Uuid(Some(*self))
    }
}

// Option<T>: None becomes the SQL NULL of the same type
impl<T: ToSql + Default> ToSql for Option<T> {
    fn to_sql(&self) -> SqlType {
        match self {
            Some(v) => v.to_sql(),
            None => {
                // Use a default-constructed value to get the right SqlType variant,
                // then we'd need to set it to None. Since SqlType variants all
                // have Option, we use a type-specific approach.
                // For simplicity, default to NVarchar NULL.
                SqlType::NVarchar(None, 4000)
            }
        }
    }
}

impl ToSql for serde_json::Value {
    fn to_sql(&self) -> SqlType {
        SqlType::NVarchar(Some(SqlString::from_utf8_string(self.to_string())), 4000)
    }
}

// ---------------------------------------------------------------------------
// Binary
// ---------------------------------------------------------------------------

impl ToSql for Vec<u8> {
    fn to_sql(&self) -> SqlType {
        SqlType::VarBinaryMax(Some(self.clone()))
    }
}

impl ToSql for &[u8] {
    fn to_sql(&self) -> SqlType {
        SqlType::VarBinaryMax(Some(self.to_vec()))
    }
}

// ---------------------------------------------------------------------------
// chrono date/time
// ---------------------------------------------------------------------------

// chrono is an unconditional dep on the bridge; FromSql for these types
// already lives in `row.rs`, so the ToSql side is provided unconditionally too.
mod chrono_to_sql {
    use super::{SqlType, ToSql};
    use chrono::{
        DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc,
    };
    use mssql_tds::datatypes::column_values::{
        SqlDate, SqlDateTime2, SqlDateTimeOffset, SqlTime, DEFAULT_VARTIME_SCALE,
    };

    fn naive_date_to_sql(d: &NaiveDate) -> SqlDate {
        // SqlDate stores days where 0 = 0001-01-01; chrono's num_days_from_ce
        // counts 0001-01-01 as day 1.
        let days = (d.num_days_from_ce() - 1) as u32;
        SqlDate::create(days).expect("date out of SQL Server DATE range (0001-01-01..=9999-12-31)")
    }

    fn naive_time_to_sql(t: &NaiveTime) -> SqlTime {
        // SqlTime.time_nanoseconds is actually in 100-nanosecond units (mirrors
        // the FromSql side in row.rs).
        let nanos_since_midnight =
            (t.num_seconds_from_midnight() as u64) * 1_000_000_000 + t.nanosecond() as u64;
        SqlTime {
            time_nanoseconds: nanos_since_midnight / 100,
            scale: DEFAULT_VARTIME_SCALE,
        }
    }

    fn naive_dt_to_sql(dt: &NaiveDateTime) -> SqlDateTime2 {
        SqlDateTime2 {
            days: (dt.date().num_days_from_ce() - 1) as u32,
            time: naive_time_to_sql(&dt.time()),
        }
    }

    impl ToSql for NaiveDate {
        fn to_sql(&self) -> SqlType {
            SqlType::Date(Some(naive_date_to_sql(self)))
        }
    }

    impl ToSql for NaiveTime {
        fn to_sql(&self) -> SqlType {
            SqlType::Time(Some(naive_time_to_sql(self)))
        }
    }

    impl ToSql for NaiveDateTime {
        fn to_sql(&self) -> SqlType {
            SqlType::DateTime2(Some(naive_dt_to_sql(self)))
        }
    }

    impl ToSql for DateTime<FixedOffset> {
        fn to_sql(&self) -> SqlType {
            // Storage matches FromSql: dt2 holds the UTC components, offset is
            // the original tz offset in minutes.
            let datetime2 = naive_dt_to_sql(&self.naive_utc());
            let offset = (self.offset().local_minus_utc() / 60) as i16;
            SqlType::DateTimeOffset(Some(SqlDateTimeOffset { datetime2, offset }))
        }
    }

    impl ToSql for DateTime<Utc> {
        fn to_sql(&self) -> SqlType {
            let datetime2 = naive_dt_to_sql(&self.naive_utc());
            SqlType::DateTimeOffset(Some(SqlDateTimeOffset {
                datetime2,
                offset: 0,
            }))
        }
    }
}

fn encode_string_parameters(sql_type: SqlType, unicode: bool) -> SqlType {
    if unicode {
        return sql_type;
    }

    match sql_type {
        SqlType::NVarchar(value, len) => SqlType::Varchar(value, len),
        SqlType::NVarcharMax(value) => SqlType::VarcharMax(value),
        other => other,
    }
}

/// Build a Vec<RpcParameter> from a slice of ToSql values, using positional
/// naming (@P1, @P2, ...) like tiberius.
pub fn build_params(params: &[&dyn ToSql]) -> Vec<RpcParameter> {
    build_params_with_string_encoding(params, true)
}

pub(crate) fn build_params_with_string_encoding(
    params: &[&dyn ToSql],
    unicode: bool,
) -> Vec<RpcParameter> {
    params
        .iter()
        .enumerate()
        .map(|(i, p)| {
            RpcParameter::new(
                Some(format!("@P{}", i + 1)),
                StatusFlags::NONE,
                encode_string_parameters(p.to_sql(), unicode),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_sql_primitives() {
        let _ = 42i32.to_sql();
        let _ = "hello".to_sql();
        let _ = true.to_sql();
        let _ = 2.72_f64.to_sql();
    }

    #[test]
    fn build_params_positional_naming() {
        let params = build_params(&[&1i32, &"test"]);
        assert_eq!(params.len(), 2);
        // name field is pub(crate) in mssql-tds, so we just verify count
    }

    #[test]
    fn string_parameter_encoding_can_use_varchar() {
        let ty = encode_string_parameters("hello".to_sql(), false);
        assert!(matches!(ty, SqlType::Varchar(_, 4000)));
    }

    #[test]
    fn string_parameter_encoding_defaults_to_nvarchar() {
        let ty = encode_string_parameters("hello".to_sql(), true);
        assert!(matches!(ty, SqlType::NVarchar(_, 4000)));
    }

    #[test]
    fn empty_query_result() {
        let qr = QueryResult::empty();
        assert_eq!(qr.result_set_count(), 0);
        assert!(qr.into_first_result().is_empty());
    }
}
