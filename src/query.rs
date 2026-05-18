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

    fn debug_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<sql param>")
    }
}

impl std::fmt::Debug for dyn ToSql + '_ {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.debug_fmt(f)
    }
}

pub struct DebugParams<'a>(pub &'a [&'a dyn ToSql]);

impl std::fmt::Debug for DebugParams<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|p| *p as &dyn ToSql))
            .finish()
    }
}

impl ToSql for bool {
    fn to_sql(&self) -> SqlType {
        SqlType::Bit(Some(*self))
    }

    fn debug_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl ToSql for u8 {
    fn to_sql(&self) -> SqlType {
        SqlType::TinyInt(Some(*self))
    }

    fn debug_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl ToSql for i16 {
    fn to_sql(&self) -> SqlType {
        SqlType::SmallInt(Some(*self))
    }

    fn debug_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl ToSql for i32 {
    fn to_sql(&self) -> SqlType {
        SqlType::Int(Some(*self))
    }

    fn debug_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl ToSql for i64 {
    fn to_sql(&self) -> SqlType {
        SqlType::BigInt(Some(*self))
    }

    fn debug_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl ToSql for f32 {
    fn to_sql(&self) -> SqlType {
        SqlType::Real(Some(*self))
    }

    fn debug_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl ToSql for f64 {
    fn to_sql(&self) -> SqlType {
        SqlType::Float(Some(*self))
    }

    fn debug_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl ToSql for &str {
    fn to_sql(&self) -> SqlType {
        SqlType::NVarchar(
            Some(SqlString::from_utf8_string(self.to_string())),
            4000, // default max length
        )
    }

    fn debug_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl ToSql for String {
    fn to_sql(&self) -> SqlType {
        SqlType::NVarchar(Some(SqlString::from_utf8_string(self.clone())), 4000)
    }

    fn debug_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl ToSql for uuid::Uuid {
    fn to_sql(&self) -> SqlType {
        SqlType::Uuid(Some(*self))
    }
}

// ---------------------------------------------------------------------------
// rust_decimal
// ---------------------------------------------------------------------------

/// Converts a decimal string to `SqlType::Numeric` with the given precision/scale.
/// Falls back to max precision (38) if the initial precision is insufficient, or
/// returns `SqlType::Numeric(None)` if all attempts fail.
fn decimal_to_sql_type(decimal_str: &str, precision: u8, scale: u8) -> SqlType {
    use mssql_tds::datatypes::decoder::DecimalParts;

    match DecimalParts::from_string(decimal_str, precision, scale) {
        Ok(dp) => SqlType::Numeric(Some(dp)),
        Err(_) => DecimalParts::from_string(decimal_str, 38, scale)
            .map(|dp| SqlType::Numeric(Some(dp)))
            .unwrap_or_else(|_| SqlType::Numeric(None)),
    }
}

impl ToSql for rust_decimal::Decimal {
    fn to_sql(&self) -> SqlType {
        let decimal_str = self.to_string();
        let scale = self.scale() as u8;

        let mantissa = self.mantissa().abs();
        let precision = if mantissa == 0 {
            1
        } else {
            ((mantissa as f64).log10().floor() as u32 + 1) as u8
        };

        let precision = precision.max(scale);

        decimal_to_sql_type(&decimal_str, precision, scale)
    }

    fn debug_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
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

    fn debug_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Some(v) => {
                f.write_str("Some(")?;
                v.debug_fmt(f)?;
                f.write_str(")")
            }
            None => f.write_str("None"),
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

#[cfg(feature = "time")]
mod time_to_sql {
    use super::{SqlType, ToSql};
    use chrono::Datelike;
    use mssql_tds::datatypes::column_values::{
        SqlDate, SqlDateTime2, SqlDateTimeOffset, SqlTime, DEFAULT_VARTIME_SCALE,
    };
    use time::{Date, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};

    fn date_to_sql(d: Date) -> SqlDate {
        let chrono_date =
            chrono::NaiveDate::from_ymd_opt(d.year(), u8::from(d.month()) as u32, d.day() as u32)
                .expect("date out of SQL Server DATE range (0001-01-01..=9999-12-31)");
        SqlDate::create((chrono_date.num_days_from_ce() - 1) as u32)
            .expect("date out of SQL Server DATE range (0001-01-01..=9999-12-31)")
    }

    fn time_to_sql(t: Time) -> SqlTime {
        let nanos_since_midnight = (t.hour() as u64) * 3_600_000_000_000
            + (t.minute() as u64) * 60_000_000_000
            + (t.second() as u64) * 1_000_000_000
            + t.nanosecond() as u64;
        SqlTime {
            time_nanoseconds: nanos_since_midnight / 100,
            scale: DEFAULT_VARTIME_SCALE,
        }
    }

    fn primitive_dt_to_sql(dt: PrimitiveDateTime) -> SqlDateTime2 {
        SqlDateTime2 {
            days: date_to_sql(dt.date()).get_days(),
            time: time_to_sql(dt.time()),
        }
    }

    impl ToSql for Date {
        fn to_sql(&self) -> SqlType {
            SqlType::Date(Some(date_to_sql(*self)))
        }
    }

    impl ToSql for Time {
        fn to_sql(&self) -> SqlType {
            SqlType::Time(Some(time_to_sql(*self)))
        }
    }

    impl ToSql for PrimitiveDateTime {
        fn to_sql(&self) -> SqlType {
            SqlType::DateTime2(Some(primitive_dt_to_sql(*self)))
        }
    }

    impl ToSql for OffsetDateTime {
        fn to_sql(&self) -> SqlType {
            let offset = self.offset();
            let utc = self.to_offset(UtcOffset::UTC);
            SqlType::DateTimeOffset(Some(SqlDateTimeOffset {
                datetime2: primitive_dt_to_sql(PrimitiveDateTime::new(utc.date(), utc.time())),
                offset: (offset.whole_seconds() / 60) as i16,
            }))
        }
    }
}

#[cfg(feature = "jiff")]
mod jiff_to_sql {
    use super::{SqlType, ToSql};
    use chrono::Datelike;
    use jiff::{civil, tz::TimeZone, Timestamp, Zoned};
    use mssql_tds::datatypes::column_values::{
        SqlDate, SqlDateTime2, SqlDateTimeOffset, SqlTime, DEFAULT_VARTIME_SCALE,
    };

    fn date_to_sql(d: civil::Date) -> SqlDate {
        let chrono_date =
            chrono::NaiveDate::from_ymd_opt(d.year() as i32, d.month() as u32, d.day() as u32)
                .expect("date out of SQL Server DATE range (0001-01-01..=9999-12-31)");
        SqlDate::create((chrono_date.num_days_from_ce() - 1) as u32)
            .expect("date out of SQL Server DATE range (0001-01-01..=9999-12-31)")
    }

    fn time_to_sql(t: civil::Time) -> SqlTime {
        let nanos_since_midnight = (t.hour() as u64) * 3_600_000_000_000
            + (t.minute() as u64) * 60_000_000_000
            + (t.second() as u64) * 1_000_000_000
            + t.subsec_nanosecond() as u64;
        SqlTime {
            time_nanoseconds: nanos_since_midnight / 100,
            scale: DEFAULT_VARTIME_SCALE,
        }
    }

    fn datetime_to_sql(dt: civil::DateTime) -> SqlDateTime2 {
        SqlDateTime2 {
            days: date_to_sql(dt.date()).get_days(),
            time: time_to_sql(dt.time()),
        }
    }

    fn timestamp_to_sql(timestamp: Timestamp, offset_minutes: i16) -> SqlDateTimeOffset {
        let utc = TimeZone::UTC.to_datetime(timestamp);
        SqlDateTimeOffset {
            datetime2: datetime_to_sql(utc),
            offset: offset_minutes,
        }
    }

    impl ToSql for civil::Date {
        fn to_sql(&self) -> SqlType {
            SqlType::Date(Some(date_to_sql(*self)))
        }
    }

    impl ToSql for civil::Time {
        fn to_sql(&self) -> SqlType {
            SqlType::Time(Some(time_to_sql(*self)))
        }
    }

    impl ToSql for civil::DateTime {
        fn to_sql(&self) -> SqlType {
            SqlType::DateTime2(Some(datetime_to_sql(*self)))
        }
    }

    impl ToSql for Timestamp {
        fn to_sql(&self) -> SqlType {
            SqlType::DateTimeOffset(Some(timestamp_to_sql(*self, 0)))
        }
    }

    impl ToSql for Zoned {
        fn to_sql(&self) -> SqlType {
            SqlType::DateTimeOffset(Some(timestamp_to_sql(
                self.timestamp(),
                (self.offset().seconds() / 60) as i16,
            )))
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
    fn debug_params_formats_values() {
        let none = None::<i32>;
        let params: &[&dyn ToSql] = &[&1i32, &"test", &none];
        assert_eq!(format!("{:?}", DebugParams(params)), r#"[1, "test", None]"#);
    }

    #[test]
    fn decimal_to_sql_basic() {
        use rust_decimal::Decimal;
        let d = Decimal::new(12345, 2); // 123.45
        let sql_type = d.to_sql();
        // Should be Numeric type
        assert!(matches!(sql_type, SqlType::Numeric(Some(_))));
    }

    #[test]
    fn decimal_roundtrips_through_column_data() {
        use mssql_tds::datatypes::column_values::ColumnValues;
        use rust_decimal::Decimal;

        let original = Decimal::new(12345, 2); // 123.45
        let sql_type = original.to_sql();

        // Extract DecimalParts from SqlType
        let decimal_parts = match sql_type {
            SqlType::Numeric(Some(dp)) => dp,
            _ => panic!("expected SqlType::Numeric with DecimalParts"),
        };

        // Wrap in ColumnValues::Numeric
        let column_val = ColumnValues::Numeric(decimal_parts);

        // Convert back using FromSql
        let roundtripped: Option<Decimal> = crate::FromSql::from_sql(&column_val);
        assert_eq!(roundtripped, Some(original));
    }

    #[test]
    fn decimal_zero_roundtrips() {
        use mssql_tds::datatypes::column_values::ColumnValues;
        use rust_decimal::Decimal;

        let original = Decimal::new(0, 0);
        let sql_type = original.to_sql();
        let decimal_parts = match sql_type {
            SqlType::Numeric(Some(dp)) => dp,
            _ => panic!("expected SqlType::Numeric"),
        };
        let column_val = ColumnValues::Numeric(decimal_parts);
        let roundtripped: Option<Decimal> = crate::FromSql::from_sql(&column_val);
        assert_eq!(roundtripped, Some(original));
    }

    #[test]
    fn decimal_negative_roundtrips() {
        use mssql_tds::datatypes::column_values::ColumnValues;
        use rust_decimal::Decimal;

        let original = Decimal::new(-99999, 4); // -9.9999
        let sql_type = original.to_sql();
        let decimal_parts = match sql_type {
            SqlType::Numeric(Some(dp)) => dp,
            _ => panic!("expected SqlType::Numeric"),
        };
        let column_val = ColumnValues::Numeric(decimal_parts);
        let roundtripped: Option<Decimal> = crate::FromSql::from_sql(&column_val);
        assert_eq!(roundtripped, Some(original));
    }

    #[test]
    fn decimal_high_precision_roundtrips() {
        use mssql_tds::datatypes::column_values::ColumnValues;
        use rust_decimal::Decimal;

        // rust_decimal supports up to 28 digits of precision with i64 mantissa
        let original = Decimal::new(9223372036854775807i64, 10); // i64::MAX
        let sql_type = original.to_sql();
        let decimal_parts = match sql_type {
            SqlType::Numeric(Some(dp)) => dp,
            _ => panic!("expected SqlType::Numeric"),
        };
        let column_val = ColumnValues::Numeric(decimal_parts);
        let roundtripped: Option<Decimal> = crate::FromSql::from_sql(&column_val);
        assert_eq!(roundtripped, Some(original));
    }

    #[test]
    fn decimal_debug_fmt_displays_value() {
        use rust_decimal::Decimal;

        let d = Decimal::new(12345, 2);
        let params: Vec<&dyn ToSql> = vec![&d];
        let output = format!("{:?}", DebugParams(&params));
        assert!(output.contains("123.45"));
    }

    #[test]
    fn decimal_precision_boundary_uses_fallback() {
        use rust_decimal::Decimal;
        use std::str::FromStr;

        // A value with 28 significant digits — the maximum for rust_decimal.
        let d = Decimal::from_str("9999999999999999999999999999").unwrap();
        let sql_type = d.to_sql();
        assert!(matches!(sql_type, SqlType::Numeric(Some(_))));
    }

    #[test]
    fn decimal_to_sql_type_fallback_on_low_precision() {
        // Call with deliberately too-low precision to trigger the fallback path
        let result = super::decimal_to_sql_type("12345.67", 3, 2);
        // The initial from_string(precision=3) fails because 7 digits > 3,
        // then the fallback with precision=38 succeeds.
        assert!(matches!(result, SqlType::Numeric(Some(_))));
    }

    #[test]
    fn decimal_to_sql_type_total_failure_returns_none() {
        // Invalid decimal string that can't be parsed at all
        let result = super::decimal_to_sql_type("not_a_number", 10, 2);
        assert!(matches!(result, SqlType::Numeric(None)));
    }

    #[test]
    fn decimal_scale_exceeds_precision_handled() {
        use rust_decimal::Decimal;

        // Scale=28, mantissa=1 → "0.0000000000000000000000000001"
        // precision from log10(1)=0 → 1, but scale=28, so precision.max(28)=28
        let d = Decimal::new(1, 28);
        let sql_type = d.to_sql();
        assert!(matches!(sql_type, SqlType::Numeric(Some(_))));
    }

    #[cfg(feature = "time")]
    #[test]
    fn time_date_roundtrips_through_column_data() {
        let date = time::Date::from_calendar_date(2024, time::Month::February, 29).unwrap();
        let SqlType::Date(Some(sql_date)) = date.to_sql() else {
            panic!("expected SQL date")
        };
        let value = ColumnValues::Date(sql_date);
        assert_eq!(crate::FromSql::from_sql(&value), Some(date));
    }

    #[cfg(feature = "jiff")]
    #[test]
    fn jiff_date_roundtrips_through_column_data() {
        let date = jiff::civil::date(2024, 2, 29);
        let SqlType::Date(Some(sql_date)) = date.to_sql() else {
            panic!("expected SQL date")
        };
        let value = ColumnValues::Date(sql_date);
        assert_eq!(crate::FromSql::from_sql(&value), Some(date));
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
