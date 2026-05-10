//! Row type with named and indexed column access, mirroring tiberius' `Row` API.
//!
//! A [`Row`] is returned from [`QueryResult::into_first_result()`](crate::QueryResult::into_first_result)
//! and provides typed access to column values via [`get()`](Row::get).

use std::collections::HashMap;
use std::sync::Arc;

use mssql_tds::datatypes::column_values::ColumnValues;
use mssql_tds::query::metadata::ColumnMetadata;

use crate::column::Column;
use crate::error::{Error, Result};

/// Per-result-set schema (column metadata + name index).
///
/// Built once per result set and shared across every [`Row`] in that set
/// via [`Arc`], so streaming N rows from a result set with K columns no
/// longer pays the O(K) `Vec<Column>` + `HashMap<String, usize>` build
/// cost per row.
#[derive(Debug)]
pub struct RowSchema {
    pub(crate) columns: Vec<Column>,
    pub(crate) name_map: HashMap<String, usize>,
}

impl RowSchema {
    /// Build a schema from a slice of mssql-tds column metadata.
    pub fn from_metadata(metadata: &[ColumnMetadata]) -> Arc<Self> {
        let columns: Vec<Column> = metadata.iter().map(Column::from_tds).collect();
        let name_map: HashMap<String, usize> = columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
        Arc::new(RowSchema { columns, name_map })
    }
}

/// A single result row with tiberius-style typed column access.
///
/// String values are eagerly decoded from UTF-16 to UTF-8 at construction
/// time, enabling `row.get::<&str, _>("col")` without allocation.
#[derive(Debug, Clone)]
pub struct Row {
    schema: Arc<RowSchema>,
    values: Vec<ColumnValues>,
    /// Pre-decoded UTF-8 strings for &str borrowing support.
    decoded_strings: Vec<Option<String>>,
}

impl Row {
    /// Build a Row reusing a pre-built [`RowSchema`]. Hot path for the
    /// streaming and buffered code paths — clones the `Arc`, never the
    /// underlying `Vec<Column>`/`HashMap`.
    pub fn from_schema(schema: Arc<RowSchema>, values: Vec<ColumnValues>) -> Self {
        let decoded_strings: Vec<Option<String>> = values
            .iter()
            .map(|v| match v {
                ColumnValues::String(s) => Some(s.to_utf8_string()),
                ColumnValues::Xml(x) => Some(x.as_string()),
                ColumnValues::Json(j) => Some(j.as_string()),
                _ => None,
            })
            .collect();
        Row {
            schema,
            values,
            decoded_strings,
        }
    }

    /// Build a Row from mssql-tds column metadata and decoded values.
    ///
    /// Convenience wrapper that builds a fresh [`RowSchema`] each call —
    /// prefer [`Row::from_schema`] inside row loops where the schema is
    /// known to be constant for the result set.
    pub fn from_tds(metadata: &[ColumnMetadata], values: Vec<ColumnValues>) -> Self {
        Self::from_schema(RowSchema::from_metadata(metadata), values)
    }

    /// Column metadata for this row.
    pub fn columns(&self) -> &[Column] {
        &self.schema.columns
    }

    /// Number of columns.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Whether the row has zero columns.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Get a column value by name or index. Returns `None` if the column is NULL
    /// or the type doesn't match. Panics if the column doesn't exist.
    pub fn get<'a, T: FromSql<'a>, I: ColumnIndex>(&'a self, col: I) -> Option<T> {
        self.try_get(col).expect("column not found")
    }

    /// Try to get a column value by name or index, returning a `Result`.
    pub fn try_get<'a, T: FromSql<'a>, I: ColumnIndex>(&'a self, col: I) -> Result<Option<T>> {
        let idx = col.resolve(self)?;
        self.try_get_at(idx)
    }

    /// Get a column value by name using case-insensitive lookup. Returns `None`
    /// if the column is NULL or the type doesn't match. Panics if the column
    /// doesn't exist.
    pub fn get_ci<'a, T: FromSql<'a>>(&'a self, name: &str) -> Option<T> {
        self.try_get_ci(name).expect("column not found")
    }

    /// Try to get a column value by name using case-insensitive lookup.
    pub fn try_get_ci<'a, T: FromSql<'a>>(&'a self, name: &str) -> Result<Option<T>> {
        let idx = self.resolve_name_ci(name)?;
        self.try_get_at(idx)
    }

    fn try_get_at<'a, T: FromSql<'a>>(&'a self, idx: usize) -> Result<Option<T>> {
        let value = self.values.get(idx).ok_or(Error::ColumnIndexOutOfBounds {
            index: idx,
            count: self.values.len(),
        })?;
        Ok(T::from_sql_with_str(
            value,
            self.decoded_strings.get(idx).and_then(|s| s.as_deref()),
        ))
    }

    fn resolve_name_ci(&self, name: &str) -> Result<usize> {
        if let Some(idx) = self.column_index(name) {
            return Ok(idx);
        }
        self.schema
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| Error::ColumnNotFound(name.to_string()))
    }

    /// Raw access to the underlying ColumnValues at a given index.
    pub fn raw_value(&self, idx: usize) -> Option<&ColumnValues> {
        self.values.get(idx)
    }

    /// Pre-decoded UTF-8 string at the given index, if any. Used by the
    /// `serde` deserializer.
    #[cfg(feature = "serde")]
    #[inline]
    pub(crate) fn decoded_str_at(&self, idx: usize) -> Option<&str> {
        self.decoded_strings.get(idx).and_then(|s| s.as_deref())
    }

    /// Deserialize this row into a `T` using `serde`. Consumes the row.
    ///
    /// Requires the `serde` cargo feature. See the
    /// [`serde_de`](crate::serde_de) module for the field/type mapping rules.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "serde")]
    /// # async fn ex(client: &mut mssql_tiberius_bridge::Client)
    /// #     -> mssql_tiberius_bridge::Result<()> {
    /// use serde::Deserialize;
    /// use mssql_tiberius_bridge::Row;
    ///
    /// #[derive(Deserialize)]
    /// struct User { id: i64, name: String }
    ///
    /// let users: Vec<User> = client.query("SELECT id, name FROM users", &[])
    ///     .await?
    ///     .into_first_result()
    ///     .into_iter()
    ///     .map(Row::deserialize)
    ///     .collect::<mssql_tiberius_bridge::Result<Vec<_>>>()?;
    /// # Ok(()) }
    /// ```
    #[cfg(feature = "serde")]
    pub fn deserialize<T: serde::de::DeserializeOwned>(self) -> Result<T> {
        T::deserialize(crate::serde_de::RowDeserializer::new(&self))
    }

    /// Borrowed-deserialize variant: the resulting value can borrow `&str`
    /// and `&[u8]` cells directly out of this row.
    ///
    /// Requires the `serde` cargo feature.
    #[cfg(feature = "serde")]
    pub fn deserialize_borrowed<'de, T: serde::Deserialize<'de>>(&'de self) -> Result<T> {
        T::deserialize(crate::serde_de::RowDeserializer::new(self))
    }

    /// Look up a column index by name (case-sensitive).
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.schema.name_map.get(name).copied()
    }
}

/// Equality on `Row` compares **column types and cell values**. Names and
/// other server-side metadata (nullability, identity, computed, collation,
/// …) are intentionally ignored so that rows produced from different result
/// sets — or built by tests with placeholder names — can be asserted equal
/// when the data shape matches.
///
/// `Row` does **not** implement `Eq` because [`ColumnValues`] holds floats
/// (`Float`/`Real`), which only satisfy `PartialEq`. Comparisons involving
/// `NaN` follow IEEE-754 semantics: `NaN != NaN`.
///
/// Mirrors the request in prisma/tiberius#402.
///
/// # Example
///
/// ```rust,no_run
/// # fn example(actual: mssql_tiberius_bridge::Row, expected: mssql_tiberius_bridge::Row) {
/// assert_eq!(actual, expected);
/// # }
/// ```
impl PartialEq for Row {
    fn eq(&self, other: &Self) -> bool {
        // Cheap pointer-equality fast path for rows that share an Arc<RowSchema>
        // (the common case inside a single result set).
        let schemas_match = Arc::ptr_eq(&self.schema, &other.schema)
            || (self.schema.columns.len() == other.schema.columns.len()
                && self
                    .schema
                    .columns
                    .iter()
                    .zip(other.schema.columns.iter())
                    .all(|(a, b)| a.column_type == b.column_type));

        schemas_match && self.values == other.values
    }
}

// ---------------------------------------------------------------------------
// ColumnIndex — resolve &str or usize to a positional index
// ---------------------------------------------------------------------------

/// Trait for types that can identify a column in a row (by name or index).
pub trait ColumnIndex {
    fn resolve(&self, row: &Row) -> Result<usize>;
}

impl ColumnIndex for usize {
    fn resolve(&self, row: &Row) -> Result<usize> {
        if *self >= row.values.len() {
            return Err(Error::ColumnIndexOutOfBounds {
                index: *self,
                count: row.values.len(),
            });
        }
        Ok(*self)
    }
}

impl ColumnIndex for &str {
    fn resolve(&self, row: &Row) -> Result<usize> {
        row.schema
            .name_map
            .get(*self)
            .copied()
            .ok_or_else(|| Error::ColumnNotFound(self.to_string()))
    }
}

// ---------------------------------------------------------------------------
// FromSql — extract a typed value from ColumnValues
// ---------------------------------------------------------------------------

/// Trait for extracting a Rust type from a `ColumnValues` cell.
pub trait FromSql<'a>: Sized {
    fn from_sql(val: &'a ColumnValues) -> Option<Self>;

    /// Extract with an optional pre-decoded string (used for &str borrowing).
    /// Default delegates to `from_sql`.
    fn from_sql_with_str(val: &'a ColumnValues, _decoded: Option<&'a str>) -> Option<Self> {
        Self::from_sql(val)
    }
}

// Option<T>: NULL → Some(None), value → Some(Some(v))
impl<'a, T: FromSql<'a>> FromSql<'a> for Option<T> {
    fn from_sql(val: &'a ColumnValues) -> Option<Self> {
        match val {
            ColumnValues::Null => Some(None),
            _ => Some(T::from_sql(val)),
        }
    }

    fn from_sql_with_str(val: &'a ColumnValues, decoded: Option<&'a str>) -> Option<Self> {
        match val {
            ColumnValues::Null => Some(None),
            _ => Some(T::from_sql_with_str(val, decoded)),
        }
    }
}

impl<'a> FromSql<'a> for bool {
    fn from_sql(val: &'a ColumnValues) -> Option<Self> {
        match val {
            ColumnValues::Bit(b) => Some(*b),
            _ => None,
        }
    }
}

impl<'a> FromSql<'a> for u8 {
    fn from_sql(val: &'a ColumnValues) -> Option<Self> {
        match val {
            ColumnValues::TinyInt(v) => Some(*v),
            _ => None,
        }
    }
}

impl<'a> FromSql<'a> for i16 {
    fn from_sql(val: &'a ColumnValues) -> Option<Self> {
        match val {
            ColumnValues::SmallInt(v) => Some(*v),
            ColumnValues::TinyInt(v) => Some(*v as i16),
            _ => None,
        }
    }
}

impl<'a> FromSql<'a> for i32 {
    fn from_sql(val: &'a ColumnValues) -> Option<Self> {
        match val {
            ColumnValues::Int(v) => Some(*v),
            ColumnValues::SmallInt(v) => Some(*v as i32),
            ColumnValues::TinyInt(v) => Some(*v as i32),
            _ => None,
        }
    }
}

impl<'a> FromSql<'a> for i64 {
    fn from_sql(val: &'a ColumnValues) -> Option<Self> {
        match val {
            ColumnValues::BigInt(v) => Some(*v),
            ColumnValues::Int(v) => Some(*v as i64),
            ColumnValues::SmallInt(v) => Some(*v as i64),
            ColumnValues::TinyInt(v) => Some(*v as i64),
            _ => None,
        }
    }
}

impl<'a> FromSql<'a> for f32 {
    fn from_sql(val: &'a ColumnValues) -> Option<Self> {
        match val {
            ColumnValues::Real(v) => Some(*v),
            _ => None,
        }
    }
}

impl<'a> FromSql<'a> for f64 {
    fn from_sql(val: &'a ColumnValues) -> Option<Self> {
        match val {
            ColumnValues::Float(v) => Some(*v),
            ColumnValues::Real(v) => Some(*v as f64),
            _ => None,
        }
    }
}

impl<'a> FromSql<'a> for String {
    fn from_sql(val: &'a ColumnValues) -> Option<Self> {
        match val {
            ColumnValues::String(s) => Some(s.to_utf8_string()),
            ColumnValues::Xml(x) => Some(x.as_string()),
            ColumnValues::Json(j) => Some(j.as_string()),
            _ => None,
        }
    }
}

impl<'a> FromSql<'a> for serde_json::Value {
    fn from_sql(val: &'a ColumnValues) -> Option<Self> {
        match val {
            ColumnValues::Json(j) => serde_json::from_str(&j.as_string()).ok(),
            ColumnValues::String(s) => serde_json::from_str(&s.to_utf8_string()).ok(),
            _ => None,
        }
    }
}

impl<'a> FromSql<'a> for &'a str {
    fn from_sql(_val: &'a ColumnValues) -> Option<Self> {
        // Can't borrow from ColumnValues directly (UTF-16 internal).
        // Use from_sql_with_str path via Row::get() instead.
        None
    }

    fn from_sql_with_str(_val: &'a ColumnValues, decoded: Option<&'a str>) -> Option<Self> {
        decoded
    }
}

impl<'a> FromSql<'a> for uuid::Uuid {
    fn from_sql(val: &'a ColumnValues) -> Option<Self> {
        match val {
            ColumnValues::Uuid(u) => Some(*u),
            _ => None,
        }
    }
}

impl<'a> FromSql<'a> for Vec<u8> {
    fn from_sql(val: &'a ColumnValues) -> Option<Self> {
        match val {
            ColumnValues::Bytes(b) => Some(b.clone()),
            _ => None,
        }
    }
}

impl<'a> FromSql<'a> for &'a [u8] {
    fn from_sql(val: &'a ColumnValues) -> Option<Self> {
        match val {
            ColumnValues::Bytes(b) => Some(b.as_slice()),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Chrono conversions
// ---------------------------------------------------------------------------

impl<'a> FromSql<'a> for chrono::NaiveDate {
    fn from_sql(val: &'a ColumnValues) -> Option<Self> {
        match val {
            ColumnValues::Date(d) => {
                chrono::NaiveDate::from_num_days_from_ce_opt(d.get_days() as i32 + 1)
            }
            ColumnValues::DateTime2(dt) => {
                chrono::NaiveDate::from_num_days_from_ce_opt(dt.days as i32 + 1)
            }
            ColumnValues::DateTime(dt) => {
                // datetime: days since 1900-01-01
                let base = chrono::NaiveDate::from_ymd_opt(1900, 1, 1)?;
                base.checked_add_signed(chrono::Duration::days(dt.days as i64))
            }
            _ => None,
        }
    }
}

impl<'a> FromSql<'a> for chrono::NaiveTime {
    fn from_sql(val: &'a ColumnValues) -> Option<Self> {
        match val {
            ColumnValues::Time(t) => {
                // time_nanoseconds is actually in 100-nanosecond units despite the field name
                let total_nanos = t.time_nanoseconds * 100;
                let secs = (total_nanos / 1_000_000_000) as u32;
                let nanos = (total_nanos % 1_000_000_000) as u32;
                chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
            }
            _ => None,
        }
    }
}

impl<'a> FromSql<'a> for chrono::NaiveDateTime {
    fn from_sql(val: &'a ColumnValues) -> Option<Self> {
        match val {
            ColumnValues::DateTime2(dt) => {
                let date = chrono::NaiveDate::from_num_days_from_ce_opt(dt.days as i32 + 1)?;
                // time_nanoseconds is in 100-nanosecond units
                let total_nanos = dt.time.time_nanoseconds * 100;
                let secs = (total_nanos / 1_000_000_000) as u32;
                let nanos = (total_nanos % 1_000_000_000) as u32;
                let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)?;
                Some(chrono::NaiveDateTime::new(date, time))
            }
            ColumnValues::DateTime(dt) => {
                let base = chrono::NaiveDate::from_ymd_opt(1900, 1, 1)?;
                let date = base.checked_add_signed(chrono::Duration::days(dt.days as i64))?;
                // datetime ticks: 1/300th of a second
                let total_ms = (dt.time as u64) * 10 / 3;
                let secs = (total_ms / 1000) as u32;
                let nanos = ((total_ms % 1000) * 1_000_000) as u32;
                let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)?;
                Some(chrono::NaiveDateTime::new(date, time))
            }
            ColumnValues::SmallDateTime(sdt) => {
                let base = chrono::NaiveDate::from_ymd_opt(1900, 1, 1)?;
                let date = base.checked_add_signed(chrono::Duration::days(sdt.days as i64))?;
                let time =
                    chrono::NaiveTime::from_num_seconds_from_midnight_opt(sdt.time as u32 * 60, 0)?;
                Some(chrono::NaiveDateTime::new(date, time))
            }
            _ => None,
        }
    }
}

impl<'a> FromSql<'a> for chrono::DateTime<chrono::FixedOffset> {
    fn from_sql(val: &'a ColumnValues) -> Option<Self> {
        match val {
            ColumnValues::DateTimeOffset(dto) => {
                let dt2 = &dto.datetime2;
                let date = chrono::NaiveDate::from_num_days_from_ce_opt(dt2.days as i32 + 1)?;
                // time_nanoseconds is in 100-nanosecond units
                let total_nanos = dt2.time.time_nanoseconds * 100;
                let secs = (total_nanos / 1_000_000_000) as u32;
                let nanos = (total_nanos % 1_000_000_000) as u32;
                let time = chrono::NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)?;
                let naive = chrono::NaiveDateTime::new(date, time);
                let offset = chrono::FixedOffset::east_opt(dto.offset as i32 * 60)?;
                Some(chrono::DateTime::from_naive_utc_and_offset(naive, offset))
            }
            _ => None,
        }
    }
}

#[cfg(feature = "time")]
mod time_from_sql {
    use super::{ColumnValues, FromSql};
    use chrono::{Datelike, Timelike};
    use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};

    fn date_from_chrono(date: chrono::NaiveDate) -> Option<Date> {
        Date::from_calendar_date(
            date.year(),
            Month::try_from(date.month() as u8).ok()?,
            date.day() as u8,
        )
        .ok()
    }

    fn time_from_chrono(time: chrono::NaiveTime) -> Option<Time> {
        Time::from_hms_nano(
            time.hour() as u8,
            time.minute() as u8,
            time.second() as u8,
            time.nanosecond(),
        )
        .ok()
    }

    impl<'a> FromSql<'a> for Date {
        fn from_sql(val: &'a ColumnValues) -> Option<Self> {
            date_from_chrono(chrono::NaiveDate::from_sql(val)?)
        }
    }

    impl<'a> FromSql<'a> for Time {
        fn from_sql(val: &'a ColumnValues) -> Option<Self> {
            time_from_chrono(chrono::NaiveTime::from_sql(val)?)
        }
    }

    impl<'a> FromSql<'a> for PrimitiveDateTime {
        fn from_sql(val: &'a ColumnValues) -> Option<Self> {
            let dt = chrono::NaiveDateTime::from_sql(val)?;
            Some(PrimitiveDateTime::new(
                date_from_chrono(dt.date())?,
                time_from_chrono(dt.time())?,
            ))
        }
    }

    impl<'a> FromSql<'a> for OffsetDateTime {
        fn from_sql(val: &'a ColumnValues) -> Option<Self> {
            let dt = chrono::DateTime::<chrono::FixedOffset>::from_sql(val)?;
            let offset = UtcOffset::from_whole_seconds(dt.offset().local_minus_utc()).ok()?;
            let utc = PrimitiveDateTime::new(
                date_from_chrono(dt.naive_utc().date())?,
                time_from_chrono(dt.naive_utc().time())?,
            )
            .assume_utc();
            Some(utc.to_offset(offset))
        }
    }
}

#[cfg(feature = "jiff")]
mod jiff_from_sql {
    use super::{ColumnValues, FromSql};
    use chrono::{Datelike, Timelike};
    use jiff::{civil, tz::Offset, Timestamp, Zoned};

    fn date_from_chrono(date: chrono::NaiveDate) -> Option<civil::Date> {
        civil::Date::new(date.year() as i16, date.month() as i8, date.day() as i8).ok()
    }

    fn time_from_chrono(time: chrono::NaiveTime) -> Option<civil::Time> {
        civil::Time::new(
            time.hour() as i8,
            time.minute() as i8,
            time.second() as i8,
            time.nanosecond() as i32,
        )
        .ok()
    }

    fn datetime_from_chrono(dt: chrono::NaiveDateTime) -> Option<civil::DateTime> {
        civil::DateTime::new(
            dt.year() as i16,
            dt.month() as i8,
            dt.day() as i8,
            dt.hour() as i8,
            dt.minute() as i8,
            dt.second() as i8,
            dt.nanosecond() as i32,
        )
        .ok()
    }

    impl<'a> FromSql<'a> for civil::Date {
        fn from_sql(val: &'a ColumnValues) -> Option<Self> {
            date_from_chrono(chrono::NaiveDate::from_sql(val)?)
        }
    }

    impl<'a> FromSql<'a> for civil::Time {
        fn from_sql(val: &'a ColumnValues) -> Option<Self> {
            time_from_chrono(chrono::NaiveTime::from_sql(val)?)
        }
    }

    impl<'a> FromSql<'a> for civil::DateTime {
        fn from_sql(val: &'a ColumnValues) -> Option<Self> {
            datetime_from_chrono(chrono::NaiveDateTime::from_sql(val)?)
        }
    }

    impl<'a> FromSql<'a> for Timestamp {
        fn from_sql(val: &'a ColumnValues) -> Option<Self> {
            let dt = chrono::DateTime::<chrono::FixedOffset>::from_sql(val)?;
            Timestamp::new(dt.timestamp(), dt.timestamp_subsec_nanos() as i32).ok()
        }
    }

    impl<'a> FromSql<'a> for Zoned {
        fn from_sql(val: &'a ColumnValues) -> Option<Self> {
            let dt = chrono::DateTime::<chrono::FixedOffset>::from_sql(val)?;
            let timestamp =
                Timestamp::new(dt.timestamp(), dt.timestamp_subsec_nanos() as i32).ok()?;
            let offset = Offset::from_seconds(dt.offset().local_minus_utc()).ok()?;
            Some(timestamp.to_zoned(offset.to_time_zone()))
        }
    }
}

// ---------------------------------------------------------------------------
// Decimal
// ---------------------------------------------------------------------------

impl<'a> FromSql<'a> for rust_decimal::Decimal {
    fn from_sql(val: &'a ColumnValues) -> Option<Self> {
        match val {
            ColumnValues::Decimal(d) | ColumnValues::Numeric(d) => {
                // DecimalParts has Display impl; parse through string for safety
                d.to_string().parse::<rust_decimal::Decimal>().ok()
            }
            ColumnValues::SmallMoney(m) => Some(rust_decimal::Decimal::new(m.int_val as i64, 4)),
            ColumnValues::Money(m) => {
                let raw = ((m.msb_part as i64) << 32) | (m.lsb_part as u32 as i64);
                Some(rust_decimal::Decimal::new(raw, 4))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mssql_tds::datatypes::column_values::ColumnValues;
    use mssql_tds::datatypes::sql_string::SqlString;

    // Helper to build a Row without real metadata
    fn make_row(names: &[&str], values: Vec<ColumnValues>) -> Row {
        let columns: Vec<Column> = names
            .iter()
            .map(|n| Column::test_column(n, crate::column::ColumnType::Null, 0))
            .collect();
        let name_map: HashMap<String, usize> = columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
        let schema = Arc::new(RowSchema { columns, name_map });
        Row::from_schema(schema, values)
    }

    #[test]
    fn get_by_name() {
        let row = make_row(
            &["id", "name"],
            vec![
                ColumnValues::Int(42),
                ColumnValues::String(SqlString::from_utf8_string("hello".into())),
            ],
        );
        assert_eq!(row.get::<i32, _>("id"), Some(42));
        assert_eq!(row.get::<String, _>("name"), Some("hello".into()));
        // &str borrowing from pre-decoded cache
        assert_eq!(row.get::<&str, _>("name"), Some("hello"));
    }

    #[test]
    fn get_by_index() {
        let row = make_row(&["a"], vec![ColumnValues::BigInt(99)]);
        assert_eq!(row.get::<i64, _>(0usize), Some(99));
    }

    #[test]
    fn get_null() {
        let row = make_row(&["x"], vec![ColumnValues::Null]);
        assert_eq!(row.get::<Option<i32>, _>("x"), Some(None));
        assert_eq!(row.get::<i32, _>("x"), None);
    }

    #[test]
    fn try_get_missing_column() {
        let row = make_row(&["a"], vec![ColumnValues::Int(1)]);
        assert!(row.try_get::<i32, _>("nope").is_err());
    }

    #[test]
    fn get_bool() {
        let row = make_row(&["b"], vec![ColumnValues::Bit(true)]);
        assert_eq!(row.get::<bool, _>("b"), Some(true));
    }

    #[test]
    fn get_uuid() {
        let u = uuid::Uuid::new_v4();
        let row = make_row(&["id"], vec![ColumnValues::Uuid(u)]);
        assert_eq!(row.get::<uuid::Uuid, _>("id"), Some(u));
    }

    #[test]
    fn widening_int_conversions() {
        let row = make_row(&["v"], vec![ColumnValues::TinyInt(7)]);
        assert_eq!(row.get::<i16, _>("v"), Some(7));
        assert_eq!(row.get::<i32, _>("v"), Some(7));
        assert_eq!(row.get::<i64, _>("v"), Some(7));
    }

    #[test]
    fn float_widening() {
        let row = make_row(&["v"], vec![ColumnValues::Real(1.5)]);
        assert_eq!(row.get::<f64, _>("v"), Some(1.5));
    }

    #[test]
    fn bytes_extraction() {
        let row = make_row(&["b"], vec![ColumnValues::Bytes(vec![1, 2, 3])]);
        assert_eq!(row.get::<Vec<u8>, _>("b"), Some(vec![1, 2, 3]));
        assert_eq!(row.get::<&[u8], _>("b"), Some(&[1, 2, 3][..]));
    }

    #[test]
    fn case_insensitive_lookup() {
        let row = make_row(
            &["UserName", "id"],
            vec![
                ColumnValues::String(SqlString::from_utf8_string("ada".into())),
                ColumnValues::Int(7),
            ],
        );

        assert_eq!(row.try_get_ci::<&str>("username").unwrap(), Some("ada"));
        assert_eq!(row.get_ci::<String>("USERNAME"), Some("ada".into()));
        assert_eq!(row.try_get_ci::<i32>("ID").unwrap(), Some(7));
        assert!(matches!(
            row.try_get_ci::<i32>("missing"),
            Err(Error::ColumnNotFound(name)) if name == "missing"
        ));
    }

    #[test]
    fn try_get_errors_without_panicking() {
        let row = make_row(
            &["a"],
            vec![ColumnValues::String(SqlString::from_utf8_string(
                "x".into(),
            ))],
        );

        assert!(matches!(
            row.try_get::<i32, _>(1usize),
            Err(Error::ColumnIndexOutOfBounds { index: 1, count: 1 })
        ));
        assert!(matches!(
            row.try_get::<i32, _>("missing"),
            Err(Error::ColumnNotFound(name)) if name == "missing"
        ));
        assert_eq!(row.try_get::<i32, _>("a").unwrap(), None);
    }

    #[test]
    fn null_smallint_as_i32_returns_none_cleanly() {
        let columns = vec![Column::test_column(
            "small",
            crate::column::ColumnType::Int2,
            2,
        )];
        let name_map = HashMap::from([("small".to_string(), 0)]);
        let schema = Arc::new(RowSchema { columns, name_map });
        let row = Row::from_schema(schema, vec![ColumnValues::Null]);

        assert_eq!(row.try_get::<i32, _>("small").unwrap(), None);
        assert_eq!(row.try_get::<Option<i32>, _>("small").unwrap(), Some(None));
    }

    #[test]
    fn str_borrowing() {
        let row = make_row(
            &["s", "n"],
            vec![
                ColumnValues::String(SqlString::from_utf8_string("borrowed".into())),
                ColumnValues::Null,
            ],
        );
        // Can borrow &str from the row
        assert_eq!(row.get::<&str, _>("s"), Some("borrowed"));
        // NULL returns None
        assert_eq!(row.get::<&str, _>("n"), None);
        // Option<&str> works too
        assert_eq!(row.get::<Option<&str>, _>("s"), Some(Some("borrowed")));
        assert_eq!(row.get::<Option<&str>, _>("n"), Some(None));
    }

    #[test]
    fn str_non_string_column_returns_none() {
        let row = make_row(&["i"], vec![ColumnValues::Int(42)]);
        assert_eq!(row.get::<&str, _>("i"), None);
    }

    // ── PartialEq for Row (issue #65) ──

    #[test]
    fn eq_same_arc_schema_same_values() {
        let columns = vec![Column::test_column(
            "id",
            crate::column::ColumnType::Int4,
            4,
        )];
        let name_map: HashMap<String, usize> = [("id".to_string(), 0)].into();
        let schema = Arc::new(RowSchema { columns, name_map });

        let a = Row::from_schema(Arc::clone(&schema), vec![ColumnValues::Int(1)]);
        let b = Row::from_schema(schema, vec![ColumnValues::Int(1)]);
        assert_eq!(a, b);
    }

    #[test]
    fn eq_independent_schemas_with_matching_types_and_values() {
        let r1 = make_row(
            &["id", "name"],
            vec![
                ColumnValues::Int(42),
                ColumnValues::String(SqlString::from_utf8_string("hi".into())),
            ],
        );
        let r2 = make_row(
            &["different_id", "different_name"],
            vec![
                ColumnValues::Int(42),
                ColumnValues::String(SqlString::from_utf8_string("hi".into())),
            ],
        );
        assert_eq!(r1, r2, "names should not affect equality");
    }

    #[test]
    fn ne_different_values() {
        let a = make_row(&["v"], vec![ColumnValues::Int(1)]);
        let b = make_row(&["v"], vec![ColumnValues::Int(2)]);
        assert_ne!(a, b);
    }

    #[test]
    fn ne_different_arity() {
        let a = make_row(&["v"], vec![ColumnValues::Int(1)]);
        let b = make_row(
            &["a", "b"],
            vec![ColumnValues::Int(1), ColumnValues::Int(2)],
        );
        assert_ne!(a, b);
    }

    #[test]
    fn ne_different_column_types_same_arity() {
        // Build two rows where the value types coincide on the wire (both
        // are the bridge's "Int" carrier) but the schemas claim different
        // column types — equality should reject the mismatch.
        let cols_a = vec![Column::test_column("v", crate::column::ColumnType::Int4, 4)];
        let cols_b = vec![Column::test_column("v", crate::column::ColumnType::Int8, 8)];
        let schema_a = Arc::new(RowSchema {
            name_map: [("v".to_string(), 0)].into(),
            columns: cols_a,
        });
        let schema_b = Arc::new(RowSchema {
            name_map: [("v".to_string(), 0)].into(),
            columns: cols_b,
        });
        let a = Row::from_schema(schema_a, vec![ColumnValues::Int(1)]);
        let b = Row::from_schema(schema_b, vec![ColumnValues::Int(1)]);
        assert_ne!(a, b);
    }

    #[test]
    fn ne_nan_floats() {
        // PartialEq follows IEEE-754 — NaN is never equal to itself.
        let a = make_row(&["v"], vec![ColumnValues::Float(f64::NAN)]);
        let b = make_row(&["v"], vec![ColumnValues::Float(f64::NAN)]);
        assert_ne!(a, b);
    }

    #[test]
    fn eq_nulls() {
        let a = make_row(&["x"], vec![ColumnValues::Null]);
        let b = make_row(&["x"], vec![ColumnValues::Null]);
        assert_eq!(a, b);
    }
}
