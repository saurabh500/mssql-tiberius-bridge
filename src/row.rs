//! Row type with named and indexed column access, mirroring tiberius' Row API.

use std::collections::HashMap;

use mssql_tds::datatypes::column_values::ColumnValues;
use mssql_tds::query::metadata::ColumnMetadata;

use crate::column::Column;
use crate::error::{Error, Result};

/// A single result row with tiberius-style `get`/`try_get` access by name or index.
#[derive(Debug, Clone)]
pub struct Row {
    columns: Vec<Column>,
    values: Vec<ColumnValues>,
    name_map: HashMap<String, usize>,
}

impl Row {
    /// Build a Row from mssql-tds column metadata and decoded values.
    pub fn from_tds(metadata: &[ColumnMetadata], values: Vec<ColumnValues>) -> Self {
        let columns: Vec<Column> = metadata.iter().map(Column::from_tds).collect();
        let name_map: HashMap<String, usize> = columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
        Row {
            columns,
            values,
            name_map,
        }
    }

    /// Column metadata for this row.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Number of columns.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Whether the row has zero columns.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Get a column value by name. Returns `None` if the column is NULL or
    /// the type doesn't match. Panics if the column name doesn't exist.
    pub fn get<'a, T: FromSql<'a>, I: ColumnIndex>(&'a self, col: I) -> Option<T> {
        let idx = col.resolve(self).expect("column not found");
        T::from_sql(&self.values[idx])
    }

    /// Try to get a column value by name or index, returning a `Result`.
    pub fn try_get<'a, T: FromSql<'a>, I: ColumnIndex>(&'a self, col: I) -> Result<Option<T>> {
        let idx = col.resolve(self)?;
        if idx >= self.values.len() {
            return Err(Error::ColumnIndexOutOfBounds {
                index: idx,
                count: self.values.len(),
            });
        }
        Ok(T::from_sql(&self.values[idx]))
    }

    /// Raw access to the underlying ColumnValues at a given index.
    pub fn raw_value(&self, idx: usize) -> Option<&ColumnValues> {
        self.values.get(idx)
    }

    /// Look up a column index by name (case-sensitive).
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.name_map.get(name).copied()
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
        row.name_map
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
}

// Option<T>: NULL → Some(None), value → Some(Some(v))
impl<'a, T: FromSql<'a>> FromSql<'a> for Option<T> {
    fn from_sql(val: &'a ColumnValues) -> Option<Self> {
        match val {
            ColumnValues::Null => Some(None),
            _ => Some(T::from_sql(val)),
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
            _ => None,
        }
    }
}

impl<'a> FromSql<'a> for &'a str {
    fn from_sql(_val: &'a ColumnValues) -> Option<Self> {
        // mssql-tds SqlString doesn't expose &str directly (it stores UTF-16
        // internally), so borrowing is not possible. Use String instead.
        None
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
                let total_nanos = t.time_nanoseconds;
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
                let total_nanos = dt.time.time_nanoseconds;
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
                let total_nanos = dt2.time.time_nanoseconds;
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
            .map(|n| Column {
                name: n.to_string(),
                column_type: crate::column::ColumnType::Null,
                nullable: true,
            })
            .collect();
        let name_map: HashMap<String, usize> = columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
        Row {
            columns,
            values,
            name_map,
        }
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
    }
}
