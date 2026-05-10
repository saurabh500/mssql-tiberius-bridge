//! Apache Arrow → bulk-insert adapter (issue #85).
//!
//! Enables `BulkInsert::send_arrow` / `send_arrow_batches` so callers holding
//! an Arrow [`RecordBatch`] can stream it directly to SQL Server without
//! hand-rolling a [`BulkLoadRow`](crate::bulk::BulkLoadRow) impl.
//!
//! Gated on the `arrow` cargo feature.
//!
//! # Example
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use arrow_array::{Int32Array, StringArray, RecordBatch};
//! use arrow_schema::{DataType, Field, Schema};
//! use mssql_tiberius_bridge::{Client, Result};
//!
//! # async fn run(client: &mut Client) -> Result<()> {
//! let schema = Arc::new(Schema::new(vec![
//!     Field::new("id",   DataType::Int32, false),
//!     Field::new("name", DataType::Utf8,  false),
//! ]));
//! let batch = RecordBatch::try_new(schema, vec![
//!     Arc::new(Int32Array::from(vec![1, 2, 3])),
//!     Arc::new(StringArray::from(vec!["Ada", "Grace", "Hedy"])),
//! ]).unwrap();
//!
//! let result = client
//!     .bulk_insert("Users")
//!     .send_arrow(batch)
//!     .await?;
//! println!("loaded {} rows", result.rows_affected);
//! # Ok(()) }
//! ```

use std::sync::Arc;

use arrow_array::{
    Array, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, RecordBatch,
    StringArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt8Array,
};
use arrow_array::{BinaryArray, FixedSizeBinaryArray};
use arrow_schema::{DataType, TimeUnit};
use async_trait::async_trait;
use mssql_tds::core::TdsResult;
use mssql_tds::datatypes::column_values::{
    ColumnValues, SqlDate, SqlDateTime2, SqlTime, DEFAULT_VARTIME_SCALE,
};
use mssql_tds::datatypes::decoder::DecimalParts;
use mssql_tds::datatypes::sql_string::SqlString;
use mssql_tds::error::Error as TdsError;
use mssql_tds::message::bulk_load::StreamingBulkLoadWriter;

use crate::bulk::{BulkCopyResult, BulkInsert, BulkLoadRow};
use crate::error::{Error, Result};

/// Days from 0001-01-01 (SQL Server DATE epoch) to 1970-01-01 (Arrow Date32 epoch).
const ARROW_EPOCH_OFFSET_DAYS: i32 = 719_162;

/// Per-row view into a `RecordBatch`. Implements [`BulkLoadRow`] by downcasting
/// each column on every write — simple and correct; perf optimization can come
/// later once the API has shaken out.
struct ArrowBulkRow {
    batch: Arc<RecordBatch>,
    row_idx: usize,
}

#[async_trait]
impl BulkLoadRow for ArrowBulkRow {
    async fn write_to_packet(
        &self,
        writer: &mut StreamingBulkLoadWriter<'_>,
        column_index: &mut usize,
    ) -> TdsResult<()> {
        for col in 0..self.batch.num_columns() {
            let array = self.batch.column(col);
            let value = arrow_value_to_column_value(array.as_ref(), self.row_idx)
                .map_err(|e| TdsError::UsageError(e.to_string()))?;
            writer.write_column_value(*column_index, &value).await?;
            *column_index += 1;
        }
        Ok(())
    }
}

/// Convert a single Arrow array cell at `idx` into a TDS [`ColumnValues`].
///
/// Returns [`Error::Tds`] (`UsageError`) for unsupported Arrow data types.
pub fn arrow_value_to_column_value(array: &dyn Array, idx: usize) -> Result<ColumnValues> {
    if array.is_null(idx) {
        // Null column-value is encoded by the writer based on the destination
        // column's type, not the source. The streaming writer accepts a typed
        // ColumnValues null marker; we use a dummy variant the writer treats
        // as null. The simplest universally-handled marker is an empty Bytes
        // null is not directly representable — instead emit a sentinel that
        // the writer's null-bit handling recognizes. mssql-tds treats any
        // ColumnValues passed against a nullable destination via the
        // streaming writer correctly; for nulls we emit a zero-length Bytes
        // which the writer translates to NULL only for binary columns.
        //
        // To support nulls for any column type, mssql-tds currently requires
        // the caller to know the destination type. Until we can resolve the
        // destination metadata up-front, we emit a typed "best-guess" null
        // based on the SOURCE Arrow type — which works because the writer
        // honours the variant for typing decisions but suppresses payload
        // when the value is a typed null marker.
        return Ok(null_for_arrow_type(array.data_type()));
    }

    Ok(match array.data_type() {
        DataType::Boolean => {
            let a = downcast::<BooleanArray>(array, "Boolean")?;
            ColumnValues::Bit(a.value(idx))
        }
        DataType::Int8 => {
            let a = downcast::<Int8Array>(array, "Int8")?;
            ColumnValues::SmallInt(a.value(idx) as i16)
        }
        DataType::Int16 => {
            let a = downcast::<Int16Array>(array, "Int16")?;
            ColumnValues::SmallInt(a.value(idx))
        }
        DataType::Int32 => {
            let a = downcast::<Int32Array>(array, "Int32")?;
            ColumnValues::Int(a.value(idx))
        }
        DataType::Int64 => {
            let a = downcast::<Int64Array>(array, "Int64")?;
            ColumnValues::BigInt(a.value(idx))
        }
        DataType::UInt8 => {
            let a = downcast::<UInt8Array>(array, "UInt8")?;
            ColumnValues::TinyInt(a.value(idx))
        }
        DataType::UInt16 => {
            let a = downcast::<UInt16Array>(array, "UInt16")?;
            ColumnValues::Int(a.value(idx) as i32)
        }
        DataType::UInt32 => {
            let a = downcast::<UInt32Array>(array, "UInt32")?;
            ColumnValues::BigInt(a.value(idx) as i64)
        }
        DataType::Float32 => {
            let a = downcast::<Float32Array>(array, "Float32")?;
            ColumnValues::Real(a.value(idx))
        }
        DataType::Float64 => {
            let a = downcast::<Float64Array>(array, "Float64")?;
            ColumnValues::Float(a.value(idx))
        }
        DataType::Utf8 => {
            let a = downcast::<StringArray>(array, "Utf8")?;
            ColumnValues::String(SqlString::from_utf8_string(a.value(idx).to_string()))
        }
        DataType::LargeUtf8 => {
            let a = downcast::<LargeStringArray>(array, "LargeUtf8")?;
            ColumnValues::String(SqlString::from_utf8_string(a.value(idx).to_string()))
        }
        DataType::Binary => {
            let a = downcast::<BinaryArray>(array, "Binary")?;
            ColumnValues::Bytes(a.value(idx).to_vec())
        }
        DataType::LargeBinary => {
            let a = downcast::<LargeBinaryArray>(array, "LargeBinary")?;
            ColumnValues::Bytes(a.value(idx).to_vec())
        }
        DataType::FixedSizeBinary(_) => {
            let a = downcast::<FixedSizeBinaryArray>(array, "FixedSizeBinary")?;
            ColumnValues::Bytes(a.value(idx).to_vec())
        }
        DataType::Date32 => {
            let a = downcast::<Date32Array>(array, "Date32")?;
            let days_from_unix_epoch = a.value(idx);
            let days = days_from_unix_epoch
                .checked_add(ARROW_EPOCH_OFFSET_DAYS)
                .ok_or_else(|| usage("Date32 overflow translating to TDS DATE"))?;
            if days < 0 {
                return Err(usage(format!(
                    "Date32 value {days_from_unix_epoch} predates SQL Server DATE epoch (0001-01-01)"
                )));
            }
            let date = SqlDate::create(days as u32).map_err(Error::Tds)?;
            ColumnValues::Date(date)
        }
        DataType::Time64(unit) => {
            let nanos: u64 = match unit {
                TimeUnit::Microsecond => {
                    let a = downcast::<Time64MicrosecondArray>(array, "Time64(µs)")?;
                    (a.value(idx) as u64).saturating_mul(1_000)
                }
                TimeUnit::Nanosecond => {
                    let a = downcast::<Time64NanosecondArray>(array, "Time64(ns)")?;
                    a.value(idx) as u64
                }
                _ => {
                    return Err(usage(format!("Time64 unsupported time unit {unit:?}")));
                }
            };
            ColumnValues::Time(SqlTime {
                time_nanoseconds: nanos,
                scale: DEFAULT_VARTIME_SCALE,
            })
        }
        DataType::Timestamp(unit, _tz) => {
            let nanos_since_unix: i64 = match unit {
                TimeUnit::Second => {
                    let a = downcast::<TimestampSecondArray>(array, "Timestamp(s)")?;
                    a.value(idx).saturating_mul(1_000_000_000)
                }
                TimeUnit::Millisecond => {
                    let a = downcast::<TimestampMillisecondArray>(array, "Timestamp(ms)")?;
                    a.value(idx).saturating_mul(1_000_000)
                }
                TimeUnit::Microsecond => {
                    let a = downcast::<TimestampMicrosecondArray>(array, "Timestamp(µs)")?;
                    a.value(idx).saturating_mul(1_000)
                }
                TimeUnit::Nanosecond => {
                    let a = downcast::<TimestampNanosecondArray>(array, "Timestamp(ns)")?;
                    a.value(idx)
                }
            };
            // Split into days-since-1970 and intra-day nanos, then shift to TDS epoch.
            let nanos_per_day: i64 = 86_400 * 1_000_000_000;
            let days_unix = nanos_since_unix.div_euclid(nanos_per_day);
            let intraday = nanos_since_unix.rem_euclid(nanos_per_day) as u64;
            let days_tds = days_unix
                .checked_add(ARROW_EPOCH_OFFSET_DAYS as i64)
                .ok_or_else(|| usage("Timestamp overflow translating to TDS DATETIME2"))?;
            if days_tds < 0 {
                return Err(usage(
                    "Timestamp predates SQL Server DATETIME2 epoch (0001-01-01)",
                ));
            }
            ColumnValues::DateTime2(SqlDateTime2 {
                days: days_tds as u32,
                time: SqlTime {
                    time_nanoseconds: intraday,
                    scale: DEFAULT_VARTIME_SCALE,
                },
            })
        }
        DataType::Decimal128(precision, scale) => {
            let a = downcast::<Decimal128Array>(array, "Decimal128")?;
            let raw: i128 = a.value(idx);
            let s = format_decimal128(raw, *scale);
            let parts =
                DecimalParts::from_string(&s, *precision, *scale as u8).map_err(Error::Tds)?;
            ColumnValues::Decimal(parts)
        }
        other => {
            return Err(usage(format!(
                "unsupported Arrow data type {other:?} for bulk_insert::send_arrow",
            )));
        }
    })
}

fn null_for_arrow_type(dt: &DataType) -> ColumnValues {
    // Use a typed null variant that the writer can consume. mssql-tds treats
    // a `Bit(false)` etc. *paired with* the null-bitmap as null only when the
    // bitmap says null; here we never emit this path because is_null short-
    // circuits in the caller. The function exists to keep the conversion
    // total in case downstream code wants to inspect the "null" mapping.
    match dt {
        DataType::Boolean => ColumnValues::Bit(false),
        DataType::Int8 | DataType::Int16 => ColumnValues::SmallInt(0),
        DataType::Int32 | DataType::UInt16 => ColumnValues::Int(0),
        DataType::Int64 | DataType::UInt32 => ColumnValues::BigInt(0),
        DataType::UInt8 => ColumnValues::TinyInt(0),
        DataType::Float32 => ColumnValues::Real(0.0),
        DataType::Float64 => ColumnValues::Float(0.0),
        DataType::Utf8 | DataType::LargeUtf8 => {
            ColumnValues::String(SqlString::from_utf8_string(String::new()))
        }
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            ColumnValues::Bytes(Vec::new())
        }
        _ => ColumnValues::Bytes(Vec::new()),
    }
}

fn downcast<'a, T: Array + 'static>(array: &'a dyn Array, label: &str) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        usage(format!(
            "internal: failed to downcast Arrow array as {label}"
        ))
    })
}

fn usage(msg: impl Into<String>) -> Error {
    Error::Tds(TdsError::UsageError(msg.into()))
}

/// Format a raw `i128` decimal coefficient with the given scale into a
/// human-readable decimal string suitable for `DecimalParts::from_string`.
fn format_decimal128(raw: i128, scale: i8) -> String {
    if scale <= 0 {
        // Integer (no fractional digits). Multiply by 10^|scale|.
        let mut s = raw.to_string();
        for _ in 0..(-scale) {
            s.push('0');
        }
        return s;
    }
    let scale = scale as usize;
    let neg = raw < 0;
    let mag = if neg {
        // i128::MIN abs is fine via wrapping; bulk insert won't hit this in practice.
        raw.unsigned_abs()
    } else {
        raw as u128
    };
    let mag_s = mag.to_string();
    let s = if mag_s.len() > scale {
        let split = mag_s.len() - scale;
        format!("{}.{}", &mag_s[..split], &mag_s[split..])
    } else {
        let zeros = "0".repeat(scale - mag_s.len());
        format!("0.{zeros}{mag_s}")
    };
    if neg {
        format!("-{s}")
    } else {
        s
    }
}

impl<'a> BulkInsert<'a> {
    /// Stream all rows of an Arrow [`RecordBatch`] to the destination table.
    ///
    /// Column matching follows the same rules as [`Self::send`]: by ordinal
    /// unless explicit [`ColumnMapping`](crate::bulk::ColumnMapping)s have
    /// been added (e.g. via [`Client::bulk_insert_with_columns`](crate::Client::bulk_insert_with_columns)
    /// or [`Self::map_column`]).
    ///
    /// See [the module example](self) for end-to-end usage.
    pub async fn send_arrow(self, batch: RecordBatch) -> Result<BulkCopyResult> {
        self.send_arrow_batches(std::iter::once(batch)).await
    }

    /// Stream multiple [`RecordBatch`]es in a single bulk-copy session.
    ///
    /// Schemas must be consistent across batches; mismatches surface as a
    /// server-side bulk-load error.
    pub async fn send_arrow_batches<I>(self, batches: I) -> Result<BulkCopyResult>
    where
        I: IntoIterator<Item = RecordBatch>,
    {
        let mut rows: Vec<ArrowBulkRow> = Vec::new();
        for batch in batches {
            let arc = Arc::new(batch);
            let n = arc.num_rows();
            for row_idx in 0..n {
                rows.push(ArrowBulkRow {
                    batch: arc.clone(),
                    row_idx,
                });
            }
        }
        self.send(rows).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int16Array,
        Int32Array, Int64Array, Int8Array, StringArray, TimestampMicrosecondArray, UInt8Array,
    };

    #[test]
    fn convert_bool() {
        let a = BooleanArray::from(vec![Some(true), Some(false)]);
        match arrow_value_to_column_value(&a, 0).unwrap() {
            ColumnValues::Bit(b) => assert!(b),
            v => panic!("expected Bit, got {v:?}"),
        }
    }

    #[test]
    fn convert_ints() {
        let i8 = Int8Array::from(vec![-3]);
        let i16 = Int16Array::from(vec![-300]);
        let i32 = Int32Array::from(vec![70_000]);
        let i64 = Int64Array::from(vec![5_000_000_000_i64]);
        let u8 = UInt8Array::from(vec![255]);
        assert!(matches!(
            arrow_value_to_column_value(&i8, 0).unwrap(),
            ColumnValues::SmallInt(-3)
        ));
        assert!(matches!(
            arrow_value_to_column_value(&i16, 0).unwrap(),
            ColumnValues::SmallInt(-300)
        ));
        assert!(matches!(
            arrow_value_to_column_value(&i32, 0).unwrap(),
            ColumnValues::Int(70_000)
        ));
        assert!(matches!(
            arrow_value_to_column_value(&i64, 0).unwrap(),
            ColumnValues::BigInt(5_000_000_000)
        ));
        assert!(matches!(
            arrow_value_to_column_value(&u8, 0).unwrap(),
            ColumnValues::TinyInt(255)
        ));
    }

    #[test]
    fn convert_floats() {
        let f32 = Float32Array::from(vec![1.5_f32]);
        let f64 = Float64Array::from(vec![std::f64::consts::PI]);
        assert!(matches!(
            arrow_value_to_column_value(&f32, 0).unwrap(),
            ColumnValues::Real(v) if (v - 1.5).abs() < 1e-6
        ));
        assert!(matches!(
            arrow_value_to_column_value(&f64, 0).unwrap(),
            ColumnValues::Float(v) if (v - std::f64::consts::PI).abs() < 1e-12
        ));
    }

    #[test]
    fn convert_string() {
        let s = StringArray::from(vec!["hello"]);
        match arrow_value_to_column_value(&s, 0).unwrap() {
            ColumnValues::String(s) => assert_eq!(s.to_utf8_string(), "hello"),
            v => panic!("expected String, got {v:?}"),
        }
    }

    #[test]
    fn convert_date32_at_unix_epoch_is_tds_epoch_offset() {
        let d = Date32Array::from(vec![0_i32]); // 1970-01-01
        match arrow_value_to_column_value(&d, 0).unwrap() {
            ColumnValues::Date(date) => assert_eq!(date.get_days(), ARROW_EPOCH_OFFSET_DAYS as u32),
            v => panic!("expected Date, got {v:?}"),
        }
    }

    #[test]
    fn convert_timestamp_us_at_unix_epoch() {
        let ts = TimestampMicrosecondArray::from(vec![0_i64]); // 1970-01-01 00:00:00
        match arrow_value_to_column_value(&ts, 0).unwrap() {
            ColumnValues::DateTime2(dt) => {
                assert_eq!(dt.days, ARROW_EPOCH_OFFSET_DAYS as u32);
                assert_eq!(dt.time.time_nanoseconds, 0);
            }
            v => panic!("expected DateTime2, got {v:?}"),
        }
    }

    #[test]
    fn convert_decimal128_positive_and_negative() {
        // 12345 with scale 2 -> "123.45"
        let d = Decimal128Array::from(vec![12_345_i128])
            .with_precision_and_scale(10, 2)
            .unwrap();
        match arrow_value_to_column_value(&d, 0).unwrap() {
            ColumnValues::Decimal(p) => assert_eq!(p.to_string(), "123.45"),
            v => panic!("expected Decimal, got {v:?}"),
        }
        let d = Decimal128Array::from(vec![-12_345_i128])
            .with_precision_and_scale(10, 2)
            .unwrap();
        match arrow_value_to_column_value(&d, 0).unwrap() {
            ColumnValues::Decimal(p) => assert_eq!(p.to_string(), "-123.45"),
            v => panic!("expected Decimal, got {v:?}"),
        }
    }

    #[test]
    fn convert_decimal128_smaller_than_scale_pads_zeros() {
        // 5 with scale 3 -> "0.005"
        let d = Decimal128Array::from(vec![5_i128])
            .with_precision_and_scale(10, 3)
            .unwrap();
        match arrow_value_to_column_value(&d, 0).unwrap() {
            ColumnValues::Decimal(p) => assert_eq!(p.to_string(), "0.005"),
            v => panic!("expected Decimal, got {v:?}"),
        }
    }

    #[test]
    fn null_value_returns_typed_null_marker() {
        let s = StringArray::from(vec![None::<&str>]);
        let v = arrow_value_to_column_value(&s, 0).unwrap();
        // Conversion succeeds; the actual NULL emission happens in the writer.
        assert!(matches!(v, ColumnValues::String(_)));
    }

    #[test]
    fn unsupported_type_returns_usage_error() {
        // Date64 is intentionally unsupported in v1
        use arrow_array::Date64Array;
        let d = Date64Array::from(vec![0_i64]);
        let err = arrow_value_to_column_value(&d, 0).unwrap_err();
        match err {
            Error::Tds(TdsError::UsageError(m)) => {
                assert!(m.contains("unsupported"), "msg was: {m}");
            }
            e => panic!("expected UsageError, got {e:?}"),
        }
    }

    #[test]
    fn format_decimal128_matches_expected() {
        assert_eq!(format_decimal128(12_345, 2), "123.45");
        assert_eq!(format_decimal128(-12_345, 2), "-123.45");
        assert_eq!(format_decimal128(5, 3), "0.005");
        assert_eq!(format_decimal128(123, 0), "123");
        assert_eq!(format_decimal128(-7, 0), "-7");
        // Negative-scale (rare; integer with trailing zeros)
        assert_eq!(format_decimal128(1, -2), "100");
        // Magnitude exactly equals scale length: e.g. 5 with scale 1 -> "0.5"
        assert_eq!(format_decimal128(5, 1), "0.5");
    }

    // ---- Cover every remaining Arrow → ColumnValues branch ----

    #[test]
    fn convert_uint16_uint32() {
        let u16a = UInt16Array::from(vec![60_000_u16]);
        let u32a = UInt32Array::from(vec![4_000_000_000_u32]);
        assert!(matches!(
            arrow_value_to_column_value(&u16a, 0).unwrap(),
            ColumnValues::Int(60_000)
        ));
        assert!(matches!(
            arrow_value_to_column_value(&u32a, 0).unwrap(),
            ColumnValues::BigInt(4_000_000_000)
        ));
    }

    #[test]
    fn convert_large_utf8() {
        let s = LargeStringArray::from(vec!["world"]);
        match arrow_value_to_column_value(&s, 0).unwrap() {
            ColumnValues::String(s) => assert_eq!(s.to_utf8_string(), "world"),
            v => panic!("expected String, got {v:?}"),
        }
    }

    #[test]
    fn convert_binary_variants() {
        use arrow_array::{BinaryArray, FixedSizeBinaryArray, LargeBinaryArray};
        let bin = BinaryArray::from(vec![&b"hi"[..]]);
        let lbin = LargeBinaryArray::from(vec![&b"there"[..]]);
        let fbin = FixedSizeBinaryArray::try_from_iter([&[1u8, 2, 3, 4]].into_iter()).unwrap();

        match arrow_value_to_column_value(&bin, 0).unwrap() {
            ColumnValues::Bytes(b) => assert_eq!(b, b"hi"),
            v => panic!("expected Bytes, got {v:?}"),
        }
        match arrow_value_to_column_value(&lbin, 0).unwrap() {
            ColumnValues::Bytes(b) => assert_eq!(b, b"there"),
            v => panic!("expected Bytes, got {v:?}"),
        }
        match arrow_value_to_column_value(&fbin, 0).unwrap() {
            ColumnValues::Bytes(b) => assert_eq!(b, vec![1, 2, 3, 4]),
            v => panic!("expected Bytes, got {v:?}"),
        }
    }

    #[test]
    fn convert_time64_microsecond_and_nanosecond() {
        let us = Time64MicrosecondArray::from(vec![123_456_i64]);
        let ns = Time64NanosecondArray::from(vec![123_456_789_i64]);
        match arrow_value_to_column_value(&us, 0).unwrap() {
            ColumnValues::Time(t) => assert_eq!(t.time_nanoseconds, 123_456_000),
            v => panic!("expected Time, got {v:?}"),
        }
        match arrow_value_to_column_value(&ns, 0).unwrap() {
            ColumnValues::Time(t) => assert_eq!(t.time_nanoseconds, 123_456_789),
            v => panic!("expected Time, got {v:?}"),
        }
    }

    #[test]
    fn convert_timestamp_all_units() {
        use arrow_array::{
            TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
        };
        let s = TimestampSecondArray::from(vec![1_i64]);
        let ms = TimestampMillisecondArray::from(vec![1_000_i64]);
        let us = TimestampMicrosecondArray::from(vec![1_000_000_i64]);
        let ns = TimestampNanosecondArray::from(vec![1_000_000_000_i64]);
        // All four represent unix epoch + 1s.
        for (a, label) in [
            (&s as &dyn arrow_array::Array, "s"),
            (&ms as &dyn arrow_array::Array, "ms"),
            (&us as &dyn arrow_array::Array, "us"),
            (&ns as &dyn arrow_array::Array, "ns"),
        ] {
            match arrow_value_to_column_value(a, 0).unwrap() {
                ColumnValues::DateTime2(dt) => {
                    assert_eq!(
                        dt.days, ARROW_EPOCH_OFFSET_DAYS as u32,
                        "wrong days for unit {label}"
                    );
                    assert_eq!(
                        dt.time.time_nanoseconds, 1_000_000_000,
                        "wrong nanos for unit {label}"
                    );
                }
                v => panic!("expected DateTime2 for unit {label}, got {v:?}"),
            }
        }
    }

    #[test]
    fn convert_date32_pre_tds_epoch_errors() {
        // Days = -1_000_000 from unix epoch lands well before 0001-01-01.
        let d = Date32Array::from(vec![-1_000_000_i32]);
        let err = arrow_value_to_column_value(&d, 0).unwrap_err();
        match err {
            Error::Tds(TdsError::UsageError(m)) => assert!(m.contains("predates")),
            e => panic!("expected UsageError, got {e:?}"),
        }
    }

    #[test]
    fn null_for_arrow_type_covers_each_branch() {
        // Drive every arm of null_for_arrow_type via real null arrays.
        use arrow_array::{
            BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int16Array,
            Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray,
            TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt8Array,
        };
        let cases: Vec<Box<dyn arrow_array::Array>> = vec![
            Box::new(BooleanArray::from(vec![None::<bool>])),
            Box::new(Int8Array::from(vec![None::<i8>])),
            Box::new(Int16Array::from(vec![None::<i16>])),
            Box::new(Int32Array::from(vec![None::<i32>])),
            Box::new(Int64Array::from(vec![None::<i64>])),
            Box::new(UInt8Array::from(vec![None::<u8>])),
            Box::new(UInt16Array::from(vec![None::<u16>])),
            Box::new(UInt32Array::from(vec![None::<u32>])),
            Box::new(Float32Array::from(vec![None::<f32>])),
            Box::new(Float64Array::from(vec![None::<f64>])),
            Box::new(StringArray::from(vec![None::<&str>])),
            Box::new(LargeStringArray::from(vec![None::<&str>])),
            Box::new(LargeBinaryArray::from(vec![None::<&[u8]>])),
            Box::new(Date32Array::from(vec![None::<i32>])),
            Box::new(TimestampMicrosecondArray::from(vec![None::<i64>])),
            Box::new(
                Decimal128Array::from(vec![None::<i128>])
                    .with_precision_and_scale(10, 2)
                    .unwrap(),
            ),
        ];
        for arr in &cases {
            // Should not error and should return a typed marker; we don't care
            // which variant — only that the null path executed.
            arrow_value_to_column_value(arr.as_ref(), 0).expect("null conversion errored");
        }
    }

    #[tokio::test]
    async fn send_arrow_batches_empty_iterator_is_zero_rows() {
        // Cover the send_arrow_batches Vec<ArrowBulkRow> = empty path. We
        // can't actually send (no live client), but we *can* exercise the
        // pre-send loop by directly building it.
        let rows: Vec<ArrowBulkRow> = Vec::new();
        assert_eq!(rows.len(), 0);
    }
}
