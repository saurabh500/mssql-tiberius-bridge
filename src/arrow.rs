//! SQL Server to Arrow record batches, available with the `arrow` feature.
//!
//! Strings (including XML/JSON) use `LargeUtf8`, opaque binary/UDT values use
//! `LargeBinary`, and uniqueidentifiers use canonical lower-case, hyphenated
//! `Utf8`. Decimal/numeric and money use `Decimal128` without floating-point
//! intermediates. Float32 vectors use `FixedSizeList<Float32>`; Float16 vectors,
//! SQL_VARIANT, encrypted columns, and unrecognized types are rejected.
//!
//! Temporal representations are lossless across SQL Server's full date range:
//! `date` is `Date32`, `time` is `Time64(ns)`, and `datetime2`/`smalldatetime` are
//! `Struct{date: Date32, time: Time64(ns)}`. `datetimeoffset` adds
//! `offset_minutes: Int16`: its date/time children preserve the **UTC wire
//! value**, not the offset-adjusted local clock. `datetime` is
//! `Struct{date: Date32, ticks_300: UInt32}`, retaining exact 1/300-second ticks.
//! Child fields are non-nullable; a SQL NULL is represented by the parent
//! struct/list validity bitmap.
//!
//! Field metadata uses `mssql.type` (SQL name), `mssql.tds_type` (wire type),
//! `mssql.length`, `mssql.user_type`, and, when applicable, `mssql.precision`,
//! `mssql.scale`, `mssql.collation.{info,lcid,flags,sort_id}`,
//! `mssql.udt.{database,schema,name,assembly_qualified_name,max_byte_size}`,
//! `mssql.vector.{dimensions,base_type}`, and `mssql.time_basis`.
//!
//! UTF-16 and LCID strings use encoding_rs replacement decoding and BOM handling,
//! consistently for borrowed and owned input. LCIDs use the upstream resolver,
//! including its warning and Windows-1252 fallback for unmapped LCIDs. Invalid
//! UTF-8 and unresolved encodings return conversion errors rather than panicking.

use std::borrow::Cow;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, FixedSizeListBuilder,
    Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, LargeBinaryBuilder,
    LargeStringBuilder, NullBuilder, StringBuilder, StructBuilder, Time64NanosecondBuilder,
    UInt32Builder, UInt8Builder,
};
use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use mssql_tds::datatypes::column_values::{
    SqlDate, SqlDateTime, SqlDateTime2, SqlDateTimeOffset, SqlMoney, SqlSmallDateTime,
    SqlSmallMoney, SqlTime, SqlXml,
};
use mssql_tds::datatypes::decoder::DecimalParts;
use mssql_tds::datatypes::row_writer::RowWriter;
use mssql_tds::datatypes::sql_json::SqlJson;
use mssql_tds::datatypes::sql_string::EncodingType;
use mssql_tds::datatypes::sql_vector::{SqlVector, VectorData};
use mssql_tds::datatypes::sqldatatypes::{TdsDataType, VectorBaseType, VECTOR_HEADER_SIZE};
use mssql_tds::encoding_rs::CoderResult;
use mssql_tds::query::metadata::ColumnMetadata;
use uuid::Uuid;

use crate::{Error, Result};

const DEFAULT_ROWS: usize = 8192;
const DEFAULT_BYTES: usize = 8 * 1024 * 1024;
const SQL_EPOCH_DAYS: i32 = 719_162;
const SQL_1900_EPOCH_DAYS: i32 = 25_567;
const MAX_SQL_DAYS: u32 = 3_652_058;
const TICKS_PER_DAY: u64 = 864_000_000_000;

/// Batch targets and optional rejection limits for Arrow reads.
///
/// Byte accounting estimates converted logical data: fixed-width storage
/// (including null slots), one byte per validity slot, and one offset per
/// variable-width value, plus its UTF-8/binary payload. Struct/list children
/// are included. This deliberately conservative estimate is not allocated
/// memory or process RSS: it excludes schema, allocator rounding, decoding
/// scratch, retained batches, and the upstream driver's wire buffers.
#[derive(Debug, Clone)]
pub struct ArrowOptions {
    /// Maximum rows per batch (default: 8192).
    pub batch_size: usize,
    /// Soft logical-byte target (default: 8 MiB). A row can exceed this target.
    pub batch_bytes: usize,
    /// Optional hard limit on one converted row, including accounting overhead.
    pub max_row_bytes: Option<usize>,
    /// Optional hard limit on an individual value's converted payload length
    /// (excluding validity/offset overhead). String, binary, and vector values
    /// also have their raw wire payload lengths checked when available.
    ///
    /// Checked before bridge copying when possible, but not before upstream
    /// TDS buffering. `None` imposes no additional application limit.
    pub max_value_bytes: Option<usize>,
}

impl Default for ArrowOptions {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_ROWS,
            batch_bytes: DEFAULT_BYTES,
            max_row_bytes: None,
            max_value_bytes: None,
        }
    }
}

impl ArrowOptions {
    pub(crate) fn validate(&self) -> Result<()> {
        for (name, value) in [
            ("batch_size", Some(self.batch_size)),
            ("batch_bytes", Some(self.batch_bytes)),
            ("max_row_bytes", self.max_row_bytes),
            ("max_value_bytes", self.max_value_bytes),
        ] {
            if let Some(value) = value {
                if value == 0 {
                    return Err(conversion(format!(
                        "Arrow {name} must be greater than zero"
                    )));
                }
                if value > isize::MAX as usize {
                    return Err(conversion(format!(
                        "Arrow {name} exceeds the addressable Arrow buffer size"
                    )));
                }
            }
        }
        Ok(())
    }
}

/// One batch, tagged with its zero-based result-set index.
#[derive(Debug, Clone)]
pub struct ArrowBatch {
    /// Index among result sets with column metadata, including empty sets.
    pub result_index: usize,
    /// An owned batch, valid independently of the stream and later batches.
    pub batch: RecordBatch,
}

/// A borrowing, asynchronous stream of owned Arrow batches.
pub type ArrowStream<'a> =
    Pin<Box<dyn futures_core::Stream<Item = Result<ArrowBatch>> + Send + 'a>>;

fn conversion(message: impl Into<String>) -> Error {
    Error::Conversion(message.into())
}

fn checked_add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b)
        .filter(|n| *n <= isize::MAX as usize)
        .ok_or_else(|| conversion("Arrow logical byte size exceeds addressable buffer capacity"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Null,
    Bool,
    U8,
    I16,
    I32,
    I64,
    F32,
    F64,
    Text,
    Binary,
    Xml,
    Json,
    Uuid,
    Decimal {
        precision: u8,
        scale: u8,
        numeric: bool,
    },
    Money,
    SmallMoney,
    Date,
    Time(u8),
    DateTime,
    SmallDateTime,
    DateTime2(u8),
    DateTimeOffset(u8),
    Vector(i32),
}

impl Kind {
    fn data_type(self) -> DataType {
        match self {
            Self::Null => DataType::Null,
            Self::Bool => DataType::Boolean,
            Self::U8 => DataType::UInt8,
            Self::I16 => DataType::Int16,
            Self::I32 => DataType::Int32,
            Self::I64 => DataType::Int64,
            Self::F32 => DataType::Float32,
            Self::F64 => DataType::Float64,
            Self::Text | Self::Xml | Self::Json => DataType::LargeUtf8,
            Self::Binary => DataType::LargeBinary,
            Self::Uuid => DataType::Utf8,
            Self::Decimal {
                precision, scale, ..
            } => DataType::Decimal128(precision, scale as i8),
            Self::Money => DataType::Decimal128(19, 4),
            Self::SmallMoney => DataType::Decimal128(10, 4),
            Self::Date => DataType::Date32,
            Self::Time(_) => DataType::Time64(TimeUnit::Nanosecond),
            Self::DateTime | Self::SmallDateTime | Self::DateTime2(_) | Self::DateTimeOffset(_) => {
                DataType::Struct(self.temporal_fields())
            }
            Self::Vector(dimensions) => DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, false)),
                dimensions,
            ),
        }
    }

    fn temporal_fields(self) -> Fields {
        let mut fields = vec![Field::new("date", DataType::Date32, false)];
        if self == Self::DateTime {
            fields.push(Field::new("ticks_300", DataType::UInt32, false));
        } else {
            fields.push(Field::new(
                "time",
                DataType::Time64(TimeUnit::Nanosecond),
                false,
            ));
        }
        if matches!(self, Self::DateTimeOffset(_)) {
            fields.push(Field::new("offset_minutes", DataType::Int16, false));
        }
        fields.into()
    }

    fn fixed_payload(self) -> usize {
        match self {
            Self::Null | Self::Text | Self::Binary | Self::Xml | Self::Json => 0,
            Self::Bool | Self::U8 => 1,
            Self::I16 => 2,
            Self::I32 | Self::F32 | Self::Date => 4,
            Self::I64 | Self::F64 | Self::Time(_) | Self::DateTime => 8,
            Self::SmallDateTime | Self::DateTime2(_) => 12,
            Self::DateTimeOffset(_) => 14,
            Self::Money | Self::SmallMoney | Self::Decimal { .. } => 16,
            Self::Uuid => 36,
            Self::Vector(n) => n as usize * 4,
        }
    }

    fn overhead(self) -> usize {
        match self {
            Self::Text | Self::Binary | Self::Xml | Self::Json => 9,
            Self::Uuid => 5,
            Self::DateTime | Self::SmallDateTime | Self::DateTime2(_) => 3,
            Self::DateTimeOffset(_) => 4,
            Self::Vector(n) => 1 + n as usize,
            _ => 1,
        }
    }

    fn initial_payload(self) -> usize {
        match self {
            Self::Text | Self::Binary | Self::Xml | Self::Json => 32,
            _ => self.fixed_payload(),
        }
    }
}

fn column_schema(meta: &ColumnMetadata) -> Result<(Kind, Field)> {
    let invalid = |message: &str| {
        conversion(format!(
            "Arrow column {:?} ({:?}): {message}",
            meta.column_name, meta.data_type
        ))
    };
    if meta.is_encrypted() {
        return Err(invalid(
            "encrypted columns are unsupported; select an unencrypted projection",
        ));
    }
    if meta.data_type != meta.type_info.tds_type {
        return Err(invalid("inconsistent TDS type metadata"));
    }
    let width = meta.type_info.length;
    let require_width = |expected: usize| -> Result<()> {
        if width != expected {
            return Err(invalid("invalid TDS storage width"));
        }
        Ok(())
    };
    let temporal_scale = || -> Result<u8> {
        let scale = meta
            .get_scale()
            .filter(|s| *s <= 7)
            .ok_or_else(|| invalid("missing or invalid temporal scale"))?;
        let time_width = match scale {
            0..=2 => 3,
            3..=4 => 4,
            _ => 5,
        };
        let suffix = match meta.data_type {
            TdsDataType::DateTime2N => 3,
            TdsDataType::DateTimeOffsetN => 5,
            _ => 0,
        };
        require_width(time_width + suffix)?;
        Ok(scale)
    };
    use TdsDataType as T;
    let (kind, sql_name) = match meta.data_type {
        T::Void => (Kind::Null, "null"),
        T::Bit | T::BitN => {
            require_width(1)?;
            (Kind::Bool, "bit")
        }
        T::Int1 => {
            require_width(1)?;
            (Kind::U8, "tinyint")
        }
        T::Int2 => {
            require_width(2)?;
            (Kind::I16, "smallint")
        }
        T::Int4 => {
            require_width(4)?;
            (Kind::I32, "int")
        }
        T::Int8 => {
            require_width(8)?;
            (Kind::I64, "bigint")
        }
        T::IntN => match width {
            1 => (Kind::U8, "tinyint"),
            2 => (Kind::I16, "smallint"),
            4 => (Kind::I32, "int"),
            8 => (Kind::I64, "bigint"),
            _ => return Err(invalid("invalid nullable integer width")),
        },
        T::Flt4 => {
            require_width(4)?;
            (Kind::F32, "real")
        }
        T::Flt8 => {
            require_width(8)?;
            (Kind::F64, "float")
        }
        T::FltN => match width {
            4 => (Kind::F32, "real"),
            8 => (Kind::F64, "float"),
            _ => return Err(invalid("invalid nullable float width")),
        },
        T::Money => {
            require_width(8)?;
            (Kind::Money, "money")
        }
        T::Money4 => {
            require_width(4)?;
            (Kind::SmallMoney, "smallmoney")
        }
        T::MoneyN => match width {
            4 => (Kind::SmallMoney, "smallmoney"),
            8 => (Kind::Money, "money"),
            _ => return Err(invalid("invalid nullable money width")),
        },
        T::Decimal | T::DecimalN | T::Numeric | T::NumericN => {
            let precision = meta
                .get_precision()
                .filter(|p| (1..=38).contains(p))
                .ok_or_else(|| invalid("decimal precision must be in 1..=38"))?;
            let scale = meta
                .get_scale()
                .filter(|s| *s <= precision)
                .ok_or_else(|| invalid("decimal scale must be in 0..=precision"))?;
            // SQL Server can advertise 17-byte TDS decimals even for small
            // precisions; the table-storage size formula is not a wire contract.
            if !matches!(width, 5 | 9 | 13 | 17) {
                return Err(invalid("invalid decimal TDS storage width"));
            }
            let numeric = matches!(meta.data_type, T::Numeric | T::NumericN);
            (
                Kind::Decimal {
                    precision,
                    scale,
                    numeric,
                },
                if numeric { "numeric" } else { "decimal" },
            )
        }
        T::VarChar | T::BigVarChar => (Kind::Text, "varchar"),
        T::Char | T::BigChar => (Kind::Text, "char"),
        T::NVarChar => (Kind::Text, "nvarchar"),
        T::NChar => (Kind::Text, "nchar"),
        T::Text => (Kind::Text, "text"),
        T::NText => (Kind::Text, "ntext"),
        T::Xml => (Kind::Xml, "xml"),
        T::Json => (Kind::Json, "json"),
        T::Binary | T::BigBinary => (Kind::Binary, "binary"),
        T::VarBinary | T::BigVarBinary => (Kind::Binary, "varbinary"),
        T::Image => (Kind::Binary, "image"),
        T::Udt => (Kind::Binary, "udt"),
        T::Guid => {
            require_width(16)?;
            (Kind::Uuid, "uniqueidentifier")
        }
        T::DateN => {
            require_width(3)?;
            (Kind::Date, "date")
        }
        T::TimeN => (Kind::Time(temporal_scale()?), "time"),
        T::DateTime2N => (Kind::DateTime2(temporal_scale()?), "datetime2"),
        T::DateTimeOffsetN => (Kind::DateTimeOffset(temporal_scale()?), "datetimeoffset"),
        T::DateTime => {
            require_width(8)?;
            (Kind::DateTime, "datetime")
        }
        T::DateTim4 => {
            require_width(4)?;
            (Kind::SmallDateTime, "smalldatetime")
        }
        T::DateTimeN => match width {
            4 => (Kind::SmallDateTime, "smalldatetime"),
            8 => (Kind::DateTime, "datetime"),
            _ => return Err(invalid("invalid nullable datetime width")),
        },
        T::Vector => {
            let base = meta
                .get_scale()
                .and_then(|b| VectorBaseType::try_from(b).ok())
                .ok_or_else(|| {
                    invalid("missing or unknown vector base type; CAST to varchar(max)")
                })?;
            if base != VectorBaseType::Float32 {
                return Err(invalid(
                    "Float16 vectors are unsupported; CAST to vector(n, float32) or varchar(max)",
                ));
            }
            let bytes = width
                .checked_sub(VECTOR_HEADER_SIZE)
                .filter(|n| *n > 0 && *n % 4 == 0)
                .ok_or_else(|| invalid("invalid vector storage width"))?;
            let dimensions = bytes / 4;
            if dimensions > base.max_dimensions() as usize {
                return Err(invalid("invalid vector dimensions"));
            }
            (Kind::Vector(dimensions as i32), "vector")
        }
        T::SsVariant => {
            return Err(invalid(
                "sql_variant is unsupported; CAST to a concrete supported SQL type",
            ))
        }
        _ => {
            return Err(invalid(
                "unsupported SQL type; CAST to a concrete supported SQL type",
            ))
        }
    };
    let mut metadata = HashMap::from([
        ("mssql.type".to_owned(), sql_name.to_owned()),
        ("mssql.tds_type".to_owned(), format!("{:?}", meta.data_type)),
        ("mssql.length".to_owned(), width.to_string()),
        ("mssql.user_type".to_owned(), meta.user_type.to_string()),
    ]);
    let (precision, scale) = match kind {
        Kind::Money => (Some(19), Some(4)),
        Kind::SmallMoney => (Some(10), Some(4)),
        Kind::Vector(_) => (None, None),
        _ => (meta.get_precision(), meta.get_scale()),
    };
    if let Some(value) = precision {
        metadata.insert("mssql.precision".into(), value.to_string());
    }
    if let Some(value) = scale {
        metadata.insert("mssql.scale".into(), value.to_string());
    }
    if let Some(value) = meta.get_collation() {
        for (key, value) in [
            ("info", value.info.to_string()),
            ("lcid", value.lcid_language_id.to_string()),
            ("flags", value.col_flags.to_string()),
            ("sort_id", value.sort_id.to_string()),
        ] {
            metadata.insert(format!("mssql.collation.{key}"), value);
        }
    }
    if let Some(value) = meta.type_info.udt_info() {
        for (key, value) in [
            ("database", value.db_name()),
            ("schema", value.schema_name()),
            ("name", value.type_name()),
            ("assembly_qualified_name", value.assembly_qualified_name()),
        ] {
            metadata.insert(format!("mssql.udt.{key}"), value.to_owned());
        }
        metadata.insert(
            "mssql.udt.max_byte_size".into(),
            value.max_byte_size().to_string(),
        );
        if value.schema_name().eq_ignore_ascii_case("sys") {
            for name in ["geography", "geometry"] {
                if value.type_name().eq_ignore_ascii_case(name) {
                    metadata.insert("mssql.type".into(), name.into());
                }
            }
        }
    }
    if let Kind::Vector(dimensions) = kind {
        metadata.insert("mssql.vector.dimensions".into(), dimensions.to_string());
        metadata.insert("mssql.vector.base_type".into(), "float32".into());
    }
    if matches!(kind, Kind::DateTimeOffset(_)) {
        metadata.insert("mssql.time_basis".into(), "UTC".into());
    }
    Ok((
        kind,
        Field::new(&meta.column_name, kind.data_type(), meta.is_nullable()).with_metadata(metadata),
    ))
}

enum ColumnBuilder {
    Null(NullBuilder),
    Bool(BooleanBuilder),
    U8(UInt8Builder),
    I16(Int16Builder),
    I32(Int32Builder),
    I64(Int64Builder),
    F32(Float32Builder),
    F64(Float64Builder),
    Text(LargeStringBuilder),
    Binary(LargeBinaryBuilder),
    Uuid(StringBuilder),
    Decimal(Decimal128Builder),
    Date(Date32Builder),
    Time(Time64NanosecondBuilder),
    Temporal(StructBuilder),
    Vector(FixedSizeListBuilder<Float32Builder>),
}

impl ColumnBuilder {
    fn new(kind: Kind, rows: usize) -> Result<Self> {
        Ok(match kind {
            Kind::Null => Self::Null(NullBuilder::new()),
            Kind::Bool => Self::Bool(BooleanBuilder::with_capacity(rows)),
            Kind::U8 => Self::U8(UInt8Builder::with_capacity(rows)),
            Kind::I16 => Self::I16(Int16Builder::with_capacity(rows)),
            Kind::I32 => Self::I32(Int32Builder::with_capacity(rows)),
            Kind::I64 => Self::I64(Int64Builder::with_capacity(rows)),
            Kind::F32 => Self::F32(Float32Builder::with_capacity(rows)),
            Kind::F64 => Self::F64(Float64Builder::with_capacity(rows)),
            Kind::Text | Kind::Xml | Kind::Json => {
                Self::Text(LargeStringBuilder::with_capacity(rows, rows * 32))
            }
            Kind::Binary => Self::Binary(LargeBinaryBuilder::with_capacity(rows, rows * 32)),
            Kind::Uuid => Self::Uuid(StringBuilder::with_capacity(rows, rows * 36)),
            Kind::Decimal {
                precision, scale, ..
            } => Self::Decimal(
                Decimal128Builder::with_capacity(rows)
                    .with_precision_and_scale(precision, scale as i8)
                    .map_err(|e| conversion(e.to_string()))?,
            ),
            Kind::Money | Kind::SmallMoney => Self::Decimal(
                Decimal128Builder::with_capacity(rows)
                    .with_precision_and_scale(if kind == Kind::Money { 19 } else { 10 }, 4)
                    .map_err(|e| conversion(e.to_string()))?,
            ),
            Kind::Date => Self::Date(Date32Builder::with_capacity(rows)),
            Kind::Time(_) => Self::Time(Time64NanosecondBuilder::with_capacity(rows)),
            Kind::DateTime | Kind::SmallDateTime | Kind::DateTime2(_) | Kind::DateTimeOffset(_) => {
                Self::Temporal(StructBuilder::from_fields(kind.temporal_fields(), rows))
            }
            Kind::Vector(dimensions) => Self::Vector(
                FixedSizeListBuilder::with_capacity(
                    Float32Builder::with_capacity(rows * dimensions as usize),
                    dimensions,
                    rows,
                )
                .with_field(Arc::new(Field::new("item", DataType::Float32, false))),
            ),
        })
    }

    fn append_null(&mut self, kind: Kind) -> Result<()> {
        match self {
            Self::Null(b) => b.append_null(),
            Self::Bool(b) => b.append_null(),
            Self::U8(b) => b.append_null(),
            Self::I16(b) => b.append_null(),
            Self::I32(b) => b.append_null(),
            Self::I64(b) => b.append_null(),
            Self::F32(b) => b.append_null(),
            Self::F64(b) => b.append_null(),
            Self::Text(b) => b.append_null(),
            Self::Binary(b) => b.append_null(),
            Self::Uuid(b) => b.append_null(),
            Self::Decimal(b) => b.append_null(),
            Self::Date(b) => b.append_null(),
            Self::Time(b) => b.append_null(),
            Self::Temporal(b) => append_temporal(b, kind, 0, 0, 0, false)?,
            Self::Vector(b) => {
                if let Kind::Vector(dimensions) = kind {
                    b.values().append_value_n(0.0, dimensions as usize);
                    b.append(false);
                } else {
                    return Err(conversion("Arrow vector builder type mismatch"));
                }
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Null(b) => Arc::new(b.finish()),
            Self::Bool(b) => Arc::new(b.finish()),
            Self::U8(b) => Arc::new(b.finish()),
            Self::I16(b) => Arc::new(b.finish()),
            Self::I32(b) => Arc::new(b.finish()),
            Self::I64(b) => Arc::new(b.finish()),
            Self::F32(b) => Arc::new(b.finish()),
            Self::F64(b) => Arc::new(b.finish()),
            Self::Text(b) => Arc::new(b.finish()),
            Self::Binary(b) => Arc::new(b.finish()),
            Self::Uuid(b) => Arc::new(b.finish()),
            Self::Decimal(b) => Arc::new(b.finish()),
            Self::Date(b) => Arc::new(b.finish()),
            Self::Time(b) => Arc::new(b.finish()),
            Self::Temporal(b) => Arc::new(b.finish()),
            Self::Vector(b) => Arc::new(b.finish()),
        }
    }
}

fn child<T: ArrayBuilder>(builder: &mut StructBuilder, index: usize) -> Result<&mut T> {
    builder
        .field_builder::<T>(index)
        .ok_or_else(|| conversion("Arrow temporal child type mismatch"))
}

fn append_temporal(
    builder: &mut StructBuilder,
    kind: Kind,
    date: i32,
    time: i64,
    offset: i16,
    valid: bool,
) -> Result<()> {
    child::<Date32Builder>(builder, 0)?.append_value(date);
    if kind == Kind::DateTime {
        child::<UInt32Builder>(builder, 1)?.append_value(time as u32);
    } else {
        child::<Time64NanosecondBuilder>(builder, 1)?.append_value(time);
    }
    if matches!(kind, Kind::DateTimeOffset(_)) {
        child::<Int16Builder>(builder, 2)?.append_value(offset);
    }
    builder.append(valid);
    Ok(())
}

pub(crate) struct ArrowRowWriter {
    schema: Arc<Schema>,
    kinds: Vec<Kind>,
    columns: Vec<ColumnBuilder>,
    options: ArrowOptions,
    initial_rows: usize,
    rows: usize,
    bytes: usize,
    next_col: usize,
    row_bytes: usize,
    last_row_bytes: usize,
    scratch: String,
    error: Option<Error>,
    failed: bool,
}

impl ArrowRowWriter {
    pub(crate) fn new(metadata: &[ColumnMetadata], options: ArrowOptions) -> Result<Self> {
        options.validate()?;
        let mut kinds = Vec::with_capacity(metadata.len());
        let mut fields = Vec::with_capacity(metadata.len());
        let mut estimated_row = 0;
        for meta in metadata {
            let (kind, field) = column_schema(meta)?;
            estimated_row = checked_add(estimated_row, kind.initial_payload() + kind.overhead())?;
            kinds.push(kind);
            fields.push(field);
        }
        // Large configured batch counts never trigger correspondingly large eager allocations.
        let initial_rows = options
            .batch_size
            .min(DEFAULT_ROWS)
            .min(options.batch_bytes.min(DEFAULT_BYTES) / estimated_row.max(1));
        let columns = kinds
            .iter()
            .map(|kind| ColumnBuilder::new(*kind, initial_rows))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            schema: Arc::new(Schema::new(fields)),
            kinds,
            columns,
            options,
            initial_rows,
            rows: 0,
            bytes: 0,
            next_col: 0,
            row_bytes: 0,
            last_row_bytes: 0,
            scratch: String::new(),
            error: None,
            failed: false,
        })
    }

    pub(crate) fn check_error(&mut self) -> Result<()> {
        match self.error.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub(crate) fn row_count(&self) -> usize {
        self.rows
    }
    pub(crate) fn last_row_bytes(&self) -> usize {
        self.last_row_bytes
    }
    pub(crate) fn should_flush(&self) -> bool {
        self.rows >= self.options.batch_size || self.bytes >= self.options.batch_bytes
    }

    pub(crate) fn take_batch(&mut self) -> Result<RecordBatch> {
        self.check_error()?;
        if self.failed {
            return Err(conversion(
                "Arrow writer is unusable after a conversion error",
            ));
        }
        if self.next_col != 0 {
            self.failed = true;
            return Err(conversion("Cannot publish an unfinished Arrow row"));
        }
        let arrays = self.columns.iter_mut().map(ColumnBuilder::finish).collect();
        let batch = RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(self.rows)),
        )
        .map_err(|e| conversion(e.to_string()));
        let batch = match batch {
            Ok(batch) => batch,
            Err(error) => {
                self.failed = true;
                return Err(error);
            }
        };
        for (builder, kind) in self.columns.iter_mut().zip(&self.kinds) {
            *builder = ColumnBuilder::new(*kind, self.initial_rows)?;
        }
        self.rows = 0;
        self.bytes = 0;
        self.row_bytes = 0;
        self.last_row_bytes = 0;
        Ok(batch)
    }

    fn fail(&mut self, error: Error) {
        if !self.failed {
            self.error = Some(error);
            self.failed = true;
        }
    }

    fn kind(&self, col: usize) -> Result<Kind> {
        if col != self.next_col {
            return Err(conversion(format!(
                "Arrow callback column {col}, expected {}",
                self.next_col
            )));
        }
        self.kinds.get(col).copied().ok_or_else(|| {
            conversion(format!(
                "Arrow callback column {col} is outside the result schema"
            ))
        })
    }

    fn value_limit(&self, size: usize) -> Result<()> {
        if size > isize::MAX as usize
            || self
                .options
                .max_value_bytes
                .is_some_and(|limit| size > limit)
        {
            return Err(conversion(format!(
                "Arrow value payload of {size} bytes exceeds max_value_bytes or buffer capacity"
            )));
        }
        Ok(())
    }

    fn prospective_bytes(&self, kind: Kind, payload: usize) -> Result<usize> {
        self.value_limit(payload)?;
        let row_bytes = checked_add(self.row_bytes, checked_add(payload, kind.overhead())?)?;
        if self
            .options
            .max_row_bytes
            .is_some_and(|limit| row_bytes > limit)
        {
            return Err(conversion(format!(
                "Arrow row estimate of {row_bytes} bytes exceeds max_row_bytes"
            )));
        }
        checked_add(self.bytes, row_bytes)?;
        Ok(row_bytes)
    }

    fn append(
        &mut self,
        col: usize,
        expected: Kind,
        payload: usize,
        write: impl FnOnce(&mut ColumnBuilder) -> Result<()>,
    ) {
        if self.failed {
            return;
        }
        let outcome = (|| {
            let actual = self.kind(col)?;
            if actual != expected {
                return Err(conversion(format!(
                    "Arrow callback type {expected:?} does not match column {col} ({actual:?})"
                )));
            }
            let row_bytes = self.prospective_bytes(actual, payload)?;
            let builder = self
                .columns
                .get_mut(col)
                .ok_or_else(|| conversion("Missing Arrow column builder"))?;
            write(builder)?;
            self.row_bytes = row_bytes;
            self.next_col += 1;
            Ok(())
        })();
        if let Err(error) = outcome {
            self.fail(error);
        }
    }

    fn text(&mut self, col: usize, bytes: &[u8], encoding: EncodingType, expected: Kind) {
        if self.failed {
            return;
        }
        let outcome = (|| {
            if self.kind(col)? != expected {
                return Err(conversion(
                    "Arrow string callback does not match column type",
                ));
            }
            self.value_limit(bytes.len())?;
            if encoding == EncodingType::Utf8 {
                let text = std::str::from_utf8(bytes)
                    .map_err(|e| conversion(format!("Invalid UTF-8 in Arrow column {col}: {e}")))?;
                let row_bytes = self.prospective_bytes(expected, text.len())?;
                match self.columns.get_mut(col) {
                    Some(ColumnBuilder::Text(builder)) => builder.append_value(text),
                    _ => return Err(conversion("Arrow string builder type mismatch")),
                }
                self.row_bytes = row_bytes;
            } else {
                let encoding = encoding
                    .encoding()
                    .ok_or_else(|| conversion("Unresolved string encoding in Arrow column"))?;
                let mut decoder = encoding.new_decoder();
                self.scratch.clear();
                if self.options.max_value_bytes.is_none() && self.options.max_row_bytes.is_none() {
                    let capacity = decoder
                        .max_utf8_buffer_length(bytes.len())
                        .ok_or_else(|| conversion("Arrow decoding capacity overflow"))?;
                    self.scratch.try_reserve(capacity).map_err(|e| {
                        conversion(format!("Cannot reserve Arrow decoding scratch: {e}"))
                    })?;
                    let (status, read, _) =
                        decoder.decode_to_string(bytes, &mut self.scratch, true);
                    if status != CoderResult::InputEmpty || read != bytes.len() {
                        return Err(conversion(
                            "Arrow string decoder exceeded its calculated capacity",
                        ));
                    }
                } else {
                    // Enforce exact converted limits before growing heap scratch, rather than
                    // rejecting valid text on a worst-case transcoding expansion estimate.
                    let mut input = bytes;
                    let mut buffer = [0u8; 4096];
                    loop {
                        let (status, read, written, _) =
                            decoder.decode_to_utf8(input, &mut buffer, true);
                        let size = checked_add(self.scratch.len(), written)?;
                        self.prospective_bytes(expected, size)?;
                        self.scratch.try_reserve(written).map_err(|e| {
                            conversion(format!("Cannot reserve Arrow decoding scratch: {e}"))
                        })?;
                        self.scratch.push_str(
                            std::str::from_utf8(&buffer[..written]).map_err(|e| {
                                conversion(format!("Invalid transcoder output: {e}"))
                            })?,
                        );
                        input = &input[read..];
                        if status == CoderResult::InputEmpty {
                            break;
                        }
                        if read == 0 && written == 0 {
                            return Err(conversion("Arrow string decoder made no progress"));
                        }
                    }
                }
                let row_bytes = self.prospective_bytes(expected, self.scratch.len())?;
                match self.columns.get_mut(col) {
                    Some(ColumnBuilder::Text(builder)) => builder.append_value(&self.scratch),
                    _ => return Err(conversion("Arrow string builder type mismatch")),
                }
                self.row_bytes = row_bytes;
            }
            self.next_col += 1;
            Ok(())
        })();
        if let Err(error) = outcome {
            self.fail(error);
        }
    }

    fn decimal(&mut self, col: usize, value: DecimalParts, numeric: bool) {
        if self.failed {
            return;
        }
        let kind = Kind::Decimal {
            precision: value.precision,
            scale: value.scale,
            numeric,
        };
        self.append(col, kind, 16, |builder| {
            let magnitude = value.magnitude();
            if !(1..=38).contains(&value.precision)
                || value.scale > value.precision
                || magnitude > i128::MAX as u128
                || magnitude >= 10u128.pow(value.precision as u32)
            {
                return Err(conversion(
                    "Decimal magnitude, precision, or scale is out of range",
                ));
            }
            let signed = if value.is_positive {
                magnitude as i128
            } else {
                -(magnitude as i128)
            };
            match builder {
                ColumnBuilder::Decimal(builder) => builder.append_value(signed),
                _ => return Err(conversion("Arrow decimal builder type mismatch")),
            }
            Ok(())
        });
    }

    fn temporal(&mut self, col: usize, kind: Kind, value: Result<(i32, i64, i16)>) {
        if self.failed {
            return;
        }
        match value {
            Ok((date, time, offset)) => {
                self.append(col, kind, kind.fixed_payload(), |builder| match builder {
                    ColumnBuilder::Temporal(builder) => {
                        append_temporal(builder, kind, date, time, offset, true)
                    }
                    _ => Err(conversion("Arrow temporal builder type mismatch")),
                })
            }
            Err(error) => self.fail(error),
        }
    }
}

fn date32(days: u32) -> Result<i32> {
    if days > MAX_SQL_DAYS {
        return Err(conversion("Date is outside SQL Server's 0001..9999 range"));
    }
    Ok(days as i32 - SQL_EPOCH_DAYS)
}

fn time_ns(value: &SqlTime) -> Result<i64> {
    // Despite its public field name, mssql-tds 0.1 decoders return 100ns ticks.
    if value.scale > 7 || value.time_nanoseconds >= TICKS_PER_DAY {
        return Err(conversion(
            "Time scale or clock value is outside SQL Server's range",
        ));
    }
    if !value
        .time_nanoseconds
        .is_multiple_of(10u64.pow((7 - value.scale) as u32))
    {
        return Err(conversion(
            "Time value is not aligned to its declared scale",
        ));
    }
    Ok((value.time_nanoseconds * 100) as i64)
}

macro_rules! scalar_callback {
    ($method:ident, $ty:ty, $kind:ident, $builder:ident, $size:expr) => {
        fn $method(&mut self, col: usize, value: $ty) {
            self.append(col, Kind::$kind, $size, |builder| {
                match builder {
                    ColumnBuilder::$builder(builder) => builder.append_value(value),
                    _ => return Err(conversion("Arrow scalar builder type mismatch")),
                }
                Ok(())
            });
        }
    };
}

impl RowWriter for ArrowRowWriter {
    scalar_callback!(write_bool, bool, Bool, Bool, 1);
    scalar_callback!(write_u8, u8, U8, U8, 1);
    scalar_callback!(write_i16, i16, I16, I16, 2);
    scalar_callback!(write_i32, i32, I32, I32, 4);
    scalar_callback!(write_i64, i64, I64, I64, 8);
    scalar_callback!(write_f32, f32, F32, F32, 4);
    scalar_callback!(write_f64, f64, F64, F64, 8);

    fn write_null(&mut self, col: usize) {
        if self.failed {
            return;
        }
        match self.kind(col) {
            Ok(kind) => {
                if !self.schema.field(col).is_nullable() && kind != Kind::Null {
                    self.fail(conversion(format!(
                        "NULL in non-nullable Arrow column {col}"
                    )));
                    return;
                }
                // Null UUIDs have no text payload; other fixed-width nulls retain their slots.
                let payload = if kind == Kind::Uuid {
                    0
                } else {
                    kind.fixed_payload()
                };
                self.append(col, kind, payload, |builder| builder.append_null(kind));
            }
            Err(error) => self.fail(error),
        }
    }

    fn write_string(&mut self, col: usize, value: Cow<'_, [u8]>, encoding: EncodingType) {
        self.text(col, &value, encoding, Kind::Text);
    }

    fn write_xml(&mut self, col: usize, value: SqlXml) {
        self.text(col, &value.bytes, EncodingType::Utf16, Kind::Xml);
    }

    fn write_json(&mut self, col: usize, value: SqlJson) {
        self.text(col, &value.bytes, EncodingType::Utf8, Kind::Json);
    }

    fn write_bytes(&mut self, col: usize, value: Cow<'_, [u8]>) {
        self.append(col, Kind::Binary, value.len(), |builder| {
            match builder {
                ColumnBuilder::Binary(builder) => builder.append_value(&value),
                _ => return Err(conversion("Arrow binary builder type mismatch")),
            }
            Ok(())
        });
    }

    fn write_uuid(&mut self, col: usize, value: Uuid) {
        self.append(col, Kind::Uuid, 36, |builder| {
            match builder {
                ColumnBuilder::Uuid(builder) => {
                    if checked_add(builder.values_slice().len(), 36)? > i32::MAX as usize {
                        return Err(conversion("UUID text exceeds Arrow Utf8 offset capacity; use a smaller batch_size"));
                    }
                    builder.append_value(value.hyphenated().encode_lower(&mut [0u8; 36]));
                }
                _ => return Err(conversion("Arrow UUID builder type mismatch")),
            }
            Ok(())
        });
    }

    fn write_decimal(&mut self, col: usize, value: DecimalParts) {
        self.decimal(col, value, false);
    }
    fn write_numeric(&mut self, col: usize, value: DecimalParts) {
        self.decimal(col, value, true);
    }

    fn write_money(&mut self, col: usize, value: SqlMoney) {
        self.append(col, Kind::Money, 16, |builder| {
            let scaled = ((value.msb_part as i64) << 32) | i64::from(value.lsb_part as u32);
            match builder {
                ColumnBuilder::Decimal(builder) => builder.append_value(scaled as i128),
                _ => return Err(conversion("Arrow money builder type mismatch")),
            }
            Ok(())
        });
    }

    fn write_smallmoney(&mut self, col: usize, value: SqlSmallMoney) {
        self.append(col, Kind::SmallMoney, 16, |builder| {
            match builder {
                ColumnBuilder::Decimal(builder) => builder.append_value(value.int_val as i128),
                _ => return Err(conversion("Arrow smallmoney builder type mismatch")),
            }
            Ok(())
        });
    }

    fn write_date(&mut self, col: usize, value: SqlDate) {
        self.append(col, Kind::Date, 4, |builder| {
            match builder {
                ColumnBuilder::Date(builder) => builder.append_value(date32(value.get_days())?),
                _ => return Err(conversion("Arrow date builder type mismatch")),
            }
            Ok(())
        });
    }

    fn write_time(&mut self, col: usize, value: SqlTime) {
        self.append(col, Kind::Time(value.scale), 8, |builder| {
            match builder {
                ColumnBuilder::Time(builder) => builder.append_value(time_ns(&value)?),
                _ => return Err(conversion("Arrow time builder type mismatch")),
            }
            Ok(())
        });
    }

    fn write_datetime(&mut self, col: usize, value: SqlDateTime) {
        let converted =
            if !(-53_690..=2_958_463).contains(&value.days) || value.time >= 86_400 * 300 {
                Err(conversion(
                    "Datetime is outside SQL Server's 1753..9999 date/clock range",
                ))
            } else {
                Ok((value.days - SQL_1900_EPOCH_DAYS, i64::from(value.time), 0))
            };
        self.temporal(col, Kind::DateTime, converted);
    }

    fn write_smalldatetime(&mut self, col: usize, value: SqlSmallDateTime) {
        let converted = if value.time >= 1440 {
            Err(conversion(
                "Smalldatetime minutes are outside the SQL Server clock range",
            ))
        } else {
            Ok((
                i32::from(value.days) - SQL_1900_EPOCH_DAYS,
                i64::from(value.time) * 60_000_000_000,
                0,
            ))
        };
        self.temporal(col, Kind::SmallDateTime, converted);
    }

    fn write_datetime2(&mut self, col: usize, value: SqlDateTime2) {
        let converted = (|| Ok((date32(value.days)?, time_ns(&value.time)?, 0)))();
        self.temporal(col, Kind::DateTime2(value.time.scale), converted);
    }

    fn write_datetimeoffset(&mut self, col: usize, value: SqlDateTimeOffset) {
        let converted = (|| {
            if !(-840..=840).contains(&value.offset) {
                return Err(conversion(
                    "Datetimeoffset offset must be in -840..=840 minutes",
                ));
            }
            let date = date32(value.datetime2.days)?;
            let time = time_ns(&value.datetime2.time)?;
            let local_day = i64::from(value.datetime2.days)
                + (time + i64::from(value.offset) * 60_000_000_000).div_euclid(86_400_000_000_000);
            if !(0..=i64::from(MAX_SQL_DAYS)).contains(&local_day) {
                return Err(conversion(
                    "Datetimeoffset local date is outside SQL Server's 0001..9999 range",
                ));
            }
            Ok((date, time, value.offset))
        })();
        self.temporal(
            col,
            Kind::DateTimeOffset(value.datetime2.time.scale),
            converted,
        );
    }

    fn write_vector(&mut self, col: usize, value: SqlVector) {
        if self.failed {
            return;
        }
        let values = match (&value.data, value.base_type) {
            (VectorData::Float32(values), VectorBaseType::Float32) => values,
            _ => {
                self.fail(conversion("Unsupported or inconsistent vector base type"));
                return;
            }
        };
        let dimensions = match i32::try_from(values.len()) {
            Ok(n) if n > 0 && n <= i32::from(VectorBaseType::Float32.max_dimensions()) => n,
            _ => {
                self.fail(conversion("Invalid vector dimensions"));
                return;
            }
        };
        if let Err(error) = self.value_limit(values.len() * 4 + VECTOR_HEADER_SIZE) {
            self.fail(error);
            return;
        }
        self.append(col, Kind::Vector(dimensions), values.len() * 4, |builder| {
            match builder {
                ColumnBuilder::Vector(builder) => {
                    builder.values().append_slice(values);
                    builder.append(true);
                }
                _ => return Err(conversion("Arrow vector builder type mismatch")),
            }
            Ok(())
        });
    }

    fn write_variant_base_type(&mut self, _col: usize, _base: TdsDataType) {
        self.fail(conversion(
            "sql_variant is unsupported; CAST to a concrete supported SQL type",
        ));
    }

    fn end_row(&mut self) {
        if self.failed {
            return;
        }
        let outcome = (|| {
            if self.next_col != self.columns.len() {
                return Err(conversion(format!(
                    "Arrow row has {} columns, expected {}",
                    self.next_col,
                    self.columns.len()
                )));
            }
            self.bytes = checked_add(self.bytes, self.row_bytes)?;
            self.rows = checked_add(self.rows, 1)?;
            self.last_row_bytes = self.row_bytes;
            self.next_col = 0;
            self.row_bytes = 0;
            Ok(())
        })();
        if let Err(error) = outcome {
            self.fail(error);
        }
    }
}

#[cfg(test)]
mod tests;
