//! Column type enumeration mirroring tiberius' ColumnType.

use mssql_tds::datatypes::sqldatatypes::TdsDataType;

/// SQL Server column data types, providing a tiberius-compatible enum
/// for pattern matching in application code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Null,
    Bit,
    Int1,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    Datetime,
    Datetime2,
    Datetime4,
    DatetimeOffset,
    Date,
    Time,
    Decimaln,
    Numericn,
    Money,
    Money4,
    Guid,
    NVarchar,
    Varchar,
    NChar,
    Char,
    NText,
    Text,
    Binary,
    VarBinary,
    Image,
    Xml,
    Json,
    Vector,
    BigVarBin,
    Ssvariant,
}

impl From<TdsDataType> for ColumnType {
    fn from(dt: TdsDataType) -> Self {
        match dt {
            TdsDataType::Void => ColumnType::Null,
            TdsDataType::Bit | TdsDataType::BitN => ColumnType::Bit,
            TdsDataType::Int1 => ColumnType::Int1,
            TdsDataType::Int2 => ColumnType::Int2,
            TdsDataType::Int4 => ColumnType::Int4,
            TdsDataType::IntN => ColumnType::Int4, // Default; use from_tds_with_length for accuracy
            TdsDataType::Int8 => ColumnType::Int8,
            TdsDataType::Flt4 => ColumnType::Float4,
            TdsDataType::Flt8 => ColumnType::Float8,
            TdsDataType::FltN => ColumnType::Float8,
            TdsDataType::DateTime | TdsDataType::DateTimeN => ColumnType::Datetime,
            TdsDataType::DateTime2N => ColumnType::Datetime2,
            TdsDataType::DateTim4 => ColumnType::Datetime4,
            TdsDataType::DateTimeOffsetN => ColumnType::DatetimeOffset,
            TdsDataType::DateN => ColumnType::Date,
            TdsDataType::TimeN => ColumnType::Time,
            TdsDataType::Decimal | TdsDataType::DecimalN => ColumnType::Decimaln,
            TdsDataType::Numeric | TdsDataType::NumericN => ColumnType::Numericn,
            TdsDataType::Money | TdsDataType::MoneyN => ColumnType::Money,
            TdsDataType::Guid => ColumnType::Guid,
            TdsDataType::NVarChar => ColumnType::NVarchar,
            TdsDataType::VarChar | TdsDataType::BigVarChar => ColumnType::Varchar,
            TdsDataType::NChar => ColumnType::NChar,
            TdsDataType::Char => ColumnType::Char,
            TdsDataType::NText => ColumnType::NText,
            TdsDataType::Text => ColumnType::Text,
            TdsDataType::Binary => ColumnType::Binary,
            TdsDataType::VarBinary | TdsDataType::BigVarBinary => ColumnType::VarBinary,
            TdsDataType::Image => ColumnType::Image,
            TdsDataType::Xml => ColumnType::Xml,
            TdsDataType::Json => ColumnType::Json,
            TdsDataType::Vector => ColumnType::Vector,
            TdsDataType::SsVariant => ColumnType::Ssvariant,
            _ => ColumnType::Null,
        }
    }
}

impl ColumnType {
    /// Resolve the column type using both the TDS data type and the wire byte
    /// length. This is necessary for variable-width nullable types like `IntN`
    /// and `FltN` where the data type alone doesn't indicate the width.
    pub fn from_tds_with_length(dt: TdsDataType, byte_length: usize) -> Self {
        match dt {
            TdsDataType::IntN => match byte_length {
                1 => ColumnType::Int1,
                2 => ColumnType::Int2,
                4 => ColumnType::Int4,
                8 => ColumnType::Int8,
                _ => ColumnType::Int4,
            },
            TdsDataType::FltN => match byte_length {
                4 => ColumnType::Float4,
                8 => ColumnType::Float8,
                _ => ColumnType::Float8,
            },
            TdsDataType::MoneyN => match byte_length {
                4 => ColumnType::Money4,
                8 => ColumnType::Money,
                _ => ColumnType::Money,
            },
            other => ColumnType::from(other),
        }
    }
}

/// Column metadata exposed to facade consumers.
#[derive(Debug, Clone)]
pub struct Column {
    /// Column name.
    pub(crate) name: String,
    /// Column data type.
    pub(crate) column_type: ColumnType,
    /// Whether the column is nullable.
    pub(crate) nullable: bool,
}

impl Column {
    /// Returns the column name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the column data type.
    pub fn column_type(&self) -> ColumnType {
        self.column_type
    }

    /// Returns whether the column is nullable.
    pub fn nullable(&self) -> bool {
        self.nullable
    }

    /// Create a Column from mssql-tds ColumnMetadata.
    pub fn from_tds(meta: &mssql_tds::query::metadata::ColumnMetadata) -> Self {
        Column {
            name: meta.column_name.clone(),
            column_type: ColumnType::from_tds_with_length(meta.data_type, meta.type_info.length),
            nullable: meta.is_nullable(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_type_from_tds() {
        assert_eq!(ColumnType::from(TdsDataType::Int4), ColumnType::Int4);
        assert_eq!(ColumnType::from(TdsDataType::Bit), ColumnType::Bit);
        assert_eq!(
            ColumnType::from(TdsDataType::NVarChar),
            ColumnType::NVarchar
        );
        assert_eq!(
            ColumnType::from(TdsDataType::DateTime),
            ColumnType::Datetime
        );
        assert_eq!(ColumnType::from(TdsDataType::Guid), ColumnType::Guid);
        assert_eq!(
            ColumnType::from(TdsDataType::DecimalN),
            ColumnType::Decimaln
        );
        assert_eq!(ColumnType::from(TdsDataType::Image), ColumnType::Image);
    }

    #[test]
    fn test_column_type_nullable_variants() {
        assert_eq!(ColumnType::from(TdsDataType::BitN), ColumnType::Bit);
        assert_eq!(ColumnType::from(TdsDataType::FltN), ColumnType::Float8);
        // IntN via From defaults to Int4 (no length info available)
        assert_eq!(ColumnType::from(TdsDataType::IntN), ColumnType::Int4);
    }

    #[test]
    fn test_intn_resolved_by_byte_length() {
        assert_eq!(
            ColumnType::from_tds_with_length(TdsDataType::IntN, 1),
            ColumnType::Int1
        );
        assert_eq!(
            ColumnType::from_tds_with_length(TdsDataType::IntN, 2),
            ColumnType::Int2
        );
        assert_eq!(
            ColumnType::from_tds_with_length(TdsDataType::IntN, 4),
            ColumnType::Int4
        );
        assert_eq!(
            ColumnType::from_tds_with_length(TdsDataType::IntN, 8),
            ColumnType::Int8
        );
    }

    #[test]
    fn test_fltn_resolved_by_byte_length() {
        assert_eq!(
            ColumnType::from_tds_with_length(TdsDataType::FltN, 4),
            ColumnType::Float4
        );
        assert_eq!(
            ColumnType::from_tds_with_length(TdsDataType::FltN, 8),
            ColumnType::Float8
        );
    }

    #[test]
    fn test_moneyn_resolved_by_byte_length() {
        assert_eq!(
            ColumnType::from_tds_with_length(TdsDataType::MoneyN, 4),
            ColumnType::Money4
        );
        assert_eq!(
            ColumnType::from_tds_with_length(TdsDataType::MoneyN, 8),
            ColumnType::Money
        );
    }

    #[test]
    fn test_from_tds_with_length_falls_through_for_fixed_types() {
        // Non-variable types should pass through to From<TdsDataType>
        assert_eq!(
            ColumnType::from_tds_with_length(TdsDataType::Int4, 4),
            ColumnType::Int4
        );
        assert_eq!(
            ColumnType::from_tds_with_length(TdsDataType::Bit, 1),
            ColumnType::Bit
        );
        assert_eq!(
            ColumnType::from_tds_with_length(TdsDataType::NVarChar, 100),
            ColumnType::NVarchar
        );
    }
}
