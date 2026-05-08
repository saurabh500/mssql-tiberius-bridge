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
            TdsDataType::IntN => ColumnType::Int4, // IntN can be 1/2/4/8; default to Int4
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
            TdsDataType::SsVariant => ColumnType::Ssvariant,
            _ => ColumnType::Null,
        }
    }
}

/// Column metadata exposed to facade consumers.
#[derive(Debug, Clone)]
pub struct Column {
    /// Column name.
    pub name: String,
    /// Column data type.
    pub column_type: ColumnType,
    /// Whether the column is nullable.
    pub nullable: bool,
}

impl Column {
    /// Create a Column from mssql-tds ColumnMetadata.
    pub fn from_tds(meta: &mssql_tds::query::metadata::ColumnMetadata) -> Self {
        Column {
            name: meta.column_name.clone(),
            column_type: ColumnType::from(meta.data_type),
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
        assert_eq!(ColumnType::from(TdsDataType::IntN), ColumnType::Int4);
    }
}
