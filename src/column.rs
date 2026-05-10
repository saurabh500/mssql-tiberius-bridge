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

/// SQL Server collation metadata exposed through a bridge-owned type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Collation {
    /// Raw 32-bit collation info value.
    pub info: u32,
    /// LCID language identifier from the collation info.
    pub lcid_language_id: i32,
    /// Collation comparison flags.
    pub col_flags: u8,
    /// SQL Server sort ID.
    pub sort_id: u8,
}

impl From<mssql_tds::token::tokens::SqlCollation> for Collation {
    fn from(value: mssql_tds::token::tokens::SqlCollation) -> Self {
        Self {
            info: value.info,
            lcid_language_id: value.lcid_language_id,
            col_flags: value.col_flags,
            sort_id: value.sort_id,
        }
    }
}

/// Four-part source table name for a column, when supplied by SQL Server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultiPartName {
    /// Server name portion.
    pub server_name: Option<String>,
    /// Catalog/database name portion.
    pub catalog_name: Option<String>,
    /// Schema name portion.
    pub schema_name: Option<String>,
    /// Table name portion.
    pub table_name: String,
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
    /// Whether the column is an identity column.
    pub(crate) is_identity: bool,
    /// Whether the column is computed by SQL Server.
    pub(crate) is_computed: bool,
    /// Whether the column collation is case-sensitive.
    pub(crate) is_case_sensitive: bool,
    /// Whether the column is a sparse column set.
    pub(crate) is_sparse_column_set: bool,
    /// Whether the column is protected by Always Encrypted.
    pub(crate) is_encrypted: bool,
    /// Whether the column uses PLP (`max`) encoding.
    pub(crate) is_plp: bool,
    /// Wire byte length from TDS type info.
    pub(crate) byte_length: usize,
    /// Decimal/numeric/time scale, when supplied by SQL Server.
    pub(crate) scale: Option<u8>,
    /// String collation metadata, when supplied by SQL Server.
    pub(crate) collation: Option<Collation>,
    /// SQL Server user type ordinal.
    pub(crate) user_type: u32,
    /// Four-part source table name, when supplied by SQL Server.
    pub(crate) multi_part_name: Option<MultiPartName>,
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

    /// Returns whether the column is an identity column.
    pub fn is_identity(&self) -> bool {
        self.is_identity
    }

    /// Returns whether the column is computed by SQL Server.
    pub fn is_computed(&self) -> bool {
        self.is_computed
    }

    /// Returns whether the column collation is case-sensitive.
    pub fn is_case_sensitive(&self) -> bool {
        self.is_case_sensitive
    }

    /// Returns whether the column is a sparse column set.
    pub fn is_sparse_column_set(&self) -> bool {
        self.is_sparse_column_set
    }

    /// Returns whether the column is protected by Always Encrypted.
    pub fn is_encrypted(&self) -> bool {
        self.is_encrypted
    }

    /// Returns whether the column uses partially length-prefixed (`max`) encoding.
    pub fn is_plp(&self) -> bool {
        self.is_plp
    }

    /// Returns the TDS wire byte length for the column.
    pub fn byte_length(&self) -> usize {
        self.byte_length
    }

    /// Returns decimal/numeric/time scale metadata, when available.
    pub fn scale(&self) -> Option<u8> {
        self.scale
    }

    /// Returns decimal/numeric precision metadata, when available.
    pub fn precision(&self) -> Option<u8> {
        // TODO: blocked on upstream get_precision().
        None
    }

    /// Returns string collation metadata, when available.
    pub fn collation(&self) -> Option<Collation> {
        self.collation
    }

    /// Returns the SQL Server user type ordinal.
    pub fn user_type(&self) -> u32 {
        self.user_type
    }

    /// Returns the source table four-part name, when available.
    pub fn multi_part_name(&self) -> Option<&MultiPartName> {
        self.multi_part_name.as_ref()
    }

    /// Declared column character length (e.g. `255` for `NVARCHAR(255)`).
    /// For unicode types (NVARCHAR/NCHAR/NTEXT) this is `byte_length / 2`.
    /// For non-string types, returns `None`.
    pub fn char_length(&self) -> Option<usize> {
        match self.column_type {
            ColumnType::NVarchar | ColumnType::NChar | ColumnType::NText => {
                Some(self.byte_length / 2)
            }
            ColumnType::Varchar | ColumnType::Char | ColumnType::Text => Some(self.byte_length),
            _ => None,
        }
    }

    /// Create a Column from mssql-tds ColumnMetadata.
    pub fn from_tds(meta: &mssql_tds::query::metadata::ColumnMetadata) -> Self {
        Column {
            name: meta.column_name.clone(),
            column_type: ColumnType::from_tds_with_length(meta.data_type, meta.type_info.length),
            nullable: meta.is_nullable(),
            is_identity: meta.is_identity(),
            is_computed: meta.is_computed(),
            is_case_sensitive: meta.is_case_sensitive(),
            is_sparse_column_set: meta.is_sparse_column_set(),
            is_encrypted: meta.is_encrypted(),
            is_plp: meta.is_plp(),
            byte_length: meta.type_info.length,
            scale: meta.get_scale(),
            collation: meta.get_collation().map(Collation::from),
            user_type: meta.user_type,
            multi_part_name: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn test_column(name: &str, column_type: ColumnType, byte_length: usize) -> Self {
        Self {
            name: name.to_string(),
            column_type,
            nullable: true,
            is_identity: false,
            is_computed: false,
            is_case_sensitive: false,
            is_sparse_column_set: false,
            is_encrypted: false,
            is_plp: false,
            byte_length,
            scale: None,
            collation: None,
            user_type: 0,
            multi_part_name: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_metadata_accessors() {
        let column = Column {
            name: "id".to_string(),
            column_type: ColumnType::Int4,
            nullable: false,
            is_identity: true,
            is_computed: true,
            is_case_sensitive: true,
            is_sparse_column_set: true,
            is_encrypted: true,
            is_plp: true,
            byte_length: 4,
            scale: Some(2),
            collation: Some(Collation {
                info: 0x0010_0409,
                lcid_language_id: 1033,
                col_flags: 1,
                sort_id: 52,
            }),
            user_type: 7,
            multi_part_name: Some(MultiPartName {
                server_name: Some("server".to_string()),
                catalog_name: Some("db".to_string()),
                schema_name: Some("dbo".to_string()),
                table_name: "users".to_string(),
            }),
        };

        assert_eq!(column.name(), "id");
        assert_eq!(column.column_type(), ColumnType::Int4);
        assert!(!column.nullable());
        assert!(column.is_identity());
        assert!(column.is_computed());
        assert!(column.is_case_sensitive());
        assert!(column.is_sparse_column_set());
        assert!(column.is_encrypted());
        assert!(column.is_plp());
        assert_eq!(column.byte_length(), 4);
        assert_eq!(column.scale(), Some(2));
        assert_eq!(column.precision(), None);
        assert_eq!(column.collation().unwrap().sort_id, 52);
        assert_eq!(column.user_type(), 7);
        assert_eq!(column.multi_part_name().unwrap().table_name, "users");
    }

    #[test]
    fn test_char_length() {
        assert_eq!(
            Column::test_column("n", ColumnType::NVarchar, 510).char_length(),
            Some(255)
        );
        assert_eq!(
            Column::test_column("c", ColumnType::Varchar, 255).char_length(),
            Some(255)
        );
        assert_eq!(
            Column::test_column("i", ColumnType::Int4, 4).char_length(),
            None
        );
    }

    #[test]
    fn test_collation_from_tds() {
        let tds = mssql_tds::token::tokens::SqlCollation {
            info: 0x0010_0409,
            lcid_language_id: 1033,
            col_flags: 1,
            sort_id: 52,
        };
        let collation = Collation::from(tds);

        assert_eq!(collation.info, 0x0010_0409);
        assert_eq!(collation.lcid_language_id, 1033);
        assert_eq!(collation.col_flags, 1);
        assert_eq!(collation.sort_id, 52);
    }

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
