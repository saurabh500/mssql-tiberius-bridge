//! Column types and result-set metadata, including tiberius' ColumnType.

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
    /// SQL Server `geography`, returned as raw serialized bytes.
    Geography,
    /// SQL Server `geometry`, returned as raw serialized bytes.
    Geometry,
    /// A CLR user-defined type without a recognized spatial identity.
    Udt,
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
            TdsDataType::Money4 => ColumnType::Money4,
            TdsDataType::Guid => ColumnType::Guid,
            TdsDataType::NVarChar => ColumnType::NVarchar,
            TdsDataType::VarChar | TdsDataType::BigVarChar => ColumnType::Varchar,
            TdsDataType::NChar => ColumnType::NChar,
            TdsDataType::Char | TdsDataType::BigChar => ColumnType::Char,
            TdsDataType::NText => ColumnType::NText,
            TdsDataType::Text => ColumnType::Text,
            TdsDataType::Binary | TdsDataType::BigBinary => ColumnType::Binary,
            TdsDataType::VarBinary | TdsDataType::BigVarBinary => ColumnType::VarBinary,
            TdsDataType::Image => ColumnType::Image,
            TdsDataType::Xml => ColumnType::Xml,
            TdsDataType::Json => ColumnType::Json,
            TdsDataType::Vector => ColumnType::Vector,
            TdsDataType::SsVariant => ColumnType::Ssvariant,
            TdsDataType::Udt => ColumnType::Udt,
            _ => ColumnType::Null,
        }
    }
}

impl ColumnType {
    fn from_metadata(meta: &mssql_tds::query::metadata::ColumnMetadata) -> Self {
        if meta.data_type == TdsDataType::Udt {
            if let Some(info) = meta.type_info.udt_info() {
                // A custom UDT can have the same name as a system spatial type.
                if info.schema_name().eq_ignore_ascii_case("sys") {
                    if info.type_name().eq_ignore_ascii_case("geography") {
                        return Self::Geography;
                    }
                    if info.type_name().eq_ignore_ascii_case("geometry") {
                        return Self::Geometry;
                    }
                }
            }
        }
        Self::from_tds_with_length(meta.data_type, meta.type_info.length)
    }

    /// Resolve the column type using both the TDS data type and the wire byte
    /// length. This is necessary for variable-width nullable types like `IntN`
    /// and `FltN` where the data type alone doesn't indicate the width.
    /// CLR UDTs resolve to [`Self::Udt`]; [`Column::from_tds`] uses the
    /// additional UDT identity metadata to recognize spatial types.
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
///
/// These are wire identifiers and flags, not a resolved SQL collation name.
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

/// Source table name components, when supplied by SQL Server.
///
/// `COLMETADATA` supplies these for legacy `TEXT`, `NTEXT`, and `IMAGE`
/// columns. This is not general base-table lineage for arbitrary result columns.
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

impl From<&mssql_tds::query::metadata::MultiPartName> for MultiPartName {
    fn from(value: &mssql_tds::query::metadata::MultiPartName) -> Self {
        Self {
            server_name: value.server_name().map(str::to_owned),
            catalog_name: value.catalog_name().map(str::to_owned),
            schema_name: value.schema_name().map(str::to_owned),
            table_name: value.table_name().to_owned(),
        }
    }
}

/// Column metadata exposed to facade consumers.
///
/// Flags describe the returned result column, not necessarily its base-table
/// definition. Ordinary `SPARSE` and `ROWGUIDCOL` properties are not present in
/// TDS result metadata; [`Self::is_sparse_column_set`] is a different property.
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
    /// The server's `fCaseSen` flag.
    pub(crate) is_case_sensitive: bool,
    /// Whether the column is a sparse column set.
    pub(crate) is_sparse_column_set: bool,
    /// Whether the column is protected by Always Encrypted.
    pub(crate) is_encrypted: bool,
    /// Whether the column uses PLP (`max`) encoding.
    pub(crate) is_plp: bool,
    /// Wire byte length from TDS type info.
    pub(crate) byte_length: usize,
    /// Numeric precision, when available.
    pub(crate) precision: Option<u8>,
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

    /// Returns the server's `fNullable` flag for this result column.
    ///
    /// This is not a base-table constraint lookup and does not resolve the
    /// separate TDS unknown-nullability flag.
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

    /// Returns the server's `fCaseSen` flag.
    ///
    /// SQL Server sets this for binary collations and XML. It does not describe
    /// all SQL collation comparison rules.
    pub fn is_case_sensitive(&self) -> bool {
        self.is_case_sensitive
    }

    /// Returns whether the column is the special XML sparse column set.
    ///
    /// This does not indicate whether an ordinary column is declared `SPARSE`.
    pub fn is_sparse_column_set(&self) -> bool {
        self.is_sparse_column_set
    }

    /// Returns whether the column is protected by Always Encrypted.
    pub fn is_encrypted(&self) -> bool {
        self.is_encrypted
    }

    /// Returns whether the column uses partially length-prefixed (PLP) encoding.
    ///
    /// This includes `(MAX)` strings/binary data, XML, and CLR UDTs.
    pub fn is_plp(&self) -> bool {
        self.is_plp
    }

    /// Returns the TDS wire byte length for the column.
    ///
    /// This is the type descriptor's length, not the current value's length.
    /// PLP types may report a sentinel such as `0xFFFF`, not a finite capacity.
    pub fn byte_length(&self) -> usize {
        self.byte_length
    }

    /// Returns decimal/numeric or temporal scale metadata, when supplied in
    /// the TDS type descriptor.
    pub fn scale(&self) -> Option<u8> {
        self.scale
    }

    /// Returns numeric precision metadata, when available.
    ///
    /// Decimal/numeric types report their declared precision. Money and
    /// smallmoney report their fixed precisions of 19 and 10, respectively.
    /// Other types return `None`.
    pub fn precision(&self) -> Option<u8> {
        self.precision
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

    /// Returns the finite string capacity (e.g. `255` for `NVARCHAR(255)`).
    ///
    /// Unicode types (`NVARCHAR`/`NCHAR`/`NTEXT`) report UTF-16 code units
    /// (`byte_length / 2`), not Unicode scalar values. `VARCHAR`/`CHAR`/`TEXT`
    /// report byte capacity, which may differ from character count.
    /// Legacy LOB types report their wire-declared capacity.
    ///
    /// Returns `None` for `(MAX)`/PLP and non-string types. Use [`Self::is_plp`]
    /// to distinguish PLP encoding; [`Self::byte_length`] retains its raw value.
    pub fn char_length(&self) -> Option<usize> {
        if self.is_plp {
            return None;
        }
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
            column_type: ColumnType::from_metadata(meta),
            nullable: meta.is_nullable(),
            is_identity: meta.is_identity(),
            is_computed: meta.is_computed(),
            is_case_sensitive: meta.is_case_sensitive(),
            is_sparse_column_set: meta.is_sparse_column_set(),
            is_encrypted: meta.is_encrypted(),
            is_plp: meta.is_plp(),
            byte_length: meta.type_info.length,
            precision: meta.get_precision(),
            scale: meta.get_scale(),
            collation: meta.get_collation().map(Collation::from),
            user_type: meta.user_type,
            multi_part_name: meta.multi_part_name.as_ref().map(MultiPartName::from),
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
            precision: None,
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
    use mssql_tds::datatypes::sqldatatypes::TypeInfo;
    use mssql_tds::query::metadata::ColumnMetadata;
    use mssql_tds::test_client_support::{int_columns, udt_column, udt_column_with_metadata};
    use mssql_tds::token::tokens::SqlCollation;

    fn metadata_with_type_info(type_info: TypeInfo) -> ColumnMetadata {
        let mut metadata = int_columns(1).into_iter().next().expect("one column");
        metadata.column_name = "value".to_owned();
        metadata.data_type = type_info.tds_type;
        metadata.type_info = type_info;
        metadata
    }

    #[test]
    fn metadata_flags_are_independent() {
        for flags in [
            0, 0x0001, 0x0002, 0x0010, 0x0020, 0x0400, 0x0800, 0x1000, 0x2000, 0x4000, 0x8000,
            0x0433, 0xFFFF,
        ] {
            let mut metadata = metadata_with_type_info(
                TypeInfo::fixed_len(TdsDataType::Int4).expect("integer descriptor"),
            );
            metadata.flags = flags;
            metadata.user_type = 12345;
            metadata.column_name = "aliased.name".to_owned();
            let column = Column::from_tds(&metadata);
            assert_eq!(column.name(), "aliased.name");
            assert_eq!(column.user_type(), 12345);
            assert_eq!(column.nullable(), flags & 0x0001 != 0, "{flags:#06x}");
            assert_eq!(
                column.is_case_sensitive(),
                flags & 0x0002 != 0,
                "{flags:#06x}"
            );
            assert_eq!(column.is_identity(), flags & 0x0010 != 0, "{flags:#06x}");
            assert_eq!(column.is_computed(), flags & 0x0020 != 0, "{flags:#06x}");
            assert_eq!(
                column.is_sparse_column_set(),
                flags & 0x0400 != 0,
                "{flags:#06x}"
            );
            assert_eq!(column.is_encrypted(), flags & 0x0800 != 0, "{flags:#06x}");
            assert!(!column.is_plp());
            assert_eq!(column.precision(), None);
            assert_eq!(column.scale(), None);
            assert_eq!(column.collation(), None);
        }
    }

    #[test]
    fn metadata_decimal_precision_is_not_inferred_from_byte_length() {
        for data_type in [
            TdsDataType::Decimal,
            TdsDataType::DecimalN,
            TdsDataType::Numeric,
            TdsDataType::NumericN,
        ] {
            for (length, precision, scale) in [
                (5, 1, 0),
                (5, 9, 4),
                (9, 10, 0),
                (9, 18, 4),
                (17, 38, 0),
                (17, 38, 38),
            ] {
                let metadata = metadata_with_type_info(
                    TypeInfo::var_len_precision_scale(data_type, length, precision, scale)
                        .expect("decimal/numeric descriptor"),
                );
                let column = Column::from_tds(&metadata);
                assert_eq!(column.precision(), Some(precision));
                assert_eq!(column.scale(), Some(scale));
                assert_eq!(column.byte_length(), length);
                assert_eq!(column.char_length(), None);
            }
        }
    }

    #[test]
    fn metadata_money_precision_and_types() {
        for (type_info, column_type, precision) in [
            (
                TypeInfo::fixed_len(TdsDataType::Money).expect("money descriptor"),
                ColumnType::Money,
                19,
            ),
            (
                TypeInfo::fixed_len(TdsDataType::Money4).expect("smallmoney descriptor"),
                ColumnType::Money4,
                10,
            ),
            (
                TypeInfo::var_len(TdsDataType::MoneyN, 8).expect("nullable money descriptor"),
                ColumnType::Money,
                19,
            ),
            (
                TypeInfo::var_len(TdsDataType::MoneyN, 4).expect("nullable smallmoney descriptor"),
                ColumnType::Money4,
                10,
            ),
        ] {
            let column = Column::from_tds(&metadata_with_type_info(type_info));
            assert_eq!(column.column_type(), column_type);
            assert_eq!(column.precision(), Some(precision));
            assert_eq!(column.scale(), None);
        }
    }

    #[test]
    fn metadata_temporal_scale_has_no_numeric_precision() {
        for (data_type, length, scale) in [
            (TdsDataType::TimeN, 3, 0),
            (TdsDataType::TimeN, 5, 7),
            (TdsDataType::DateTime2N, 8, 7),
            (TdsDataType::DateTimeOffsetN, 10, 7),
        ] {
            let column = Column::from_tds(&metadata_with_type_info(
                TypeInfo::var_len_scale(data_type, length, scale).expect("temporal descriptor"),
            ));
            assert_eq!(column.scale(), Some(scale));
            assert_eq!(column.precision(), None);
        }
    }

    #[test]
    fn metadata_finite_string_lengths_and_collation() {
        let collation = SqlCollation {
            info: 0x0010_0409,
            lcid_language_id: 1033,
            col_flags: 1,
            sort_id: 52,
        };
        for (data_type, length, character_length, column_type) in [
            (TdsDataType::NVarChar, 510, 255, ColumnType::NVarchar),
            (TdsDataType::NChar, 510, 255, ColumnType::NChar),
            (TdsDataType::BigVarChar, 255, 255, ColumnType::Varchar),
            (TdsDataType::BigChar, 255, 255, ColumnType::Char),
            (TdsDataType::Text, 1024, 1024, ColumnType::Text),
            (TdsDataType::NText, 1024, 512, ColumnType::NText),
        ] {
            for collation in [Some(collation), None] {
                let column = Column::from_tds(&metadata_with_type_info(
                    TypeInfo::var_len_string(data_type, length, collation)
                        .expect("finite string descriptor"),
                ));
                assert_eq!(column.column_type(), column_type);
                assert_eq!(column.byte_length(), length);
                assert_eq!(column.char_length(), Some(character_length));
                assert_eq!(column.collation(), collation.map(Collation::from));
                assert_eq!(column.precision(), None);
                assert_eq!(column.scale(), None);
                assert!(!column.is_plp());
            }
        }
    }

    #[test]
    fn metadata_plp_lengths_are_not_finite_capacities() {
        let collation = SqlCollation {
            info: 0x0010_0409,
            lcid_language_id: 1033,
            col_flags: 1,
            sort_id: 52,
        };
        for data_type in [TdsDataType::NVarChar, TdsDataType::BigVarChar] {
            for collation in [Some(collation), None] {
                let column = Column::from_tds(&metadata_with_type_info(
                    TypeInfo::partial_len(data_type, usize::from(u16::MAX), collation)
                        .expect("PLP string descriptor"),
                ));
                assert!(column.is_plp());
                assert_eq!(column.byte_length(), usize::from(u16::MAX));
                assert_eq!(column.char_length(), None);
                assert_eq!(column.collation(), collation.map(Collation::from));
            }
        }
        for data_type in [
            TdsDataType::BigVarBinary,
            TdsDataType::Xml,
            TdsDataType::Udt,
        ] {
            let column = Column::from_tds(&metadata_with_type_info(
                TypeInfo::partial_len(data_type, usize::from(u16::MAX), None)
                    .expect("PLP descriptor"),
            ));
            assert!(column.is_plp());
            assert_eq!(column.byte_length(), usize::from(u16::MAX));
            assert_eq!(column.char_length(), None);
            assert_eq!(column.collation(), None);
        }
    }

    #[test]
    fn metadata_binary_lengths_and_types() {
        for (data_type, column_type) in [
            (TdsDataType::BigBinary, ColumnType::Binary),
            (TdsDataType::BigVarBinary, ColumnType::VarBinary),
        ] {
            let column = Column::from_tds(&metadata_with_type_info(
                TypeInfo::var_len(data_type, 255).expect("binary descriptor"),
            ));
            assert_eq!(column.column_type(), column_type);
            assert_eq!(column.byte_length(), 255);
            assert_eq!(column.char_length(), None);
            assert_eq!(column.collation(), None);
            assert!(!column.is_plp());
        }
    }

    #[test]
    fn metadata_source_name_preserves_absence_and_empty_table_name() {
        let mut metadata = metadata_with_type_info(
            TypeInfo::fixed_len(TdsDataType::Int4).expect("integer descriptor"),
        );
        assert_eq!(Column::from_tds(&metadata).multi_part_name(), None);
        metadata.multi_part_name = Some(mssql_tds::query::metadata::MultiPartName::default());
        assert_eq!(
            Column::from_tds(&metadata).multi_part_name(),
            Some(&MultiPartName {
                server_name: None,
                catalog_name: None,
                schema_name: None,
                table_name: String::new(),
            })
        );
    }

    #[test]
    fn metadata_rows_preserve_precision_without_changing_equality() {
        use crate::{ColumnValues, Row};

        let left = metadata_with_type_info(
            TypeInfo::var_len_precision_scale(TdsDataType::DecimalN, 9, 10, 2)
                .expect("decimal(10,2) descriptor"),
        );
        let right = metadata_with_type_info(
            TypeInfo::var_len_precision_scale(TdsDataType::DecimalN, 9, 18, 4)
                .expect("decimal(18,4) descriptor"),
        );
        let left = Row::from_tds(&[left], vec![ColumnValues::Null]);
        let right = Row::from_tds(&[right], vec![ColumnValues::Null]);
        assert_eq!(
            left.columns().first().expect("column").precision(),
            Some(10)
        );
        assert_eq!(
            right.columns().first().expect("column").precision(),
            Some(18)
        );
        assert_eq!(
            left, right,
            "non-type metadata must not affect row equality"
        );
    }

    #[test]
    fn spatial_column_types_use_udt_identity() {
        for (schema, name, expected) in [
            ("sys", "geography", ColumnType::Geography),
            ("sys", "geometry", ColumnType::Geometry),
            ("SYS", "GEOGRAPHY", ColumnType::Geography),
            ("SYS", "GEOMETRY", ColumnType::Geometry),
            ("dbo", "geography", ColumnType::Udt),
            ("dbo", "geometry", ColumnType::Udt),
            ("sys", "hierarchyid", ColumnType::Udt),
            ("sys", "geography_extra", ColumnType::Udt),
            ("", "geometry", ColumnType::Udt),
        ] {
            let mut meta = udt_column_with_metadata(u16::MAX, "", schema, name, "assembly");
            // Classification must not depend on a database's user-type ordinals.
            meta.user_type = 12345;
            meta.column_name = "value".into();
            let column = Column::from_tds(&meta);
            assert_eq!(column.column_type(), expected, "{schema}.{name}");
            assert_eq!(column.name(), "value");
            assert_eq!(column.user_type(), 12345);
            assert!(column.nullable());
            assert!(column.is_plp());
            assert_eq!(column.byte_length(), usize::from(u16::MAX));
            assert_eq!(column.char_length(), None);
        }
    }

    #[test]
    fn spatial_column_type_requires_udt_metadata() {
        assert_eq!(ColumnType::from(TdsDataType::Udt), ColumnType::Udt);
        assert_eq!(
            ColumnType::from_tds_with_length(TdsDataType::Udt, 65535),
            ColumnType::Udt
        );
        assert_eq!(
            Column::from_tds(&udt_column(u16::MAX)).column_type(),
            ColumnType::Udt
        );
        let mut meta = int_columns(1).remove(0);
        meta.column_name = "geography".into();
        assert_eq!(Column::from_tds(&meta).column_type(), ColumnType::Int4);
    }

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
            precision: Some(18),
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
        assert_eq!(column.precision(), Some(18));
        assert_eq!(
            column.collation().expect("column has a collation").sort_id,
            52
        );
        assert_eq!(column.user_type(), 7);
        assert_eq!(
            column
                .multi_part_name()
                .expect("column has a table name")
                .table_name,
            "users"
        );
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
