use std::borrow::Cow;

use mssql_tds::datatypes::column_values::{
    ColumnValues, SqlDate, SqlDateTime, SqlDateTime2, SqlDateTimeOffset, SqlMoney,
    SqlSmallDateTime, SqlSmallMoney, SqlTime, SqlXml,
};
use mssql_tds::datatypes::decoder::DecimalParts;
use mssql_tds::datatypes::sql_json::SqlJson;
use mssql_tds::datatypes::sql_string::SqlString;
use mssql_tds::datatypes::sqltypes::SqlType;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::query::ToSql as NativeToSql;
use crate::row::FromSql as NativeFromSql;
use crate::ColumnType;

/// Tiberius-shaped SQL value used by the compatibility conversion traits.
///
/// Values delegate their encoding to the bridge's native [`NativeToSql`]
/// implementations. Unsupported native-only parameter forms remain available
/// through [`ColumnData::Native`] without claiming a Tiberius equivalent.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnData<'a> {
    Bit(Option<bool>),
    U8(Option<u8>),
    I16(Option<i16>),
    I32(Option<i32>),
    I64(Option<i64>),
    F32(Option<f32>),
    F64(Option<f64>),
    String(Option<Cow<'a, str>>),
    Guid(Option<Uuid>),
    Binary(Option<Cow<'a, [u8]>>),
    Numeric(Option<DecimalParts>),
    Xml(Option<Cow<'a, str>>),
    DateTime(Option<SqlDateTime>),
    SmallDateTime(Option<SqlSmallDateTime>),
    Time(Option<SqlTime>),
    Date(Option<SqlDate>),
    DateTime2(Option<SqlDateTime2>),
    DateTimeOffset(Option<SqlDateTimeOffset>),
    Money(Option<SqlMoney>),
    SmallMoney(Option<SqlSmallMoney>),
    Json(Option<Cow<'a, str>>),
    /// A bridge-native parameter with no faithful Tiberius `ColumnData` shape.
    Native(SqlType),
}

impl ColumnData<'_> {
    fn from_native(value: SqlType) -> ColumnData<'static> {
        match value {
            SqlType::Bit(value) => ColumnData::Bit(value),
            SqlType::TinyInt(value) => ColumnData::U8(value),
            SqlType::SmallInt(value) => ColumnData::I16(value),
            SqlType::Int(value) => ColumnData::I32(value),
            SqlType::BigInt(value) => ColumnData::I64(value),
            SqlType::Real(value) => ColumnData::F32(value),
            SqlType::Float(value) => ColumnData::F64(value),
            SqlType::Decimal(value) | SqlType::Numeric(value) => ColumnData::Numeric(value),
            SqlType::Money(value) => ColumnData::Money(value),
            SqlType::SmallMoney(value) => ColumnData::SmallMoney(value),
            SqlType::Time(value) => ColumnData::Time(value),
            SqlType::DateTime2(value) => ColumnData::DateTime2(value),
            SqlType::DateTimeOffset(value) => ColumnData::DateTimeOffset(value),
            SqlType::SmallDateTime(value) => ColumnData::SmallDateTime(value),
            SqlType::DateTime(value) => ColumnData::DateTime(value),
            SqlType::Date(value) => ColumnData::Date(value),
            SqlType::NVarchar(value, _)
            | SqlType::NVarcharMax(value)
            | SqlType::Varchar(value, _)
            | SqlType::VarcharMax(value)
            | SqlType::Char(value, _)
            | SqlType::NChar(value, _)
            | SqlType::Text(value)
            | SqlType::NText(value) => {
                ColumnData::String(value.map(|value| Cow::Owned(value.to_utf8_string())))
            }
            SqlType::VarBinary(value, _)
            | SqlType::VarBinaryMax(value)
            | SqlType::Binary(value, _) => ColumnData::Binary(value.map(Cow::Owned)),
            SqlType::Json(value) => {
                ColumnData::Json(value.map(|value| Cow::Owned(value.as_string())))
            }
            SqlType::Xml(value) => {
                ColumnData::Xml(value.map(|value| Cow::Owned(value.as_string())))
            }
            SqlType::Uuid(value) => ColumnData::Guid(value),
            native @ (SqlType::Vector(..) | SqlType::Variant(_) | SqlType::Table(..)) => {
                ColumnData::Native(native)
            }
        }
    }

    pub(crate) fn into_column_value(self) -> Result<ColumnValues> {
        let value = match self {
            ColumnData::Bit(Some(value)) => ColumnValues::Bit(value),
            ColumnData::U8(Some(value)) => ColumnValues::TinyInt(value),
            ColumnData::I16(Some(value)) => ColumnValues::SmallInt(value),
            ColumnData::I32(Some(value)) => ColumnValues::Int(value),
            ColumnData::I64(Some(value)) => ColumnValues::BigInt(value),
            ColumnData::F32(Some(value)) => ColumnValues::Real(value),
            ColumnData::F64(Some(value)) => ColumnValues::Float(value),
            ColumnData::String(Some(value)) => {
                ColumnValues::String(SqlString::from_utf8_string(value.into_owned()))
            }
            ColumnData::Guid(Some(value)) => ColumnValues::Uuid(value),
            ColumnData::Binary(Some(value)) => ColumnValues::Bytes(value.into_owned()),
            ColumnData::Numeric(Some(value)) => ColumnValues::Numeric(value),
            ColumnData::Xml(Some(value)) => ColumnValues::Xml(SqlXml::from(value.into_owned())),
            ColumnData::DateTime(Some(value)) => ColumnValues::DateTime(value),
            ColumnData::SmallDateTime(Some(value)) => ColumnValues::SmallDateTime(value),
            ColumnData::Time(Some(value)) => ColumnValues::Time(value),
            ColumnData::Date(Some(value)) => ColumnValues::Date(value),
            ColumnData::DateTime2(Some(value)) => ColumnValues::DateTime2(value),
            ColumnData::DateTimeOffset(Some(value)) => ColumnValues::DateTimeOffset(value),
            ColumnData::Money(Some(value)) => ColumnValues::Money(value),
            ColumnData::SmallMoney(Some(value)) => ColumnValues::SmallMoney(value),
            ColumnData::Json(Some(value)) => ColumnValues::Json(SqlJson::from(value.into_owned())),
            ColumnData::Native(value) => {
                return Err(Error::Conversion(format!(
                    "cannot decode bridge-native parameter {value:?} as a row value"
                )));
            }
            ColumnData::Bit(None)
            | ColumnData::U8(None)
            | ColumnData::I16(None)
            | ColumnData::I32(None)
            | ColumnData::I64(None)
            | ColumnData::F32(None)
            | ColumnData::F64(None)
            | ColumnData::String(None)
            | ColumnData::Guid(None)
            | ColumnData::Binary(None)
            | ColumnData::Numeric(None)
            | ColumnData::Xml(None)
            | ColumnData::DateTime(None)
            | ColumnData::SmallDateTime(None)
            | ColumnData::Time(None)
            | ColumnData::Date(None)
            | ColumnData::DateTime2(None)
            | ColumnData::DateTimeOffset(None)
            | ColumnData::Money(None)
            | ColumnData::SmallMoney(None)
            | ColumnData::Json(None) => ColumnValues::Null,
        };
        Ok(value)
    }

    fn is_null(&self) -> bool {
        matches!(
            self,
            ColumnData::Bit(None)
                | ColumnData::U8(None)
                | ColumnData::I16(None)
                | ColumnData::I32(None)
                | ColumnData::I64(None)
                | ColumnData::F32(None)
                | ColumnData::F64(None)
                | ColumnData::String(None)
                | ColumnData::Guid(None)
                | ColumnData::Binary(None)
                | ColumnData::Numeric(None)
                | ColumnData::Xml(None)
                | ColumnData::DateTime(None)
                | ColumnData::SmallDateTime(None)
                | ColumnData::Time(None)
                | ColumnData::Date(None)
                | ColumnData::DateTime2(None)
                | ColumnData::DateTimeOffset(None)
                | ColumnData::Money(None)
                | ColumnData::SmallMoney(None)
                | ColumnData::Json(None)
        )
    }
}

pub(crate) fn column_data_ref<'a>(
    value: &'a ColumnValues,
    decoded: Option<&'a str>,
    column_type: ColumnType,
) -> ColumnData<'a> {
    match value {
        ColumnValues::TinyInt(value) => ColumnData::U8(Some(*value)),
        ColumnValues::SmallInt(value) => ColumnData::I16(Some(*value)),
        ColumnValues::Int(value) => ColumnData::I32(Some(*value)),
        ColumnValues::BigInt(value) => ColumnData::I64(Some(*value)),
        ColumnValues::Real(value) => ColumnData::F32(Some(*value)),
        ColumnValues::Float(value) => ColumnData::F64(Some(*value)),
        ColumnValues::Decimal(value) | ColumnValues::Numeric(value) => {
            ColumnData::Numeric(Some(*value))
        }
        ColumnValues::Bit(value) => ColumnData::Bit(Some(*value)),
        ColumnValues::String(value) => ColumnData::String(Some(match decoded {
            Some(decoded) => Cow::Borrowed(decoded),
            None => Cow::Owned(value.to_utf8_string()),
        })),
        ColumnValues::DateTime(value) => ColumnData::DateTime(Some(value.clone())),
        ColumnValues::Date(value) => ColumnData::Date(Some(value.clone())),
        ColumnValues::Time(value) => ColumnData::Time(Some(value.clone())),
        ColumnValues::DateTime2(value) => ColumnData::DateTime2(Some(value.clone())),
        ColumnValues::DateTimeOffset(value) => ColumnData::DateTimeOffset(Some(value.clone())),
        ColumnValues::SmallDateTime(value) => ColumnData::SmallDateTime(Some(value.clone())),
        ColumnValues::SmallMoney(value) => ColumnData::SmallMoney(Some(value.clone())),
        ColumnValues::Money(value) => ColumnData::Money(Some(value.clone())),
        ColumnValues::Bytes(value) => ColumnData::Binary(Some(Cow::Borrowed(value))),
        ColumnValues::Xml(value) => ColumnData::Xml(Some(match decoded {
            Some(decoded) => Cow::Borrowed(decoded),
            None => Cow::Owned(value.as_string()),
        })),
        ColumnValues::Null => null_column_data(column_type),
        ColumnValues::Uuid(value) => ColumnData::Guid(Some(*value)),
        ColumnValues::Json(value) => ColumnData::Json(Some(match decoded {
            Some(decoded) => Cow::Borrowed(decoded),
            None => Cow::Owned(value.as_string()),
        })),
        ColumnValues::Vector(value) => ColumnData::Native(SqlType::Vector(
            Some(value.clone()),
            value.dimension_count(),
            value.base_type(),
        )),
    }
}

pub(crate) fn column_data_owned(
    value: ColumnValues,
    decoded: Option<String>,
    column_type: ColumnType,
) -> ColumnData<'static> {
    match value {
        ColumnValues::String(value) => ColumnData::String(Some(Cow::Owned(
            decoded.unwrap_or_else(|| value.to_utf8_string()),
        ))),
        ColumnValues::Bytes(value) => ColumnData::Binary(Some(Cow::Owned(value))),
        ColumnValues::Xml(value) => ColumnData::Xml(Some(Cow::Owned(
            decoded.unwrap_or_else(|| value.as_string()),
        ))),
        ColumnValues::Json(value) => ColumnData::Json(Some(Cow::Owned(
            decoded.unwrap_or_else(|| value.as_string()),
        ))),
        value => column_data_ref(&value, None, column_type).into_owned(),
    }
}

fn null_column_data(column_type: ColumnType) -> ColumnData<'static> {
    match column_type {
        ColumnType::Bit => ColumnData::Bit(None),
        ColumnType::Int1 => ColumnData::U8(None),
        ColumnType::Int2 => ColumnData::I16(None),
        ColumnType::Int4 => ColumnData::I32(None),
        ColumnType::Int8 => ColumnData::I64(None),
        ColumnType::Float4 => ColumnData::F32(None),
        ColumnType::Float8 => ColumnData::F64(None),
        ColumnType::Datetime | ColumnType::Datetime4 => ColumnData::DateTime(None),
        ColumnType::Datetime2 => ColumnData::DateTime2(None),
        ColumnType::DatetimeOffset => ColumnData::DateTimeOffset(None),
        ColumnType::Date => ColumnData::Date(None),
        ColumnType::Time => ColumnData::Time(None),
        ColumnType::Decimaln | ColumnType::Numericn => ColumnData::Numeric(None),
        ColumnType::Money => ColumnData::Money(None),
        ColumnType::Money4 => ColumnData::SmallMoney(None),
        ColumnType::Guid => ColumnData::Guid(None),
        ColumnType::Xml => ColumnData::Xml(None),
        ColumnType::Json => ColumnData::Json(None),
        ColumnType::NVarchar
        | ColumnType::Varchar
        | ColumnType::NChar
        | ColumnType::Char
        | ColumnType::NText
        | ColumnType::Text
        | ColumnType::Null => ColumnData::String(None),
        ColumnType::Binary
        | ColumnType::VarBinary
        | ColumnType::Image
        | ColumnType::BigVarBin
        | ColumnType::Ssvariant
        | ColumnType::Geography
        | ColumnType::Geometry
        | ColumnType::Udt
        | ColumnType::Vector => ColumnData::Binary(None),
    }
}

impl ColumnData<'_> {
    fn into_owned(self) -> ColumnData<'static> {
        match self {
            ColumnData::String(value) => {
                ColumnData::String(value.map(|value| Cow::Owned(value.into_owned())))
            }
            ColumnData::Binary(value) => {
                ColumnData::Binary(value.map(|value| Cow::Owned(value.into_owned())))
            }
            ColumnData::Xml(value) => {
                ColumnData::Xml(value.map(|value| Cow::Owned(value.into_owned())))
            }
            ColumnData::Json(value) => {
                ColumnData::Json(value.map(|value| Cow::Owned(value.into_owned())))
            }
            ColumnData::Bit(value) => ColumnData::Bit(value),
            ColumnData::U8(value) => ColumnData::U8(value),
            ColumnData::I16(value) => ColumnData::I16(value),
            ColumnData::I32(value) => ColumnData::I32(value),
            ColumnData::I64(value) => ColumnData::I64(value),
            ColumnData::F32(value) => ColumnData::F32(value),
            ColumnData::F64(value) => ColumnData::F64(value),
            ColumnData::Guid(value) => ColumnData::Guid(value),
            ColumnData::Numeric(value) => ColumnData::Numeric(value),
            ColumnData::DateTime(value) => ColumnData::DateTime(value),
            ColumnData::SmallDateTime(value) => ColumnData::SmallDateTime(value),
            ColumnData::Time(value) => ColumnData::Time(value),
            ColumnData::Date(value) => ColumnData::Date(value),
            ColumnData::DateTime2(value) => ColumnData::DateTime2(value),
            ColumnData::DateTimeOffset(value) => ColumnData::DateTimeOffset(value),
            ColumnData::Money(value) => ColumnData::Money(value),
            ColumnData::SmallMoney(value) => ColumnData::SmallMoney(value),
            ColumnData::Native(value) => ColumnData::Native(value),
        }
    }
}

impl NativeToSql for ColumnData<'_> {
    fn to_sql(&self) -> SqlType {
        match self.clone() {
            ColumnData::Bit(value) => SqlType::Bit(value),
            ColumnData::U8(value) => SqlType::TinyInt(value),
            ColumnData::I16(value) => SqlType::SmallInt(value),
            ColumnData::I32(value) => SqlType::Int(value),
            ColumnData::I64(value) => SqlType::BigInt(value),
            ColumnData::F32(value) => SqlType::Real(value),
            ColumnData::F64(value) => SqlType::Float(value),
            ColumnData::String(value) => SqlType::NVarchar(
                value.map(|value| SqlString::from_utf8_string(value.into_owned())),
                4000,
            ),
            ColumnData::Guid(value) => SqlType::Uuid(value),
            ColumnData::Binary(value) => {
                SqlType::VarBinaryMax(value.map(|value| value.into_owned()))
            }
            ColumnData::Numeric(value) => SqlType::Numeric(value),
            ColumnData::Xml(value) => {
                SqlType::Xml(value.map(|value| SqlXml::from(value.into_owned())))
            }
            ColumnData::DateTime(value) => SqlType::DateTime(value),
            ColumnData::SmallDateTime(value) => SqlType::SmallDateTime(value),
            ColumnData::Time(value) => SqlType::Time(value),
            ColumnData::Date(value) => SqlType::Date(value),
            ColumnData::DateTime2(value) => SqlType::DateTime2(value),
            ColumnData::DateTimeOffset(value) => SqlType::DateTimeOffset(value),
            ColumnData::Money(value) => SqlType::Money(value),
            ColumnData::SmallMoney(value) => SqlType::SmallMoney(value),
            ColumnData::Json(value) => {
                SqlType::Json(value.map(|value| SqlJson::from(value.into_owned())))
            }
            ColumnData::Native(value) => value,
        }
    }
}

/// Tiberius-shaped by-reference conversion to [`ColumnData`].
pub trait ToSql: Send + Sync {
    fn to_sql(&self) -> ColumnData<'static>;
}

impl<T: NativeToSql + ?Sized> ToSql for T {
    fn to_sql(&self) -> ColumnData<'static> {
        ColumnData::from_native(NativeToSql::to_sql(self))
    }
}

/// Tiberius-shaped by-value conversion to [`ColumnData`].
pub trait IntoSql<'a>: Send + Sync {
    fn into_sql(self) -> ColumnData<'a>;
}

impl<'a, T: NativeToSql> IntoSql<'a> for T {
    fn into_sql(self) -> ColumnData<'a> {
        ColumnData::from_native(NativeToSql::to_sql(&self))
    }
}

trait FromColumnData<'a>: Sized {
    fn from_column_data(value: &'a ColumnData<'_>) -> Result<Option<Self>>;
}

fn decode_owned<T>(value: &ColumnData<'_>) -> Result<Option<T>>
where
    T: for<'a> NativeFromSql<'a>,
{
    let value = value.clone().into_column_value()?;
    match (&value, T::from_sql(&value)) {
        (ColumnValues::Null, _) => Ok(None),
        (_, Some(value)) => Ok(Some(value)),
        _ => Err(Error::Conversion(format!(
            "cannot interpret {value:?} as {}",
            std::any::type_name::<T>()
        ))),
    }
}

macro_rules! from_column_data_owned {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl<'a> FromColumnData<'a> for $ty {
                fn from_column_data(value: &'a ColumnData<'_>) -> Result<Option<Self>> {
                    decode_owned(value)
                }
            }
        )+
    };
}

from_column_data_owned!(
    bool,
    u8,
    i16,
    i32,
    i64,
    f32,
    f64,
    String,
    Uuid,
    Vec<u8>,
    serde_json::Value,
    chrono::NaiveDate,
    chrono::NaiveTime,
    chrono::NaiveDateTime,
    chrono::DateTime<chrono::FixedOffset>,
    rust_decimal::Decimal,
);

#[cfg(feature = "time")]
from_column_data_owned!(
    time::Date,
    time::Time,
    time::PrimitiveDateTime,
    time::OffsetDateTime,
);

#[cfg(feature = "jiff")]
from_column_data_owned!(
    jiff::civil::Date,
    jiff::civil::Time,
    jiff::civil::DateTime,
    jiff::Timestamp,
    jiff::Zoned,
);

impl<'a> FromColumnData<'a> for &'a str {
    fn from_column_data(value: &'a ColumnData<'_>) -> Result<Option<Self>> {
        match value {
            ColumnData::String(Some(value)) => Ok(Some(value.as_ref())),
            _ if value.is_null() => Ok(None),
            _ => Err(Error::Conversion(format!(
                "cannot interpret {value:?} as &str"
            ))),
        }
    }
}

impl<'a> FromColumnData<'a> for &'a [u8] {
    fn from_column_data(value: &'a ColumnData<'_>) -> Result<Option<Self>> {
        match value {
            ColumnData::Binary(Some(value)) => Ok(Some(value.as_ref())),
            _ if value.is_null() => Ok(None),
            _ => Err(Error::Conversion(format!(
                "cannot interpret {value:?} as &[u8]"
            ))),
        }
    }
}

/// Convert a compatibility value through the shared row decoder.
pub trait FromSql<'a>: Sized {
    /// SQL NULL is `Ok(None)`; a non-NULL type mismatch is an error.
    fn from_sql(value: &'a ColumnData<'_>) -> Result<Option<Self>>;

    #[doc(hidden)]
    fn from_sql_with_str(value: &'a ColumnValues, decoded: Option<&'a str>)
        -> Result<Option<Self>>;
}

impl<'a, T> FromSql<'a> for T
where
    T: NativeFromSql<'a> + FromColumnData<'a>,
{
    fn from_sql(value: &'a ColumnData<'_>) -> Result<Option<Self>> {
        T::from_column_data(value)
    }

    fn from_sql_with_str(
        value: &'a ColumnValues,
        decoded: Option<&'a str>,
    ) -> Result<Option<Self>> {
        match (value, NativeFromSql::from_sql_with_str(value, decoded)) {
            (ColumnValues::Null, _) => Ok(None),
            (_, Some(value)) => Ok(Some(value)),
            _ => Err(Error::Conversion(format!(
                "cannot interpret {value:?} as {}",
                std::any::type_name::<Self>()
            ))),
        }
    }
}

/// Convert an owned compatibility value through the shared row decoder.
pub trait FromSqlOwned: Sized {
    fn from_sql_owned(value: ColumnData<'static>) -> Result<Option<Self>>;
}

impl<T> FromSqlOwned for T
where
    T: for<'a> FromSql<'a>,
{
    fn from_sql_owned(value: ColumnData<'static>) -> Result<Option<Self>> {
        <T as FromSql>::from_sql(&value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use mssql_tds::datatypes::sql_tvp::TvpTypeName;
    use mssql_tds::datatypes::sql_vector::SqlVector;
    use mssql_tds::datatypes::sqldatatypes::VectorBaseType;

    fn samples() -> (
        DecimalParts,
        SqlDate,
        SqlTime,
        SqlDateTime2,
        SqlDateTimeOffset,
    ) {
        let numeric = DecimalParts::new(true, 5, 2, 12_345);
        let date = SqlDate::create(738_944).expect("valid SQL date");
        let time = SqlTime {
            time_nanoseconds: 45_296_123_456,
            scale: 7,
        };
        let datetime2 = SqlDateTime2 {
            days: date.get_days(),
            time: time.clone(),
        };
        let datetime_offset = SqlDateTimeOffset {
            datetime2: datetime2.clone(),
            offset: 330,
        };
        (numeric, date, time, datetime2, datetime_offset)
    }

    fn sql_string(value: &str) -> SqlString {
        SqlString::from_utf8_string(value.to_string())
    }

    fn assert_roundtrip<T>(value: T)
    where
        T: NativeToSql + for<'a> FromSql<'a> + Clone + PartialEq + std::fmt::Debug,
    {
        let by_ref = <T as ToSql>::to_sql(&value);
        assert_eq!(
            <T as FromSql>::from_sql(&by_ref).expect("decode by-reference conversion"),
            Some(value.clone())
        );
        let by_value = <T as IntoSql>::into_sql(value.clone());
        assert_eq!(
            T::from_sql_owned(by_value).expect("decode owned conversion"),
            Some(value)
        );
    }

    #[test]
    fn owned_and_borrowed_values_use_native_encoding() {
        let owned = String::from("owned").into_sql();
        assert!(matches!(owned, ColumnData::String(Some(_))));
        assert_eq!(
            String::from_sql_owned(owned.clone()).expect("decode owned string"),
            Some("owned".to_string())
        );
        assert_eq!(
            String::from_sql_owned(owned).expect("decode owned string again"),
            Some("owned".to_string())
        );

        let bytes = [1, 2, 3];
        assert!(matches!(
            bytes.as_slice().into_sql(),
            ColumnData::Binary(Some(value)) if value.as_ref() == bytes
        ));

        let borrowed = ColumnData::String(Some(Cow::Borrowed("borrowed")));
        assert_eq!(
            <&str as FromSql>::from_sql(&borrowed).expect("decode borrowed string"),
            Some("borrowed")
        );
        assert_eq!(
            <i32 as FromSql>::from_sql(&ColumnData::I32(None)).expect("decode NULL"),
            None
        );
        assert!(matches!(
            <i32 as FromSql>::from_sql(&borrowed),
            Err(Error::Conversion(_))
        ));
    }

    #[test]
    fn null_and_wrong_type_remain_distinct() {
        assert_eq!(
            i32::from_sql_owned(ColumnData::I32(None)).expect("decode NULL"),
            None
        );
        assert!(matches!(
            i32::from_sql_owned(ColumnData::String(Some(Cow::Borrowed("wrong")))),
            Err(Error::Conversion(_))
        ));
    }

    #[test]
    fn native_sql_types_map_to_exact_compat_variants() {
        let (numeric, date, time, datetime2, datetime_offset) = samples();
        let uuid = Uuid::from_u128(0x1234);
        let vector = SqlVector::try_from_f32(vec![1.0, 2.0]).expect("valid vector");
        let table_name = TvpTypeName::new(Some("dbo".into()), "Items".into());
        let native_values = vec![
            (SqlType::Bit(Some(true)), ColumnData::Bit(Some(true))),
            (SqlType::TinyInt(Some(1)), ColumnData::U8(Some(1))),
            (SqlType::SmallInt(Some(2)), ColumnData::I16(Some(2))),
            (SqlType::Int(Some(3)), ColumnData::I32(Some(3))),
            (SqlType::BigInt(Some(4)), ColumnData::I64(Some(4))),
            (SqlType::Real(Some(5.0)), ColumnData::F32(Some(5.0))),
            (SqlType::Float(Some(6.0)), ColumnData::F64(Some(6.0))),
            (
                SqlType::Decimal(Some(numeric)),
                ColumnData::Numeric(Some(numeric)),
            ),
            (
                SqlType::Numeric(Some(numeric)),
                ColumnData::Numeric(Some(numeric)),
            ),
            (
                SqlType::Money(Some(SqlMoney::from(70_000))),
                ColumnData::Money(Some(SqlMoney::from(70_000))),
            ),
            (
                SqlType::SmallMoney(Some(SqlSmallMoney::from(80_000))),
                ColumnData::SmallMoney(Some(SqlSmallMoney::from(80_000))),
            ),
            (
                SqlType::Time(Some(time.clone())),
                ColumnData::Time(Some(time.clone())),
            ),
            (
                SqlType::DateTime2(Some(datetime2.clone())),
                ColumnData::DateTime2(Some(datetime2.clone())),
            ),
            (
                SqlType::DateTimeOffset(Some(datetime_offset.clone())),
                ColumnData::DateTimeOffset(Some(datetime_offset)),
            ),
            (
                SqlType::SmallDateTime(Some(SqlSmallDateTime { days: 9, time: 10 })),
                ColumnData::SmallDateTime(Some(SqlSmallDateTime { days: 9, time: 10 })),
            ),
            (
                SqlType::DateTime(Some(SqlDateTime { days: 11, time: 12 })),
                ColumnData::DateTime(Some(SqlDateTime { days: 11, time: 12 })),
            ),
            (
                SqlType::Date(Some(date.clone())),
                ColumnData::Date(Some(date)),
            ),
            (
                SqlType::NVarchar(Some(sql_string("nvarchar")), 20),
                ColumnData::String(Some(Cow::Owned("nvarchar".into()))),
            ),
            (
                SqlType::NVarcharMax(Some(sql_string("nvarchar max"))),
                ColumnData::String(Some(Cow::Owned("nvarchar max".into()))),
            ),
            (
                SqlType::Varchar(Some(sql_string("varchar")), 20),
                ColumnData::String(Some(Cow::Owned("varchar".into()))),
            ),
            (
                SqlType::VarcharMax(Some(sql_string("varchar max"))),
                ColumnData::String(Some(Cow::Owned("varchar max".into()))),
            ),
            (
                SqlType::Char(Some(sql_string("char")), 4),
                ColumnData::String(Some(Cow::Owned("char".into()))),
            ),
            (
                SqlType::NChar(Some(sql_string("nchar")), 5),
                ColumnData::String(Some(Cow::Owned("nchar".into()))),
            ),
            (
                SqlType::Text(Some(sql_string("text"))),
                ColumnData::String(Some(Cow::Owned("text".into()))),
            ),
            (
                SqlType::NText(Some(sql_string("ntext"))),
                ColumnData::String(Some(Cow::Owned("ntext".into()))),
            ),
            (
                SqlType::VarBinary(Some(vec![1]), 1),
                ColumnData::Binary(Some(Cow::Owned(vec![1]))),
            ),
            (
                SqlType::VarBinaryMax(Some(vec![2])),
                ColumnData::Binary(Some(Cow::Owned(vec![2]))),
            ),
            (
                SqlType::Binary(Some(vec![3]), 1),
                ColumnData::Binary(Some(Cow::Owned(vec![3]))),
            ),
            (
                SqlType::Json(Some(SqlJson::from("{\"n\":1}".to_string()))),
                ColumnData::Json(Some(Cow::Owned("{\"n\":1}".into()))),
            ),
            (
                SqlType::Xml(Some(SqlXml::from("<n>1</n>".to_string()))),
                ColumnData::Xml(Some(Cow::Owned("<n>1</n>".into()))),
            ),
            (SqlType::Uuid(Some(uuid)), ColumnData::Guid(Some(uuid))),
        ];

        for (native, expected) in native_values {
            assert_eq!(ColumnData::from_native(native), expected);
        }

        let native_only = vec![
            SqlType::Vector(Some(vector), 2, VectorBaseType::Float32),
            SqlType::Variant(Box::new(SqlType::Int(Some(1)))),
            SqlType::Table(table_name, None),
        ];
        for native in native_only {
            assert_eq!(
                ColumnData::from_native(native.clone()),
                ColumnData::Native(native)
            );
        }
    }

    #[test]
    fn compatibility_values_encode_all_variants_and_nulls() {
        let (numeric, date, time, datetime2, datetime_offset) = samples();
        let uuid = Uuid::from_u128(0x5678);
        let values = vec![
            (ColumnData::Bit(Some(true)), ColumnValues::Bit(true)),
            (ColumnData::U8(Some(1)), ColumnValues::TinyInt(1)),
            (ColumnData::I16(Some(2)), ColumnValues::SmallInt(2)),
            (ColumnData::I32(Some(3)), ColumnValues::Int(3)),
            (ColumnData::I64(Some(4)), ColumnValues::BigInt(4)),
            (ColumnData::F32(Some(5.0)), ColumnValues::Real(5.0)),
            (ColumnData::F64(Some(6.0)), ColumnValues::Float(6.0)),
            (
                ColumnData::String(Some(Cow::Borrowed("text"))),
                ColumnValues::String(sql_string("text")),
            ),
            (ColumnData::Guid(Some(uuid)), ColumnValues::Uuid(uuid)),
            (
                ColumnData::Binary(Some(Cow::Borrowed(&[7, 8]))),
                ColumnValues::Bytes(vec![7, 8]),
            ),
            (
                ColumnData::Numeric(Some(numeric)),
                ColumnValues::Numeric(numeric),
            ),
            (
                ColumnData::Xml(Some(Cow::Borrowed("<x/>"))),
                ColumnValues::Xml(SqlXml::from("<x/>".to_string())),
            ),
            (
                ColumnData::DateTime(Some(SqlDateTime { days: 9, time: 10 })),
                ColumnValues::DateTime(SqlDateTime { days: 9, time: 10 }),
            ),
            (
                ColumnData::SmallDateTime(Some(SqlSmallDateTime { days: 11, time: 12 })),
                ColumnValues::SmallDateTime(SqlSmallDateTime { days: 11, time: 12 }),
            ),
            (
                ColumnData::Time(Some(time.clone())),
                ColumnValues::Time(time),
            ),
            (
                ColumnData::Date(Some(date.clone())),
                ColumnValues::Date(date),
            ),
            (
                ColumnData::DateTime2(Some(datetime2.clone())),
                ColumnValues::DateTime2(datetime2),
            ),
            (
                ColumnData::DateTimeOffset(Some(datetime_offset.clone())),
                ColumnValues::DateTimeOffset(datetime_offset),
            ),
            (
                ColumnData::Money(Some(SqlMoney::from(13))),
                ColumnValues::Money(SqlMoney::from(13)),
            ),
            (
                ColumnData::SmallMoney(Some(SqlSmallMoney::from(14))),
                ColumnValues::SmallMoney(SqlSmallMoney::from(14)),
            ),
            (
                ColumnData::Json(Some(Cow::Borrowed("{\"ok\":true}"))),
                ColumnValues::Json(SqlJson::from("{\"ok\":true}".to_string())),
            ),
        ];
        for (value, expected) in values {
            assert_eq!(
                value
                    .into_column_value()
                    .expect("encode compatibility value"),
                expected
            );
        }

        let nulls = vec![
            ColumnData::Bit(None),
            ColumnData::U8(None),
            ColumnData::I16(None),
            ColumnData::I32(None),
            ColumnData::I64(None),
            ColumnData::F32(None),
            ColumnData::F64(None),
            ColumnData::String(None),
            ColumnData::Guid(None),
            ColumnData::Binary(None),
            ColumnData::Numeric(None),
            ColumnData::Xml(None),
            ColumnData::DateTime(None),
            ColumnData::SmallDateTime(None),
            ColumnData::Time(None),
            ColumnData::Date(None),
            ColumnData::DateTime2(None),
            ColumnData::DateTimeOffset(None),
            ColumnData::Money(None),
            ColumnData::SmallMoney(None),
            ColumnData::Json(None),
        ];
        for value in nulls {
            assert!(value.is_null());
            assert_eq!(
                value.into_column_value().expect("encode typed NULL"),
                ColumnValues::Null
            );
        }
        assert!(!ColumnData::I32(Some(1)).is_null());

        let native = ColumnData::Native(SqlType::Variant(Box::new(SqlType::Int(Some(1)))));
        assert!(matches!(
            native.into_column_value(),
            Err(Error::Conversion(message))
                if message.contains("bridge-native parameter")
        ));
    }

    #[test]
    fn native_row_values_adapt_borrowed_owned_and_typed_nulls() {
        let (numeric, date, time, datetime2, datetime_offset) = samples();
        let uuid = Uuid::from_u128(0x9abc);
        let vector = SqlVector::try_from_f32(vec![1.0, 2.0]).expect("valid vector");
        let values = vec![
            (
                ColumnValues::TinyInt(1),
                None,
                ColumnType::Int1,
                ColumnData::U8(Some(1)),
            ),
            (
                ColumnValues::SmallInt(2),
                None,
                ColumnType::Int2,
                ColumnData::I16(Some(2)),
            ),
            (
                ColumnValues::Int(3),
                None,
                ColumnType::Int4,
                ColumnData::I32(Some(3)),
            ),
            (
                ColumnValues::BigInt(4),
                None,
                ColumnType::Int8,
                ColumnData::I64(Some(4)),
            ),
            (
                ColumnValues::Real(5.0),
                None,
                ColumnType::Float4,
                ColumnData::F32(Some(5.0)),
            ),
            (
                ColumnValues::Float(6.0),
                None,
                ColumnType::Float8,
                ColumnData::F64(Some(6.0)),
            ),
            (
                ColumnValues::Decimal(numeric),
                None,
                ColumnType::Decimaln,
                ColumnData::Numeric(Some(numeric)),
            ),
            (
                ColumnValues::Numeric(numeric),
                None,
                ColumnType::Numericn,
                ColumnData::Numeric(Some(numeric)),
            ),
            (
                ColumnValues::Bit(true),
                None,
                ColumnType::Bit,
                ColumnData::Bit(Some(true)),
            ),
            (
                ColumnValues::String(sql_string("wire")),
                Some("decoded"),
                ColumnType::NVarchar,
                ColumnData::String(Some(Cow::Borrowed("decoded"))),
            ),
            (
                ColumnValues::DateTime(SqlDateTime { days: 7, time: 8 }),
                None,
                ColumnType::Datetime,
                ColumnData::DateTime(Some(SqlDateTime { days: 7, time: 8 })),
            ),
            (
                ColumnValues::Date(date.clone()),
                None,
                ColumnType::Date,
                ColumnData::Date(Some(date)),
            ),
            (
                ColumnValues::Time(time.clone()),
                None,
                ColumnType::Time,
                ColumnData::Time(Some(time)),
            ),
            (
                ColumnValues::DateTime2(datetime2.clone()),
                None,
                ColumnType::Datetime2,
                ColumnData::DateTime2(Some(datetime2)),
            ),
            (
                ColumnValues::DateTimeOffset(datetime_offset.clone()),
                None,
                ColumnType::DatetimeOffset,
                ColumnData::DateTimeOffset(Some(datetime_offset)),
            ),
            (
                ColumnValues::SmallDateTime(SqlSmallDateTime { days: 9, time: 10 }),
                None,
                ColumnType::Datetime4,
                ColumnData::SmallDateTime(Some(SqlSmallDateTime { days: 9, time: 10 })),
            ),
            (
                ColumnValues::SmallMoney(SqlSmallMoney::from(11)),
                None,
                ColumnType::Money4,
                ColumnData::SmallMoney(Some(SqlSmallMoney::from(11))),
            ),
            (
                ColumnValues::Money(SqlMoney::from(12)),
                None,
                ColumnType::Money,
                ColumnData::Money(Some(SqlMoney::from(12))),
            ),
            (
                ColumnValues::Bytes(vec![13, 14]),
                None,
                ColumnType::VarBinary,
                ColumnData::Binary(Some(Cow::Borrowed(&[13, 14]))),
            ),
            (
                ColumnValues::Xml(SqlXml::from("<x/>".to_string())),
                Some("<decoded/>"),
                ColumnType::Xml,
                ColumnData::Xml(Some(Cow::Borrowed("<decoded/>"))),
            ),
            (
                ColumnValues::Uuid(uuid),
                None,
                ColumnType::Guid,
                ColumnData::Guid(Some(uuid)),
            ),
            (
                ColumnValues::Json(SqlJson::from("{\"wire\":true}".to_string())),
                Some("{\"decoded\":true}"),
                ColumnType::Json,
                ColumnData::Json(Some(Cow::Borrowed("{\"decoded\":true}"))),
            ),
        ];
        for (value, decoded, column_type, expected) in &values {
            assert_eq!(
                column_data_ref(value, *decoded, *column_type),
                expected.clone()
            );
        }

        let uncached_text = [
            (
                ColumnValues::String(sql_string("string fallback")),
                ColumnType::NVarchar,
                ColumnData::String(Some(Cow::Owned("string fallback".into()))),
            ),
            (
                ColumnValues::Xml(SqlXml::from("<fallback/>".to_string())),
                ColumnType::Xml,
                ColumnData::Xml(Some(Cow::Owned("<fallback/>".into()))),
            ),
            (
                ColumnValues::Json(SqlJson::from("{\"fallback\":true}".to_string())),
                ColumnType::Json,
                ColumnData::Json(Some(Cow::Owned("{\"fallback\":true}".into()))),
            ),
        ];
        for (value, column_type, expected) in &uncached_text {
            assert_eq!(column_data_ref(value, None, *column_type), expected.clone());
        }

        assert!(matches!(
            column_data_ref(
                &ColumnValues::Vector(vector.clone()),
                None,
                ColumnType::Vector
            ),
            ColumnData::Native(SqlType::Vector(Some(value), 2, VectorBaseType::Float32))
                if value == vector
        ));

        let typed_nulls = vec![
            (ColumnType::Bit, ColumnData::Bit(None)),
            (ColumnType::Int1, ColumnData::U8(None)),
            (ColumnType::Int2, ColumnData::I16(None)),
            (ColumnType::Int4, ColumnData::I32(None)),
            (ColumnType::Int8, ColumnData::I64(None)),
            (ColumnType::Float4, ColumnData::F32(None)),
            (ColumnType::Float8, ColumnData::F64(None)),
            (ColumnType::Datetime, ColumnData::DateTime(None)),
            (ColumnType::Datetime4, ColumnData::DateTime(None)),
            (ColumnType::Datetime2, ColumnData::DateTime2(None)),
            (ColumnType::DatetimeOffset, ColumnData::DateTimeOffset(None)),
            (ColumnType::Date, ColumnData::Date(None)),
            (ColumnType::Time, ColumnData::Time(None)),
            (ColumnType::Decimaln, ColumnData::Numeric(None)),
            (ColumnType::Numericn, ColumnData::Numeric(None)),
            (ColumnType::Money, ColumnData::Money(None)),
            (ColumnType::Money4, ColumnData::SmallMoney(None)),
            (ColumnType::Guid, ColumnData::Guid(None)),
            (ColumnType::Xml, ColumnData::Xml(None)),
            (ColumnType::Json, ColumnData::Json(None)),
            (ColumnType::NVarchar, ColumnData::String(None)),
            (ColumnType::Varchar, ColumnData::String(None)),
            (ColumnType::NChar, ColumnData::String(None)),
            (ColumnType::Char, ColumnData::String(None)),
            (ColumnType::NText, ColumnData::String(None)),
            (ColumnType::Text, ColumnData::String(None)),
            (ColumnType::Null, ColumnData::String(None)),
            (ColumnType::Binary, ColumnData::Binary(None)),
            (ColumnType::VarBinary, ColumnData::Binary(None)),
            (ColumnType::Image, ColumnData::Binary(None)),
            (ColumnType::BigVarBin, ColumnData::Binary(None)),
            (ColumnType::Ssvariant, ColumnData::Binary(None)),
            (ColumnType::Geography, ColumnData::Binary(None)),
            (ColumnType::Geometry, ColumnData::Binary(None)),
            (ColumnType::Udt, ColumnData::Binary(None)),
            (ColumnType::Vector, ColumnData::Binary(None)),
        ];
        for (column_type, expected) in typed_nulls {
            assert_eq!(
                column_data_ref(&ColumnValues::Null, None, column_type),
                expected
            );
        }

        assert!(matches!(
            column_data_owned(
                ColumnValues::String(sql_string("wire")),
                Some("decoded".into()),
                ColumnType::NVarchar
            ),
            ColumnData::String(Some(Cow::Owned(value))) if value == "decoded"
        ));
        assert!(matches!(
            column_data_owned(
                ColumnValues::String(sql_string("fallback")),
                None,
                ColumnType::NVarchar
            ),
            ColumnData::String(Some(Cow::Owned(value))) if value == "fallback"
        ));
        assert!(matches!(
            column_data_owned(ColumnValues::Bytes(vec![1, 2]), None, ColumnType::VarBinary),
            ColumnData::Binary(Some(Cow::Owned(value))) if value == [1, 2]
        ));
        assert!(matches!(
            column_data_owned(
                ColumnValues::Xml(SqlXml::from("<wire/>".to_string())),
                Some("<decoded/>".into()),
                ColumnType::Xml
            ),
            ColumnData::Xml(Some(Cow::Owned(value))) if value == "<decoded/>"
        ));
        assert!(matches!(
            column_data_owned(
                ColumnValues::Xml(SqlXml::from("<fallback/>".to_string())),
                None,
                ColumnType::Xml
            ),
            ColumnData::Xml(Some(Cow::Owned(value))) if value == "<fallback/>"
        ));
        assert!(matches!(
            column_data_owned(
                ColumnValues::Json(SqlJson::from("{\"wire\":true}".to_string())),
                Some("{\"decoded\":true}".into()),
                ColumnType::Json
            ),
            ColumnData::Json(Some(Cow::Owned(value))) if value == "{\"decoded\":true}"
        ));
        assert!(matches!(
            column_data_owned(
                ColumnValues::Json(SqlJson::from("{\"fallback\":true}".to_string())),
                None,
                ColumnType::Json
            ),
            ColumnData::Json(Some(Cow::Owned(value))) if value == "{\"fallback\":true}"
        ));
        assert_eq!(
            column_data_owned(ColumnValues::Int(42), None, ColumnType::Int4),
            ColumnData::I32(Some(42))
        );
    }

    #[test]
    fn owned_and_native_parameter_adapters_preserve_every_variant() {
        let (numeric, date, time, datetime2, datetime_offset) = samples();
        let uuid = Uuid::from_u128(0xdef0);
        let values = vec![
            ColumnData::Bit(Some(true)),
            ColumnData::U8(Some(1)),
            ColumnData::I16(Some(2)),
            ColumnData::I32(Some(3)),
            ColumnData::I64(Some(4)),
            ColumnData::F32(Some(5.0)),
            ColumnData::F64(Some(6.0)),
            ColumnData::String(Some(Cow::Borrowed("borrowed"))),
            ColumnData::Guid(Some(uuid)),
            ColumnData::Binary(Some(Cow::Borrowed(&[7, 8]))),
            ColumnData::Numeric(Some(numeric)),
            ColumnData::Xml(Some(Cow::Borrowed("<x/>"))),
            ColumnData::DateTime(Some(SqlDateTime { days: 9, time: 10 })),
            ColumnData::SmallDateTime(Some(SqlSmallDateTime { days: 11, time: 12 })),
            ColumnData::Time(Some(time)),
            ColumnData::Date(Some(date)),
            ColumnData::DateTime2(Some(datetime2)),
            ColumnData::DateTimeOffset(Some(datetime_offset)),
            ColumnData::Money(Some(SqlMoney::from(13))),
            ColumnData::SmallMoney(Some(SqlSmallMoney::from(14))),
            ColumnData::Json(Some(Cow::Borrowed("{\"ok\":true}"))),
            ColumnData::Native(SqlType::Variant(Box::new(SqlType::Int(Some(15))))),
        ];
        for value in values {
            let owned = value.clone().into_owned();
            assert_eq!(ColumnData::from_native(NativeToSql::to_sql(&value)), owned);
        }
        assert!(matches!(
            ColumnData::String(Some(Cow::Borrowed("text"))).into_owned(),
            ColumnData::String(Some(Cow::Owned(value))) if value == "text"
        ));
        assert!(matches!(
            ColumnData::Binary(Some(Cow::Borrowed(&[1, 2]))).into_owned(),
            ColumnData::Binary(Some(Cow::Owned(value))) if value == [1, 2]
        ));
        assert!(matches!(
            ColumnData::Xml(Some(Cow::Borrowed("<x/>"))).into_owned(),
            ColumnData::Xml(Some(Cow::Owned(value))) if value == "<x/>"
        ));
        assert!(matches!(
            ColumnData::Json(Some(Cow::Borrowed("{}"))).into_owned(),
            ColumnData::Json(Some(Cow::Owned(value))) if value == "{}"
        ));
    }

    #[test]
    fn public_conversion_traits_roundtrip_supported_types() {
        assert_roundtrip(true);
        assert_roundtrip(1u8);
        assert_roundtrip(2i16);
        assert_roundtrip(3i32);
        assert_roundtrip(4i64);
        assert_roundtrip(5.25f32);
        assert_roundtrip(6.5f64);
        assert_roundtrip("owned string".to_string());
        assert_roundtrip(Uuid::from_u128(0x1234_5678));
        assert_roundtrip(vec![1u8, 2, 3]);
        assert_roundtrip(serde_json::json!({"compatible": true}));
        assert_roundtrip(
            "12345.67"
                .parse::<rust_decimal::Decimal>()
                .expect("valid decimal"),
        );

        let chrono_date = chrono::NaiveDate::from_ymd_opt(2024, 2, 29).expect("valid date");
        let chrono_time =
            chrono::NaiveTime::from_hms_nano_opt(23, 45, 56, 123_456_700).expect("valid time");
        let chrono_datetime = chrono_date.and_time(chrono_time);
        let chrono_offset = chrono::FixedOffset::east_opt(19_800).expect("valid offset");
        let chrono_zoned = chrono_offset
            .from_local_datetime(&chrono_datetime)
            .single()
            .expect("unambiguous fixed-offset datetime");
        assert_roundtrip(chrono_date);
        assert_roundtrip(chrono_time);
        assert_roundtrip(chrono_datetime);
        assert_roundtrip(chrono_zoned);

        #[cfg(feature = "time")]
        {
            let date = time::Date::from_calendar_date(2024, time::Month::February, 29)
                .expect("valid date");
            let time = time::Time::from_hms_nano(23, 45, 56, 123_456_700).expect("valid time");
            let datetime = time::PrimitiveDateTime::new(date, time);
            let offset = time::UtcOffset::from_hms(5, 30, 0).expect("valid offset");
            assert_roundtrip(date);
            assert_roundtrip(time);
            assert_roundtrip(datetime);
            assert_roundtrip(datetime.assume_offset(offset));
        }

        #[cfg(feature = "jiff")]
        {
            let datetime = jiff::civil::DateTime::new(2024, 2, 29, 23, 45, 56, 123_456_700)
                .expect("valid datetime");
            let offset = jiff::tz::Offset::from_seconds(19_800)
                .expect("valid offset")
                .to_time_zone();
            let zoned = datetime.to_zoned(offset).expect("valid zoned datetime");
            assert_roundtrip(datetime.date());
            assert_roundtrip(datetime.time());
            assert_roundtrip(datetime);
            assert_roundtrip(zoned.timestamp());
            assert_roundtrip(zoned);
        }

        assert_eq!(
            String::from_sql_owned(ColumnData::Xml(Some(Cow::Borrowed("<root/>"))))
                .expect("decode XML"),
            Some("<root/>".into())
        );
        assert_eq!(
            <&str as FromSql>::from_sql(&ColumnData::String(Some(Cow::Borrowed("borrowed"))))
                .expect("decode borrowed string"),
            Some("borrowed")
        );
        assert_eq!(
            <&str as FromSql>::from_sql(&ColumnData::String(None))
                .expect("decode borrowed NULL string"),
            None
        );
        assert!(matches!(
            <&str as FromSql>::from_sql(&ColumnData::I32(Some(1))),
            Err(Error::Conversion(_))
        ));
        assert_eq!(
            <&[u8] as FromSql>::from_sql(&ColumnData::Binary(Some(Cow::Borrowed(&[1, 2]))))
                .expect("decode borrowed bytes"),
            Some([1, 2].as_slice())
        );
        assert_eq!(
            <&[u8] as FromSql>::from_sql(&ColumnData::Binary(None))
                .expect("decode borrowed NULL bytes"),
            None
        );
        assert!(matches!(
            <&[u8] as FromSql>::from_sql(&ColumnData::I32(Some(1))),
            Err(Error::Conversion(_))
        ));
        assert_eq!(
            <i32 as FromSql>::from_sql_with_str(&ColumnValues::Int(9), None)
                .expect("decode native row value"),
            Some(9)
        );
        assert_eq!(
            <i32 as FromSql>::from_sql_with_str(&ColumnValues::Null, None)
                .expect("decode native NULL"),
            None
        );
        assert!(matches!(
            <i32 as FromSql>::from_sql_with_str(
                &ColumnValues::String(sql_string("wrong")),
                Some("wrong")
            ),
            Err(Error::Conversion(_))
        ));
    }
}
