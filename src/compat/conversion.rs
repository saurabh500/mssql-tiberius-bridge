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

    fn into_column_value(self) -> Result<ColumnValues> {
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
        ColumnValues::String(_) => ColumnData::String(decoded.map(Cow::Borrowed)),
        ColumnValues::DateTime(value) => ColumnData::DateTime(Some(value.clone())),
        ColumnValues::Date(value) => ColumnData::Date(Some(value.clone())),
        ColumnValues::Time(value) => ColumnData::Time(Some(value.clone())),
        ColumnValues::DateTime2(value) => ColumnData::DateTime2(Some(value.clone())),
        ColumnValues::DateTimeOffset(value) => ColumnData::DateTimeOffset(Some(value.clone())),
        ColumnValues::SmallDateTime(value) => ColumnData::SmallDateTime(Some(value.clone())),
        ColumnValues::SmallMoney(value) => ColumnData::SmallMoney(Some(value.clone())),
        ColumnValues::Money(value) => ColumnData::Money(Some(value.clone())),
        ColumnValues::Bytes(value) => ColumnData::Binary(Some(Cow::Borrowed(value))),
        ColumnValues::Xml(_) => ColumnData::Xml(decoded.map(Cow::Borrowed)),
        ColumnValues::Null => null_column_data(column_type),
        ColumnValues::Uuid(value) => ColumnData::Guid(Some(*value)),
        ColumnValues::Json(_) => ColumnData::Json(decoded.map(Cow::Borrowed)),
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
}
