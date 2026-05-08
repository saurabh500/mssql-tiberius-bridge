//! SQL Server vector type support.

use mssql_tds::datatypes::column_values::ColumnValues;
use mssql_tds::datatypes::sql_vector::{SqlVector, VectorData};
use mssql_tds::datatypes::sqldatatypes::VectorBaseType;
use mssql_tds::datatypes::sqltypes::SqlType;

use crate::query::ToSql;
use crate::row::FromSql;

/// Wrapper for SQL Server vector values (float32 dimensions).
#[derive(Debug, Clone, PartialEq)]
pub struct VectorValue {
    dimensions: Vec<f32>,
}

impl VectorValue {
    /// Create from a Vec of f32 dimensions.
    pub fn new(dimensions: Vec<f32>) -> Self {
        Self { dimensions }
    }

    /// Borrow the dimensions as a slice.
    pub fn dimensions(&self) -> &[f32] {
        &self.dimensions
    }

    /// Consume and return the inner Vec.
    pub fn into_dimensions(self) -> Vec<f32> {
        self.dimensions
    }

    /// Number of dimensions.
    pub fn len(&self) -> usize {
        self.dimensions.len()
    }

    /// Whether the vector is empty.
    pub fn is_empty(&self) -> bool {
        self.dimensions.is_empty()
    }
}

impl From<Vec<f32>> for VectorValue {
    fn from(v: Vec<f32>) -> Self {
        Self { dimensions: v }
    }
}

impl From<VectorValue> for Vec<f32> {
    fn from(v: VectorValue) -> Self {
        v.dimensions
    }
}

impl<'a> FromSql<'a> for VectorValue {
    fn from_sql(val: &'a ColumnValues) -> Option<Self> {
        match val {
            ColumnValues::Vector(sv) => match &sv.data {
                VectorData::Float32(dims) => Some(VectorValue::new(dims.clone())),
            },
            _ => None,
        }
    }
}

impl ToSql for VectorValue {
    fn to_sql(&self) -> SqlType {
        let dim_count = self.dimensions.len() as u16;
        match SqlVector::try_from_f32(self.dimensions.clone()) {
            Ok(sv) => SqlType::Vector(Some(sv), dim_count, VectorBaseType::Float32),
            Err(_) => SqlType::Vector(None, dim_count, VectorBaseType::Float32),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_value_new_and_accessors() {
        let v = VectorValue::new(vec![1.0, 2.0, 3.0]);
        assert_eq!(v.len(), 3);
        assert!(!v.is_empty());
        assert_eq!(v.dimensions(), &[1.0, 2.0, 3.0]);
    }

    #[test]
    fn vector_value_from_vec() {
        let v: VectorValue = vec![0.1f32, 0.2, 0.3].into();
        assert_eq!(v.dimensions(), &[0.1, 0.2, 0.3]);
    }

    #[test]
    fn vector_value_into_vec() {
        let v = VectorValue::new(vec![1.0, 2.0]);
        let dims: Vec<f32> = v.into();
        assert_eq!(dims, vec![1.0, 2.0]);
    }

    #[test]
    fn vector_value_empty() {
        let v = VectorValue::new(vec![]);
        assert!(v.is_empty());
        assert_eq!(v.len(), 0);
    }

    #[test]
    fn from_sql_vector() {
        let sv = SqlVector::try_from_f32(vec![1.0, 2.0, 3.0]).unwrap();
        let cv = ColumnValues::Vector(sv);
        let v = VectorValue::from_sql(&cv).unwrap();
        assert_eq!(v.dimensions(), &[1.0, 2.0, 3.0]);
    }

    #[test]
    fn from_sql_non_vector_returns_none() {
        let cv = ColumnValues::Int(42);
        assert!(VectorValue::from_sql(&cv).is_none());
    }

    #[test]
    fn to_sql_roundtrip() {
        let v = VectorValue::new(vec![1.5, 2.5, 3.5]);
        let sql_type = v.to_sql();
        match sql_type {
            SqlType::Vector(Some(_), 3, VectorBaseType::Float32) => {}
            _ => panic!("Expected SqlType::Vector(Some(_), 3, Float32)"),
        }
    }
}
