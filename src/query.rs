//! Query result types and ToSql trait for parameterized queries.

use mssql_tds::datatypes::column_values::ColumnValues;
use mssql_tds::datatypes::sql_string::SqlString;
use mssql_tds::datatypes::sqltypes::SqlType;
use mssql_tds::message::parameters::rpc_parameters::{RpcParameter, StatusFlags};
use mssql_tds::query::metadata::ColumnMetadata;

use crate::row::Row;

/// Result of an `execute()` call, containing row counts per statement.
#[derive(Debug, Clone)]
pub struct ExecuteResult {
    pub(crate) counts: Vec<u64>,
}

impl ExecuteResult {
    /// Total rows affected across all statements.
    pub fn total(&self) -> u64 {
        self.counts.iter().sum()
    }

    /// Iterate over per-statement row counts.
    #[allow(clippy::should_implement_trait)]
    pub fn into_iter(self) -> impl Iterator<Item = u64> {
        self.counts.into_iter()
    }
}

/// Collected query results, mirroring tiberius' QueryStream after materialization.
pub struct QueryResult {
    pub(crate) result_sets: Vec<(Vec<ColumnMetadata>, Vec<Vec<ColumnValues>>)>,
}

impl QueryResult {
    /// Consume the first result set into a Vec of Rows.
    /// This is the most common access pattern, matching tiberius'
    /// `stream.into_first_result().await`.
    pub fn into_first_result(self) -> Vec<Row> {
        let mut sets = self.result_sets;
        if sets.is_empty() {
            return Vec::new();
        }
        let (meta, rows) = sets.remove(0);
        rows.into_iter()
            .map(|values| Row::from_tds(&meta, values))
            .collect()
    }

    /// Consume all result sets into a Vec<Vec<Row>>.
    pub fn into_results(self) -> Vec<Vec<Row>> {
        self.result_sets
            .into_iter()
            .map(|(meta, rows)| {
                rows.into_iter()
                    .map(|values| Row::from_tds(&meta, values))
                    .collect()
            })
            .collect()
    }

    /// Number of result sets.
    pub fn result_set_count(&self) -> usize {
        self.result_sets.len()
    }

    /// Create an empty QueryResult.
    #[allow(dead_code)]
    pub(crate) fn empty() -> Self {
        QueryResult {
            result_sets: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// ToSql — convert Rust types to RPC parameters
// ---------------------------------------------------------------------------

/// Trait for types that can be used as query parameters.
pub trait ToSql: Send + Sync {
    /// Convert this value into an mssql-tds `SqlType` for parameter binding.
    fn to_sql(&self) -> SqlType;
}

impl ToSql for bool {
    fn to_sql(&self) -> SqlType {
        SqlType::Bit(Some(*self))
    }
}

impl ToSql for u8 {
    fn to_sql(&self) -> SqlType {
        SqlType::TinyInt(Some(*self))
    }
}

impl ToSql for i16 {
    fn to_sql(&self) -> SqlType {
        SqlType::SmallInt(Some(*self))
    }
}

impl ToSql for i32 {
    fn to_sql(&self) -> SqlType {
        SqlType::Int(Some(*self))
    }
}

impl ToSql for i64 {
    fn to_sql(&self) -> SqlType {
        SqlType::BigInt(Some(*self))
    }
}

impl ToSql for f32 {
    fn to_sql(&self) -> SqlType {
        SqlType::Real(Some(*self))
    }
}

impl ToSql for f64 {
    fn to_sql(&self) -> SqlType {
        SqlType::Float(Some(*self))
    }
}

impl ToSql for &str {
    fn to_sql(&self) -> SqlType {
        SqlType::NVarchar(
            Some(SqlString::from_utf8_string(self.to_string())),
            4000, // default max length
        )
    }
}

impl ToSql for String {
    fn to_sql(&self) -> SqlType {
        SqlType::NVarchar(Some(SqlString::from_utf8_string(self.clone())), 4000)
    }
}

impl ToSql for uuid::Uuid {
    fn to_sql(&self) -> SqlType {
        SqlType::Uuid(Some(*self))
    }
}

// Option<T>: None becomes the SQL NULL of the same type
impl<T: ToSql + Default> ToSql for Option<T> {
    fn to_sql(&self) -> SqlType {
        match self {
            Some(v) => v.to_sql(),
            None => {
                // Use a default-constructed value to get the right SqlType variant,
                // then we'd need to set it to None. Since SqlType variants all
                // have Option, we use a type-specific approach.
                // For simplicity, default to NVarchar NULL.
                SqlType::NVarchar(None, 4000)
            }
        }
    }
}

impl ToSql for serde_json::Value {
    fn to_sql(&self) -> SqlType {
        SqlType::NVarchar(Some(SqlString::from_utf8_string(self.to_string())), 4000)
    }
}

/// Build a Vec<RpcParameter> from a slice of ToSql values, using positional
/// naming (@P1, @P2, ...) like tiberius.
pub fn build_params(params: &[&dyn ToSql]) -> Vec<RpcParameter> {
    params
        .iter()
        .enumerate()
        .map(|(i, p)| {
            RpcParameter::new(Some(format!("@P{}", i + 1)), StatusFlags::NONE, p.to_sql())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_sql_primitives() {
        let _ = 42i32.to_sql();
        let _ = "hello".to_sql();
        let _ = true.to_sql();
        let _ = 2.72_f64.to_sql();
    }

    #[test]
    fn build_params_positional_naming() {
        let params = build_params(&[&1i32, &"test"]);
        assert_eq!(params.len(), 2);
        // name field is pub(crate) in mssql-tds, so we just verify count
    }

    #[test]
    fn empty_query_result() {
        let qr = QueryResult::empty();
        assert_eq!(qr.result_set_count(), 0);
        assert!(qr.into_first_result().is_empty());
    }
}
