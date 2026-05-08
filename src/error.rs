//! Error types for mssql-tiberius-bridge.
//!
//! All fallible operations in this crate return [`Result<T>`], which uses
//! the unified [`Error`] enum. Errors from the underlying `mssql-tds` driver
//! are wrapped in [`Error::Tds`].

use std::fmt;

/// Unified error type for all mssql-tiberius-bridge operations.
///
/// Wraps errors from the underlying `mssql-tds` driver alongside
/// facade-specific errors for column access and type conversion.
#[derive(Debug)]
pub enum Error {
    /// An error from the underlying mssql-tds TDS protocol driver.
    ///
    /// This includes connection failures, authentication errors,
    /// SQL syntax errors, and protocol-level issues.
    Tds(mssql_tds::error::Error),

    /// A column was requested by name but does not exist in the result set.
    ///
    /// Returned by [`Row::try_get()`](crate::Row::try_get) when the column
    /// name doesn't match any column in the row.
    ColumnNotFound(String),

    /// A column was requested by index but the index exceeds the column count.
    ///
    /// Returned by [`Row::try_get()`](crate::Row::try_get) when the numeric
    /// index is out of bounds.
    ColumnIndexOutOfBounds {
        /// The requested column index.
        index: usize,
        /// The actual number of columns in the row.
        count: usize,
    },

    /// A type conversion failed when extracting a column value.
    Conversion(String),

    /// A connection pool error occurred.
    Pool(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Tds(e) => write!(f, "TDS error: {e}"),
            Error::ColumnNotFound(name) => write!(f, "Column not found: {name}"),
            Error::ColumnIndexOutOfBounds { index, count } => {
                write!(f, "Column index {index} out of bounds (count: {count})")
            }
            Error::Conversion(msg) => write!(f, "Conversion error: {msg}"),
            Error::Pool(msg) => write!(f, "Pool error: {msg}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Tds(e) => Some(e),
            _ => None,
        }
    }
}

impl From<mssql_tds::error::Error> for Error {
    fn from(e: mssql_tds::error::Error) -> Self {
        Error::Tds(e)
    }
}

/// Result type alias using [`Error`] for all mssql-tiberius-bridge operations.
pub type Result<T> = std::result::Result<T, Error>;
