//! Error types for mssql-facade.

use std::fmt;

/// Unified error type wrapping mssql-tds errors and facade-specific errors.
#[derive(Debug)]
pub enum Error {
    /// An error from the underlying mssql-tds driver.
    Tds(mssql_tds::error::Error),
    /// Column not found by name.
    ColumnNotFound(String),
    /// Column index out of bounds.
    ColumnIndexOutOfBounds { index: usize, count: usize },
    /// Type conversion error.
    Conversion(String),
    /// Pool error.
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

/// Result type alias for mssql-facade operations.
pub type Result<T> = std::result::Result<T, Error>;
