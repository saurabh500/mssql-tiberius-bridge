//! Native row-writer callbacks for the checked [`Client`](crate::Client) API.
//!
//! These are re-exports of the pinned-compatible `mssql-tds` API, not bridge
//! wrappers. They version-couple this public surface to the native driver.
//! Consumers need only a direct dependency on `mssql-tiberius-bridge`.
//!
//! Callbacks run during decoding. Borrowed string/binary bytes are valid only
//! for that callback: copy or transcode into caller-owned storage before returning.
//! Owned and borrowed bytes must receive the same conversion/error treatment.
//! `RowWriter` callbacks cannot fail; latch conversion errors in your writer,
//! track column order/count and `end_row`, and inspect them before publishing a
//! row, even when `Client::next_row_into` returns `Ok(true)`.
//!
//! A decoding error or dropped pending read can leave partially written data.
//! Never publish that row. If using `value_destination`, follow its native
//! initialization contract: `commit_value(false)` cannot be read, and a dropped
//! future may never call `commit_value` at all. Discard uncommitted storage.
//! A completed consumer conversion error does not itself damage the connection;
//! drain with `Client::close_query` or discard the client before returning it
//! to a pool. Dropped pending I/O marks the client dead and must not be resumed.
//!
//! Refill size is controlled by the consumer (for example, 32 successful rows
//! per `block_on`); a row-count bound is **not** a byte bound for MAX values.

pub use mssql_tds::datatypes::column_values::{
    SqlDate, SqlDateTime, SqlDateTime2, SqlDateTimeOffset, SqlMoney, SqlSmallDateTime,
    SqlSmallMoney, SqlTime, SqlXml,
};
pub use mssql_tds::datatypes::decoder::DecimalParts;
pub use mssql_tds::datatypes::row_writer::{RowWriter, ValueKind};
pub use mssql_tds::datatypes::sql_json::SqlJson;
pub use mssql_tds::datatypes::sql_string::{EncodingType, SqlString};
pub use mssql_tds::datatypes::sql_vector::SqlVector;
pub use mssql_tds::datatypes::sqldatatypes::TdsDataType;
pub use mssql_tds::error::Error as TdsError;
pub use mssql_tds::query::metadata::ColumnMetadata;
pub use uuid::Uuid;
