# Arrow query reads

Enable the existing `arrow` feature to decode SQL results directly into
Arrow 55 record batches. The existing row APIs and Arrow bulk-insert APIs
are unchanged.

```toml
mssql-tiberius-bridge = { version = "0.1.0", features = ["arrow"] }
```

## Read batches

```rust
use futures_util::TryStreamExt;
use mssql_tiberius_bridge::{ArrowOptions, Client, Result};

async fn read(client: &mut Client) -> Result<()> {
    let options = ArrowOptions {
        batch_size: 8192,
        batch_bytes: 8 * 1024 * 1024,
        max_row_bytes: None,
        max_value_bytes: None,
    };
    let mut stream = client.query_arrow_with_options(
        "SELECT id, name FROM dbo.users WHERE id >= @P1",
        &[&100i32],
        options,
    );
    while let Some(result) = stream.try_next().await? {
        let batch = result.batch;
        println!("result {}: {} rows", result.result_index, batch.num_rows());
        // Consume/write this batch before requesting another one.
    }
    Ok(())
}
```

| Method | Parameters | Options |
|---|---|---|
| `query_arrow(sql, params)` | Positional `@P1`, `@P2`, ... | Defaults |
| `query_arrow_with_options(sql, params, options)` | Positional | Explicit |
| `simple_query_arrow(sql)` | None | Defaults |
| `simple_query_arrow_with_options(sql, options)` | None | Explicit |

These methods return an `ArrowStream` directly, not a future. Execution starts
when the stream is first polled. Parameters use the same binding and
`send_string_parameters_as_unicode` configuration as ordinary queries.
The stream borrows the client until it is exhausted or dropped.

`ArrowBatch` contains `result_index: usize` and `batch: RecordBatch`. The index
is zero-based among result sets, ignoring statements without column metadata.
Each empty result set produces one zero-row batch with its schema. A batch
never mixes result sets, even when consecutive schemas are identical.
Affected-row counts are not included; use `execute` for those.

Use `TryStreamExt::try_collect::<Vec<_>>()` when collecting everything is
intentional. Collection retains the entire result and defeats streaming's
bounded-batch memory advantage. There is no background prefetch queue.

## Batch and value limits

`ArrowOptions::default()` uses 8,192 rows and an **8 MiB soft byte target**,
with no application-level row/value rejection limit. All configured limits
must be nonzero.

The byte target counts fixed-width slots (including NULLs), one byte per
validity slot, an offset per variable-width value, converted payload, and
nested children. It excludes schema, scratch buffers and allocated capacity;
it is not RSS. It is checked after a row completes and can be
exceeded by one row. A row exceeding the target is emitted as its own batch,
without truncation. Splitting an oversized row uses Arrow slices; neighboring
batches may share backing buffers, so retaining a small slice can retain a
larger allocation.

Set `max_row_bytes` and/or `max_value_bytes` for explicit rejection policies.
The row limit includes the accounting overhead above. The value limit excludes
validity/offset overhead and checks converted payload length; strings, binary
and vectors additionally check available raw wire payload lengths. Thus a
UTF-16 string can exceed the value cap even if its UTF-8 representation fits.
Exceeded limits return `Error::Conversion`, never partial/truncated values.
They are checked during conversion, before further bridge copying where
possible. These limits do **not** prevent the underlying driver from buffering
an entire LOB before delivering it to the writer, do not limit allocations in
native TLS libraries, and are not a process-memory ceiling. Scratch and Arrow
buffers can retain capacity, and consumers can retain arbitrarily many batches.

## SQL-to-Arrow mapping

The schema is derived from TDS metadata before fetching rows, including for
empty result sets. Names, duplicate names and nullability are preserved.
Fields also retain SQL-specific information in metadata using `mssql.*` keys.

| SQL Server type | Arrow type |
|---|---|
| bit | Boolean |
| tinyint / smallint / int / bigint | UInt8 / Int16 / Int32 / Int64 |
| real / float | Float32 / Float64 |
| decimal / numeric | Decimal128 with declared precision and scale |
| money / smallmoney | Decimal128(19,4) / Decimal128(10,4) |
| char / varchar / nchar / nvarchar / text / ntext | LargeUtf8 |
| XML / JSON | LargeUtf8 |
| binary / varbinary / image / CLR UDTs | LargeBinary |
| geography / geometry | LargeBinary containing native SQL serialization, not WKB |
| uniqueidentifier | Utf8, canonical hyphenated UUID |
| date | Date32, days since 1970-01-01 |
| time | Time64(Nanosecond) |
| datetime2 / smalldatetime | Struct with `date: Date32`, `time: Time64(Nanosecond)` |
| datetime | Struct with `date: Date32`, `ticks_300: UInt32` |
| datetimeoffset | Struct with UTC `date: Date32`, `time: Time64(Nanosecond)`, `offset_minutes: Int16` |
| vector (Float32) | FixedSizeList of Float32 with declared dimensions |

Temporal structs avoid silently dropping the seventh fractional digit or
overflowing Arrow nanosecond timestamps for dates outside approximately
1677-2262. `datetime.ticks_300` preserves the exact number of 1/300-second
ticks since midnight; these values cannot always be represented exactly in
integer nanoseconds. `datetimeoffset` preserves its per-row offset as well as
the UTC wire date/time. NULL temporal values are NULL parent structs, not a
non-null struct whose children happen to be NULL.

No decimal or money conversion goes through floating point.
Read mappings are not a promise of automatic bulk-insert round trips:
`send_arrow` retains its existing supported-type contract and does not accept
these lossless temporal structs.

Strings are decoded with the upstream encoding family, with reusable UTF-8
scratch storage. UTF-16 replacement decoding preserves the bridge's behavior
for malformed surrogate sequences; invalid UTF-8 is an explicit conversion
error instead of a panic. Narrow strings follow the upstream collation
resolver, including its warning/fallback for unknown LCIDs. This API does not
provide an independent collation database.

Float16 vectors, `sql_variant`, encrypted columns whose plaintext schema is
unavailable, and unsupported metadata types are rejected explicitly, even for empty results.
Cast unsupported columns to a supported SQL type rather than relying on
inference from the first value or silent stringification. Float16 vectors are
not silently widened; cast to a Float32 vector or a supported string type
explicitly. Consult the `arrow` module documentation for exact field metadata.

## Errors, cancellation and pooling

After execution starts, **fully exhaust the stream to reuse its connection**.
On an execution/schema/conversion failure, cancelled fetch, or early drop,
the connection is marked dead. Pools discard it instead of recycling a
possibly partially decoded row. This intentionally avoids draining arbitrary
remaining values after a caller has cancelled or exceeded a limit.

Dropping an unpolled stream and failing options validation do not start SQL or
poison the connection. Fully consumed successful queries remain reusable.

Previously yielded batches remain valid after an error; the unfinished batch
is not published. Applications exporting to an external destination must
decide whether to discard already-written output or keep a partial export.
Neither the stream nor the writer rolls back external side effects or commits
SQL transactions.

See [Arrow performance](arrow-performance.md) for the experiment that led to
this implementation and the distinction between allocation traffic and peak
memory.
