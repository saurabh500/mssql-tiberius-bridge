# Tiberius comparison and RowWriter opportunities

## Findings

The bridge **already uses RowWriter** in both `Client::query_streamed` and
`Client::collect_results`. Adding RowWriter to those paths is not a new
optimization. In the measurements below, bridge streaming delivered about
**1.3x Tiberius's numeric throughput and 2.7-2.8x its mixed-data throughput**.

Bypassing bridge rows with a direct RowWriter checksum sink did **not**
demonstrate a substantial additional throughput improvement for these narrow
datasets. In the reverse-order run its point estimate was only 0.8% faster for
numeric data and 1.2% faster for mixed data, with overlapping or nearly touching
confidence intervals. One forward numeric measurement was much slower and
highly variable. Do not promise another large speedup simply from a different
RowWriter adapter.

The architectural follow-up is an **optional direct query-output API**,
implemented as [Arrow reads](arrow-reads.md), rather than changing the
owned, Tiberius-compatible `Row` contract. The
[Arrow follow-up](arrow-performance.md) now measures actual RecordBatch
construction and allocation reductions. Full application export throughput
and peak-memory benefits remain unmeasured.

## Measured comparison

Measured on 2026-09-11 from bridge base commit `3240343`, with the added
[`driver_comparison` benchmark](../benches/driver_comparison.rs).
No production query or row implementation was changed.

| Setting | Value |
|---|---|
| Drivers | Tiberius 0.12.3; mssql-tds 0.1.0 |
| Build | Rust 1.93.0, Cargo bench/release profile, locked dependencies |
| Host | Linux/WSL, AMD EPYC 7763, 32 logical CPUs, approximately 63 GiB RAM |
| Server | SQL Server 2025 RTM, 17.0.1000.7, isolated local Docker container |
| Container limits | 4 CPUs, 4 GiB RAM, SQL memory target 2 GiB |
| Transport | Loopback TCP; required TLS and trusted test certificate for both drivers |
| Runtime | One current-thread Tokio runtime; reused connections |
| Sampling | Criterion 0.5.1; 20 samples, 2-second warm-up, 5-second target extended for slow cases |

The image ID was
`sha256:55c3fe0f8428be3d5d5e009739e900b8e7ec3759c111613409d65f0cfcd8464b`.
Driver transport defaults were retained; packet sizes were not independently
normalized. The repository's dev-dependency `mssql-tds/test-util` feature was
enabled, using real network connections rather than a mock transport.

Each workload contains **1,000,000 rows and four columns**:

| Workload | Columns |
|---|---|
| Numeric | INT, nullable BIGINT, FLOAT, BIT |
| Mixed | INT, nullable NVARCHAR(100), FLOAT, nullable VARBINARY(32) |

Every tenth nullable cell is NULL. Text includes ASCII, an accented character,
and Chinese characters. Both drivers receive identical generated values in
separate connection-local temporary tables.

Times below are milliseconds per complete result, expressed as
**mean [95% confidence interval]**. Both orders are reported rather than selecting
only favorable results.

| Workload | Path | Forward order | Reverse order |
|---|---|---|---|
| Numeric | Tiberius streaming | 628.99 [622.12, 636.74] | 609.90 [605.33, 615.67] |
| Numeric | Bridge streaming | 480.88 [470.22, 494.01] | 464.71 [460.03, 470.11] |
| Numeric | Bridge buffered | 559.31 [549.98, 569.69] | 542.98 [540.51, 545.66] |
| Numeric | Direct RowWriter | 788.54 [663.75, 920.90] | 460.98 [457.98, 464.34] |
| Mixed | Tiberius streaming | 1796.64 [1756.55, 1850.02] | 1689.42 [1676.81, 1705.89] |
| Mixed | Bridge streaming | 641.20 [636.22, 646.14] | 627.98 [623.39, 632.97] |
| Mixed | Bridge buffered | 951.28 [939.61, 964.72] | 938.91 [925.31, 955.49] |
| Mixed | Direct RowWriter | 634.77 [625.20, 646.97] | 620.77 [618.01, 623.54] |

For the reverse-order run, the corresponding throughput is:

| Workload | Tiberius streaming | Bridge streaming | Bridge buffered | Direct RowWriter |
|---|---|---|---|---|
| Numeric | 1.640 M rows/s | 2.152 M rows/s | 1.842 M rows/s | 2.169 M rows/s |
| Mixed | 0.592 M rows/s | 1.592 M rows/s | 1.065 M rows/s | 1.611 M rows/s |

### What the benchmark includes

Timing starts before query submission and ends after all values have been
consumed and all result rows destroyed. Connection establishment, fixture
creation, and expected-checksum generation are outside timing. Every path
checks the row count and a checksum against independently generated expected
values, including NULL markers, column positions, all text characters, and all
binary bytes. The checksum is order-independent because the query does not
promise row order.

The direct writer uses `Client::inner_mut().next_row_into(...)`. It decodes
strings to UTF-8 with `SqlString::decode` before the shared checksum, rather
than hashing raw UTF-16 wire bytes. It avoids owned rows and binary copies but
still allocates decoded text. It is a **decode-and-consume sink**, not a
replacement implementation of the owned-row API or an Arrow/Parquet exporter.

These are end-to-end local query-consumption measurements, not isolated decoder
CPU measurements. Fixed method order, shared-host activity, server behavior,
and allocator state can affect results; reversing the order exposed variability.
No allocator counts, client CPU profiles, peak RSS, time-to-first-row, or
application export throughput were measured. Wide schemas, large/MAX values,
other collations, WAN latency, prepared queries, and concurrent consumers remain
outside this comparison.

## Why RowWriter is not currently zero-copy

[`BridgeRowWriter`](../src/row.rs) still constructs the compatibility
representation:

| Cost | Current implementation |
|---|---|
| Row containers | `take_row()` allocates replacement `values` and `decoded_strings` vectors for each nonempty row |
| Numeric-only rows | The string-cache vector still holds a `None` entry for every column |
| Text storage | `write_string()` retains owned wire bytes in `SqlString` plus a decoded UTF-8 `String` |
| Borrowed binary input | `Cow::into_owned()` copies it so returned rows can outlive the callback |
| Owned string access | `FromSql for String` decodes again instead of using the already-decoded cache |
| Metadata | Already shared per result set through `Arc<RowSchema>` |

Moving a vector into an independently owned row prevents simply reusing that
same allocation for the next row. `raw_value()` also exposes the upstream value
representation, so dropping raw string storage silently would change observable
behavior.

Tiberius 0.12.3 also shares metadata and supports real wire streaming; it does
not allocate a separate heap object for every scalar. Its string decoder does,
however, build an intermediate UTF-16 vector, and its variable-length reader
performs per-byte async-reader operations. These are plausible contributors to
the mixed-data difference, not a measured attribution of the entire speedup.
See its [string decoder](https://github.com/prisma/tiberius/blob/c34fab2e14c52ab74519d073d7a7b65bd023fc1a/src/tds/codec/column_data/string.rs#L18-L40)
and [variable-length reader](https://github.com/prisma/tiberius/blob/c34fab2e14c52ab74519d073d7a7b65bd023fc1a/src/tds/codec/column_data/plp.rs#L8-L61).
An async read operation is not necessarily a syscall or scheduler switch.

Historical bridge PRs [#99](https://github.com/saurabh500/mssql-tiberius-bridge/pull/99)
and [#100](https://github.com/saurabh500/mssql-tiberius-bridge/pull/100) introduced
the current writer paths and reported a roughly 9% streaming improvement over
the previous bridge. Those figures were not comparisons against Tiberius and
used a different dataset/environment.

## How to improve large-result processing

1. **Preserve the compatibility API and remove avoidable work.** Investigate
   using the decoded cache for owned `String` getters, and avoiding a full
   string-cache allocation for schemas without strings. Preserve `raw_value()`,
   custom `FromSql` implementations, and existing malformed-string behavior.
   The main comparison uses cached `&str` getters, so it does not measure the
   possible benefit of fixing repeated owned-string decoding.

2. **Use the opt-in Arrow query-batch API for columnar consumers.**
   `query_arrow` and `simple_query_arrow` now implement
   `TDS decoder -> RowWriter -> typed column buffers -> RecordBatch`, bypassing
   both per-row vectors and retained raw-plus-decoded text. They are additive
   APIs under the existing `arrow` feature; Arrow bulk insertion is unchanged.
   Arrow 55 builders work without upgrading mssql-tds; RowWriter itself contains
   no Arrow-version-specific types. See [Arrow reads](arrow-reads.md) for limits
   and supported SQL mappings.

3. **Measure the actual destination before claiming a larger speedup.**
   Compare Tiberius-to-Arrow, bridge-rows-to-Arrow, and direct-writer-to-Arrow
   with identical final batches. Record allocations, peak RSS, CPU, rows/s,
   bytes/s, and first-batch latency on wide, string-heavy and MAX-value
   datasets. The checksum result above does not establish an Arrow speedup.

### Released API constraints

Published [mssql-tds 0.1.0](https://docs.rs/mssql-tds/0.1.0/mssql_tds/connection/tds_client/struct.TdsClient.html)
already provides the required generic
`next_row_into<W: RowWriter + Send + ?Sized>(&mut W)` API.
There is **no public `read_rows_into` batch method or built-in Arrow query
writer** in that release; the wrapper would own the batching loop.

The [RowWriter contract](https://docs.rs/mssql-tds/0.1.0/src/mssql_tds/datatypes/row_writer.rs.html#23-165)
requires careful handling:

- Callbacks are synchronous and return `()`. Surface sink errors explicitly
  after each fetch, and apply async output backpressure between batches.
- Borrowed callback bytes cannot escape the callback. Append/copy/transcode
  into owned batch storage; publish only completed rows.
- Handle metadata and empty/multiple result sets outside the writer. A batch
  must not mix incompatible schemas. Preserve connection cleanup after an
  error, cancellation, or early stop.
- Bound batches by rows and bytes. This alone does not bound a single large
  value: full-value callbacks may already have materialized it.
- `value_destination`/`commit_value` can avoid intermediate storage for
  known-length MAX string/binary PLP values. Unknown lengths and ordinary short
  values use other paths. Returning `None` selects fallback materialization,
  not size rejection. UTF-16 still needs transcoding for UTF-8 output.

`try_next_buffered_row_into` plus `finish_row_into` is a lower-level option, but
`next_row_into` already attempts synchronous buffered decoding. Hand-writing
that loop is not evidence of an additional throughput gain.

## Reproduce

The benchmark requires an explicit password and creates only connection-local
`#throughput_rows` tables, which disappear on disconnect. No permanent table is
created or dropped. Use a dedicated server because the workload can consume
substantial resources. The benchmark trusts the server certificate for local
testing.

With an already provisioned test server:

```bash
# Set BENCH_DB_PASSWORD in your environment; do not put credentials in source.
export BENCH_DB_HOST=127.0.0.1 BENCH_DB_PORT=1433
cargo bench --locked --features _bench --bench driver_comparison -- --noplot

# Reproduce the million-row comparisons above:
BENCH_ROW_COUNTS=1000000 cargo bench --locked --features _bench \
  --bench driver_comparison -- --noplot --save-baseline rowwriter-comparison
BENCH_ROW_COUNTS=1000000 BENCH_REVERSE=1 cargo bench --locked \
  --features _bench --bench driver_comparison -- \
  --noplot --save-baseline reverse-order
```

`BENCH_DB_USER` and `BENCH_DB_NAME` default to `sa` and `master`.
`BENCH_ROW_COUNTS` defaults to `100000,1000000` and accepts comma-separated
integers from 1 through 1,000,000. `BENCH_REVERSE=1` reverses the four methods;
its default is `0`. Criterion's filter argument can select a workload/path.

To provision and automatically remove an isolated local SQL Server:

```bash
(
  set -eu
  export BENCH_DB_PASSWORD="$(openssl rand -hex 24)Aa1!"
  export BENCH_DB_HOST=127.0.0.1 BENCH_DB_USER=sa BENCH_DB_NAME=master
  container="bridge-bench-$$"
  docker run --detach --name "$container" --cpus 4 --memory 4g \
    --publish 127.0.0.1::1433 --env ACCEPT_EULA=Y \
    --env MSSQL_PID=Developer --env MSSQL_MEMORY_LIMIT_MB=2048 \
    --env MSSQL_SA_PASSWORD="$BENCH_DB_PASSWORD" \
    mcr.microsoft.com/mssql/server:2025-latest
  trap 'docker rm --force "$container" >/dev/null' EXIT
  export BENCH_DB_PORT="$(docker port "$container" 1433/tcp | awk -F: '{print $NF}')"
  ready=0
  for attempt in $(seq 1 60); do
    if docker exec "$container" /bin/bash -c \
      'SQLCMDPASSWORD="$MSSQL_SA_PASSWORD" /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -C -Q "SELECT 1" -b' \
      >/dev/null 2>&1; then ready=1; break; fi
    sleep 2
  done
  if [ "$ready" != 1 ]; then docker logs "$container"; exit 1; fi
  cargo bench --locked --features _bench --bench driver_comparison -- --noplot
)
```

Criterion saves raw samples and estimates under
`target/criterion/<workload>/<method>/<row-count>/`. These are ignored build
artifacts; the tables above preserve both measured million-row runs.
This benchmark is separate from the older `row_streaming` benchmark, whose
fixed table and connection settings should not be used on a shared database.
