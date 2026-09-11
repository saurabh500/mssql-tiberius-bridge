# Direct Arrow output experiment

Tracking: [prototype and evaluation issue](https://github.com/saurabh500/mssql-tiberius-bridge/issues/112).

## Decision

**Keep this change benchmark-only; do not ship the fixture-specific adapter as
production code.** The results justify developing an opt-in, production-quality
Arrow query-output path, particularly for its allocation reduction, but not
replacing the current row API.

No public methods, return types, existing feature defaults, or production
implementations were changed. The existing Arrow bulk-insert API is unchanged.
The prototype is entirely in [`benches/arrow_comparison.rs`](../benches/arrow_comparison.rs).
Its only dependency addition is benchmark/dev access to the already-resolved
`encoding_rs` decoder.

On the measured workloads, direct Arrow output with reusable text decoding
improved throughput over **bridge rows converted to Arrow** by:

| Workload | Throughput improvement |
|---|---|
| 1 million numeric rows, 4 columns | 1.6-3.1% across forward/reverse runs |
| 1 million mixed rows, 4 columns | 4.6-6.5% across forward/reverse runs |
| 100,000 numeric rows, 16 columns | 4.5%, one run |
| 100,000 mixed rows, 16 columns | 7.4%, one run |

The more substantial result was **99.6-99.7% fewer Rust allocation/reallocation
calls** and **78.8-93.2% fewer requested allocation bytes** on the million-row
cases. These are allocation-traffic reductions, **not peak-RAM reductions**.
They do not establish a corresponding concurrency or application-export speedup.

## What was implemented

All four paths produce the same Arrow 55.2.0 `RecordBatch` schema, values,
null bitmaps and batch boundaries:

| Path | Implementation |
|---|---|
| `tiberius_arrow` | Tiberius wire stream -> borrowed typed row getters -> Arrow builders |
| `bridge_arrow` | Bridge wire stream -> borrowed typed row getters -> the same Arrow builders |
| `direct_arrow` | mssql-tds RowWriter -> Arrow builders; temporary UTF-8 string per text cell |
| `direct_arrow_reuse` | Direct writer, with a reusable UTF-8 scratch buffer and borrowed UTF-8 fast path |

The direct paths bypass `Row`, its two vectors, retained raw text bytes, and
intermediate owned binary values when upstream supplies borrowed bytes.
Arrow still owns its final column buffers; this is not zero-copy networking.

The reusable path decodes UTF-16 with `encoding_rs::UTF_16LE.new_decoder()` into
a retained `String`, then appends to the Arrow values buffer. This removes the
per-cell temporary UTF-8 allocation, not the required transcoding or final
append/copy. It uses the same decoder family and replacement/BOM behavior as
`SqlString` for the supported encodings.

An initial prototype with allocation instrumentation present in timing builds
was used only for exploration. **All timing tables below come from binaries
without the allocator hook.** Allocation profiling is a separate build using
the internal `_bench_alloc` feature; the harness rejects mixing its timing
and allocation modes.

## Measurement setup

Measured on 2026-09-11, bridge base `3240343`, with the benchmark changes in this
session. The machine, released drivers, and isolated SQL Server configuration
match the [driver comparison](performance.md#measured-comparison):
Rust 1.93.0 release profile, Tiberius 0.12.3, mssql-tds 0.1.0,
SQL Server 2025 RTM 17.0.1000.7, required TLS, loopback networking, 4 server CPUs,
4 GiB container RAM, and a 2 GiB SQL memory target.

The numeric and mixed four-column fixtures are shared with the driver
comparison. The 16-column case repeats the four-column projection four times
with unique column names. An index is created outside timing, and queries use
`ORDER BY id` so exact row/batch equivalence can be checked.

Connections and fixture creation are outside timing. Each timed iteration
includes query submission, wire reads, UTF-8 decoding, Arrow construction,
final partial-batch handling, and destruction of all rows and batches.
The consumer black-boxes and immediately releases each completed batch;
it does not retain the entire dataset or serialize it to IPC, Parquet, or S3.
At most one completed batch plus the writer's next buffers are held by this
consumer.

Before measurement, every output batch is compared with independently generated
expected values, not just a row count. Additional self-checks cover NULLs,
empty results, exact/partial batches, batch sizes 1/8/33/64, four and sixteen
columns, Unicode and binary values. Reused decoding is compared with the
upstream decoder for borrowed/owned UTF-16, BOMs, malformed surrogate/odd-byte
input, supplementary characters, valid UTF-8, and retained batches.

Criterion uses 20 samples, a 2-second warm-up and a 5-second measurement target
that expands for slow cases. The main experiment was repeated in reverse method
order. The following percentages are ratios of point estimates, not guarantees.
This is a shared host and not a CPU-isolated laboratory.

## Million-row results

Four columns, batches of **8,192 rows**. Values are milliseconds for the complete
million-row result: **mean [95% confidence interval]**.

| Workload | Path | Forward order | Reverse order |
|---|---|---|---|
| Numeric | Tiberius -> Arrow | 609.32 [602.56, 616.36] | 599.44 [594.08, 606.08] |
| Numeric | Bridge rows -> Arrow | 456.91 [453.77, 460.79] | 432.37 [429.68, 435.30] |
| Numeric | Direct Arrow | 440.67 [438.17, 443.29] | 420.69 [417.93, 424.04] |
| Numeric | Direct Arrow + reuse | 449.53 [443.86, 455.79] | 419.40 [415.70, 423.81] |
| Mixed | Tiberius -> Arrow | 1717.79 [1694.06, 1746.87] | 1713.46 [1708.73, 1718.51] |
| Mixed | Bridge rows -> Arrow | 600.67 [594.45, 607.51] | 606.47 [599.89, 613.83] |
| Mixed | Direct Arrow | 576.89 [569.90, 585.37] | 578.42 [572.45, 584.54] |
| Mixed | Direct Arrow + reuse | 574.24 [568.66, 580.03] | 569.23 [564.94, 574.28] |

Numeric data contains no strings, so the two direct variants perform the same
logical work there; their differing point estimates illustrate run variability.
The repeatable mixed-data gain is modest, not an order-of-magnitude change.

### Wider results

100,000 rows, sixteen columns, batches of 8,192 rows; one forward-order run.
Times are milliseconds, mean [95% CI].

| Workload | Tiberius -> Arrow | Bridge rows -> Arrow | Direct Arrow | Direct Arrow + reuse |
|---|---|---|---|---|
| Numeric | 191.22 [188.82, 193.98] | 98.76 [97.81, 99.71] | 95.17 [93.20, 97.67] | 94.48 [93.49, 95.51] |
| Mixed | 608.52 [605.00, 612.81] | 172.72 [170.30, 175.67] | 165.17 [162.20, 168.86] | 160.82 [158.84, 163.08] |

### Larger batches

One million rows, four columns, **65,536-row batches**; one forward-order run.
Only the bridge baseline and reused direct writer were timed.

| Workload | Bridge rows -> Arrow | Direct Arrow + reuse | Throughput improvement |
|---|---|---|---|
| Numeric | 430.31 [426.39, 435.34] ms | 421.84 [418.71, 424.98] ms | 2.0% |
| Mixed | 597.78 [593.16, 602.54] ms | 580.81 [572.88, 591.53] ms | 2.9% |

Larger batches did not provide an obvious win over 8,192 rows on this setup.
They also retain larger buffers and delay the first batch. First-batch latency
was not measured, so no latency improvement is claimed.

## Allocation results

One fully consumed query after preflight, one million rows, four columns,
8,192-row batches. Profiling uses a delegating `System` allocator with counting
enabled only around the query/Arrow-consumption operation, excluding connection
setup, fixtures, expected values and reporting.

| Workload | Path | Allocation/reallocation calls | Total requested bytes |
|---|---|---:|---:|
| Numeric | Tiberius -> Arrow | 1,007,923 | 276,829,654 |
| Numeric | Bridge rows -> Arrow | 2,005,292 | 308,955,148 |
| Numeric | Direct Arrow | 5,277 | 20,940,134 |
| Numeric | Direct Arrow + reuse | 5,277 | 20,940,134 |
| Mixed | Tiberius -> Arrow | 5,618,498 | 483,529,158 |
| Mixed | Bridge rows -> Arrow | 4,711,037 | 484,406,356 |
| Mixed | Direct Arrow | 916,601 | 151,857,001 |
| Mixed | Direct Arrow + reuse | 16,603 | 102,657,232 |

These count successful Rust global allocator calls, including reallocations.
Requested bytes include the full new size of a reallocation, not just its growth.
They do not count native-library allocations that bypass Rust's allocator or
server allocations, and are neither live bytes nor peak RSS. Remaining direct
allocations include Arrow batch storage and upstream owned-value fallbacks at
buffer boundaries. There is no claim that this sink is allocation-free.

## Product/API evaluation

The allocation evidence makes this a worthwhile **optional export feature**.
It does not justify an automatic change to existing `query`, `query_streamed`,
`Row`, `FromSql`, `raw_value`, or Arrow bulk-insert semantics.

First-class query output would require an **additive** surface, such as a
`query_arrow_batches` method under the existing `arrow` feature. That can be
backward-compatible, but it is not literally zero API additions: the current
product has no Arrow query-output method to optimize internally. Callers can
already experiment through `inner_mut()` without any public API addition.

Before adopting a first-class API, replace the fixture-specific assumptions:

1. Derive schemas from actual metadata and define all supported SQL-to-Arrow
   mappings, including decimals, temporal values, collations, spatial and
   MAX/PLP values. Unsupported types must return errors, not benchmark panics.
2. Support parameterized queries, empty/multiple result-set metadata and
   schema transitions, without silently mixing batches from different schemas.
3. Add explicit sink errors, partial-row rollback, cancellation/drop handling,
   and connection draining or invalidation. RowWriter callbacks return `()`,
   so failures need a checked error state outside the callbacks.
4. Define row, byte, and individual-value limits with asynchronous backpressure.
   A row-count limit alone cannot bound arbitrary LOB materialization.
5. Measure a real Arrow/Parquet export workload, peak RSS and concurrent
   consumers. The present result measures RecordBatch production, not storage
   serialization, compression, networking to an object store, or memory pressure.

There is no production merge recommendation for the current prototype as-is.
Its unsupported-type panics and predeclared schemas are deliberate constraints
of an experiment, not a proposed product contract.

## Reproduce

Use the isolated-server setup in [performance.md](performance.md#reproduce).
It creates only connection-local temporary tables. Then run:

```bash
# Set BENCH_DB_PASSWORD and the matching BENCH_DB_HOST/PORT first.
# Normal timing builds do not include the allocation hook.
BENCH_ROW_COUNTS=1000000 cargo bench --locked --features _bench,arrow \
  --bench arrow_comparison -- --noplot --save-baseline arrow-final-forward
BENCH_ROW_COUNTS=1000000 BENCH_REVERSE=1 cargo bench --locked \
  --features _bench,arrow --bench arrow_comparison -- \
  --noplot --save-baseline arrow-final-reverse

BENCH_ROW_COUNTS=100000 BENCH_ARROW_REPEATS=4 cargo bench --locked \
  --features _bench,arrow --bench arrow_comparison -- \
  --noplot --save-baseline arrow-wide

BENCH_ROW_COUNTS=1000000 BENCH_ARROW_BATCH_ROWS=65536 cargo bench --locked \
  --features _bench,arrow --bench arrow_comparison -- \
  '(bridge_arrow|direct_arrow_reuse)' --noplot --save-baseline arrow-large-batch

# Separate allocation-only build: no Criterion timing is performed.
BENCH_ROW_COUNTS=1000000 BENCH_ARROW_ALLOCATIONS=1 cargo bench --locked \
  --features _bench_alloc,arrow --bench arrow_comparison -- --noplot
```

`BENCH_ARROW_REPEATS` accepts comma-separated values 1-8, default `1`; each
repetition adds four columns. `BENCH_ARROW_BATCH_ROWS` accepts comma-separated
values 1-65,536, default `8192`. Other connection/count/order settings are shared
with `driver_comparison`. Correctness preflight runs even for filtered cases.
Raw samples and estimates are under `target/criterion/arrow_*`.

The isolated SQL Server container was removed after these measurements.
