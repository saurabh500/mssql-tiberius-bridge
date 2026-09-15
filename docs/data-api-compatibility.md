# Tiberius public data API compatibility

This contract is grounded in `saurabh500/tiberius` commit
`e01b6a433ff97f91afb3db75852eab879f2927b2` (baseline
`c1d2e741ee6fe0935091826d91fb5886b6a4082a`), specifically
`DATA_API_COVERAGE.md`, `tests/data_api.rs`, `tests/query.rs`, and
`tests/bulk.rs`. Connection construction and all connection-management
behavior are excluded.

The machine-readable traceability matrix is `TRACEABILITY` in
`tests/data_api_compat.rs`. Its 40 scenarios currently classify as:

| Status | Scenarios |
|---|---:|
| Passes with the unchanged Tiberius-facing call | 7 |
| Passes through the current bridge API | 19 |
| Compile/API gap | 11 |
| Behavioral gap | 1 |
| Intentional bridge improvement | 2 |

The behavioral gap is wrong-type row extraction: Tiberius returns a conversion
error while the bridge's current `FromSql` contract returns `None`. The two
intentional improvements are recoverable out-of-range numeric access and
preservation of an empty middle result set in collected results. Tiberius
panics for the former and drops the empty set when consecutive metadata items
are collected for the latter.

Issue #125 is available through the separate `compat` module and the
bridge-native `Client::query_compat` / `Client::simple_query_compat` entry
points. They expose `QueryStream`, `QueryItem`, `ResultMetadata`, columns, and
zero-based result indexes while reusing the native wire stream. Its collectors
intentionally preserve every metadata boundary, including empty middle and
trailing result sets; the pinned Tiberius implementation can drop those empty
sets. Errors returned while `QueryStream::columns()` looks ahead are delivered
once because the bridge's native error is not cloneable; a later stream poll
continues after that error. Existing buffered and row-only streaming methods
keep their contracts.

## Phase 3 order

| Order | Logical API | Issue | Compile fixtures |
|---:|---|---|---|
| 1 | Query stream/items, metadata, result indexes, async collectors | [#125](https://github.com/saurabh500/mssql-tiberius-bridge/issues/125) | `data_api_query_stream.rs`, `data_api_query_stream_collectors.rs` |
| 2 | Public conversion traits and error channel | [#128](https://github.com/saurabh500/mssql-tiberius-bridge/issues/128) | `data_api_conversions.rs`, `data_api_conversion_errors.rs` |
| 3 | Dynamic `Query` builder | [#127](https://github.com/saurabh500/mssql-tiberius-bridge/issues/127) | `data_api_query_builder.rs` |
| 4 | `ExecuteResult` access and standard iteration | [#131](https://github.com/saurabh500/mssql-tiberius-bridge/issues/131) | `data_api_execute_rows_affected.rs`, `data_api_execute_into_iterator.rs` |
| 5 | Row cell/consuming iteration | [#130](https://github.com/saurabh500/mssql-tiberius-bridge/issues/130) | `data_api_row_iteration.rs` |
| 6 | `TokenRow`, `IntoRow`, and incremental bulk lifecycle | [#129](https://github.com/saurabh500/mssql-tiberius-bridge/issues/129) | `data_api_bulk_row.rs`, `data_api_bulk_lifecycle.rs` |

Unimplemented fixtures are expected compile failures in `tests/compile_fail`.
Implementing an issue means moving the same source to `tests/pass`; changing a
probe to fit an incompatible API does not satisfy the issue.
