# mssql-tiberius-bridge

A tiberius-compatible API bridge over Microsoft's [`mssql-tds`](https://crates.io/crates/mssql-tds) crate. Migrate from tiberius with minimal code changes.

## Why?

[tiberius](https://github.com/prisma/tiberius) is the most popular Rust TDS driver, but it's community-maintained. Microsoft's `mssql-tds` is the official, supported implementation — but it has a different API surface.

**mssql-tiberius-bridge** gives you the tiberius API you know on top of the mssql-tds engine:

- `row.get::<T, _>("column_name")` — named and indexed column access
- `stream.into_first_result().await?` — collect the first result into `Vec<Row>` and drain the rest
- `stream.into_row_stream()` — wire-level `Stream<Item = Result<Row>>`
- `client.query_streamed(sql, params)` / `simple_query_streamed(sql)` — true wire-level row streaming for memory-bounded large result sets
- `stream.columns().await?` — inspect metadata even for empty result sets
- `client.ping()` — cached connection-health check without SQL or network I/O
- `client.reset_session()` — native TDS session reset with `READ COMMITTED` isolation
- `conn.query(sql, &[&param])` — positional `@P1, @P2` parameters
- `Config::new().host().port().trust_cert()` — fluent builder
- `Config::trust_cert_ca("ca.pem")` — pin a CA certificate (mirrors tiberius)
- `AuthMethod::aad_token(jwt)` — Microsoft Entra ID / AAD federated auth
- deadpool connection pooling with native session reset and validation before reuse

## Quick Start

The query API shown below is **unreleased** and intentionally breaks the
published bridge 0.1.0 buffered-query contract to match Tiberius 0.7.3's query
patterns. Use a reviewed immutable Git revision until it is released.
The bridge still uses published `mssql-tds` 0.1.0; no native fork is required.

```rust
use mssql_tiberius_bridge::{Config, AuthMethod, Client};

#[tokio::main]
async fn main() -> mssql_tiberius_bridge::Result<()> {
    let mut cfg = Config::new();
    cfg.host("localhost")
       .port(1433)
       .database("master")
       .authentication(AuthMethod::sql_server("sa", "password"))
       .trust_cert();

    let mut client = Client::connect(&cfg).await?;

    let rows = client
        .simple_query("SELECT name FROM sys.databases")
        .await?
        .into_first_result().await?;

    for row in rows {
        let name: String = row.get("name").unwrap();
        println!("{name}");
    }
    Ok(())
}
```

## Streaming metadata, including empty results

`client.query(...).await?` and `simple_query(...).await?` return
`QueryStream<'_>`, borrowing the client and positioned before the first metadata
event (or at EOF). Initial errors are returned by that await. Rows and trailing
errors are read during consumption, without collecting the query or issuing
separate metadata SQL. The stream yields `QueryItem::Metadata(ResultMetadata)`
before the ordinary rows of every rowset. Metadata shares the same schema as
`Row::columns()`.

```rust,no_run
use futures_util::StreamExt;
use mssql_tiberius_bridge::{Client, QueryItem};

async fn read(client: &mut Client) -> mssql_tiberius_bridge::Result<()> {
    let mut items = client.query(
        "SELECT @P1 AS empty_int WHERE 1 = 0; SELECT 42 AS answer",
        &[&1i32],
    ).await?;
    let columns = items.columns().await?.expect("SELECT has columns even when empty");
    assert_eq!(columns[0].name(), "empty_int");
    while let Some(item) = items.next().await {
        match item? {
            QueryItem::Metadata(meta) => {
                // Even rowset 0 has columns, despite returning no rows.
                println!("rowset {}: {:?}", meta.result_index(), meta.columns());
            }
            QueryItem::Row(row) => println!("{:?}", row.try_get::<i32, _>("answer")?),
        }
    }
    Ok(())
}
```

Indexes start at zero and count all rowsets, including empty first,
intermediate, and final rowsets. Each metadata item starts a new rowset;
subsequent rows belong to it until the next metadata item or EOF. Statements
without columns (including DML row counts) are skipped and do not increment
the index. A batch without rowsets produces no items, unlike an empty SELECT,
which still produces metadata. Use `execute` for affected-row counts.

Consume through EOF to observe trailing SQL errors. An error is yielded once
and ends the stream; receiving metadata or the last row alone is not query
success. A metadata-only probe can consume and discard rows through EOF without
collecting them. Alternatively, drop the stream at a yielded metadata/row item:
the next query drains outstanding results using the existing connection
lifecycle. `ping()` does **not** drain. Dropping during pending I/O retires the
connection; retaining the stream allows a cancelled `next()` to resume. No
async cleanup or SQL cancellation happens in `Drop`. Owned metadata and rows
can outlive the stream, but the stream itself borrows the client.

`columns().await?` peeks without consuming an event: it returns the next
metadata if one is immediately next, otherwise the current schema. At EOF the
last schema remains cached; a batch with no rowsets returns `None`. Repeated
peeks do not rebuild the schema. Metadata and rows expose `result_index()`.
`QueryItem` provides `as_metadata`, `as_row`, `into_metadata`, and `into_row`.
An error observed by `columns()` terminates the stream and is not repeated;
the bridge retains its existing single-error policy rather than cloning native
errors as Tiberius does.

### Breaking migration from bridge 0.1.0

Add `.await?` to `into_first_result()` and `into_results()`. `into_row().await?`
returns the first row of the first remaining rowset, or `None` for an empty
first remaining rowset. Collectors also work after partial stream consumption.
All three collectors consume through EOF and report trailing errors. Only
`into_results()` retains all rowsets; the other collectors discard later rows.
The `into_row_stream()` conversion is still synchronous, but now reads the wire.

The stream holds the mutable client borrow until consumed or dropped. Code that
discarded `query().await?` expecting complete execution must explicitly drain
with `into_results().await?`, or use `execute(sql, params).await?` for non-row
operations. `result_set_count()` is not available before streaming completion;
use the length of the collected results when needed.

Bridge-specific prepared-query APIs retain their buffered `QueryResult` and
synchronous collectors: Tiberius 0.7.3 has no corresponding public prepared
API. Existing `query_streamed` / `simple_query_streamed` convenience methods
retain their signatures and lazy initialization. The proposed `query_items` /
`simple_query_items` entry points were removed before release.

This aligns the query/metadata call patterns, not every type in the crate:
connection setup, `ColumnType` variants, and bridge-specific extensions still
differ from Tiberius.

## Migration from tiberius

| tiberius | mssql-tiberius-bridge |
|----------|----------------------|
| `tiberius::Config` | `Config` (same fluent API) |
| `tiberius::Client` | `Client` |
| `Client::connect(config, tcp)` | `Client::connect(&config)` (handles TCP internally) |
| `conn.simple_query(sql)` | `client.simple_query(sql)` |
| `conn.query(sql, &[&p1])` | `client.query(sql, &[&p1])` |
| cached connection-health check (no round trip) | `client.ping()` |
| connection-pool session cleanup | `client.reset_session()` (the default `TdsManager` recycling policy) |
| `stream.columns().await?` | `stream.columns().await?` |
| `stream.into_first_result().await?` | `stream.into_first_result().await?` |
| `stream.into_results().await?` | `stream.into_results().await?` |
| `stream.into_row().await?` | `stream.into_row().await?` |
| `stream.into_row_stream()` | `stream.into_row_stream()` |
| `row.get::<&str, _>("col")` | `row.get::<&str, _>("col")` |
| `tiberius::AuthMethod::sql_server` | `AuthMethod::sql_server` |

### Pooling behavior change

`TdsManager::new` and `TdsManager::create_pool` now reset reused sessions using
`mssql-tds`'s native `RESETCONNECTION` flag, restore `READ COMMITTED` isolation,
and validate the response. Temporary tables, changed session settings, open
transactions, and prepared handles no longer carry over to the next borrower.
Prepare and close statements within one checkout. Finish transactions before
returning a connection: recycling runs at the next checkout, not at check-in.

For intentional legacy session reuse, build a pool with
`TdsManager::new(config).with_recycling_method(RecyclingMethod::Ping)`.
`Ping` now uses the driver's cached `is_connection_dead()` status instead of
`SELECT 1`: it performs no I/O and leaves outstanding results untouched.
Success means "not known dead," not verified server responsiveness; an idle
connection failure may only be detected by the next operation.
See [connection pooling](docs/connection-examples.md#connection-pooling) for
examples and timeout configuration. `deadpool` still owns capacity and checkout;
`mssql-tds` provides the native reset and health primitives.

## Cancellation and timeouts

Dropping an in-flight bridge operation with `tokio::time::timeout`, `select!`,
or task cancellation marks the connection dead. Later bridge operations fail
immediately with `Error::Tds(mssql_tds::error::Error::ConnectionClosed(...))`.
Check `client.is_connection_dead()` without I/O, then drop the client and
reconnect. The pool rejects dead clients before reset or ping and replaces them
on checkout.

Wire streams remain reusable when dropped between fully yielded rows; dropping
a stream while its I/O is pending marks the connection dead. Cancelling only
`stream.next()` retains the internal future if you keep the stream and resume it.
Unpolled futures, unused bulk builders, and buffered result streams do not poison
the connection. Completed SQL errors retain the native driver's liveness outcome.
A failed or cancelled session reset always retires the connection.

Cancellation does not guarantee that SQL stopped executing or rolled back, so
do not automatically retry writes. Native cooperative cancellation requires
polling through cleanup; an external timeout that drops the operation is different.
Direct calls through `inner_mut()` bypass the bridge guards: after abandoning
native I/O, mark the native client dead and discard it instead of returning it
to a pool as healthy. See the `Client` rustdoc for the full contract.

## Spatial values

`geography` and `geometry` columns can be read directly in buffered or streamed
queries as `Vec<u8>` (owned) or `&[u8]` (borrowed). SQL `NULL` returns `None`.
Their column types are `ColumnType::Geography` and `ColumnType::Geometry`;
other CLR user-defined types report `ColumnType::Udt`.

```rust
use mssql_tiberius_bridge::ColumnType;

let rows = client
    .simple_query("SELECT geography::Point(47.6, -122.3, 4326) AS location")
    .await?
    .into_first_result().await?;
assert_eq!(rows[0].columns()[0].column_type(), ColumnType::Geography);
let bytes: &[u8] = rows[0].get("location").unwrap();
```

These are SQL Server's native serialized bytes (equivalent to `.Serialize()`),
including the SRID, **not OGC Well-Known Binary (WKB)**. Use `.STAsBinary()` in
SQL when you need WKB instead. `Row::raw_value()` continues to return
`ColumnValues::Bytes`; no new value enum or spatial parser is required.
Point/LineString/Polygon parsing, `geo` integration, and native spatial parameter
binding are not provided. Existing byte parameters remain `varbinary`.

## Runtime requirements

The bridge itself is pure Rust. **No native libraries are linked at compile time**, so binaries build cleanly on minimal targets (alpine, distroless, scratch, musl). However, some authentication modes load system libraries at runtime via `dlopen` and require those libraries to be present on the host where the binary runs.

### SQL authentication (`AuthMethod::sql_server`)

No runtime dependencies beyond a working TCP stack. Works in any container.

### AAD token authentication (`AuthMethod::aad_token`)

No runtime dependencies on the bridge side — you supply the JWT yourself (typically via `azure_identity` or MSAL). Works in any container.

### Integrated authentication (`AuthMethod::Integrated`) — Linux / macOS

Uses Kerberos via `libgssapi_krb5`, loaded at **runtime** with `dlopen`. The library is **not** linked at build time; binaries build without it, but calling `Client::connect` with `Integrated` auth will fail at runtime if it's missing.

| OS | Package | Library file searched |
|---|---|---|
| Debian / Ubuntu | `libgssapi-krb5-2` (usually preinstalled; install via `apt-get install libgssapi-krb5-2`) | `libgssapi_krb5.so.2` |
| RHEL / Fedora / Rocky | `krb5-libs` | `libgssapi_krb5.so.2` |
| Alpine | `krb5-libs` (`apk add krb5-libs`) | `libgssapi_krb5.so.2` |
| Arch | `krb5` | `libgssapi_krb5.so.2` |
| macOS | Bundled with the OS (Heimdal) | `libgssapi_krb5.dylib` / `/System/Library/Frameworks/GSS.framework` |

**Distroless / scratch images:** must add the libgssapi-krb5 shared object explicitly, or use a base image that includes it. `gcr.io/distroless/cc-debian12` does **not** include it.

In addition to the library, integrated auth needs:

- A valid Kerberos ticket-granting ticket. Run `kinit user@REALM` (or use a keytab) before connecting.
- A correctly configured `/etc/krb5.conf` pointing at your KDC.
- Network reachability to the KDC (typically port 88) and a SPN registered for the SQL Server service principal.

### Integrated authentication (`AuthMethod::Integrated`) — Windows

Uses SSPI via `secur32.dll`, which is part of every supported Windows install. **No extra packages required.** The Windows account running the process must be a domain account (or have cached domain credentials) for Kerberos/NTLM to succeed.

### TLS

`mssql-tds` uses `native-tls`, which on Linux requires OpenSSL at runtime (already a dep of nearly every Linux distro and most container base images). Alpine needs `apk add openssl ca-certificates`.

## Development

`Cargo.toml` enables the Clippy lints listed in the
[`microsoft/mssql-rs` workspace manifest](https://github.com/microsoft/mssql-rs/blob/main/Cargo.toml),
including its commented-out lints. CI treats warnings as errors across all
targets and features, including benchmarks:

```sh
cargo clippy --all-targets --all-features -- -D warnings
```

## License

MIT
