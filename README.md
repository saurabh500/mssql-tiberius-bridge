# mssql-tiberius-bridge

A tiberius-compatible API bridge over Microsoft's [`mssql-tds`](https://crates.io/crates/mssql-tds) crate. Migrate from tiberius with minimal code changes.

## Why?

[tiberius](https://github.com/prisma/tiberius) is the most popular Rust TDS driver, but it's community-maintained. Microsoft's `mssql-tds` is the official, supported implementation — but it has a different API surface.

**mssql-tiberius-bridge** gives you the tiberius API you know on top of the mssql-tds engine:

- `row.get::<T, _>("column_name")` — named and indexed column access
- `stream.into_first_result()` — collect results into `Vec<Row>`
- `stream.into_row_stream()` — `Stream<Item = Result<Row>>` over a buffered `QueryResult` (rows pre-buffered)
- `client.query_streamed(sql, params)` / `simple_query_streamed(sql)` — true wire-level row streaming for memory-bounded large result sets
- `client.ping()` — cached connection-health check without SQL or network I/O
- `client.reset_session()` — native TDS session reset with `READ COMMITTED` isolation
- `conn.query(sql, &[&param])` — positional `@P1, @P2` parameters
- `Config::new().host().port().trust_cert()` — fluent builder
- `Config::trust_cert_ca("ca.pem")` — pin a CA certificate (mirrors tiberius)
- `AuthMethod::aad_token(jwt)` — Microsoft Entra ID / AAD federated auth
- deadpool connection pooling with native session reset and validation before reuse

## Quick Start

Add `mssql-tiberius-bridge = "0.1.0"` to your Cargo dependencies. The bridge
uses `mssql-tds` 0.1.0 from crates.io, replacing `mssql-tds-preview`.

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
        .into_first_result();

    for row in rows {
        let name: String = row.get("name").unwrap();
        println!("{name}");
    }
    Ok(())
}
```

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
| `stream.into_first_result()` | `.into_first_result()` |
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

## Incremental custom row writers

Use the checked incremental API when the consumer owns its client and reusable
value buffers, without retaining a borrowed stream or creating bridge `Row`s:

```rust,ignore
let mut on_result = client.start_query(sql, &[]).await?;
while on_result {
    // Copy/validate metadata before reading; empty rowsets also expose columns.
    configure_writer(client.query_metadata()?)?;
    loop {
        let mut writer = make_writer_for_next_row();
        let decoded = client.next_row_into(&mut writer).await?;
        // Check your writer's conversion-error latch, column count, and end_row.
        writer.check_completed_row(decoded)?;
        if !decoded { break; } // Current rowset boundary only.
        publish_completed_row(writer)?;
    }
    on_result = client.next_result().await?; // false means query EOF.
}
client.close_query().await?;
```

This sketch omits application-specific writers and error cleanup; see the complete
[integer-writer example](examples/incremental.rs), including drain-on-error.
`start_query` drains previous results and skips non-row statements. `next_result`
drains the current result if necessary and skips subsequent non-row statements;
it must be called even after a row boundary to observe trailing results/errors.
Repeated row reads at a boundary return `false` without advancing, and repeated
`next_result` calls at query EOF return `false`. `query_metadata` returns an error
outside a current unread rowset, including after its boundary is read.

`close_query` drains rather than cancels; early close of a large query can still
take time. Completed SQL errors preserve native connection health; failed
transport drains and dropped pending I/O retire it. Unpolled futures do not.
For a pool that does not drain on return, reject a client when
`is_connection_dead() || has_pending_results()`. Neither being set proves that
the remote server is still responsive. Existing bridge operations continue to
drain outstanding results before issuing a new request.

`query_first(sql, params)` materializes at most one bridge `Row`, then drains
everything. It reads the **first rowset**, returning `None` for an empty first
rowset even if later rowsets have rows. Trailing errors are not hidden by an
already-read row. Use this for scalar/count/range queries, not data refills.

Import `RowWriter`, its callback argument types, `ColumnMetadata`, `TdsDataType`,
and `TdsError` through `mssql_tiberius_bridge::writer`. These native re-exports
couple this part of the bridge's public API to `mssql-tds` versions. Callback
bytes may be borrowed only for the callback; copy/transcode into owned storage.
Discard partial rows and uncommitted `value_destination` storage on failed or
cancelled reads. Callback conversion errors must be latched and checked by the
writer before publishing even an `Ok(true)` row; then drain or discard the client.

Consumers control refill size (for example 32 rows per `block_on`). This is not
a MAX-value byte cap, and no bridge-backed performance claim follows from
direct-driver measurements. The optional features `sspi`, `gssapi`, and
`tls-schannel-direct-on-windows` forward the native features of those names,
without changing defaults, TLS semantics, or requiring a direct native dependency.

The standalone consumer fixture has **only the bridge as a SQL-driver
dependency** and compiles the same example without the bridge's dev dependencies:

```powershell
cargo check --manifest-path tests\consumer\Cargo.toml
```

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
    .into_first_result();
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
