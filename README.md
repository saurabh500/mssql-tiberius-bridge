# mssql-tiberius-bridge

A tiberius-compatible API bridge over Microsoft's [`mssql-tds`](https://crates.io/crates/mssql-tds) crate. Migrate from tiberius with minimal code changes.

## Why?

[tiberius](https://github.com/prisma/tiberius) is the most popular Rust TDS driver, but it's community-maintained. Microsoft's `mssql-tds` is the official, supported implementation — but it has a different API surface.

**mssql-tiberius-bridge** gives you the tiberius API you know on top of the mssql-tds engine:

- `row.get::<T, _>("column_name")` — named and indexed column access
- `stream.into_first_result()` — collect results into `Vec<Row>`
- `stream.into_row_stream()` — `Stream<Item = Result<Row>>` over a buffered `QueryResult` (rows pre-buffered)
- `client.query_streamed(sql, params)` / `simple_query_streamed(sql)` — true wire-level row streaming for memory-bounded large result sets
- `client.query_arrow(sql, params)` / `simple_query_arrow(sql)` — direct Arrow read batches under the `arrow` feature, with result-set indexes and configurable limits
- `client.ping()` — lightweight liveness check for connection pools
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
| connection-pool validation | `client.ping()` |
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
See [connection pooling](docs/connection-examples.md#connection-pooling) for
examples and timeout configuration. `deadpool` still owns capacity and checkout;
`mssql-tds` provides the native reset and health primitives.

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

## Performance

Both buffered and streamed queries already use `mssql-tds`'s `RowWriter`.
The [performance comparison](docs/performance.md) includes measured results
against Tiberius, a reproducible Criterion benchmark, and an assessment of
direct-writer and Arrow query-output opportunities.
The [Arrow follow-up](docs/arrow-performance.md) records prototype and production
measurements. [Arrow reads](docs/arrow-reads.md) documents the additive public
APIs, SQL type mappings, limits, and cancellation behavior.

For large results, use `query_streamed` / `simple_query_streamed`.
Calling `into_row_stream()` on a buffered `QueryResult` does not undo its
up-front memory usage.

## License

MIT
