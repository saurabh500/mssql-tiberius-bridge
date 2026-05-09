# mssql-tiberius-bridge

A tiberius-compatible API bridge over Microsoft's [`mssql-tds`](https://github.com/microsoft/mssql-rs) crate. Migrate from tiberius with minimal code changes.

## Why?

[tiberius](https://github.com/prisma/tiberius) is the most popular Rust TDS driver, but it's community-maintained. Microsoft's `mssql-tds` is the official, supported implementation — but it has a different API surface.

**mssql-tiberius-bridge** gives you the tiberius API you know on top of the mssql-tds engine:

- `row.get::<T, _>("column_name")` — named and indexed column access
- `stream.into_first_result()` — collect results into `Vec<Row>`
- `conn.query(sql, &[&param])` — positional `@P1, @P2` parameters
- `Config::new().host().port().trust_cert()` — fluent builder
- `Config::trust_cert_ca("ca.pem")` — pin a CA certificate (mirrors tiberius)
- deadpool connection pooling out of the box

## Quick Start

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
| `stream.into_first_result()` | `.into_first_result()` |
| `row.get::<&str, _>("col")` | `row.get::<&str, _>("col")` |
| `tiberius::AuthMethod::sql_server` | `AuthMethod::sql_server` |

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

## License

MIT
