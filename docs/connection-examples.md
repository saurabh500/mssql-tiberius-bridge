# Connection examples

Terse examples for every connection option exposed by `mssql-tiberius-bridge` `0.1.0-preview.2`. All samples assume they run inside an async context.

## Quick start

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config};
let mut cfg = Config::new();
cfg.host("localhost").authentication(AuthMethod::sql_server("sa", "password")).trust_cert();
let mut client = Client::connect(&cfg).await?;
client.ping().await?;
# Ok(())
# }
```

## Endpoint

### `Config::new()` defaults

`Config::new()` defaults to `localhost`, port `1433`, database `master`, encryption `On`, certificate validation enabled, SQL authentication with empty credentials, read-write intent, Unicode string parameters, and MultiSubnetFailover off. Override at least authentication before connecting.

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config};
let mut cfg = Config::new();
cfg.authentication(AuthMethod::sql_server("sa", "password"));
let mut client = Client::connect(&cfg).await?;
# Ok(())
# }
```

### `host(...)`

Default is `localhost`. Override it for any remote SQL Server, Azure SQL endpoint, AG listener, or container host name.

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config};
let mut cfg = Config::new();
cfg.host("sql.example.com")
    .authentication(AuthMethod::sql_server("app", "secret"));
let mut client = Client::connect(&cfg).await?;
# Ok(())
# }
```

### `port(...)`

Default is `1433`. Override it for non-default SQL Server listeners or port-mapped containers; do not rely on it when `instance_name(...)` is set, because the instance datasource omits the port for SQL Browser resolution.

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config};
let mut cfg = Config::new();
cfg.host("localhost")
    .port(14330)
    .authentication(AuthMethod::sql_server("sa", "password"))
    .trust_cert();
let mut client = Client::connect(&cfg).await?;
# Ok(())
# }
```

### `database(...)`

Default is `master`. Set this to choose the initial database in the Login7 packet instead of connecting to `master` and issuing `USE` later.

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config};
let mut cfg = Config::new();
cfg.host("sql.example.com")
    .database("appdb")
    .authentication(AuthMethod::sql_server("app", "secret"));
let mut client = Client::connect(&cfg).await?;
# Ok(())
# }
```

### `instance_name(...)`

Default is no named instance, so the datasource is `tcp:host,port`. Set an instance such as `SQLEXPRESS` when you want SQL Browser / SSRP to resolve the TCP port; when set, the bridge builds `tcp:host\\instance` and drops the explicit port.

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config};
let mut cfg = Config::new();
cfg.host("mssql.lab")
    .instance_name("SQLEXPRESS")
    .authentication(AuthMethod::sql_server("sa", "password"))
    .trust_cert();
let mut client = Client::connect(&cfg).await?;
# Ok(())
# }
```

## Authentication

### `authentication(AuthMethod::sql_server(...))`

`Config::new()` starts with empty SQL authentication, which is not useful for real logins. Use `AuthMethod::sql_server(user, password)` for SQL Server authentication; it maps to `user_name` and `password` in the client context.

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config};
let mut cfg = Config::new();
cfg.host("sql.example.com")
    .authentication(AuthMethod::sql_server("app_user", "secret"));
let mut client = Client::connect(&cfg).await?;
# Ok(())
# }
```

### `authentication(AuthMethod::integrated())`

Default is SQL authentication, not integrated auth. Use `AuthMethod::integrated()` for Windows SSPI or Kerberos/GSSAPI; the bridge sets `TdsAuthenticationMethod::SSPI`.

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config};
let mut cfg = Config::new();
cfg.host("sql.corp.example")
    .database("appdb")
    .authentication(AuthMethod::integrated());
let mut client = Client::connect(&cfg).await?;
# Ok(())
# }
```

### `authentication(AuthMethod::aad_token(...))`

Default is SQL authentication, not Entra ID. Use `AuthMethod::aad_token(token)` with a pre-acquired JWT scoped for `https://database.windows.net/.default`; the bridge sets access-token authentication and leaves username/password empty.

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config, EncryptionLevel};
let token = "eyJ0eXAi.fake.jwt";
let mut cfg = Config::new();
cfg.host("myserver.database.windows.net")
    .database("appdb")
    .encryption(EncryptionLevel::Required)
    .authentication(AuthMethod::aad_token(token));
let mut client = Client::connect(&cfg).await?;
# Ok(())
# }
```

## Encryption / TLS

### `encryption(EncryptionLevel::On)`

`On` is the default. Keep it for normal encrypted connections where the server may validate with the platform trust store, a pinned CA, or a development-only trust override.

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config, EncryptionLevel};
let mut cfg = Config::new();
cfg.host("sql.example.com")
    .authentication(AuthMethod::sql_server("app", "secret"))
    .encryption(EncryptionLevel::On);
let mut client = Client::connect(&cfg).await?;
# Ok(())
# }
```

### `encryption(EncryptionLevel::Required)`

Default `On` requests encryption; `Required` fails if the server cannot encrypt. Use it for production policies where silent downgrade is unacceptable.

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config, EncryptionLevel};
let mut cfg = Config::new();
cfg.host("sql.example.com")
    .authentication(AuthMethod::sql_server("app", "secret"))
    .encryption(EncryptionLevel::Required);
let mut client = Client::connect(&cfg).await?;
# Ok(())
# }
```

### `encryption(EncryptionLevel::Strict)`

Default is not Strict. `Strict` is TDS 8.0 strict mode: TLS starts before TDS pre-login, ALPN uses `tds/8.0`, certificate validation is enforced, and `trust_cert()` is ignored; available from the next release if your current release does not expose it yet.

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config, EncryptionLevel};
let mut cfg = Config::new();
cfg.host("sql.example.com")
    .authentication(AuthMethod::sql_server("app", "secret"))
    .encryption(EncryptionLevel::Strict)
    .trust_cert_ca("/etc/ssl/sql-ca.pem");
let mut client = Client::connect(&cfg).await?;
# Ok(())
# }
```

### `encryption(EncryptionLevel::Off)`

Default is `On`; `Off` maps to `PreferOff`, encrypting login only and allowing data to flow unencrypted if the server permits it. Use only for legacy servers or controlled test networks.

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config, EncryptionLevel};
let mut cfg = Config::new();
cfg.host("legacy-sql.local")
    .authentication(AuthMethod::sql_server("sa", "password"))
    .encryption(EncryptionLevel::Off);
let mut client = Client::connect(&cfg).await?;
# Ok(())
# }
```

### `encryption(EncryptionLevel::NotSupported)`

Default is `On`; `NotSupported` also maps to `PreferOff` in the current bridge wiring. Use it only when porting tiberius code that already uses this spelling.

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config, EncryptionLevel};
let mut cfg = Config::new();
cfg.host("legacy-sql.local")
    .authentication(AuthMethod::sql_server("sa", "password"))
    .encryption(EncryptionLevel::NotSupported);
let mut client = Client::connect(&cfg).await?;
# Ok(())
# }
```

### `trust_cert()`

Default is `false`, so the server certificate is validated. Use `trust_cert()` only for local development, test containers, or throwaway environments with self-signed certificates; it is ignored under `EncryptionLevel::Strict`.

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config};
let mut cfg = Config::new();
cfg.host("localhost")
    .authentication(AuthMethod::sql_server("sa", "password"))
    .trust_cert();
let mut client = Client::connect(&cfg).await?;
# Ok(())
# }
```

### `trust_cert_ca(...)`

Default is no pinned certificate or CA path. Set `trust_cert_ca(path)` to provide a PEM or DER X.509 file for private PKI or self-signed SQL Server certificates; it sets `server_certificate` in the underlying TLS options.

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config, EncryptionLevel};
let mut cfg = Config::new();
cfg.host("db.internal")
    .authentication(AuthMethod::sql_server("app", "secret"))
    .encryption(EncryptionLevel::Required)
    .trust_cert_ca("/etc/ssl/private-sql-ca.pem");
let mut client = Client::connect(&cfg).await?;
# Ok(())
# }
```

### `host_name_in_certificate(...)`

Default is no override, so TLS validates against `host(...)`. Set this when connecting through an IP, CNAME, proxy, or listener whose DNS name differs from the certificate SAN/CN.

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config};
let mut cfg = Config::new();
cfg.host("10.0.0.15")
    .host_name_in_certificate("sql-prod.internal")
    .authentication(AuthMethod::sql_server("app", "secret"));
let mut client = Client::connect(&cfg).await?;
# Ok(())
# }
```

## Identity & telemetry

### `application_name(...)`

Default is the underlying driver default. Set it to label sessions in `sys.dm_exec_sessions`, SQL auditing, and server-side telemetry.

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config};
let mut cfg = Config::new();
cfg.host("sql.example.com")
    .application_name("orders-api")
    .authentication(AuthMethod::sql_server("app", "secret"));
let mut client = Client::connect(&cfg).await?;
# Ok(())
# }
```

### `client_name(...)`

Default comes from `mssql-tds`'s client context. Set it to control the workstation name / `workstation_id` sent in Login7.

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config};
let mut cfg = Config::new();
cfg.host("sql.example.com")
    .client_name("worker-01")
    .authentication(AuthMethod::sql_server("app", "secret"));
let mut client = Client::connect(&cfg).await?;
# Ok(())
# }
```

### Built-in driver identity

There is no setter for the bridge driver identity. `to_client_context()` always sends library name `MS-TIB-BRID`, the Cargo package version as driver version, and UserAgent fields so SQL Server can identify this bridge.

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config};
let mut cfg = Config::new();
cfg.host("sql.example.com")
    .application_name("billing-job")
    .authentication(AuthMethod::sql_server("app", "secret"));
let mut client = Client::connect(&cfg).await?;
# Ok(())
# }
```

## Read-only routing

### `readonly(...)`

Default is `false`, which sends `ApplicationIntent=ReadWrite`. Set `readonly(true)` for Always On readable secondaries or Azure SQL geo-replicas that route read-only sessions differently.

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config};
let mut cfg = Config::new();
cfg.host("ag-listener.example.com")
    .database("reporting")
    .authentication(AuthMethod::sql_server("report_user", "secret"))
    .readonly(true);
let mut client = Client::connect(&cfg).await?;
# Ok(())
# }
```

## Multi-subnet failover

### `multi_subnet_failover(...)`

Default is `false`, the normal single-target TCP connect path. Set `multi_subnet_failover(true)` for Always On listeners spanning multiple subnets; the driver resolves all A/AAAA records and races TCP connects in parallel. Available from the next release if your current release does not expose it yet.

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config};
let mut cfg = Config::new();
cfg.host("ag-listener.example.com")
    .database("appdb")
    .authentication(AuthMethod::sql_server("app", "secret"))
    .trust_cert()
    .multi_subnet_failover(true);
let mut client = Client::connect(&cfg).await?;
# Ok(())
# }
```

### `is_multi_subnet_failover()`

This is a readback helper, not a connection setting. Use it for assertions or diagnostics when constructing config in layers.

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config};
let mut cfg = Config::new();
cfg.host("ag-listener.example.com")
    .authentication(AuthMethod::sql_server("app", "secret"))
    .multi_subnet_failover(true);
assert!(cfg.is_multi_subnet_failover());
let mut client = Client::connect(&cfg).await?;
# Ok(())
# }
```

## Misc

### `send_string_parameters_as_unicode(...)`

Default is `true`, so `&str` and `String` parameters are sent as NVARCHAR. Set it to `false` only when you intentionally want VARCHAR parameter metadata for legacy schema, collation, or index behavior.

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config};
let mut cfg = Config::new();
cfg.host("sql.example.com")
    .authentication(AuthMethod::sql_server("app", "secret"))
    .send_string_parameters_as_unicode(false);
let mut client = Client::connect(&cfg).await?;
let _ = client.query("SELECT @P1", &[&"varchar-shaped"]).await?;
# Ok(())
# }
```

### `get_addr()`

This is a convenience accessor, not a setting. It returns `host:port` for logging and does not reflect `instance_name(...)` datasource rewriting.

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config};
let mut cfg = Config::new();
cfg.host("sql.example.com")
    .port(1433)
    .authentication(AuthMethod::sql_server("app", "secret"));
let _addr = cfg.get_addr();
let mut client = Client::connect(&cfg).await?;
# Ok(())
# }
```

### `datasource_string()`

This is a convenience accessor for the string passed to `TdsConnectionProvider`. It returns `tcp:host,port` normally or `tcp:host\\instance` when `instance_name(...)` is set.

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config};
let mut cfg = Config::new();
cfg.host("mssql.lab")
    .instance_name("SQLEXPRESS")
    .authentication(AuthMethod::sql_server("sa", "password"))
    .trust_cert();
let _datasource = cfg.datasource_string();
let mut client = Client::connect(&cfg).await?;
# Ok(())
# }
```

### `to_client_context()`

This is the internal bridge to `mssql-tds`, exposed publicly for inspection. It wires database, authentication, TLS options, application/client names, driver identity, application intent, and MultiSubnetFailover into `ClientContext`; normal code should call `Client::connect(&cfg)` instead.

```rust,no_run
# async fn example() -> mssql_tiberius_bridge::Result<()> {
use mssql_tiberius_bridge::{AuthMethod, Client, Config};
let mut cfg = Config::new();
cfg.host("sql.example.com")
    .authentication(AuthMethod::sql_server("app", "secret"));
let _ctx = cfg.to_client_context();
let mut client = Client::connect(&cfg).await?;
# Ok(())
# }
```

## Connection pooling

`Client` owns one connection and is not cloneable; use `TdsManager` with `deadpool` for shared concurrency. The manager clones the `Config`, creates connections with `Client::connect(&config)`, and recycles them with `Client::ping()`.

```rust,no_run
# async fn example() -> std::result::Result<(), Box<dyn std::error::Error>> {
use mssql_tiberius_bridge::{AuthMethod, Config, TdsManager};
let mut cfg = Config::new();
cfg.host("sql.example.com")
    .database("appdb")
    .authentication(AuthMethod::sql_server("app", "secret"));
let pool = TdsManager::create_pool(cfg, 16)?;
let mut conn = pool.get().await?;
let _ = conn.simple_query("SELECT 1").await?;
# Ok(())
# }
```

If you need a custom `deadpool` builder, construct `TdsManager::new(cfg)` yourself and pass it to `deadpool::managed::Pool::builder`.

```rust,no_run
# fn example() -> std::result::Result<(), Box<dyn std::error::Error>> {
use mssql_tiberius_bridge::{AuthMethod, Config, Pool, TdsManager};
let mut cfg = Config::new();
cfg.host("sql.example.com")
    .authentication(AuthMethod::sql_server("app", "secret"));
let manager = TdsManager::new(cfg);
let _pool = Pool::builder(manager).max_size(8).build()?;
# Ok(())
# }
```

## Tiberius API mapping

Use this table when migrating code that already builds a `tiberius::Config`.

| Bridge option | tiberius `Config` API |
|---|---|
| `Config::new()` | `tiberius::Config::new()` |
| `host(...)` | `tiberius::Config::host(...)` |
| `port(...)` | `tiberius::Config::port(...)` |
| `database(...)` | `tiberius::Config::database(...)` |
| `authentication(AuthMethod::sql_server(...))` | `tiberius::Config::authentication(tiberius::AuthMethod::sql_server(...))` |
| `authentication(AuthMethod::integrated())` | `tiberius::Config::authentication(tiberius::AuthMethod::Integrated)` / integrated auth helper in your tiberius version |
| `authentication(AuthMethod::aad_token(...))` | `tiberius::Config::authentication(tiberius::AuthMethod::aad_token(...))` |
| `encryption(EncryptionLevel::On)` | `tiberius::Config::encryption(tiberius::EncryptionLevel::On)` |
| `encryption(EncryptionLevel::Required)` | `tiberius::Config::encryption(tiberius::EncryptionLevel::Required)` |
| `encryption(EncryptionLevel::Strict)` | `tiberius::Config::encryption(tiberius::EncryptionLevel::Strict)` |
| `encryption(EncryptionLevel::Off)` | `tiberius::Config::encryption(tiberius::EncryptionLevel::Off)` |
| `encryption(EncryptionLevel::NotSupported)` | `tiberius::Config::encryption(tiberius::EncryptionLevel::NotSupported)` |
| `trust_cert()` | `tiberius::Config::trust_cert()` |
| `trust_cert_ca(...)` | `tiberius::Config::trust_cert_ca(...)` |
| `host_name_in_certificate(...)` | `tiberius::Config::host_name_in_certificate(...)` |
| `application_name(...)` | `tiberius::Config::application_name(...)` |
| `client_name(...)` | `tiberius::Config::client_name(...)` |
| `readonly(...)` | `tiberius::Config::readonly(...)` |
| `multi_subnet_failover(...)` | `tiberius::Config::multi_subnet_failover(...)` |
| `send_string_parameters_as_unicode(...)` | `tiberius::Config::send_string_parameters_as_unicode(...)` |
| `instance_name(...)` | `tiberius::Config::instance_name(...)` |
| `get_addr()` | `tiberius::Config::get_addr()` |
| `datasource_string()` | No direct migration requirement; bridge-specific inspection of the datasource passed to `mssql-tds` |
| `to_client_context()` | No tiberius equivalent; bridge-specific conversion to `mssql-tds` `ClientContext` |
| `Client::connect(&cfg)` | `tiberius::Client::connect(config, tcp_stream)`; the bridge owns TCP setup |
| `TdsManager::create_pool(cfg, max_size)` | Common tiberius + `deadpool` manager pattern, provided by the bridge |
