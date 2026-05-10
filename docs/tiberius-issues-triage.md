# Tiberius Open Issues — Categorization for mssql-tiberius-bridge Triage

Source: https://github.com/prisma/tiberius/issues (101 open issues, fetched 2026-05-09)

This document categorizes every open issue in `prisma/tiberius` to determine which ones may also exist in `mssql-tiberius-bridge` (which sits on Microsoft's `mssql-tds`, not on tiberius's own protocol implementation). For each category we note:
- **Likely status in bridge**: how the bridge probably behaves (based on its architecture: it wraps `mssql-tds` and exposes a tiberius-compatible facade).
- **Action**: whether to file a duplicate in bridge + attempt a fix.

> **Architecture reminder**: The bridge does **not** inherit tiberius's protocol bugs. Bugs in `src/tds/codec/...` of tiberius are most likely *not* present in the bridge because we use a different wire-protocol implementation. However, **API-surface gaps** (missing methods, missing trait impls, ergonomics) and **feature gaps** (transactions, prepared statements, AAD variants, etc.) are very likely shared.

## Triage Legend

| Symbol | Meaning |
|--------|---------|
| 🟢 | Bridge unaffected — tiberius-internal bug or already handled by `mssql-tds`. No action. |
| 🟡 | Possibly affected — needs hands-on repro in bridge. |
| 🔴 | Almost certainly affected — file duplicate + fix. |
| ⚪ | Not actionable — usage question, unrelated, or invalid. No action. |
| 🔵 | Feature request — file as feature in bridge if aligned with roadmap. |

---

## A. Wire-Protocol / Decoder Bugs (in tiberius's `src/tds/codec/...`)

These are bugs in tiberius's TDS implementation. The bridge uses `mssql-tds`, an entirely separate parser, so most are **not** present.

| # | Title | Status | Notes |
|---|-------|--------|-------|
| 418 | Display impl for PacketSize/Database EnvChange shows old/new swapped | 🟢 | tiberius-internal log formatting bug. mssql-tds has its own EnvChange impl. |
| 410 | BCP failure when DATE column precedes TIME column | 🟢 | tiberius-only BCP encoder bug. Bridge has no `bulk_insert` yet anyway. |
| 368 | Negative Numeric Floats sign issue (`-17.-80`) | 🟡 | **Repro**: did we already fix this? Numeric Display in `DecimalParts`. Worth a regression test. |
| 316 | Panic reading datetime field with date < 1900 (overflow in `time` feature) | 🟡 | **Repro**: try with `chrono`. mssql-tds may have same overflow. |
| 325 | UTF-16 error on broken characters | 🟡 | **Repro**: check `to_utf8_string` on `SqlString` for replacement-char fallback. |
| 322 | Bulk insert large varchar/nvarchar fails (BCP colid 8) | 🟢 | tiberius BCP-only; bridge has no bulk_insert. |
| 358 | `bulk_insert` does not support Money MS SQL data type | 🟢 | tiberius-only. Money/SmallMoney *are* readable in bridge. |
| 370 | (n/a placeholder) | — | — |
| 211 | Panic from `try_get` (unwrap path) | 🟡 | **Repro**: bridge `Row::try_get` shouldn't panic on type mismatch. |
| 263 | Cannot interpret `I16(None)` as `i32` | 🟡 | **Repro**: bridge `FromSql for i32` — does it accept SmallInt(None)? Likely improvement. |
| 226 | Performance: handling buffers in `plp.rs` | 🟢 | tiberius-only PLP decoder; mssql-tds is separate. |

## B. TLS / Encryption Issues

| # | Title | Status | Notes |
|---|-------|--------|-------|
| 412 | TDS 8.0 and "Strict" encryption option | 🔵 | Feature gap. mssql-tds may already do TDS 7.4 encryption; check if it speaks 8.0/Strict. File as feature. |
| 412 | (same) | — | — |
| 327 | `UnsupportedCertVersion` with rustls | 🟢 | tiberius-rustls bug. Bridge uses native-tls. |
| 320 | TLS handshake stalls when `EncryptionLevel::Off` | 🟡 | **Repro**: connect with `EncryptionLevel::Off` in bridge — does it skip handshake? |
| 305 | Unencrypted traffic despite `encrypt=true` due to TLS feature flags disablement | 🟢 | tiberius feature-flag config bug. Bridge's encryption is unconditional. |
| 364 | macOS 15 + SQL Server 2014 doesn't work (rustls/vendored-openssl) | 🟡 | **Repro**: try bridge against SQL Server 2014. native-tls may behave differently. |
| 274 | TLS handshake EOF | 🟡 | **Repro**: similar — try basic connect with bridge against the user's reported config. |
| 323 | Failing to connect with TLS (rustls compile errors) | 🟢 | tiberius rustls feature-set issue; N/A. |
| 218 | tiberius 0.9 keep crashing on macOS | 🟢 | Old version; tiberius-internal. |
| 340 | Setting `HostNameInCertificate` property | 🔵 | Feature: bridge could expose `Config::host_name_in_certificate`. |
| 224 | `danger_accept_invalid_hostnames` + `danger_accept_invalid_certs` | 🔵 | Bridge already has `trust_cert` (skip verify). Hostname-only skip is a separate variant. |
| 381 | openssl support (TLS 1.0 for old SQL Server) | 🔵 | mssql-tds uses native-tls (which on macOS uses SecureTransport, on Linux uses openssl). May already support TLS 1.0. |

## C. Authentication

| # | Title | Status | Notes |
|---|-------|--------|-------|
| 407 | sspi-rs for SSPI NTLM on Linux/macOS without Kerberos | 🔵 | Bridge currently does Kerberos via libgssapi dlopen on Linux. NTLM-on-Linux is a real gap. |
| 343 | Panic in libgssapi when using Integrated Security (Rust 1.78) | 🟡 | **Repro**: bridge's libgssapi version — verify it includes the fix referenced (estokes/libgssapi#23). |
| 283 | Build problems with `Auth::Integrated` on CI | 🟢 | tiberius-feature build issue. Bridge gates differently. |
| 276 | GSS-NTLMSSP instead of Kerberos on Linux | 🔵 | Same as 407 — NTLM-via-GSSAPI. |
| 175 | Is Azure SQL/AAD supported? | ✅ | Already done — bridge has `AuthMethod::aad_token` (PR #26). |
| 97 | WindowsAuth should be available in Linux | 🔵 | Same theme as #407/#276. Bridge uses libgssapi for Kerberos. NTLM gap. |
| 333 | Connection failure with special-character password | 🟡 | **Repro**: try special chars in `AuthMethod::sql_server`. Likely fine since we don't ADO-parse. |

## D. Bulk Insert / BCP (entire feature missing in bridge)

| # | Title | Status | Notes |
|---|-------|--------|-------|
| 410 | BCP date+time column-order failure | 🟢 | (covered above) |
| 358 | Money in bulk_insert | 🟢 | (covered above) |
| 352 | NTEXT bulk insert example | 🟢 | how-to; bridge has no bulk_insert. |
| 322 | Large varchar/nvarchar bulk insert | 🟢 | (covered above) |
| 319 | Insert into TEXT column | 🟢 | how-to; bridge can `INSERT` via `query`/`execute`. |
| 311 | Bulk Insert: Allow column names | 🔵 | Feature for when bridge implements bulk_insert. |
| 307 | Bulk insert with datetime field | 🟢 | how-to / tiberius-specific. |
| 302 | Bulk Copy Options | 🔵 | Feature for future bulk_insert. |
| 373 | Date issue in bulk insert (target schema) | 🟢 | tiberius-only. |

> **Bridge gap**: `bulk_insert` is not yet implemented. All BCP-related issues map to a single roadmap item. ⇒ File one tracking issue: "Implement `Client::bulk_insert` (BCP)" referencing this cluster.

## E. Type Conversion / `FromSql` / `ToSql` / `IntoSql`

| # | Title | Status | Notes |
|---|-------|--------|-------|
| 401 | Implement `IntoSql` for `rust_decimal` | 🟡 | **Check**: bridge's `ToSql for Decimal`. Probably already has. |
| 277 | `IntoSql` impl missing for `time` crate | 🔵 | Bridge added chrono ToSql in PR #23; `time` crate is a separate gap. |
| 244 | `impl IntoSql<'a>` in `.bind()` method | ⚪ | Bridge has no `Query::bind` builder; N/A. |
| 257 | Can't get `geography` type | 🟡 | **Repro**: bridge `ColumnValues` — does it have a Geography variant? Likely no. |
| 354 | jiff crate support | 🔵 | Feature. Mirror chrono/time impls. |
| 277 | (dup) | — | — |
| 401 | (dup) | — | — |
| 169 | Owned variants on `FromSql` for `String`/`Vec` | 🔵 | **Check**: bridge `FromSql<String>` / `FromSql<Vec<u8>>`. |
| 367 | `sendStringParametersAsUnicode` connection property | 🔵 | Feature: control NVARCHAR-vs-VARCHAR encoding for string params. |
| 334 | Why tinyint converted to u8? | ⚪ | Tiberius design — TDS TinyInt is unsigned 1-byte. Not a bug. |
| 216 | Distinguishing `Intn` for bigint vs int | ⚪ | how-to / FromSql usage. |
| 219 | Case insensitive `row.get()` | 🔵 | Feature gap. **Check** if bridge `Row::get(name)` is case-sensitive. Probably yes. |
| 221 | f64::NAN insert into decimal column | 🟡 | **Repro**: send NaN as `f64` param to bridge — should error gracefully, not corrupt protocol. |
| 282 | Binding string adds quotation marks (with proc) | 🟡 | **Repro**: call a SP via bridge with a `&str` param. |
| 278 | Dynamic interaction with query results | ⚪ | how-to / design discussion. |

## F. Connectivity / Connection Setup

| # | Title | Status | Notes |
|---|-------|--------|-------|
| 414 | Client hostname in `LoginMessage` | 🔵 | **Check**: does bridge send a real hostname in Login7? It likely uses `localhost`. |
| 386 | smol 2.0 incompat with `sql-browser-smol` | 🟢 | tiberius-runtime. Bridge is tokio-only. |
| 360 | Timeout connecting over VPN | ⚪ | Diagnostic / network. |
| 345 | Dynamic ports in Docker | ⚪ | Use SSRP / instance_name. Bridge already supports SSRP (PR #27). |
| 313 | "Key-value pairs must be separated by a `;`" parsing | 🟢 | tiberius ADO-string parser. Bridge has its own `Config` builder. |
| 329 | Update `tokio_rustls` 0.24 → 0.25 | 🟢 | tiberius dep. N/A. |
| 337 | MultiSubnetFailover support | 🔵 | Feature. mssql-tds may have routing redirect; MSF is separate. |
| 348 | Send ReadOnlyIntent (already merged in tiberius PR #297) | ✅ | Bridge already has `Config::readonly` (PR #24). |
| 335 | Read-only routing examples | ⚪ | how-to. |
| 375 | azure-sql-edge on macOS hangs | 🟡 | **Repro**: try connecting bridge to an azure-sql-edge container. |
| 198 | Check if TCP connection is alive | 🔵 | Feature: `Client::ping` or `is_connected`. |
| 299 | Reset connection (`sp_reset_connection`) | 🔵 | Feature for pooling. |
| 301 | How do I call `ping`? | ⚪ | how-to. |
| 131 | Named pipes support | 🔵 | Feature. mssql-tds may not support named pipes. |
| 53  | Other connection methods than TCP | 🔵 | Same theme. |
| 125 | COUNT() fails on GCP | ⚪ | Environment issue. |
| 70  | diesel + tiberius | ⚪ | Out of scope. |

## G. API Surface / Ergonomics

| # | Title | Status | Notes |
|---|-------|--------|-------|
| 404 | Implement `Debug` for `ToSql` | 🔵 | Trait surface improvement. |
| 402 | Implement `Eq` for `Row` and `TokenRow` | 🔵 | For test asserts. |
| 397 | Expose `BaseMetaDataColumn` & `TypeInfo` | 🔵 | Bridge's `Column` already exposes type/precision/scale to some extent — verify completeness. |
| 403 | `BaseMetaDataColumn` does not retrieve Identity flag | 🔵 | **Check**: bridge's column metadata — does it carry IDENTITY/nullable/size? |
| 217 | Column nullable/size/scale enhancement | 🔵 | Same theme. |
| 383 | Constructing a `Row` for tests | 🔵 | Bridge `Row::from_schema` is `pub` already; verify usable for tests. |
| 262 | Make `QueryIdx` public | 🔵 | **Check**: bridge has `ColumnIndex` trait — public? |
| 258 | How to write wrapper for try_get? | ⚪ | how-to (related to #262). |
| 336 | `Config::trust_cert_ca` should take `Into<PathBuf>` | 🔵 | Bridge already added `trust_cert_ca` (PR #22) — check signature. |
| 382 | Raw identifier prefixes (r#) should be ignored | 🔵 | `IntoRow`/derive macros — bridge has none yet. |
| 30  | Statements (prepared) | 🔵 | Feature: prepared statements. mssql-tds may support `sp_prepare`/`sp_execute`. |
| 28  | Transactions | 🔵 | Feature: high-level Transaction wrapper. |
| 115 | Add Serde (de)serialization | 🔵 | Feature: `Row` ⇒ struct via serde. |
| 54  | Column Encryption (CEK) | 🔵 | Large feature. |
| 289 | ServiceBroker / SqlDependency | 🔵 | Large feature. |

## H. Streaming / Query Results

| # | Title | Status | Notes |
|---|-------|--------|-------|
| 380 | `QueryStream::into_results` doesn't return correct number | 🟡 | **Repro**: bridge `QueryResult::into_results` — empty SELECTs preserved? |
| 371 | QueryStream returns 1 row, ends with more remaining | 🟡 | **Repro**: bridge has streaming via `into_row_stream` (PR #29). |
| 365 | Cannot return `RowStream` from a function (lifetime tied to connection) | 🔵 | Bridge has same architecture; design issue. Could be addressed via owned stream. |
| 79  | Cancel safety on futures | 🔵 | Bridge inherits this from mssql-tds — needs investigation. |
| 300 | Cancel is not safe (tokio::time::timeout corrupts state) | 🟡 | **Repro**: timeout a `client.simple_query` and re-use. |
| 160 | `rows_affected` length incorrect when table has trigger | 🟡 | **Repro**: bridge's `ExecuteResult::rows_affected`. |
| 157 | "IN" prepared statement | ⚪ | how-to (TDS doesn't support array params). |

## I. Logging / Diagnostics

| # | Title | Status | Notes |
|---|-------|--------|-------|
| 281 | Opt out of INFO level logs | ⚪ | tracing-subscriber filter, user-side. Document. |
| 332 | How to close these logs | ⚪ | Same. |
| 321 | Repo status | ⚪ | meta. |

## J. Documentation / How-To

| # | Title | Status | Notes |
|---|-------|--------|-------|
| 377 | Extract date using `get` | ⚪ | how-to. |
| 275 | How to use stored procedures | ⚪ | how-to. |
| 310 | CREATE SCHEMA only works in `simple_query` | ⚪ | TDS RPC vs SQLBatch difference. Document. |
| 236 | CREATE FUNCTION error | ⚪ | how-to / SQL syntax. |
| 399 | CREATE VIEW error | ⚪ | how-to. |
| 101 | More examples | ⚪ | docs. |
| 344 | SQL Server 2000 invalid token | ⚪ | unsupported version. |

## K. Performance

| # | Title | Status | Notes |
|---|-------|--------|-------|
| 294 | tiberius slower than odbc-api / C# | 🟡 | **Benchmark**: bridge vs tiberius vs ODBC. |
| 226 | plp.rs buffer handling perf | 🟢 | tiberius-internal. |

## L. Build / Dependency / Security Advisories

| # | Title | Status | Notes |
|---|-------|--------|-------|
| 417 | `cargo audit` fails on rustls-webpki RUSTSEC-2026-0098/9/0104 | 🟡 | **Check**: run `cargo audit` on bridge. mssql-tds uses native-tls so likely clean. |
| 329 | Update tokio_rustls 0.24→0.25 | 🟢 | tiberius dep. |
| 323 | TLS compile errors | 🟢 | tiberius. |
| 317 | rustls duplicate-definition errors | 🟢 | tiberius. |
| 42  | Replace BytesMut/Bytes with Vec | 🟢 | tiberius-internal refactor. |

## M. Misc / Out of Scope

| # | Title | Status | Notes |
|---|-------|--------|-------|
| 199 | (closed/missing) | ⚪ | — |

---

## Action Plan — Prioritized for Bridge

### Tier 1 — High-confidence repros (file + likely fix)

| Tib# | Bridge action | Type |
|------|---------------|------|
| 414  | Send real hostname in `LoginMessage` | Feature |
| 219  | Case-insensitive `Row::get(name)` opt-in | Feature |
| 198/299/301 | `Client::ping()` / `is_connected()` | Feature |
| 262  | Ensure `ColumnIndex` is public + ergonomic for wrapping | Verify/expose |
| 401/277/354 | `IntoSql`/`ToSql` for `rust_decimal`, `time`, `jiff` | Verify + add gaps |
| 367  | `sendStringParametersAsUnicode` config knob | Feature |
| 224  | `Config::accept_invalid_hostnames` (separate from `trust_cert`) | Feature |
| 340  | `Config::host_name_in_certificate` | Feature |
| 169  | `FromSql for String` (owned) and `for Vec<u8>` (owned) | Verify |
| 417  | Run `cargo audit` on bridge | Audit |

### Tier 2 — Hands-on repro needed (may or may not affect bridge)

| Tib# | Repro target |
|------|--------------|
| 368  | Negative numeric Display |
| 316  | DateTime < 1900 panic |
| 325  | UTF-16 broken char |
| 211  | `try_get` panic path |
| 263  | I16(None) → i32 conversion |
| 320  | TLS handshake on `EncryptionLevel::Off` |
| 274  | TLS handshake EOF |
| 364  | macOS + SQL Server 2014 |
| 375  | azure-sql-edge on macOS |
| 380/371 | Streaming/result count |
| 300/79 | Cancel safety |
| 160  | rows_affected with triggers |
| 282  | String param to stored proc |
| 343  | libgssapi panic on Rust 1.78 |
| 333  | Special-char password |
| 221  | NaN → decimal |

### Tier 3 — Feature backlog

| Tib# | Feature |
|------|---------|
| 28   | Transactions |
| 30   | Prepared statements |
| 115  | Serde row deserialization |
| 54   | Column Encryption (CEK) |
| 289  | ServiceBroker |
| 131/53 | Named pipes / non-TCP transports |
| 337  | MultiSubnetFailover |
| 412  | TDS 8.0 / Strict encryption |
| 311/302 | bulk_insert column names + options (after bulk_insert ships) |
| 397/403/217 | Full column metadata exposure |
| 404/402 | Debug/Eq trait surface |
| 382  | r# raw-identifier handling in IntoRow derive |
| 407/276/97 | NTLM on Linux without Kerberos |

### Tier 4 — Already done in bridge

- 175 / 348 — AAD auth & ReadOnly intent (PRs #26, #24).
- 336 — `trust_cert_ca` exists (PR #22) — but verify signature matches request.

### Tier 5 — Out of scope / docs / unsupported

- 281 / 332 / 321 / 70 / 125 / 70 / 344 / 218 / 313 / 386 / 417 (rustls only) / 329 / 323 / 317 / 42 / 226 / 305 / 327 / 358 / 410 / 322 / 319 / 311 / 307 / 373 / 352 / 302 (most BCP) / 244 / 334 / 216 / 278 / 377 / 275 / 310 / 236 / 399 / 101 / 157

---

## Execution Strategy

The 101 issues collapse to **~25 tractable Tier 1+2 work items** for the bridge. Many are clusters that share a single fix (e.g., the four NTLM-on-Linux issues all become one `AuthMethod::ntlm` feature).

I'll fan out the **Tier 1 + Tier 2** work to background agents in parallel. Each agent:
1. Repros in `/Users/saurabh/work/mssql-tiberius-bridge` against `10.0.0.21,1434` (or unit test where possible).
2. If the issue exists, files a duplicate issue in `saurabh500/mssql-tiberius-bridge` linking the upstream tiberius issue.
3. Submits a PR with a minimal fix and tests.

Each agent owns one tiberius issue or a tightly-related cluster.

---

_Last updated: 2026-05-09_

---

## Bridge Issues Filed (Cross-Reference)

The following bridge issues were filed from this triage to track the work:

### Implemented (PRs landed or open)

| Bridge | PR | Tiberius | What |
|--------|----|----------|------|
| #37 | #38 | #417 | cargo-audit clean + weekly CI |
| #39 | #43 | #262 | `ColumnIndex` public |
| #40 | #43 | #169 | Owned `String`/`Vec<u8>` `FromSql` |
| #41 | #43 | #219 | Case-insensitive `Row::get_ci`/`try_get_ci` |
| #42 | #43 | #211 | `try_get` panic audit |
| #44 | #45 | #198, #299, #301 | `Client::ping()` |
| #46 | #50 | #414 | `Config::client_name` |
| #47 | #50 | #340 | `Config::host_name_in_certificate` |
| #49 | #50 | #367 | `Config::send_string_parameters_as_unicode` |
| —  | #51 | #368, #316, #160, #380, #371, #282, #221, #333 | Tier-2 repro tests |

### Filed as tracking issues (work pending)

| Bridge | Tiberius | Topic |
|--------|----------|-------|
| #48 | #224 | `accept_invalid_hostnames` (blocked on mssql-tds) |
| #52 | #299 | `Client::reset_session()` / `sp_reset_connection` (blocked on mssql-tds) |
| #53 | #311, #302, #410, #358, #322, #307, #319, #352, #373 | `Client::bulk_insert` (BCP) + options |
| #55 | #28  | Transactions API |
| #56 | #30  | Prepared Statements (`sp_prepare`/`sp_execute`) |
| #57 | #115 | Serde `Deserialize` for `Row` |
| #58 | #54  | Always Encrypted (CEK) |
| #59 | #289 | Service Broker / `SqlDependency` |
| #60 | #131, #53 | Named pipe + shared-memory transport |
| #61 | #337 | MultiSubnetFailover |
| #62 | #412 | TDS 8.0 Strict encryption (verify wiring) |
| #63 | #397, #403, #217 | Full column metadata (Identity, nullable, size, scale, collation) |
| #64 | #404 | `Debug` for `ToSql` |
| #65 | #402 | `PartialEq`/`Eq` for `Row` |
| #66 | #407, #276, #97 | NTLM on Linux/macOS without Kerberos |
| #67 | #277 | `time` crate `ToSql`/`IntoSql` |
| #68 | #354 | `jiff` crate support |
| #69 | #257 | `geography`/`geometry` spatial types |
| #70 | #381 | openssl backend for TLS 1.0/1.1 (legacy SQL Server) |

**Total: 14 PRs/issues already implemented + 19 tracking issues filed = 33 bridge work items derived from the triage.**
