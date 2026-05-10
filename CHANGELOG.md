# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This file is maintained by [release-plz](https://release-plz.dev/) — entries
are generated from [Conventional Commits](https://www.conventionalcommits.org/)
when the Release PR is opened.

## [Unreleased]

## [0.1.0-preview.3](https://github.com/saurabh500/mssql-tiberius-bridge/compare/v0.1.0-preview.2...v0.1.0-preview.3) - 2026-05-10

### Added

- *(row)* Impl PartialEq for Row (closes #65) ([#65](https://github.com/saurabh500/mssql-tiberius-bridge/issues/65), [#402](https://github.com/saurabh500/mssql-tiberius-bridge/issues/402), [#65](https://github.com/saurabh500/mssql-tiberius-bridge/issues/65), [#402](https://github.com/saurabh500/mssql-tiberius-bridge/issues/402))
- *(config)* Add Named Pipes and Shared Memory transports (closes #60) ([#60](https://github.com/saurabh500/mssql-tiberius-bridge/issues/60), [#131](https://github.com/saurabh500/mssql-tiberius-bridge/issues/131), [#53](https://github.com/saurabh500/mssql-tiberius-bridge/issues/53), [#60](https://github.com/saurabh500/mssql-tiberius-bridge/issues/60), [#131](https://github.com/saurabh500/mssql-tiberius-bridge/issues/131), [#53](https://github.com/saurabh500/mssql-tiberius-bridge/issues/53))
- *(config)* Add MultiSubnetFailover (closes #61) ([#61](https://github.com/saurabh500/mssql-tiberius-bridge/issues/61), [#61](https://github.com/saurabh500/mssql-tiberius-bridge/issues/61))
- *(config)* Verify and test TDS 8.0 Strict encryption (closes #62) ([#62](https://github.com/saurabh500/mssql-tiberius-bridge/issues/62), [#62](https://github.com/saurabh500/mssql-tiberius-bridge/issues/62))

### Tests

- *(strict)* Gate on BRIDGE_STRICT_READY=1 so default CI skips ([#74](https://github.com/saurabh500/mssql-tiberius-bridge/issues/74), [#74](https://github.com/saurabh500/mssql-tiberius-bridge/issues/74))
- *(strict)* Reuse TEST_DB_* env vars; track infra in #74 ([#74](https://github.com/saurabh500/mssql-tiberius-bridge/issues/74), [#74](https://github.com/saurabh500/mssql-tiberius-bridge/issues/74), [#74](https://github.com/saurabh500/mssql-tiberius-bridge/issues/74), [#74](https://github.com/saurabh500/mssql-tiberius-bridge/issues/74))
- Add tier-2 tiberius repro coverage

## [0.1.0-preview.2] - 2026-05-09

### Added
- *(config)* `Config::trust_cert_ca` for CA pinning ([#22](https://github.com/saurabh500/mssql-tiberius-bridge/pull/22))
- *(query)* `ToSql` for `Vec<u8>`, `&[u8]`, and chrono date/time types ([#23](https://github.com/saurabh500/mssql-tiberius-bridge/pull/23))
- *(config)* `Config::readonly()` for `ApplicationIntent=ReadOnly` ([#24](https://github.com/saurabh500/mssql-tiberius-bridge/pull/24))
- *(auth)* `AuthMethod::aad_token` for Entra ID federated auth ([#26](https://github.com/saurabh500/mssql-tiberius-bridge/pull/26))
- *(config)* SSRP instance lookup auto-trigger when `instance_name` is set ([#27](https://github.com/saurabh500/mssql-tiberius-bridge/pull/27))
- *(query)* `QueryResult::into_row_stream` for streaming API compatibility ([#29](https://github.com/saurabh500/mssql-tiberius-bridge/pull/29))
- *(lib)* Re-export `DecimalParts` from `mssql_tds` ([#32](https://github.com/saurabh500/mssql-tiberius-bridge/pull/32))

### Fixed
- *(config)* Drop port from datasource when `instance_name` is set

### Performance
- *(row)* Share per-result-set schema via `Arc<RowSchema>` ([#31](https://github.com/saurabh500/mssql-tiberius-bridge/pull/31))

### Documentation
- Document runtime dependencies for each `AuthMethod` ([#30](https://github.com/saurabh500/mssql-tiberius-bridge/pull/30))

## [0.1.0-preview.1] - 2026-04-17

Initial public preview.

### Added
- Tiberius-compatible API surface (`Client`, `Config`, `AuthMethod`, `Row`, `QueryResult`, `ToSql`, `FromSql`).
- `AuthMethod::sql_server` and `AuthMethod::integrated` (Kerberos/NTLM via Windows SSPI / Linux GSSAPI dlopen).
- Connection pooling via `deadpool` (`Pool`, `PooledConnection`, `TdsManager`).
- TLS via `native-tls` with `EncryptionLevel::Off | On | Required` and `trust_cert`.
- Azure SQL routing redirects (auto-handled by `mssql-tds`, no manual reconnect needed).
- JSON and Vector column type support (closes #3).
- Pre-decoded string cache enabling `&str` borrowing from `Row`.
- Unique UserAgent identity in TDS Login7 packet.
- Code coverage via `cargo-llvm-cov` + Codecov upload.
- Comprehensive tiberius-compatibility test suite + API documentation.
- Publish workflow for crates.io.

### Fixed
- IntN/FltN/MoneyN column type resolution using wire byte length.
- Time decode: `time_nanoseconds` is in 100ns units, not nanoseconds.

### Changed
- *(refactor)* Expose `Column` fields via getter methods instead of `pub`.

[Unreleased]: https://github.com/saurabh500/mssql-tiberius-bridge/compare/v0.1.0-preview.2...HEAD
[0.1.0-preview.2]: https://github.com/saurabh500/mssql-tiberius-bridge/compare/v0.1.0-preview.1...v0.1.0-preview.2
[0.1.0-preview.1]: https://github.com/saurabh500/mssql-tiberius-bridge/releases/tag/v0.1.0-preview.1
