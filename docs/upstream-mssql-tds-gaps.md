# Upstream mssql-tds metadata support and limitations

The bridge uses `mssql-tds` 0.1.0. The APIs below are available in that published
version; the historical issues no longer block the bridge's wire-metadata support
for #63.

## `ColumnMetadata::get_precision() -> Option<u8>`

Historical upstream tracking: saurabh500/mssql-rs#2.

`ColumnMetadata::get_precision()` exposes declared DECIMAL/NUMERIC precision and
fixed MONEY/SMALLMONEY precision. The bridge stores this value on `Column` rather
than inferring precision from storage length. `get_scale()` supplies scale where
the wire descriptor carries it.

## `MultiPartName` accessors

Historical upstream tracking: saurabh500/mssql-rs#3.

The upstream type exposes `server_name()`, `catalog_name()`, `schema_name()`, and
`table_name()`. The bridge copies supplied components into its own `MultiPartName`.
`COLMETADATA` table names apply to legacy TEXT/NTEXT/IMAGE columns, not arbitrary
result-column lineage. Missing parts remain absent; formatted output is never
parsed to reconstruct names.

## Public `ColumnMetadata`/`TypeInfo` construction for tests

Historical upstream tracking: saurabh500/mssql-rs#4.

`TypeInfo` now has public constructors for fixed, variable, string,
precision/scale, temporal-scale, and PLP descriptors. `ColumnMetadata` still
contains private fields, but the existing `test-util` feature provides
`test_client_support::int_columns()` as a metadata seed and scripted clients.
Bridge tests replace public fields on that seed to exercise real conversion
without requiring new dependencies or a live parser for each unit test.

Populated upstream source-name components are not publicly constructible; their
conversion is also covered with SQL Server's real legacy-LOB metadata.

## Catalog-only properties

The [MS-TDS COLMETADATA flags](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/58880b9f-381c-43b2-bf8b-0727a98c4f4c)
include `fSparseColumnSet`, not ordinary `SPARSE` or `ROWGUIDCOL` properties.
These properties require catalog inspection and remain explicitly deferred for
#63. The bridge neither issues hidden catalog queries nor reports guessed flags.

The historical issue links above do not imply their GitHub issue states changed.
