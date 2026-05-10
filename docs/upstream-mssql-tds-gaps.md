# Upstream mssql-tds gaps

## `ColumnMetadata::get_precision() -> Option<u8>`

Upstream tracking: microsoft/mssql-rs#34.

Bridge issue #63 needs DECIMAL/NUMERIC precision metadata. `mssql-tds` already stores precision in the private `TypeInfoVariant::VarLenPrecisionScale` variant and exposes `get_scale()`, but there is no matching public precision accessor.

## `MultiPartName` accessors

Upstream tracking: microsoft/mssql-rs#35.

Bridge issue #63 needs four-part source table names. `ColumnMetadata::multi_part_name` is public, but the upstream `MultiPartName` fields are `pub(crate)` and there are no accessors, so the bridge cannot re-expose a stable `MultiPartName` without parsing formatted output.

## Public `ColumnMetadata`/`TypeInfo` construction for tests

Upstream tracking: microsoft/mssql-rs#36.

Bridge issue #63 would benefit from downstream unit tests that construct `ColumnMetadata` directly. Today `TypeInfo::type_info_variant` is private and there is no public constructor, so bridge tests cannot synthesize scale/collation/PLP metadata without a live TDS parser path.
