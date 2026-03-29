# DuckDB Compatibility and Feature Coverage

## Summary

papera aims to preserve query intent across Trino, Redshift, and Hive while targeting DuckDB execution semantics. The supported surface is broad, and later journal work expanded it further with format-string conversion, COPY lowering, Hive SerDe fallback mapping, Trino S3 external-table rewriting, and several function emulations in addition to direct renames and AST-level custom rewrites.

The most reliable compatibility signal comes from executing transpiled SQL against real DuckDB in integration tests. Those tests surfaced several semantic mismatches that string comparison alone would not catch and directly drove follow-up rewrites such as Redshift `DATEADD` and `DATEDIFF`.

## Key Facts

- The recorded function coverage reached 134 explicit mappings before a later `date_part` addition and related quoting fix extended the Redshift surface further.
- SHOW support is implemented by rewriting into DuckDB catalog queries rather than relying on native DuckDB SHOW behavior.
- External tables and Iceberg tables can be mapped to `CREATE VIEW` statements backed by reader functions, but only when explicitly enabled through options.
- Trino `WITH (external_location = ..., format = ...)` tables now use the same opt-in external-table strategy as Hive-style `CREATE EXTERNAL TABLE`.
- Redshift `COPY` can be lowered to `INSERT INTO ... SELECT * FROM read_* (...)` when `CopyBehavior::MapToInsert` is enabled.
- Hive external table metadata such as delimiters and partitioning is translated into reader function options when possible, and common SerDe classes are recognized even without `STORED AS`.
- Parameterized placeholders such as `$1`, `:name`, and `?` pass through transpilation unchanged via sqlparser placeholder values.
- Some mappings are intentionally approximate, especially URL extraction, SHOW CREATE TABLE reconstruction, reader-backed ingestion rewrites, and several array and JSON helpers.

## Details

### Function and type coverage

The journal records broad support across these categories:

- Trino: aggregate, date/time, string, array/map, JSON, math/numeric, bitwise, timezone, and formatting functions
- Redshift: date/time, string, aggregate, JSON, and ingestion-adjacent compatibility helpers
- Shared type rewrites such as `ROW` to `STRUCT`, `VARCHAR(MAX)` normalization, and `SUPER` to `JSON`

Many mappings are straightforward renames, but several require custom AST construction. Notable custom or emulated cases include:

- Trino bitwise functions rewritten into DuckDB infix and unary operators
- Trino `at_timezone(ts, tz)` and `with_timezone(ts, tz)` rewritten into `AT TIME ZONE`
- Trino `date_format`, `format_datetime`, and `parse_datetime` routed through Java-style format conversion before DuckDB formatting functions
- `NVL2` rewritten into `CASE WHEN`
- Array overlap and containment helpers expressed through `list_intersect`, `len`, and related list functions
- JSON parsing and formatting expressed through explicit casts
- Redshift `DATEADD`, `DATEDIFF`, `ADD_MONTHS`, and `MONTHS_BETWEEN` rewritten into DuckDB interval or `date_diff` forms instead of rename-only mappings
- Redshift `date_part` and `date_trunc` normalize unquoted keyword first arguments into quoted string literals when needed
- Redshift `to_char`, `to_date`, and `to_timestamp` normalized through PostgreSQL-style format conversion before reaching DuckDB formatting functions
- Redshift `STRTOL(str, base)` emulated with a `CASE` expression so the base can remain runtime-dependent
- Trino `map_agg(k, v)` emulated as `map(list(k), list(v))`
- Redshift `RATIO_TO_REPORT(col) OVER (...)` emulated as `col / SUM(col) OVER (...)`

The later coverage audit also filled specific remaining gaps with:

- Trino: `from_hex`, `rand`, `date_format`, `at_timezone`, `to_unixtime`, `parse_datetime`, `with_timezone`, `current_timezone`
- Redshift: `MONTHS_BETWEEN`, `ADD_MONTHS`, `STRTOL`, `RATIO_TO_REPORT`

`src/transforms/functions.rs` remained the main compatibility registry, with `src/transforms/format_strings.rs` added later to keep dialect-format conversion out of the main mapping table.

Parameterized queries do not need a dedicated compatibility layer in papera. The journal records that sqlparser-rs already models placeholders such as `$1`, `:name`, and `?` as placeholder values, so they flow through transpilation unchanged.

### SHOW, external tables, Iceberg, and COPY handling

`SHOW CREATE TABLE` is emulated by reading `information_schema.columns` and rebuilding an approximate `CREATE TABLE` statement with `string_agg`. This preserves the basic column list but does not reproduce constraints, defaults, or indexes.

`SHOW CREATE VIEW` is rewritten to query `duckdb_views()` because DuckDB stores the original view SQL there.

External and Iceberg table conversion is controlled through `TranspileOptions`:

- `ExternalTableBehavior::MapToView` converts supported external table definitions into `CREATE VIEW ... AS SELECT * FROM read_parquet(...)` or similar reader-based SQL.
- The same `ExternalTableBehavior::MapToView` gate also covers Trino tables whose `WITH` options declare `external_location`.
- `IcebergTableBehavior::MapToView` converts supported Iceberg definitions into `CREATE VIEW ... AS SELECT * FROM iceberg_scan(...)`.
- The default behavior is `Error`, which avoids silently emitting partial or incorrect rewrites.

Iceberg detection is based on table options such as `table_type = 'ICEBERG'`, not on parser flags that target unrelated dialects.

Hive external table conversion preserves relevant storage metadata where possible:

- `PARTITIONED BY` enables `hive_partitioning = true`
- `FIELDS TERMINATED BY` becomes `delim`
- `ESCAPED BY` becomes `escape`
- `LINES TERMINATED BY` becomes `new_line`
- `NULL DEFINED AS` becomes `nullstr`

When Hive metadata omits `STORED AS`, the DDL layer falls back to common SerDe class names:

- `ParquetHiveSerDe` and `OrcSerde` map to `read_parquet`
- `JsonSerDe` maps to `read_json`
- `OpenCSVSerde` and `LazySimpleSerDe` map to `read_csv`

Redshift `COPY` is also opt-in and reader-backed:

- `PARQUET` uses `read_parquet`
- `JSON` uses `read_json`
- `CSV` or default-delimited input uses `read_csv`
- Options such as `DELIMITER`, `IGNOREHEADER`, `NULL`, `EMPTYASNULL`, `ESCAPE`, `GZIP`, `BZIP2`, `ZSTD`, `DATEFORMAT`, and `TIMEFORMAT` are translated when DuckDB has a compatible parameter
- Infrastructure-specific Redshift options such as `IAM_ROLE`, `MANIFEST`, `COMPUPDATE`, `STATUPDATE`, and `REGION` are dropped

Trino S3-backed non-Iceberg tables use `external_location` plus optional `format` to choose a DuckDB reader. Recorded mappings include `PARQUET` and `ORC` to `read_parquet`, `TEXTFILE` and `CSV` to `read_csv`, and `JSON` to `read_json`. Missing `format` defaults to `PARQUET`.

### Integration-test findings

The DuckDB-backed integration suite exposed several semantic mismatches:

- Redshift `dateadd(part, n, date)` does not match DuckDB `date_add(date, interval)` by signature, which led to a later custom interval-arithmetic rewrite.
- The same issue applies to Redshift `datediff`, which now emits quoted datepart strings for DuckDB `date_diff`.
- DuckDB `length()` does not accept `BLOB`, so VARBINARY-to-BLOB rewrites can break downstream string-length assumptions and may require `octet_length()`.
- DuckDB list and map values often need explicit `CAST(... AS VARCHAR)` in assertions because the Rust bindings do not auto-coerce them into strings.

These findings justify keeping the DuckDB execution suite as a first-class regression layer instead of relying on SQL string snapshots alone.

Later integration coverage expanded to include:

- Redshift DATEADD and DATEDIFF rewrites
- Trino and Redshift format-string conversion paths
- Hive SerDe-based external-table rewriting
- Redshift COPY lowering paths and error-by-default behavior
- Combined ETL-style scenarios involving several rewrite families in one query stream

### Known limits

The journal explicitly calls out these unresolved gaps or approximations:

- Nested Trino `ROW` types are not reliably reconstructed.
- `CLUSTERED BY ... INTO N BUCKETS` has no DuckDB equivalent.
- Some collection and map delimiters have no direct `read_csv` equivalent.
- Trino `ARRAY(T)` syntax does not parse with the current parser choice.
- Trino S3 formats such as `AVRO`, `SEQUENCEFILE`, and `RCFILE` are still rejected.
- Redshift system views such as `svv_*`, `stl_*`, and `stv_*` are intentionally not mapped because the schemas and runtime semantics do not line up with DuckDB.

## Files

- `src/transforms/functions.rs`: mapping registry and custom expression rewrites
- `src/transforms/format_strings.rs`: dialect-format to DuckDB-format conversion helpers
- `src/transforms/types.rs`: compatibility-oriented type normalization
- `src/transforms/ddl.rs`: external table, Iceberg, SerDe, and reader-backed conversion logic
- `src/transforms/show.rs`: SHOW emulation
- `tests/integration.rs`: string-based end-to-end assertions
- `tests/duckdb_integration.rs`: execution-based compatibility checks

## Test Coverage

The journal recorded these suites after the later expansion work:

- 149 unit tests across transform modules
- 44 string-comparison integration tests in `tests/integration.rs`
- 52 DuckDB execution tests in `tests/duckdb_integration.rs`
- 245 total tests

Useful commands:

- `cargo test`
- `cargo test --test integration`
- `cargo test --test duckdb_integration`

## Pitfalls

- A valid-looking SQL rewrite can still be semantically wrong under DuckDB execution.
- Several supported features are approximations, not exact source-dialect reproductions.
- External and Iceberg rewrites should stay opt-in because incorrect path or format inference can silently change behavior.
- COPY lowering is also a compatibility approximation. Reader selection and option dropping can preserve utility without reproducing Redshift ingestion semantics exactly.
- Format conversion can only be done for literal format strings embedded in the source SQL. Runtime expressions stay unchanged.
- Coverage breadth in the mapping table does not eliminate parser-level syntax gaps.
