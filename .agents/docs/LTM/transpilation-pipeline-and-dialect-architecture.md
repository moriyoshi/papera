# Transpilation Pipeline and Dialect Architecture

## Summary

papera rewrites source SQL into DuckDB SQL using a two-stage AST transformation pipeline. Statement-level transforms handle structural rewrites such as DDL, SHOW emulation, COPY conversion, and external-table lowering, then an expression walker applies cross-cutting rewrites for functions, types, format strings, keyword-style function arguments, UNNEST behavior, and lateral expansion.

The code is organized so each source dialect owns its parsing and statement transform entrypoints while sharing common rewrite helpers. The main architectural constraints come from sqlparser-rs AST shapes and parser behavior, so implementation details are tightly coupled to the library version in use and to the option-gated compatibility behavior exposed by `TranspileOptions`.

## Key Facts

- The pipeline is split into statement-level rewrites first, then `ExprRewriter` traversal via `VisitorMut`.
- Each dialect implements `Transpiler`, and Hive reuses much of Trino's type and function rewrite logic.
- External-table, Iceberg, and Redshift COPY handling are caller-configurable through `TranspileOptions`.
- Trino `CREATE TABLE ... WITH (external_location = ...)` is treated as an external-table rewrite path, not as a plain DDL passthrough.
- Dialect-specific format strings are normalized through dedicated helpers before reaching DuckDB `strftime` and `strptime` calls.
- Redshift functions such as `date_part` and `date_trunc` may require identifier-to-string normalization when source SQL uses unquoted keyword arguments.
- sqlparser-rs limitations directly shape the implementation, especially around `DataType`, Trino `ROW`, and dialect-specific parsing support.
- The upgrade from sqlparser `0.55` to `0.61` changed several AST variants and required explicit adaptation in the rewrite layer.

## Details

### Pipeline shape

The core rewrite flow is:

1. Parse source SQL with the dialect-specific parser.
2. Apply statement-level transforms to top-level `Statement` variants such as `CREATE TABLE`, `ALTER TABLE`, `SHOW`, and `COPY`.
3. Walk the transformed AST with `ExprRewriter`, which implements `VisitorMut`.
4. Emit DuckDB-compatible SQL from the rewritten AST.

This split exists because some operations fundamentally change the statement kind. Examples include:

- `SHOW CREATE TABLE` becoming a `SELECT` against catalog metadata.
- External or Iceberg table DDL becoming `CREATE VIEW ... AS SELECT ...`.
- Trino S3-backed `CREATE TABLE ... WITH (external_location = ...)` becoming `CREATE VIEW ... AS SELECT * FROM read_* (...)`.
- Redshift `COPY` becoming `INSERT INTO ... SELECT * FROM read_* (...)` when enabled.
- Hive `LATERAL VIEW explode()` becoming `CROSS JOIN UNNEST`.

Expression-level rewrites then handle cross-cutting concerns without duplicating logic across statements. The walker rewrites:

- Function names and argument orderings
- `CAST` data types
- Format-string literals for formatting and parsing functions
- Identifier-like keyword arguments that DuckDB expects as quoted string literals
- `UNNEST` flags such as `WITH OFFSET` to `WITH ORDINALITY`
- Lateral expansion cases that can be normalized at the table-factor level
- Emulated expressions such as `AT TIME ZONE`, windowed ratios, and function-to-expression substitutions

### Dialect responsibilities

Each dialect implements the `Transpiler` trait and follows the same control flow, but parser choice and statement preprocessing differ:

- Trino uses `GenericDialect`.
- Redshift uses `RedshiftSqlDialect`.
- Hive uses `HiveDialect`.

Hive is kept as a separate dialect even though it shares much of Trino's rewrite logic. That separation is necessary because Hive-specific syntax such as `ROW FORMAT DELIMITED`, `STORED AS`, `PARTITIONED BY`, and `TBLPROPERTIES` must be parsed by `HiveDialect`.

### Data structures and module layout

The main implementation modules are:

- `src/transpiler/mod.rs`: `Transpiler` trait
- `src/transpiler/rewrite.rs`: `ExprRewriter`
- `src/dialect/trino.rs`: Trino entrypoint
- `src/dialect/redshift.rs`: Redshift entrypoint
- `src/dialect/hive.rs`: Hive entrypoint
- `src/transforms/types.rs`: type rewrites
- `src/transforms/functions.rs`: function mapping registry and custom rewrites
- `src/transforms/format_strings.rs`: Redshift and Trino format-token normalization
- `src/transforms/ddl.rs`: DDL rewrites including external, Iceberg, and Trino S3 table handling
- `src/transforms/dml.rs`: DML passthrough and explicit rejections
- `src/transforms/show.rs`: SHOW emulation
- `src/transforms/unnest.rs`: UNNEST normalization
- `src/transforms/lateral.rs`: Hive lateral expansion rewrite

Function mapping uses `HashMap<&str, FunctionMapping>`. `FunctionMapping` supports:

- `Rename`: rename only
- `RenameReorder`: rename plus argument reordering
- `Custom`: arbitrary AST-level rewrite, optionally replacing a function call with a different `Expr`

Additional option-gated helpers introduced later include:

- `CopyBehavior` in `src/lib.rs` to control Redshift `COPY` lowering
- Trino S3 external-table detection in `src/transforms/ddl.rs` via `external_location` and `format`
- SerDe-class fallback detection in `src/transforms/ddl.rs` when Hive metadata omits `STORED AS`

Other reusable expression helpers added later include:

- `redshift_quote_first_arg` in `src/transforms/functions.rs` for keyword-style first arguments such as `date_part(year, ts)`

### Parser and AST constraints

Implementation details are driven by several sqlparser-rs behaviors:

- `VisitorMut` does not visit DDL column `DataType` nodes directly, so `CreateTable.columns` must be rewritten explicitly during statement handling.
- Trino `ROW(a INT, b VARCHAR)` parsed through `GenericDialect` becomes a flattened `DataType::Custom(...)` payload rather than a nested structured type, so reconstruction is string-based and only reliable for simple cases.
- Trino `ARRAY(T)` does not parse under `GenericDialect`; only `ARRAY<T>` or `T[]` forms are usable.
- `Expr::Value` wraps `ValueWithSpan`, not raw `Value`.
- `ObjectName` parts must be rebuilt with `ObjectNamePart::Identifier(Ident)`.
- `TableAlias.columns` uses `TableAliasColumnDef`, not raw identifiers.
- Nested Trino `ROW` types remain blocked upstream. Across all tested sqlparser `0.61` dialects, `ROW(...)` falls through to flat `Custom`-type modifier parsing, so nested parentheses are not preserved.

### Option-gated rewrite boundaries

The architecture deliberately keeps behavior-changing rewrites behind explicit options:

- `ExternalTableBehavior::MapToView` covers both Hive-style `CREATE EXTERNAL TABLE` and Trino-style `WITH (external_location = ...)` S3 tables.
- `IcebergTableBehavior::MapToView` remains a separate gate for `iceberg_scan(...)` view generation.
- `CopyBehavior::MapToInsert` controls whether Redshift `COPY` is rejected or lowered to reader-backed `INSERT INTO ... SELECT`.

Within DDL rewriting, Iceberg detection takes priority over Trino S3 external-table detection. A table carrying both `table_type = 'ICEBERG'` and `external_location` routes through the Iceberg path.

### sqlparser `0.61` adaptation

The upgrade from `0.55` to `0.61` required targeted AST updates:

- `Statement::CreateView` changed into a tuple variant wrapping `CreateView`.
- `Statement::AlterTable` changed into a tuple variant wrapping `AlterTable`.
- `CreateTable.table_properties` and `with_options` were unified into `CreateTableOptions`.
- `Expr::Cast` gained an `array` field.
- `Expr::Case` gained token fields.
- `StructField` gained `options`.
- `TableAlias` gained `explicit`.
- Some operator variants were renamed or gained explicit constraint payloads.

These were mechanical changes, but they confirm that the rewrite layer is sensitive to upstream AST evolution.

## Files

- `src/lib.rs`: top-level entrypoints and options types
- `src/transpiler/mod.rs`: dialect-independent transpiler interface
- `src/transpiler/rewrite.rs`: shared AST visitor for expression and table rewrites
- `src/dialect/trino.rs`: Trino parser and statement transform orchestration
- `src/dialect/redshift.rs`: Redshift parser and statement transform orchestration
- `src/dialect/hive.rs`: Hive parser and statement transform orchestration
- `src/transforms/ddl.rs`: structural DDL rewrites
- `src/transforms/functions.rs`: mapping tables and custom function rewrites
- `src/transforms/format_strings.rs`: format-token conversion helpers
- `src/transforms/types.rs`: source-to-DuckDB type normalization

## Test Coverage

Architecture behavior is exercised indirectly by the main test suites:

- `cargo test` for the full suite
- `cargo test --test integration` for string-level end-to-end assertions
- `cargo test --test duckdb_integration` for semantic execution checks against in-memory DuckDB

The later journal entries raised the recorded totals to 245 passing tests: 149 unit tests, 44 string-level integration tests, and 52 DuckDB execution tests.

## Pitfalls

- Statement and expression rewrites cannot be collapsed into one pass without losing the ability to replace whole statement kinds.
- String-based reconstruction of Trino `ROW` metadata is brittle for nested types.
- Parser support is dialect-sensitive, so adding syntax support often requires parser changes, not just rewrite logic.
- Upstream sqlparser AST changes can break the rewrite layer even when the project logic stays the same.
- Reusing option gates matters. External tables, Iceberg tables, and COPY all change execution semantics and should not silently rewrite under default settings.
- Trino `external_location` handling and Iceberg handling overlap in the same statement family, so precedence must stay explicit.
- Format conversion and keyword quoting are only safe when the SQL text provides enough static structure. Literal conversion can happen at transpile time, but runtime-driven formats cannot be normalized ahead of execution.
