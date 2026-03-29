# Architecture

## System Shape

papera uses a two-stage AST transformation pipeline:

1. Parse source SQL with the selected dialect parser.
2. Apply statement-level transforms to top-level `Statement` variants.
3. Run `ExprRewriter` over the resulting AST via `VisitorMut`.
4. Emit SQL for the selected target dialect from the rewritten tree.

The split between statement-level and expression-level rewrites is intentional. Statement handlers own structural changes that may replace one statement kind with another, while `ExprRewriter` handles cross-cutting expression and table-factor rewrites.

Source dialect and target dialect are separate dimensions in the design. Source dialect controls parsing and some dialect-specific preprocessing, while target dialect controls function mappings, type rewrites, selected SHOW behavior, and some DDL lowering decisions.

## Dialect Layer

Each source dialect implements `Transpiler` and owns its parser selection plus any dialect-specific statement preprocessing:

- Trino: `src/dialect/trino.rs`
- Redshift: `src/dialect/redshift.rs`
- Hive: `src/dialect/hive.rs`

Hive shares substantial rewrite logic with Trino, but it remains separate because Hive-specific syntax such as `ROW FORMAT DELIMITED`, `STORED AS`, `PARTITIONED BY`, and `TBLPROPERTIES` must be parsed and normalized explicitly.

## Rewrite Subsystems

- `src/transpiler/rewrite.rs`: shared `ExprRewriter` traversal
- `src/transforms/functions.rs`: function registry plus custom AST rewrites
- `src/transforms/format_strings.rs`: Trino and Redshift format-token normalization
- `src/transforms/types.rs`: target-aware type normalization
- `src/transforms/ddl.rs`: DDL restructuring, external table conversion, and Iceberg conversion
- `src/transforms/dml.rs`: DML passthrough and explicit rejections
- `src/transforms/show.rs`: SHOW rewriting and target-specific passthrough or emulation
- `src/transforms/unnest.rs`: UNNEST normalization
- `src/transforms/lateral.rs`: Hive `LATERAL VIEW explode()` conversion

Function rewrites are split between declarative mappings and custom handlers:

- `Rename` for simple renames
- `RenameReorder` when the selected target wants the same function with a different argument order
- `Custom` when the rewrite must replace the original call with a different AST shape

`src/transforms/functions.rs` and `src/transforms/types.rs` dispatch on both source and target dialect. The current targets are `TargetDialect::DuckDB` and `TargetDialect::DataFusion`, with DuckDB as the default.

## Structural Design Constraints

- DDL column `DataType` nodes are not covered directly by `VisitorMut`, so `CREATE TABLE` column types are rewritten in statement handlers.
- Some source syntax support is constrained by sqlparser-rs AST output. Trino `ROW(a INT, b VARCHAR)` is exposed as flattened custom type data under `GenericDialect`, which makes nested `ROW` handling brittle. **Known limitation (sqlparser-rs 0.61)**: nested ROW types such as `ROW(x BIGINT, y ROW(i DOUBLE, j DOUBLE))` fail to parse entirely because `parse_optional_type_modifiers()` cannot handle nested parentheses. This affects all available dialects equally.
- Trino `ARRAY(T)` parsing is limited by the current parser choice, while `ARRAY<T>` and `T[]` are usable.
- Upstream sqlparser changes can require broad but mechanical rewrite updates because the code is tightly coupled to AST shape.

## Compatibility-Critical Paths

Several features require structural rewrites rather than simple renaming:

- `SHOW CREATE TABLE` and `SHOW CREATE VIEW` become DuckDB catalog queries
- External and Iceberg tables can become `CREATE VIEW ... AS SELECT ...` when enabled through options
- Redshift `COPY` can become reader-backed `INSERT INTO ... SELECT ...` when enabled through options
- Hive `LATERAL VIEW explode()` becomes `CROSS JOIN UNNEST`
- Some functions such as `NVL2` and bitwise helpers become non-function AST expressions

The architecture treats `TranspileOptions` as a safety boundary. Features that can silently change semantics or storage assumptions stay opt-in.

## Target Dialect Layer

`TranspileOptions` carries the target dialect through the rewrite system. That choice is threaded into:

- `ExprRewriter::new(source, target)` in `src/transpiler/rewrite.rs`
- target-aware function mappings in `src/transforms/functions.rs`
- target-aware type rewriting in `src/transforms/types.rs`
- selected DDL helpers and SHOW rewriting paths in `src/transforms/ddl.rs` and `src/transforms/show.rs`

DuckDB remains the more complete target. DataFusion support reuses the same high-level pipeline but needs its own mapping tables and unsupported paths where DuckDB-specific reader functions or catalog behavior do not exist.

## Packaging Boundary

The crate is library-first:

- `Cargo.toml` declares a library target
- the CLI binary is feature-gated behind `cli`
- `src/transforms` is `pub(crate)` so internal rewrite machinery is not part of the public API contract
- the stable public API is centered on `transpile`, `transpile_with_options`, source and target dialect selection, option types, the `SerdeClassResolver` extension hook, and shared error types

## Verification Strategy

The project uses layered regression coverage:

- Unit tests for focused rewrite behavior
- String-comparison integration tests for end-to-end transpilation output
- DuckDB execution tests for semantic validation against real engine behavior

This layered testing matters because valid SQL output can still be semantically wrong in DuckDB. Known examples include signature mismatches such as Redshift `dateadd` forms and downstream type-behavior mismatches such as `BLOB` with DuckDB `length()`.

## Engineering Guidance

- Decide first whether a feature is blocked by parsing, AST representation, or rewrite logic.
- Put structural statement changes in statement handlers and cross-cutting expression changes in `ExprRewriter`.
- Prefer declarative mappings where possible, but move to `Custom` rewrites when DuckDB requires different expression structure.
- Treat DuckDB execution tests as the authoritative check for compatibility-sensitive changes.
