# Long-Term Memory Index

## Synthesis Documents

| Document | Consolidates | Summary |
|----------|--------------|---------|
| [sql-transpilation-system-synthesis.md](sql-transpilation-system-synthesis.md) | `transpilation-pipeline-and-dialect-architecture.md`, `duckdb-compatibility-and-feature-coverage.md` | High-level synthesis of the rewrite pipeline, dialect responsibilities, compatibility behavior, and the main implementation risks. |
| [library-api-and-target-dialect-synthesis.md](library-api-and-target-dialect-synthesis.md) | `library-packaging-and-public-api.md`, `serde-class-resolver.md`, `datafusion-target-dialect.md` | High-level synthesis of the crate's stable caller-facing API, extension points, target selection, and the execution-backed guidance for DataFusion support. |

## Source Topic Documents

| Document | Summary |
|----------|---------|
| [transpilation-pipeline-and-dialect-architecture.md](transpilation-pipeline-and-dialect-architecture.md) | Core AST rewrite pipeline, option-gated DDL and COPY lowering, module layout, and sqlparser-driven implementation constraints. |
| [duckdb-compatibility-and-feature-coverage.md](duckdb-compatibility-and-feature-coverage.md) | Supported dialect coverage, format and reader-function rewrites, DuckDB execution behavior, and known compatibility limits. |
| [library-packaging-and-public-api.md](library-packaging-and-public-api.md) | Library-first crate packaging, feature-gated CLI behavior, and the intended public API surface. |
| [serde-class-resolver.md](serde-class-resolver.md) | Configurable user-supplied callback for mapping Hive SerDe class names to DuckDB reader functions, with fallthrough to built-in inference. |
| [datafusion-target-dialect.md](datafusion-target-dialect.md) | `TargetDialect::DataFusion` support: function/type mapping tables, execution-backed DataFusion 52.4.0 audit results, custom date arithmetic, and remaining unsupported paths. |
