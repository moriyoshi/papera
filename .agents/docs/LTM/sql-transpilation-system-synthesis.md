# SQL Transpilation System Synthesis

## Summary

papera is built around a two-stage AST rewrite pipeline that converts Trino, Redshift, and Hive SQL into DuckDB-compatible SQL. The system combines dialect-specific parsing and statement restructuring with shared expression-level rewrites, then validates behavior with both string-level and execution-level regression tests.

The durable lesson across the source documents is that most implementation risk sits at the boundary between source-dialect semantics, sqlparser-rs AST behavior, and actual DuckDB execution semantics. The later journal work reinforces that point by adding format conversion, S3 external-table lowering, and COPY lowering only behind explicit options and only after expanding regression coverage.

## Included Documents

| Document | Focus |
|----------|-------|
| [transpilation-pipeline-and-dialect-architecture.md](./transpilation-pipeline-and-dialect-architecture.md) | Core rewrite pipeline, dialect structure, module layout, and parser-driven architecture constraints |
| [duckdb-compatibility-and-feature-coverage.md](./duckdb-compatibility-and-feature-coverage.md) | Feature coverage, compatibility behavior in DuckDB, execution findings, and known limits |

## Stable Knowledge

- The rewrite pipeline intentionally has two phases: statement-level transforms first, then `ExprRewriter` traversal via `VisitorMut`.
- Statement-level rewrites are required for cases that change statement kind, such as `SHOW CREATE TABLE` to catalog queries, external-table DDL to `CREATE VIEW`, or Redshift `COPY` to reader-backed `INSERT INTO ... SELECT`.
- Expression-level rewrites cover function mapping, argument reordering, custom AST rewrites, type normalization inside expressions, format-string normalization, UNNEST normalization, and some table-factor-level rewrites.
- Each source dialect implements `Transpiler`, but the project shares large portions of rewrite logic across dialects.
- Hive remains a separate dialect because parsing support, not just rewrite behavior, differs materially from Trino and Redshift.
- `TranspileOptions` is a safety boundary: external-table, Iceberg, and COPY rewrites are opt-in and default to `Error`.
- Format-string normalization is part of compatibility work, not just cosmetic cleanup, because Trino and Redshift formatting tokens diverge materially from DuckDB.
- Parameter placeholders pass through unchanged, while some Redshift keyword-style function arguments still need AST rewriting into quoted string literals.
- Exact-looking rewrites can still be semantically wrong in DuckDB, so execution-backed tests are part of the core design, not an optional extra.
- sqlparser-rs AST details are a hard dependency of the architecture. Upstream AST changes can require non-trivial adaptation even when papera's user-visible behavior stays the same.
- Nested Trino `ROW` handling remains blocked by upstream parser behavior, which is a reminder to separate parser limitations from rewrite limitations before designing fixes.

## Operational Guidance

When modifying the transpilation system, approach the work in this order:

1. Confirm whether the feature is blocked by parsing, AST representation, or only by rewrite logic.
2. Decide whether the rewrite belongs at statement level or expression level.
3. Prefer declarative mappings for simple renames and reorderings, but use `Custom` handlers when DuckDB requires real AST restructuring.
4. Verify the result with execution tests when the target behavior depends on DuckDB semantics rather than on SQL surface syntax alone.

For new compatibility work, use these heuristics:

- Put DDL column-type rewrites in statement handlers because `VisitorMut` does not directly cover those nodes.
- Treat parser quirks as first-class constraints. Trino `ROW` and `ARRAY(T)` handling already show that parser output may limit what is feasible.
- Keep approximation boundaries explicit. URL extraction, SHOW reconstruction, some array helpers, and reader-backed ingestion rewrites are not exact semantic matches.
- Preserve opt-in behavior for conversions that can silently change semantics or storage assumptions, including external-table view generation and Redshift COPY lowering.
- When reader-backed rewrites are introduced, document both supported option mappings and intentionally dropped options so callers understand the semantic gap.
- Distinguish between values that can be rewritten statically and values that are only known at runtime. Literal format strings can be converted during transpilation, but runtime format expressions cannot.

## Files

- `src/lib.rs`: top-level API and option types used to control compatibility-sensitive behavior
- `src/transpiler/mod.rs`: `Transpiler` trait that defines the dialect contract
- `src/transpiler/rewrite.rs`: shared expression and table rewrite walker
- `src/dialect/trino.rs`: Trino parsing and orchestration
- `src/dialect/redshift.rs`: Redshift parsing and orchestration
- `src/dialect/hive.rs`: Hive parsing and orchestration
- `src/transforms/functions.rs`: large mapping registry and custom rewrites
- `src/transforms/format_strings.rs`: dialect-specific format-token conversion
- `src/transforms/types.rs`: type compatibility rewrites
- `src/transforms/ddl.rs`: DDL restructuring, external table handling, Iceberg conversion, and Trino S3 table lowering
- `src/transforms/show.rs`: SHOW emulation through DuckDB metadata queries
- `tests/integration.rs`: string-level regression checks
- `tests/duckdb_integration.rs`: execution-level semantic checks against real DuckDB

## Tests

The project relies on layered regression coverage:

- Unit tests for focused transform behavior
- String-comparison integration tests for end-to-end rewrite output
- DuckDB execution tests for semantic validation

Useful commands recorded in the source docs:

- `cargo test`
- `cargo test --test integration`
- `cargo test --test duckdb_integration`

The later journal state raised the recorded totals to 245 passing tests, with broader coverage for format conversion, COPY rewriting, SerDe-based external tables, function emulation, keyword-argument quoting, and Trino S3-backed table lowering.

## Pitfalls

- Do not assume SQL string equivalence implies behavioral equivalence in DuckDB.
- Do not collapse statement and expression rewrites into one generic pass; some transforms require replacing whole statement forms.
- Trino `ROW` handling is still constrained by flattened parser output, so nested cases remain risky.
- Redshift `dateadd` and related functions illustrate that rename-only mappings are insufficient when signatures diverge.
- Type rewrites can trigger downstream behavioral mismatches, such as `VARBINARY` to `BLOB` interacting badly with DuckDB `length()`.
- Parser upgrades can break rewrite code through AST shape changes alone.
- COPY and external-table rewrites are useful compatibility tools, but they are still approximations around reader selection, ingestion options, and storage semantics.
- Not every source-dialect difference should become a broad rewrite rule. Some fixes, such as quoting unquoted Redshift keyword arguments, are intentionally narrow because they depend on AST shape and literal detectability.
