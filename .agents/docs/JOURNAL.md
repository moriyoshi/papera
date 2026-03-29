# papera Development Journal

## 2026-03-29: Initial Implementation

Compressed after LTM consolidation.

Retained note: this session established the initial two-stage AST rewrite pipeline, the three source dialects ( Trino, Redshift, Hive ), the option-gated external-table and Iceberg behavior, and the first execution-backed compatibility findings.

See:
- `transpilation-pipeline-and-dialect-architecture.md`
- `duckdb-compatibility-and-feature-coverage.md`

---

## 2026-03-29: User-driven expansion and sqlparser 0.61 upgrade

Compressed after LTM consolidation.

Retained note: this pass expanded function coverage, completed the sqlparser `0.61` adaptation, strengthened SHOW and Iceberg handling, and recorded the remaining compatibility gaps that were later resolved or explicitly closed.

See:
- `transpilation-pipeline-and-dialect-architecture.md`
- `duckdb-compatibility-and-feature-coverage.md`

---

## 2026-03-29: Library packaging and public API cleanup

Compressed after LTM consolidation.

Retained note: this entry recorded the library-first packaging change, feature-gated CLI build, and the reduced semver-facing public API surface.

See:
- `library-packaging-and-public-api.md`

---

## LTM Consolidation Record

The following sections have been consolidated into long-term memory documents under `.agents/docs/LTM/`:

| Section | LTM Document |
|---------|--------------|
| `2026-03-29: Initial Implementation` | `transpilation-pipeline-and-dialect-architecture.md`, `duckdb-compatibility-and-feature-coverage.md` |
| `2026-03-29: User-driven expansion and sqlparser 0.61 upgrade` | `transpilation-pipeline-and-dialect-architecture.md`, `duckdb-compatibility-and-feature-coverage.md` |
| `2026-03-29: Library packaging and public API cleanup` | `library-packaging-and-public-api.md` |

See `.agents/docs/LTM/INDEX.md` for the full index.

---

## Deep Sleep Consolidation Record

Created the following synthesis document under `.agents/docs/LTM/`:

| Synthesis Document | Source LTM Documents |
|--------------------|----------------------|
| `sql-transpilation-system-synthesis.md` | `transpilation-pipeline-and-dialect-architecture.md`, `duckdb-compatibility-and-feature-coverage.md` |

Left the following source LTM document standalone because it is already cohesive and only lightly overlaps with the rewrite and compatibility material:

- `library-packaging-and-public-api.md`

## 2026-03-29: TODO resolution pass

Compressed after LTM consolidation.

Retained note: this pass cleared the original TODO backlog by implementing Redshift date arithmetic rewrites, format-string conversion, SerDe fallback handling, and COPY lowering, while also closing nested Trino `ROW` and Redshift system-view support as parser or semantic boundary issues.

See:
- `transpilation-pipeline-and-dialect-architecture.md`
- `duckdb-compatibility-and-feature-coverage.md`
- `sql-transpilation-system-synthesis.md`

---

## 2026-03-29: Trino WITH-clause S3 external table rewriting

Compressed after LTM consolidation.

Retained note: this entry added the Trino `WITH ( external_location = ... )` external-table rewrite path under the existing `ExternalTableBehavior` gate, with reader selection based on the `format` option and explicit Iceberg precedence.

See:
- `transpilation-pipeline-and-dialect-architecture.md`
- `duckdb-compatibility-and-feature-coverage.md`
- `sql-transpilation-system-synthesis.md`

## 2026-03-29: Additional function mappings

Compressed after LTM consolidation.

Retained note: this coverage audit added targeted Trino and Redshift function mappings where DuckDB compatibility still needed rename or interval-expression rewrites.

See:
- `duckdb-compatibility-and-feature-coverage.md`
- `sql-transpilation-system-synthesis.md`

## 2026-03-29: Emulated function mappings

Compressed after LTM consolidation.

Retained note: this pass filled remaining compatibility gaps with function emulation for `STRTOL`, `map_agg`, and `RATIO_TO_REPORT`, plus a few additional Trino time and timezone mappings.

See:
- `duckdb-compatibility-and-feature-coverage.md`
- `sql-transpilation-system-synthesis.md`

---

## LTM Consolidation Record ( 2026-03-29 second pass )

The following sections have been consolidated into long-term memory documents under `.agents/docs/LTM/`:

| Section | LTM Document |
|---------|--------------|
| `2026-03-29: TODO resolution pass` | `transpilation-pipeline-and-dialect-architecture.md`, `duckdb-compatibility-and-feature-coverage.md`, `sql-transpilation-system-synthesis.md` |
| `2026-03-29: Trino WITH-clause S3 external table rewriting` | `transpilation-pipeline-and-dialect-architecture.md`, `duckdb-compatibility-and-feature-coverage.md`, `sql-transpilation-system-synthesis.md` |
| `2026-03-29: Additional function mappings` | `duckdb-compatibility-and-feature-coverage.md`, `sql-transpilation-system-synthesis.md` |
| `2026-03-29: Emulated function mappings` | `duckdb-compatibility-and-feature-coverage.md`, `sql-transpilation-system-synthesis.md` |

No new open items were appended to `.agents/docs/TODO.md` during this consolidation pass because the journal entries in this batch either resolved previously tracked items or documented final decisions.

See `.agents/docs/LTM/INDEX.md` for the current index.

## 2026-03-29: Unquoted keyword argument quoting

Compressed after LTM consolidation.

Retained note: this entry captured the Redshift first-argument quoting fix for `date_part` and `date_trunc`, plus the end-of-session summary after the later compatibility and coverage passes.

See:
- `transpilation-pipeline-and-dialect-architecture.md`
- `duckdb-compatibility-and-feature-coverage.md`
- `sql-transpilation-system-synthesis.md`

---

## LTM Consolidation Record ( 2026-03-29 third pass )

The following section has been consolidated into long-term memory documents under `.agents/docs/LTM/`:

| Section | LTM Document |
|---------|--------------|
| `2026-03-29: Unquoted keyword argument quoting` | `transpilation-pipeline-and-dialect-architecture.md`, `duckdb-compatibility-and-feature-coverage.md`, `sql-transpilation-system-synthesis.md` |

This note is appended at the current end of the journal and supersedes the earlier second-pass consolidation marker as the latest consolidation point.

No new open items were appended to `.agents/docs/TODO.md` during this consolidation pass.

See `.agents/docs/LTM/INDEX.md` for the current index.

---

## 2026-03-29: Configurable SerDe class resolver via TranspileOptions

### Motivation

The built-in `reader_from_serde_class` covered common Hive SerDe classes by substring
matching but returned an `Unsupported` error for any unknown class. Users with custom
or private SerDe classes ( e.g. `com.example.CustomSerDe` ) had no escape hatch
without patching the library.

### Changes

**`src/lib.rs`**

- Added `SerdeClassResolver` — a newtype wrapping
  `Arc<dyn Fn(&str) -> Option<String> + Send + Sync + 'static>`.
  - `Clone` via `Arc` ref-count bump.
  - Manual `Debug` impl emits `SerdeClassResolver(<fn>)`.
  - `#[allow(clippy::type_complexity)]` on the field to suppress the clippy lint.
- Added `serde_class_resolver: Option<SerdeClassResolver>` to `TranspileOptions`
  ( defaults to `None` because `Option<T>: Default` ).
- Exported `SerdeClassResolver` as a public API item.

**`src/transforms/ddl.rs`**

- Threaded `opts: &TranspileOptions` into `external_table_to_view` ( previously it
  did not receive `opts` ), and from there into `determine_reader_function` and
  `reader_from_serde_class`.
- `determine_reader_function` and `reader_from_serde_class` return type changed from
  `Result<&'static str>` to `Result<String>` to accommodate heap-allocated strings
  from the user resolver.
- Split built-in substring inference into a separate
  `infer_reader_from_serde_class(class: &str) -> Result<String>` function.
- `reader_from_serde_class` now calls `resolver.resolve(class)` first; falls through
  to `infer_reader_from_serde_class` when it returns `None`.

**`examples/migration.rs`**

- Added `..Default::default()` to the `TranspileOptions` struct literal to cover the
  new `serde_class_resolver` field without breaking the example.

### Resolver semantics

The resolver is called with the full class name exactly as written in the SQL
`ROW FORMAT SERDE '...'` clause. Returning `Some(reader_fn)` short-circuits the
built-in mapping entirely; returning `None` falls through. This makes it easy to
add new mappings, override built-in ones, or implement any matching strategy
( prefix, regex, external config, etc. ).

The resolver fires for both `SourceDialect::Hive` and `SourceDialect::Trino`
whenever the SQL contains `CREATE EXTERNAL TABLE ... ROW FORMAT SERDE`.

### Tests added

- `ddl.rs` unit: `hive_external_table_serde_custom_resolver` ( resolver matches
  exact class name ), `hive_external_table_serde_custom_resolver_fallthrough` ( resolver
  returns `None`, built-in logic returns `Unsupported` ).
- `integration.rs`: `hive_serde_custom_resolver_overrides_unknown` ( Hive dialect ),
  `trino_hive_style_serde_custom_resolver` ( Trino dialect with Hive-style DDL ).
- Doctest in `src/lib.rs` on `SerdeClassResolver`.

All 151 unit + 52 DuckDB integration + 46 integration + 1 doctest pass cleanly with
no warnings.
