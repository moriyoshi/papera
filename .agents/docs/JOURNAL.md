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

---

## 2026-03-30: DataFusion target dialect

### What was built

Added `TargetDialect` as a second dimension to the transpilation pipeline so the
three existing source dialects ( Trino, Redshift, Hive ) can now emit either DuckDB
or Apache DataFusion SQL.

**Public API additions** (`src/lib.rs`, `src/dialect/mod.rs`):

- `TargetDialect` enum — `DuckDB` ( `#[default]` ) and `DataFusion`. Exported from
  the crate root alongside `SourceDialect`.
- `TranspileOptions::target: TargetDialect` — defaults to `DuckDB`, so all existing
  call sites compile unchanged.

**Pipeline changes** (mechanical, no logic change for DuckDB path):

- `ExprRewriter::new(source, target)` carries the target.
- `function_mappings(source, target)` and `rewrite_data_type(dt, source, target)`
  take both dimensions.
- All three dialect structs (`TrinoDialect`, `RedshiftDialect`, `HiveDialect`) read
  `opts.target` and thread it through.
- `ddl::rewrite_ddl`, internal helpers `iceberg_table_to_view`,
  `external_table_to_view`, `trino_s3_table_to_view`, and `rewrite_alter_operation`
  each gained a `target: TargetDialect` parameter forwarded from `opts`.
- `show::rewrite_show` dispatches on `opts.target` and calls a new
  `rewrite_show_datafusion` branch.

**DataFusion function mappings** (`src/transforms/functions.rs`):

`trino_to_datafusion_mappings()` — DataFusion is Presto/Trino-compatible, so most
functions require only minor renames or pass through unchanged. Key differences from
the DuckDB path:

| Trino function | DuckDB | DataFusion |
|---|---|---|
| `approx_distinct` | `approx_count_distinct` | passthrough |
| `cardinality` | `len` | passthrough |
| `array_join` | `array_to_string` | passthrough |
| `regexp_like` | `regexp_matches` | passthrough |
| `transform` | `list_transform` | `array_transform` |
| `element_at` | `list_extract` | `array_element` |
| `filter` | `list_filter` | `array_filter` |
| `contains` | `list_contains` | `array_has` |
| `slice` | `list_slice` | `array_slice` |
| `array_distinct/sort/max/min/position` | `list_*` variants | passthrough |
| `array_intersect/concat/except` | `list_*` variants | passthrough |
| `array_union` | `list_distinct(list_concat(...))` | simple `array_union` rename |
| `arrays_overlap` | `len(list_intersect(...)) > 0` | `array_length(array_intersect(...)) > 0` |
| `array_has_all` | `len(list_intersect...) = len(...)` | `array_length(...)` variant |
| `array_has_any` | `len(list_intersect...) > 0` | `array_length(...)` variant |
| `split` | `str_split` | `string_to_array` |
| `regexp_extract` | `regexp_extract` | `regexp_match` |
| `approx_percentile` | `approx_quantile` | `approx_percentile_cont` |
| `codepoint` | `unicode` | `ascii` |
| `typeof` | `typeof` | `arrow_typeof` |
| `date_parse` / `parse_datetime` | `strptime(x, fmt)` | `to_timestamp(x, fmt)` |
| `format_datetime` / `date_format` | `strftime(x, fmt)` | `to_char(x, fmt)` |
| `week_of_year` | `weekofyear` | `date_part('week', x)` Custom |
| `year_of_week` | DuckDB Custom | `date_part('year', date_trunc('week', x))` |
| `url_extract_path/protocol/query/fragment/port` | `regexp_extract` variants | `regexp_match` variants |

`redshift_to_datafusion_mappings()` — largely parallel to the DuckDB path, with:
- `getdate`/`sysdate` → `now` ( vs `current_timestamp` for DuckDB )
- `regexp_substr` → `regexp_match` ( vs `regexp_extract` )
- `array_concat` → passthrough `array_concat` ( vs `list_concat` )
- `to_char`, `to_date`, `to_timestamp` → passthrough with no format conversion
  ( DataFusion uses PostgreSQL-style format strings natively )
- `json_typeof` → passthrough ( vs `json_type` for DuckDB )

**DataFusion type mappings** (`src/transforms/types.rs`):

| Source type | DuckDB | DataFusion |
|---|---|---|
| `VARBINARY` ( Trino / Redshift ) | `BLOB` | `BYTEA` |
| `ROW(a T, b T)` | `STRUCT(...)` Parentheses | `STRUCT<...>` AngleBrackets |
| `ARRAY(T)` / `ARRAY<T>` / `T[]` | `T[]` SquareBracket | `ARRAY<T>` AngleBracket |
| Redshift `SUPER` | `JSON` | `VARCHAR` |
| `IPADDRESS` | `VARCHAR` | `VARCHAR` |

**SHOW handling** (`src/transforms/show.rs`):

For DataFusion target, `SHOW TABLES/COLUMNS/DATABASES/SCHEMAS/FUNCTIONS` pass
through natively. `SHOW CREATE TABLE/VIEW` returns `Error::Unsupported` because
DataFusion has no system catalog equivalent. `SHOW VARIABLE` passes through
( DataFusion supports `SHOW <variable>` natively ).

**Tests**: 17 new integration tests in `tests/integration.rs` covering the
DataFusion mapping behaviours, plus regression coverage that default DuckDB output
is unchanged. Total test count: 154 unit + 52 DuckDB integration + 63 integration.

---

### Bugs found during review

Four places in the DataFusion mappings reuse DuckDB-specific custom handlers,
causing DuckDB function names to appear in DataFusion output:

**Bug 1 — `url_extract_host` emits `regexp_extract` for DataFusion**

`trino_to_datafusion_mappings` maps `url_extract_host` to
`Custom(trino_url_extract_host)`. That handler hardcodes `make_function("regexp_extract", ...)`,
which is the DuckDB name. DataFusion uses `regexp_match` for regex extraction. All
other URL-extract functions have dedicated `*_datafusion` variants that correctly
use `regexp_match`; `url_extract_host` was accidentally left pointing at the shared
DuckDB handler.

**Bug 2 — `to_utf8` / `from_utf8` emit `encode` / `decode` for DataFusion**

Both share `trino_to_utf8` / `trino_from_utf8`, which rename to `encode` and
`decode` — DuckDB-specific. DataFusion has no direct equivalents; these should
either be approximated differently or return `Error::Unsupported` for the DataFusion
target.

**Bug 3 — `map_agg` emits `map(list(key), list(value))` for DataFusion**

`trino_to_datafusion_mappings` maps `map_agg` to `Custom(trino_map_agg)`. That
handler generates `map(list(key), list(value))` where `list()` is a DuckDB
aggregate. DataFusion has no `list()` aggregate; the correct DataFusion equivalent
would be `MAP(ARRAY_AGG(key), ARRAY_AGG(value))` if it exists, or
`Error::Unsupported`.

**Bug 4 — `json_object_keys` maps to `json_keys` for DataFusion**

`trino_to_datafusion_mappings` contains `("json_object_keys", FunctionMapping::Rename("json_keys"))`.
`json_keys` is a DuckDB function. DataFusion does not expose `json_keys`; this
mapping should either use the correct DataFusion name or return
`Error::Unsupported`.

All four bugs produce silently wrong output rather than errors — the generated SQL
will fail at DataFusion execution time. None are caught by the current integration
tests because those tests only check function renaming at the transpilation
( string-comparison ) level, not execution.

---

## LTM Consolidation Record ( 2026-03-30 )

The following sections have been consolidated into long-term memory documents under `.agents/docs/LTM/`:

| Section | LTM Document |
|---------|-------------|
| `2026-03-29: Configurable SerDe class resolver via TranspileOptions` | `serde-class-resolver.md` |
| `2026-03-30: DataFusion target dialect` | `datafusion-target-dialect.md` |

See `.agents/docs/LTM/INDEX.md` for the full index.

## Deep Sleep Consolidation Record ( 2026-03-30 )

Created `library-api-and-target-dialect-synthesis.md` under `.agents/docs/LTM/` to consolidate these source LTM documents:

- `library-packaging-and-public-api.md`
- `serde-class-resolver.md`
- `datafusion-target-dialect.md`

Left these documents standalone:

- `sql-transpilation-system-synthesis.md` remains the existing subsystem synthesis for rewrite architecture and DuckDB compatibility behavior.
- All source LTM documents remain in place for traceability and detailed lookup.

---

## 2026-03-30: DataFusion bug fixes ( four open TODO items )

Fixed all four bugs identified during the DataFusion target dialect implementation review where DuckDB-specific function names leaked into DataFusion output.

### Bug 1 — `url_extract_host` now uses `regexp_match`

Added `trino_url_extract_host_datafusion` in `src/transforms/functions.rs` that emits
`regexp_match(url, '://([^/:]+)')`. The DataFusion mapping entry in
`trino_to_datafusion_mappings` now points to this handler instead of the shared
`trino_url_extract_host` which hardcoded `regexp_extract` ( DuckDB name ).

### Bug 2 — `to_utf8` / `from_utf8` return `Unsupported` for DataFusion

Added `trino_to_utf8_datafusion` and `trino_from_utf8_datafusion` in `src/transforms/functions.rs`
that return `Error::Unsupported`. DataFusion has no direct equivalents for these functions.
The DataFusion mapping entries now use these handlers instead of the DuckDB-specific ones
that renamed to `encode` / `decode`.

### Bug 3 — `map_agg` passes through for DataFusion

Added `trino_map_agg_datafusion` that renames to `map_agg` ( passthrough ). DataFusion
supports `MAP_AGG` as a native aggregate function, so the correct fix is a simple rename
( preserving the original function name ) rather than the DuckDB workaround of
`map(list(key), list(value))`.

### Bug 4 — `json_object_keys` returns `Unsupported` for DataFusion

Added `trino_json_object_keys_datafusion` that returns `Error::Unsupported`. DataFusion
has no equivalent function. Previously the mapping erroneously used `Rename("json_keys")`
which is a DuckDB-only function.

### Tests

Added 5 new integration tests in `tests/integration.rs`:

- `datafusion_url_extract_host_uses_regexp_match` — verifies `regexp_match` is emitted and `regexp_extract` is not
- `datafusion_to_utf8_unsupported` — verifies `Error::Unsupported`
- `datafusion_from_utf8_unsupported` — verifies `Error::Unsupported`
- `datafusion_map_agg_native` — verifies `map_agg` passthrough and absence of `list()`
- `datafusion_json_object_keys_unsupported` — verifies `Error::Unsupported`

All tests pass: 154 unit + 52 DuckDB integration + 68 integration.

---

## 2026-03-30: DataFusion integration test suite and function mapping audit

### Goal

Complete the DataFusion target by:
1. Creating `tests/datafusion_integration.rs` — tests that transpile SQL and execute it against a real in-process DataFusion 52.4.0 instance.
2. Auditing and correcting DataFusion function mappings by observing which functions DataFusion 52 actually supports.

### DataFusion integration test infrastructure

Added `tests/datafusion_integration.rs` with three helpers:

- `new_ctx() -> SessionContext` — plain context; one per test.
- `async fn exec(ctx, sql, dialect)` — transpile and execute, discarding results (panics on error).
- `async fn query(ctx, sql, dialect) -> Vec<RecordBatch>` — transpile and collect results.
- `fn display(batches) -> String` — `pretty_format_batches` for type-agnostic value assertions.

Tests assert with `display(&batches).contains("expected")` to avoid Arrow downcast complexity.
`SHOW TABLES` tests require `SessionConfig::new().with_information_schema(true)`.

### DataFusion 52 function availability audit

Running the integration tests revealed which functions are actually available in DataFusion 52.
Key findings ( functions that do NOT exist in DataFusion 52 ) :

| Function | Status |
|---|---|
| `any_value` | Missing — `arbitrary` now returns `Unsupported` |
| `array_filter` | Missing — `filter` now returns `Unsupported` |
| `isfinite` / `isinf` | Missing — `is_finite` / `is_infinite` now return `Unsupported` |
| `json_extract_scalar` | Missing — returns `Unsupported`; cascades to `json_array_get`, `json_extract` |
| `map_agg` | Missing — now returns `Unsupported` (previous session wrongly marked as supported) |
| `datediff` ( DataFusion name ) | Missing — implemented via epoch / `date_part` arithmetic instead |
| `dayofweek` / `dayofyear` | Missing — mapped to `date_part('dow', ...)` / `date_part('doy', ...)` |
| JSON type | Missing — `json_parse`, `json_format` now return `Unsupported` for DataFusion target |

Functions confirmed to exist in DataFusion 52: `array_max`, `array_min`, `array_sort`, `array_distinct`, `arrays_overlap`, `array_has`, `cardinality`, `regexp_like`, `string_to_array`, `levenshtein`, `isnan`, `approx_distinct`, `date_part`, `to_unixtime`, `date_trunc`, `to_timestamp`, `chr`, `ascii`, `map_keys`, `map_values`, `arrow_typeof`, `from_hex`, `random`.

### Mapping corrections in `src/transforms/functions.rs`

The following `trino_to_datafusion_mappings` entries were corrected:

- `is_finite` → `Custom(trino_is_finite_datafusion)` ( Unsupported )
- `is_infinite` → `Custom(trino_is_infinite_datafusion)` ( Unsupported )
- `map_agg` → `Custom(trino_map_agg_datafusion)` ( Unsupported )
- `json_parse` → `Custom(trino_json_parse_datafusion)` ( Unsupported )
- `json_array_get` → `Custom(trino_json_array_get_datafusion)` ( Unsupported, since it relied on `json_extract_scalar` )
- `json_extract_scalar` → `Custom(trino_json_extract_scalar_datafusion)` ( Unsupported )
- `json_extract` → `Custom(trino_json_extract_datafusion)` ( Unsupported )
- `arbitrary` → `Custom(trino_arbitrary_datafusion)` ( Unsupported )
- `filter` → `Custom(trino_filter_datafusion)` ( Unsupported )
- `date_diff` → `Custom(trino_date_diff_datafusion)` ( new epoch / date_part implementation )
- `day_of_week` → `Custom(trino_day_of_week_datafusion)` — `date_part('dow', x)`
- `day_of_year` → `Custom(trino_day_of_year_datafusion)` — `date_part('doy', x)`

The following `redshift_to_datafusion_mappings` entries were corrected:

- `datediff` → `Custom(redshift_datediff_datafusion)` ( delegates to `trino_date_diff_datafusion` )
- `json_extract_path_text` → `Custom(redshift_json_extract_path_text_datafusion)` ( Unsupported )
- `months_between` → `Custom(redshift_months_between_datafusion)` ( new `date_part` implementation )
- `strtol` → `Custom(redshift_strtol_datafusion)` ( Unsupported — hex string casting unavailable )

### `date_diff` implementation for DataFusion

DataFusion 52 has no `date_diff` function. Implemented `trino_date_diff_datafusion` as a Custom handler dispatching on the literal unit argument:

**Second / minute / hour / day / week** — epoch arithmetic:
```sql
CAST((to_unixtime(CAST(d2 AS TIMESTAMP)) - to_unixtime(CAST(d1 AS TIMESTAMP))) / <seconds_per_unit> AS BIGINT)
```

**Month / quarter / year** — exact calendar arithmetic, matching Trino's complete-unit semantics ( a month is only counted if the day-of-month boundary has been crossed ) :

```sql
-- month:
CAST(
  ((date_part('year',d2) - date_part('year',d1)) * 12
   + (date_part('month',d2) - date_part('month',d1)))
  - CASE WHEN date_part('day',d2) < date_part('day',d1) THEN 1 ELSE 0 END
  AS BIGINT)

-- year:
CAST(
  (date_part('year',d2) - date_part('year',d1))
  - CASE WHEN month(d2)<month(d1) OR (month(d2)=month(d1) AND day(d2)<day(d1))
         THEN 1 ELSE 0 END
  AS BIGINT)
```

Trino semantics verified:
- `date_diff('month', 1970-01-20, 1970-02-19)` = 0 ( day 19 < 20 → adjustment )
- `date_diff('month', 1970-01-20, 1970-02-20)` = 1 ( day 20 >= 20 → no adjustment )

Key implementation note: all intermediate `BinaryOp` expressions involving subtraction that are then multiplied or divided must be wrapped in `Expr::Nested` to force parentheses in the serialized SQL. Without this, operator precedence causes `a - b * c` instead of `(a - b) * c`.

Redshift `datediff` and `months_between` use the same implementation via delegation.

### Test counts

- 154 unit tests ( `cargo test --lib --test integration` ) — all pass
- 51 DataFusion integration tests ( `cargo test --test datafusion_integration` ) — all pass
- 1 pre-existing DuckDB failure ( `trino_unnest_with_ordinality` ) — DuckDB does not yet implement `WITH ORDINALITY`

---

## LTM Consolidation Record ( 2026-03-30 refresh )

The following sections have been consolidated into long-term memory documents under `.agents/docs/LTM/`:

| Section | LTM Document |
|---------|--------------|
| `2026-03-30: DataFusion bug fixes ( four open TODO items )` | `datafusion-target-dialect.md`, `library-api-and-target-dialect-synthesis.md` |
| `2026-03-30: DataFusion integration test suite and function mapping audit` | `datafusion-target-dialect.md`, `library-api-and-target-dialect-synthesis.md` |

Added one new open item to `.agents/docs/TODO.md` for the remaining DuckDB
`WITH ORDINALITY` limitation exposed by the integration audit. The four
DataFusion bug-fix TODO items were not re-added because they are already marked
resolved in `.agents/docs/TODO.md`.

See `.agents/docs/LTM/INDEX.md` for the current index.

## Deep Sleep Consolidation Record ( 2026-03-30 refresh )

Refreshed the following synthesis document under `.agents/docs/LTM/`:

| Synthesis Document | Source LTM Documents |
|--------------------|----------------------|
| `library-api-and-target-dialect-synthesis.md` | `library-packaging-and-public-api.md`, `serde-class-resolver.md`, `datafusion-target-dialect.md` |

No new synthesis document was created in this pass because the current LTM set is
already compact. This refresh updated the existing synthesis to reflect the
execution-backed DataFusion support guidance and the corrected unsupported-function
surface.
