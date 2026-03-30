# DataFusion Target Dialect

## Summary

`TargetDialect::DataFusion` was added as a second output dimension alongside the
existing `TargetDialect::DuckDB` default. Callers can now transpile from Trino,
Redshift, or Hive SQL to Apache DataFusion-compatible SQL by setting
`TranspileOptions::target`. DataFusion is Presto/Trino-compatible in naming
conventions, so many Trino functions pass through unchanged, but support is still
selective and must be verified against real DataFusion execution rather than
string-level transpilation alone. The main stable differences from DuckDB are in
array function names, several date/time rewrites, type syntax, and the set of
functions that must return `Error::Unsupported`.

The implementation went through two validation passes on 2026-03-30. The first
fixed four obvious DuckDB-name leaks in DataFusion mappings; the second added a
real `tests/datafusion_integration.rs` suite against DataFusion 52.4.0 and
corrected several mappings that had looked plausible in string-comparison tests
but were not actually supported at execution time.

## Key Facts

- `TargetDialect` defaults to `DuckDB`; all existing callers remain binary-compatible.
- Setting `target: TargetDialect::DataFusion` in `TranspileOptions` is the only API change needed.
- DataFusion support is validated against real execution in DataFusion 52.4.0, not just SQL string output.
- DataFusion is Presto-compatible, so many Trino function names pass through without renaming, but several apparently compatible names are still unsupported.
- DuckDB uses `list_*` prefix for array operations; DataFusion uses `array_*`.
- Type syntax: DuckDB uses `T[]` and `STRUCT(...)`, DataFusion uses `ARRAY<T>` and `STRUCT<...>`.
- `VARBINARY` maps to `BLOB` for DuckDB and `BYTEA` for DataFusion.
- Redshift `SUPER` maps to `JSON` for DuckDB and `VARCHAR` for DataFusion.
- `SHOW CREATE TABLE/VIEW` is unsupported for DataFusion ( no system catalog ).
- `date_diff` and Redshift `datediff` require custom DataFusion rewrites because DataFusion 52 has no native `date_diff`.
- Several functions now intentionally return `Error::Unsupported` on the DataFusion path, including `to_utf8`, `from_utf8`, `json_object_keys`, `json_extract_scalar`, `json_extract`, `json_array_get`, `arbitrary`, `filter`, `is_finite`, `is_infinite`, `map_agg`, Redshift `json_extract_path_text`, and Redshift `strtol`.

## Details

### Public API additions

**`src/dialect/mod.rs`**:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TargetDialect {
    #[default]
    DuckDB,
    DataFusion,
}
```

**`src/lib.rs`**:

```rust
pub struct TranspileOptions {
    pub target: TargetDialect,   // new; defaults to DuckDB
    pub external_table: ExternalTableBehavior,
    pub iceberg_table: IcebergTableBehavior,
    pub copy: CopyBehavior,
    pub serde_class_resolver: Option<SerdeClassResolver>,
}
```

`TargetDialect` is re-exported at the crate root alongside `SourceDialect`.

### Pipeline threading

The target flows from `TranspileOptions::target` into:

- `ExprRewriter::new(source, target)` — new `target` field
- `functions::function_mappings(source, target)` — dispatches to correct mapping table
- `types::rewrite_data_type(dt, source, target)` — dispatches to correct type rewrite
- All three dialect structs read `opts.target` and pass it to `ExprRewriter::new`
- `ddl::rewrite_ddl` internal helpers ( `iceberg_table_to_view`,
  `external_table_to_view`, `trino_s3_table_to_view`, `rewrite_alter_operation` )
  each gained a `target: TargetDialect` parameter forwarded from `opts`
- `show::rewrite_show` dispatches on `opts.target` to `rewrite_show_duckdb` or
  `rewrite_show_datafusion`

### Function mapping: Trino → DataFusion

DataFusion is Presto-compatible; most Trino functions either pass through unchanged
or need only minor renaming, but the final mapping set is based on execution-backed
verification against DataFusion 52.4.0. Implemented in `trino_to_datafusion_mappings()`.

| Trino | DuckDB | DataFusion | Note |
|---|---|---|---|
| `approx_distinct` | `approx_count_distinct` | passthrough | Presto-compatible |
| `cardinality` | `len` | passthrough | DataFusion has `cardinality` |
| `array_join` | `array_to_string` | passthrough | DataFusion has `array_join` |
| `regexp_like` | `regexp_matches` | passthrough | DataFusion has `regexp_like` |
| `array_distinct/sort/max/min/position` | `list_*` | passthrough | `array_*` is native |
| `array_intersect/concat/except` | `list_*` | passthrough | `array_*` is native |
| `arrays_overlap` | `len(list_intersect(...)) > 0` | passthrough | DataFusion has `arrays_overlap` |
| `array_has` | `list_contains` | passthrough | DataFusion has `array_has` |
| `flatten` | `flatten` | passthrough | |
| `map_keys`, `map_values` | passthrough | passthrough | |
| `strpos`, `length`, `reverse`, `lpad`, `rpad`, `chr` | passthrough | passthrough | |
| `date_add` | passthrough | passthrough | |
| `transform` | `list_transform` | `array_transform` | |
| `element_at` | `list_extract` | `array_element` | |
| `filter` | `list_filter` | `Error::Unsupported` | `array_filter` is not available in DataFusion 52 |
| `contains` | `list_contains` | `array_has` | |
| `slice` | `list_slice` | `array_slice` | |
| `array_union` | `list_distinct(list_concat(...))` | `array_union` ( rename ) | DataFusion has native |
| `array_has_all` | `len(list_intersect...) = len(...)` | `array_length(...)` variant | Custom handler |
| `array_has_any` | `len(list_intersect...) > 0` | `array_length(...)` variant | Custom handler |
| `split` | `str_split` | `string_to_array` | |
| `regexp_extract` | `regexp_extract` | `regexp_match` | |
| `approx_percentile` | `approx_quantile` | `approx_percentile_cont` | |
| `codepoint` | `unicode` | `ascii` | |
| `typeof` | `typeof` | `arrow_typeof` | DataFusion-specific |
| `date_parse` / `parse_datetime` | `strptime(x, fmt)` | `to_timestamp(x, fmt)` | Custom handler |
| `format_datetime` / `date_format` | `strftime(x, fmt)` | `to_char(x, fmt)` | Custom handler |
| `date_diff` | passthrough | Custom | Epoch arithmetic for fixed-size units; `date_part` arithmetic for month / quarter / year |
| `week_of_year` | `weekofyear` | `date_part('week', x)` | Custom handler |
| `year_of_week` | Custom | `date_part('year', date_trunc('week', x))` | Custom handler |
| `day_of_week` | passthrough | `date_part('dow', x)` | Custom handler |
| `day_of_year` | passthrough | `date_part('doy', x)` | Custom handler |
| `url_extract_path/protocol/query/fragment/port` | `regexp_extract` variants | `regexp_match` variants | Dedicated `*_datafusion` handlers |
| `url_extract_host` | `regexp_extract(url, '://([^/:]+)', 1)` | `regexp_match(url, '://([^/:]+)')` | Fixed 2026-03-30 |
| `to_utf8` | `encode` | `Error::Unsupported` | No DataFusion equivalent |
| `from_utf8` | `decode` | `Error::Unsupported` | No DataFusion equivalent |
| `map_agg` | `map(list(k), list(v))` | `Error::Unsupported` | Initial assumption of native support was incorrect for DataFusion 52 |
| `json_object_keys` | `json_keys` | `Error::Unsupported` | No DataFusion equivalent |
| `json_parse`, `json_extract_scalar`, `json_extract`, `json_array_get` | DuckDB JSON helpers | `Error::Unsupported` | `json_extract_scalar` is not available in DataFusion 52 |
| `arbitrary` | `any_value` | `Error::Unsupported` | DataFusion 52 has no `any_value` equivalent |
| `is_finite`, `is_infinite` | `isfinite`, `isinf` | `Error::Unsupported` | DataFusion 52 has no equivalent scalar functions |

### Function mapping: Redshift → DataFusion

Implemented in `redshift_to_datafusion_mappings()`. Main differences from DuckDB path:

| Redshift | DuckDB | DataFusion |
|---|---|---|
| `getdate` / `sysdate` | `current_timestamp` | `now` |
| `regexp_substr` | `regexp_extract` | `regexp_match` |
| `array_concat` | `list_concat` | `array_concat` ( passthrough ) |
| `to_char` | `strftime` + format conversion | passthrough ( no format conversion needed ) |
| `to_date` | `strptime` + CAST + format conversion | passthrough |
| `to_timestamp` | `strptime` + format conversion | passthrough |
| `json_typeof` | `json_type` | passthrough |
| `datediff` | DuckDB custom rewrite | DataFusion custom rewrite | Delegates to `trino_date_diff_datafusion` |
| `months_between` | DuckDB custom rewrite | DataFusion custom rewrite | Implemented with `date_part` arithmetic |
| `json_extract_path_text` | DuckDB JSON helper | `Error::Unsupported` | No equivalent in DataFusion 52 |
| `strtol` | DuckDB emulation | `Error::Unsupported` | Required hex-string casting is unavailable |
| `nvl`, `nvl2`, `decode`, `len`, `charindex`, etc. | same as DuckDB | same as DuckDB |

Note on format strings: Redshift uses PostgreSQL-style format tokens; DuckDB uses
strftime. DataFusion also accepts PostgreSQL-style format tokens, so no conversion
is needed on the DataFusion path.

### Type mapping: source → DataFusion

| Source type | DuckDB | DataFusion |
|---|---|---|
| `VARBINARY` ( Trino / Redshift ) | `BLOB` | `BYTEA` |
| `ROW(a T, b T)` ( Trino ) | `STRUCT(a T, b T)` Parentheses | `STRUCT<a T, b T>` AngleBrackets |
| `ARRAY(T)` ( Trino ) | `T[]` SquareBracket | `ARRAY<T>` AngleBracket |
| `ARRAY<T>` ( Trino ) | `T[]` SquareBracket | passthrough AngleBracket |
| `T[]` ( Trino ) | passthrough | `ARRAY<T>` AngleBracket |
| Redshift `SUPER` | `JSON` | `VARCHAR` |
| Redshift `HLLSKETCH` | `Unsupported` error | `Unsupported` error |
| Redshift `GEOMETRY` | `Unsupported` error | `Unsupported` error |
| `IPADDRESS` ( Trino ) | `VARCHAR` | `VARCHAR` |

Hive type rewrites share the Trino path.

### SHOW handling

`show::rewrite_show` dispatches on `opts.target`:

- **DuckDB path** ( `rewrite_show_duckdb` ): unchanged from before.
  `SHOW TABLES/DATABASES/SCHEMAS/COLUMNS/VIEWS` pass through.
  `SHOW CREATE TABLE` and `SHOW CREATE VIEW` are emulated via DuckDB catalog queries.
  `SHOW VARIABLE` becomes `SELECT current_setting(...)`.
  `SHOW FUNCTIONS` becomes a query on `information_schema.routines`.

- **DataFusion path** ( `rewrite_show_datafusion` ):
  `SHOW TABLES/COLUMNS/DATABASES/SCHEMAS/FUNCTIONS` pass through natively.
  `SHOW CREATE TABLE/VIEW` returns `Error::Unsupported` ( no system catalog equivalent ).
  `SHOW VARIABLE` passes through ( DataFusion supports `SHOW <variable>` ).

### Execution-backed audit results

`tests/datafusion_integration.rs` executes transpiled SQL against a real in-process
DataFusion 52.4.0 context. That audit corrected several earlier assumptions that
had survived string-comparison tests.

Functions confirmed to exist in DataFusion 52 include:

- `array_max`, `array_min`, `array_sort`, `array_distinct`
- `arrays_overlap`, `array_has`, `cardinality`
- `regexp_like`, `string_to_array`, `levenshtein`
- `isnan`, `approx_distinct`, `date_part`, `date_trunc`, `to_unixtime`
- `to_timestamp`, `chr`, `ascii`, `map_keys`, `map_values`
- `arrow_typeof`, `from_hex`, `random`

Functions confirmed missing in DataFusion 52 and therefore mapped to `Error::Unsupported`
on the DataFusion path include:

- `any_value`
- `array_filter`
- `isfinite`, `isinf`
- `json_extract_scalar`
- `map_agg`
- the JSON typed-function path used by `json_parse` and `json_format`

### `date_diff` implementation

DataFusion 52 has no native `date_diff` function. `trino_date_diff_datafusion`
dispatches on the literal unit argument:

- `second`, `minute`, `hour`, `day`, `week`: epoch arithmetic via `to_unixtime`
- `month`, `quarter`, `year`: exact calendar arithmetic via `date_part`

Redshift `datediff` delegates to the same implementation, and Redshift
`months_between` uses a related `date_part`-based calculation.

## Files

- `src/dialect/mod.rs`: `TargetDialect` enum
- `src/lib.rs`: `TranspileOptions::target`, re-exports
- `src/transpiler/rewrite.rs`: `ExprRewriter::new(source, target)`
- `src/dialect/trino.rs`, `redshift.rs`, `hive.rs`: pass `opts.target` to `ExprRewriter`
- `src/transforms/functions.rs`: `function_mappings(source, target)`,
  `trino_to_datafusion_mappings()`, `redshift_to_datafusion_mappings()`,
  DataFusion-specific custom handlers
- `src/transforms/types.rs`: `rewrite_data_type(dt, source, target)`,
  `rewrite_trino_type_duckdb`, `rewrite_trino_type_datafusion`,
  `rewrite_redshift_type_duckdb`, `rewrite_redshift_type_datafusion`
- `src/transforms/ddl.rs`: `target: TargetDialect` parameter threaded to
  `iceberg_table_to_view`, `external_table_to_view`, `trino_s3_table_to_view`,
  `rewrite_alter_operation`
- `src/transforms/show.rs`: `rewrite_show_duckdb`, `rewrite_show_datafusion`
- `tests/integration.rs`: string-level regression coverage for target-specific rewrites
- `tests/datafusion_integration.rs`: execution-backed DataFusion 52.4.0 coverage

## Test Coverage

- `tests/integration.rs` keeps string-level checks for target selection, rewrite output,
  backward compatibility of the default DuckDB target, and error cases such as
  `SHOW CREATE TABLE` on the DataFusion path.
- `tests/datafusion_integration.rs` executes transpiled SQL against a real
  DataFusion 52.4.0 `SessionContext` and covers:
  - basic query execution helpers
  - real function-availability checks
  - custom `date_diff` semantics
  - `SHOW TABLES` with `information_schema`
  - unsupported-function behavior that must fail explicitly rather than emit bad SQL

Run with:

- `cargo test --lib --test integration`
- `cargo test --test datafusion_integration`

Recorded counts after the audit:

- 154 unit tests
- 52 DuckDB integration tests
- 68 integration tests
- 51 DataFusion integration tests

One pre-existing DuckDB failure remains outside the DataFusion target work:
`trino_unnest_with_ordinality`, because DuckDB does not yet implement `WITH ORDINALITY`.

## Pitfalls

- Passing string-comparison tests is not sufficient for target-dialect work. Several
  mappings that looked correct at the transpilation layer were only disproved by
  executing the generated SQL against a real DataFusion context.
- Initial review fixed four obvious DuckDB-name leaks ( `url_extract_host`,
  `to_utf8`, `from_utf8`, `json_object_keys` ), but the later execution-backed audit
  also corrected broader support assumptions such as `map_agg`, `filter`,
  `json_extract_scalar`, and `arbitrary`.
- When custom arithmetic is built from `BinaryOp` expressions and then multiplied or
  divided, wrap intermediate subtraction expressions in `Expr::Nested`. Without
  explicit nesting, serialized SQL can lose required parentheses and change meaning.

### External table / Iceberg behavior

`ExternalTableBehavior::MapToView` and `IcebergTableBehavior::MapToView` still
generate views using DuckDB-specific reader functions ( `read_parquet`,
`iceberg_scan`, etc. ). DataFusion does not support these functions. DataFusion
users should keep external table and Iceberg behavior at the default `Error`
setting. Native DataFusion external table normalization is out of scope for the
current implementation.

### Format string conversion

For Redshift → DataFusion, `to_char`, `to_date`, and `to_timestamp` pass through
without format string conversion because both Redshift and DataFusion use PostgreSQL-style
format tokens. The `convert_format_arg_redshift` helper is intentionally skipped on
the DataFusion path.
