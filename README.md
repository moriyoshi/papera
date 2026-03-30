# Papera

A SQL compatibility layer that transpiles Trino, Redshift, and Hive SQL to target-specific analytical SQL.

papera parses source SQL using [sqlparser-rs](https://github.com/apache/datafusion-sqlparser-rs), applies dialect-specific AST transformations, and emits SQL for the selected target dialect. DuckDB remains the default and most fully supported target. The library API also exposes `TargetDialect::DataFusion` for callers that need DataFusion-compatible output, while the current CLI remains DuckDB-targeted.

## Installation

### As a Library

Add to your `Cargo.toml`:

```toml
[dependencies]
papera = "0.1"
```

### CLI

The CLI binary is feature-gated and not built by default:

```sh
cargo build --features cli
# or install globally
cargo install --path . --features cli
```

## Usage

### CLI

```sh
# Pipe SQL through papera
echo "SELECT NVL(a, b) FROM t" | papera redshift
# Output: SELECT coalesce(a, b) FROM t

echo "SELECT approx_distinct(col) FROM t" | papera trino
# Output: SELECT approx_count_distinct(col) FROM t
```

```
Usage: papera <trino|redshift|hive>
  Reads SQL from stdin and writes DuckDB-compatible SQL to stdout.
```

### Library

```rust
use papera::{
    transpile, transpile_with_options, SourceDialect, TargetDialect,
    TranspileOptions, ExternalTableBehavior, IcebergTableBehavior,
    CopyBehavior,
};

// Simple usage
let sql = "SELECT NVL(a, b) FROM t";
let result = transpile(sql, SourceDialect::Redshift).unwrap();
assert_eq!(result, "SELECT coalesce(a, b) FROM t");

// With options (e.g., convert external tables to views)
let sql = "CREATE EXTERNAL TABLE t (a INT) STORED AS PARQUET LOCATION 's3://bucket/path'";
let opts = TranspileOptions {
    external_table: ExternalTableBehavior::MapToView,
    ..Default::default()
};
let result = transpile_with_options(sql, SourceDialect::Redshift, &opts).unwrap();
// Output: CREATE VIEW t (a) AS SELECT * FROM read_parquet('s3://bucket/path')

// Migration mode: opt into all conversions
let opts = TranspileOptions {
    external_table: ExternalTableBehavior::MapToView,
    iceberg_table: IcebergTableBehavior::MapToView,
    copy: CopyBehavior::MapToInsert,
    ..Default::default()
};

// Select a non-default output target through the library API
let opts = TranspileOptions {
    target: TargetDialect::DataFusion,
    ..Default::default()
};
let result = transpile_with_options("SELECT split(name, ',') FROM t", SourceDialect::Trino, &opts).unwrap();
// Output: SELECT string_to_array(name, ',') FROM t
```

Custom SerDe class mappings for classes not covered by the built-in resolver:

```rust
use papera::{SerdeClassResolver, TranspileOptions, ExternalTableBehavior};

let opts = TranspileOptions {
    external_table: ExternalTableBehavior::MapToView,
    serde_class_resolver: Some(SerdeClassResolver::new(|class| {
        match class {
            c if c.eq_ignore_ascii_case("com.example.MyParquetSerde") => Some("read_parquet".to_string()),
            c if c.eq_ignore_ascii_case("com.example.MyJsonSerde")    => Some("read_json".to_string()),
            _ => None, // fall through to built-in logic
        }
    })),
    ..Default::default()
};
```

### Multi-Statement SQL

Both `transpile` and `transpile_with_options` accept multi-statement SQL. Statements are parsed together and emitted joined with `;\n`:

```rust
let script = "SELECT NVL(a, b) FROM t; SELECT GETDATE()";
let result = transpile(script, SourceDialect::Redshift).unwrap();
// result: "SELECT coalesce(a, b) FROM t;\nSELECT current_timestamp()"
```

See `cargo run --example multi_statement` for a full ETL script example.

### Error Handling

`transpile` and `transpile_with_options` return `papera::Result<String>`, where `papera::Error` has two variants:

```rust
pub enum Error {
    /// The source SQL could not be parsed by sqlparser-rs.
    Parse(sqlparser::parser::ParserError),
    /// The SQL uses a feature that cannot be transpiled for the configured target
    /// (e.g., an unsupported type or a conversion that was not opted into).
    Unsupported(String),
}
```

`Error::Unsupported` is returned for:
- `CREATE EXTERNAL TABLE` when `external_table` is `Error` (the default)
- Iceberg tables when `iceberg_table` is `Error` (the default)
- `COPY FROM` when `copy` is `Error` (the default)
- Types with no DuckDB equivalent ( `HLLSKETCH`, `GEOMETRY` )

See `examples/` for more complete usage patterns (`cargo run --example basic`, `cargo run --example migration`, `cargo run --example serde_resolver`).

## Feature Coverage

Unless noted otherwise, the compatibility tables in this section document the default DuckDB target. DataFusion support exists through `TranspileOptions::target`, but it is narrower and should be treated as a separate compatibility path.

### Supported Dialects

| Dialect | Parser | Notes |
|---------|--------|-------|
| Trino | `GenericDialect` | Also handles Hive-style DDL (STORED AS, TBLPROPERTIES, etc.) |
| Redshift | `RedshiftSqlDialect` | Includes Redshift Spectrum external tables |
| Hive | `HiveDialect` | ROW FORMAT, SERDE, PARTITIONED BY support |

### Function Mappings: Trino to DuckDB

#### Aggregate

| Trino | DuckDB | Notes |
|-------|--------|-------|
| `approx_distinct(x)` | `approx_count_distinct(x)` | |
| `arbitrary(x)` | `any_value(x)` | |
| `approx_percentile(x, p)` | `approx_quantile(x, p)` | |
| `map_agg(k, v)` | `map(list(k), list(v))` | |

#### Date / Time

| Trino | DuckDB | Notes |
|-------|--------|-------|
| `date_parse(s, fmt)` | `strptime(s, fmt)` | Java format strings converted |
| `format_datetime(ts, fmt)` | `strftime(ts, fmt)` | Java format strings converted |
| `date_format(ts, fmt)` | `strftime(ts, fmt)` | Java format strings converted |
| `at_timezone(ts, tz)` | `ts AT TIME ZONE tz` | |
| `with_timezone(ts, tz)` | `ts AT TIME ZONE tz` | |
| `parse_datetime(s, fmt)` | `strptime(s, fmt)` | Java format strings converted |
| `to_unixtime(ts)` | `epoch(ts)` | |
| `current_timezone()` | `current_setting('TimeZone')` | |
| `from_unixtime(t)` | `to_timestamp(t)` | |
| `date_diff(unit, t1, t2)` | `date_diff(unit, t1, t2)` | |
| `date_add(unit, n, ts)` | `date_add(unit, n, ts)` | |
| `day_of_week(d)` | `dayofweek(d)` | |
| `day_of_year(d)` | `dayofyear(d)` | |
| `week_of_year(d)` | `weekofyear(d)` | |
| `year_of_week(d)` | `yearofweek(d)` | |

#### String

| Trino | DuckDB | Notes |
|-------|--------|-------|
| `split(s, del)` | `str_split(s, del)` | |
| `levenshtein_distance(a, b)` | `levenshtein(a, b)` | |
| `regexp_like(s, p)` | `regexp_matches(s, p)` | |
| `regexp_extract(s, p[, g])` | `regexp_extract(s, p[, g])` | |
| `regexp_replace(s, p, r)` | `regexp_replace(s, p, r)` | |
| `strpos(s, sub)` | `strpos(s, sub)` | |
| `length(s)` | `length(s)` | |
| `reverse(s)` | `reverse(s)` | |
| `lpad(s, n, c)` | `lpad(s, n, c)` | |
| `rpad(s, n, c)` | `rpad(s, n, c)` | |
| `chr(n)` | `chr(n)` | |
| `codepoint(c)` | `unicode(c)` | |
| `from_hex(s)` | `unhex(s)` | |
| `to_utf8(s)` | `encode(s)` | Returns BLOB |
| `from_utf8(b)` | `decode(b)` | |
| `url_extract_host(url)` | `regexp_extract(url, ...)` | Approximation via regex |
| `url_extract_path(url)` | `regexp_extract(url, ...)` | Approximation via regex |
| `url_extract_protocol(url)` | `regexp_extract(url, ...)` | Approximation via regex |
| `url_extract_query(url)` | `regexp_extract(url, ...)` | Approximation via regex |
| `url_extract_fragment(url)` | `regexp_extract(url, ...)` | Approximation via regex |
| `url_extract_port(url)` | `regexp_extract(url, ...)` | Approximation via regex |

#### Array / Map

| Trino | DuckDB | Notes |
|-------|--------|-------|
| `transform(arr, fn)` | `list_transform(arr, fn)` | |
| `sequence(start, stop)` | `generate_series(start, stop)` | |
| `element_at(arr, i)` | `list_extract(arr, i)` | |
| `cardinality(x)` | `len(x)` | |
| `array_join(arr, sep)` | `array_to_string(arr, sep)` | |
| `reduce(arr, ...)` | `list_reduce(arr, ...)` | |
| `filter(arr, fn)` | `list_filter(arr, fn)` | |
| `contains(arr, x)` | `list_contains(arr, x)` | |
| `zip(a, b)` | `list_zip(a, b)` | |
| `flatten(arr)` | `flatten(arr)` | |
| `slice(arr, ...)` | `list_slice(arr, ...)` | |
| `array_distinct(arr)` | `list_distinct(arr)` | |
| `array_sort(arr)` | `list_sort(arr)` | |
| `array_max(arr)` | `list_max(arr)` | |
| `array_min(arr)` | `list_min(arr)` | |
| `array_position(arr, x)` | `list_position(arr, x)` | |
| `array_remove(arr, x)` | `list_filter(arr, x)` | Approximate |
| `array_intersect(a, b)` | `list_intersect(a, b)` | |
| `array_concat(a, b)` | `list_concat(a, b)` | |
| `array_except(a, b)` | `list_except(a, b)` | |
| `array_union(a, b)` | `list_distinct(list_concat(a, b))` | |
| `arrays_overlap(a, b)` | `len(list_intersect(a, b)) > 0` | |
| `array_has(arr, x)` | `list_contains(arr, x)` | |
| `array_has_all(arr, req)` | `len(list_intersect(arr, req)) = len(req)` | |
| `array_has_any(arr, cands)` | `len(list_intersect(arr, cands)) > 0` | |
| `array_sum(arr)` | `list_sum(arr)` | |
| `array_average(arr)` | `list_avg(arr)` | |
| `map_keys(m)` | `map_keys(m)` | |
| `map_values(m)` | `map_values(m)` | |
| `map_concat(m1, m2)` | `map_concat(m1, m2)` | |

#### JSON

| Trino | DuckDB | Notes |
|-------|--------|-------|
| `json_extract_scalar(j, p)` | `json_extract_string(j, p)` | |
| `json_extract(j, p)` | `json_extract(j, p)` | |
| `json_parse(s)` | `CAST(s AS JSON)` | |
| `json_format(j)` | `CAST(j AS VARCHAR)` | |
| `json_array_get(j, idx)` | `json_extract_string(j, '$[idx]')` | Literal index only |
| `json_array_length(j)` | `json_array_length(j)` | |
| `json_object_keys(j)` | `json_keys(j)` | |

#### Math / Numeric

| Trino | DuckDB | Notes |
|-------|--------|-------|
| `is_nan(x)` | `isnan(x)` | |
| `is_finite(x)` | `isfinite(x)` | |
| `is_infinite(x)` | `isinf(x)` | |
| `nan()` | `CAST('NaN' AS DOUBLE)` | |
| `infinity()` | `CAST('Infinity' AS DOUBLE)` | |
| `rand()` | `random()` | |
| `typeof(x)` | `typeof(x)` | |

#### Bitwise

| Trino | DuckDB | Notes |
|-------|--------|-------|
| `bitwise_and(a, b)` | `a & b` | |
| `bitwise_or(a, b)` | `a \| b` | |
| `bitwise_xor(a, b)` | `a ^ b` | |
| `bitwise_not(a)` | `~a` | |
| `bitwise_left_shift(a, b)` | `a << b` | |
| `bitwise_right_shift(a, b)` | `a >> b` | |

### Function Mappings: Redshift to DuckDB

#### Date / Time

| Redshift | DuckDB | Notes |
|----------|--------|-------|
| `GETDATE()` | `current_timestamp()` | |
| `SYSDATE` | `current_timestamp` | |
| `DATEADD(part, n, d)` | `d + INTERVAL 'n' part` | Interval arithmetic |
| `DATEDIFF(part, d1, d2)` | `date_diff('part', d1, d2)` | Datepart quoted |
| `DATE_TRUNC(part, d)` | `date_trunc(part, d)` | |
| `CONVERT_TIMEZONE(tz, ts)` | `ts AT TIME ZONE 'tz'` | 2-arg form |
| `CONVERT_TIMEZONE(src, dst, ts)` | `ts AT TIME ZONE 'src' AT TIME ZONE 'dst'` | 3-arg form |
| `TO_DATE(s, fmt)` | `CAST(strptime(s, fmt) AS DATE)` | PG format strings converted |
| `TO_TIMESTAMP(s, fmt)` | `strptime(s, fmt)` | PG format strings converted |
| `TO_CHAR(ts, fmt)` | `strftime(ts, fmt)` | PG format strings converted |
| `MONTHS_BETWEEN(d1, d2)` | `datediff('month', d2, d1)` | |
| `ADD_MONTHS(d, n)` | `d + INTERVAL 'n' MONTH` | |

#### String

| Redshift | DuckDB | Notes |
|----------|--------|-------|
| `NVL(a, b)` | `coalesce(a, b)` | |
| `NVL2(e, a, b)` | `CASE WHEN e IS NOT NULL THEN a ELSE b END` | |
| `ISNULL(v, r)` | `coalesce(v, r)` | 2-arg form only |
| `LEN(s)` | `length(s)` | |
| `LCASE(s)` | `lower(s)` | |
| `UCASE(s)` | `upper(s)` | |
| `UPPER(s)` | `upper(s)` | |
| `LOWER(s)` | `lower(s)` | |
| `LEFT(s, n)` | `left(s, n)` | |
| `RIGHT(s, n)` | `right(s, n)` | |
| `SUBSTRING(s, ...)` | `substring(s, ...)` | |
| `REPLACE(s, from, to)` | `replace(s, from, to)` | |
| `BTRIM(s)` | `trim(s)` | |
| `TRIM(s)` | `trim(s)` | |
| `CHARINDEX(sub, str)` | `strpos(str, sub)` | Args swapped |
| `SPACE(n)` | `repeat(' ', n)` | |
| `REGEXP_SUBSTR(s, p)` | `regexp_extract(s, p)` | |
| `REGEXP_COUNT(s, p)` | `len(regexp_extract_all(s, p))` | |

#### Aggregate

| Redshift | DuckDB | Notes |
|----------|--------|-------|
| `DECODE(e, s1, r1, ..., def)` | `CASE e WHEN s1 THEN r1 ... ELSE def END` | |
| `LISTAGG(col, sep)` | `string_agg(col, sep)` | |

#### JSON

| Redshift | DuckDB | Notes |
|----------|--------|-------|
| `JSON_EXTRACT_PATH_TEXT(j, k1, k2, ...)` | `json_extract_string(j, '$.k1.k2...')` | Literal keys only |
| `JSON_EXTRACT_ARRAY_ELEMENT_TEXT(j, i)` | `json_extract_string(j, '$[i]')` | Literal index only |
| `JSON_ARRAY_LENGTH(j)` | `json_array_length(j)` | |
| `JSON_TYPEOF(j)` | `json_type(j)` | |
| `JSON_SERIALIZE(j)` | `CAST(j AS VARCHAR)` | |
| `JSON_DESERIALIZE(s)` | `CAST(s AS JSON)` | |
| `IS_VALID_JSON(s)` | `json_valid(s)` | |

#### Array

| Redshift | DuckDB | Notes |
|----------|--------|-------|
| `ARRAY_CONCAT(a, b)` | `list_concat(a, b)` | |

#### Crypto / Encoding

| Redshift | DuckDB | Notes |
|----------|--------|-------|
| `MD5(s)` | `md5(s)` | |
| `SHA1(s)` | `sha1(s)` | |
| `SHA2(s, 256)` | `sha256(s)` | 256-bit only |

#### Emulated

| Redshift | DuckDB | Notes |
|----------|--------|-------|
| `STRTOL(s, base)` | `CASE base WHEN 16 THEN CAST(('0x' \|\| s) AS BIGINT) WHEN 10 THEN CAST(s AS BIGINT) END` | Base 10 and 16 only |
| `RATIO_TO_REPORT(col) OVER (...)` | `col / SUM(col) OVER (...)` | Window clause preserved |

#### Unsupported

| Redshift | Notes |
|----------|-------|
| `BPCHARCMP(a, b)` | No DuckDB equivalent |

### Type Mappings: Trino to DuckDB

| Trino | DuckDB | Context |
|-------|--------|---------|
| `ROW(a INT, b VARCHAR)` | `STRUCT(a INT, b VARCHAR)` | CAST, DDL |
| `ARRAY(T)` | `T[]` | CAST, DDL |
| `ARRAY<T>` | `T[]` | CAST, DDL |
| `MAP(K, V)` | `MAP(K, V)` | Passthrough |
| `VARBINARY` | `BLOB` | CAST, DDL |
| `IPADDRESS` | `VARCHAR` | CAST, DDL |

### Type Mappings: Redshift to DuckDB

| Redshift | DuckDB | Context |
|----------|--------|---------|
| `VARCHAR(MAX)` | `VARCHAR` | CAST, DDL |
| `CHARACTER VARYING(MAX)` | `VARCHAR` | CAST, DDL |
| `NVARCHAR(MAX)` | `VARCHAR` | CAST, DDL |
| `SUPER` | `JSON` | CAST, DDL |
| `VARBINARY` | `BLOB` | CAST, DDL |
| `HLLSKETCH` | Unsupported | |
| `GEOMETRY` | Unsupported | |
| `TIMETZ` | `TIMETZ` | Passthrough |
| `TIMESTAMPTZ` | `TIMESTAMPTZ` | Passthrough |

### Type Mappings: Hive to DuckDB

Hive uses the same type rewrite rules as Trino.

| Hive | DuckDB | Context |
|------|--------|---------|
| `ROW(a INT, b VARCHAR)` | `STRUCT(a INT, b VARCHAR)` | CAST, DDL |
| `ARRAY(T)` | `T[]` | CAST, DDL |
| `ARRAY<T>` | `T[]` | CAST, DDL |
| `MAP(K, V)` | `MAP(K, V)` | Passthrough |
| `VARBINARY` | `BLOB` | CAST, DDL |
| `IPADDRESS` | `VARCHAR` | CAST, DDL |

### DDL Support

#### CREATE TABLE

| Feature | Behavior |
|---------|----------|
| Column type rewriting | Automatic (all type mappings applied) |
| `CREATE EXTERNAL TABLE ... STORED AS ... LOCATION` | Configurable: `MapToView` or `Error` |
| Iceberg via `TBLPROPERTIES ('table_type'='ICEBERG')` | Configurable: `MapToView` (uses `iceberg_scan()`) or `Error` |
| Iceberg via `WITH (table_type = 'ICEBERG')` | Same as above (Trino syntax) |
| `PARTITIONED BY` | `hive_partitioning = true` added to reader options |
| `ROW FORMAT DELIMITED FIELDS TERMINATED BY` | `delim = '...'` added to `read_csv` options |
| `ROW FORMAT DELIMITED ESCAPED BY` | `escape = '...'` added to `read_csv` options |
| `ROW FORMAT DELIMITED LINES TERMINATED BY` | `new_line = '...'` added to `read_csv` options |
| `ROW FORMAT DELIMITED NULL DEFINED AS` | `nullstr = '...'` added to `read_csv` options |
| `ROW FORMAT SERDE 'class'` | Reader function inferred from SerDe class name |

#### External Table Format Mapping

| STORED AS | DuckDB Reader |
|-----------|---------------|
| `PARQUET` | `read_parquet()` |
| `ORC` | `read_parquet()` |
| `TEXTFILE` | `read_csv()` |
| `JSONFILE` | `read_json()` |
| `AVRO` | Unsupported |
| `SEQUENCEFILE` | Unsupported |
| `RCFILE` | Unsupported |
| (none specified) | `read_parquet()` (default) |

#### SerDe Class Mapping

When no `STORED AS` is present, the reader function is inferred from `ROW FORMAT SERDE`:

| SerDe Class | DuckDB Reader |
|-------------|---------------|
| `ParquetHiveSerDe` | `read_parquet()` |
| `OrcSerde` | `read_parquet()` |
| `JsonSerDe` (Hive or OpenX) | `read_json()` |
| `OpenCSVSerde` | `read_csv()` |
| `LazySimpleSerDe` | `read_csv()` |
| `RegexSerDe` | Unsupported |
| Unknown classes | `Error` by default; override via `serde_class_resolver` |

The built-in mapping is substring-based and case-insensitive. For classes not listed above, supply a `SerdeClassResolver` in `TranspileOptions` (see the Library section for an example). The resolver is called first; returning `None` falls through to the built-in logic.

#### ALTER TABLE

| Operation | Behavior |
|-----------|----------|
| `ADD COLUMN` | Column type rewritten |
| `ALTER COLUMN ... SET DATA TYPE` | Data type rewritten |
| Other operations | Passthrough |

### DML Support

| Statement | Behavior |
|-----------|----------|
| `INSERT INTO ... SELECT` | Passthrough (expressions rewritten) |
| `INSERT INTO ... VALUES` | Passthrough |
| `UPDATE ... SET ... FROM` (Redshift) | Passthrough (DuckDB supports this) |
| `DELETE ... USING` (Redshift) | Passthrough (DuckDB supports this) |
| `MERGE` | Passthrough |
| `COPY FROM` (Redshift) | Configurable: `MapToInsert` (uses `read_parquet`/`read_csv`/`read_json`) or `Error` |

### SHOW Commands

| Command | DuckDB Output |
|---------|---------------|
| `SHOW TABLES` | Passthrough |
| `SHOW DATABASES` | Passthrough |
| `SHOW SCHEMAS` | Passthrough |
| `SHOW COLUMNS FROM t` | Passthrough |
| `SHOW VIEWS` | Passthrough |
| `SHOW CREATE TABLE t` | Emulated via `information_schema.columns` (reconstructs DDL) |
| `SHOW CREATE VIEW v` | Emulated via `duckdb_views()` (retrieves view SQL) |
| `SHOW variable` | `SELECT current_setting('variable')` |
| `SHOW FUNCTIONS` | `SELECT ... FROM information_schema.routines` |

### UNNEST and Lateral Joins

| Source Syntax | DuckDB Output |
|---------------|---------------|
| `CROSS JOIN UNNEST(arr) AS t(x)` | Passthrough |
| `CROSS JOIN UNNEST(arr) WITH ORDINALITY AS t(x, n)` | Passthrough |
| `LATERAL VIEW explode(arr) t AS x` | `CROSS JOIN UNNEST(arr) AS t(x)` |
| `LATERAL VIEW posexplode(arr) t AS x` | `CROSS JOIN UNNEST(arr) AS t(x)` |
| `CROSS JOIN LATERAL (subquery)` | Passthrough |

### Parameterized Queries

Parameterized queries (prepared statement placeholders) are passed through unchanged by papera.

| Dialect | Supported Styles | Example |
|---------|-----------------|---------|
| Trino | `?`, `$1` | `SELECT * FROM t WHERE x = ?` |
| Redshift | `$1` | `SELECT * FROM t WHERE x = $1` |
| Hive | `?`, `$1` | `SELECT * FROM t WHERE x = ?` |

On the default DuckDB path, `$1`-style positional parameters are native. The `?` style is also accepted by DuckDB in its client APIs.

## Configuration

### TranspileOptions

| Option | Values | Default | Description |
|--------|--------|---------|-------------|
| `target` | `DuckDB`, `DataFusion` | `DuckDB` | The target SQL dialect to emit |
| `external_table` | `MapToView`, `Error` | `Error` | How to handle `CREATE EXTERNAL TABLE` |
| `iceberg_table` | `MapToView`, `Error` | `Error` | How to handle Iceberg tables (detected via TBLPROPERTIES) |
| `copy` | `MapToInsert`, `Error` | `Error` | How to handle Redshift `COPY FROM` |
| `serde_class_resolver` | `Some(SerdeClassResolver)`, `None` | `None` | Custom resolver for `ROW FORMAT SERDE` class names not covered by the built-in mapping. Return `Some(reader_fn)` to override, `None` to fall through. |

## Project Structure

```
src/
  lib.rs                  Public API, TranspileOptions, crate-root exports
  error.rs                Error types
  main.rs                 CLI entry point
  transpiler/
    mod.rs                Transpiler trait
    rewrite.rs            ExprRewriter (VisitorMut-based AST walker)
  dialect/
    mod.rs                SourceDialect and TargetDialect enums
    trino.rs              Trino transpiler
    redshift.rs           Redshift transpiler
    hive.rs               Hive transpiler
  transforms/
    mod.rs                Module re-exports
    types.rs              Data type rewriting
    functions.rs          Function name/signature mapping
    format_strings.rs     Format string conversion (PG/Java → strftime)
    ddl.rs                CREATE TABLE, ALTER TABLE, external/iceberg tables
    dml.rs                INSERT, UPDATE, DELETE, MERGE
    show.rs               SHOW command translation
    unnest.rs             UNNEST syntax normalization
    lateral.rs            LATERAL VIEW to CROSS JOIN UNNEST
tests/
  common/mod.rs           Test helpers
  integration.rs          End-to-end tests
  duckdb_integration.rs   DuckDB execution tests
examples/
  basic.rs                Function, type, and syntax rewrites
  migration.rs            External tables, Iceberg, COPY with TranspileOptions
  multi_statement.rs      Multi-statement ETL script transpilation
  serde_resolver.rs       Custom SerDe class resolver with built-in fallthrough
```

## Architecture and Design

### Transpilation Pipeline

papera uses a two-stage AST transformation pipeline:

1. **Parse** source SQL with the dialect-specific parser from sqlparser-rs.
2. **Statement-level transforms** restructure top-level `Statement` variants (e.g., converting `CREATE EXTERNAL TABLE` into `CREATE VIEW`, or rewriting `SHOW CREATE TABLE` into catalog queries).
3. **Expression-level rewrites** via `ExprRewriter` (a `VisitorMut`-based AST walker) handle cross-cutting concerns such as function renaming, type casting, and table-factor normalization.
4. **Emit** SQL for the selected target dialect from the rewritten AST.

This split is intentional: statement handlers own structural changes that may replace one statement kind with another, while `ExprRewriter` handles expression-level rewrites that apply uniformly across statement types. Source dialect and target dialect are separate dimensions in the design: source dialect controls parsing and dialect-specific preprocessing, while target dialect controls function mappings, type rewrites, selected SHOW behavior, and some DDL lowering decisions.

### Library-First Design

The crate is designed as a library first. The CLI binary is feature-gated behind `cli` and is not built by default. Internal rewrite machinery under `src/transforms` is `pub(crate)`, keeping the stable public API surface small: `transpile`, `transpile_with_options`, source and target dialect selection, option types, the `SerdeClassResolver` extension hook, and shared error types.

### Opt-In Semantics for Risky Conversions

Features that can silently change semantics or storage assumptions are controlled by `TranspileOptions` and default to `Error`. External-table-to-view, Iceberg-table-to-view, and Redshift `COPY` lowering are examples: they are useful but alter storage or ingestion behavior, so callers must explicitly opt in.

### Parser as Feature Boundary

papera's feature coverage is bounded not only by the rewrite logic but also by what sqlparser-rs can parse and represent. For example:

- DDL column `DataType` nodes are not visited by `VisitorMut`, so `CREATE TABLE` column types must be rewritten in statement handlers rather than in the expression walker.
- Trino `ROW(a INT, b VARCHAR)` is exposed as flattened custom type data under `GenericDialect`, making nested `ROW` handling fragile.
- Some source syntax cannot be supported cleanly until the upstream parser exposes a usable AST for it.

### Rewrite Strategy

Function rewrites are classified by complexity:

- **Rename**: simple name substitution (e.g., `approx_distinct` to `approx_count_distinct`).
- **RenameReorder**: same function with reordered arguments (e.g., `CHARINDEX(sub, str)` to `strpos(str, sub)`).
- **Custom**: the rewrite must produce a different AST shape entirely (e.g., `NVL2(e, a, b)` becomes a `CASE WHEN` expression, bitwise functions become infix operators).

Declarative mappings are preferred where possible, with custom rewrites reserved for cases where the selected target requires a structurally different expression.

### Compatibility Model

papera targets engine-correct output, not just syntactically valid SQL. DuckDB execution remains the strongest validation path in the current test strategy, and some mappings are approximations rather than exact semantic matches (e.g., `url_extract_*` functions use regex approximations). String-level rewrite success alone is not considered sufficient evidence of compatibility, which is why the test suite includes DuckDB execution tests alongside string-comparison tests.

### Target Dialect Notes

DuckDB is the mature target and the one documented by most compatibility tables in this README. The library also supports `TargetDialect::DataFusion`, but that path has narrower coverage and different unsupported cases, especially for reader-backed external-table and Iceberg rewrites that currently rely on DuckDB-specific functions.

## Known Limitations

### Nested ROW types (sqlparser-rs 0.61)

Nested `ROW` types such as `ROW(x BIGINT, y ROW(i DOUBLE, j DOUBLE))` fail to parse in sqlparser-rs 0.61. The root cause is `parse_optional_type_modifiers()` in the parser, which cannot handle nested parentheses. This affects all dialects (including `HiveDialect`). Flat `ROW(a INT, b VARCHAR)` works correctly.

### ARRAY(T) vs ARRAY\<T\>

`ARRAY(T)` (Trino parenthesis syntax) is rewritten to `T[]`, but type inference inside `ARRAY(T)` depends on the parser recognizing the inner type. `ARRAY<T>` and `T[]` are fully supported.

### Approximate mappings

Some functions are approximations rather than exact semantic matches. For example, `url_extract_*` functions use regex-based approximations. Always validate output against DuckDB execution for compatibility-sensitive queries.

### DataFusion target scope

`TargetDialect::DataFusion` is available through the library API, but it is not feature-equivalent with DuckDB. In particular, reader-backed external-table and Iceberg rewrites remain DuckDB-specific, and some DataFusion-specific mappings are still better treated as explicit unsupported cases than as silent approximations.

### Redshift COPY options

When `copy` is set to `MapToInsert`, Redshift-specific options such as `IAM_ROLE`, `IGNOREHEADER`, and `GZIP` are silently dropped. The generated `INSERT INTO ... SELECT * FROM read_parquet/read_csv/read_json` reflects format and location only.

## Building

```sh
cargo build
cargo test
```

## License

MIT License. Copyright (c) 2026 Moriyoshi Koizumi. See [LICENSE](./LICENSE) for details.
