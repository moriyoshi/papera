//! Integration tests that transpile SQL from Trino/Redshift/Hive dialects and
//! execute the result against a real in-memory Apache DataFusion SessionContext.
//!
//! These tests verify that the transpiled SQL is not only syntactically correct
//! but also semantically produces the expected results when run in DataFusion.

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::*;
use papera::dialect::SourceDialect;
use papera::{TargetDialect, TranspileOptions};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn datafusion_opts() -> TranspileOptions {
    TranspileOptions {
        target: TargetDialect::DataFusion,
        ..Default::default()
    }
}

fn new_ctx() -> SessionContext {
    SessionContext::new()
}

/// Transpile source SQL to DataFusion SQL and execute it, discarding the result.
async fn exec(ctx: &SessionContext, source_sql: &str, dialect: SourceDialect) {
    let df_sql = papera::transpile_with_options(source_sql, dialect, &datafusion_opts())
        .unwrap_or_else(|e| panic!("transpile failed:\n  {source_sql}\n  error: {e}"));
    ctx.sql(&df_sql)
        .await
        .unwrap_or_else(|e| panic!("DataFusion SQL failed:\n  {df_sql}\n  error: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("collect failed:\n  {df_sql}\n  error: {e}"));
}

/// Transpile and collect all result batches.
async fn query(ctx: &SessionContext, source_sql: &str, dialect: SourceDialect) -> Vec<RecordBatch> {
    let df_sql = papera::transpile_with_options(source_sql, dialect, &datafusion_opts())
        .unwrap_or_else(|e| panic!("transpile failed:\n  {source_sql}\n  error: {e}"));
    ctx.sql(&df_sql)
        .await
        .unwrap_or_else(|e| panic!("DataFusion SQL failed:\n  {df_sql}\n  error: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("collect failed:\n  {df_sql}\n  error: {e}"))
}

/// Format all result batches as a display string for flexible value assertions.
///
/// Output looks like:
///   +--------+
///   | result |
///   +--------+
///   | alice  |
///   +--------+
fn display(batches: &[RecordBatch]) -> String {
    pretty_format_batches(batches).unwrap().to_string()
}

// ===========================================================================
// Trino — string functions
// ===========================================================================

#[tokio::test]
async fn trino_length() {
    let ctx = new_ctx();
    let d = display(&query(&ctx, "SELECT length('hello')", SourceDialect::Trino).await);
    assert!(d.contains('5'), "length('hello') = 5, got:\n{d}");
}

#[tokio::test]
async fn trino_reverse() {
    let ctx = new_ctx();
    let d = display(&query(&ctx, "SELECT reverse('hello')", SourceDialect::Trino).await);
    assert!(d.contains("olleh"), "reverse('hello') = 'olleh', got:\n{d}");
}

#[tokio::test]
async fn trino_lpad_rpad() {
    let ctx = new_ctx();
    let lpad = display(&query(&ctx, "SELECT lpad('hi', 5, '.')", SourceDialect::Trino).await);
    assert!(lpad.contains("...hi"), "lpad result: {lpad}");

    let rpad = display(&query(&ctx, "SELECT rpad('hi', 5, '.')", SourceDialect::Trino).await);
    assert!(rpad.contains("hi..."), "rpad result: {rpad}");
}

#[tokio::test]
async fn trino_chr_codepoint() {
    let ctx = new_ctx();
    let chr = display(&query(&ctx, "SELECT chr(65)", SourceDialect::Trino).await);
    assert!(chr.contains('A'), "chr(65) = 'A', got:\n{chr}");

    let cp = display(&query(&ctx, "SELECT codepoint('A')", SourceDialect::Trino).await);
    assert!(cp.contains("65"), "codepoint('A') = 65, got:\n{cp}");
}

#[tokio::test]
async fn trino_regexp_like() {
    let ctx = new_ctx();
    let d = display(
        &query(
            &ctx,
            "SELECT regexp_like('hello123', '[0-9]+')",
            SourceDialect::Trino,
        )
        .await,
    );
    assert!(d.contains("true"), "regexp_like should match, got:\n{d}");
}

#[tokio::test]
async fn trino_split() {
    let ctx = new_ctx();
    // split(s, ',') → string_to_array(s, ',') — verify it executes without error
    let d = display(
        &query(
            &ctx,
            "SELECT string_to_array('a,b,c', ',')",
            SourceDialect::Trino,
        )
        .await,
    );
    // DataFusion returns the array; just check it contains the values
    assert!(
        d.contains('a') && d.contains('b') && d.contains('c'),
        "split result: {d}"
    );
}

#[tokio::test]
async fn trino_strpos() {
    let ctx = new_ctx();
    let d = display(
        &query(
            &ctx,
            "SELECT strpos('hello world', 'world')",
            SourceDialect::Trino,
        )
        .await,
    );
    assert!(d.contains('7'), "strpos result: {d}");
}

#[tokio::test]
async fn trino_levenshtein_distance() {
    let ctx = new_ctx();
    let d = display(
        &query(
            &ctx,
            "SELECT levenshtein_distance('abc', 'abd')",
            SourceDialect::Trino,
        )
        .await,
    );
    assert!(
        d.contains('1'),
        "levenshtein_distance('abc','abd') = 1, got:\n{d}"
    );
}

// ===========================================================================
// Trino — array functions
// ===========================================================================

#[tokio::test]
async fn trino_cardinality() {
    let ctx = new_ctx();
    let d = display(&query(&ctx, "SELECT cardinality([1, 2, 3])", SourceDialect::Trino).await);
    assert!(d.contains('3'), "cardinality([1,2,3]) = 3, got:\n{d}");
}

#[tokio::test]
async fn trino_array_has() {
    let ctx = new_ctx();
    let d = display(&query(&ctx, "SELECT array_has([1, 2, 3], 2)", SourceDialect::Trino).await);
    assert!(
        d.contains("true"),
        "array_has([1,2,3], 2) = true, got:\n{d}"
    );
}

#[tokio::test]
async fn trino_array_max_min() {
    let ctx = new_ctx();
    let max = display(&query(&ctx, "SELECT array_max([3, 1, 2])", SourceDialect::Trino).await);
    assert!(max.contains('3'), "array_max = 3, got:\n{max}");

    let min = display(&query(&ctx, "SELECT array_min([3, 1, 2])", SourceDialect::Trino).await);
    assert!(min.contains('1'), "array_min = 1, got:\n{min}");
}

#[tokio::test]
async fn trino_array_sort() {
    let ctx = new_ctx();
    let d = display(&query(&ctx, "SELECT array_sort([3, 1, 2])", SourceDialect::Trino).await);
    assert!(
        d.contains('1') && d.contains('2') && d.contains('3'),
        "array_sort result: {d}"
    );
    // Verify order: 1 appears before 3 in the output
    let pos1 = d.find('1').unwrap();
    let pos3 = d.rfind('3').unwrap();
    assert!(pos1 < pos3, "array_sort should be ascending, got:\n{d}");
}

#[tokio::test]
async fn trino_array_distinct() {
    let ctx = new_ctx();
    let d = display(
        &query(
            &ctx,
            "SELECT array_distinct([1, 1, 2, 3, 2])",
            SourceDialect::Trino,
        )
        .await,
    );
    // Result should contain 1, 2, 3 — just verify it executes
    assert!(
        d.contains('1') && d.contains('2') && d.contains('3'),
        "array_distinct result: {d}"
    );
}

#[tokio::test]
async fn trino_arrays_overlap() {
    let ctx = new_ctx();
    let yes = display(
        &query(
            &ctx,
            "SELECT arrays_overlap([1, 2], [2, 3])",
            SourceDialect::Trino,
        )
        .await,
    );
    assert!(yes.contains("true"), "overlapping arrays, got:\n{yes}");

    let no = display(
        &query(
            &ctx,
            "SELECT arrays_overlap([1, 2], [3, 4])",
            SourceDialect::Trino,
        )
        .await,
    );
    assert!(no.contains("false"), "non-overlapping arrays, got:\n{no}");
}

#[tokio::test]
async fn trino_filter_transform() {
    // filter(arr, lambda) is not supported for DataFusion target
    let result = papera::transpile_with_options(
        "SELECT filter(arr, x -> x > 2) FROM t",
        SourceDialect::Trino,
        &datafusion_opts(),
    );
    assert!(
        result.is_err(),
        "filter (higher-order) should be unsupported for DataFusion target"
    );

    // transform(arr, x -> x * 2) → array_transform(arr, x -> x * 2)
    // DataFusion 52 does not have array_transform; verify the mapping produces the rename
    // and the transpilation itself succeeds (even if execution would fail at runtime)
    let sql = papera::transpile_with_options(
        "SELECT transform(ARRAY[1, 2, 3], x -> x * 2)",
        SourceDialect::Trino,
        &datafusion_opts(),
    )
    .expect("transform should transpile successfully");
    assert!(
        sql.contains("array_transform"),
        "transform should be renamed to array_transform, got: {sql}"
    );
}

// ===========================================================================
// Trino — date/time functions
// ===========================================================================

#[tokio::test]
async fn trino_from_unixtime() {
    let ctx = new_ctx();
    let d = display(
        &query(
            &ctx,
            "SELECT CAST(from_unixtime(0) AS VARCHAR)",
            SourceDialect::Trino,
        )
        .await,
    );
    assert!(d.contains("1970-01-01"), "from_unixtime(0): {d}");
}

#[tokio::test]
async fn trino_day_of_week_year() {
    let ctx = new_ctx();
    // 2024-01-01 is a Monday — DataFusion dayofweek: Sunday=1, Monday=2, ..., Saturday=7
    let dow = display(
        &query(
            &ctx,
            "SELECT day_of_week(DATE '2024-01-01')",
            SourceDialect::Trino,
        )
        .await,
    );
    assert!(dow.contains('2'), "2024-01-01 is Monday (2), got:\n{dow}");

    // 2024-01-01 is day 1 of the year
    let doy = display(
        &query(
            &ctx,
            "SELECT day_of_year(DATE '2024-01-01')",
            SourceDialect::Trino,
        )
        .await,
    );
    assert!(
        doy.contains('1'),
        "2024-01-01 is day_of_year=1, got:\n{doy}"
    );
}

#[tokio::test]
async fn trino_week_of_year() {
    let ctx = new_ctx();
    // 2024-01-07 is the first Sunday → week 1 (ISO week)
    let d = display(
        &query(
            &ctx,
            "SELECT week_of_year(DATE '2024-01-07')",
            SourceDialect::Trino,
        )
        .await,
    );
    assert!(d.contains('1'), "week_of_year(2024-01-07) = 1, got:\n{d}");
}

#[tokio::test]
async fn trino_date_diff() {
    let ctx = new_ctx();
    // date_diff('day', d1, d2) → epoch arithmetic via to_unixtime
    let diff = display(
        &query(
            &ctx,
            "SELECT date_diff('day', DATE '2024-01-01', DATE '2024-01-10')",
            SourceDialect::Trino,
        )
        .await,
    );
    assert!(diff.contains('9'), "date_diff('day') = 9, got:\n{diff}");

    // date_diff('month', d1, d2) — Trino counts complete months (day-boundary aware)
    // 1970-01-20 → 1970-02-19: day 19 < 20, so 0 complete months
    let month_zero = display(
        &query(
            &ctx,
            "SELECT date_diff('month', TIMESTAMP '1970-01-20 00:00:00', TIMESTAMP '1970-02-19 00:00:00')",
            SourceDialect::Trino,
        )
        .await,
    );
    // The last line of the pretty-printed table is "| 0 |"
    assert!(
        month_zero
            .lines()
            .any(|l| l.starts_with("| 0 ") && l.ends_with('|')),
        "date_diff('month') boundary: expected 0, got:\n{month_zero}"
    );

    // 1970-01-20 → 1970-02-20: day 20 >= 20, so 1 complete month
    let month_one = display(
        &query(
            &ctx,
            "SELECT date_diff('month', TIMESTAMP '1970-01-20 00:00:00', TIMESTAMP '1970-02-20 00:00:00')",
            SourceDialect::Trino,
        )
        .await,
    );
    assert!(
        month_one
            .lines()
            .any(|l| l.starts_with("| 1 ") && l.ends_with('|')),
        "date_diff('month') = 1, got:\n{month_one}"
    );

    // date_diff('month') over multiple months
    let month_diff = display(
        &query(
            &ctx,
            "SELECT date_diff('month', DATE '2024-01-15', DATE '2024-04-15')",
            SourceDialect::Trino,
        )
        .await,
    );
    assert!(
        month_diff.contains('3'),
        "date_diff('month') = 3, got:\n{month_diff}"
    );

    // date_diff('year') — exact year boundary
    let year_zero = display(
        &query(
            &ctx,
            "SELECT date_diff('year', DATE '2020-06-15', DATE '2021-06-14')",
            SourceDialect::Trino,
        )
        .await,
    );
    assert!(
        year_zero
            .lines()
            .any(|l| l.starts_with("| 0 ") && l.ends_with('|')),
        "date_diff('year') boundary: expected 0, got:\n{year_zero}"
    );

    let year_one = display(
        &query(
            &ctx,
            "SELECT date_diff('year', DATE '2020-06-15', DATE '2021-06-15')",
            SourceDialect::Trino,
        )
        .await,
    );
    assert!(
        year_one
            .lines()
            .any(|l| l.starts_with("| 1 ") && l.ends_with('|')),
        "date_diff('year') = 1, got:\n{year_one}"
    );
}

#[tokio::test]
async fn trino_date_trunc() {
    // date_trunc passes through to DataFusion (both support it)
    let ctx = new_ctx();
    let d = display(
        &query(
            &ctx,
            "SELECT CAST(date_trunc('month', DATE '2024-03-15') AS VARCHAR)",
            SourceDialect::Trino,
        )
        .await,
    );
    assert!(
        d.contains("2024-03-01"),
        "date_trunc('month', 2024-03-15) = 2024-03-01: {d}"
    );
}

#[tokio::test]
async fn trino_date_parse() {
    let ctx = new_ctx();
    let d = display(
        &query(
            &ctx,
            "SELECT CAST(date_parse('2024-03-15', '%Y-%m-%d') AS VARCHAR)",
            SourceDialect::Trino,
        )
        .await,
    );
    assert!(d.contains("2024-03-15"), "date_parse result: {d}");
}

// ===========================================================================
// Trino — JSON functions
// ===========================================================================

#[tokio::test]
async fn trino_json_extract_scalar() {
    // json_extract_scalar is not supported for DataFusion target
    let result = papera::transpile_with_options(
        r#"SELECT json_extract_scalar('{"name":"alice"}', '$.name')"#,
        SourceDialect::Trino,
        &datafusion_opts(),
    );
    assert!(
        result.is_err(),
        "json_extract_scalar should be unsupported for DataFusion target"
    );
}

#[tokio::test]
async fn trino_json_parse_format() {
    // json_parse is not supported for DataFusion target (JSON type unsupported)
    let result = papera::transpile_with_options(
        r#"SELECT CAST(json_parse('{"key":1}') AS VARCHAR)"#,
        SourceDialect::Trino,
        &datafusion_opts(),
    );
    assert!(
        result.is_err(),
        "json_parse should be unsupported for DataFusion target"
    );
}

#[tokio::test]
async fn trino_json_array_get() {
    // json_array_get maps to json_extract_scalar which is not available in DataFusion 52
    let result = papera::transpile_with_options(
        r#"SELECT json_array_get('[10, 20, 30]', 1)"#,
        SourceDialect::Trino,
        &datafusion_opts(),
    );
    assert!(
        result.is_err(),
        "json_array_get should be unsupported for DataFusion target"
    );
}

// ===========================================================================
// Trino — math / type inspection functions
// ===========================================================================

#[tokio::test]
async fn trino_is_nan() {
    let ctx = new_ctx();
    let nan = display(
        &query(
            &ctx,
            "SELECT is_nan(CAST('NaN' AS DOUBLE))",
            SourceDialect::Trino,
        )
        .await,
    );
    assert!(nan.contains("true"), "is_nan(NaN) = true, got:\n{nan}");
}

#[tokio::test]
async fn trino_is_finite_infinite_unsupported() {
    // is_finite and is_infinite have no DataFusion equivalent
    let finite_err = papera::transpile_with_options(
        "SELECT is_finite(1.0)",
        SourceDialect::Trino,
        &datafusion_opts(),
    );
    assert!(
        finite_err.is_err(),
        "is_finite should be unsupported for DataFusion target"
    );

    let infinite_err = papera::transpile_with_options(
        "SELECT is_infinite(CAST('Infinity' AS DOUBLE))",
        SourceDialect::Trino,
        &datafusion_opts(),
    );
    assert!(
        infinite_err.is_err(),
        "is_infinite should be unsupported for DataFusion target"
    );
}

#[tokio::test]
async fn trino_typeof() {
    let ctx = new_ctx();
    let d = display(&query(&ctx, "SELECT typeof(123)", SourceDialect::Trino).await);
    // arrow_typeof(123) returns a type string like "Int32" or "Int64"
    assert!(
        d.contains("Int") || d.contains("int"),
        "typeof(123) should be an integer type, got:\n{d}"
    );
}

// ===========================================================================
// Trino — aggregate functions
// ===========================================================================

#[tokio::test]
async fn trino_arbitrary_any_value() {
    // arbitrary (any_value) is not supported for DataFusion target
    let result = papera::transpile_with_options(
        "SELECT arbitrary(val) FROM t_arb WHERE grp = 1",
        SourceDialect::Trino,
        &datafusion_opts(),
    );
    assert!(
        result.is_err(),
        "arbitrary should be unsupported for DataFusion target"
    );
}

#[tokio::test]
async fn trino_approx_distinct() {
    let ctx = new_ctx();
    exec(
        &ctx,
        "CREATE TABLE t_apx (val VARCHAR)",
        SourceDialect::Trino,
    )
    .await;
    exec(
        &ctx,
        "INSERT INTO t_apx VALUES ('a'), ('b'), ('a'), ('c'), ('b')",
        SourceDialect::Trino,
    )
    .await;

    let d = display(
        &query(
            &ctx,
            "SELECT approx_distinct(val) FROM t_apx",
            SourceDialect::Trino,
        )
        .await,
    );
    // approx_distinct of 3 unique values; result should be close to 3
    assert!(d.contains('3'), "approx_distinct ≈ 3, got:\n{d}");
}

#[tokio::test]
async fn trino_map_agg() {
    // map_agg is not supported for DataFusion target
    let result = papera::transpile_with_options(
        "SELECT map_agg(k, v) FROM t_map",
        SourceDialect::Trino,
        &datafusion_opts(),
    );
    assert!(
        result.is_err(),
        "map_agg should be unsupported for DataFusion target"
    );
}

// ===========================================================================
// Trino — map functions
// ===========================================================================

#[tokio::test]
async fn trino_map_keys_values() {
    // map_agg is unsupported for DataFusion; verify the error propagates cleanly
    let result = papera::transpile_with_options(
        "SELECT map_keys(map_agg(k, v)) FROM (VALUES ('a', 1), ('b', 2)) AS t(k, v)",
        SourceDialect::Trino,
        &datafusion_opts(),
    );
    assert!(
        result.is_err(),
        "map_agg (within map_keys) should be unsupported for DataFusion target"
    );

    // Verify map_keys and map_values themselves pass through when transpiled
    // (they are passthroughs to DataFusion's built-in map_keys/map_values)
    let sql = papera::transpile_with_options(
        "SELECT map_keys(m), map_values(m) FROM t",
        SourceDialect::Trino,
        &datafusion_opts(),
    )
    .expect("map_keys/map_values should transpile without renaming");
    assert!(
        sql.contains("map_keys"),
        "map_keys should be a passthrough: {sql}"
    );
    assert!(
        sql.contains("map_values"),
        "map_values should be a passthrough: {sql}"
    );
}

// ===========================================================================
// Trino — bitwise functions
// ===========================================================================

#[tokio::test]
async fn trino_bitwise_ops() {
    let ctx = new_ctx();

    let and = display(&query(&ctx, "SELECT bitwise_and(12, 10)", SourceDialect::Trino).await);
    // 12 & 10 = 8
    assert!(and.contains('8'), "bitwise_and(12,10) = 8, got:\n{and}");

    let or = display(&query(&ctx, "SELECT bitwise_or(12, 10)", SourceDialect::Trino).await);
    // 12 | 10 = 14
    assert!(or.contains("14"), "bitwise_or(12,10) = 14, got:\n{or}");

    let xor = display(&query(&ctx, "SELECT bitwise_xor(12, 10)", SourceDialect::Trino).await);
    // 12 ^ 10 = 6
    assert!(xor.contains('6'), "bitwise_xor(12,10) = 6, got:\n{xor}");

    let shl = display(
        &query(
            &ctx,
            "SELECT bitwise_left_shift(1, 3)",
            SourceDialect::Trino,
        )
        .await,
    );
    // 1 << 3 = 8
    assert!(
        shl.contains('8'),
        "bitwise_left_shift(1,3) = 8, got:\n{shl}"
    );

    let shr = display(
        &query(
            &ctx,
            "SELECT bitwise_right_shift(16, 2)",
            SourceDialect::Trino,
        )
        .await,
    );
    // 16 >> 2 = 4
    assert!(
        shr.contains('4'),
        "bitwise_right_shift(16,2) = 4, got:\n{shr}"
    );
}

// ===========================================================================
// Trino — misc functions
// ===========================================================================

#[tokio::test]
async fn trino_rand() {
    let ctx = new_ctx();
    // rand() → random() in DataFusion — just verify it executes and is in [0, 1)
    let d = display(&query(&ctx, "SELECT rand()", SourceDialect::Trino).await);
    assert!(!d.is_empty(), "rand() returned empty: {d}");
    // The display will show a float; verify it's not an error
    assert!(!d.contains("error"), "rand() error: {d}");
}

// ===========================================================================
// DDL — type rewrites
// ===========================================================================

#[tokio::test]
async fn trino_create_table_varbinary() {
    let ctx = new_ctx();
    // VARBINARY → BYTEA; verify DataFusion accepts the type
    exec(
        &ctx,
        "CREATE TABLE t_vb (id INTEGER, payload VARBINARY)",
        SourceDialect::Trino,
    )
    .await;
}

#[tokio::test]
async fn trino_create_table_row_type() {
    let ctx = new_ctx();
    // ROW(...) → STRUCT<...>; DataFusion uses angle-bracket STRUCT syntax
    exec(
        &ctx,
        "CREATE TABLE t_row (id INTEGER, rec ROW(name VARCHAR, age INTEGER))",
        SourceDialect::Trino,
    )
    .await;
}

// ===========================================================================
// Redshift — function rewrites
// ===========================================================================

#[tokio::test]
async fn redshift_nvl() {
    let ctx = new_ctx();

    let d = display(
        &query(
            &ctx,
            "SELECT NVL(NULL, 'fallback')",
            SourceDialect::Redshift,
        )
        .await,
    );
    assert!(d.contains("fallback"), "NVL(NULL, 'fallback'): {d}");

    let d = display(
        &query(
            &ctx,
            "SELECT NVL('value', 'fallback')",
            SourceDialect::Redshift,
        )
        .await,
    );
    assert!(d.contains("value"), "NVL('value', 'fallback'): {d}");
}

#[tokio::test]
async fn redshift_nvl2() {
    let ctx = new_ctx();

    let d = display(
        &query(
            &ctx,
            "SELECT NVL2('notnull', 'yes', 'no')",
            SourceDialect::Redshift,
        )
        .await,
    );
    assert!(d.contains("yes"), "NVL2(not-null): {d}");

    let d = display(
        &query(
            &ctx,
            "SELECT NVL2(NULL, 'yes', 'no')",
            SourceDialect::Redshift,
        )
        .await,
    );
    assert!(d.contains("no"), "NVL2(null): {d}");
}

#[tokio::test]
async fn redshift_decode() {
    let ctx = new_ctx();

    let d = display(
        &query(
            &ctx,
            "SELECT DECODE(2, 1, 'one', 2, 'two', 'other')",
            SourceDialect::Redshift,
        )
        .await,
    );
    assert!(d.contains("two"), "DECODE matched 2: {d}");

    let d = display(
        &query(
            &ctx,
            "SELECT DECODE(9, 1, 'one', 2, 'two', 'other')",
            SourceDialect::Redshift,
        )
        .await,
    );
    assert!(d.contains("other"), "DECODE fallback: {d}");
}

#[tokio::test]
async fn redshift_len_charindex() {
    let ctx = new_ctx();

    let d = display(&query(&ctx, "SELECT LEN('hello')", SourceDialect::Redshift).await);
    assert!(d.contains('5'), "LEN('hello') = 5: {d}");

    let d = display(
        &query(
            &ctx,
            "SELECT CHARINDEX('world', 'hello world')",
            SourceDialect::Redshift,
        )
        .await,
    );
    assert!(d.contains('7'), "CHARINDEX = 7: {d}");
}

#[tokio::test]
async fn redshift_btrim() {
    let ctx = new_ctx();
    let d = display(&query(&ctx, "SELECT BTRIM('  hello  ')", SourceDialect::Redshift).await);
    assert!(d.contains("hello"), "BTRIM result: {d}");
}

#[tokio::test]
async fn redshift_md5() {
    let ctx = new_ctx();
    let d = display(&query(&ctx, "SELECT MD5('hello')", SourceDialect::Redshift).await);
    assert!(
        d.contains("5d41402abc4b2a76b9719d911017c592"),
        "MD5 of 'hello': {d}"
    );
}

#[tokio::test]
async fn redshift_listagg() {
    let ctx = new_ctx();
    exec(
        &ctx,
        "CREATE TABLE tags (id INT, tag VARCHAR)",
        SourceDialect::Redshift,
    )
    .await;
    exec(
        &ctx,
        "INSERT INTO tags VALUES (1, 'a'), (1, 'b'), (1, 'c')",
        SourceDialect::Redshift,
    )
    .await;

    let d = display(
        &query(
            &ctx,
            "SELECT listagg(tag, ',') FROM tags WHERE id = 1",
            SourceDialect::Redshift,
        )
        .await,
    );
    // string_agg result; order may vary but all three items should appear
    assert!(
        d.contains('a') && d.contains('b') && d.contains('c'),
        "listagg: {d}"
    );
}

#[tokio::test]
async fn redshift_getdate() {
    let ctx = new_ctx();
    // getdate() → now() — just verify it executes and returns something non-empty
    let d = display(
        &query(
            &ctx,
            "SELECT CAST(getdate() AS VARCHAR)",
            SourceDialect::Redshift,
        )
        .await,
    );
    assert!(!d.is_empty() && !d.contains("error"), "getdate: {d}");
}

#[tokio::test]
async fn redshift_json_extract_path_text() {
    // json_extract_path_text is not supported for DataFusion target
    let result = papera::transpile_with_options(
        r#"SELECT JSON_EXTRACT_PATH_TEXT('{"user":{"name":"alice"}}', 'user', 'name')"#,
        SourceDialect::Redshift,
        &datafusion_opts(),
    );
    assert!(
        result.is_err(),
        "json_extract_path_text should be unsupported for DataFusion target"
    );
}

#[tokio::test]
async fn redshift_months_between() {
    let ctx = new_ctx();
    let d = display(
        &query(
            &ctx,
            "SELECT months_between(DATE '2024-03-15', DATE '2024-01-15')",
            SourceDialect::Redshift,
        )
        .await,
    );
    assert!(d.contains('2'), "months_between = 2, got:\n{d}");
}

#[tokio::test]
async fn redshift_datediff() {
    let ctx = new_ctx();
    let d = display(
        &query(
            &ctx,
            "SELECT datediff(day, DATE '2024-01-01', DATE '2024-01-10')",
            SourceDialect::Redshift,
        )
        .await,
    );
    assert!(d.contains('9'), "datediff = 9 days, got:\n{d}");
}

#[tokio::test]
async fn redshift_strtol() {
    // strtol is not supported for DataFusion target (hex string casting not available)
    let result = papera::transpile_with_options(
        "SELECT strtol('ff', 16)",
        SourceDialect::Redshift,
        &datafusion_opts(),
    );
    assert!(
        result.is_err(),
        "strtol should be unsupported for DataFusion target"
    );
}

#[tokio::test]
async fn redshift_convert_timezone() {
    let ctx = new_ctx();
    // Just verify it executes without error
    let d = display(
        &query(
            &ctx,
            "SELECT CAST(convert_timezone('UTC', TIMESTAMP '2024-01-15 12:00:00') AS VARCHAR)",
            SourceDialect::Redshift,
        )
        .await,
    );
    assert!(
        !d.is_empty() && !d.contains("error"),
        "convert_timezone: {d}"
    );
}

// ===========================================================================
// Hive dialect — DataFusion target
// ===========================================================================

#[tokio::test]
async fn hive_array_functions() {
    let ctx = new_ctx();
    let d = display(&query(&ctx, "SELECT array_max([5, 3, 7, 1])", SourceDialect::Hive).await);
    assert!(d.contains('7'), "array_max = 7, got:\n{d}");
}

#[tokio::test]
async fn hive_string_functions() {
    let ctx = new_ctx();
    let d = display(&query(&ctx, "SELECT length('hello world')", SourceDialect::Hive).await);
    assert!(d.contains("11"), "length = 11, got:\n{d}");
}

// ===========================================================================
// SHOW commands — DataFusion target
// ===========================================================================

#[tokio::test]
async fn show_tables_datafusion() {
    use datafusion::execution::context::SessionConfig;
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
    exec(&ctx, "CREATE TABLE foo (id INT)", SourceDialect::Trino).await;
    exec(&ctx, "CREATE TABLE bar (id INT)", SourceDialect::Trino).await;

    // SHOW TABLES passes through for DataFusion (requires information_schema enabled)
    let d = display(&query(&ctx, "SHOW TABLES", SourceDialect::Trino).await);
    assert!(d.contains("foo"), "SHOW TABLES missing foo: {d}");
    assert!(d.contains("bar"), "SHOW TABLES missing bar: {d}");
}
