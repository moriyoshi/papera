//! Integration tests that transpile SQL from Trino/Redshift/Hive dialects and
//! execute the result against a real in-memory DuckDB instance.
//!
//! These tests verify that the transpiled SQL is not only syntactically correct
//! but also semantically produces the expected results.

use duckdb::{Connection, params};
use papera::dialect::SourceDialect;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Transpile source SQL and execute every resulting statement against DuckDB.
fn exec(conn: &Connection, source_sql: &str, dialect: SourceDialect) {
    let duckdb_sql = papera::transpile(source_sql, dialect)
        .unwrap_or_else(|e| panic!("transpile failed for:\n  {source_sql}\n  error: {e}"));
    for stmt in duckdb_sql.split(";\n") {
        let stmt = stmt.trim();
        if !stmt.is_empty() {
            conn.execute_batch(stmt)
                .unwrap_or_else(|e| panic!("DuckDB exec failed for:\n  {stmt}\n  error: {e}"));
        }
    }
}

/// Transpile and execute a query, returning all rows from the first column as strings.
fn query_col(conn: &Connection, source_sql: &str, dialect: SourceDialect) -> Vec<String> {
    let duckdb_sql = papera::transpile(source_sql, dialect)
        .unwrap_or_else(|e| panic!("transpile failed for:\n  {source_sql}\n  error: {e}"));
    let mut stmt = conn
        .prepare(&duckdb_sql)
        .unwrap_or_else(|e| panic!("DuckDB prepare failed for:\n  {duckdb_sql}\n  error: {e}"));
    let rows = stmt
        .query_map(params![], |row| row.get::<_, String>(0))
        .unwrap_or_else(|e| panic!("DuckDB query failed for:\n  {duckdb_sql}\n  error: {e}"));
    rows.map(|r| r.unwrap()).collect()
}

/// Transpile and execute a query, returning all rows from the first column as i64.
fn query_i64(conn: &Connection, source_sql: &str, dialect: SourceDialect) -> Vec<i64> {
    let duckdb_sql = papera::transpile(source_sql, dialect)
        .unwrap_or_else(|e| panic!("transpile failed for:\n  {source_sql}\n  error: {e}"));
    let mut stmt = conn
        .prepare(&duckdb_sql)
        .unwrap_or_else(|e| panic!("DuckDB prepare failed for:\n  {duckdb_sql}\n  error: {e}"));
    let rows = stmt
        .query_map(params![], |row| row.get::<_, i64>(0))
        .unwrap_or_else(|e| panic!("DuckDB query failed for:\n  {duckdb_sql}\n  error: {e}"));
    rows.map(|r| r.unwrap()).collect()
}

/// Transpile and execute a scalar query, returning the first column of the first row as String.
fn query_scalar(conn: &Connection, source_sql: &str, dialect: SourceDialect) -> String {
    query_col(conn, source_sql, dialect)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("Expected at least one row for:\n  {source_sql}"))
}

/// Transpile and execute a scalar query, returning the first column of the first row as i64.
fn query_scalar_i64(conn: &Connection, source_sql: &str, dialect: SourceDialect) -> i64 {
    query_i64(conn, source_sql, dialect)
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("Expected at least one row for:\n  {source_sql}"))
}

fn new_conn() -> Connection {
    Connection::open_in_memory().unwrap()
}

// ===========================================================================
// DDL — CREATE TABLE with type rewrites
// ===========================================================================

#[test]
fn redshift_create_table_types() {
    let conn = new_conn();
    // VARCHAR(MAX) → VARCHAR, SUPER → JSON
    exec(
        &conn,
        "CREATE TABLE t (id INTEGER, data SUPER, name VARCHAR(MAX))",
        SourceDialect::Redshift,
    );
    exec(
        &conn,
        "INSERT INTO t VALUES (1, '\"hello\"', 'world')",
        SourceDialect::Redshift,
    );
    let name = query_scalar(
        &conn,
        "SELECT name FROM t WHERE id = 1",
        SourceDialect::Redshift,
    );
    assert_eq!(name, "world");
}

#[test]
fn trino_create_table_varbinary_to_blob() {
    let conn = new_conn();
    exec(
        &conn,
        "CREATE TABLE t (id INTEGER, payload VARBINARY)",
        SourceDialect::Trino,
    );
    // Verify the column was created as BLOB
    let col_type: String = conn.query_row(
        "SELECT data_type FROM information_schema.columns WHERE table_name = 't' AND column_name = 'payload'",
        [], |r| r.get(0),
    ).unwrap();
    assert_eq!(col_type, "BLOB");
}

// ===========================================================================
// Redshift function transpilation — executed in DuckDB
// ===========================================================================

#[test]
fn redshift_nvl() {
    let conn = new_conn();
    let result = query_scalar(
        &conn,
        "SELECT NVL(NULL, 'fallback')",
        SourceDialect::Redshift,
    );
    assert_eq!(result, "fallback");

    let result = query_scalar(
        &conn,
        "SELECT NVL('value', 'fallback')",
        SourceDialect::Redshift,
    );
    assert_eq!(result, "value");
}

#[test]
fn redshift_nvl2() {
    let conn = new_conn();
    let result = query_scalar(
        &conn,
        "SELECT NVL2('notnull', 'yes', 'no')",
        SourceDialect::Redshift,
    );
    assert_eq!(result, "yes");

    let result = query_scalar(
        &conn,
        "SELECT NVL2(NULL, 'yes', 'no')",
        SourceDialect::Redshift,
    );
    assert_eq!(result, "no");
}

#[test]
fn redshift_decode() {
    let conn = new_conn();
    let result = query_scalar(
        &conn,
        "SELECT DECODE(2, 1, 'one', 2, 'two', 'other')",
        SourceDialect::Redshift,
    );
    assert_eq!(result, "two");

    let result = query_scalar(
        &conn,
        "SELECT DECODE(9, 1, 'one', 2, 'two', 'other')",
        SourceDialect::Redshift,
    );
    assert_eq!(result, "other");
}

#[test]
fn redshift_listagg() {
    let conn = new_conn();
    conn.execute_batch("CREATE TABLE tags (id INT, tag VARCHAR); INSERT INTO tags VALUES (1, 'a'), (1, 'b'), (1, 'c')").unwrap();
    let result = query_scalar(
        &conn,
        "SELECT listagg(tag, ',') FROM tags WHERE id = 1",
        SourceDialect::Redshift,
    );
    // string_agg order is non-deterministic; just check length
    assert_eq!(result.len(), 5); // "a,b,c"
}

#[test]
fn redshift_charindex() {
    let conn = new_conn();
    let result = query_scalar_i64(
        &conn,
        "SELECT CHARINDEX('world', 'hello world')",
        SourceDialect::Redshift,
    );
    assert_eq!(result, 7);
}

#[test]
fn redshift_len() {
    let conn = new_conn();
    let result = query_scalar_i64(&conn, "SELECT LEN('hello')", SourceDialect::Redshift);
    assert_eq!(result, 5);
}

#[test]
fn redshift_btrim() {
    let conn = new_conn();
    let result = query_scalar(&conn, "SELECT BTRIM('  hello  ')", SourceDialect::Redshift);
    assert_eq!(result, "hello");
}

#[test]
fn redshift_upper_lower() {
    let conn = new_conn();
    let result = query_scalar(&conn, "SELECT UPPER('hello')", SourceDialect::Redshift);
    assert_eq!(result, "HELLO");
    let result = query_scalar(&conn, "SELECT LOWER('HELLO')", SourceDialect::Redshift);
    assert_eq!(result, "hello");
}

#[test]
fn redshift_left_right() {
    let conn = new_conn();
    let result = query_scalar(&conn, "SELECT LEFT('hello', 3)", SourceDialect::Redshift);
    assert_eq!(result, "hel");
    let result = query_scalar(&conn, "SELECT RIGHT('hello', 3)", SourceDialect::Redshift);
    assert_eq!(result, "llo");
}

#[test]
fn redshift_replace() {
    let conn = new_conn();
    let result = query_scalar(
        &conn,
        "SELECT REPLACE('hello world', 'world', 'rust')",
        SourceDialect::Redshift,
    );
    assert_eq!(result, "hello rust");
}

#[test]
fn redshift_md5() {
    let conn = new_conn();
    let result = query_scalar(&conn, "SELECT MD5('hello')", SourceDialect::Redshift);
    assert_eq!(result, "5d41402abc4b2a76b9719d911017c592");
}

#[test]
fn redshift_json_extract_path_text() {
    let conn = new_conn();
    let result = query_scalar(
        &conn,
        r#"SELECT JSON_EXTRACT_PATH_TEXT('{"user":{"name":"alice"}}', 'user', 'name')"#,
        SourceDialect::Redshift,
    );
    assert_eq!(result, "alice");
}

#[test]
fn redshift_json_extract_array_element_text() {
    let conn = new_conn();
    let result = query_scalar(
        &conn,
        r#"SELECT JSON_EXTRACT_ARRAY_ELEMENT_TEXT('[10, 20, 30]', 1)"#,
        SourceDialect::Redshift,
    );
    assert_eq!(result, "20");
}

#[test]
fn redshift_convert_timezone_2arg() {
    let conn = new_conn();
    // Just verify it executes without error and returns something
    let result = query_scalar(
        &conn,
        "SELECT CAST(CONVERT_TIMEZONE('UTC', TIMESTAMP '2024-01-15 12:00:00') AS VARCHAR)",
        SourceDialect::Redshift,
    );
    assert!(!result.is_empty());
}

#[test]
fn redshift_date_functions() {
    let conn = new_conn();
    // date_trunc works directly
    let result = query_scalar(
        &conn,
        "SELECT CAST(date_trunc('month', DATE '2024-03-15') AS VARCHAR)",
        SourceDialect::Redshift,
    );
    assert!(result.contains("2024-03-01"), "Got: {result}");

    // TODO: Redshift's 3-arg dateadd(part, n, date) needs custom transform
    // to DuckDB's 2-arg date_add(date, interval). Currently just renames,
    // which causes a signature mismatch. This needs a Custom handler.
}

#[test]
fn redshift_date_trunc() {
    let conn = new_conn();
    let result = query_scalar(
        &conn,
        "SELECT CAST(date_trunc('month', DATE '2024-03-15') AS VARCHAR)",
        SourceDialect::Redshift,
    );
    assert!(result.contains("2024-03-01"), "Got: {result}");
}

#[test]
fn redshift_regexp_substr() {
    let conn = new_conn();
    let result = query_scalar(
        &conn,
        "SELECT regexp_substr('hello123world', '[0-9]+')",
        SourceDialect::Redshift,
    );
    assert_eq!(result, "123");
}

// ===========================================================================
// Trino function transpilation — executed in DuckDB
// ===========================================================================

#[test]
fn trino_arbitrary_any_value() {
    let conn = new_conn();
    conn.execute_batch("CREATE TABLE t (grp INT, val VARCHAR); INSERT INTO t VALUES (1, 'a')")
        .unwrap();
    let result = query_scalar(
        &conn,
        "SELECT arbitrary(val) FROM t WHERE grp = 1",
        SourceDialect::Trino,
    );
    assert_eq!(result, "a");
}

#[test]
fn trino_cardinality() {
    let conn = new_conn();
    let result = query_scalar_i64(&conn, "SELECT cardinality([1, 2, 3])", SourceDialect::Trino);
    assert_eq!(result, 3);
}

#[test]
fn trino_strpos() {
    let conn = new_conn();
    let result = query_scalar_i64(
        &conn,
        "SELECT strpos('hello world', 'world')",
        SourceDialect::Trino,
    );
    assert_eq!(result, 7);
}

#[test]
fn trino_length() {
    let conn = new_conn();
    let result = query_scalar_i64(&conn, "SELECT length('hello')", SourceDialect::Trino);
    assert_eq!(result, 5);
}

#[test]
fn trino_reverse() {
    let conn = new_conn();
    let result = query_scalar(&conn, "SELECT reverse('hello')", SourceDialect::Trino);
    assert_eq!(result, "olleh");
}

#[test]
fn trino_lpad_rpad() {
    let conn = new_conn();
    let result = query_scalar(&conn, "SELECT lpad('hi', 5, '.')", SourceDialect::Trino);
    assert_eq!(result, "...hi");
    let result = query_scalar(&conn, "SELECT rpad('hi', 5, '.')", SourceDialect::Trino);
    assert_eq!(result, "hi...");
}

#[test]
fn trino_chr_codepoint() {
    let conn = new_conn();
    let result = query_scalar(&conn, "SELECT chr(65)", SourceDialect::Trino);
    assert_eq!(result, "A");
    let result = query_scalar_i64(&conn, "SELECT codepoint('A')", SourceDialect::Trino);
    assert_eq!(result, 65);
}

#[test]
fn trino_regexp_like() {
    let conn = new_conn();
    // regexp_matches returns bool; DuckDB represents it as true/false
    let duckdb_sql = papera::transpile(
        "SELECT regexp_like('hello123', '[0-9]+')",
        SourceDialect::Trino,
    )
    .unwrap();
    let result: bool = conn.query_row(&duckdb_sql, [], |r| r.get(0)).unwrap();
    assert!(result);
}

#[test]
fn trino_regexp_replace() {
    let conn = new_conn();
    let result = query_scalar(
        &conn,
        "SELECT regexp_replace('hello 123 world', '[0-9]+', 'NUM')",
        SourceDialect::Trino,
    );
    assert_eq!(result, "hello NUM world");
}

#[test]
fn trino_array_functions() {
    let conn = new_conn();

    let result = query_scalar_i64(&conn, "SELECT array_max([3, 1, 2])", SourceDialect::Trino);
    assert_eq!(result, 3);

    let result = query_scalar_i64(&conn, "SELECT array_min([3, 1, 2])", SourceDialect::Trino);
    assert_eq!(result, 1);

    let result = query_scalar_i64(
        &conn,
        "SELECT array_position([10, 20, 30], 20)",
        SourceDialect::Trino,
    );
    assert_eq!(result, 2);
}

#[test]
fn trino_flatten() {
    let conn = new_conn();
    let duckdb_sql = papera::transpile(
        "SELECT CAST(flatten([[1, 2], [3, 4]]) AS VARCHAR)",
        SourceDialect::Trino,
    )
    .unwrap();
    let result: String = conn.query_row(&duckdb_sql, [], |r| r.get(0)).unwrap();
    assert_eq!(result, "[1, 2, 3, 4]");
}

#[test]
fn trino_map_keys_values() {
    let conn = new_conn();
    let duckdb_sql = papera::transpile(
        "SELECT CAST(map_keys(MAP {'a': 1, 'b': 2}) AS VARCHAR)",
        SourceDialect::Trino,
    )
    .unwrap();
    let result: String = conn.query_row(&duckdb_sql, [], |r| r.get(0)).unwrap();
    assert!(
        result.contains('a') && result.contains('b'),
        "Got: {result}"
    );
}

#[test]
fn trino_json_extract_scalar() {
    let conn = new_conn();
    let result = query_scalar(
        &conn,
        r#"SELECT json_extract_scalar('{"name":"alice"}', '$.name')"#,
        SourceDialect::Trino,
    );
    assert_eq!(result, "alice");
}

#[test]
fn trino_from_unixtime() {
    let conn = new_conn();
    // from_unixtime(0) → to_timestamp(0) → 1970-01-01 00:00:00
    let result = query_scalar(
        &conn,
        "SELECT CAST(from_unixtime(0) AS VARCHAR)",
        SourceDialect::Trino,
    );
    assert!(result.contains("1970-01-01"), "Got: {result}");
}

#[test]
fn trino_date_parse() {
    let conn = new_conn();
    let result = query_scalar(
        &conn,
        "SELECT CAST(date_parse('2024-03-15', '%Y-%m-%d') AS VARCHAR)",
        SourceDialect::Trino,
    );
    assert!(result.contains("2024-03-15"), "Got: {result}");
}

#[test]
fn trino_format_datetime() {
    let conn = new_conn();
    let result = query_scalar(
        &conn,
        "SELECT format_datetime(DATE '2024-03-15', '%Y/%m/%d')",
        SourceDialect::Trino,
    );
    assert_eq!(result, "2024/03/15");
}

// ===========================================================================
// Type casts — executed in DuckDB
// ===========================================================================

#[test]
fn redshift_cast_varchar_max() {
    let conn = new_conn();
    let result = query_scalar(
        &conn,
        "SELECT CAST(42 AS VARCHAR(MAX))",
        SourceDialect::Redshift,
    );
    assert_eq!(result, "42");
}

#[test]
fn redshift_cast_super() {
    let conn = new_conn();
    let result = query_scalar(
        &conn,
        r#"SELECT CAST('{"a":1}' AS SUPER)"#,
        SourceDialect::Redshift,
    );
    assert!(result.contains("a"), "Got: {result}");
}

// ===========================================================================
// DML — INSERT, UPDATE, DELETE
// ===========================================================================

#[test]
fn redshift_full_dml_cycle() {
    let conn = new_conn();
    exec(
        &conn,
        "CREATE TABLE t (id INTEGER, name VARCHAR(MAX), status INTEGER)",
        SourceDialect::Redshift,
    );

    exec(
        &conn,
        "INSERT INTO t VALUES (1, 'alice', 1), (2, 'bob', 2), (3, 'charlie', 1)",
        SourceDialect::Redshift,
    );

    // UPDATE with NVL
    exec(
        &conn,
        "UPDATE t SET name = NVL(name, 'unknown') WHERE id = 1",
        SourceDialect::Redshift,
    );

    // SELECT with DECODE
    let result = query_col(
        &conn,
        "SELECT DECODE(status, 1, 'active', 2, 'inactive', 'unknown') FROM t ORDER BY id",
        SourceDialect::Redshift,
    );
    assert_eq!(result, vec!["active", "inactive", "active"]);

    // DELETE
    exec(&conn, "DELETE FROM t WHERE id = 2", SourceDialect::Redshift);
    let count = query_scalar_i64(&conn, "SELECT COUNT(*) FROM t", SourceDialect::Redshift);
    assert_eq!(count, 2);
}

#[test]
fn trino_insert_with_function_rewrites() {
    let conn = new_conn();
    conn.execute_batch(
        "CREATE TABLE src (val VARCHAR); INSERT INTO src VALUES ('hello'), ('world')",
    )
    .unwrap();
    conn.execute_batch("CREATE TABLE dst (val VARCHAR, len BIGINT)")
        .unwrap();

    exec(
        &conn,
        "INSERT INTO dst SELECT val, length(val) FROM src",
        SourceDialect::Trino,
    );

    let lengths = query_i64(
        &conn,
        "SELECT len FROM dst ORDER BY len",
        SourceDialect::Trino,
    );
    assert_eq!(lengths, vec![5, 5]);
}

// ===========================================================================
// UNNEST
// ===========================================================================

#[test]
fn trino_unnest_array() {
    let conn = new_conn();
    conn.execute_batch(
        "CREATE TABLE t (id INT, arr INT[]); INSERT INTO t VALUES (1, [10, 20, 30])",
    )
    .unwrap();

    let result = query_i64(
        &conn,
        "SELECT u.x FROM t CROSS JOIN UNNEST(arr) AS u(x) WHERE t.id = 1 ORDER BY u.x",
        SourceDialect::Trino,
    );
    assert_eq!(result, vec![10, 20, 30]);
}

#[test]
fn trino_unnest_with_ordinality() {
    let conn = new_conn();
    conn.execute_batch(
        "CREATE TABLE t (id INT, arr INT[]); INSERT INTO t VALUES (1, [10, 20, 30])",
    )
    .unwrap();

    let duckdb_sql = papera::transpile(
        "SELECT u.x, u.n FROM t CROSS JOIN UNNEST(arr) WITH ORDINALITY AS u(x, n) WHERE t.id = 1 ORDER BY u.n",
        SourceDialect::Trino,
    ).unwrap();

    let mut stmt = conn.prepare(&duckdb_sql).unwrap();
    let rows: Vec<(i32, i64)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(rows, vec![(10, 1), (20, 2), (30, 3)]);
}

// ===========================================================================
// SHOW commands
// ===========================================================================

#[test]
fn show_tables_works() {
    let conn = new_conn();
    conn.execute_batch("CREATE TABLE foo (id INT); CREATE TABLE bar (id INT)")
        .unwrap();
    // SHOW TABLES passes through — just verify it doesn't error
    let duckdb_sql = papera::transpile("SHOW TABLES", SourceDialect::Trino).unwrap();
    let mut stmt = conn.prepare(&duckdb_sql).unwrap();
    let names: Vec<String> = stmt
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert!(names.contains(&"foo".to_string()));
    assert!(names.contains(&"bar".to_string()));
}

#[test]
fn show_create_table_emulated() {
    let conn = new_conn();
    conn.execute_batch("CREATE TABLE my_table (id INTEGER, name VARCHAR, age INTEGER)")
        .unwrap();

    let result = query_scalar(&conn, "SHOW CREATE TABLE my_table", SourceDialect::Trino);
    // The emulated DDL should contain the table name and column info
    assert!(result.contains("my_table"), "Got: {result}");
    assert!(result.contains("id"), "Got: {result}");
    assert!(result.contains("name"), "Got: {result}");
}

#[test]
fn show_create_view_emulated() {
    let conn = new_conn();
    conn.execute_batch("CREATE TABLE base (id INT, val VARCHAR)")
        .unwrap();
    conn.execute_batch("CREATE VIEW my_view AS SELECT id, val FROM base WHERE id > 0")
        .unwrap();

    let result = query_scalar(&conn, "SHOW CREATE VIEW my_view", SourceDialect::Trino);
    assert!(result.contains("my_view"), "Got: {result}");
}

// ===========================================================================
// Complex / realistic queries
// ===========================================================================

#[test]
fn redshift_realistic_analytics() {
    let conn = new_conn();
    conn.execute_batch(
        r#"
        CREATE TABLE events (
            user_id INTEGER,
            event_type VARCHAR,
            payload VARCHAR,
            created_at TIMESTAMP
        );
        INSERT INTO events VALUES
            (1, 'click', '{"page":"home"}', '2024-01-15 10:00:00'),
            (1, 'click', '{"page":"about"}', '2024-01-15 11:00:00'),
            (2, 'view',  '{"page":"home"}', '2024-01-15 12:00:00'),
            (2, 'click', '{"page":"home"}', '2024-01-15 13:00:00'),
            (3, 'view',  '{"page":"about"}', '2024-01-15 14:00:00');
    "#,
    )
    .unwrap();

    // Redshift-style analytics query with function rewrites
    let result = query_col(
        &conn,
        r#"SELECT
            DECODE(event_type, 'click', 'Click', 'view', 'View', 'Other') AS event_label,
            LEN(payload) AS payload_len,
            JSON_EXTRACT_PATH_TEXT(payload, 'page') AS page
        FROM events
        WHERE user_id = 1
        ORDER BY created_at"#,
        SourceDialect::Redshift,
    );
    assert_eq!(result, vec!["Click", "Click"]);
}

#[test]
fn trino_realistic_aggregation() {
    let conn = new_conn();
    conn.execute_batch(
        r#"
        CREATE TABLE sales (
            product VARCHAR,
            region VARCHAR,
            amount DOUBLE
        );
        INSERT INTO sales VALUES
            ('A', 'US', 100.0), ('A', 'EU', 200.0),
            ('B', 'US', 150.0), ('B', 'EU', 250.0),
            ('A', 'US', 300.0);
    "#,
    )
    .unwrap();

    // Trino-style query using arbitrary (→ any_value) and length
    let result = query_col(
        &conn,
        "SELECT arbitrary(region) FROM sales GROUP BY product ORDER BY product",
        SourceDialect::Trino,
    );
    assert_eq!(result.len(), 2); // one per product
}

#[test]
fn mixed_function_and_type_rewrite() {
    let conn = new_conn();
    // Combines CAST type rewrite (VARCHAR(MAX)) with function rewrite (NVL, LEN)
    let result = query_scalar_i64(
        &conn,
        "SELECT LEN(CAST(NVL(NULL, 'hello world') AS VARCHAR(MAX)))",
        SourceDialect::Redshift,
    );
    assert_eq!(result, 11);
}

// ===========================================================================
// Hive dialect
// ===========================================================================

#[test]
fn hive_simple_select() {
    let conn = new_conn();
    conn.execute_batch(
        "CREATE TABLE t (a INT, b VARCHAR); INSERT INTO t VALUES (1, 'x'), (2, 'y')",
    )
    .unwrap();
    let result = query_i64(&conn, "SELECT a FROM t ORDER BY a", SourceDialect::Hive);
    assert_eq!(result, vec![1, 2]);
}

// ===========================================================================
// Edge cases
// ===========================================================================

#[test]
fn passthrough_standard_sql() {
    let conn = new_conn();
    conn.execute_batch("CREATE TABLE t (a INT, b INT); INSERT INTO t VALUES (1, 2), (3, 4)")
        .unwrap();

    // Standard SQL should pass through all dialects unchanged and work
    for dialect in [
        SourceDialect::Trino,
        SourceDialect::Redshift,
        SourceDialect::Hive,
    ] {
        let result = query_i64(&conn, "SELECT a + b FROM t ORDER BY a", dialect);
        assert_eq!(result, vec![3, 7], "Failed for {dialect:?}");
    }
}

#[test]
fn multiple_statements_execute() {
    let conn = new_conn();
    // Transpile two statements and verify both execute
    let sql = "CREATE TABLE t1 (id INT); CREATE TABLE t2 (id INT)";
    let duckdb_sql = papera::transpile(sql, SourceDialect::Trino).unwrap();
    for stmt in duckdb_sql.split(";\n") {
        conn.execute_batch(stmt).unwrap();
    }
    // Verify both tables exist
    let tables: Vec<String> = {
        let mut s = conn.prepare("SHOW TABLES").unwrap();
        s.query_map([], |r| r.get(0))
            .unwrap()
            .map(|r| r.unwrap())
            .collect()
    };
    assert!(tables.contains(&"t1".to_string()));
    assert!(tables.contains(&"t2".to_string()));
}

#[test]
fn nested_function_rewrites() {
    let conn = new_conn();
    // NVL inside LEN inside DECODE — multiple nested rewrites
    let result = query_scalar(
        &conn,
        "SELECT DECODE(LEN(NVL('abc', '')), 3, 'three', 'other')",
        SourceDialect::Redshift,
    );
    assert_eq!(result, "three");
}

#[test]
fn subquery_function_rewrites() {
    let conn = new_conn();
    conn.execute_batch(
        "CREATE TABLE t (val VARCHAR); INSERT INTO t VALUES ('hello'), (NULL), ('world')",
    )
    .unwrap();

    let result = query_i64(
        &conn,
        "SELECT COUNT(*) FROM (SELECT NVL(val, 'default') AS v FROM t) sub WHERE LEN(v) > 0",
        SourceDialect::Redshift,
    );
    assert_eq!(result, vec![3]);
}
