//! Basic transpilation examples.
//!
//! Run with: cargo run --example basic

use papera::{SourceDialect, transpile};

fn main() {
    let examples: &[(&str, SourceDialect, &str)] = &[
        // Trino function rewrites
        (
            "SELECT approx_distinct(user_id) FROM events",
            SourceDialect::Trino,
            "Trino approx_distinct",
        ),
        (
            "SELECT date_parse(ts, 'yyyy-MM-dd HH:mm:ss') FROM logs",
            SourceDialect::Trino,
            "Trino date_parse with Java format",
        ),
        (
            "SELECT bitwise_and(flags, 0xFF) FROM t",
            SourceDialect::Trino,
            "Trino bitwise function to operator",
        ),
        (
            "SELECT json_extract_scalar(payload, '$.user.name') FROM events",
            SourceDialect::Trino,
            "Trino JSON extraction",
        ),
        (
            "SELECT array_union(tags_a, tags_b) FROM items",
            SourceDialect::Trino,
            "Trino array_union (structural rewrite)",
        ),
        // Redshift function rewrites
        (
            "SELECT NVL2(email, 'has_email', 'no_email') FROM users",
            SourceDialect::Redshift,
            "Redshift NVL2 to CASE",
        ),
        (
            "SELECT DECODE(status, 1, 'active', 2, 'paused', 'unknown') FROM accounts",
            SourceDialect::Redshift,
            "Redshift DECODE to CASE",
        ),
        (
            "SELECT TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI:SS') FROM orders",
            SourceDialect::Redshift,
            "Redshift TO_CHAR with PG format",
        ),
        (
            "SELECT DATEADD(month, 3, start_date) FROM subscriptions",
            SourceDialect::Redshift,
            "Redshift DATEADD to interval",
        ),
        (
            "SELECT CONVERT_TIMEZONE('UTC', 'US/Eastern', event_time) FROM events",
            SourceDialect::Redshift,
            "Redshift CONVERT_TIMEZONE",
        ),
        // Hive lateral view
        (
            "SELECT t.id, x.val FROM my_table t LATERAL VIEW explode(arr) x AS val",
            SourceDialect::Hive,
            "Hive LATERAL VIEW explode",
        ),
        // Type rewrites in CAST
        (
            "SELECT CAST(data AS VARBINARY) FROM t",
            SourceDialect::Trino,
            "Trino VARBINARY to BLOB",
        ),
        (
            "SELECT CAST(payload AS SUPER) FROM t",
            SourceDialect::Redshift,
            "Redshift SUPER to JSON",
        ),
    ];

    for (sql, dialect, label) in examples {
        match transpile(sql, *dialect) {
            Ok(result) => {
                println!("[{dialect:?}] {label}");
                println!("  IN:  {sql}");
                println!("  OUT: {result}");
                println!();
            }
            Err(e) => {
                println!("[{dialect:?}] {label}");
                println!("  IN:  {sql}");
                println!("  ERR: {e}");
                println!();
            }
        }
    }
}
