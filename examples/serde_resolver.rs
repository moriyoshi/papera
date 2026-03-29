//! Demonstrates supplying a custom SerDe class resolver via TranspileOptions.
//!
//! The built-in resolver recognises common Hive SerDe classes by substring
//! matching (JsonSerDe, ParquetHiveSerDe, OpenCSVSerde, etc.).  For
//! proprietary or uncommon classes the built-in resolver returns an error.
//! `SerdeClassResolver` lets callers provide their own mapping logic that
//! runs first and falls through to the built-in logic when it returns `None`.
//!
//! Run with: cargo run --example serde_resolver

use papera::{
    ExternalTableBehavior, SerdeClassResolver, SourceDialect, TranspileOptions,
    transpile_with_options,
};

fn main() {
    // A resolver that maps two proprietary SerDe classes used in a hypothetical
    // data platform, then returns None for everything else so the built-in
    // rules still apply to standard classes.
    let resolver = SerdeClassResolver::new(|class| match class {
        c if c.eq_ignore_ascii_case("com.example.platform.ParquetSerDe") => {
            Some("read_parquet".to_string())
        }
        c if c.eq_ignore_ascii_case("com.example.platform.JsonSerDe") => {
            Some("read_json".to_string())
        }
        _ => None,
    });

    let opts = TranspileOptions {
        external_table: ExternalTableBehavior::MapToView,
        serde_class_resolver: Some(resolver),
        ..Default::default()
    };

    let examples: &[(&str, SourceDialect, &str)] = &[
        // Proprietary Parquet SerDe — handled by the custom resolver
        (
            "CREATE EXTERNAL TABLE orders (
                id      BIGINT,
                amount  DOUBLE,
                dt      STRING
            )
            ROW FORMAT SERDE 'com.example.platform.ParquetSerDe'
            LOCATION 's3://data-lake/orders/'",
            SourceDialect::Hive,
            "Proprietary Parquet SerDe (custom resolver)",
        ),
        // Proprietary JSON SerDe — handled by the custom resolver
        (
            "CREATE EXTERNAL TABLE events (
                id      INT,
                payload STRING
            )
            ROW FORMAT SERDE 'com.example.platform.JsonSerDe'
            LOCATION 's3://data-lake/events/'",
            SourceDialect::Hive,
            "Proprietary JSON SerDe (custom resolver)",
        ),
        // Standard JsonSerDe — custom resolver returns None, built-in takes over
        (
            "CREATE EXTERNAL TABLE metrics (
                name  STRING,
                value DOUBLE
            )
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
            LOCATION 's3://data-lake/metrics/'",
            SourceDialect::Hive,
            "Standard JsonSerDe (built-in resolver fallthrough)",
        ),
        // Unknown SerDe — custom resolver returns None, built-in returns an error
        (
            "CREATE EXTERNAL TABLE legacy (
                col STRING
            )
            ROW FORMAT SERDE 'com.example.legacy.OldSerDe'
            LOCATION 's3://data-lake/legacy/'",
            SourceDialect::Hive,
            "Unknown SerDe (error after fallthrough)",
        ),
    ];

    for (sql, dialect, label) in examples {
        println!("--- {label} ---");
        match transpile_with_options(sql, *dialect, &opts) {
            Ok(result) => {
                println!("Input ({dialect:?}):");
                for line in sql.lines() {
                    println!("  {}", line.trim());
                }
                println!("Output (DuckDB):");
                println!("  {result}");
            }
            Err(e) => {
                println!("Input ({dialect:?}):");
                for line in sql.lines() {
                    println!("  {}", line.trim());
                }
                println!("Error: {e}");
            }
        }
        println!();
    }
}
