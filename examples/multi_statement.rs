//! Demonstrates transpiling multi-statement SQL scripts.
//!
//! Run with: cargo run --example multi_statement

use papera::{
    CopyBehavior, ExternalTableBehavior, SourceDialect, TranspileOptions, transpile_with_options,
};

fn main() {
    let opts = TranspileOptions {
        external_table: ExternalTableBehavior::MapToView,
        copy: CopyBehavior::MapToInsert,
        ..Default::default()
    };

    // A typical Redshift ETL script
    let redshift_script = "\
        CREATE TABLE staging (id INTEGER, name VARCHAR(MAX), data SUPER, created TIMESTAMP);\
        INSERT INTO staging SELECT id, name, data, created FROM raw_events WHERE created > getdate();\
        SELECT NVL(name, 'anonymous'), DATEDIFF(day, created, getdate()) AS age_days FROM staging";

    println!("=== Redshift ETL Script ===\n");
    match transpile_with_options(redshift_script, SourceDialect::Redshift, &opts) {
        Ok(result) => {
            for (i, stmt) in result.split(";\n").enumerate() {
                println!("Statement {}:", i + 1);
                println!("  {stmt}");
                println!();
            }
        }
        Err(e) => println!("Error: {e}"),
    }

    // A Trino analytics query
    let trino_script = "\
        SELECT \
            date_trunc('day', event_time) AS day, \
            approx_distinct(user_id) AS unique_users, \
            array_join(array_agg(DISTINCT category), ', ') AS categories, \
            json_extract_scalar(metadata, '$.source') AS source \
        FROM events \
        WHERE event_time > date_add('day', -7, current_timestamp) \
        GROUP BY 1, 4";

    println!("=== Trino Analytics Query ===\n");
    match transpile_with_options(trino_script, SourceDialect::Trino, &opts) {
        Ok(result) => {
            println!("  {result}");
        }
        Err(e) => println!("Error: {e}"),
    }
}
