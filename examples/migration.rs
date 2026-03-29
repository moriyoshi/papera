//! Demonstrates using TranspileOptions for migration scenarios.
//!
//! Run with: cargo run --example migration

use papera::{
    CopyBehavior, ExternalTableBehavior, IcebergTableBehavior, SourceDialect, TranspileOptions,
    transpile_with_options,
};

fn main() {
    // Migration-friendly options: opt into all conversions
    let opts = TranspileOptions {
        external_table: ExternalTableBehavior::MapToView,
        iceberg_table: IcebergTableBehavior::MapToView,
        copy: CopyBehavior::MapToInsert,
        ..Default::default()
    };

    let examples: &[(&str, SourceDialect, &str)] = &[
        // External table → DuckDB view with read_parquet
        (
            "CREATE EXTERNAL TABLE events (
                id BIGINT,
                ts TIMESTAMP,
                payload STRING
            )
            PARTITIONED BY (dt STRING)
            STORED AS PARQUET
            LOCATION 's3://data-lake/events/'",
            SourceDialect::Hive,
            "Hive external table with partitioning",
        ),
        // Hive external table with CSV and row format
        (
            "CREATE EXTERNAL TABLE logs (
                ts STRING,
                level STRING,
                message STRING
            )
            ROW FORMAT DELIMITED
                FIELDS TERMINATED BY ','
                LINES TERMINATED BY '\\n'
            STORED AS TEXTFILE
            LOCATION 's3://data-lake/logs/'",
            SourceDialect::Hive,
            "Hive CSV external table with delimiters",
        ),
        // Hive external table with SerDe
        (
            "CREATE EXTERNAL TABLE json_events (
                id INT,
                data STRING
            )
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
            LOCATION 's3://data-lake/json/'",
            SourceDialect::Hive,
            "Hive external table with JSON SerDe",
        ),
        // Iceberg table (Athena-style)
        (
            "CREATE TABLE analytics (
                user_id BIGINT,
                event VARCHAR,
                ts TIMESTAMP
            )
            LOCATION 's3://warehouse/analytics/'
            TBLPROPERTIES ('table_type'='ICEBERG')",
            SourceDialect::Trino,
            "Iceberg table (Athena TBLPROPERTIES)",
        ),
        // Iceberg table (Trino-style)
        (
            "CREATE TABLE metrics (
                name VARCHAR,
                value DOUBLE,
                ts TIMESTAMP
            )
            WITH (
                table_type = 'ICEBERG',
                location = 's3://warehouse/metrics/',
                format = 'PARQUET'
            )",
            SourceDialect::Trino,
            "Iceberg table (Trino WITH clause)",
        ),
        // Redshift COPY
        (
            "COPY staging_orders FROM 's3://etl-bucket/orders/' IAM_ROLE 'arn:aws:iam::123456789:role/redshift-role' PARQUET",
            SourceDialect::Redshift,
            "Redshift COPY (Parquet from S3)",
        ),
        // Redshift COPY with CSV options
        (
            "COPY staging_users FROM 's3://etl-bucket/users.csv.gz' IAM_ROLE 'arn:aws:iam::123456789:role/redshift-role' CSV DELIMITER ',' IGNOREHEADER 1 GZIP",
            SourceDialect::Redshift,
            "Redshift COPY (gzipped CSV with header)",
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
                println!("Error: {e}");
            }
        }
        println!();
    }
}
