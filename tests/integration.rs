mod common;

use papera::dialect::SourceDialect;
use papera::{
    CopyBehavior, ExternalTableBehavior, IcebergTableBehavior, SerdeClassResolver, TranspileOptions,
};

#[test]
fn redshift_etl_script() {
    // A realistic Redshift ETL pipeline with multiple statement types
    let create = "CREATE TABLE staging (id INTEGER, data SUPER, name VARCHAR(MAX))";
    let result = papera::transpile(create, SourceDialect::Redshift).unwrap();
    assert!(result.contains("JSON"), "SUPER → JSON: {result}");
    assert!(!result.contains("MAX"), "VARCHAR(MAX) → VARCHAR: {result}");

    let insert = "INSERT INTO staging SELECT id, NVL(data, '{}'), UPPER(name) FROM source";
    let result = papera::transpile(insert, SourceDialect::Redshift).unwrap();
    assert!(result.contains("coalesce"), "NVL → coalesce: {result}");

    let update = "UPDATE staging SET name = DECODE(status, 1, 'active', 2, 'inactive', 'unknown') WHERE id > 0";
    let result = papera::transpile(update, SourceDialect::Redshift).unwrap();
    assert!(result.contains("CASE"), "DECODE → CASE: {result}");
}

#[test]
fn trino_analytics_query() {
    // A Trino analytics query with function rewrites and type casts
    let sql = "SELECT approx_distinct(user_id), json_extract_scalar(metadata, '$.type'), CAST(ts AS VARBINARY) FROM events WHERE date_parse(event_date, '%Y-%m-%d') > CURRENT_DATE";
    let result = papera::transpile(sql, SourceDialect::Trino).unwrap();
    assert!(
        result.contains("approx_count_distinct"),
        "approx_distinct rewrite: {result}"
    );
    assert!(
        result.contains("json_extract_string"),
        "json_extract_scalar rewrite: {result}"
    );
    assert!(result.contains("BLOB"), "VARBINARY → BLOB: {result}");
    assert!(
        result.contains("strptime"),
        "date_parse → strptime: {result}"
    );
}

#[test]
fn redshift_json_functions() {
    let sql = "SELECT JSON_EXTRACT_PATH_TEXT(data, 'user', 'email'), NVL2(phone, 'has_phone', 'no_phone') FROM users";
    let result = papera::transpile(sql, SourceDialect::Redshift).unwrap();
    assert!(
        result.contains("json_extract_string"),
        "JSON_EXTRACT_PATH_TEXT rewrite: {result}"
    );
    assert!(
        result.contains("$.user.email"),
        "JSON path construction: {result}"
    );
    assert!(result.contains("CASE WHEN"), "NVL2 → CASE: {result}");
}

#[test]
fn trino_create_table_with_complex_types() {
    let sql = "CREATE TABLE t (a VARBINARY, b INTEGER)";
    let result = papera::transpile(sql, SourceDialect::Trino).unwrap();
    assert!(result.contains("BLOB"), "VARBINARY → BLOB: {result}");
}

#[test]
fn redshift_timezone_conversion() {
    let sql = "SELECT CONVERT_TIMEZONE('UTC', 'US/Pacific', created_at), CHARINDEX('@', email), LEN(name) FROM users";
    let result = papera::transpile(sql, SourceDialect::Redshift).unwrap();
    assert!(
        result.contains("AT TIME ZONE"),
        "CONVERT_TIMEZONE: {result}"
    );
    assert!(result.contains("strpos"), "CHARINDEX → strpos: {result}");
    assert!(result.contains("length"), "LEN → length: {result}");
}

#[test]
fn multiple_statements() {
    let sql = "SELECT 1; SELECT 2";
    let result = papera::transpile(sql, SourceDialect::Trino).unwrap();
    assert_eq!(result, "SELECT 1;\nSELECT 2");
}

#[test]
fn redshift_cast_varchar_max() {
    let sql = "SELECT CAST(col AS VARCHAR(MAX)) FROM t";
    let result = papera::transpile(sql, SourceDialect::Redshift).unwrap();
    assert!(
        result.contains("VARCHAR") && !result.contains("MAX"),
        "Got: {result}"
    );
}

#[test]
fn trino_show_tables() {
    let sql = "SHOW TABLES";
    let result = papera::transpile(sql, SourceDialect::Trino).unwrap();
    assert_eq!(result, "SHOW TABLES");
}

#[test]
fn trino_unnest() {
    let sql = "SELECT t.x FROM my_table CROSS JOIN UNNEST(arr) AS t(x)";
    let result = papera::transpile(sql, SourceDialect::Trino).unwrap();
    assert!(result.contains("UNNEST"), "UNNEST preserved: {result}");
}

#[test]
fn trino_hive_style_external_table() {
    // Trino also supports Hive-style DDL — PARTITIONED BY, STORED AS, ROW FORMAT, LOCATION
    let sql = "CREATE EXTERNAL TABLE events (id INT, payload VARCHAR) PARTITIONED BY (dt VARCHAR) STORED AS PARQUET LOCATION 's3://bucket/events/'";
    let opts = TranspileOptions {
        external_table: ExternalTableBehavior::MapToView,
        ..Default::default()
    };
    let result = papera::transpile_with_options(sql, SourceDialect::Trino, &opts).unwrap();
    assert!(
        result.contains("CREATE VIEW"),
        "Expected CREATE VIEW: {result}"
    );
    assert!(
        result.contains("read_parquet"),
        "Expected read_parquet: {result}"
    );
    assert!(
        result.contains("hive_partitioning"),
        "Expected hive_partitioning: {result}"
    );
}

#[test]
fn trino_hive_style_serde_custom_resolver() {
    // Trino Hive-style DDL with ROW FORMAT SERDE and a custom resolver
    let sql = "CREATE EXTERNAL TABLE events (id INT) ROW FORMAT SERDE 'com.example.MySerDe' LOCATION 's3://bucket/events/'";
    let opts = TranspileOptions {
        external_table: ExternalTableBehavior::MapToView,
        serde_class_resolver: Some(SerdeClassResolver::new(|class| {
            if class.eq_ignore_ascii_case("com.example.MySerDe") {
                Some("read_json".to_string())
            } else {
                None
            }
        })),
        ..Default::default()
    };
    let result = papera::transpile_with_options(sql, SourceDialect::Trino, &opts).unwrap();
    assert!(result.contains("read_json"), "Expected read_json: {result}");
}

#[test]
fn trino_iceberg_table_via_tblproperties() {
    let sql = "CREATE TABLE iceberg_t (id INT, name VARCHAR) LOCATION 's3://bucket/iceberg/' TBLPROPERTIES ('table_type'='ICEBERG')";
    let opts = TranspileOptions {
        iceberg_table: IcebergTableBehavior::MapToView,
        ..Default::default()
    };
    let result = papera::transpile_with_options(sql, SourceDialect::Trino, &opts).unwrap();
    assert!(
        result.contains("iceberg_scan"),
        "Expected iceberg_scan: {result}"
    );
}

#[test]
fn hive_full_csv_pipeline() {
    let sql = "CREATE EXTERNAL TABLE logs (ts STRING, msg STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' STORED AS TEXTFILE LOCATION 's3://bucket/logs/'";
    let opts = TranspileOptions {
        external_table: ExternalTableBehavior::MapToView,
        ..Default::default()
    };
    let result = papera::transpile_with_options(sql, SourceDialect::Hive, &opts).unwrap();
    assert!(result.contains("read_csv"), "Expected read_csv: {result}");
    assert!(result.contains("delim"), "Expected delim option: {result}");
}

// ===========================================================================
// Redshift DATEADD / DATEDIFF
// ===========================================================================

#[test]
fn redshift_dateadd_to_interval() {
    let result = papera::transpile(
        "SELECT DATEADD(month, 3, created_at) FROM t",
        SourceDialect::Redshift,
    )
    .unwrap();
    assert!(
        result.contains("INTERVAL '3' MONTH"),
        "Expected interval: {result}"
    );
    assert!(result.contains("+"), "Expected + operator: {result}");
}

#[test]
fn redshift_dateadd_day() {
    let result = papera::transpile(
        "SELECT DATEADD(day, 7, order_date) FROM t",
        SourceDialect::Redshift,
    )
    .unwrap();
    assert!(
        result.contains("INTERVAL '7' DAY"),
        "Expected interval: {result}"
    );
}

#[test]
fn redshift_datediff_quoted_part() {
    let result = papera::transpile(
        "SELECT DATEDIFF(day, start_date, end_date) FROM t",
        SourceDialect::Redshift,
    )
    .unwrap();
    assert!(
        result.contains("date_diff('day'"),
        "Expected quoted part: {result}"
    );
}

#[test]
fn redshift_datediff_combined_with_getdate() {
    let result = papera::transpile(
        "SELECT DATEDIFF(year, hire_date, GETDATE()) FROM t",
        SourceDialect::Redshift,
    )
    .unwrap();
    assert!(
        result.contains("date_diff('year'"),
        "Expected quoted part: {result}"
    );
    assert!(
        result.contains("current_timestamp"),
        "Expected getdate rewrite: {result}"
    );
}

// ===========================================================================
// Format string conversion
// ===========================================================================

#[test]
fn trino_format_datetime_java_format() {
    let result = papera::transpile(
        "SELECT format_datetime(ts, 'yyyy-MM-dd HH:mm:ss') FROM t",
        SourceDialect::Trino,
    )
    .unwrap();
    assert!(result.contains("strftime"), "Expected strftime: {result}");
    assert!(
        result.contains("%Y-%m-%d %H:%M:%S"),
        "Expected converted: {result}"
    );
}

#[test]
fn trino_date_parse_java_format() {
    let result = papera::transpile(
        "SELECT date_parse(s, 'yyyy/MM/dd') FROM t",
        SourceDialect::Trino,
    )
    .unwrap();
    assert!(result.contains("strptime"), "Expected strptime: {result}");
    assert!(result.contains("%Y/%m/%d"), "Expected converted: {result}");
}

#[test]
fn trino_format_preserves_strftime() {
    let result = papera::transpile(
        "SELECT format_datetime(ts, '%Y-%m-%d') FROM t",
        SourceDialect::Trino,
    )
    .unwrap();
    assert!(
        result.contains("%Y-%m-%d"),
        "Format should be preserved: {result}"
    );
}

#[test]
fn trino_format_month_names() {
    let result = papera::transpile(
        "SELECT format_datetime(ts, 'dd MMM yyyy') FROM t",
        SourceDialect::Trino,
    )
    .unwrap();
    assert!(
        result.contains("%d %b %Y"),
        "Expected abbrev month: {result}"
    );
}

#[test]
fn redshift_to_char_pg_format() {
    let result = papera::transpile(
        "SELECT TO_CHAR(ts, 'YYYY-MM-DD HH24:MI:SS') FROM t",
        SourceDialect::Redshift,
    )
    .unwrap();
    assert!(result.contains("strftime"), "Expected strftime: {result}");
    assert!(
        result.contains("%Y-%m-%d %H:%M:%S"),
        "Expected converted: {result}"
    );
}

#[test]
fn redshift_to_char_12h_ampm() {
    let result = papera::transpile(
        "SELECT TO_CHAR(ts, 'HH12:MI:SS AM') FROM t",
        SourceDialect::Redshift,
    )
    .unwrap();
    assert!(
        result.contains("%I:%M:%S %p"),
        "Expected 12h format: {result}"
    );
}

#[test]
fn redshift_to_date_pg_format() {
    let result = papera::transpile(
        "SELECT TO_DATE('2024-01-15', 'YYYY-MM-DD') FROM t",
        SourceDialect::Redshift,
    )
    .unwrap();
    assert!(result.contains("strptime"), "Expected strptime: {result}");
    assert!(result.contains("%Y-%m-%d"), "Expected converted: {result}");
    assert!(result.contains("CAST"), "Expected CAST: {result}");
}

#[test]
fn redshift_to_timestamp_pg_format() {
    let result = papera::transpile(
        "SELECT TO_TIMESTAMP(s, 'YYYY-MM-DD HH24:MI:SS') FROM t",
        SourceDialect::Redshift,
    )
    .unwrap();
    assert!(result.contains("strptime"), "Expected strptime: {result}");
    assert!(
        result.contains("%Y-%m-%d %H:%M:%S"),
        "Expected converted: {result}"
    );
}

// ===========================================================================
// ROW FORMAT SERDE
// ===========================================================================

#[test]
fn hive_serde_json() {
    let sql = "CREATE EXTERNAL TABLE t (id INT, data STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe' LOCATION 's3://bucket/json/'";
    let opts = TranspileOptions {
        external_table: ExternalTableBehavior::MapToView,
        ..Default::default()
    };
    let result = papera::transpile_with_options(sql, SourceDialect::Hive, &opts).unwrap();
    assert!(result.contains("read_json"), "Expected read_json: {result}");
}

#[test]
fn hive_serde_parquet() {
    let sql = "CREATE EXTERNAL TABLE t (id INT) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' LOCATION 's3://bucket/pq/'";
    let opts = TranspileOptions {
        external_table: ExternalTableBehavior::MapToView,
        ..Default::default()
    };
    let result = papera::transpile_with_options(sql, SourceDialect::Hive, &opts).unwrap();
    assert!(
        result.contains("read_parquet"),
        "Expected read_parquet: {result}"
    );
}

#[test]
fn hive_serde_opencsv() {
    let sql = "CREATE EXTERNAL TABLE t (id INT) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' LOCATION 's3://bucket/csv/'";
    let opts = TranspileOptions {
        external_table: ExternalTableBehavior::MapToView,
        ..Default::default()
    };
    let result = papera::transpile_with_options(sql, SourceDialect::Hive, &opts).unwrap();
    assert!(result.contains("read_csv"), "Expected read_csv: {result}");
}

#[test]
fn hive_serde_unknown_errors() {
    let sql = "CREATE EXTERNAL TABLE t (id INT) ROW FORMAT SERDE 'com.example.UnknownSerDe' LOCATION 's3://bucket/data/'";
    let opts = TranspileOptions {
        external_table: ExternalTableBehavior::MapToView,
        ..Default::default()
    };
    let result = papera::transpile_with_options(sql, SourceDialect::Hive, &opts);
    assert!(matches!(result, Err(papera::Error::Unsupported(_))));
}

#[test]
fn hive_serde_custom_resolver_overrides_unknown() {
    let sql = "CREATE EXTERNAL TABLE t (id INT) ROW FORMAT SERDE 'com.example.UnknownSerDe' LOCATION 's3://bucket/data/'";
    let opts = TranspileOptions {
        external_table: ExternalTableBehavior::MapToView,
        serde_class_resolver: Some(papera::SerdeClassResolver::new(|class| {
            if class.eq_ignore_ascii_case("com.example.UnknownSerDe") {
                Some("read_parquet".to_string())
            } else {
                None
            }
        })),
        ..Default::default()
    };
    let result = papera::transpile_with_options(sql, SourceDialect::Hive, &opts).unwrap();
    assert!(
        result.contains("read_parquet"),
        "Expected read_parquet: {result}"
    );
}

// ===========================================================================
// Redshift COPY
// ===========================================================================

#[test]
fn redshift_copy_default_errors() {
    let sql = "COPY my_table FROM 's3://bucket/data/' IAM_ROLE 'arn:aws:iam::123:role/r' PARQUET";
    let result = papera::transpile(sql, SourceDialect::Redshift);
    assert!(matches!(result, Err(papera::Error::Unsupported(_))));
}

#[test]
fn redshift_copy_parquet() {
    let sql = "COPY orders FROM 's3://bucket/orders/' IAM_ROLE 'arn:aws:iam::123:role/r' PARQUET";
    let opts = TranspileOptions {
        copy: CopyBehavior::MapToInsert,
        ..Default::default()
    };
    let result = papera::transpile_with_options(sql, SourceDialect::Redshift, &opts).unwrap();
    assert!(result.contains("INSERT INTO"), "Expected INSERT: {result}");
    assert!(
        result.contains("read_parquet"),
        "Expected read_parquet: {result}"
    );
    assert!(
        !result.contains("IAM_ROLE"),
        "IAM_ROLE should be dropped: {result}"
    );
}

#[test]
fn redshift_copy_csv_with_options() {
    let sql = "COPY users FROM 's3://bucket/users/' IAM_ROLE 'arn:aws:iam::123:role/r' CSV DELIMITER ',' IGNOREHEADER 1";
    let opts = TranspileOptions {
        copy: CopyBehavior::MapToInsert,
        ..Default::default()
    };
    let result = papera::transpile_with_options(sql, SourceDialect::Redshift, &opts).unwrap();
    assert!(result.contains("read_csv"), "Expected read_csv: {result}");
    assert!(
        result.contains("header = true"),
        "Expected header: {result}"
    );
}

#[test]
fn redshift_copy_json() {
    let sql =
        "COPY events FROM 's3://bucket/events/' IAM_ROLE 'arn:aws:iam::123:role/r' JSON 'auto'";
    let opts = TranspileOptions {
        copy: CopyBehavior::MapToInsert,
        ..Default::default()
    };
    let result = papera::transpile_with_options(sql, SourceDialect::Redshift, &opts).unwrap();
    assert!(result.contains("read_json"), "Expected read_json: {result}");
}

#[test]
fn redshift_copy_default_pipe_delimiter() {
    let sql = "COPY data FROM 's3://bucket/data/' IAM_ROLE 'arn:aws:iam::123:role/r'";
    let opts = TranspileOptions {
        copy: CopyBehavior::MapToInsert,
        ..Default::default()
    };
    let result = papera::transpile_with_options(sql, SourceDialect::Redshift, &opts).unwrap();
    assert!(result.contains("'|'"), "Expected pipe delimiter: {result}");
}

#[test]
fn redshift_copy_gzip() {
    let sql = "COPY data FROM 's3://bucket/data.gz' IAM_ROLE 'arn:aws:iam::123:role/r' GZIP DELIMITER ','";
    let opts = TranspileOptions {
        copy: CopyBehavior::MapToInsert,
        ..Default::default()
    };
    let result = papera::transpile_with_options(sql, SourceDialect::Redshift, &opts).unwrap();
    assert!(result.contains("gzip"), "Expected gzip: {result}");
}

#[test]
fn redshift_copy_with_columns() {
    let sql = "COPY my_table (a, b, c) FROM 's3://bucket/data/' IAM_ROLE 'arn:aws:iam::123:role/r' PARQUET";
    let opts = TranspileOptions {
        copy: CopyBehavior::MapToInsert,
        ..Default::default()
    };
    let result = papera::transpile_with_options(sql, SourceDialect::Redshift, &opts).unwrap();
    assert!(result.contains("a, b, c"), "Expected columns: {result}");
}

// ===========================================================================
// Combined / realistic scenarios
// ===========================================================================

#[test]
fn redshift_etl_with_format_and_dateadd() {
    let sql = "SELECT TO_CHAR(DATEADD(day, 30, order_date), 'YYYY-MM-DD') AS due, DATEDIFF(day, order_date, ship_date) AS days FROM orders";
    let result = papera::transpile(sql, SourceDialect::Redshift).unwrap();
    assert!(result.contains("strftime"), "strftime: {result}");
    assert!(result.contains("INTERVAL"), "INTERVAL: {result}");
    assert!(result.contains("date_diff"), "date_diff: {result}");
    assert!(result.contains("%Y-%m-%d"), "format converted: {result}");
}

#[test]
fn trino_format_parse_roundtrip() {
    let sql = "SELECT format_datetime(date_parse(s, 'yyyy-MM-dd'), 'dd/MM/yyyy') FROM t";
    let result = papera::transpile(sql, SourceDialect::Trino).unwrap();
    assert!(result.contains("strftime"), "strftime: {result}");
    assert!(result.contains("strptime"), "strptime: {result}");
    assert!(result.contains("%Y-%m-%d"), "parse format: {result}");
    assert!(result.contains("%d/%m/%Y"), "output format: {result}");
}

#[test]
fn hive_serde_with_partitioning() {
    let ddl = "CREATE EXTERNAL TABLE events (user_id INT, data STRING) PARTITIONED BY (dt STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe' LOCATION 's3://lake/events/'";
    let opts = TranspileOptions {
        external_table: ExternalTableBehavior::MapToView,
        ..Default::default()
    };
    let result = papera::transpile_with_options(ddl, SourceDialect::Hive, &opts).unwrap();
    assert!(result.contains("read_json"), "read_json: {result}");
    assert!(
        result.contains("hive_partitioning"),
        "partitioning: {result}"
    );
}

#[test]
fn redshift_copy_then_query() {
    let copy_sql = "COPY staging FROM 's3://etl/data/' IAM_ROLE 'arn:aws:iam::123:role/r' PARQUET";
    let opts = TranspileOptions {
        copy: CopyBehavior::MapToInsert,
        ..Default::default()
    };
    let copy_result =
        papera::transpile_with_options(copy_sql, SourceDialect::Redshift, &opts).unwrap();
    assert!(
        copy_result.contains("INSERT INTO staging"),
        "COPY: {copy_result}"
    );

    let query_sql = "SELECT NVL(name, 'unknown'), DATEDIFF(day, created, GETDATE()) FROM staging";
    let query_result = papera::transpile(query_sql, SourceDialect::Redshift).unwrap();
    assert!(query_result.contains("coalesce"), "NVL: {query_result}");
    assert!(
        query_result.contains("date_diff"),
        "DATEDIFF: {query_result}"
    );
}

#[test]
fn trino_bitwise_in_where() {
    let result = papera::transpile(
        "SELECT id FROM flags WHERE bitwise_and(permissions, 4) > 0",
        SourceDialect::Trino,
    )
    .unwrap();
    assert!(result.contains("&"), "Expected &: {result}");
    assert!(
        !result.contains("bitwise_and"),
        "Should not contain original: {result}"
    );
}

#[test]
fn redshift_nested_json_functions() {
    let sql =
        r#"SELECT JSON_EXTRACT_PATH_TEXT(JSON_EXTRACT_PATH_TEXT(data, 'user'), 'email') FROM t"#;
    let result = papera::transpile(sql, SourceDialect::Redshift).unwrap();
    assert_eq!(
        result.matches("json_extract_string").count(),
        2,
        "Two rewrites: {result}"
    );
}

#[test]
fn trino_json_parse_and_extract() {
    let sql = "SELECT json_extract_scalar(json_parse(raw_data), '$.name') FROM t";
    let result = papera::transpile(sql, SourceDialect::Trino).unwrap();
    assert!(
        result.contains("CAST(raw_data AS JSON)"),
        "json_parse: {result}"
    );
    assert!(
        result.contains("json_extract_string"),
        "json_extract_scalar: {result}"
    );
}

#[test]
fn redshift_space_and_sha2() {
    let result = papera::transpile(
        "SELECT SPACE(10), SHA2(name, 256) FROM t",
        SourceDialect::Redshift,
    )
    .unwrap();
    assert!(result.contains("repeat(' ', 10)"), "SPACE: {result}");
    assert!(result.contains("sha256"), "SHA2: {result}");
}

#[test]
fn redshift_isnull_lcase_ucase() {
    let result = papera::transpile(
        "SELECT ISNULL(name, 'N/A'), LCASE(cat), UCASE(brand) FROM t",
        SourceDialect::Redshift,
    )
    .unwrap();
    assert!(result.contains("coalesce"), "ISNULL: {result}");
    assert!(result.contains("lower"), "LCASE: {result}");
    assert!(result.contains("upper"), "UCASE: {result}");
}
