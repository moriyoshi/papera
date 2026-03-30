mod common;

use papera::dialect::{SourceDialect, TargetDialect};
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

// ---------------------------------------------------------------------------
// DataFusion target tests
// ---------------------------------------------------------------------------

fn datafusion_opts() -> TranspileOptions {
    TranspileOptions {
        target: TargetDialect::DataFusion,
        ..Default::default()
    }
}

#[test]
fn trino_to_datafusion_passthrough_functions() {
    // Functions that share the same name in Trino and DataFusion should not be renamed
    let sql = "SELECT approx_distinct(user_id), cardinality(arr), array_join(arr, ',') FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(
        result.contains("approx_distinct"),
        "approx_distinct should pass through: {result}"
    );
    assert!(
        !result.contains("approx_count_distinct"),
        "should not rename to DuckDB name: {result}"
    );
    assert!(
        result.contains("cardinality"),
        "cardinality should pass through: {result}"
    );
    assert!(
        !result.contains("len("),
        "should not rename to DuckDB len: {result}"
    );
    assert!(
        result.contains("array_join"),
        "array_join should pass through: {result}"
    );
    assert!(
        !result.contains("array_to_string"),
        "should not rename to DuckDB array_to_string: {result}"
    );
}

#[test]
fn trino_to_datafusion_array_functions() {
    // filter is not supported for DataFusion target
    let filter_err = papera::transpile_with_options(
        "SELECT filter(arr, x -> x > 0) FROM t",
        SourceDialect::Trino,
        &datafusion_opts(),
    );
    assert!(
        filter_err.is_err(),
        "filter should be Unsupported for DataFusion target"
    );

    // transform and contains are supported
    let sql = "SELECT transform(arr, x -> x * 2), contains(arr, 1) FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(
        result.contains("array_transform"),
        "transform → array_transform: {result}"
    );
    assert!(
        result.contains("array_has"),
        "contains → array_has: {result}"
    );
    assert!(
        !result.contains("list_filter"),
        "no DuckDB list_filter: {result}"
    );
    assert!(
        !result.contains("list_transform"),
        "no DuckDB list_transform: {result}"
    );
    assert!(
        !result.contains("list_contains"),
        "no DuckDB list_contains: {result}"
    );
}

#[test]
fn trino_to_datafusion_more_array_functions() {
    let sql = "SELECT element_at(arr, 1), slice(arr, 1, 3), array_distinct(arr) FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(
        result.contains("array_element"),
        "element_at → array_element: {result}"
    );
    assert!(
        result.contains("array_slice"),
        "slice → array_slice: {result}"
    );
    assert!(
        result.contains("array_distinct"),
        "array_distinct passes through: {result}"
    );
    assert!(
        !result.contains("list_extract"),
        "no DuckDB list_extract: {result}"
    );
    assert!(
        !result.contains("list_slice"),
        "no DuckDB list_slice: {result}"
    );
    assert!(
        !result.contains("list_distinct"),
        "no DuckDB list_distinct: {result}"
    );
}

#[test]
fn trino_to_datafusion_type_varbinary() {
    let sql = "CREATE TABLE t (a VARBINARY, b INTEGER)";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(result.contains("BYTEA"), "VARBINARY → BYTEA: {result}");
    assert!(!result.contains("BLOB"), "no DuckDB BLOB: {result}");
}

#[test]
fn trino_to_datafusion_cast_varbinary() {
    let sql = "SELECT CAST(col AS VARBINARY) FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(result.contains("BYTEA"), "CAST VARBINARY → BYTEA: {result}");
}

#[test]
fn trino_to_datafusion_array_union_simple_rename() {
    // For DataFusion, array_union is available natively — no complex rewrite needed
    let sql = "SELECT array_union(a, b) FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(
        result.contains("array_union"),
        "array_union passes through: {result}"
    );
    assert!(
        !result.contains("list_distinct"),
        "no DuckDB list_distinct wrapping: {result}"
    );
}

#[test]
fn trino_to_datafusion_approx_percentile() {
    let sql = "SELECT approx_percentile(col, 0.95) FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(
        result.contains("approx_percentile_cont"),
        "approx_percentile → approx_percentile_cont: {result}"
    );
    assert!(
        !result.contains("approx_quantile"),
        "no DuckDB approx_quantile: {result}"
    );
}

#[test]
fn trino_to_datafusion_regexp_functions() {
    let sql = "SELECT regexp_like(s, 'a+'), split(s, ',') FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(
        result.contains("regexp_like"),
        "regexp_like passes through: {result}"
    );
    assert!(
        !result.contains("regexp_matches"),
        "no DuckDB regexp_matches: {result}"
    );
    assert!(
        result.contains("string_to_array"),
        "split → string_to_array: {result}"
    );
    assert!(
        !result.contains("str_split"),
        "no DuckDB str_split: {result}"
    );
}

#[test]
fn trino_to_datafusion_format_datetime() {
    let sql = "SELECT format_datetime(ts, 'yyyy-MM-dd') FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(
        result.contains("to_char"),
        "format_datetime → to_char: {result}"
    );
    assert!(!result.contains("strftime"), "no DuckDB strftime: {result}");
}

#[test]
fn trino_to_datafusion_date_parse() {
    let sql = "SELECT date_parse(s, 'yyyy-MM-dd') FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(
        result.contains("to_timestamp"),
        "date_parse → to_timestamp: {result}"
    );
    assert!(!result.contains("strptime"), "no DuckDB strptime: {result}");
}

#[test]
fn trino_to_datafusion_typeof() {
    let sql = "SELECT typeof(col) FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(
        result.contains("arrow_typeof"),
        "typeof → arrow_typeof: {result}"
    );
}

#[test]
fn hive_to_datafusion_array_functions() {
    // Hive shares mapping with Trino; verify DataFusion target works
    let sql = "SELECT array_sort(arr), array_max(arr) FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Hive, &datafusion_opts()).unwrap();
    assert!(
        result.contains("array_sort"),
        "array_sort passes through: {result}"
    );
    assert!(
        result.contains("array_max"),
        "array_max passes through: {result}"
    );
    assert!(
        !result.contains("list_sort"),
        "no DuckDB list_sort: {result}"
    );
    assert!(!result.contains("list_max"), "no DuckDB list_max: {result}");
}

#[test]
fn redshift_to_datafusion_basic_functions() {
    let sql = "SELECT NVL(a, b), LEN(s), CHARINDEX('@', email) FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Redshift, &datafusion_opts()).unwrap();
    assert!(result.contains("coalesce"), "NVL → coalesce: {result}");
    assert!(result.contains("length"), "LEN → length: {result}");
    assert!(result.contains("strpos"), "CHARINDEX → strpos: {result}");
}

#[test]
fn redshift_to_datafusion_type_super() {
    let sql = "CREATE TABLE t (data SUPER, name VARCHAR(MAX))";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Redshift, &datafusion_opts()).unwrap();
    assert!(
        !result.contains("JSON"),
        "SUPER should not become JSON for DataFusion: {result}"
    );
    assert!(
        result.contains("VARCHAR"),
        "SUPER → VARCHAR for DataFusion: {result}"
    );
}

#[test]
fn redshift_to_datafusion_array_concat() {
    // For DataFusion, array_concat maps to array_concat (not list_concat)
    let sql = "SELECT array_concat(a, b) FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Redshift, &datafusion_opts()).unwrap();
    assert!(
        result.contains("array_concat"),
        "array_concat passes through for DataFusion: {result}"
    );
    assert!(
        !result.contains("list_concat"),
        "no DuckDB list_concat: {result}"
    );
}

#[test]
fn duckdb_target_backward_compatible() {
    // Default target is DuckDB — existing behavior unchanged
    let sql = "SELECT approx_distinct(x), contains(arr, 1) FROM t";
    let result = papera::transpile(sql, SourceDialect::Trino).unwrap();
    assert!(
        result.contains("approx_count_distinct"),
        "DuckDB default: approx_count_distinct: {result}"
    );
    assert!(
        result.contains("list_contains"),
        "DuckDB default: list_contains: {result}"
    );
}

#[test]
fn datafusion_show_create_table_error() {
    let sql = "SHOW CREATE TABLE t";
    let result = papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts());
    assert!(
        result.is_err(),
        "SHOW CREATE TABLE should error for DataFusion target"
    );
}

#[test]
fn datafusion_url_extract_host_uses_regexp_match() {
    let sql = "SELECT url_extract_host(url) FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(
        result.contains("regexp_match"),
        "url_extract_host for DataFusion should use regexp_match, got: {result}"
    );
    assert!(
        !result.contains("regexp_extract"),
        "url_extract_host for DataFusion must not emit regexp_extract (DuckDB name), got: {result}"
    );
}

#[test]
fn datafusion_to_utf8_unsupported() {
    let sql = "SELECT to_utf8(s) FROM t";
    let result = papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts());
    assert!(
        result.is_err(),
        "to_utf8 should be Unsupported for DataFusion target"
    );
}

#[test]
fn datafusion_from_utf8_unsupported() {
    let sql = "SELECT from_utf8(b) FROM t";
    let result = papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts());
    assert!(
        result.is_err(),
        "from_utf8 should be Unsupported for DataFusion target"
    );
}

#[test]
fn datafusion_map_agg_unsupported() {
    // map_agg is not available in DataFusion 52
    let sql = "SELECT map_agg(k, v) FROM t";
    let result = papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts());
    assert!(
        result.is_err(),
        "map_agg should be Unsupported for DataFusion target"
    );
}

#[test]
fn datafusion_json_object_keys_unsupported() {
    let sql = "SELECT json_object_keys(j) FROM t";
    let result = papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts());
    assert!(
        result.is_err(),
        "json_object_keys should be Unsupported for DataFusion target"
    );
}

// ---------------------------------------------------------------------------
// Trino → DataFusion: date/time functions
// ---------------------------------------------------------------------------

#[test]
fn trino_to_datafusion_from_unixtime() {
    let sql = "SELECT from_unixtime(ts) FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(
        result.contains("to_timestamp"),
        "from_unixtime → to_timestamp: {result}"
    );
}

#[test]
fn trino_to_datafusion_at_timezone() {
    let sql = "SELECT at_timezone(ts, 'UTC') FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(
        result.contains("AT TIME ZONE"),
        "at_timezone → AT TIME ZONE: {result}"
    );
}

#[test]
fn trino_to_datafusion_current_timezone_unsupported() {
    let sql = "SELECT current_timezone() FROM t";
    let result = papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts());
    assert!(
        result.is_err(),
        "current_timezone() should be Unsupported for DataFusion target"
    );
}

#[test]
fn trino_to_datafusion_date_diff_and_add() {
    // date_diff('day', d1, d2) → epoch arithmetic via to_unixtime
    let result = papera::transpile_with_options(
        "SELECT date_diff('day', d1, d2) FROM t",
        SourceDialect::Trino,
        &datafusion_opts(),
    )
    .unwrap();
    assert!(
        result.contains("to_unixtime"),
        "date_diff('day') → epoch arithmetic: {result}"
    );
    assert!(
        result.contains("86400"),
        "date_diff('day') divides by 86400: {result}"
    );

    // date_diff('month', ...) → date_part arithmetic
    let month_result = papera::transpile_with_options(
        "SELECT date_diff('month', d1, d2) FROM t",
        SourceDialect::Trino,
        &datafusion_opts(),
    )
    .unwrap();
    assert!(
        month_result.contains("date_part"),
        "date_diff('month') → date_part arithmetic: {month_result}"
    );

    // date_add passes through
    let add_result = papera::transpile_with_options(
        "SELECT date_add('month', 1, d) FROM t",
        SourceDialect::Trino,
        &datafusion_opts(),
    )
    .unwrap();
    assert!(
        add_result.contains("date_add"),
        "date_add passthrough: {add_result}"
    );
}

#[test]
fn trino_to_datafusion_day_of_week_and_year() {
    let sql = "SELECT day_of_week(d), day_of_year(d) FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    // day_of_week → date_part('dow', d) for DataFusion
    assert!(
        result.contains("date_part") && result.contains("'dow'"),
        "day_of_week → date_part('dow', ...): {result}"
    );
    // day_of_year → date_part('doy', d) for DataFusion
    assert!(
        result.contains("date_part") && result.contains("'doy'"),
        "day_of_year → date_part('doy', ...): {result}"
    );
}

#[test]
fn trino_to_datafusion_week_of_year() {
    let sql = "SELECT week_of_year(d) FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(
        result.contains("date_part"),
        "week_of_year → date_part: {result}"
    );
    assert!(
        result.contains("'week'"),
        "week_of_year: date_part('week', ...): {result}"
    );
}

#[test]
fn trino_to_datafusion_year_of_week() {
    let sql = "SELECT year_of_week(d) FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(
        result.contains("date_part"),
        "year_of_week → date_part: {result}"
    );
    assert!(
        result.contains("date_trunc"),
        "year_of_week uses date_trunc: {result}"
    );
    assert!(
        result.contains("'year'"),
        "year_of_week extracts year: {result}"
    );
}

// ---------------------------------------------------------------------------
// Trino → DataFusion: string functions
// ---------------------------------------------------------------------------

#[test]
fn trino_to_datafusion_string_functions() {
    let sql = "SELECT levenshtein_distance(a, b), strpos(s, 'x'), length(s), reverse(s) FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(
        result.contains("levenshtein"),
        "levenshtein_distance → levenshtein: {result}"
    );
    assert!(result.contains("strpos"), "strpos passthrough: {result}");
    assert!(result.contains("length"), "length passthrough: {result}");
    assert!(result.contains("reverse"), "reverse passthrough: {result}");
}

#[test]
fn trino_to_datafusion_pad_functions() {
    let sql = "SELECT lpad(s, 5, '0'), rpad(s, 5, ' '), chr(65), codepoint('A') FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(result.contains("lpad"), "lpad passthrough: {result}");
    assert!(result.contains("rpad"), "rpad passthrough: {result}");
    assert!(result.contains("chr"), "chr passthrough: {result}");
    assert!(result.contains("ascii"), "codepoint → ascii: {result}");
}

#[test]
fn trino_to_datafusion_regexp_extract() {
    let sql = "SELECT regexp_extract(s, '[0-9]+') FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(
        result.contains("regexp_match"),
        "regexp_extract → regexp_match: {result}"
    );
    assert!(
        !result.contains("regexp_extract"),
        "must not keep regexp_extract: {result}"
    );
}

#[test]
fn trino_to_datafusion_regexp_replace() {
    let sql = "SELECT regexp_replace(s, '[0-9]+', 'N') FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(
        result.contains("regexp_replace"),
        "regexp_replace passthrough: {result}"
    );
}

// ---------------------------------------------------------------------------
// Trino → DataFusion: additional array functions
// ---------------------------------------------------------------------------

#[test]
fn trino_to_datafusion_array_sequence_and_zip() {
    let sql = "SELECT sequence(1, 10), zip(a, b) FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(
        result.contains("generate_series"),
        "sequence → generate_series: {result}"
    );
    assert!(result.contains("zip"), "zip passthrough: {result}");
}

#[test]
fn trino_to_datafusion_array_set_operations() {
    let sql = "SELECT array_intersect(a, b), array_except(a, b), array_concat(a, b) FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(
        result.contains("array_intersect"),
        "array_intersect passthrough: {result}"
    );
    assert!(
        result.contains("array_except"),
        "array_except passthrough: {result}"
    );
    assert!(
        result.contains("array_concat"),
        "array_concat passthrough: {result}"
    );
}

#[test]
fn trino_to_datafusion_arrays_overlap() {
    let sql = "SELECT arrays_overlap(a, b) FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(
        result.contains("array_intersect"),
        "arrays_overlap uses array_intersect: {result}"
    );
    assert!(
        result.contains("array_length"),
        "arrays_overlap uses array_length: {result}"
    );
    assert!(
        result.contains("> 0"),
        "arrays_overlap checks > 0: {result}"
    );
}

#[test]
fn trino_to_datafusion_array_has_all_and_any() {
    let r_all = papera::transpile_with_options(
        "SELECT array_has_all(a, b) FROM t",
        SourceDialect::Trino,
        &datafusion_opts(),
    )
    .unwrap();
    assert!(
        r_all.contains("array_intersect"),
        "array_has_all uses array_intersect: {r_all}"
    );
    assert!(
        r_all.contains("array_length"),
        "array_has_all uses array_length: {r_all}"
    );

    let r_any = papera::transpile_with_options(
        "SELECT array_has_any(a, b) FROM t",
        SourceDialect::Trino,
        &datafusion_opts(),
    )
    .unwrap();
    assert!(
        r_any.contains("array_intersect"),
        "array_has_any uses array_intersect: {r_any}"
    );
    assert!(r_any.contains("> 0"), "array_has_any checks > 0: {r_any}");
}

#[test]
fn trino_to_datafusion_array_aggregates() {
    let sql = "SELECT array_sum(a), array_average(a), array_has(a, 1) FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(
        result.contains("array_sum"),
        "array_sum passthrough: {result}"
    );
    assert!(
        result.contains("array_avg"),
        "array_average → array_avg: {result}"
    );
    assert!(
        result.contains("array_has"),
        "array_has passthrough: {result}"
    );
}

#[test]
fn trino_to_datafusion_array_position_and_remove() {
    let sql = "SELECT array_position(arr, 3), flatten(arr) FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(
        result.contains("array_position"),
        "array_position passthrough: {result}"
    );
    assert!(result.contains("flatten"), "flatten passthrough: {result}");
}

// ---------------------------------------------------------------------------
// Trino → DataFusion: URL extraction remaining functions
// ---------------------------------------------------------------------------

#[test]
fn trino_to_datafusion_url_extraction_remaining() {
    let cases = [
        ("SELECT url_extract_path(url) FROM t", "regexp_match"),
        ("SELECT url_extract_protocol(url) FROM t", "regexp_match"),
        ("SELECT url_extract_query(url) FROM t", "regexp_match"),
        ("SELECT url_extract_fragment(url) FROM t", "regexp_match"),
        ("SELECT url_extract_port(url) FROM t", "regexp_match"),
    ];
    for (sql, expected) in &cases {
        let result =
            papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
        assert!(
            result.contains(expected),
            "{sql}: expected {expected} in: {result}"
        );
    }
}

// ---------------------------------------------------------------------------
// Trino → DataFusion: JSON functions
// ---------------------------------------------------------------------------

#[test]
fn trino_to_datafusion_json_functions() {
    // json_extract_scalar is not supported for DataFusion target
    let scalar_err = papera::transpile_with_options(
        "SELECT json_extract_scalar(j, '$.k') FROM t",
        SourceDialect::Trino,
        &datafusion_opts(),
    );
    assert!(
        scalar_err.is_err(),
        "json_extract_scalar should be Unsupported for DataFusion target"
    );

    // json_extract is also not supported for DataFusion target
    let extract_err = papera::transpile_with_options(
        "SELECT json_extract(j, '$.k') FROM t",
        SourceDialect::Trino,
        &datafusion_opts(),
    );
    assert!(
        extract_err.is_err(),
        "json_extract should be Unsupported for DataFusion target"
    );

    // json_array_length passes through
    let result = papera::transpile_with_options(
        "SELECT json_array_length(j) FROM t",
        SourceDialect::Trino,
        &datafusion_opts(),
    )
    .unwrap();
    assert!(
        result.contains("json_array_length"),
        "json_array_length passthrough: {result}"
    );
}

#[test]
fn trino_to_datafusion_json_parse_and_format() {
    // json_parse is not supported for DataFusion target (no JSON type)
    let parse_err = papera::transpile_with_options(
        "SELECT json_parse(s) FROM t",
        SourceDialect::Trino,
        &datafusion_opts(),
    );
    assert!(
        parse_err.is_err(),
        "json_parse should be Unsupported for DataFusion target"
    );

    // json_format still casts to VARCHAR (passthrough)
    let format_result = papera::transpile_with_options(
        "SELECT json_format(j) FROM t",
        SourceDialect::Trino,
        &datafusion_opts(),
    )
    .unwrap();
    assert!(
        format_result.contains("CAST"),
        "json_format → CAST: {format_result}"
    );
    assert!(
        format_result.contains("VARCHAR"),
        "json_format → CAST AS VARCHAR: {format_result}"
    );
}

#[test]
fn trino_to_datafusion_json_array_get() {
    // json_array_get requires json_extract_scalar which is not available in DataFusion 52
    let result = papera::transpile_with_options(
        "SELECT json_array_get(j, 2) FROM t",
        SourceDialect::Trino,
        &datafusion_opts(),
    );
    assert!(
        result.is_err(),
        "json_array_get should be Unsupported for DataFusion target"
    );
}

// ---------------------------------------------------------------------------
// Trino → DataFusion: math functions
// ---------------------------------------------------------------------------

#[test]
fn trino_to_datafusion_math_functions() {
    // is_nan passes through as isnan
    let result = papera::transpile_with_options(
        "SELECT is_nan(x) FROM t",
        SourceDialect::Trino,
        &datafusion_opts(),
    )
    .unwrap();
    assert!(result.contains("isnan"), "is_nan → isnan: {result}");

    // is_finite and is_infinite are not available in DataFusion 52
    let finite_err = papera::transpile_with_options(
        "SELECT is_finite(x) FROM t",
        SourceDialect::Trino,
        &datafusion_opts(),
    );
    assert!(
        finite_err.is_err(),
        "is_finite should be Unsupported for DataFusion target"
    );

    let infinite_err = papera::transpile_with_options(
        "SELECT is_infinite(x) FROM t",
        SourceDialect::Trino,
        &datafusion_opts(),
    );
    assert!(
        infinite_err.is_err(),
        "is_infinite should be Unsupported for DataFusion target"
    );
}

#[test]
fn trino_to_datafusion_misc_functions() {
    let sql = "SELECT from_hex(h), rand() FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(
        result.contains("from_hex"),
        "from_hex passthrough: {result}"
    );
    assert!(result.contains("random"), "rand → random: {result}");
}

#[test]
fn trino_to_datafusion_map_functions() {
    let sql = "SELECT map_keys(m), map_values(m) FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(
        result.contains("map_keys"),
        "map_keys passthrough: {result}"
    );
    assert!(
        result.contains("map_values"),
        "map_values passthrough: {result}"
    );
}

#[test]
fn trino_to_datafusion_aggregate_arbitrary() {
    // arbitrary (any_value) is not available in DataFusion 52
    let result = papera::transpile_with_options(
        "SELECT arbitrary(col) FROM t",
        SourceDialect::Trino,
        &datafusion_opts(),
    );
    assert!(
        result.is_err(),
        "arbitrary should be Unsupported for DataFusion target"
    );
}

// ---------------------------------------------------------------------------
// Trino → DataFusion: bitwise functions
// ---------------------------------------------------------------------------

#[test]
fn trino_to_datafusion_bitwise_functions() {
    let cases = [
        ("SELECT bitwise_and(a, b) FROM t", "&"),
        ("SELECT bitwise_or(a, b) FROM t", "|"),
        ("SELECT bitwise_xor(a, b) FROM t", "^"),
    ];
    for (sql, expected_op) in &cases {
        let result =
            papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
        assert!(
            result.contains(expected_op),
            "{sql}: expected op '{expected_op}' in: {result}"
        );
    }
}

#[test]
fn trino_to_datafusion_bitwise_shifts() {
    let left = papera::transpile_with_options(
        "SELECT bitwise_left_shift(a, 2) FROM t",
        SourceDialect::Trino,
        &datafusion_opts(),
    )
    .unwrap();
    assert!(left.contains("<<"), "bitwise_left_shift → <<: {left}");

    let right = papera::transpile_with_options(
        "SELECT bitwise_right_shift(a, 2) FROM t",
        SourceDialect::Trino,
        &datafusion_opts(),
    )
    .unwrap();
    assert!(right.contains(">>"), "bitwise_right_shift → >>: {right}");
}

// ---------------------------------------------------------------------------
// Trino → DataFusion: ROW / ARRAY type mappings
// ---------------------------------------------------------------------------

#[test]
fn trino_to_datafusion_row_type() {
    let sql = "CREATE TABLE t (r ROW(a INTEGER, b VARCHAR))";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(
        result.contains("STRUCT<"),
        "ROW → STRUCT<...> for DataFusion: {result}"
    );
    assert!(
        !result.contains("STRUCT("),
        "ROW must not become STRUCT(...) (DuckDB syntax): {result}"
    );
}

#[test]
fn trino_to_datafusion_array_type() {
    // Trino T[] syntax (square bracket form) → ARRAY<T> for DataFusion
    let sql = "CREATE TABLE t (a INTEGER[])";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Trino, &datafusion_opts()).unwrap();
    assert!(
        result.contains("ARRAY<"),
        "T[] → ARRAY<T> for DataFusion: {result}"
    );
    assert!(
        !result.contains("[]"),
        "must not keep T[] syntax for DataFusion: {result}"
    );
}

// ---------------------------------------------------------------------------
// Redshift → DataFusion: date/time functions
// ---------------------------------------------------------------------------

#[test]
fn redshift_to_datafusion_getdate_sysdate() {
    let sql = "SELECT getdate(), sysdate() FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Redshift, &datafusion_opts()).unwrap();
    assert_eq!(
        result.matches("now()").count(),
        2,
        "getdate/sysdate → now(): {result}"
    );
}

#[test]
fn redshift_to_datafusion_dateadd() {
    let sql = "SELECT dateadd(day, 7, d) FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Redshift, &datafusion_opts()).unwrap();
    assert!(
        result.contains("INTERVAL"),
        "dateadd → date + INTERVAL: {result}"
    );
}

#[test]
fn redshift_to_datafusion_datediff() {
    // datediff('day', ...) → epoch arithmetic via to_unixtime
    let result = papera::transpile_with_options(
        "SELECT datediff(day, d1, d2) FROM t",
        SourceDialect::Redshift,
        &datafusion_opts(),
    )
    .unwrap();
    assert!(
        result.contains("to_unixtime"),
        "datediff('day') → epoch arithmetic: {result}"
    );

    // datediff('month', ...) → date_part arithmetic
    let month_result = papera::transpile_with_options(
        "SELECT datediff(month, d1, d2) FROM t",
        SourceDialect::Redshift,
        &datafusion_opts(),
    )
    .unwrap();
    assert!(
        month_result.contains("date_part"),
        "datediff('month') → date_part arithmetic: {month_result}"
    );
}

#[test]
fn redshift_to_datafusion_months_between() {
    // months_between → date_part('year',...) and date_part('month',...) arithmetic
    let result = papera::transpile_with_options(
        "SELECT months_between(d1, d2) FROM t",
        SourceDialect::Redshift,
        &datafusion_opts(),
    )
    .unwrap();
    assert!(
        result.contains("date_part"),
        "months_between → date_part arithmetic: {result}"
    );
    assert!(
        result.contains("'month'"),
        "months_between uses 'month' datepart: {result}"
    );
}

#[test]
fn redshift_to_datafusion_add_months() {
    let sql = "SELECT add_months(d, 3) FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Redshift, &datafusion_opts()).unwrap();
    assert!(
        result.contains("INTERVAL"),
        "add_months → date + INTERVAL: {result}"
    );
}

#[test]
fn redshift_to_datafusion_convert_timezone() {
    let sql = "SELECT convert_timezone('US/Eastern', ts) FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Redshift, &datafusion_opts()).unwrap();
    assert!(
        result.contains("AT TIME ZONE"),
        "convert_timezone → AT TIME ZONE: {result}"
    );
}

// ---------------------------------------------------------------------------
// Redshift → DataFusion: string functions
// ---------------------------------------------------------------------------

#[test]
fn redshift_to_datafusion_string_misc() {
    let sql = "SELECT btrim(s), space(5), regexp_substr(s, '[0-9]+') FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Redshift, &datafusion_opts()).unwrap();
    assert!(result.contains("trim"), "btrim → trim: {result}");
    assert!(result.contains("repeat"), "space → repeat: {result}");
    assert!(
        result.contains("regexp_match"),
        "regexp_substr → regexp_match: {result}"
    );
}

#[test]
fn redshift_to_datafusion_nvl2_and_isnull() {
    let nvl2 = papera::transpile_with_options(
        "SELECT nvl2(a, b, c) FROM t",
        SourceDialect::Redshift,
        &datafusion_opts(),
    )
    .unwrap();
    assert!(nvl2.contains("CASE"), "nvl2 → CASE: {nvl2}");

    let isnull = papera::transpile_with_options(
        "SELECT isnull(a, b) FROM t",
        SourceDialect::Redshift,
        &datafusion_opts(),
    )
    .unwrap();
    assert!(isnull.contains("coalesce"), "isnull → coalesce: {isnull}");
}

#[test]
fn redshift_to_datafusion_listagg() {
    let sql = "SELECT listagg(col, ',') FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Redshift, &datafusion_opts()).unwrap();
    assert!(
        result.contains("string_agg"),
        "listagg → string_agg: {result}"
    );
}

// ---------------------------------------------------------------------------
// Redshift → DataFusion: hash functions
// ---------------------------------------------------------------------------

#[test]
fn redshift_to_datafusion_hash_functions() {
    let sql = "SELECT md5(s), sha1(s), sha2(s, 256) FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Redshift, &datafusion_opts()).unwrap();
    assert!(result.contains("md5"), "md5 passthrough: {result}");
    assert!(result.contains("sha1"), "sha1 passthrough: {result}");
    assert!(result.contains("sha256"), "sha2(s, 256) → sha256: {result}");
}

// ---------------------------------------------------------------------------
// Redshift → DataFusion: JSON functions
// ---------------------------------------------------------------------------

#[test]
fn redshift_to_datafusion_json_functions() {
    let sql = "SELECT json_typeof(j), json_array_length(j), is_valid_json(j) FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Redshift, &datafusion_opts()).unwrap();
    assert!(
        result.contains("json_typeof"),
        "json_typeof passthrough: {result}"
    );
    assert!(
        result.contains("json_array_length"),
        "json_array_length passthrough: {result}"
    );
    assert!(
        result.contains("is_valid_json"),
        "is_valid_json passthrough: {result}"
    );
}

#[test]
fn redshift_to_datafusion_json_extract_path_text() {
    // json_extract_path_text is not supported for DataFusion target
    let result = papera::transpile_with_options(
        "SELECT json_extract_path_text(j, 'key') FROM t",
        SourceDialect::Redshift,
        &datafusion_opts(),
    );
    assert!(
        result.is_err(),
        "json_extract_path_text should be Unsupported for DataFusion target"
    );
}

// ---------------------------------------------------------------------------
// Redshift → DataFusion: other functions
// ---------------------------------------------------------------------------

#[test]
fn redshift_to_datafusion_strtol() {
    // strtol is not supported for DataFusion target (hex string casting not available)
    let result = papera::transpile_with_options(
        "SELECT strtol('ff', 16) FROM t",
        SourceDialect::Redshift,
        &datafusion_opts(),
    );
    assert!(
        result.is_err(),
        "strtol should be Unsupported for DataFusion target"
    );
}

#[test]
fn redshift_to_datafusion_decode() {
    let sql = "SELECT decode(status, 1, 'active', 'inactive') FROM t";
    let result =
        papera::transpile_with_options(sql, SourceDialect::Redshift, &datafusion_opts()).unwrap();
    assert!(result.contains("CASE"), "decode → CASE: {result}");
}
