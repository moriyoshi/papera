use sqlparser::ast::{
    AlterTableOperation, CreateTableOptions, CreateView, Expr, FileFormat, HiveDelimiter,
    HiveDistributionStyle, HiveFormat, HiveIOFormat, HiveRowFormat, SqlOption, Statement, Value,
    ValueWithSpan, ViewColumnDef,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::dialect::SourceDialect;
use crate::transforms::types;
use crate::{ExternalTableBehavior, IcebergTableBehavior, Result, TranspileOptions};

/// Transpile DDL statements (CREATE TABLE, ALTER TABLE, etc.).
pub fn rewrite_ddl(
    mut stmt: Statement,
    dialect: SourceDialect,
    opts: &TranspileOptions,
) -> Result<Statement> {
    match &mut stmt {
        Statement::CreateTable(ct) => {
            // Check for iceberg FIRST (Athena iceberg tables are NOT external)
            if is_iceberg_table(ct) {
                return rewrite_iceberg_table(stmt, dialect, opts);
            }
            if ct.external {
                return rewrite_external_table(stmt, dialect, opts);
            }
            if is_trino_s3_table(ct) {
                return rewrite_trino_s3_table(stmt, dialect, opts);
            }
            for col in ct.columns.iter_mut() {
                types::rewrite_data_type(&mut col.data_type, dialect)?;
            }
        }
        Statement::AlterTable(alter) => {
            for op in alter.operations.iter_mut() {
                rewrite_alter_operation(op, dialect)?;
            }
        }
        _ => {}
    }
    Ok(stmt)
}

// ---------------------------------------------------------------------------
// Iceberg table detection and rewriting
// ---------------------------------------------------------------------------

/// Detect Trino-style S3-backed tables by checking WITH options for `external_location`.
///
/// Trino: `CREATE TABLE t (...) WITH (external_location = 's3://...', format = 'PARQUET')`
fn is_trino_s3_table(ct: &sqlparser::ast::CreateTable) -> bool {
    let opts = extract_table_options(&ct.table_options);
    get_property_value(opts, "external_location").is_some()
}

/// Detect iceberg tables by checking TBLPROPERTIES or WITH options for
/// `table_type = 'ICEBERG'`.
///
/// Athena: `TBLPROPERTIES ('table_type'='ICEBERG')`
/// Trino:  `WITH (table_type = 'ICEBERG', ...)`
fn is_iceberg_table(ct: &sqlparser::ast::CreateTable) -> bool {
    let opts = extract_table_options(&ct.table_options);
    has_property_value(opts, "table_type", "ICEBERG")
}

/// Extract the SqlOption slice from the unified CreateTableOptions enum.
fn extract_table_options(opts: &CreateTableOptions) -> &[SqlOption] {
    match opts {
        CreateTableOptions::With(v)
        | CreateTableOptions::Options(v)
        | CreateTableOptions::Plain(v)
        | CreateTableOptions::TableProperties(v) => v,
        CreateTableOptions::None => &[],
    }
}

/// Check if a list of SqlOption contains a key-value pair matching the given
/// key (case-insensitive) and value (case-insensitive).
fn has_property_value(options: &[SqlOption], key: &str, value: &str) -> bool {
    options.iter().any(|opt| {
        if let SqlOption::KeyValue { key: k, value: v } = opt {
            k.value.eq_ignore_ascii_case(key) && expr_string_eq(v, value)
        } else {
            false
        }
    })
}

/// Extract a string property value from SqlOption list.
fn get_property_value<'a>(options: &'a [SqlOption], key: &str) -> Option<&'a str> {
    options.iter().find_map(|opt| {
        if let SqlOption::KeyValue { key: k, value: v } = opt {
            if k.value.eq_ignore_ascii_case(key) {
                expr_as_str(v)
            } else {
                None
            }
        } else {
            None
        }
    })
}

fn expr_string_eq(expr: &Expr, target: &str) -> bool {
    expr_as_str(expr)
        .map(|s| s.eq_ignore_ascii_case(target))
        .unwrap_or(false)
}

fn expr_as_str(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(s),
            ..
        }) => Some(s.as_str()),
        Expr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(s),
            ..
        }) => Some(s.as_str()),
        Expr::Identifier(ident) => Some(ident.value.as_str()),
        _ => None,
    }
}

/// Rewrite a CREATE TABLE with iceberg table_type to a DuckDB-compatible form.
///
/// Athena syntax:
///   CREATE TABLE t (...) LOCATION 's3://...'
///     TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet')
///
/// Trino syntax:
///   CREATE TABLE t (...) WITH (table_type = 'ICEBERG', location = 's3://...', format = 'PARQUET')
///
/// DuckDB (with iceberg extension):
///   CREATE VIEW t AS SELECT * FROM iceberg_scan('s3://...')
fn rewrite_iceberg_table(
    stmt: Statement,
    dialect: SourceDialect,
    opts: &TranspileOptions,
) -> Result<Statement> {
    let ct = match stmt {
        Statement::CreateTable(ct) => ct,
        _ => unreachable!(),
    };

    match opts.iceberg_table {
        IcebergTableBehavior::Error => Err(crate::Error::Unsupported(format!(
            "Iceberg table {} is not supported. \
             Set IcebergTableBehavior::MapToView to convert to a DuckDB view using iceberg_scan().",
            ct.name,
        ))),
        IcebergTableBehavior::MapToView => iceberg_table_to_view(ct, dialect),
    }
}

fn iceberg_table_to_view(
    mut ct: sqlparser::ast::CreateTable,
    dialect: SourceDialect,
) -> Result<Statement> {
    // Extract location from:
    // 1. CreateTable.location (Athena: LOCATION 'path')
    // 2. TBLPROPERTIES('location' = 'path')
    // 3. WITH(location = 'path') (Trino)
    // 4. HiveFormat.location
    let location = ct
        .location
        .take()
        .or_else(|| {
            get_property_value(extract_table_options(&ct.table_options), "location")
                .map(|s| s.to_string())
        })
        .or_else(|| ct.hive_formats.as_ref().and_then(|h| h.location.clone()))
        .ok_or_else(|| {
            crate::Error::Unsupported(format!("Iceberg table {} has no LOCATION clause", ct.name,))
        })?;

    // Rewrite column types
    for col in ct.columns.iter_mut() {
        types::rewrite_data_type(&mut col.data_type, dialect)?;
    }

    let columns: Vec<ViewColumnDef> = ct
        .columns
        .iter()
        .map(|c| ViewColumnDef {
            name: c.name.clone(),
            data_type: None,
            options: None,
        })
        .collect();

    let escaped_location = location.replace('\'', "''");
    let select_sql = format!("SELECT * FROM iceberg_scan('{escaped_location}')");
    let query = Parser::parse_sql(&GenericDialect {}, &select_sql)
        .map_err(|e| crate::Error::Unsupported(format!("Failed to build view query: {e}")))?;

    let query = match query.into_iter().next() {
        Some(Statement::Query(q)) => q,
        _ => {
            return Err(crate::Error::Unsupported(
                "Failed to parse generated view query".to_string(),
            ));
        }
    };

    Ok(Statement::CreateView(CreateView {
        or_alter: false,
        or_replace: ct.or_replace,
        materialized: false,
        secure: false,
        name: ct.name,
        name_before_not_exists: false,
        columns,
        query,
        options: CreateTableOptions::None,
        cluster_by: vec![],
        comment: None,
        with_no_schema_binding: false,
        if_not_exists: ct.if_not_exists,
        temporary: false,
        to: None,
        params: None,
    }))
}

// ---------------------------------------------------------------------------
// External (non-iceberg) table rewriting
// ---------------------------------------------------------------------------

fn rewrite_external_table(
    stmt: Statement,
    dialect: SourceDialect,
    opts: &TranspileOptions,
) -> Result<Statement> {
    let ct = match stmt {
        Statement::CreateTable(ct) => ct,
        _ => unreachable!(),
    };

    // External tables with iceberg TBLPROPERTIES are handled by rewrite_iceberg_table
    // (called earlier in rewrite_ddl), so this only handles non-iceberg external tables.

    match opts.external_table {
        ExternalTableBehavior::Error => Err(crate::Error::Unsupported(format!(
            "CREATE EXTERNAL TABLE {} is not supported. \
             Set ExternalTableBehavior::MapToView to convert to a DuckDB view.",
            ct.name,
        ))),
        ExternalTableBehavior::MapToView => external_table_to_view(ct, dialect, opts),
    }
}

fn external_table_to_view(
    mut ct: sqlparser::ast::CreateTable,
    dialect: SourceDialect,
    opts: &TranspileOptions,
) -> Result<Statement> {
    let location = ct
        .location
        .take()
        .or_else(|| ct.hive_formats.as_ref().and_then(|h| h.location.clone()))
        .ok_or_else(|| {
            crate::Error::Unsupported(format!(
                "CREATE EXTERNAL TABLE {} has no LOCATION clause",
                ct.name,
            ))
        })?;

    let reader_fn = determine_reader_function(&ct.hive_formats, opts)?;
    let reader_opts = build_reader_options(&ct.hive_formats, &ct.hive_distribution);

    for col in ct.columns.iter_mut() {
        types::rewrite_data_type(&mut col.data_type, dialect)?;
    }

    let columns: Vec<ViewColumnDef> = ct
        .columns
        .iter()
        .map(|c| ViewColumnDef {
            name: c.name.clone(),
            data_type: None,
            options: None,
        })
        .collect();

    let escaped_location = location.replace('\'', "''");
    let select_sql = if reader_opts.is_empty() {
        format!("SELECT * FROM {reader_fn}('{escaped_location}')")
    } else {
        format!("SELECT * FROM {reader_fn}('{escaped_location}', {reader_opts})")
    };
    let query = Parser::parse_sql(&GenericDialect {}, &select_sql)
        .map_err(|e| crate::Error::Unsupported(format!("Failed to build view query: {e}")))?;

    let query = match query.into_iter().next() {
        Some(Statement::Query(q)) => q,
        _ => {
            return Err(crate::Error::Unsupported(
                "Failed to parse generated view query".to_string(),
            ));
        }
    };

    Ok(Statement::CreateView(CreateView {
        or_alter: false,
        or_replace: ct.or_replace,
        materialized: false,
        secure: false,
        name: ct.name,
        name_before_not_exists: false,
        columns,
        query,
        options: CreateTableOptions::None,
        cluster_by: vec![],
        comment: None,
        with_no_schema_binding: false,
        if_not_exists: ct.if_not_exists,
        temporary: false,
        to: None,
        params: None,
    }))
}

// ---------------------------------------------------------------------------
// Trino WITH-clause S3 table rewriting
// ---------------------------------------------------------------------------

fn rewrite_trino_s3_table(
    stmt: Statement,
    dialect: SourceDialect,
    opts: &TranspileOptions,
) -> Result<Statement> {
    let ct = match stmt {
        Statement::CreateTable(ct) => ct,
        _ => unreachable!(),
    };

    match opts.external_table {
        ExternalTableBehavior::Error => Err(crate::Error::Unsupported(format!(
            "CREATE TABLE {} WITH (external_location = ...) is not supported. \
             Set ExternalTableBehavior::MapToView to convert to a DuckDB view.",
            ct.name,
        ))),
        ExternalTableBehavior::MapToView => trino_s3_table_to_view(ct, dialect),
    }
}

fn trino_s3_table_to_view(
    mut ct: sqlparser::ast::CreateTable,
    dialect: SourceDialect,
) -> Result<Statement> {
    let opts = extract_table_options(&ct.table_options);

    let location = get_property_value(opts, "external_location")
        .ok_or_else(|| {
            crate::Error::Unsupported(format!(
                "CREATE TABLE {} WITH clause has no external_location",
                ct.name,
            ))
        })?
        .to_string();

    let format = get_property_value(opts, "format")
        .unwrap_or("PARQUET")
        .to_uppercase();

    let reader_fn = reader_from_trino_format(&format)?;

    for col in ct.columns.iter_mut() {
        types::rewrite_data_type(&mut col.data_type, dialect)?;
    }

    let columns: Vec<ViewColumnDef> = ct
        .columns
        .iter()
        .map(|c| ViewColumnDef {
            name: c.name.clone(),
            data_type: None,
            options: None,
        })
        .collect();

    let escaped_location = location.replace('\'', "''");
    let select_sql = format!("SELECT * FROM {reader_fn}('{escaped_location}')");
    let query = Parser::parse_sql(&GenericDialect {}, &select_sql)
        .map_err(|e| crate::Error::Unsupported(format!("Failed to build view query: {e}")))?;

    let query = match query.into_iter().next() {
        Some(Statement::Query(q)) => q,
        _ => {
            return Err(crate::Error::Unsupported(
                "Failed to parse generated view query".to_string(),
            ));
        }
    };

    Ok(Statement::CreateView(CreateView {
        or_alter: false,
        or_replace: ct.or_replace,
        materialized: false,
        secure: false,
        name: ct.name,
        name_before_not_exists: false,
        columns,
        query,
        options: CreateTableOptions::None,
        cluster_by: vec![],
        comment: None,
        with_no_schema_binding: false,
        if_not_exists: ct.if_not_exists,
        temporary: false,
        to: None,
        params: None,
    }))
}

/// Map a Trino WITH-clause format value to a DuckDB reader function.
fn reader_from_trino_format(format: &str) -> Result<&'static str> {
    match format {
        "PARQUET" | "ORC" => Ok("read_parquet"),
        "TEXTFILE" | "CSV" => Ok("read_csv"),
        "JSON" => Ok("read_json"),
        "AVRO" => Err(crate::Error::Unsupported(
            "AVRO format is not directly supported by DuckDB".to_string(),
        )),
        "SEQUENCEFILE" => Err(crate::Error::Unsupported(
            "SEQUENCEFILE format is not supported by DuckDB".to_string(),
        )),
        "RCFILE" => Err(crate::Error::Unsupported(
            "RCFILE format is not supported by DuckDB".to_string(),
        )),
        other => Err(crate::Error::Unsupported(format!(
            "Unknown Trino table format '{other}'"
        ))),
    }
}

/// Build additional reader function options from Hive format metadata.
///
/// For read_csv: extract field delimiter, escape char, line terminator from ROW FORMAT DELIMITED.
/// For all readers: add hive_partitioning=true if PARTITIONED BY is present.
fn build_reader_options(
    hive_formats: &Option<HiveFormat>,
    hive_distribution: &HiveDistributionStyle,
) -> String {
    let mut opts = Vec::new();

    // Hive partitioning
    if matches!(hive_distribution, HiveDistributionStyle::PARTITIONED { .. }) {
        opts.push("hive_partitioning = true".to_string());
    }

    // ROW FORMAT DELIMITED options → read_csv parameters
    if let Some(HiveFormat {
        row_format: Some(HiveRowFormat::DELIMITED { delimiters }),
        ..
    }) = hive_formats
    {
        for delim in delimiters {
            let char_val = &delim.char.value;
            match delim.delimiter {
                HiveDelimiter::FieldsTerminatedBy => {
                    opts.push(format!("delim = '{}'", escape_quote(char_val)));
                }
                HiveDelimiter::FieldsEscapedBy => {
                    opts.push(format!("escape = '{}'", escape_quote(char_val)));
                }
                HiveDelimiter::LinesTerminatedBy => {
                    opts.push(format!("new_line = '{}'", escape_quote(char_val)));
                }
                HiveDelimiter::NullDefinedAs => {
                    opts.push(format!("nullstr = '{}'", escape_quote(char_val)));
                }
                // COLLECTION ITEMS TERMINATED BY and MAP KEYS TERMINATED BY
                // don't have direct read_csv equivalents
                _ => {}
            }
        }
    }

    opts.join(", ")
}

fn escape_quote(s: &str) -> String {
    s.replace('\'', "''")
}

fn determine_reader_function(
    hive_formats: &Option<HiveFormat>,
    opts: &TranspileOptions,
) -> Result<String> {
    // First check STORED AS file format
    if let Some(HiveFormat {
        storage: Some(HiveIOFormat::FileFormat { format }),
        ..
    }) = hive_formats
    {
        return match format {
            FileFormat::PARQUET => Ok("read_parquet".to_string()),
            FileFormat::ORC => Ok("read_parquet".to_string()),
            FileFormat::TEXTFILE => Ok("read_csv".to_string()),
            FileFormat::JSONFILE => Ok("read_json".to_string()),
            FileFormat::AVRO => Err(crate::Error::Unsupported(
                "AVRO format is not directly supported by DuckDB".to_string(),
            )),
            FileFormat::SEQUENCEFILE => Err(crate::Error::Unsupported(
                "SEQUENCEFILE format is not supported by DuckDB".to_string(),
            )),
            FileFormat::RCFILE => Err(crate::Error::Unsupported(
                "RCFILE format is not supported by DuckDB".to_string(),
            )),
        };
    }

    // Fall back to ROW FORMAT SERDE class to infer the reader function
    if let Some(HiveFormat {
        row_format: Some(HiveRowFormat::SERDE { class }),
        ..
    }) = hive_formats
    {
        return reader_from_serde_class(class, opts);
    }

    Ok("read_parquet".to_string())
}

/// Map a Hive SerDe class name to a DuckDB reader function.
///
/// The user-supplied resolver in `opts.serde_class_resolver` is called first.
/// If it returns `Some(reader_fn)` that value is used; returning `None` falls
/// through to `infer_reader_from_serde_class`.
fn reader_from_serde_class(class: &str, opts: &TranspileOptions) -> Result<String> {
    if let Some(ref resolver) = opts.serde_class_resolver
        && let Some(reader_fn) = resolver.resolve(class)
    {
        return Ok(reader_fn);
    }
    infer_reader_from_serde_class(class)
}

/// Built-in substring-based inference of the DuckDB reader function for a
/// Hive SerDe class name.
fn infer_reader_from_serde_class(class: &str) -> Result<String> {
    let class_lower = class.to_lowercase();
    if class_lower.contains("parquethiveserde")
        || class_lower.contains("parquet")
        || class_lower.contains("orcserde")
        || class_lower.contains("orc")
    {
        // DuckDB reads both Parquet and ORC via read_parquet
        Ok("read_parquet".to_string())
    } else if class_lower.contains("jsonserde") || class_lower.contains("json") {
        Ok("read_json".to_string())
    } else if class_lower.contains("opencsvserde")
        || class_lower.contains("csv")
        || class_lower.contains("lazysimpleserde")
        || class_lower.contains("lazysimple")
    {
        // LazySimpleSerDe is delimited text, best mapped to CSV
        Ok("read_csv".to_string())
    } else if class_lower.contains("regexserde") {
        Err(crate::Error::Unsupported(
            "RegexSerDe has no DuckDB equivalent".to_string(),
        ))
    } else {
        Err(crate::Error::Unsupported(format!(
            "Hive SerDe class '{class}' has no known DuckDB reader equivalent"
        )))
    }
}

// ---------------------------------------------------------------------------
// ALTER TABLE
// ---------------------------------------------------------------------------

fn rewrite_alter_operation(op: &mut AlterTableOperation, dialect: SourceDialect) -> Result<()> {
    match op {
        AlterTableOperation::AddColumn { column_def, .. } => {
            types::rewrite_data_type(&mut column_def.data_type, dialect)?;
        }
        AlterTableOperation::AlterColumn {
            op: sqlparser::ast::AlterColumnOperation::SetDataType { data_type, .. },
            ..
        } => {
            types::rewrite_data_type(data_type, dialect)?;
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::dialect::SourceDialect;
    use crate::{
        ExternalTableBehavior, IcebergTableBehavior, SerdeClassResolver, TranspileOptions,
    };

    #[test]
    fn redshift_create_table_type_rewrite() {
        let sql = "CREATE TABLE t (a VARCHAR(MAX), b SUPER, c INTEGER)";
        let result = crate::transpile(sql, SourceDialect::Redshift).unwrap();
        assert_eq!(result, "CREATE TABLE t (a VARCHAR, b JSON, c INTEGER)");
    }

    #[test]
    fn trino_create_table_varbinary() {
        let sql = "CREATE TABLE t (a VARBINARY)";
        let result = crate::transpile(sql, SourceDialect::Trino).unwrap();
        assert_eq!(result, "CREATE TABLE t (a BLOB)");
    }

    #[test]
    fn external_table_default_errors() {
        let sql =
            "CREATE EXTERNAL TABLE t (a INTEGER) STORED AS PARQUET LOCATION 's3://bucket/path'";
        let result = crate::transpile(sql, SourceDialect::Redshift);
        assert!(matches!(result, Err(crate::Error::Unsupported(_))));
    }

    #[test]
    fn external_table_map_to_view() {
        let sql =
            "CREATE EXTERNAL TABLE t (a INTEGER) STORED AS PARQUET LOCATION 's3://bucket/path'";
        let opts = TranspileOptions {
            external_table: ExternalTableBehavior::MapToView,
            ..Default::default()
        };
        let result = crate::transpile_with_options(sql, SourceDialect::Redshift, &opts).unwrap();
        assert!(
            result.contains("CREATE VIEW"),
            "Expected CREATE VIEW in: {result}"
        );
        assert!(
            result.contains("read_parquet"),
            "Expected read_parquet in: {result}"
        );
        assert!(
            result.contains("s3://bucket/path"),
            "Expected location in: {result}"
        );
    }

    #[test]
    fn alter_table_add_column_type_rewrite() {
        let sql = "ALTER TABLE t ADD COLUMN x VARCHAR(MAX)";
        let result = crate::transpile(sql, SourceDialect::Redshift).unwrap();
        assert!(
            result.contains("VARCHAR") && !result.contains("MAX"),
            "Got: {result}"
        );
    }

    // Iceberg table tests

    #[test]
    fn iceberg_table_default_errors() {
        // Athena style: TBLPROPERTIES ('table_type'='ICEBERG')
        let sql = "CREATE TABLE t (id INT, name VARCHAR) LOCATION 's3://bucket/iceberg/' TBLPROPERTIES ('table_type'='ICEBERG')";
        let result = crate::transpile(sql, SourceDialect::Trino);
        assert!(
            matches!(result, Err(crate::Error::Unsupported(_))),
            "Got: {result:?}"
        );
    }

    #[test]
    fn iceberg_table_tblproperties_map_to_view() {
        // Athena style
        let sql = "CREATE TABLE t (id INT, name VARCHAR) LOCATION 's3://bucket/iceberg/' TBLPROPERTIES ('table_type'='ICEBERG')";
        let opts = TranspileOptions {
            iceberg_table: IcebergTableBehavior::MapToView,
            ..Default::default()
        };
        let result = crate::transpile_with_options(sql, SourceDialect::Trino, &opts).unwrap();
        assert!(
            result.contains("CREATE VIEW"),
            "Expected CREATE VIEW in: {result}"
        );
        assert!(
            result.contains("iceberg_scan"),
            "Expected iceberg_scan in: {result}"
        );
        assert!(
            result.contains("s3://bucket/iceberg/"),
            "Expected location in: {result}"
        );
    }

    #[test]
    fn iceberg_table_trino_with_clause() {
        // Trino style: WITH (table_type = 'ICEBERG', location = '...')
        let sql = "CREATE TABLE t (id INT, name VARCHAR) WITH (table_type = 'ICEBERG', location = 's3://bucket/iceberg/', format = 'PARQUET')";
        let opts = TranspileOptions {
            iceberg_table: IcebergTableBehavior::MapToView,
            ..Default::default()
        };
        let result = crate::transpile_with_options(sql, SourceDialect::Trino, &opts).unwrap();
        assert!(
            result.contains("CREATE VIEW"),
            "Expected CREATE VIEW in: {result}"
        );
        assert!(
            result.contains("iceberg_scan"),
            "Expected iceberg_scan in: {result}"
        );
        assert!(
            result.contains("s3://bucket/iceberg/"),
            "Expected location in: {result}"
        );
    }

    // Hive DDL tests

    #[test]
    fn hive_external_table_with_partitioning() {
        let sql = "CREATE EXTERNAL TABLE t (id INT, name STRING) PARTITIONED BY (dt STRING) STORED AS PARQUET LOCATION 's3://bucket/data/'";
        let opts = TranspileOptions {
            external_table: ExternalTableBehavior::MapToView,
            ..Default::default()
        };
        let result = crate::transpile_with_options(sql, SourceDialect::Hive, &opts).unwrap();
        assert!(
            result.contains("CREATE VIEW"),
            "Expected CREATE VIEW in: {result}"
        );
        assert!(
            result.contains("read_parquet"),
            "Expected read_parquet in: {result}"
        );
        assert!(
            result.contains("hive_partitioning"),
            "Expected hive_partitioning in: {result}"
        );
    }

    #[test]
    fn hive_external_table_csv_with_row_format() {
        let sql = "CREATE EXTERNAL TABLE t (id INT, name STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION 's3://bucket/csv/'";
        let opts = TranspileOptions {
            external_table: ExternalTableBehavior::MapToView,
            ..Default::default()
        };
        let result = crate::transpile_with_options(sql, SourceDialect::Hive, &opts).unwrap();
        assert!(
            result.contains("CREATE VIEW"),
            "Expected CREATE VIEW in: {result}"
        );
        assert!(
            result.contains("read_csv"),
            "Expected read_csv in: {result}"
        );
        assert!(
            result.contains("delim"),
            "Expected delim option in: {result}"
        );
    }

    #[test]
    fn hive_simple_select_passthrough() {
        let sql = "SELECT a, b FROM t WHERE a > 10";
        let result = crate::transpile(sql, SourceDialect::Hive).unwrap();
        assert_eq!(result, "SELECT a, b FROM t WHERE a > 10");
    }

    #[test]
    fn hive_external_table_serde_json() {
        let sql = "CREATE EXTERNAL TABLE t (id INT, name STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe' LOCATION 's3://bucket/json/'";
        let opts = TranspileOptions {
            external_table: ExternalTableBehavior::MapToView,
            ..Default::default()
        };
        let result = crate::transpile_with_options(sql, SourceDialect::Hive, &opts).unwrap();
        assert!(
            result.contains("CREATE VIEW"),
            "Expected CREATE VIEW in: {result}"
        );
        assert!(
            result.contains("read_json"),
            "Expected read_json in: {result}"
        );
    }

    #[test]
    fn hive_external_table_serde_parquet() {
        let sql = "CREATE EXTERNAL TABLE t (id INT) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' LOCATION 's3://bucket/pq/'";
        let opts = TranspileOptions {
            external_table: ExternalTableBehavior::MapToView,
            ..Default::default()
        };
        let result = crate::transpile_with_options(sql, SourceDialect::Hive, &opts).unwrap();
        assert!(
            result.contains("read_parquet"),
            "Expected read_parquet in: {result}"
        );
    }

    #[test]
    fn hive_external_table_serde_opencsv() {
        let sql = "CREATE EXTERNAL TABLE t (id INT) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' LOCATION 's3://bucket/csv/'";
        let opts = TranspileOptions {
            external_table: ExternalTableBehavior::MapToView,
            ..Default::default()
        };
        let result = crate::transpile_with_options(sql, SourceDialect::Hive, &opts).unwrap();
        assert!(
            result.contains("read_csv"),
            "Expected read_csv in: {result}"
        );
    }

    #[test]
    fn hive_external_table_serde_lazy_simple() {
        let sql = "CREATE EXTERNAL TABLE t (id INT) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' LOCATION 's3://bucket/txt/'";
        let opts = TranspileOptions {
            external_table: ExternalTableBehavior::MapToView,
            ..Default::default()
        };
        let result = crate::transpile_with_options(sql, SourceDialect::Hive, &opts).unwrap();
        assert!(
            result.contains("read_csv"),
            "Expected read_csv in: {result}"
        );
    }

    #[test]
    fn hive_external_table_serde_unknown_errors() {
        let sql = "CREATE EXTERNAL TABLE t (id INT) ROW FORMAT SERDE 'com.example.CustomSerDe' LOCATION 's3://bucket/data/'";
        let opts = TranspileOptions {
            external_table: ExternalTableBehavior::MapToView,
            ..Default::default()
        };
        let result = crate::transpile_with_options(sql, SourceDialect::Hive, &opts);
        assert!(matches!(result, Err(crate::Error::Unsupported(_))));
    }

    #[test]
    fn hive_external_table_serde_custom_resolver() {
        let sql = "CREATE EXTERNAL TABLE t (id INT) ROW FORMAT SERDE 'com.example.CustomSerDe' LOCATION 's3://bucket/data/'";
        let opts = TranspileOptions {
            external_table: ExternalTableBehavior::MapToView,
            serde_class_resolver: Some(SerdeClassResolver::new(|class| {
                if class.eq_ignore_ascii_case("com.example.CustomSerDe") {
                    Some("read_parquet".to_string())
                } else {
                    None
                }
            })),
            ..Default::default()
        };
        let result = crate::transpile_with_options(sql, SourceDialect::Hive, &opts).unwrap();
        assert!(
            result.contains("read_parquet"),
            "Expected read_parquet in: {result}"
        );
    }

    #[test]
    fn hive_external_table_serde_custom_resolver_fallthrough() {
        // Resolver returns None for unknown classes → built-in logic takes over → error
        let sql = "CREATE EXTERNAL TABLE t (id INT) ROW FORMAT SERDE 'com.example.CustomSerDe' LOCATION 's3://bucket/data/'";
        let opts = TranspileOptions {
            external_table: ExternalTableBehavior::MapToView,
            serde_class_resolver: Some(SerdeClassResolver::new(|_| None)),
            ..Default::default()
        };
        let result = crate::transpile_with_options(sql, SourceDialect::Hive, &opts);
        assert!(matches!(result, Err(crate::Error::Unsupported(_))));
    }

    // Trino WITH-clause S3 table tests

    #[test]
    fn trino_s3_table_default_errors() {
        let sql = "CREATE TABLE t (id BIGINT, name VARCHAR) WITH (external_location = 's3://bucket/prefix/', format = 'PARQUET')";
        let result = crate::transpile(sql, SourceDialect::Trino);
        assert!(
            matches!(result, Err(crate::Error::Unsupported(_))),
            "Got: {result:?}"
        );
    }

    #[test]
    fn trino_s3_table_parquet_map_to_view() {
        let sql = "CREATE TABLE t (id BIGINT, name VARCHAR) WITH (external_location = 's3://bucket/prefix/', format = 'PARQUET')";
        let opts = TranspileOptions {
            external_table: ExternalTableBehavior::MapToView,
            ..Default::default()
        };
        let result = crate::transpile_with_options(sql, SourceDialect::Trino, &opts).unwrap();
        assert!(
            result.contains("CREATE VIEW"),
            "Expected CREATE VIEW in: {result}"
        );
        assert!(
            result.contains("read_parquet"),
            "Expected read_parquet in: {result}"
        );
        assert!(
            result.contains("s3://bucket/prefix/"),
            "Expected location in: {result}"
        );
    }

    #[test]
    fn trino_s3_table_orc_map_to_view() {
        let sql = "CREATE TABLE t (id BIGINT) WITH (external_location = 's3://bucket/orc/', format = 'ORC')";
        let opts = TranspileOptions {
            external_table: ExternalTableBehavior::MapToView,
            ..Default::default()
        };
        let result = crate::transpile_with_options(sql, SourceDialect::Trino, &opts).unwrap();
        assert!(
            result.contains("read_parquet"),
            "Expected read_parquet for ORC in: {result}"
        );
    }

    #[test]
    fn trino_s3_table_textfile_map_to_view() {
        let sql = "CREATE TABLE t (id BIGINT) WITH (external_location = 's3://bucket/csv/', format = 'TEXTFILE')";
        let opts = TranspileOptions {
            external_table: ExternalTableBehavior::MapToView,
            ..Default::default()
        };
        let result = crate::transpile_with_options(sql, SourceDialect::Trino, &opts).unwrap();
        assert!(
            result.contains("read_csv"),
            "Expected read_csv for TEXTFILE in: {result}"
        );
    }

    #[test]
    fn trino_s3_table_json_map_to_view() {
        let sql = "CREATE TABLE t (id BIGINT) WITH (external_location = 's3://bucket/json/', format = 'JSON')";
        let opts = TranspileOptions {
            external_table: ExternalTableBehavior::MapToView,
            ..Default::default()
        };
        let result = crate::transpile_with_options(sql, SourceDialect::Trino, &opts).unwrap();
        assert!(
            result.contains("read_json"),
            "Expected read_json in: {result}"
        );
    }

    #[test]
    fn trino_s3_table_no_format_defaults_to_parquet() {
        let sql = "CREATE TABLE t (id BIGINT) WITH (external_location = 's3://bucket/data/')";
        let opts = TranspileOptions {
            external_table: ExternalTableBehavior::MapToView,
            ..Default::default()
        };
        let result = crate::transpile_with_options(sql, SourceDialect::Trino, &opts).unwrap();
        assert!(
            result.contains("read_parquet"),
            "Expected read_parquet as default in: {result}"
        );
    }
}
