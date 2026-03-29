use sqlparser::ast::{CopyLegacyOption, CopySource, CopyTarget, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::dialect::SourceDialect;
use crate::{CopyBehavior, Result, TranspileOptions};

/// Transpile DML statements (INSERT, UPDATE, DELETE, MERGE, COPY).
///
/// Most DML is compatible between Trino/Redshift and DuckDB:
/// - UPDATE ... FROM ... (Redshift) → DuckDB supports this syntax
/// - DELETE ... USING ... (Redshift) → DuckDB supports this syntax
/// - MERGE → DuckDB supports standard MERGE
///
/// Redshift COPY FROM → controlled by CopyBehavior option.
pub fn rewrite_dml(
    stmt: Statement,
    dialect: SourceDialect,
    opts: &TranspileOptions,
) -> Result<Statement> {
    if let Statement::Copy {
        ref source,
        to,
        ref target,
        ref legacy_options,
        ..
    } = stmt
        && dialect == SourceDialect::Redshift
        && !to
    {
        return rewrite_redshift_copy(source, target, legacy_options, opts);
    }

    // Most DML passes through — expression-level rewrites (functions, types)
    // are handled by the ExprRewriter in the visitor walk.
    Ok(stmt)
}

/// Rewrite Redshift COPY FROM to INSERT INTO ... SELECT * FROM read_*().
fn rewrite_redshift_copy(
    source: &CopySource,
    target: &CopyTarget,
    legacy_options: &[CopyLegacyOption],
    opts: &TranspileOptions,
) -> Result<Statement> {
    match opts.copy {
        CopyBehavior::Error => Err(crate::Error::Unsupported(
            "Redshift COPY command is not directly compatible with DuckDB. \
             Set CopyBehavior::MapToInsert to convert to INSERT INTO ... SELECT * FROM read_*()."
                .to_string(),
        )),
        CopyBehavior::MapToInsert => copy_to_insert(source, target, legacy_options),
    }
}

fn copy_to_insert(
    source: &CopySource,
    target: &CopyTarget,
    legacy_options: &[CopyLegacyOption],
) -> Result<Statement> {
    let (table_name, columns) = match source {
        CopySource::Table {
            table_name,
            columns,
        } => (table_name.clone(), columns.clone()),
        CopySource::Query(_) => {
            return Err(crate::Error::Unsupported(
                "COPY from query is not supported for MapToInsert conversion".to_string(),
            ));
        }
    };

    let path = match target {
        CopyTarget::File { filename } => filename.clone(),
        _ => {
            return Err(crate::Error::Unsupported(
                "COPY FROM requires a file path (e.g., 's3://bucket/path')".to_string(),
            ));
        }
    };

    let reader_fn = determine_copy_reader(legacy_options);
    let reader_opts = build_copy_reader_options(legacy_options);

    let escaped_path = path.replace('\'', "''");
    let col_list = if columns.is_empty() {
        "*".to_string()
    } else {
        columns
            .iter()
            .map(|c| c.value.clone())
            .collect::<Vec<_>>()
            .join(", ")
    };

    let select_sql = if reader_opts.is_empty() {
        format!("INSERT INTO {table_name} SELECT {col_list} FROM {reader_fn}('{escaped_path}')")
    } else {
        format!(
            "INSERT INTO {table_name} SELECT {col_list} FROM {reader_fn}('{escaped_path}', {reader_opts})"
        )
    };

    let stmts = Parser::parse_sql(&GenericDialect {}, &select_sql)
        .map_err(|e| crate::Error::Unsupported(format!("Failed to build INSERT query: {e}")))?;

    stmts
        .into_iter()
        .next()
        .ok_or_else(|| crate::Error::Unsupported("Empty INSERT query".to_string()))
}

/// Determine the DuckDB reader function from Redshift COPY legacy options.
fn determine_copy_reader(options: &[CopyLegacyOption]) -> &'static str {
    for opt in options {
        match opt {
            CopyLegacyOption::Parquet => return "read_parquet",
            CopyLegacyOption::Json(_) => return "read_json",
            CopyLegacyOption::Csv(_) => return "read_csv",
            CopyLegacyOption::FixedWidth(_) => return "read_csv", // approximate
            _ => {}
        }
    }
    // Default: Redshift COPY defaults to pipe-delimited text
    "read_csv"
}

/// Build DuckDB reader function options from Redshift COPY legacy options.
fn build_copy_reader_options(options: &[CopyLegacyOption]) -> String {
    let mut opts = Vec::new();
    let mut has_explicit_delimiter = false;

    for opt in options {
        match opt {
            CopyLegacyOption::Delimiter(ch) => {
                opts.push(format!("delim = '{ch}'"));
                has_explicit_delimiter = true;
            }
            CopyLegacyOption::IgnoreHeader(n) => {
                if *n > 0 {
                    opts.push("header = true".to_string());
                }
            }
            CopyLegacyOption::Header => {
                opts.push("header = true".to_string());
            }
            CopyLegacyOption::Null(s) => {
                let escaped = s.replace('\'', "''");
                opts.push(format!("nullstr = '{escaped}'"));
            }
            CopyLegacyOption::EmptyAsNull => {
                opts.push("nullstr = ''".to_string());
            }
            CopyLegacyOption::DateFormat(Some(fmt)) if fmt != "auto" => {
                let escaped = fmt.replace('\'', "''");
                opts.push(format!("dateformat = '{escaped}'"));
            }
            CopyLegacyOption::TimeFormat(Some(fmt))
                if fmt != "auto" && fmt != "epochsecs" && fmt != "epochmillisecs" =>
            {
                let escaped = fmt.replace('\'', "''");
                opts.push(format!("timestampformat = '{escaped}'"));
            }
            CopyLegacyOption::Escape => {
                opts.push("escape = '\\\\'".to_string());
            }
            CopyLegacyOption::Gzip => {
                opts.push("compression = 'gzip'".to_string());
            }
            CopyLegacyOption::Bzip2 => {
                opts.push("compression = 'bzip2'".to_string());
            }
            CopyLegacyOption::Zstd => {
                opts.push("compression = 'zstd'".to_string());
            }
            // Silently dropped: IAM_ROLE, MANIFEST, COMPUPDATE, STATUPDATE,
            // REGION, ACCEPTANYDATE, ACCEPTINVCHARS, BLANKSASNULL, TRUNCATECOLUMNS,
            // ENCRYPTED, PARALLEL, etc.
            _ => {}
        }
    }

    // Redshift default delimiter is '|' (pipe), not ',' like DuckDB
    if !has_explicit_delimiter && determine_copy_reader(options) == "read_csv" {
        // Check if CSV mode was explicitly set (CSV implies comma delimiter)
        let is_csv_mode = options
            .iter()
            .any(|o| matches!(o, CopyLegacyOption::Csv(_)));
        if !is_csv_mode {
            opts.insert(0, "delim = '|'".to_string());
        }
    }

    opts.join(", ")
}

#[cfg(test)]
mod tests {
    use crate::dialect::SourceDialect;
    use crate::{CopyBehavior, TranspileOptions};

    #[test]
    fn redshift_insert_passthrough() {
        let sql = "INSERT INTO t (a, b) VALUES (1, 'hello')";
        let result = crate::transpile(sql, SourceDialect::Redshift).unwrap();
        assert_eq!(result, "INSERT INTO t (a, b) VALUES (1, 'hello')");
    }

    #[test]
    fn redshift_update_from_passthrough() {
        let sql = "UPDATE t SET a = s.a FROM staging s WHERE t.id = s.id";
        let result = crate::transpile(sql, SourceDialect::Redshift).unwrap();
        assert!(
            result.contains("UPDATE t SET a = s.a FROM staging"),
            "Got: {result}"
        );
    }

    #[test]
    fn redshift_delete_using_passthrough() {
        let sql = "DELETE FROM t USING staging s WHERE t.id = s.id";
        let result = crate::transpile(sql, SourceDialect::Redshift).unwrap();
        assert!(result.contains("DELETE FROM t"), "Got: {result}");
    }

    #[test]
    fn trino_insert_with_function_rewrite() {
        let sql = "INSERT INTO t SELECT approx_distinct(col) FROM src";
        let result = crate::transpile(sql, SourceDialect::Trino).unwrap();
        assert!(result.contains("approx_count_distinct"), "Got: {result}");
    }

    // COPY tests

    #[test]
    fn redshift_copy_default_errors() {
        let sql = "COPY my_table FROM 's3://bucket/data/' IAM_ROLE 'arn:aws:iam::123:role/myrole' PARQUET";
        let result = crate::transpile(sql, SourceDialect::Redshift);
        assert!(matches!(result, Err(crate::Error::Unsupported(_))));
    }

    #[test]
    fn redshift_copy_parquet_map_to_insert() {
        let sql = "COPY my_table FROM 's3://bucket/data/' IAM_ROLE 'arn:aws:iam::123:role/myrole' PARQUET";
        let opts = TranspileOptions {
            copy: CopyBehavior::MapToInsert,
            ..Default::default()
        };
        let result = crate::transpile_with_options(sql, SourceDialect::Redshift, &opts).unwrap();
        assert!(
            result.contains("INSERT INTO"),
            "Expected INSERT INTO in: {result}"
        );
        assert!(
            result.contains("read_parquet"),
            "Expected read_parquet in: {result}"
        );
        assert!(
            result.contains("s3://bucket/data/"),
            "Expected S3 path in: {result}"
        );
    }

    #[test]
    fn redshift_copy_csv_with_options() {
        let sql = "COPY my_table FROM 's3://bucket/csv/' IAM_ROLE 'arn:aws:iam::123:role/r' CSV DELIMITER ',' IGNOREHEADER 1";
        let opts = TranspileOptions {
            copy: CopyBehavior::MapToInsert,
            ..Default::default()
        };
        let result = crate::transpile_with_options(sql, SourceDialect::Redshift, &opts).unwrap();
        assert!(
            result.contains("INSERT INTO"),
            "Expected INSERT INTO in: {result}"
        );
        assert!(
            result.contains("read_csv"),
            "Expected read_csv in: {result}"
        );
        assert!(
            result.contains("header"),
            "Expected header option in: {result}"
        );
    }

    #[test]
    fn redshift_copy_json() {
        let sql =
            "COPY my_table FROM 's3://bucket/json/' IAM_ROLE 'arn:aws:iam::123:role/r' JSON 'auto'";
        let opts = TranspileOptions {
            copy: CopyBehavior::MapToInsert,
            ..Default::default()
        };
        let result = crate::transpile_with_options(sql, SourceDialect::Redshift, &opts).unwrap();
        assert!(
            result.contains("INSERT INTO"),
            "Expected INSERT INTO in: {result}"
        );
        assert!(
            result.contains("read_json"),
            "Expected read_json in: {result}"
        );
    }

    #[test]
    fn redshift_copy_default_delimiter_pipe() {
        // Redshift defaults to pipe-delimited, not comma
        let sql = "COPY my_table FROM 's3://bucket/data/' IAM_ROLE 'arn:aws:iam::123:role/r'";
        let opts = TranspileOptions {
            copy: CopyBehavior::MapToInsert,
            ..Default::default()
        };
        let result = crate::transpile_with_options(sql, SourceDialect::Redshift, &opts).unwrap();
        assert!(
            result.contains("read_csv"),
            "Expected read_csv (default) in: {result}"
        );
        assert!(
            result.contains("'|'"),
            "Expected pipe delimiter in: {result}"
        );
    }

    #[test]
    fn redshift_copy_with_gzip() {
        let sql = "COPY my_table FROM 's3://bucket/data.gz' IAM_ROLE 'arn:aws:iam::123:role/r' GZIP DELIMITER ','";
        let opts = TranspileOptions {
            copy: CopyBehavior::MapToInsert,
            ..Default::default()
        };
        let result = crate::transpile_with_options(sql, SourceDialect::Redshift, &opts).unwrap();
        assert!(
            result.contains("gzip"),
            "Expected gzip compression in: {result}"
        );
    }

    #[test]
    fn redshift_copy_with_columns() {
        let sql = "COPY my_table (a, b, c) FROM 's3://bucket/data/' IAM_ROLE 'arn:aws:iam::123:role/r' PARQUET";
        let opts = TranspileOptions {
            copy: CopyBehavior::MapToInsert,
            ..Default::default()
        };
        let result = crate::transpile_with_options(sql, SourceDialect::Redshift, &opts).unwrap();
        assert!(
            result.contains("INSERT INTO"),
            "Expected INSERT INTO in: {result}"
        );
        assert!(
            result.contains("a, b, c"),
            "Expected column list in: {result}"
        );
    }
}
