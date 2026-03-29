use sqlparser::ast::{ShowCreateObject, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::dialect::SourceDialect;
use crate::{Result, TranspileOptions};

/// Transpile SHOW commands to DuckDB equivalents or information_schema queries.
///
/// DuckDB natively supports: SHOW TABLES, SHOW DATABASES, SHOW SCHEMAS.
/// For SHOW commands it supports, we pass through.
/// For unsupported ones, we translate to information_schema queries.
pub fn rewrite_show(
    stmt: Statement,
    _dialect: SourceDialect,
    _opts: &TranspileOptions,
) -> Result<Statement> {
    match &stmt {
        // DuckDB supports these natively — pass through
        Statement::ShowTables { .. }
        | Statement::ShowDatabases { .. }
        | Statement::ShowSchemas { .. }
        | Statement::ShowColumns { .. }
        | Statement::ShowViews { .. } => Ok(stmt),

        // SHOW CREATE TABLE/VIEW → emulate via DuckDB system catalog
        Statement::ShowCreate { obj_type, obj_name } => rewrite_show_create(obj_type, obj_name),

        // SHOW VARIABLE → translate to SELECT current_setting(...)
        Statement::ShowVariable { variable } => {
            let var_name = variable
                .iter()
                .map(|i| i.value.clone())
                .collect::<Vec<_>>()
                .join(".");
            let sql = format!("SELECT current_setting('{var_name}')");
            parse_single_statement(&sql)
        }

        // SHOW FUNCTIONS → information_schema.routines
        Statement::ShowFunctions { .. } => {
            let sql = "SELECT routine_name, routine_type FROM information_schema.routines ORDER BY routine_name";
            parse_single_statement(sql)
        }

        // Anything else — pass through and let DuckDB handle it
        _ => Ok(stmt),
    }
}

/// Emulate SHOW CREATE TABLE / VIEW using DuckDB's system catalog.
///
/// For tables, reconstructs the DDL from information_schema.columns and
/// information_schema.table_constraints. For views, retrieves the definition
/// from duckdb_views().
fn rewrite_show_create(
    obj_type: &ShowCreateObject,
    obj_name: &sqlparser::ast::ObjectName,
) -> Result<Statement> {
    let name = obj_name.to_string();
    let escaped_name = name.replace('\'', "''");

    match obj_type {
        ShowCreateObject::Table => {
            // Reconstruct CREATE TABLE DDL from information_schema.columns.
            // This query concatenates column definitions into a CREATE TABLE statement.
            //
            // DuckDB's string_agg and information_schema support make this possible
            // as a single query.
            let sql = format!(
                r#"SELECT 'CREATE TABLE {name} (' || string_agg(column_name || ' ' || data_type, ', ' ORDER BY ordinal_position) || ')' AS create_table FROM information_schema.columns WHERE table_name = '{escaped_name}'"#,
            );
            parse_single_statement(&sql)
        }
        ShowCreateObject::View => {
            // DuckDB stores view SQL in duckdb_views()
            let sql = format!("SELECT sql FROM duckdb_views() WHERE view_name = '{escaped_name}'");
            parse_single_statement(&sql)
        }
        other => Err(crate::Error::Unsupported(format!(
            "SHOW CREATE {other} is not supported"
        ))),
    }
}

fn parse_single_statement(sql: &str) -> Result<Statement> {
    let stmts = Parser::parse_sql(&GenericDialect {}, sql).map_err(|e| {
        crate::Error::Unsupported(format!("Failed to build replacement query: {e}"))
    })?;
    stmts
        .into_iter()
        .next()
        .ok_or_else(|| crate::Error::Unsupported("Empty replacement query".to_string()))
}

#[cfg(test)]
mod tests {
    use crate::dialect::SourceDialect;

    #[test]
    fn show_tables_passthrough() {
        let sql = "SHOW TABLES";
        let result = crate::transpile(sql, SourceDialect::Trino).unwrap();
        assert_eq!(result, "SHOW TABLES");
    }

    #[test]
    fn show_schemas_passthrough() {
        let sql = "SHOW SCHEMAS";
        let result = crate::transpile(sql, SourceDialect::Trino).unwrap();
        assert_eq!(result, "SHOW SCHEMAS");
    }

    #[test]
    fn show_create_table_emulated() {
        let sql = "SHOW CREATE TABLE t";
        let result = crate::transpile(sql, SourceDialect::Trino).unwrap();
        assert!(
            result.contains("information_schema.columns"),
            "Got: {result}"
        );
        assert!(result.contains("CREATE TABLE"), "Got: {result}");
    }

    #[test]
    fn show_create_view_emulated() {
        let sql = "SHOW CREATE VIEW v";
        let result = crate::transpile(sql, SourceDialect::Trino).unwrap();
        assert!(result.contains("duckdb_views"), "Got: {result}");
    }

    #[test]
    fn show_variable_to_current_setting() {
        let sql = "SHOW search_path";
        let result = crate::transpile(sql, SourceDialect::Redshift).unwrap();
        assert!(result.contains("current_setting"), "Got: {result}");
    }
}
