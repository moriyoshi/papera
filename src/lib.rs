pub mod dialect;
pub mod error;
pub mod transpiler;

pub(crate) mod transforms;

pub use dialect::{SourceDialect, TargetDialect};
pub use error::{Error, Result};
pub use transpiler::Transpiler;

use std::fmt;
use std::sync::Arc;

use sqlparser::ast::Statement;
use sqlparser::dialect::{GenericDialect, HiveDialect, RedshiftSqlDialect};
use sqlparser::parser::Parser;

/// How to handle external table statements backed by S3 storage.
///
/// Applies to:
/// - `CREATE EXTERNAL TABLE ... LOCATION 's3://...'` (Hive/Redshift style)
/// - `CREATE TABLE ... WITH (external_location = 's3://...', format = '...')` (Trino style)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExternalTableBehavior {
    /// Emit `CREATE VIEW ... AS SELECT * FROM read_parquet(...)` etc.
    MapToView,
    /// Return an error for unsupported external table syntax.
    #[default]
    Error,
}

/// How to handle Iceberg tables (detected via TBLPROPERTIES 'table_type'='ICEBERG').
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IcebergTableBehavior {
    /// Emit `CREATE VIEW ... AS SELECT * FROM iceberg_scan('location')`.
    /// Requires the DuckDB iceberg extension.
    MapToView,
    /// Return an error for iceberg table syntax.
    #[default]
    Error,
}

/// How to handle Redshift COPY FROM statements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CopyBehavior {
    /// Emit `INSERT INTO table SELECT * FROM read_parquet(...)` etc.
    /// Redshift-specific options like IAM_ROLE are silently dropped.
    MapToInsert,
    /// Return an error for Redshift COPY commands.
    #[default]
    Error,
}

/// A user-supplied resolver that maps a Hive SerDe class name to a DuckDB reader function.
///
/// The function receives the full SerDe class name (e.g.
/// `"com.example.CustomSerDe"`) and should return `Some("read_parquet")` (or
/// any other DuckDB table-function name) when it recognises the class, or
/// `None` to fall through to the built-in resolver.
///
/// # Example
///
/// ```rust
/// use papera::{SerdeClassResolver, TranspileOptions, ExternalTableBehavior};
///
/// let resolver = SerdeClassResolver::new(|class| {
///     if class.eq_ignore_ascii_case("com.example.CustomSerDe") {
///         Some("read_parquet".to_string())
///     } else {
///         None
///     }
/// });
///
/// let opts = TranspileOptions {
///     external_table: ExternalTableBehavior::MapToView,
///     serde_class_resolver: Some(resolver),
///     ..Default::default()
/// };
/// ```
#[derive(Clone)]
#[allow(clippy::type_complexity)]
pub struct SerdeClassResolver(Arc<dyn Fn(&str) -> Option<String> + Send + Sync + 'static>);

impl SerdeClassResolver {
    pub fn new(f: impl Fn(&str) -> Option<String> + Send + Sync + 'static) -> Self {
        Self(Arc::new(f))
    }

    pub(crate) fn resolve(&self, class: &str) -> Option<String> {
        (self.0)(class)
    }
}

impl fmt::Debug for SerdeClassResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SerdeClassResolver(<fn>)")
    }
}

/// Options controlling transpilation behavior.
#[derive(Debug, Clone, Default)]
pub struct TranspileOptions {
    /// The target SQL dialect to emit. Defaults to [`TargetDialect::DuckDB`].
    pub target: TargetDialect,
    pub external_table: ExternalTableBehavior,
    pub iceberg_table: IcebergTableBehavior,
    pub copy: CopyBehavior,
    /// Optional user-supplied Hive SerDe class resolver.
    ///
    /// When set, the resolver is called before the built-in class mapping.
    /// Return `Some(reader_fn)` to override, or `None` to fall through to
    /// the built-in logic.
    pub serde_class_resolver: Option<SerdeClassResolver>,
}

/// Parse source SQL, transpile it, and emit DuckDB-compatible SQL.
pub fn transpile(sql: &str, source: SourceDialect) -> Result<String> {
    transpile_with_options(sql, source, &TranspileOptions::default())
}

/// Parse source SQL, transpile it with the given options, and emit DuckDB-compatible SQL.
pub fn transpile_with_options(
    sql: &str,
    source: SourceDialect,
    opts: &TranspileOptions,
) -> Result<String> {
    let statements = parse(sql, source)?;

    let transpiler: Box<dyn transpiler::Transpiler> = match source {
        SourceDialect::Trino => Box::new(dialect::TrinoDialect),
        SourceDialect::Redshift => Box::new(dialect::RedshiftDialect),
        SourceDialect::Hive => Box::new(dialect::HiveTranspileDialect),
    };

    let output: Result<Vec<String>> = statements
        .into_iter()
        .map(|stmt| {
            let transformed = transpiler.transpile_statement(stmt, opts)?;
            Ok(emit(&transformed))
        })
        .collect();

    Ok(output?.join(";\n"))
}

/// Parse SQL using the appropriate source dialect's parser.
fn parse(sql: &str, source: SourceDialect) -> Result<Vec<Statement>> {
    let dialect: Box<dyn sqlparser::dialect::Dialect> = match source {
        SourceDialect::Trino => Box::new(GenericDialect),
        SourceDialect::Redshift => Box::new(RedshiftSqlDialect {}),
        SourceDialect::Hive => Box::new(HiveDialect {}),
    };
    let stmts = Parser::parse_sql(&*dialect, sql)?;
    Ok(stmts)
}

/// Emit a statement as DuckDB SQL text.
fn emit(stmt: &Statement) -> String {
    stmt.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn passthrough_simple_select() {
        let sql = "SELECT 1";
        let result = transpile(sql, SourceDialect::Trino).unwrap();
        assert_eq!(result, "SELECT 1");
    }

    #[test]
    fn passthrough_select_from_table() {
        let sql = "SELECT a, b FROM my_table WHERE a > 10";
        let result = transpile(sql, SourceDialect::Redshift).unwrap();
        assert_eq!(result, "SELECT a, b FROM my_table WHERE a > 10");
    }
}
