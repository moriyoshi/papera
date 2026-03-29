mod hive;
mod redshift;
mod trino;

pub use hive::HiveDialect as HiveTranspileDialect;
pub use redshift::RedshiftDialect;
pub use trino::TrinoDialect;

/// The source SQL dialect to transpile from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceDialect {
    Trino,
    Redshift,
    Hive,
}

/// The target SQL dialect to transpile to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TargetDialect {
    /// Apache DuckDB (default).
    #[default]
    DuckDB,
    /// Apache DataFusion.
    DataFusion,
}
