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
