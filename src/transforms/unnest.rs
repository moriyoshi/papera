use sqlparser::ast::TableFactor;

use crate::Result;
use crate::dialect::SourceDialect;

/// Rewrite UNNEST table factors for DuckDB compatibility.
///
/// Key differences:
/// - Trino uses `WITH ORDINALITY` for ordinal column → DuckDB also supports this
/// - BigQuery uses `WITH OFFSET` → DuckDB uses `WITH ORDINALITY` (1-indexed vs 0-indexed)
///
/// For Trino, UNNEST syntax is generally compatible with DuckDB, so most cases pass through.
pub fn rewrite_unnest(tf: &mut TableFactor, dialect: SourceDialect) -> Result<()> {
    if let TableFactor::UNNEST {
        with_offset,
        with_ordinality,
        ..
    } = tf
    {
        match dialect {
            SourceDialect::Trino => {
                // Trino uses WITH ORDINALITY, which DuckDB supports natively.
                // No transformation needed.
            }
            SourceDialect::Redshift | SourceDialect::Hive => {
                // Redshift doesn't have UNNEST natively, but if the parser
                // produces one (from standard SQL mode), ensure DuckDB compatibility.
                // Convert WITH OFFSET (BigQuery-style) to WITH ORDINALITY if present.
                if *with_offset && !*with_ordinality {
                    *with_ordinality = true;
                    *with_offset = false;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::dialect::SourceDialect;

    #[test]
    fn trino_unnest_passthrough() {
        let sql = "SELECT t.x FROM my_table CROSS JOIN UNNEST(arr) AS t(x)";
        let result = crate::transpile(sql, SourceDialect::Trino).unwrap();
        assert!(result.contains("UNNEST"), "Got: {result}");
    }

    #[test]
    fn trino_unnest_with_ordinality() {
        let sql = "SELECT t.x, t.n FROM my_table CROSS JOIN UNNEST(arr) WITH ORDINALITY AS t(x, n)";
        let result = crate::transpile(sql, SourceDialect::Trino).unwrap();
        assert!(result.contains("WITH ORDINALITY"), "Got: {result}");
    }
}
