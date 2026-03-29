pub mod rewrite;

use sqlparser::ast::Statement;

use crate::TranspileOptions;

/// Trait for dialect-specific transpilation logic.
pub trait Transpiler {
    /// Transform a parsed SQL statement into target-dialect-compatible form.
    fn transpile_statement(
        &self,
        stmt: Statement,
        opts: &TranspileOptions,
    ) -> crate::Result<Statement>;
}
