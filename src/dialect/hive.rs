use sqlparser::ast::Statement;

use crate::dialect::SourceDialect;
use crate::transforms::{ddl, dml, show};
use crate::transpiler::Transpiler;
use crate::transpiler::rewrite::ExprRewriter;
use crate::{Result, TranspileOptions};

/// Hive-specific transpilation rules.
pub struct HiveDialect;

impl Transpiler for HiveDialect {
    fn transpile_statement(
        &self,
        mut stmt: Statement,
        opts: &TranspileOptions,
    ) -> Result<Statement> {
        // Statement-level transforms
        stmt = match stmt {
            s @ Statement::CreateTable { .. } | s @ Statement::AlterTable(..) => {
                ddl::rewrite_ddl(s, SourceDialect::Hive, opts)?
            }
            s @ Statement::Insert(_)
            | s @ Statement::Update { .. }
            | s @ Statement::Delete(_)
            | s @ Statement::Merge { .. } => dml::rewrite_dml(s, SourceDialect::Hive, opts)?,
            s if is_show_statement(&s) => show::rewrite_show(s, SourceDialect::Hive, opts)?,
            s => s,
        };

        // Expression-level transforms via VisitorMut
        let mut rewriter = ExprRewriter::new(SourceDialect::Hive);
        rewriter.rewrite(&mut stmt)?;

        Ok(stmt)
    }
}

fn is_show_statement(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::ShowTables { .. }
            | Statement::ShowColumns { .. }
            | Statement::ShowDatabases { .. }
            | Statement::ShowSchemas { .. }
            | Statement::ShowViews { .. }
            | Statement::ShowCreate { .. }
            | Statement::ShowVariable { .. }
            | Statement::ShowVariables { .. }
            | Statement::ShowFunctions { .. }
    )
}
