use std::collections::HashMap;
use std::ops::ControlFlow;

use sqlparser::ast::{Expr, Query, SetExpr, Statement, TableFactor, VisitMut, VisitorMut};

use crate::Result;
use crate::dialect::{SourceDialect, TargetDialect};
use crate::transforms::functions::{self, FunctionMapping};
use crate::transforms::lateral;
use crate::transforms::types;
use crate::transforms::unnest;

/// AST rewriter that walks expressions and table factors, applying
/// dialect-specific transformations for target-dialect compatibility.
pub struct ExprRewriter {
    pub dialect: SourceDialect,
    pub target: TargetDialect,
    pub function_map: HashMap<&'static str, FunctionMapping>,
    pub errors: Vec<crate::Error>,
}

impl ExprRewriter {
    pub fn new(dialect: SourceDialect, target: TargetDialect) -> Self {
        Self {
            function_map: functions::function_mappings(dialect, target),
            dialect,
            target,
            errors: Vec::new(),
        }
    }

    /// Run the visitor over a statement, returning the first error if any.
    pub fn rewrite(&mut self, stmt: &mut Statement) -> Result<()> {
        let _ = stmt.visit(self);
        if let Some(err) = self.errors.pop() {
            Err(err)
        } else {
            Ok(())
        }
    }
}

impl VisitorMut for ExprRewriter {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        // Rewrite function calls
        if let Expr::Function(func) = expr {
            let name_lower = func.name.to_string().to_lowercase();
            if let Some(mapping) = self.function_map.get(name_lower.as_str()) {
                match functions::apply_mapping(func, mapping) {
                    Ok(None) => {}
                    Ok(Some(replacement)) => {
                        *expr = replacement;
                    }
                    Err(e) => {
                        self.errors.push(e);
                        return ControlFlow::Break(());
                    }
                }
            }
        }

        // Rewrite data types in CAST expressions
        if let Expr::Cast { data_type, .. } = expr
            && let Err(e) = types::rewrite_data_type(data_type, self.dialect, self.target)
        {
            self.errors.push(e);
            return ControlFlow::Break(());
        }

        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        // Rewrite LATERAL VIEW clauses to CROSS JOIN UNNEST before visiting children
        if let SetExpr::Select(select) = query.body.as_mut()
            && !select.lateral_views.is_empty()
            && let Err(e) = lateral::rewrite_lateral_views(select, self.dialect)
        {
            self.errors.push(e);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }

    fn post_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        if let TableFactor::UNNEST { .. } = table_factor
            && let Err(e) = unnest::rewrite_unnest(table_factor, self.dialect)
        {
            self.errors.push(e);
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}
