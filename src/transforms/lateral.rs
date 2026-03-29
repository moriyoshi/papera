use sqlparser::ast::{
    Expr, Ident, Join, JoinConstraint, JoinOperator, Select, TableAlias, TableAliasColumnDef,
    TableFactor,
};

use crate::Result;
use crate::dialect::SourceDialect;

/// Transform Hive-style LATERAL VIEW clauses to CROSS JOIN UNNEST for DuckDB.
///
/// Hive: `SELECT ... FROM t LATERAL VIEW explode(arr) AS x`
/// DuckDB: `SELECT ... FROM t CROSS JOIN UNNEST(arr) AS __lv(x)`
///
/// This is mainly relevant for Trino queries that use Hive-compatible syntax.
pub fn rewrite_lateral_views(select: &mut Select, _dialect: SourceDialect) -> Result<()> {
    if select.lateral_views.is_empty() {
        return Ok(());
    }

    let lateral_views = std::mem::take(&mut select.lateral_views);

    for (i, lv) in lateral_views.into_iter().enumerate() {
        // Extract the array expression from the LATERAL VIEW.
        // The lateral_view field is the function call (e.g., explode(arr)).
        // For DuckDB, we need UNNEST(arr) instead.
        let array_expr = match &lv.lateral_view {
            Expr::Function(func) => {
                // Extract the first argument from explode/posexplode
                let func_name = func.name.to_string().to_lowercase();
                match func_name.as_str() {
                    "explode" | "posexplode" | "explode_outer" | "posexplode_outer" => {
                        // Extract first arg
                        extract_first_arg(func)
                    }
                    _ => {
                        // Unknown function — pass through the whole expression
                        Some(lv.lateral_view.clone())
                    }
                }
            }
            other => Some(other.clone()),
        };

        let array_expr = array_expr.unwrap_or(lv.lateral_view.clone());

        // Build alias from lateral_col_alias
        let alias_name = if lv.lateral_view_name.0.is_empty() {
            Ident::new(format!("__lv{i}"))
        } else {
            lv.lateral_view_name
                .0
                .last()
                .unwrap()
                .as_ident()
                .cloned()
                .unwrap_or_else(|| Ident::new(format!("__lv{i}")))
        };

        let col_aliases = lv.lateral_col_alias;

        let unnest = TableFactor::UNNEST {
            alias: Some(TableAlias {
                name: alias_name,
                columns: col_aliases
                    .into_iter()
                    .map(|c| TableAliasColumnDef {
                        name: c,
                        data_type: None,
                    })
                    .collect(),
                explicit: true,
            }),
            array_exprs: vec![array_expr],
            with_offset: false,
            with_offset_alias: None,
            with_ordinality: false,
        };

        let join = Join {
            relation: unnest,
            global: false,
            join_operator: JoinOperator::CrossJoin(JoinConstraint::None),
        };

        // Add the join to the first FROM item
        if let Some(first_from) = select.from.first_mut() {
            first_from.joins.push(join);
        }
    }

    Ok(())
}

fn extract_first_arg(func: &sqlparser::ast::Function) -> Option<Expr> {
    match &func.args {
        sqlparser::ast::FunctionArguments::List(list) => {
            list.args.first().and_then(|arg| match arg {
                sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => {
                    Some(e.clone())
                }
                _ => None,
            })
        }
        _ => None,
    }
}
