use super::super::logical_plan::*;
use super::OptimizationRule;

/// Merges views / subqueries by inlining their definition into the parent
/// plan. When a `Project` sits directly on top of another `Project`, the
/// inner projection is folded into the outer one (column substitution).
///
/// This avoids unnecessary materialisation of intermediate projections
/// that are a common artefact of view expansion and subquery-in-FROM.
pub struct ViewMerging;

impl OptimizationRule for ViewMerging {
    fn name(&self) -> &'static str {
        "view_merging"
    }

    fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
        merge(plan)
    }
}

fn merge(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        // Project over Project → fold into one Project by substituting
        // inner aliases with their expressions.
        LogicalPlan::Project {
            expressions: outer_exprs,
            input,
        } => {
            let inner = merge(*input);
            if let LogicalPlan::Project {
                expressions: ref inner_exprs,
                input: ref inner_input,
            } = inner
            {
                // Build a mapping: alias → expr for the inner projection
                let substituted: Vec<ProjectionExpr> = outer_exprs
                    .into_iter()
                    .map(|pe| {
                        let new_expr = substitute_expr(&pe.expr, inner_exprs);
                        ProjectionExpr {
                            expr: new_expr,
                            alias: pe.alias,
                        }
                    })
                    .collect();
                return LogicalPlan::Project {
                    expressions: substituted,
                    input: inner_input.clone(),
                };
            }
            LogicalPlan::Project {
                expressions: outer_exprs,
                input: Box::new(inner),
            }
        }
        // Recurse into other node types
        LogicalPlan::Filter { predicate, input } => LogicalPlan::Filter {
            predicate,
            input: Box::new(merge(*input)),
        },
        LogicalPlan::Join {
            join_type,
            condition,
            left,
            right,
            schema,
        } => LogicalPlan::Join {
            join_type,
            condition,
            left: Box::new(merge(*left)),
            right: Box::new(merge(*right)),
            schema,
        },
        LogicalPlan::Aggregate {
            group_by,
            aggregates,
            input,
            schema,
        } => LogicalPlan::Aggregate {
            group_by,
            aggregates,
            input: Box::new(merge(*input)),
            schema,
        },
        LogicalPlan::Sort { order_by, input } => LogicalPlan::Sort {
            order_by,
            input: Box::new(merge(*input)),
        },
        LogicalPlan::Limit {
            count,
            offset,
            input,
        } => LogicalPlan::Limit {
            count,
            offset,
            input: Box::new(merge(*input)),
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(merge(*input)),
        },
        LogicalPlan::Union { all, left, right } => LogicalPlan::Union {
            all,
            left: Box::new(merge(*left)),
            right: Box::new(merge(*right)),
        },
        LogicalPlan::Intersect { all, left, right } => LogicalPlan::Intersect {
            all,
            left: Box::new(merge(*left)),
            right: Box::new(merge(*right)),
        },
        LogicalPlan::Except { all, left, right } => LogicalPlan::Except {
            all,
            left: Box::new(merge(*left)),
            right: Box::new(merge(*right)),
        },
        other => other,
    }
}

/// Substitute column references in `expr` with the expressions from the
/// inner projection. If a reference matches an inner alias, replace it.
fn substitute_expr(
    expr: &crate::sql::ast::Expr,
    inner: &[ProjectionExpr],
) -> crate::sql::ast::Expr {
    use crate::sql::ast::Expr;
    match expr {
        Expr::Identifier(name) => {
            for pe in inner {
                if pe.alias == *name {
                    return pe.expr.clone();
                }
            }
            expr.clone()
        }
        Expr::QualifiedIdentifier { column, .. } => {
            for pe in inner {
                if pe.alias == *column {
                    return pe.expr.clone();
                }
            }
            expr.clone()
        }
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(substitute_expr(left, inner)),
            op: *op,
            right: Box::new(substitute_expr(right, inner)),
        },
        Expr::UnaryOp { op, expr: e } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(substitute_expr(e, inner)),
        },
        Expr::IsNull { expr: e, negated } => Expr::IsNull {
            expr: Box::new(substitute_expr(e, inner)),
            negated: *negated,
        },
        Expr::Cast { expr: e, data_type } => Expr::Cast {
            expr: Box::new(substitute_expr(e, inner)),
            data_type: *data_type,
        },
        Expr::FunctionCall {
            name,
            args,
            distinct,
        } => Expr::FunctionCall {
            name: name.clone(),
            args: args.iter().map(|a| substitute_expr(a, inner)).collect(),
            distinct: *distinct,
        },
        // Literals and other leaf nodes pass through unchanged
        _ => expr.clone(),
    }
}
