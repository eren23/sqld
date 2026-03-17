use crate::sql::ast::{BinaryOp, Expr, UnaryOp};

use super::super::logical_plan::*;
use super::OptimizationRule;

/// Simplifies logical expressions:
/// - x AND true → x
/// - x AND false → false
/// - x OR true → true
/// - x OR false → x
/// - NOT NOT x → x
/// - x = x → true (for non-nullable columns)
/// - Removes redundant casts, double negations, etc.
pub struct Simplification;

impl OptimizationRule for Simplification {
    fn name(&self) -> &'static str {
        "simplification"
    }

    fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
        simplify_plan(plan)
    }
}

fn simplify_plan(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { predicate, input } => {
            let simplified = simplify_expr(predicate);
            let input = simplify_plan(*input);

            if simplified == Expr::Boolean(true) {
                return input;
            }

            LogicalPlan::Filter {
                predicate: simplified,
                input: Box::new(input),
            }
        }
        LogicalPlan::Project { expressions, input } => {
            let expressions = expressions
                .into_iter()
                .map(|pe| ProjectionExpr {
                    expr: simplify_expr(pe.expr),
                    alias: pe.alias,
                })
                .collect();
            LogicalPlan::Project {
                expressions,
                input: Box::new(simplify_plan(*input)),
            }
        }
        LogicalPlan::Join {
            join_type,
            condition,
            left,
            right,
            schema,
        } => LogicalPlan::Join {
            join_type,
            condition: condition.map(simplify_expr),
            left: Box::new(simplify_plan(*left)),
            right: Box::new(simplify_plan(*right)),
            schema,
        },
        LogicalPlan::Aggregate {
            group_by,
            aggregates,
            input,
            schema,
        } => LogicalPlan::Aggregate {
            group_by: group_by.into_iter().map(simplify_expr).collect(),
            aggregates,
            input: Box::new(simplify_plan(*input)),
            schema,
        },
        LogicalPlan::Sort { order_by, input } => LogicalPlan::Sort {
            order_by,
            input: Box::new(simplify_plan(*input)),
        },
        LogicalPlan::Limit {
            count,
            offset,
            input,
        } => LogicalPlan::Limit {
            count,
            offset,
            input: Box::new(simplify_plan(*input)),
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(simplify_plan(*input)),
        },
        LogicalPlan::Union { all, left, right } => LogicalPlan::Union {
            all,
            left: Box::new(simplify_plan(*left)),
            right: Box::new(simplify_plan(*right)),
        },
        LogicalPlan::Intersect { all, left, right } => LogicalPlan::Intersect {
            all,
            left: Box::new(simplify_plan(*left)),
            right: Box::new(simplify_plan(*right)),
        },
        LogicalPlan::Except { all, left, right } => LogicalPlan::Except {
            all,
            left: Box::new(simplify_plan(*left)),
            right: Box::new(simplify_plan(*right)),
        },
        other => other,
    }
}

pub fn simplify_expr(expr: Expr) -> Expr {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let left = simplify_expr(*left);
            let right = simplify_expr(*right);

            // AND simplifications
            if op == BinaryOp::And {
                if left == Expr::Boolean(true) {
                    return right;
                }
                if right == Expr::Boolean(true) {
                    return left;
                }
                if left == Expr::Boolean(false) || right == Expr::Boolean(false) {
                    return Expr::Boolean(false);
                }
                // x AND x → x
                if left == right {
                    return left;
                }
            }

            // OR simplifications
            if op == BinaryOp::Or {
                if left == Expr::Boolean(false) {
                    return right;
                }
                if right == Expr::Boolean(false) {
                    return left;
                }
                if left == Expr::Boolean(true) || right == Expr::Boolean(true) {
                    return Expr::Boolean(true);
                }
                // x OR x → x
                if left == right {
                    return left;
                }
            }

            // x + 0 → x, 0 + x → x
            if op == BinaryOp::Add {
                if right == Expr::Integer(0) {
                    return left;
                }
                if left == Expr::Integer(0) {
                    return right;
                }
            }

            // x * 1 → x, 1 * x → x
            if op == BinaryOp::Mul {
                if right == Expr::Integer(1) {
                    return left;
                }
                if left == Expr::Integer(1) {
                    return right;
                }
                // x * 0 → 0
                if right == Expr::Integer(0) || left == Expr::Integer(0) {
                    return Expr::Integer(0);
                }
            }

            // x - 0 → x
            if op == BinaryOp::Sub && right == Expr::Integer(0) {
                return left;
            }

            Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            }
        }

        Expr::UnaryOp { op, expr } => {
            let inner = simplify_expr(*expr);

            // NOT NOT x → x
            if op == UnaryOp::Not {
                if let Expr::UnaryOp {
                    op: UnaryOp::Not,
                    expr: inner2,
                } = inner
                {
                    return *inner2;
                }
            }

            // +x → x for literals
            if op == UnaryOp::Plus {
                if matches!(&inner, Expr::Integer(_) | Expr::Float(_)) {
                    return inner;
                }
            }

            Expr::UnaryOp {
                op,
                expr: Box::new(inner),
            }
        }

        Expr::IsNull { expr, negated } => {
            let inner = simplify_expr(*expr);
            match &inner {
                Expr::Null => Expr::Boolean(!negated),
                Expr::Integer(_) | Expr::Float(_) | Expr::String(_) | Expr::Boolean(_) => {
                    Expr::Boolean(negated)
                }
                _ => Expr::IsNull {
                    expr: Box::new(inner),
                    negated,
                },
            }
        }

        other => other,
    }
}
