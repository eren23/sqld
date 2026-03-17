use crate::sql::ast::{BinaryOp, Expr, UnaryOp};

use super::super::logical_plan::*;
use super::OptimizationRule;

/// Evaluates constant expressions at plan time.
/// Examples: 1 + 2 → 3, true AND x → x, 'a' || 'b' → 'ab'.
pub struct ConstantFolding;

impl OptimizationRule for ConstantFolding {
    fn name(&self) -> &'static str {
        "constant_folding"
    }

    fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
        fold_plan(plan)
    }
}

fn fold_plan(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { predicate, input } => {
            let folded_pred = fold_expr(predicate);
            let input = fold_plan(*input);

            // If predicate folded to TRUE, eliminate the filter
            if folded_pred == Expr::Boolean(true) {
                return input;
            }
            // If predicate folded to FALSE, return empty
            if folded_pred == Expr::Boolean(false) {
                return LogicalPlan::Empty {
                    schema: input.schema(),
                };
            }

            LogicalPlan::Filter {
                predicate: folded_pred,
                input: Box::new(input),
            }
        }
        LogicalPlan::Project { expressions, input } => {
            let expressions = expressions
                .into_iter()
                .map(|pe| ProjectionExpr {
                    expr: fold_expr(pe.expr),
                    alias: pe.alias,
                })
                .collect();
            LogicalPlan::Project {
                expressions,
                input: Box::new(fold_plan(*input)),
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
            condition: condition.map(fold_expr),
            left: Box::new(fold_plan(*left)),
            right: Box::new(fold_plan(*right)),
            schema,
        },
        LogicalPlan::Aggregate {
            group_by,
            aggregates,
            input,
            schema,
        } => LogicalPlan::Aggregate {
            group_by: group_by.into_iter().map(fold_expr).collect(),
            aggregates,
            input: Box::new(fold_plan(*input)),
            schema,
        },
        LogicalPlan::Sort { order_by, input } => LogicalPlan::Sort {
            order_by,
            input: Box::new(fold_plan(*input)),
        },
        LogicalPlan::Limit {
            count,
            offset,
            input,
        } => LogicalPlan::Limit {
            count,
            offset,
            input: Box::new(fold_plan(*input)),
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(fold_plan(*input)),
        },
        LogicalPlan::Union { all, left, right } => LogicalPlan::Union {
            all,
            left: Box::new(fold_plan(*left)),
            right: Box::new(fold_plan(*right)),
        },
        LogicalPlan::Intersect { all, left, right } => LogicalPlan::Intersect {
            all,
            left: Box::new(fold_plan(*left)),
            right: Box::new(fold_plan(*right)),
        },
        LogicalPlan::Except { all, left, right } => LogicalPlan::Except {
            all,
            left: Box::new(fold_plan(*left)),
            right: Box::new(fold_plan(*right)),
        },
        other => other,
    }
}

/// Fold a single expression, evaluating constant sub-expressions.
pub fn fold_expr(expr: Expr) -> Expr {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let left = fold_expr(*left);
            let right = fold_expr(*right);
            fold_binary(left, op, right)
        }
        Expr::UnaryOp { op, expr } => {
            let inner = fold_expr(*expr);
            fold_unary(op, inner)
        }
        Expr::IsNull { expr, negated } => {
            let inner = fold_expr(*expr);
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
        Expr::Cast { expr, data_type } => {
            let inner = fold_expr(*expr);
            Expr::Cast {
                expr: Box::new(inner),
                data_type,
            }
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: Box::new(fold_expr(*expr)),
            low: Box::new(fold_expr(*low)),
            high: Box::new(fold_expr(*high)),
            negated,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(fold_expr(*expr)),
            list: list.into_iter().map(fold_expr).collect(),
            negated,
        },
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            let operand = operand.map(|e| Box::new(fold_expr(*e)));
            let when_clauses = when_clauses
                .into_iter()
                .map(|wc| crate::sql::ast::WhenClause {
                    condition: fold_expr(wc.condition),
                    result: fold_expr(wc.result),
                })
                .collect();
            let else_clause = else_clause.map(|e| Box::new(fold_expr(*e)));
            Expr::Case {
                operand,
                when_clauses,
                else_clause,
            }
        }
        other => other,
    }
}

fn fold_binary(left: Expr, op: BinaryOp, right: Expr) -> Expr {
    // Integer arithmetic
    if let (Expr::Integer(a), Expr::Integer(b)) = (&left, &right) {
        match op {
            BinaryOp::Add => return Expr::Integer(a + b),
            BinaryOp::Sub => return Expr::Integer(a - b),
            BinaryOp::Mul => return Expr::Integer(a * b),
            BinaryOp::Div if *b != 0 => return Expr::Integer(a / b),
            BinaryOp::Mod if *b != 0 => return Expr::Integer(a % b),
            BinaryOp::Eq => return Expr::Boolean(a == b),
            BinaryOp::NotEq => return Expr::Boolean(a != b),
            BinaryOp::Lt => return Expr::Boolean(a < b),
            BinaryOp::Gt => return Expr::Boolean(a > b),
            BinaryOp::LtEq => return Expr::Boolean(a <= b),
            BinaryOp::GtEq => return Expr::Boolean(a >= b),
            _ => {}
        }
    }

    // Float arithmetic
    if let (Expr::Float(a), Expr::Float(b)) = (&left, &right) {
        match op {
            BinaryOp::Add => return Expr::Float(a + b),
            BinaryOp::Sub => return Expr::Float(a - b),
            BinaryOp::Mul => return Expr::Float(a * b),
            BinaryOp::Div if *b != 0.0 => return Expr::Float(a / b),
            _ => {}
        }
    }

    // String concatenation
    if let (Expr::String(a), Expr::String(b)) = (&left, &right) {
        if op == BinaryOp::Concat {
            return Expr::String(format!("{}{}", a, b));
        }
    }

    // Boolean logic
    if let (Expr::Boolean(a), Expr::Boolean(b)) = (&left, &right) {
        match op {
            BinaryOp::And => return Expr::Boolean(*a && *b),
            BinaryOp::Or => return Expr::Boolean(*a || *b),
            BinaryOp::Eq => return Expr::Boolean(a == b),
            BinaryOp::NotEq => return Expr::Boolean(a != b),
            _ => {}
        }
    }

    // AND simplifications with one constant
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
    }

    // OR simplifications with one constant
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
    }

    Expr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

fn fold_unary(op: UnaryOp, inner: Expr) -> Expr {
    match (op, &inner) {
        (UnaryOp::Minus, Expr::Integer(n)) => Expr::Integer(-n),
        (UnaryOp::Minus, Expr::Float(n)) => Expr::Float(-n),
        (UnaryOp::Plus, Expr::Integer(_)) | (UnaryOp::Plus, Expr::Float(_)) => inner,
        (UnaryOp::Not, Expr::Boolean(b)) => Expr::Boolean(!b),
        // NOT NOT x → x
        (UnaryOp::Not, Expr::UnaryOp { op: UnaryOp::Not, expr }) => *expr.clone(),
        _ => Expr::UnaryOp {
            op,
            expr: Box::new(inner),
        },
    }
}
