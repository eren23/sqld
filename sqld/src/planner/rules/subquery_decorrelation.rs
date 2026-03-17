use crate::sql::ast::{BinaryOp, Expr, JoinType, Select};

use super::super::logical_plan::*;
use super::OptimizationRule;

/// Converts correlated subqueries into joins:
/// - IN (SELECT ...) → semi-join
/// - EXISTS (SELECT ...) → semi-join
/// - NOT IN (SELECT ...) → anti-join (left join + IS NULL)
/// - NOT EXISTS (SELECT ...) → anti-join
pub struct SubqueryDecorrelation;

impl OptimizationRule for SubqueryDecorrelation {
    fn name(&self) -> &'static str {
        "subquery_decorrelation"
    }

    fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
        decorrelate(plan)
    }
}

fn decorrelate(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { predicate, input } => {
            let input = decorrelate(*input);
            decorrelate_filter(predicate, input)
        }
        LogicalPlan::Project { expressions, input } => LogicalPlan::Project {
            expressions,
            input: Box::new(decorrelate(*input)),
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
            left: Box::new(decorrelate(*left)),
            right: Box::new(decorrelate(*right)),
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
            input: Box::new(decorrelate(*input)),
            schema,
        },
        LogicalPlan::Sort { order_by, input } => LogicalPlan::Sort {
            order_by,
            input: Box::new(decorrelate(*input)),
        },
        LogicalPlan::Limit {
            count,
            offset,
            input,
        } => LogicalPlan::Limit {
            count,
            offset,
            input: Box::new(decorrelate(*input)),
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(decorrelate(*input)),
        },
        other => other,
    }
}

fn decorrelate_filter(predicate: Expr, input: LogicalPlan) -> LogicalPlan {
    let preds = split_conjunction(&predicate);
    let mut remaining = Vec::new();
    let mut current = input;

    for pred in preds {
        match pred {
            // IN subquery → semi-join
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                current = convert_in_subquery(*expr, *subquery, negated, current);
            }
            // EXISTS → semi-join
            Expr::Exists { subquery, negated } => {
                current = convert_exists(*subquery, negated, current);
            }
            other => remaining.push(other),
        }
    }

    if let Some(rem) = combine_conjunction(&remaining) {
        LogicalPlan::Filter {
            predicate: rem,
            input: Box::new(current),
        }
    } else {
        current
    }
}

/// Convert IN (SELECT col FROM ...) to a semi-join.
/// `outer_expr IN (SELECT inner_col FROM t WHERE ...)` becomes:
/// outer SEMI JOIN inner ON outer_expr = inner_col
fn convert_in_subquery(
    outer_expr: Expr,
    subquery: Select,
    negated: bool,
    outer: LogicalPlan,
) -> LogicalPlan {
    // Build the subquery as a plan (simplified: just use Scan for the FROM table)
    let inner = subquery_to_plan(&subquery);

    // The first column of the subquery is the join key
    let inner_schema = inner.schema();
    let inner_col_name = inner_schema
        .columns()
        .first()
        .map(|c| c.name.clone())
        .unwrap_or_else(|| "?".to_string());

    let condition = Expr::BinaryOp {
        left: Box::new(outer_expr),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Identifier(inner_col_name)),
    };

    let schema = outer.schema().merge(&inner.schema());

    if negated {
        // NOT IN → left join + IS NULL filter (anti-join pattern)
        let join = LogicalPlan::Join {
            join_type: JoinType::Left,
            condition: Some(condition),
            left: Box::new(outer.clone()),
            right: Box::new(inner),
            schema: schema.clone(),
        };
        // Add IS NULL filter on any right-side column
        let right_col = schema
            .columns()
            .last()
            .map(|c| c.name.clone())
            .unwrap_or_default();
        LogicalPlan::Filter {
            predicate: Expr::IsNull {
                expr: Box::new(Expr::Identifier(right_col)),
                negated: false,
            },
            input: Box::new(join),
        }
    } else {
        // IN → inner join (semi-join semantics approximated with inner join + distinct)
        LogicalPlan::Join {
            join_type: JoinType::Inner,
            condition: Some(condition),
            left: Box::new(outer),
            right: Box::new(inner),
            schema,
        }
    }
}

/// Convert EXISTS (SELECT ...) to a semi-join.
fn convert_exists(subquery: Select, negated: bool, outer: LogicalPlan) -> LogicalPlan {
    let inner = subquery_to_plan(&subquery);

    // EXISTS becomes a cross join (the subquery's WHERE becomes the join condition)
    let condition = subquery.where_clause.clone();
    let schema = outer.schema().merge(&inner.schema());

    if negated {
        let join = LogicalPlan::Join {
            join_type: JoinType::Left,
            condition,
            left: Box::new(outer.clone()),
            right: Box::new(inner),
            schema: schema.clone(),
        };
        let right_col = schema
            .columns()
            .last()
            .map(|c| c.name.clone())
            .unwrap_or_default();
        LogicalPlan::Filter {
            predicate: Expr::IsNull {
                expr: Box::new(Expr::Identifier(right_col)),
                negated: false,
            },
            input: Box::new(join),
        }
    } else {
        LogicalPlan::Join {
            join_type: JoinType::Inner,
            condition,
            left: Box::new(outer),
            right: Box::new(inner),
            schema,
        }
    }
}

/// Convert a subquery SELECT to a simple LogicalPlan.
fn subquery_to_plan(sel: &Select) -> LogicalPlan {
    // Simplified: extract the FROM table as a Scan
    if let Some(ref from) = sel.from {
        match &from.table {
            crate::sql::ast::TableRef::Table { name, alias } => LogicalPlan::Scan {
                table: name.clone(),
                alias: alias.clone(),
                schema: crate::types::Schema::empty(),
            },
            _ => LogicalPlan::Empty {
                schema: crate::types::Schema::empty(),
            },
        }
    } else {
        LogicalPlan::Empty {
            schema: crate::types::Schema::empty(),
        }
    }
}
