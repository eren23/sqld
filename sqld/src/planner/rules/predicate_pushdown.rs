use crate::sql::ast::{Expr, JoinType};

use super::super::logical_plan::*;
use super::OptimizationRule;

/// Pushes filter predicates down through joins, projections, and other nodes
/// to reduce the number of rows processed early in the plan.
pub struct PredicatePushdown;

impl OptimizationRule for PredicatePushdown {
    fn name(&self) -> &'static str {
        "predicate_pushdown"
    }

    fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
        push_down(plan)
    }
}

fn push_down(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter { predicate, input } => {
            let input = push_down(*input);
            push_filter_into(predicate, input)
        }
        // Recurse into children for all other node types
        LogicalPlan::Project { expressions, input } => LogicalPlan::Project {
            expressions,
            input: Box::new(push_down(*input)),
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
            left: Box::new(push_down(*left)),
            right: Box::new(push_down(*right)),
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
            input: Box::new(push_down(*input)),
            schema,
        },
        LogicalPlan::Sort { order_by, input } => LogicalPlan::Sort {
            order_by,
            input: Box::new(push_down(*input)),
        },
        LogicalPlan::Limit {
            count,
            offset,
            input,
        } => LogicalPlan::Limit {
            count,
            offset,
            input: Box::new(push_down(*input)),
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(push_down(*input)),
        },
        LogicalPlan::Union { all, left, right } => LogicalPlan::Union {
            all,
            left: Box::new(push_down(*left)),
            right: Box::new(push_down(*right)),
        },
        LogicalPlan::Intersect { all, left, right } => LogicalPlan::Intersect {
            all,
            left: Box::new(push_down(*left)),
            right: Box::new(push_down(*right)),
        },
        LogicalPlan::Except { all, left, right } => LogicalPlan::Except {
            all,
            left: Box::new(push_down(*left)),
            right: Box::new(push_down(*right)),
        },
        other => other,
    }
}

/// Try to push a filter predicate into the given plan node.
fn push_filter_into(predicate: Expr, plan: LogicalPlan) -> LogicalPlan {
    match plan {
        // Push through projection if predicate only references projected columns
        LogicalPlan::Project { expressions, input } => {
            // Try to push through; if the predicate only uses columns from the input,
            // we can push it below the projection.
            let input_with_filter = push_filter_into(predicate, *input);
            LogicalPlan::Project {
                expressions,
                input: Box::new(input_with_filter),
            }
        }

        // Push into join
        LogicalPlan::Join {
            join_type,
            condition,
            left,
            right,
            schema,
        } => push_filter_into_join(predicate, join_type, condition, *left, *right, schema),

        // Push through sort (sort doesn't affect rows)
        LogicalPlan::Sort { order_by, input } => {
            let input = push_filter_into(predicate, *input);
            LogicalPlan::Sort {
                order_by,
                input: Box::new(input),
            }
        }

        // Push through limit is not safe (changes semantics), keep filter above
        // Push through aggregate is not safe either (predicate applies to pre-agg data)
        // For aggregate, only push if predicate references only group-by columns
        LogicalPlan::Aggregate {
            group_by,
            aggregates,
            input,
            schema,
        } => {
            let preds = split_conjunction(&predicate);
            let mut pushable = Vec::new();
            let mut remaining = Vec::new();

            for pred in preds {
                let cols = collect_columns(&pred);
                let group_col_names: Vec<String> = group_by
                    .iter()
                    .filter_map(|e| match e {
                        Expr::Identifier(name) => Some(name.clone()),
                        Expr::QualifiedIdentifier { column, .. } => Some(column.clone()),
                        _ => None,
                    })
                    .collect();

                let is_group_only = cols.iter().all(|(_, col)| group_col_names.contains(col));
                if is_group_only {
                    pushable.push(pred);
                } else {
                    remaining.push(pred);
                }
            }

            let mut agg_input = *input;
            if let Some(push_pred) = combine_conjunction(&pushable) {
                agg_input = push_filter_into(push_pred, agg_input);
            }

            let mut result = LogicalPlan::Aggregate {
                group_by,
                aggregates,
                input: Box::new(agg_input),
                schema,
            };

            if let Some(rem_pred) = combine_conjunction(&remaining) {
                result = LogicalPlan::Filter {
                    predicate: rem_pred,
                    input: Box::new(result),
                };
            }

            result
        }

        // Can't push further: put filter on top
        other => LogicalPlan::Filter {
            predicate,
            input: Box::new(other),
        },
    }
}

/// Push a filter predicate into a join. Splits the predicate into:
/// - predicates that only reference the left side
/// - predicates that only reference the right side
/// - predicates that reference both (remain as join condition or filter above)
fn push_filter_into_join(
    predicate: Expr,
    join_type: JoinType,
    condition: Option<Expr>,
    left: LogicalPlan,
    right: LogicalPlan,
    schema: crate::types::Schema,
) -> LogicalPlan {
    let preds = split_conjunction(&predicate);
    let left_tables = collect_scan_tables(&left);
    let right_tables = collect_scan_tables(&right);

    let left_refs: Vec<&str> = left_tables.iter().map(|s| s.as_str()).collect();
    let right_refs: Vec<&str> = right_tables.iter().map(|s| s.as_str()).collect();

    let mut left_preds = Vec::new();
    let mut right_preds = Vec::new();
    let mut join_preds = Vec::new();

    for pred in preds {
        let tables = referenced_tables(&pred);
        if tables.is_empty() {
            // Constant predicate — push to left
            left_preds.push(pred);
        } else if tables.iter().all(|t| left_refs.contains(&t.as_str())) {
            match join_type {
                JoinType::Inner | JoinType::Cross => left_preds.push(pred),
                JoinType::Left => left_preds.push(pred),
                // For right/full joins, we can't push left predicates below
                _ => join_preds.push(pred),
            }
        } else if tables.iter().all(|t| right_refs.contains(&t.as_str())) {
            match join_type {
                JoinType::Inner | JoinType::Cross => right_preds.push(pred),
                JoinType::Right => right_preds.push(pred),
                // For left/full joins, we can't push right predicates below
                _ => join_preds.push(pred),
            }
        } else {
            join_preds.push(pred);
        }
    }

    // Apply pushed predicates
    let new_left = if let Some(pred) = combine_conjunction(&left_preds) {
        LogicalPlan::Filter {
            predicate: pred,
            input: Box::new(left),
        }
    } else {
        left
    };

    let new_right = if let Some(pred) = combine_conjunction(&right_preds) {
        LogicalPlan::Filter {
            predicate: pred,
            input: Box::new(right),
        }
    } else {
        right
    };

    // Merge remaining predicates with existing join condition
    let mut all_join_conds = Vec::new();
    if let Some(cond) = condition {
        all_join_conds.extend(split_conjunction(&cond));
    }
    all_join_conds.extend(join_preds);

    let new_condition = combine_conjunction(&all_join_conds);

    LogicalPlan::Join {
        join_type,
        condition: new_condition,
        left: Box::new(new_left),
        right: Box::new(new_right),
        schema,
    }
}

/// Collect all table names from Scan nodes in a plan tree.
fn collect_scan_tables(plan: &LogicalPlan) -> Vec<String> {
    let mut tables = Vec::new();
    collect_scan_tables_inner(plan, &mut tables);
    tables
}

fn collect_scan_tables_inner(plan: &LogicalPlan, out: &mut Vec<String>) {
    match plan {
        LogicalPlan::Scan { table, alias, .. } => {
            out.push(alias.as_ref().unwrap_or(table).clone());
        }
        other => {
            for child in other.children() {
                collect_scan_tables_inner(child, out);
            }
        }
    }
}
