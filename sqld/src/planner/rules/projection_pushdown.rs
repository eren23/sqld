use std::collections::HashSet;

use super::super::logical_plan::*;
use super::OptimizationRule;

/// Pushes projections down through plan nodes to eliminate unnecessary columns
/// early, reducing memory and I/O.
pub struct ProjectionPushdown;

impl OptimizationRule for ProjectionPushdown {
    fn name(&self) -> &'static str {
        "projection_pushdown"
    }

    fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
        push_projections(plan)
    }
}

fn push_projections(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Project { expressions, input } => {
            // Collect columns needed by the projection
            let needed: HashSet<String> = expressions
                .iter()
                .flat_map(|pe| {
                    let cols = collect_columns(&pe.expr);
                    cols.into_iter().map(|(_, name)| name)
                })
                .collect();

            let input = push_needed_columns(*input, &needed);
            LogicalPlan::Project {
                expressions,
                input: Box::new(input),
            }
        }
        // For other nodes, recurse
        LogicalPlan::Filter { predicate, input } => {
            let input = push_projections(*input);
            LogicalPlan::Filter {
                predicate,
                input: Box::new(input),
            }
        }
        LogicalPlan::Sort { order_by, input } => {
            let input = push_projections(*input);
            LogicalPlan::Sort {
                order_by,
                input: Box::new(input),
            }
        }
        LogicalPlan::Limit {
            count,
            offset,
            input,
        } => LogicalPlan::Limit {
            count,
            offset,
            input: Box::new(push_projections(*input)),
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(push_projections(*input)),
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
            left: Box::new(push_projections(*left)),
            right: Box::new(push_projections(*right)),
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
            input: Box::new(push_projections(*input)),
            schema,
        },
        other => other,
    }
}

/// Push a set of needed column names down into the plan, potentially adding
/// projections to narrow scans.
fn push_needed_columns(plan: LogicalPlan, needed: &HashSet<String>) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan {
            table,
            alias,
            schema,
        } => {
            // Don't narrow the scan schema: the executor returns full tuples
            // from the storage layer, so column indices must match the full
            // table schema. The Project operator above will select the
            // columns it needs.
            LogicalPlan::Scan {
                table,
                alias,
                schema,
            }
        }
        LogicalPlan::Filter { predicate, input } => {
            // Add columns needed by the predicate
            let pred_cols = collect_columns(&predicate);
            let mut extended = needed.clone();
            for (_, name) in pred_cols {
                extended.insert(name);
            }
            let input = push_needed_columns(*input, &extended);
            LogicalPlan::Filter {
                predicate,
                input: Box::new(input),
            }
        }
        LogicalPlan::Join {
            join_type,
            condition,
            left,
            right,
            schema: _,
        } => {
            // Add columns needed by the join condition
            let mut extended = needed.clone();
            if let Some(ref cond) = condition {
                for (_, name) in collect_columns(cond) {
                    extended.insert(name);
                }
            }

            let left = push_needed_columns(*left, &extended);
            let right = push_needed_columns(*right, &extended);

            // Rebuild schema based on remaining columns
            let new_schema = left.schema().merge(&right.schema());

            LogicalPlan::Join {
                join_type,
                condition,
                left: Box::new(left),
                right: Box::new(right),
                schema: new_schema,
            }
        }
        other => other,
    }
}
