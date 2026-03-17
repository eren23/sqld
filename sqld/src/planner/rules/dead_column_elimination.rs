use std::collections::HashSet;

use super::super::logical_plan::*;
use super::OptimizationRule;

/// Removes columns from projections that are never referenced by any
/// ancestor node, reducing data transfer between operators.
pub struct DeadColumnElimination;

impl OptimizationRule for DeadColumnElimination {
    fn name(&self) -> &'static str {
        "dead_column_elimination"
    }

    fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
        // Collect all columns needed at the root, then propagate down
        let needed = root_needed_columns(&plan);
        eliminate(plan, &needed)
    }
}

/// Determine which columns the root node needs.
fn root_needed_columns(plan: &LogicalPlan) -> HashSet<String> {
    let schema = plan.schema();
    schema.columns().iter().map(|c| c.name.clone()).collect()
}

fn eliminate(plan: LogicalPlan, needed: &HashSet<String>) -> LogicalPlan {
    match plan {
        LogicalPlan::Project { expressions, input } => {
            // Remove expressions whose alias is not needed
            let filtered: Vec<ProjectionExpr> = expressions
                .into_iter()
                .filter(|pe| needed.contains(&pe.alias))
                .collect();

            if filtered.is_empty() {
                // Don't create an empty projection; keep at least one column
                return eliminate(*input, needed);
            }

            // Collect columns needed by the remaining projection expressions
            let child_needed: HashSet<String> = filtered
                .iter()
                .flat_map(|pe| collect_columns(&pe.expr).into_iter().map(|(_, n)| n))
                .collect();

            LogicalPlan::Project {
                expressions: filtered,
                input: Box::new(eliminate(*input, &child_needed)),
            }
        }
        LogicalPlan::Filter { predicate, input } => {
            // Filter needs its predicate columns plus what the parent needs
            let pred_cols = collect_columns(&predicate);
            let mut child_needed = needed.clone();
            for (_, name) in pred_cols {
                child_needed.insert(name);
            }
            LogicalPlan::Filter {
                predicate,
                input: Box::new(eliminate(*input, &child_needed)),
            }
        }
        LogicalPlan::Join {
            join_type,
            condition,
            left,
            right,
            schema,
        } => {
            let mut child_needed = needed.clone();
            if let Some(ref cond) = condition {
                for (_, name) in collect_columns(cond) {
                    child_needed.insert(name);
                }
            }
            LogicalPlan::Join {
                join_type,
                condition,
                left: Box::new(eliminate(*left, &child_needed)),
                right: Box::new(eliminate(*right, &child_needed)),
                schema,
            }
        }
        LogicalPlan::Aggregate {
            group_by,
            aggregates,
            input,
            schema,
        } => {
            // Aggregate needs its group-by and aggregate input columns
            let mut child_needed: HashSet<String> = HashSet::new();
            for expr in &group_by {
                for (_, name) in collect_columns(expr) {
                    child_needed.insert(name);
                }
            }
            for agg in &aggregates {
                for (_, name) in collect_columns(&agg.arg) {
                    child_needed.insert(name);
                }
            }
            LogicalPlan::Aggregate {
                group_by,
                aggregates,
                input: Box::new(eliminate(*input, &child_needed)),
                schema,
            }
        }
        LogicalPlan::Sort { order_by, input } => {
            let mut child_needed = needed.clone();
            for se in &order_by {
                for (_, name) in collect_columns(&se.expr) {
                    child_needed.insert(name);
                }
            }
            LogicalPlan::Sort {
                order_by,
                input: Box::new(eliminate(*input, &child_needed)),
            }
        }
        // For all other nodes, just recurse
        LogicalPlan::Limit {
            count,
            offset,
            input,
        } => LogicalPlan::Limit {
            count,
            offset,
            input: Box::new(eliminate(*input, needed)),
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(eliminate(*input, needed)),
        },
        other => other,
    }
}
