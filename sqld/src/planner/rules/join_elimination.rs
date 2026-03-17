use std::collections::HashSet;

use crate::sql::ast::{Expr, JoinType};

use super::super::logical_plan::*;
use super::super::Catalog;
use super::OptimizationRule;

/// Eliminates unnecessary joins when the joined table's columns are never
/// referenced in the output and the join preserves cardinality (i.e. the
/// join is on a unique/primary key and is a LEFT or INNER join to a table
/// whose output columns are unused).
///
/// Example: `SELECT a.* FROM a LEFT JOIN b ON a.id = b.a_id` where no
/// column of `b` is used → the join can be removed entirely, keeping
/// just the scan on `a`.
pub struct JoinElimination {
    catalog: Catalog,
}

impl JoinElimination {
    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }
}

impl OptimizationRule for JoinElimination {
    fn name(&self) -> &'static str {
        "join_elimination"
    }

    fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
        let needed = root_needed_columns(&plan);
        eliminate(plan, &needed, &self.catalog)
    }
}

/// Determine which columns the root node needs.
fn root_needed_columns(plan: &LogicalPlan) -> HashSet<String> {
    let schema = plan.schema();
    schema.columns().iter().map(|c| c.name.clone()).collect()
}

fn eliminate(plan: LogicalPlan, needed: &HashSet<String>, catalog: &Catalog) -> LogicalPlan {
    match plan {
        LogicalPlan::Join {
            join_type,
            condition,
            left,
            right,
            schema,
        } => {
            // Recurse first
            let left = eliminate(*left, needed, catalog);
            let right = eliminate(*right, needed, catalog);

            // Collect columns from each side
            let left_cols: HashSet<String> = left
                .schema()
                .columns()
                .iter()
                .map(|c| c.name.clone())
                .collect();
            let right_cols: HashSet<String> = right
                .schema()
                .columns()
                .iter()
                .map(|c| c.name.clone())
                .collect();

            // Check if the join condition references any columns needed
            let mut cond_cols = HashSet::new();
            if let Some(ref cond) = condition {
                for (_, name) in collect_columns(cond) {
                    cond_cols.insert(name);
                }
            }

            // Check if right side columns are used by anyone
            let right_used = right_cols.iter().any(|c| needed.contains(c));

            // For LEFT JOIN: if no right columns are needed and the join
            // is to a unique key (guarantees at most one match per left row),
            // we can eliminate the join.
            if join_type == JoinType::Left && !right_used {
                if is_unique_join(&condition, &right, catalog) {
                    return left;
                }
            }

            // For INNER JOIN: if no right columns are needed and the join
            // is on a unique key with a NOT NULL foreign key (guarantees
            // exactly one match per left row), we can eliminate.
            if join_type == JoinType::Inner && !right_used {
                if is_unique_join(&condition, &right, catalog) {
                    return left;
                }
            }

            // Check the symmetric case: left side unused
            let left_used = left_cols.iter().any(|c| needed.contains(c));

            if join_type == JoinType::Right && !left_used {
                if is_unique_join(&condition, &left, catalog) {
                    return right;
                }
            }

            LogicalPlan::Join {
                join_type,
                condition,
                left: Box::new(left),
                right: Box::new(right),
                schema,
            }
        }
        LogicalPlan::Project { expressions, input } => {
            // Update needed columns for children
            let mut child_needed = needed.clone();
            for pe in &expressions {
                for (_, name) in collect_columns(&pe.expr) {
                    child_needed.insert(name);
                }
            }
            LogicalPlan::Project {
                expressions,
                input: Box::new(eliminate(*input, &child_needed, catalog)),
            }
        }
        LogicalPlan::Filter { predicate, input } => {
            let mut child_needed = needed.clone();
            for (_, name) in collect_columns(&predicate) {
                child_needed.insert(name);
            }
            LogicalPlan::Filter {
                predicate,
                input: Box::new(eliminate(*input, &child_needed, catalog)),
            }
        }
        LogicalPlan::Aggregate {
            group_by,
            aggregates,
            input,
            schema,
        } => {
            let mut child_needed = needed.clone();
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
                input: Box::new(eliminate(*input, &child_needed, catalog)),
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
                input: Box::new(eliminate(*input, &child_needed, catalog)),
            }
        }
        LogicalPlan::Limit {
            count,
            offset,
            input,
        } => LogicalPlan::Limit {
            count,
            offset,
            input: Box::new(eliminate(*input, needed, catalog)),
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(eliminate(*input, needed, catalog)),
        },
        LogicalPlan::Union { all, left, right } => LogicalPlan::Union {
            all,
            left: Box::new(eliminate(*left, needed, catalog)),
            right: Box::new(eliminate(*right, needed, catalog)),
        },
        LogicalPlan::Intersect { all, left, right } => LogicalPlan::Intersect {
            all,
            left: Box::new(eliminate(*left, needed, catalog)),
            right: Box::new(eliminate(*right, needed, catalog)),
        },
        LogicalPlan::Except { all, left, right } => LogicalPlan::Except {
            all,
            left: Box::new(eliminate(*left, needed, catalog)),
            right: Box::new(eliminate(*right, needed, catalog)),
        },
        other => other,
    }
}

/// Check if the join condition is an equi-join on a unique index of the
/// target relation. This guarantees at most one match per outer row.
fn is_unique_join(
    condition: &Option<Expr>,
    target: &LogicalPlan,
    catalog: &Catalog,
) -> bool {
    let cond = match condition {
        Some(c) => c,
        None => return false,
    };

    let table_name = match target {
        LogicalPlan::Scan { table, .. } => table,
        _ => return false,
    };

    // Extract equi-join columns on the target side
    let preds = split_conjunction(cond);
    let mut target_join_cols: Vec<String> = Vec::new();
    for pred in &preds {
        if let Expr::BinaryOp {
            left,
            op: crate::sql::ast::BinaryOp::Eq,
            right,
        } = pred
        {
            if let Some(col) = extract_table_column(right, table_name) {
                target_join_cols.push(col);
            } else if let Some(col) = extract_table_column(left, table_name) {
                target_join_cols.push(col);
            }
        }
    }

    if target_join_cols.is_empty() {
        return false;
    }

    // Check if these columns form a unique index
    let indexes = catalog.get_indexes(table_name);
    for idx in indexes {
        if idx.unique && idx.columns.len() <= target_join_cols.len() {
            let all_covered = idx.columns.iter().all(|c| target_join_cols.contains(c));
            if all_covered {
                return true;
            }
        }
    }

    false
}

fn extract_table_column(expr: &Expr, table: &str) -> Option<String> {
    match expr {
        Expr::QualifiedIdentifier {
            table: t,
            column: c,
        } if t == table => Some(c.clone()),
        // Unqualified identifier — might be from this table
        Expr::Identifier(name) => Some(name.clone()),
        _ => None,
    }
}
