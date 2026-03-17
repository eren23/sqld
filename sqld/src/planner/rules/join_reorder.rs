use std::collections::{HashMap, HashSet};

use crate::sql::ast::{Expr, JoinType};
use crate::types::Schema;

use super::super::logical_plan::*;
use super::super::cardinality::CardinalityEstimator;
use super::super::Catalog;
use super::OptimizationRule;

/// Reorders joins to minimize estimated cost.
/// - DPccp for ≤6 tables (enumerates connected subgraph pairs)
/// - Greedy for 7-12 tables
/// - Left-deep greedy for >12 tables
pub struct JoinReorder {
    catalog: Catalog,
}

impl JoinReorder {
    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }
}

impl OptimizationRule for JoinReorder {
    fn name(&self) -> &'static str {
        "join_reorder"
    }

    fn apply(&self, plan: LogicalPlan) -> LogicalPlan {
        reorder(plan, &self.catalog)
    }
}

fn reorder(plan: LogicalPlan, catalog: &Catalog) -> LogicalPlan {
    match plan {
        // Only reorder inner joins (outer joins have fixed ordering)
        LogicalPlan::Join { join_type: JoinType::Inner, .. } => {
            // Flatten the join tree into base relations and predicates
            let mut relations = Vec::new();
            let mut predicates = Vec::new();
            flatten_inner_joins(&plan, &mut relations, &mut predicates);

            let n = relations.len();
            if n <= 1 {
                return plan;
            }

            if n <= 6 {
                dp_join_order(&relations, &predicates, catalog)
            } else if n <= 12 {
                greedy_join_order(&relations, &predicates, catalog)
            } else {
                left_deep_greedy(&relations, &predicates, catalog)
            }
        }
        // Recurse into children
        LogicalPlan::Filter { predicate, input } => LogicalPlan::Filter {
            predicate,
            input: Box::new(reorder(*input, catalog)),
        },
        LogicalPlan::Project { expressions, input } => LogicalPlan::Project {
            expressions,
            input: Box::new(reorder(*input, catalog)),
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
            left: Box::new(reorder(*left, catalog)),
            right: Box::new(reorder(*right, catalog)),
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
            input: Box::new(reorder(*input, catalog)),
            schema,
        },
        LogicalPlan::Sort { order_by, input } => LogicalPlan::Sort {
            order_by,
            input: Box::new(reorder(*input, catalog)),
        },
        LogicalPlan::Limit { count, offset, input } => LogicalPlan::Limit {
            count,
            offset,
            input: Box::new(reorder(*input, catalog)),
        },
        other => other,
    }
}

/// Flatten a tree of INNER JOINs into a list of base relations and predicates.
fn flatten_inner_joins(
    plan: &LogicalPlan,
    relations: &mut Vec<LogicalPlan>,
    predicates: &mut Vec<Expr>,
) {
    match plan {
        LogicalPlan::Join {
            join_type: JoinType::Inner,
            condition,
            left,
            right,
            ..
        } => {
            flatten_inner_joins(left, relations, predicates);
            flatten_inner_joins(right, relations, predicates);
            if let Some(cond) = condition {
                predicates.extend(split_conjunction(cond));
            }
        }
        other => {
            relations.push(other.clone());
        }
    }
}

/// Get the table name from a plan (for matching predicates to relations).
fn plan_table_name(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::Scan { table, alias, .. } => {
            Some(alias.as_ref().unwrap_or(table).clone())
        }
        _ => None,
    }
}

/// Estimate the cardinality of a base relation.
fn estimate_base_card(plan: &LogicalPlan, catalog: &Catalog) -> f64 {
    match plan {
        LogicalPlan::Scan { table, .. } => {
            let stats = catalog.get_stats(table);
            stats.row_count
        }
        _ => 1000.0,
    }
}

/// Build a join plan from two sub-plans and applicable predicates.
fn build_join(
    left: LogicalPlan,
    right: LogicalPlan,
    predicates: &[Expr],
    left_tables: &HashSet<String>,
    right_tables: &HashSet<String>,
) -> LogicalPlan {
    let mut applicable = Vec::new();
    let all_tables: HashSet<String> = left_tables.union(right_tables).cloned().collect();

    for pred in predicates {
        let refs = referenced_tables(pred);
        if !refs.is_empty() && refs.iter().all(|t| all_tables.contains(t)) {
            // Check that the predicate actually references both sides
            let refs_left = refs.iter().any(|t| left_tables.contains(t));
            let refs_right = refs.iter().any(|t| right_tables.contains(t));
            if refs_left && refs_right {
                applicable.push(pred.clone());
            }
        }
    }

    // Also include predicates that reference only one side (pushed down)
    for pred in predicates {
        let refs = referenced_tables(pred);
        if refs.iter().all(|t| all_tables.contains(t)) && !applicable.contains(pred) {
            applicable.push(pred.clone());
        }
    }

    let condition = combine_conjunction(&applicable);
    let schema = left.schema().merge(&right.schema());

    LogicalPlan::Join {
        join_type: JoinType::Inner,
        condition,
        left: Box::new(left),
        right: Box::new(right),
        schema,
    }
}

/// Estimate the cost of a join (simplified: product of cardinalities / selectivity).
fn estimate_join_cost(
    left: &LogicalPlan,
    right: &LogicalPlan,
    predicates: &[Expr],
    catalog: &Catalog,
) -> f64 {
    let estimator = CardinalityEstimator::new(catalog);
    let left_card = estimator.estimate(left);
    let right_card = estimator.estimate(right);
    let selectivity = if predicates.is_empty() {
        1.0
    } else {
        0.1_f64.powi(predicates.len() as i32)
    };
    left_card * right_card * selectivity
}

// ---------------------------------------------------------------------------
// DP join ordering (DPccp-style) for ≤6 tables
// ---------------------------------------------------------------------------

fn dp_join_order(
    relations: &[LogicalPlan],
    predicates: &[Expr],
    catalog: &Catalog,
) -> LogicalPlan {
    let n = relations.len();
    // Use bitmask DP: dp[mask] = (best_plan, cost, tables_in_mask)
    let mut dp: HashMap<u32, (LogicalPlan, f64, HashSet<String>)> = HashMap::new();

    // Initialize single-relation entries
    for (i, rel) in relations.iter().enumerate() {
        let mask = 1u32 << i;
        let card = estimate_base_card(rel, catalog);
        let mut tables = HashSet::new();
        if let Some(name) = plan_table_name(rel) {
            tables.insert(name);
        }
        dp.insert(mask, (rel.clone(), card, tables));
    }

    // Enumerate subsets by increasing size
    let full_mask = (1u32 << n) - 1;
    for size in 2..=n {
        for mask in 1..=full_mask {
            if (mask as u32).count_ones() as usize != size {
                continue;
            }

            // Try all non-empty proper subsets
            let mut sub = (mask - 1) & mask;
            while sub > 0 {
                let complement = mask & !sub;
                if complement > 0 && sub < complement {
                    // Only consider sub < complement to avoid duplicates
                    if let (Some(left_entry), Some(right_entry)) =
                        (dp.get(&sub), dp.get(&complement))
                    {
                        let join = build_join(
                            left_entry.0.clone(),
                            right_entry.0.clone(),
                            predicates,
                            &left_entry.2,
                            &right_entry.2,
                        );
                        let cost = estimate_join_cost(
                            &left_entry.0,
                            &right_entry.0,
                            predicates,
                            catalog,
                        );
                        let mut tables = left_entry.2.clone();
                        tables.extend(right_entry.2.iter().cloned());

                        let better = match dp.get(&mask) {
                            Some((_, existing_cost, _)) => cost < *existing_cost,
                            None => true,
                        };
                        if better {
                            dp.insert(mask, (join, cost, tables));
                        }
                    }
                }
                sub = (sub - 1) & mask;
            }
        }
    }

    dp.remove(&full_mask)
        .map(|(plan, _, _)| plan)
        .unwrap_or_else(|| relations[0].clone())
}

// ---------------------------------------------------------------------------
// Greedy join ordering for 7-12 tables
// ---------------------------------------------------------------------------

fn greedy_join_order(
    relations: &[LogicalPlan],
    predicates: &[Expr],
    catalog: &Catalog,
) -> LogicalPlan {
    let mut remaining: Vec<(LogicalPlan, HashSet<String>)> = relations
        .iter()
        .map(|r| {
            let mut tables = HashSet::new();
            if let Some(name) = plan_table_name(r) {
                tables.insert(name);
            }
            (r.clone(), tables)
        })
        .collect();

    while remaining.len() > 1 {
        let mut best_i = 0;
        let mut best_j = 1;
        let mut best_cost = f64::MAX;

        for i in 0..remaining.len() {
            for j in (i + 1)..remaining.len() {
                let cost =
                    estimate_join_cost(&remaining[i].0, &remaining[j].0, predicates, catalog);
                if cost < best_cost {
                    best_cost = cost;
                    best_i = i;
                    best_j = j;
                }
            }
        }

        let right = remaining.remove(best_j);
        let left = remaining.remove(best_i);

        let join = build_join(left.0, right.0, predicates, &left.1, &right.1);
        let mut tables = left.1;
        tables.extend(right.1);
        remaining.push((join, tables));
    }

    remaining.into_iter().next().map(|(p, _)| p).unwrap()
}

// ---------------------------------------------------------------------------
// Left-deep greedy for >12 tables
// ---------------------------------------------------------------------------

fn left_deep_greedy(
    relations: &[LogicalPlan],
    predicates: &[Expr],
    catalog: &Catalog,
) -> LogicalPlan {
    if relations.is_empty() {
        return LogicalPlan::Empty {
            schema: Schema::empty(),
        };
    }

    // Start with the smallest relation
    let estimator = CardinalityEstimator::new(catalog);
    let mut indices: Vec<usize> = (0..relations.len()).collect();
    indices.sort_by(|&a, &b| {
        let ca = estimator.estimate(&relations[a]);
        let cb = estimator.estimate(&relations[b]);
        ca.partial_cmp(&cb).unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut current = relations[indices[0]].clone();
    let mut current_tables = HashSet::new();
    if let Some(name) = plan_table_name(&current) {
        current_tables.insert(name);
    }

    for &idx in &indices[1..] {
        let right = relations[idx].clone();
        let mut right_tables = HashSet::new();
        if let Some(name) = plan_table_name(&right) {
            right_tables.insert(name);
        }
        current = build_join(current, right, predicates, &current_tables, &right_tables);
        current_tables.extend(right_tables);
    }

    current
}
