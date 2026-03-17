use crate::sql::ast::{BinaryOp, Expr, UnaryOp};

use super::logical_plan::*;
use super::Catalog;

/// Default selectivity for unknown predicates.
const DEFAULT_SELECTIVITY: f64 = 0.1;
/// Default selectivity for equality predicates when no stats available.
const DEFAULT_EQ_SELECTIVITY: f64 = 0.01;
/// Default selectivity for range predicates.
const DEFAULT_RANGE_SELECTIVITY: f64 = 0.33;
/// Default selectivity for BETWEEN.
const DEFAULT_BETWEEN_SELECTIVITY: f64 = 0.25;
/// Default selectivity for LIKE with prefix.
const DEFAULT_LIKE_SELECTIVITY: f64 = 0.1;
/// Default selectivity for IS NULL.
const DEFAULT_NULL_SELECTIVITY: f64 = 0.02;

/// Estimates the cardinality (row count) of logical plan nodes using
/// catalog statistics and selectivity formulas.
pub struct CardinalityEstimator<'a> {
    catalog: &'a Catalog,
}

impl<'a> CardinalityEstimator<'a> {
    pub fn new(catalog: &'a Catalog) -> Self {
        Self { catalog }
    }

    /// Estimate the output cardinality of a logical plan node.
    pub fn estimate(&self, plan: &LogicalPlan) -> f64 {
        match plan {
            LogicalPlan::Scan { table, .. } => {
                let stats = self.catalog.get_stats(table);
                stats.row_count
            }

            LogicalPlan::Filter { predicate, input } => {
                let input_card = self.estimate(input);
                let sel = self.estimate_selectivity(predicate, input);
                (input_card * sel).max(1.0)
            }

            LogicalPlan::Project { input, .. } => self.estimate(input),

            LogicalPlan::Join {
                join_type,
                condition,
                left,
                right,
                ..
            } => {
                let left_card = self.estimate(left);
                let right_card = self.estimate(right);
                self.estimate_join_cardinality(
                    *join_type,
                    condition.as_ref(),
                    left_card,
                    right_card,
                    left,
                    right,
                )
            }

            LogicalPlan::Aggregate {
                group_by, input, ..
            } => {
                let input_card = self.estimate(input);
                if group_by.is_empty() {
                    1.0 // Scalar aggregate
                } else {
                    // Estimate number of groups
                    self.estimate_group_count(group_by, input, input_card)
                }
            }

            LogicalPlan::Sort { input, .. } => self.estimate(input),

            LogicalPlan::Limit {
                count,
                offset,
                input,
            } => {
                let input_card = self.estimate(input);
                let available = (input_card - *offset as f64).max(0.0);
                match count {
                    Some(c) => available.min(*c as f64),
                    None => available,
                }
            }

            LogicalPlan::Distinct { input } => {
                let input_card = self.estimate(input);
                // Assume distinct eliminates ~20% of rows
                (input_card * 0.8).max(1.0)
            }

            LogicalPlan::Union { all, left, right } => {
                let l = self.estimate(left);
                let r = self.estimate(right);
                if *all {
                    l + r
                } else {
                    // UNION removes duplicates
                    (l + r) * 0.8
                }
            }

            LogicalPlan::Intersect { left, right, .. } => {
                let l = self.estimate(left);
                let r = self.estimate(right);
                l.min(r) * 0.5
            }

            LogicalPlan::Except { left, right, .. } => {
                let l = self.estimate(left);
                let r = self.estimate(right);
                (l - r * 0.5).max(1.0)
            }

            LogicalPlan::Values { rows, .. } => rows.len() as f64,

            LogicalPlan::Empty { .. } => 0.0,

            LogicalPlan::Insert { input, .. }
            | LogicalPlan::Update { input, .. }
            | LogicalPlan::Delete { input, .. } => self.estimate(input),
        }
    }

    /// Estimate the selectivity of a predicate (0.0 to 1.0).
    pub fn estimate_selectivity(&self, predicate: &Expr, input: &LogicalPlan) -> f64 {
        match predicate {
            // col = literal → 1/ndv
            Expr::BinaryOp {
                left,
                op: BinaryOp::Eq,
                right,
            } => {
                if let Some((table, col)) = self.extract_column(left) {
                    let stats = self.catalog.get_column_stats(&table, &col);
                    if stats.distinct_count > 0.0 {
                        return 1.0 / stats.distinct_count;
                    }
                }
                if let Some((table, col)) = self.extract_column(right) {
                    let stats = self.catalog.get_column_stats(&table, &col);
                    if stats.distinct_count > 0.0 {
                        return 1.0 / stats.distinct_count;
                    }
                }
                DEFAULT_EQ_SELECTIVITY
            }

            // col != literal → 1 - 1/ndv
            Expr::BinaryOp {
                left,
                op: BinaryOp::NotEq,
                right,
            } => {
                let eq_sel = self.estimate_selectivity(
                    &Expr::BinaryOp {
                        left: left.clone(),
                        op: BinaryOp::Eq,
                        right: right.clone(),
                    },
                    input,
                );
                1.0 - eq_sel
            }

            // Range: col > val, col < val, col >= val, col <= val
            Expr::BinaryOp {
                left,
                op: BinaryOp::Lt | BinaryOp::Gt | BinaryOp::LtEq | BinaryOp::GtEq,
                right,
            } => {
                if let (Some((table, col)), Some(val)) =
                    (self.extract_column(left), self.expr_to_f64(right))
                {
                    let stats = self.catalog.get_column_stats(&table, &col);
                    if let (Some(min), Some(max)) = (stats.min_value, stats.max_value) {
                        if max > min {
                            return match left {
                                _ if matches!(predicate, Expr::BinaryOp { op: BinaryOp::Lt, .. })
                                    || matches!(predicate, Expr::BinaryOp { op: BinaryOp::LtEq, .. }) =>
                                {
                                    ((val - min) / (max - min)).clamp(0.0, 1.0)
                                }
                                _ => ((max - val) / (max - min)).clamp(0.0, 1.0),
                            };
                        }
                    }
                }
                DEFAULT_RANGE_SELECTIVITY
            }

            // AND: sel(A) * sel(B)
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                right,
            } => {
                let sel_l = self.estimate_selectivity(left, input);
                let sel_r = self.estimate_selectivity(right, input);
                sel_l * sel_r
            }

            // OR: sel(A) + sel(B) - sel(A)*sel(B)
            Expr::BinaryOp {
                left,
                op: BinaryOp::Or,
                right,
            } => {
                let sel_l = self.estimate_selectivity(left, input);
                let sel_r = self.estimate_selectivity(right, input);
                sel_l + sel_r - sel_l * sel_r
            }

            // NOT: 1 - sel
            Expr::UnaryOp {
                op: UnaryOp::Not,
                expr,
            } => {
                let sel = self.estimate_selectivity(expr, input);
                1.0 - sel
            }

            // BETWEEN: (high - low) / (max - min) or default
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let sel = if let (Some((table, col)), Some(lo), Some(hi)) = (
                    self.extract_column(expr),
                    self.expr_to_f64(low),
                    self.expr_to_f64(high),
                ) {
                    let stats = self.catalog.get_column_stats(&table, &col);
                    if let (Some(min), Some(max)) = (stats.min_value, stats.max_value) {
                        if max > min {
                            ((hi - lo) / (max - min)).clamp(0.0, 1.0)
                        } else {
                            DEFAULT_BETWEEN_SELECTIVITY
                        }
                    } else {
                        DEFAULT_BETWEEN_SELECTIVITY
                    }
                } else {
                    DEFAULT_BETWEEN_SELECTIVITY
                };
                if *negated { 1.0 - sel } else { sel }
            }

            // LIKE: prefix matching heuristic
            Expr::Like { negated, .. } => {
                let sel = DEFAULT_LIKE_SELECTIVITY;
                if *negated { 1.0 - sel } else { sel }
            }

            // IS NULL
            Expr::IsNull { expr, negated } => {
                let sel = if let Some((table, col)) = self.extract_column(expr) {
                    let stats = self.catalog.get_column_stats(&table, &col);
                    if stats.null_fraction > 0.0 {
                        stats.null_fraction
                    } else {
                        DEFAULT_NULL_SELECTIVITY
                    }
                } else {
                    DEFAULT_NULL_SELECTIVITY
                };
                if *negated { 1.0 - sel } else { sel }
            }

            // IN list: min(len/ndv, 1)
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let list_len = list.len() as f64;
                let sel = if let Some((table, col)) = self.extract_column(expr) {
                    let stats = self.catalog.get_column_stats(&table, &col);
                    if stats.distinct_count > 0.0 {
                        (list_len / stats.distinct_count).min(1.0)
                    } else {
                        (list_len * DEFAULT_EQ_SELECTIVITY).min(1.0)
                    }
                } else {
                    (list_len * DEFAULT_EQ_SELECTIVITY).min(1.0)
                };
                if *negated { 1.0 - sel } else { sel }
            }

            // Boolean literal
            Expr::Boolean(true) => 1.0,
            Expr::Boolean(false) => 0.0,

            // Default for anything else
            _ => DEFAULT_SELECTIVITY,
        }
    }

    /// Estimate join cardinality.
    fn estimate_join_cardinality(
        &self,
        join_type: crate::sql::ast::JoinType,
        condition: Option<&Expr>,
        left_card: f64,
        right_card: f64,
        left: &LogicalPlan,
        _right: &LogicalPlan,
    ) -> f64 {
        let base = match condition {
            Some(cond) => {
                // For equi-join on col = col: |L| * |R| / max(ndv_L, ndv_R)
                if let Some((l_table, l_col, r_table, r_col)) = self.extract_equi_join_cols(cond) {
                    let l_stats = self.catalog.get_column_stats(&l_table, &l_col);
                    let r_stats = self.catalog.get_column_stats(&r_table, &r_col);
                    let max_ndv = l_stats.distinct_count.max(r_stats.distinct_count).max(1.0);
                    left_card * right_card / max_ndv
                } else {
                    // Non-equi join: use selectivity
                    let sel = self.estimate_selectivity(cond, left);
                    left_card * right_card * sel
                }
            }
            None => left_card * right_card, // cross join
        };

        match join_type {
            crate::sql::ast::JoinType::Inner | crate::sql::ast::JoinType::Cross => base,
            crate::sql::ast::JoinType::Left => base.max(left_card),
            crate::sql::ast::JoinType::Right => base.max(right_card),
            crate::sql::ast::JoinType::Full => base.max(left_card).max(right_card),
            crate::sql::ast::JoinType::LeftSemi => left_card * 0.5_f64.max(base / left_card.max(1.0)),
            crate::sql::ast::JoinType::LeftAnti => left_card * (1.0 - 0.5_f64.min(base / left_card.max(1.0))),
        }
    }

    /// Estimate number of groups for GROUP BY.
    fn estimate_group_count(
        &self,
        group_by: &[Expr],
        _input: &LogicalPlan,
        input_card: f64,
    ) -> f64 {
        let mut groups = 1.0;
        for expr in group_by {
            if let Some((table, col)) = self.extract_column(expr) {
                let stats = self.catalog.get_column_stats(&table, &col);
                groups *= stats.distinct_count.max(1.0);
            } else {
                groups *= 10.0; // default group estimate for expressions
            }
        }
        groups.min(input_card).max(1.0)
    }

    /// Extract a (table, column) pair from a column reference expression.
    fn extract_column(&self, expr: &Expr) -> Option<(String, String)> {
        match expr {
            Expr::QualifiedIdentifier { table, column } => {
                Some((table.clone(), column.clone()))
            }
            Expr::Identifier(name) => {
                // Try to find which table owns this column
                for (table_name, schema) in &self.catalog.tables {
                    if schema.has_column(name) {
                        return Some((table_name.clone(), name.clone()));
                    }
                }
                None
            }
            _ => None,
        }
    }

    /// Extract equi-join columns: col1 = col2.
    fn extract_equi_join_cols(
        &self,
        expr: &Expr,
    ) -> Option<(String, String, String, String)> {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOp::Eq,
                right,
            } => {
                let l = self.extract_column(left)?;
                let r = self.extract_column(right)?;
                Some((l.0, l.1, r.0, r.1))
            }
            // For AND, just use the first equi-join predicate
            Expr::BinaryOp {
                left,
                op: BinaryOp::And,
                ..
            } => self.extract_equi_join_cols(left),
            _ => None,
        }
    }

    fn expr_to_f64(&self, expr: &Expr) -> Option<f64> {
        match expr {
            Expr::Integer(n) => Some(*n as f64),
            Expr::Float(n) => Some(*n),
            _ => None,
        }
    }
}
