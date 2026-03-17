use crate::sql::ast::{BinaryOp, Expr, JoinType};
use crate::types::Schema;

use super::cardinality::CardinalityEstimator;
use super::cost_model::CostModel;
use super::logical_plan::*;
use super::physical_plan::*;
use super::Catalog;

/// Converts an optimized logical plan into a physical (executable) plan
/// by choosing physical operators (scan type, join algorithm, etc.).
pub struct PhysicalPlanner<'a> {
    catalog: &'a Catalog,
    estimator: CardinalityEstimator<'a>,
    cost_model: CostModel<'a>,
}

impl<'a> PhysicalPlanner<'a> {
    pub fn new(catalog: &'a Catalog) -> Self {
        Self {
            catalog,
            estimator: CardinalityEstimator::new(catalog),
            cost_model: CostModel::new(catalog),
        }
    }

    /// Convert a logical plan into a physical plan.
    pub fn plan(&self, logical: &LogicalPlan) -> PhysicalPlan {
        match logical {
            LogicalPlan::Scan {
                table,
                alias,
                schema,
            } => self.plan_scan(table, alias, schema, None),

            LogicalPlan::Filter { predicate, input } => {
                // Try to push filter into scan as a predicate
                if let LogicalPlan::Scan {
                    table,
                    alias,
                    schema,
                } = input.as_ref()
                {
                    return self.plan_scan(table, alias, schema, Some(predicate));
                }

                PhysicalPlan::Filter {
                    predicate: predicate.clone(),
                    input: Box::new(self.plan(input)),
                }
            }

            LogicalPlan::Project { expressions, input } => PhysicalPlan::Project {
                expressions: expressions.clone(),
                input: Box::new(self.plan(input)),
            },

            LogicalPlan::Join {
                join_type,
                condition,
                left,
                right,
                schema,
            } => self.plan_join(*join_type, condition.as_ref(), left, right, schema),

            LogicalPlan::Aggregate {
                group_by,
                aggregates,
                input,
                schema,
            } => self.plan_aggregate(group_by, aggregates, input, schema),

            LogicalPlan::Sort { order_by, input } => PhysicalPlan::ExternalSort {
                order_by: order_by.clone(),
                input: Box::new(self.plan(input)),
            },

            LogicalPlan::Limit {
                count,
                offset,
                input,
            } => PhysicalPlan::Limit {
                count: *count,
                offset: *offset,
                input: Box::new(self.plan(input)),
            },

            LogicalPlan::Distinct { input } => self.plan_distinct(input),

            LogicalPlan::Union { all, left, right } => PhysicalPlan::Union {
                all: *all,
                left: Box::new(self.plan(left)),
                right: Box::new(self.plan(right)),
            },

            LogicalPlan::Intersect { all, left, right } => PhysicalPlan::Intersect {
                all: *all,
                left: Box::new(self.plan(left)),
                right: Box::new(self.plan(right)),
            },

            LogicalPlan::Except { all, left, right } => PhysicalPlan::Except {
                all: *all,
                left: Box::new(self.plan(left)),
                right: Box::new(self.plan(right)),
            },

            LogicalPlan::Insert {
                table,
                columns,
                input,
            } => PhysicalPlan::Insert {
                table: table.clone(),
                columns: columns.clone(),
                input: Box::new(self.plan(input)),
            },

            LogicalPlan::Update {
                table,
                assignments,
                input,
            } => PhysicalPlan::Update {
                table: table.clone(),
                assignments: assignments.clone(),
                input: Box::new(self.plan(input)),
            },

            LogicalPlan::Delete { table, input } => PhysicalPlan::Delete {
                table: table.clone(),
                input: Box::new(self.plan(input)),
            },

            LogicalPlan::Values { rows, schema } => PhysicalPlan::Values {
                rows: rows.clone(),
                schema: schema.clone(),
            },

            LogicalPlan::Empty { schema } => PhysicalPlan::Empty {
                schema: schema.clone(),
            },
        }
    }

    // -----------------------------------------------------------------------
    // Scan planning: SeqScan vs IndexScan
    // -----------------------------------------------------------------------

    fn plan_scan(
        &self,
        table: &str,
        alias: &Option<String>,
        schema: &Schema,
        predicate: Option<&Expr>,
    ) -> PhysicalPlan {
        let indexes = self.catalog.get_indexes(table);

        // Try to find a usable index for the predicate
        if let Some(pred) = predicate {
            for idx in &indexes {
                if let Some(ranges) = self.extract_key_ranges(pred, &idx.columns) {
                    // Estimate costs
                    let index_plan = PhysicalPlan::IndexScan {
                        table: table.to_string(),
                        alias: alias.clone(),
                        index_name: idx.name.clone(),
                        schema: schema.clone(),
                        key_ranges: ranges,
                        predicate: Some(pred.clone()),
                    };

                    let seq_plan = PhysicalPlan::SeqScan {
                        table: table.to_string(),
                        alias: alias.clone(),
                        schema: schema.clone(),
                        predicate: Some(pred.clone()),
                    };

                    let idx_cost = self.cost_model.estimate_cost(&index_plan);
                    let seq_cost = self.cost_model.estimate_cost(&seq_plan);

                    if idx_cost < seq_cost {
                        return index_plan;
                    }
                }
            }

            // No useful index: SeqScan with predicate
            return PhysicalPlan::SeqScan {
                table: table.to_string(),
                alias: alias.clone(),
                schema: schema.clone(),
                predicate: Some(pred.clone()),
            };
        }

        PhysicalPlan::SeqScan {
            table: table.to_string(),
            alias: alias.clone(),
            schema: schema.clone(),
            predicate: None,
        }
    }

    /// Try to extract key ranges from a predicate for a given set of index columns.
    fn extract_key_ranges(&self, predicate: &Expr, index_cols: &[String]) -> Option<Vec<KeyRange>> {
        let first_col = index_cols.first()?;
        let preds = split_conjunction(predicate);

        let mut ranges = Vec::new();
        for pred in &preds {
            match pred {
                // col = val
                Expr::BinaryOp {
                    left,
                    op: BinaryOp::Eq,
                    right,
                } => {
                    if self.is_column(left, first_col) && self.is_constant(right) {
                        ranges.push(KeyRange::eq(right.as_ref().clone()));
                    } else if self.is_column(right, first_col) && self.is_constant(left) {
                        ranges.push(KeyRange::eq(left.as_ref().clone()));
                    }
                }
                // col > val
                Expr::BinaryOp {
                    left,
                    op: BinaryOp::Gt,
                    right,
                } if self.is_column(left, first_col) && self.is_constant(right) => {
                    ranges.push(KeyRange {
                        low: Bound::Exclusive(right.as_ref().clone()),
                        high: Bound::Unbounded,
                    });
                }
                // col >= val
                Expr::BinaryOp {
                    left,
                    op: BinaryOp::GtEq,
                    right,
                } if self.is_column(left, first_col) && self.is_constant(right) => {
                    ranges.push(KeyRange {
                        low: Bound::Inclusive(right.as_ref().clone()),
                        high: Bound::Unbounded,
                    });
                }
                // col < val
                Expr::BinaryOp {
                    left,
                    op: BinaryOp::Lt,
                    right,
                } if self.is_column(left, first_col) && self.is_constant(right) => {
                    ranges.push(KeyRange {
                        low: Bound::Unbounded,
                        high: Bound::Exclusive(right.as_ref().clone()),
                    });
                }
                // col <= val
                Expr::BinaryOp {
                    left,
                    op: BinaryOp::LtEq,
                    right,
                } if self.is_column(left, first_col) && self.is_constant(right) => {
                    ranges.push(KeyRange {
                        low: Bound::Unbounded,
                        high: Bound::Inclusive(right.as_ref().clone()),
                    });
                }
                // col BETWEEN low AND high
                Expr::Between {
                    expr,
                    low,
                    high,
                    negated: false,
                } if self.is_column(expr, first_col)
                    && self.is_constant(low)
                    && self.is_constant(high) =>
                {
                    ranges.push(KeyRange {
                        low: Bound::Inclusive(low.as_ref().clone()),
                        high: Bound::Inclusive(high.as_ref().clone()),
                    });
                }
                // col IN (v1, v2, ...) → multiple point ranges
                Expr::InList {
                    expr,
                    list,
                    negated: false,
                } if self.is_column(expr, first_col) => {
                    for val in list {
                        if self.is_constant(val) {
                            ranges.push(KeyRange::eq(val.clone()));
                        }
                    }
                }
                _ => {}
            }
        }

        if ranges.is_empty() {
            None
        } else {
            Some(ranges)
        }
    }

    fn is_column(&self, expr: &Expr, name: &str) -> bool {
        match expr {
            Expr::Identifier(n) => n == name,
            Expr::QualifiedIdentifier { column, .. } => column == name,
            _ => false,
        }
    }

    fn is_constant(&self, expr: &Expr) -> bool {
        matches!(
            expr,
            Expr::Integer(_)
                | Expr::Float(_)
                | Expr::String(_)
                | Expr::Boolean(_)
                | Expr::Null
        )
    }

    // -----------------------------------------------------------------------
    // Join planning: HashJoin vs SortMergeJoin vs NestedLoopJoin
    // -----------------------------------------------------------------------

    fn plan_join(
        &self,
        join_type: JoinType,
        condition: Option<&Expr>,
        left: &LogicalPlan,
        right: &LogicalPlan,
        schema: &Schema,
    ) -> PhysicalPlan {
        let left_plan = self.plan(left);
        let right_plan = self.plan(right);
        let _left_card = self.estimator.estimate(left);
        let _right_card = self.estimator.estimate(right);

        // Extract equi-join keys if any
        let equi_keys = condition.and_then(|c| self.extract_equi_keys(c));

        if let Some((left_keys, right_keys)) = equi_keys {
            // Compare HashJoin vs SortMergeJoin
            let hash_join = PhysicalPlan::HashJoin {
                join_type,
                left_keys: left_keys.clone(),
                right_keys: right_keys.clone(),
                condition: condition.cloned(),
                left: Box::new(left_plan.clone()),
                right: Box::new(right_plan.clone()),
                schema: schema.clone(),
            };

            let sort_merge_join = PhysicalPlan::SortMergeJoin {
                join_type,
                left_keys: left_keys.clone(),
                right_keys: right_keys.clone(),
                condition: condition.cloned(),
                left: Box::new(left_plan.clone()),
                right: Box::new(right_plan.clone()),
                schema: schema.clone(),
            };

            let hash_cost = self.cost_model.estimate_cost(&hash_join);
            let smj_cost = self.cost_model.estimate_cost(&sort_merge_join);

            if hash_cost <= smj_cost {
                return hash_join;
            } else {
                return sort_merge_join;
            }
        }

        // No equi-join keys: NestedLoopJoin
        PhysicalPlan::NestedLoopJoin {
            join_type,
            condition: condition.cloned(),
            left: Box::new(left_plan),
            right: Box::new(right_plan),
            schema: schema.clone(),
        }
    }

    /// Extract equi-join keys from a condition like a.x = b.y AND a.z = b.w.
    fn extract_equi_keys(&self, condition: &Expr) -> Option<(Vec<Expr>, Vec<Expr>)> {
        let preds = split_conjunction(condition);
        let mut left_keys = Vec::new();
        let mut right_keys = Vec::new();

        for pred in &preds {
            if let Expr::BinaryOp {
                left,
                op: BinaryOp::Eq,
                right,
            } = pred
            {
                if self.is_column_ref(left) && self.is_column_ref(right) {
                    left_keys.push(left.as_ref().clone());
                    right_keys.push(right.as_ref().clone());
                }
            }
        }

        if left_keys.is_empty() {
            None
        } else {
            Some((left_keys, right_keys))
        }
    }

    fn is_column_ref(&self, expr: &Expr) -> bool {
        matches!(
            expr,
            Expr::Identifier(_) | Expr::QualifiedIdentifier { .. }
        )
    }

    // -----------------------------------------------------------------------
    // Aggregate planning: HashAggregate vs SortAggregate
    // -----------------------------------------------------------------------

    fn plan_aggregate(
        &self,
        group_by: &[Expr],
        aggregates: &[AggregateExpr],
        input: &LogicalPlan,
        schema: &Schema,
    ) -> PhysicalPlan {
        let input_plan = self.plan(input);
        let _input_rows = self.estimator.estimate(input);

        if group_by.is_empty() {
            // Scalar aggregate always uses hash
            return PhysicalPlan::HashAggregate {
                group_by: group_by.to_vec(),
                aggregates: aggregates.to_vec(),
                input: Box::new(input_plan),
                schema: schema.clone(),
            };
        }

        // Compare hash vs sort aggregate
        let hash_agg = PhysicalPlan::HashAggregate {
            group_by: group_by.to_vec(),
            aggregates: aggregates.to_vec(),
            input: Box::new(input_plan.clone()),
            schema: schema.clone(),
        };

        let sort_agg = PhysicalPlan::SortAggregate {
            group_by: group_by.to_vec(),
            aggregates: aggregates.to_vec(),
            input: Box::new(PhysicalPlan::ExternalSort {
                order_by: group_by
                    .iter()
                    .map(|e| SortExpr {
                        expr: e.clone(),
                        ascending: true,
                        nulls_first: false,
                    })
                    .collect(),
                input: Box::new(input_plan),
            }),
            schema: schema.clone(),
        };

        let hash_cost = self.cost_model.estimate_cost(&hash_agg);
        let sort_cost = self.cost_model.estimate_cost(&sort_agg);

        if hash_cost <= sort_cost {
            hash_agg
        } else {
            sort_agg
        }
    }

    // -----------------------------------------------------------------------
    // Distinct planning: HashDistinct vs SortDistinct
    // -----------------------------------------------------------------------

    fn plan_distinct(&self, input: &LogicalPlan) -> PhysicalPlan {
        let input_plan = self.plan(input);
        let _rows = self.estimator.estimate(input);

        let hash_distinct = PhysicalPlan::HashDistinct {
            input: Box::new(input_plan.clone()),
        };

        let sort_distinct = PhysicalPlan::SortDistinct {
            input: Box::new(PhysicalPlan::ExternalSort {
                order_by: vec![], // Sort by all columns
                input: Box::new(input_plan),
            }),
        };

        let hash_cost = self.cost_model.estimate_cost(&hash_distinct);
        let sort_cost = self.cost_model.estimate_cost(&sort_distinct);

        if hash_cost <= sort_cost {
            hash_distinct
        } else {
            sort_distinct
        }
    }
}
