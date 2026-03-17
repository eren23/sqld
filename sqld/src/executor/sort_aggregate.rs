use std::cmp::Ordering;

use crate::planner::logical_plan::AggregateExpr;
use crate::sql::ast::Expr;
use crate::types::{Datum, Schema, Tuple};
use crate::utils::error::Result;

use super::executor::{intermediate_tuple, Executor};
use super::expr_eval::{compile_expr, evaluate_expr, ExprOp};
use super::hash_aggregate::Accumulator;

// ---------------------------------------------------------------------------
// SortAggregate — assumes input is sorted by GROUP BY keys
// ---------------------------------------------------------------------------

pub struct SortAggregateExecutor {
    child: Box<dyn Executor>,
    group_by_src: Vec<Expr>,
    aggregates: Vec<AggregateExpr>,
    schema: Schema,

    group_by_ops: Vec<Vec<ExprOp>>,
    agg_input_ops: Vec<Vec<ExprOp>>,
    results: Vec<Tuple>,
    position: usize,
    initialized: bool,
}

impl SortAggregateExecutor {
    pub fn new(
        child: Box<dyn Executor>,
        group_by: Vec<Expr>,
        aggregates: Vec<AggregateExpr>,
        schema: Schema,
    ) -> Self {
        Self {
            child,
            group_by_src: group_by,
            aggregates,
            schema,
            group_by_ops: Vec::new(),
            agg_input_ops: Vec::new(),
            results: Vec::new(),
            position: 0,
            initialized: false,
        }
    }
}

impl Executor for SortAggregateExecutor {
    fn init(&mut self) -> Result<()> {
        self.child.init()?;
        let input_schema = self.child.schema().clone();

        self.group_by_ops = self
            .group_by_src
            .iter()
            .map(|e| compile_expr(e, &input_schema))
            .collect::<Result<Vec<_>>>()?;

        self.agg_input_ops = self
            .aggregates
            .iter()
            .map(|ae| compile_expr(&ae.arg, &input_schema))
            .collect::<Result<Vec<_>>>()?;

        self.results.clear();

        let mut current_key: Option<Vec<Datum>> = None;
        let mut accumulators: Vec<Accumulator> = self
            .aggregates
            .iter()
            .map(|ae| Accumulator::new(ae.func, ae.distinct))
            .collect();

        while let Some(tuple) = self.child.next()? {
            let key: Vec<Datum> = self
                .group_by_ops
                .iter()
                .map(|ops| evaluate_expr(ops, &tuple))
                .collect::<Result<Vec<_>>>()?;

            let agg_vals: Vec<Datum> = self
                .agg_input_ops
                .iter()
                .map(|ops| evaluate_expr(ops, &tuple))
                .collect::<Result<Vec<_>>>()?;

            // Check if we've moved to a new group
            let is_new_group = match &current_key {
                None => true,
                Some(ck) => !keys_equal(ck, &key),
            };

            if is_new_group {
                // Emit previous group
                if let Some(ref ck) = current_key {
                    let mut row = ck.clone();
                    for acc in &accumulators {
                        row.push(acc.finalize()?);
                    }
                    self.results.push(intermediate_tuple(row));
                }

                // Start new group
                current_key = Some(key);
                accumulators = self
                    .aggregates
                    .iter()
                    .map(|ae| Accumulator::new(ae.func, ae.distinct))
                    .collect();
            }

            // Feed values to accumulators
            for (i, acc) in accumulators.iter_mut().enumerate() {
                acc.accumulate(&agg_vals[i])?;
            }
        }

        // Emit last group
        if let Some(ref ck) = current_key {
            let mut row = ck.clone();
            for acc in &accumulators {
                row.push(acc.finalize()?);
            }
            self.results.push(intermediate_tuple(row));
        } else if self.group_by_src.is_empty() {
            // No input rows, no group by → emit default aggregate values
            let mut row = Vec::new();
            for acc in &accumulators {
                row.push(acc.finalize()?);
            }
            self.results.push(intermediate_tuple(row));
        }

        self.position = 0;
        self.initialized = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.position < self.results.len() {
            let t = self.results[self.position].clone();
            self.position += 1;
            Ok(Some(t))
        } else {
            Ok(None)
        }
    }

    fn close(&mut self) -> Result<()> {
        self.results.clear();
        self.child.close()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn keys_equal(a: &[Datum], b: &[Datum]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    for (da, db) in a.iter().zip(b.iter()) {
        match (da.is_null(), db.is_null()) {
            (true, true) => continue,
            (true, false) | (false, true) => return false,
            (false, false) => {
                if da.sql_cmp(db).unwrap_or(None) != Some(Ordering::Equal) {
                    return false;
                }
            }
        }
    }
    true
}
