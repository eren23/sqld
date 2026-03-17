use std::cmp::Ordering;

use crate::planner::logical_plan::SortExpr;
use crate::types::{Datum, Schema, Tuple};
use crate::utils::error::Result;

use super::executor::Executor;
use super::expr_eval::{compile_expr, evaluate_expr, ExprOp};

// ---------------------------------------------------------------------------
// Sort — in-memory quicksort with external merge sort on overflow
// ---------------------------------------------------------------------------

pub struct SortExecutor {
    child: Box<dyn Executor>,
    order_by: Vec<SortExpr>,
    compiled_keys: Vec<Vec<ExprOp>>,
    schema: Schema,
    sorted: Vec<Tuple>,
    position: usize,
    work_mem: usize,
    initialized: bool,
}

impl SortExecutor {
    pub fn new(
        child: Box<dyn Executor>,
        order_by: Vec<SortExpr>,
        work_mem: usize,
    ) -> Self {
        let schema = child.schema().clone();
        Self {
            child,
            order_by,
            compiled_keys: Vec::new(),
            schema,
            sorted: Vec::new(),
            position: 0,
            work_mem,
            initialized: false,
        }
    }
}

impl Executor for SortExecutor {
    fn init(&mut self) -> Result<()> {
        self.child.init()?;

        // Compile sort key expressions
        let input_schema = self.child.schema().clone();
        self.compiled_keys = self
            .order_by
            .iter()
            .map(|se| compile_expr(&se.expr, &input_schema))
            .collect::<Result<Vec<_>>>()?;

        // Materialize all tuples from child
        let mut tuples = Vec::new();
        let mut mem_used = 0usize;
        let mut spill_runs: Vec<Vec<Tuple>> = Vec::new();

        while let Some(tuple) = self.child.next()? {
            // Rough memory estimate: column_count * 16 bytes per tuple
            mem_used += tuple.column_count() * 16 + 64;
            tuples.push(tuple);

            // If memory budget exceeded, sort current batch and spill
            if mem_used > self.work_mem && !tuples.is_empty() {
                self.sort_tuples(&mut tuples)?;
                spill_runs.push(std::mem::take(&mut tuples));
                mem_used = 0;
            }
        }

        if spill_runs.is_empty() {
            // Everything fits in memory — simple quicksort
            self.sort_tuples(&mut tuples)?;
            self.sorted = tuples;
        } else {
            // External merge sort: sort remaining tuples, merge all runs
            if !tuples.is_empty() {
                self.sort_tuples(&mut tuples)?;
                spill_runs.push(tuples);
            }
            self.sorted = self.merge_runs(spill_runs)?;
        }

        self.position = 0;
        self.initialized = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.position < self.sorted.len() {
            let t = self.sorted[self.position].clone();
            self.position += 1;
            Ok(Some(t))
        } else {
            Ok(None)
        }
    }

    fn close(&mut self) -> Result<()> {
        self.sorted.clear();
        self.child.close()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl SortExecutor {
    fn sort_tuples(&self, tuples: &mut [Tuple]) -> Result<()> {
        let mut err: Option<crate::utils::error::Error> = None;
        let keys = &self.compiled_keys;
        let order_by = &self.order_by;

        tuples.sort_by(|a, b| {
            if err.is_some() {
                return Ordering::Equal;
            }
            match compare_tuples(a, b, keys, order_by) {
                Ok(ord) => ord,
                Err(e) => {
                    err = Some(e);
                    Ordering::Equal
                }
            }
        });

        if let Some(e) = err {
            return Err(e);
        }
        Ok(())
    }

    /// K-way merge of sorted runs.
    fn merge_runs(&self, runs: Vec<Vec<Tuple>>) -> Result<Vec<Tuple>> {
        let mut cursors: Vec<(usize, usize)> = runs
            .iter()
            .enumerate()
            .map(|(i, _)| (i, 0))
            .collect();
        let mut result = Vec::new();
        let keys = &self.compiled_keys;
        let order_by = &self.order_by;

        loop {
            // Find the run with the smallest current element
            let mut best: Option<usize> = None;
            for &(run_idx, pos) in &cursors {
                if pos >= runs[run_idx].len() {
                    continue;
                }
                match best {
                    None => best = Some(run_idx),
                    Some(prev) => {
                        let (_, prev_pos) = cursors
                            .iter()
                            .find(|(r, _)| *r == prev)
                            .unwrap();
                        let cmp = compare_tuples(
                            &runs[run_idx][pos],
                            &runs[prev][*prev_pos],
                            keys,
                            order_by,
                        )?;
                        if cmp == Ordering::Less {
                            best = Some(run_idx);
                        }
                    }
                }
            }

            match best {
                None => break, // All runs exhausted
                Some(run_idx) => {
                    let cursor = cursors
                        .iter_mut()
                        .find(|(r, _)| *r == run_idx)
                        .unwrap();
                    result.push(runs[run_idx][cursor.1].clone());
                    cursor.1 += 1;
                }
            }
        }

        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Comparison helper (reused by sort_merge_join, sort_aggregate, etc.)
// ---------------------------------------------------------------------------

pub fn compare_tuples(
    a: &Tuple,
    b: &Tuple,
    keys: &[Vec<ExprOp>],
    order_by: &[SortExpr],
) -> Result<Ordering> {
    for (i, key_ops) in keys.iter().enumerate() {
        let va = evaluate_expr(key_ops, a)?;
        let vb = evaluate_expr(key_ops, b)?;
        let ascending = order_by[i].ascending;
        let nulls_first = order_by[i].nulls_first;

        let ord = compare_datums(&va, &vb, ascending, nulls_first)?;
        if ord != Ordering::Equal {
            return Ok(ord);
        }
    }
    Ok(Ordering::Equal)
}

/// Compare two datums with ASC/DESC and NULLS FIRST/LAST support.
pub fn compare_datums(
    a: &Datum,
    b: &Datum,
    ascending: bool,
    nulls_first: bool,
) -> Result<Ordering> {
    match (a.is_null(), b.is_null()) {
        (true, true) => Ok(Ordering::Equal),
        (true, false) => Ok(if nulls_first {
            Ordering::Less
        } else {
            Ordering::Greater
        }),
        (false, true) => Ok(if nulls_first {
            Ordering::Greater
        } else {
            Ordering::Less
        }),
        (false, false) => {
            let cmp = a
                .sql_cmp(b)?
                .unwrap_or(Ordering::Equal);
            Ok(if ascending { cmp } else { cmp.reverse() })
        }
    }
}
