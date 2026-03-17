use std::cmp::Ordering;

use crate::sql::ast::{Expr, JoinType};
use crate::types::{Datum, Schema, Tuple};
use crate::utils::error::Result;

use super::executor::{intermediate_tuple, Executor};
use super::expr_eval::{compile_expr, evaluate_expr, is_truthy, ExprOp};

// ---------------------------------------------------------------------------
// SortMergeJoin — merge two sorted inputs
// ---------------------------------------------------------------------------

pub struct SortMergeJoinExecutor {
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    join_type: JoinType,
    left_keys_src: Vec<Expr>,
    right_keys_src: Vec<Expr>,
    condition_src: Option<Expr>,
    schema: Schema,

    left_key_ops: Vec<Vec<ExprOp>>,
    right_key_ops: Vec<Vec<ExprOp>>,
    condition_ops: Option<Vec<ExprOp>>,

    left_col_count: usize,
    right_col_count: usize,

    // Materialized (since we need to handle groups for non-INNER joins)
    left_tuples: Vec<Tuple>,
    right_tuples: Vec<Tuple>,
    output_buffer: Vec<Tuple>,
    buffer_pos: usize,
    _left_pos: usize,
    _right_pos: usize,
    initialized: bool,
}

impl SortMergeJoinExecutor {
    pub fn new(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        join_type: JoinType,
        left_keys: Vec<Expr>,
        right_keys: Vec<Expr>,
        condition: Option<Expr>,
        schema: Schema,
    ) -> Self {
        Self {
            left,
            right,
            join_type,
            left_keys_src: left_keys,
            right_keys_src: right_keys,
            condition_src: condition,
            schema,
            left_key_ops: Vec::new(),
            right_key_ops: Vec::new(),
            condition_ops: None,
            left_col_count: 0,
            right_col_count: 0,
            left_tuples: Vec::new(),
            right_tuples: Vec::new(),
            output_buffer: Vec::new(),
            buffer_pos: 0,
            _left_pos: 0,
            _right_pos: 0,
            initialized: false,
        }
    }

    fn extract_left_key(&self, tuple: &Tuple) -> Result<Vec<Datum>> {
        self.left_key_ops
            .iter()
            .map(|ops| evaluate_expr(ops, tuple))
            .collect()
    }

    fn extract_right_key(&self, tuple: &Tuple) -> Result<Vec<Datum>> {
        self.right_key_ops
            .iter()
            .map(|ops| evaluate_expr(ops, tuple))
            .collect()
    }

    fn compare_keys(left_key: &[Datum], right_key: &[Datum]) -> Result<Ordering> {
        for (l, r) in left_key.iter().zip(right_key.iter()) {
            if l.is_null() || r.is_null() {
                // NULLs sort to the end, never match
                return Ok(match (l.is_null(), r.is_null()) {
                    (true, false) => Ordering::Greater,
                    (false, true) => Ordering::Less,
                    _ => Ordering::Equal,
                });
            }
            let cmp = l.sql_cmp(r)?.unwrap_or(Ordering::Equal);
            if cmp != Ordering::Equal {
                return Ok(cmp);
            }
        }
        Ok(Ordering::Equal)
    }
}

impl Executor for SortMergeJoinExecutor {
    fn init(&mut self) -> Result<()> {
        self.left.init()?;
        self.right.init()?;

        let left_schema = self.left.schema().clone();
        let right_schema = self.right.schema().clone();
        self.left_col_count = left_schema.column_count();
        self.right_col_count = right_schema.column_count();

        self.left_key_ops = self
            .left_keys_src
            .iter()
            .map(|e| compile_expr(e, &left_schema))
            .collect::<Result<Vec<_>>>()?;
        self.right_key_ops = self
            .right_keys_src
            .iter()
            .map(|e| compile_expr(e, &right_schema))
            .collect::<Result<Vec<_>>>()?;

        if let Some(ref cond) = self.condition_src {
            self.condition_ops = Some(compile_expr(cond, &self.schema)?);
        }

        // Materialize both sides (they should already be sorted)
        self.left_tuples.clear();
        while let Some(t) = self.left.next()? {
            self.left_tuples.push(t);
        }
        self.right_tuples.clear();
        while let Some(t) = self.right.next()? {
            self.right_tuples.push(t);
        }

        // Perform the merge join and buffer all results
        self.output_buffer = self.merge_join()?;
        self.buffer_pos = 0;
        self.initialized = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.buffer_pos < self.output_buffer.len() {
            let t = self.output_buffer[self.buffer_pos].clone();
            self.buffer_pos += 1;
            Ok(Some(t))
        } else {
            Ok(None)
        }
    }

    fn close(&mut self) -> Result<()> {
        self.left_tuples.clear();
        self.right_tuples.clear();
        self.output_buffer.clear();
        self.left.close()?;
        self.right.close()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl SortMergeJoinExecutor {
    fn merge_join(&self) -> Result<Vec<Tuple>> {
        let mut results = Vec::new();
        let mut li = 0usize;
        let mut ri = 0usize;
        let mut left_matched = vec![false; self.left_tuples.len()];
        let mut right_matched = vec![false; self.right_tuples.len()];

        while li < self.left_tuples.len() && ri < self.right_tuples.len() {
            let lk = self.extract_left_key(&self.left_tuples[li])?;
            let rk = self.extract_right_key(&self.right_tuples[ri])?;

            match Self::compare_keys(&lk, &rk)? {
                Ordering::Less => {
                    li += 1;
                }
                Ordering::Greater => {
                    ri += 1;
                }
                Ordering::Equal => {
                    // Find extent of equal keys on both sides
                    let li_start = li;
                    while li < self.left_tuples.len() {
                        let k = self.extract_left_key(&self.left_tuples[li])?;
                        if Self::compare_keys(&k, &lk)? != Ordering::Equal {
                            break;
                        }
                        li += 1;
                    }
                    let ri_start = ri;
                    while ri < self.right_tuples.len() {
                        let k = self.extract_right_key(&self.right_tuples[ri])?;
                        if Self::compare_keys(&k, &rk)? != Ordering::Equal {
                            break;
                        }
                        ri += 1;
                    }

                    // Cross-product of matching groups
                    for l in li_start..li {
                        for r in ri_start..ri {
                            let mut vals = self.left_tuples[l].values().to_vec();
                            vals.extend_from_slice(self.right_tuples[r].values());
                            let candidate = intermediate_tuple(vals);

                            if let Some(ref cond) = self.condition_ops {
                                let result = evaluate_expr(cond, &candidate)?;
                                if !is_truthy(&result) {
                                    continue;
                                }
                            }

                            left_matched[l] = true;
                            right_matched[r] = true;
                            results.push(candidate);
                        }
                    }
                }
            }
        }

        // SEMI join: emit left rows that matched (left columns only)
        if self.join_type == JoinType::LeftSemi {
            let mut semi_results = Vec::new();
            for (i, &matched) in left_matched.iter().enumerate() {
                if matched {
                    semi_results.push(intermediate_tuple(
                        self.left_tuples[i].values().to_vec(),
                    ));
                }
            }
            return Ok(semi_results);
        }

        // ANTI join: emit left rows that did NOT match (left columns only)
        if self.join_type == JoinType::LeftAnti {
            let mut anti_results = Vec::new();
            for (i, &matched) in left_matched.iter().enumerate() {
                if !matched {
                    anti_results.push(intermediate_tuple(
                        self.left_tuples[i].values().to_vec(),
                    ));
                }
            }
            return Ok(anti_results);
        }

        // Emit unmatched rows for outer joins
        if matches!(self.join_type, JoinType::Left | JoinType::Full) {
            for (i, &matched) in left_matched.iter().enumerate() {
                if !matched {
                    let mut vals = self.left_tuples[i].values().to_vec();
                    vals.resize(self.left_col_count + self.right_col_count, Datum::Null);
                    results.push(intermediate_tuple(vals));
                }
            }
        }
        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
            for (i, &matched) in right_matched.iter().enumerate() {
                if !matched {
                    let mut vals = vec![Datum::Null; self.left_col_count];
                    vals.extend_from_slice(self.right_tuples[i].values());
                    results.push(intermediate_tuple(vals));
                }
            }
        }

        Ok(results)
    }
}
