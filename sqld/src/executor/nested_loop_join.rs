use crate::sql::ast::{Expr, JoinType};
use crate::types::{Datum, Schema, Tuple};
use crate::utils::error::Result;

use super::executor::{intermediate_tuple, Executor};
use super::expr_eval::{compile_expr, evaluate_expr, is_truthy, ExprOp};

// ---------------------------------------------------------------------------
// NestedLoopJoin — parameterized inner, supports all join types
// ---------------------------------------------------------------------------

pub struct NestedLoopJoinExecutor {
    left: Box<dyn Executor>,
    right: Box<dyn Executor>,
    join_type: JoinType,
    condition_src: Option<Expr>,
    schema: Schema,

    condition_ops: Option<Vec<ExprOp>>,
    left_col_count: usize,
    right_col_count: usize,

    // Materialized right side (re-scanned for each left tuple)
    right_tuples: Vec<Tuple>,
    output_buffer: Vec<Tuple>,
    buffer_pos: usize,
    initialized: bool,
}

impl NestedLoopJoinExecutor {
    pub fn new(
        left: Box<dyn Executor>,
        right: Box<dyn Executor>,
        join_type: JoinType,
        condition: Option<Expr>,
        schema: Schema,
    ) -> Self {
        Self {
            left,
            right,
            join_type,
            condition_src: condition,
            schema,
            condition_ops: None,
            left_col_count: 0,
            right_col_count: 0,
            right_tuples: Vec::new(),
            output_buffer: Vec::new(),
            buffer_pos: 0,
            initialized: false,
        }
    }
}

impl Executor for NestedLoopJoinExecutor {
    fn init(&mut self) -> Result<()> {
        self.left.init()?;
        self.right.init()?;

        self.left_col_count = self.left.schema().column_count();
        self.right_col_count = self.right.schema().column_count();

        if let Some(ref cond) = self.condition_src {
            self.condition_ops = Some(compile_expr(cond, &self.schema)?);
        }

        // Materialize right side
        self.right_tuples.clear();
        while let Some(t) = self.right.next()? {
            self.right_tuples.push(t);
        }

        // Perform the nested loop join
        self.output_buffer = self.do_join()?;
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
        self.right_tuples.clear();
        self.output_buffer.clear();
        self.left.close()?;
        self.right.close()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl NestedLoopJoinExecutor {
    fn do_join(&mut self) -> Result<Vec<Tuple>> {
        let mut results = Vec::new();

        // Materialize left side too
        let mut left_tuples = Vec::new();
        while let Some(t) = self.left.next()? {
            left_tuples.push(t);
        }

        let mut left_matched = vec![false; left_tuples.len()];
        let mut right_matched = vec![false; self.right_tuples.len()];

        for (li, left_tuple) in left_tuples.iter().enumerate() {
            let left_vals = left_tuple.values();

            for (ri, right_tuple) in self.right_tuples.iter().enumerate() {
                let right_vals = right_tuple.values();

                // Build combined tuple
                let mut combined_vals = left_vals.to_vec();
                combined_vals.extend_from_slice(right_vals);
                let combined = intermediate_tuple(combined_vals);

                // Evaluate join condition
                let pass = if let Some(ref cond) = self.condition_ops {
                    let result = evaluate_expr(cond, &combined)?;
                    is_truthy(&result)
                } else {
                    true // CROSS JOIN
                };

                if pass {
                    left_matched[li] = true;
                    right_matched[ri] = true;
                    match self.join_type {
                        JoinType::Inner
                        | JoinType::Left
                        | JoinType::Right
                        | JoinType::Full
                        | JoinType::Cross => {
                            results.push(combined);
                        }
                        JoinType::LeftSemi => {
                            // For SEMI, stop inner loop on first match
                            break;
                        }
                        JoinType::LeftAnti => {
                            // For ANTI, just mark matched, don't emit
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
                        left_tuples[i].values().to_vec(),
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
                        left_tuples[i].values().to_vec(),
                    ));
                }
            }
            return Ok(anti_results);
        }

        // Emit unmatched rows for outer joins
        if matches!(self.join_type, JoinType::Left | JoinType::Full) {
            for (i, &matched) in left_matched.iter().enumerate() {
                if !matched {
                    let mut vals = left_tuples[i].values().to_vec();
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
