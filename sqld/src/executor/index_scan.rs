use std::sync::Arc;

use crate::planner::physical_plan::KeyRange;
use crate::sql::ast::Expr;
use crate::types::{Schema, Tuple};
use crate::utils::error::Result;

use super::executor::{Executor, ExecutorContext};
use super::expr_eval::{compile_expr, evaluate_expr, is_truthy, ExprOp};

// ---------------------------------------------------------------------------
// IndexScan — B+ tree walk, fetch heap tuples by TID
// ---------------------------------------------------------------------------

pub struct IndexScanExecutor {
    ctx: Arc<ExecutorContext>,
    table: String,
    index_name: String,
    schema: Schema,
    key_ranges: Vec<KeyRange>,
    predicate_src: Option<Expr>,
    predicate: Option<Vec<ExprOp>>,
    tuples: Vec<Tuple>,
    position: usize,
    initialized: bool,
}

impl IndexScanExecutor {
    pub fn new(
        ctx: Arc<ExecutorContext>,
        table: String,
        index_name: String,
        schema: Schema,
        key_ranges: Vec<KeyRange>,
        predicate: Option<Expr>,
    ) -> Self {
        Self {
            ctx,
            table,
            index_name,
            schema,
            key_ranges,
            predicate_src: predicate,
            predicate: None,
            tuples: Vec::new(),
            position: 0,
            initialized: false,
        }
    }
}

impl Executor for IndexScanExecutor {
    fn init(&mut self) -> Result<()> {
        // Compile residual predicate
        if let Some(ref expr) = self.predicate_src {
            self.predicate = Some(compile_expr(expr, &self.schema)?);
        }
        // Use index to get matching tuples (index-only scan when possible,
        // otherwise fetch heap tuples by TID)
        self.tuples = self.ctx.catalog.scan_index(
            &self.table,
            &self.index_name,
            &self.key_ranges,
        )?;
        self.position = 0;
        self.initialized = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        while self.position < self.tuples.len() {
            let tuple = self.tuples[self.position].clone();
            self.position += 1;

            // Apply residual predicate
            if let Some(ref pred) = self.predicate {
                let result = evaluate_expr(pred, &tuple)?;
                if !is_truthy(&result) {
                    continue;
                }
            }

            return Ok(Some(tuple));
        }
        Ok(None)
    }

    fn close(&mut self) -> Result<()> {
        self.tuples.clear();
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
