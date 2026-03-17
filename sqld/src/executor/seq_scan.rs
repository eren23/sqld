use std::sync::Arc;

use crate::sql::ast::Expr;
use crate::types::{Schema, Tuple};
use crate::utils::error::Result;

use super::executor::{Executor, ExecutorContext};
use super::expr_eval::{compile_expr, evaluate_expr, is_truthy, ExprOp};

// ---------------------------------------------------------------------------
// SeqScan — sequential heap scan with optional pushed-down predicate
// ---------------------------------------------------------------------------

pub struct SeqScanExecutor {
    ctx: Arc<ExecutorContext>,
    table: String,
    schema: Schema,
    predicate_src: Option<Expr>,
    predicate: Option<Vec<ExprOp>>,
    tuples: Vec<Tuple>,
    position: usize,
    initialized: bool,
}

impl SeqScanExecutor {
    pub fn new(
        ctx: Arc<ExecutorContext>,
        table: String,
        schema: Schema,
        predicate: Option<Expr>,
    ) -> Self {
        Self {
            ctx,
            table,
            schema,
            predicate_src: predicate,
            predicate: None,
            tuples: Vec::new(),
            position: 0,
            initialized: false,
        }
    }
}

impl Executor for SeqScanExecutor {
    fn init(&mut self) -> Result<()> {
        // Compile predicate
        if let Some(ref expr) = self.predicate_src {
            self.predicate = Some(compile_expr(expr, &self.schema)?);
        }
        // Scan the table via the catalog provider (handles MVCC visibility)
        self.tuples = self.ctx.catalog.scan_table(&self.table)?;
        self.position = 0;
        self.initialized = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        while self.position < self.tuples.len() {
            let tuple = self.tuples[self.position].clone();
            self.position += 1;

            // Apply pushed-down predicate filter
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
