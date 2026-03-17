use crate::sql::ast::Expr;
use crate::types::{Schema, Tuple};
use crate::utils::error::Result;

use super::executor::Executor;
use super::expr_eval::{compile_expr, evaluate_expr, is_truthy, ExprOp};

// ---------------------------------------------------------------------------
// Filter — predicate evaluation
// ---------------------------------------------------------------------------

pub struct FilterExecutor {
    child: Box<dyn Executor>,
    predicate_src: Expr,
    predicate: Vec<ExprOp>,
    schema: Schema,
    initialized: bool,
}

impl FilterExecutor {
    pub fn new(child: Box<dyn Executor>, predicate: Expr) -> Self {
        let schema = child.schema().clone();
        Self {
            child,
            predicate_src: predicate,
            predicate: Vec::new(),
            schema,
            initialized: false,
        }
    }
}

impl Executor for FilterExecutor {
    fn init(&mut self) -> Result<()> {
        self.child.init()?;
        self.predicate = compile_expr(&self.predicate_src, &self.schema)?;
        self.initialized = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        loop {
            match self.child.next()? {
                None => return Ok(None),
                Some(tuple) => {
                    let result = evaluate_expr(&self.predicate, &tuple)?;
                    if is_truthy(&result) {
                        return Ok(Some(tuple));
                    }
                    // Skip non-matching tuples
                }
            }
        }
    }

    fn close(&mut self) -> Result<()> {
        self.child.close()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
