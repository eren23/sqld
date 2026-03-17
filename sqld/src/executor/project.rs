use crate::planner::logical_plan::ProjectionExpr;
use crate::types::{Column, Schema, Tuple};
use crate::utils::error::Result;

use super::executor::{intermediate_tuple, Executor};
use super::expr_eval::{compile_expr, evaluate_expr, ExprOp};

// ---------------------------------------------------------------------------
// Project — expression projection
// ---------------------------------------------------------------------------

pub struct ProjectExecutor {
    child: Box<dyn Executor>,
    expressions: Vec<ProjectionExpr>,
    compiled: Vec<Vec<ExprOp>>,
    schema: Schema,
    initialized: bool,
}

impl ProjectExecutor {
    pub fn new(child: Box<dyn Executor>, expressions: Vec<ProjectionExpr>) -> Self {
        // Build output schema from projection expressions
        let cols: Vec<Column> = expressions
            .iter()
            .map(|pe| {
                let dt =
                    crate::planner::logical_plan::LogicalPlan::infer_expr_type(&pe.expr);
                Column::new(pe.alias.clone(), dt, true)
            })
            .collect();
        let schema = Schema::new(cols);
        Self {
            child,
            expressions,
            compiled: Vec::new(),
            schema,
            initialized: false,
        }
    }
}

impl Executor for ProjectExecutor {
    fn init(&mut self) -> Result<()> {
        self.child.init()?;
        let input_schema = self.child.schema();
        self.compiled = self
            .expressions
            .iter()
            .map(|pe| compile_expr(&pe.expr, input_schema))
            .collect::<Result<Vec<_>>>()?;
        self.initialized = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        match self.child.next()? {
            None => Ok(None),
            Some(tuple) => {
                let mut values = Vec::with_capacity(self.compiled.len());
                for ops in &self.compiled {
                    values.push(evaluate_expr(ops, &tuple)?);
                }
                Ok(Some(intermediate_tuple(values)))
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
