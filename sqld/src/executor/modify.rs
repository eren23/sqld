use std::sync::Arc;

use crate::sql::ast::Expr;
use crate::types::{Schema, Tuple};
use crate::utils::error::Result;

use super::executor::{Executor, ExecutorContext};
use super::expr_eval::{compile_expr, evaluate_expr, ExprOp};

// ---------------------------------------------------------------------------
// Modify — INSERT / UPDATE / DELETE with RETURNING support
// ---------------------------------------------------------------------------

pub enum ModifyOp {
    Insert {
        table: String,
        columns: Vec<String>,
    },
    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        compiled_assignments: Vec<(String, Vec<ExprOp>)>,
    },
    Delete {
        table: String,
    },
}

pub struct ModifyExecutor {
    ctx: Arc<ExecutorContext>,
    child: Box<dyn Executor>,
    op: ModifyOp,
    schema: Schema,
    results: Vec<Tuple>,
    position: usize,
    initialized: bool,
}

impl ModifyExecutor {
    pub fn new_insert(
        ctx: Arc<ExecutorContext>,
        child: Box<dyn Executor>,
        table: String,
        columns: Vec<String>,
    ) -> Self {
        let schema = child.schema().clone();
        Self {
            ctx,
            child,
            op: ModifyOp::Insert { table, columns },
            schema,
            results: Vec::new(),
            position: 0,
            initialized: false,
        }
    }

    pub fn new_update(
        ctx: Arc<ExecutorContext>,
        child: Box<dyn Executor>,
        table: String,
        assignments: Vec<(String, Expr)>,
    ) -> Self {
        let schema = child.schema().clone();
        Self {
            ctx,
            child,
            op: ModifyOp::Update {
                table,
                assignments,
                compiled_assignments: Vec::new(),
            },
            schema,
            results: Vec::new(),
            position: 0,
            initialized: false,
        }
    }

    pub fn new_delete(
        ctx: Arc<ExecutorContext>,
        child: Box<dyn Executor>,
        table: String,
    ) -> Self {
        let schema = child.schema().clone();
        Self {
            ctx,
            child,
            op: ModifyOp::Delete { table },
            schema,
            results: Vec::new(),
            position: 0,
            initialized: false,
        }
    }
}

impl Executor for ModifyExecutor {
    fn init(&mut self) -> Result<()> {
        self.child.init()?;

        // Compile update assignment expressions
        if let ModifyOp::Update {
            ref assignments,
            ref mut compiled_assignments,
            ..
        } = self.op
        {
            let input_schema = self.child.schema().clone();
            *compiled_assignments = assignments
                .iter()
                .map(|(col, expr)| {
                    let ops = compile_expr(expr, &input_schema)?;
                    Ok((col.clone(), ops))
                })
                .collect::<Result<Vec<_>>>()?;
        }

        // Process all input rows
        self.results.clear();
        while let Some(tuple) = self.child.next()? {
            match &self.op {
                ModifyOp::Insert { table, columns: _ } => {
                    // Insert the tuple's values into the table
                    // The catalog provider handles index maintenance,
                    // constraint checking, and WAL writes
                    let values = tuple.values().to_vec();
                    let inserted = self.ctx.catalog.insert_tuple(table, values)?;
                    self.results.push(inserted);
                }
                ModifyOp::Update {
                    table,
                    compiled_assignments,
                    ..
                } => {
                    // Evaluate new values for assigned columns
                    let table_schema = self.ctx.catalog.table_schema(table)?;
                    let mut new_values = tuple.values().to_vec();

                    for (col_name, ops) in compiled_assignments {
                        let new_val = evaluate_expr(ops, &tuple)?;
                        // Find column ordinal in table schema
                        if let Some((idx, _)) = table_schema.column_by_name(col_name) {
                            if idx < new_values.len() {
                                new_values[idx] = new_val;
                            }
                        }
                    }

                    let updated =
                        self.ctx.catalog.update_tuple(table, &tuple, new_values)?;
                    self.results.push(updated);
                }
                ModifyOp::Delete { table } => {
                    // Delete the tuple
                    let deleted = self.ctx.catalog.delete_tuple(table, &tuple)?;
                    self.results.push(deleted);
                }
            }
        }

        self.position = 0;
        self.initialized = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        // RETURNING: emit the affected rows
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
