use crate::sql::ast::Expr;
use crate::types::{Datum, Schema, Tuple};
use crate::utils::error::Result;

use super::executor::{intermediate_tuple, Executor};

// ---------------------------------------------------------------------------
// Values — literal row set
// ---------------------------------------------------------------------------

pub struct ValuesExecutor {
    rows: Vec<Vec<Expr>>,
    schema: Schema,
    materialized: Vec<Tuple>,
    position: usize,
    initialized: bool,
}

impl ValuesExecutor {
    pub fn new(rows: Vec<Vec<Expr>>, schema: Schema) -> Self {
        Self {
            rows,
            schema,
            materialized: Vec::new(),
            position: 0,
            initialized: false,
        }
    }
}

impl Executor for ValuesExecutor {
    fn init(&mut self) -> Result<()> {
        self.materialized.clear();
        for row in &self.rows {
            let values: Vec<Datum> = row.iter().map(expr_to_datum).collect();
            self.materialized.push(intermediate_tuple(values));
        }
        self.position = 0;
        self.initialized = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.position < self.materialized.len() {
            let t = self.materialized[self.position].clone();
            self.position += 1;
            Ok(Some(t))
        } else {
            Ok(None)
        }
    }

    fn close(&mut self) -> Result<()> {
        self.materialized.clear();
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

/// Convert an AST literal expression to a Datum.
fn expr_to_datum(expr: &Expr) -> Datum {
    match expr {
        Expr::Integer(v) => {
            if *v >= i32::MIN as i64 && *v <= i32::MAX as i64 {
                Datum::Integer(*v as i32)
            } else {
                Datum::BigInt(*v)
            }
        }
        Expr::Float(v) => Datum::Float(*v),
        Expr::String(s) => Datum::Text(s.clone()),
        Expr::Boolean(b) => Datum::Boolean(*b),
        Expr::Null => Datum::Null,
        _ => Datum::Null, // Non-literal expressions default to NULL
    }
}
