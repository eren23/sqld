use std::sync::Arc;

use sqld::executor::executor::{
    intermediate_tuple, CatalogProvider, Executor, ExecutorContext,
};
use sqld::executor::modify::ModifyExecutor;
use sqld::executor::values::ValuesExecutor;
use sqld::planner::physical_plan::KeyRange;
use sqld::sql::ast::Expr;
use sqld::types::{Column, DataType, Datum, Schema, Tuple};
use sqld::utils::error::Result;

// ---------------------------------------------------------------------------
// Mock catalog
// ---------------------------------------------------------------------------

struct MockCatalog {
    schema: Schema,
}

impl CatalogProvider for MockCatalog {
    fn table_schema(&self, _table: &str) -> Result<Schema> {
        Ok(self.schema.clone())
    }

    fn scan_table(&self, _table: &str) -> Result<Vec<Tuple>> {
        Ok(vec![])
    }

    fn scan_index(
        &self,
        _table: &str,
        _index: &str,
        _ranges: &[KeyRange],
    ) -> Result<Vec<Tuple>> {
        Ok(vec![])
    }

    fn insert_tuple(&self, _table: &str, values: Vec<Datum>) -> Result<Tuple> {
        Ok(intermediate_tuple(values))
    }

    fn delete_tuple(&self, _table: &str, tuple: &Tuple) -> Result<Tuple> {
        Ok(tuple.clone())
    }

    fn update_tuple(
        &self,
        _table: &str,
        _old_tuple: &Tuple,
        new_values: Vec<Datum>,
    ) -> Result<Tuple> {
        Ok(intermediate_tuple(new_values))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn datum_to_expr(d: Datum) -> Expr {
    match d {
        Datum::Integer(v) => Expr::Integer(v as i64),
        Datum::BigInt(v) => Expr::Integer(v),
        Datum::Float(v) => Expr::Float(v),
        Datum::Text(s) | Datum::Varchar(s) => Expr::String(s),
        Datum::Boolean(b) => Expr::Boolean(b),
        Datum::Null => Expr::Null,
        _ => Expr::Null,
    }
}

fn make_source(rows: Vec<Vec<Datum>>, schema: Schema) -> Box<dyn Executor> {
    let expr_rows: Vec<Vec<Expr>> = rows
        .into_iter()
        .map(|row| row.into_iter().map(datum_to_expr).collect())
        .collect();
    Box::new(ValuesExecutor::new(expr_rows, schema))
}

fn collect(exec: &mut dyn Executor) -> Vec<Vec<Datum>> {
    exec.init().unwrap();
    let mut out = Vec::new();
    while let Some(t) = exec.next().unwrap() {
        out.push(t.values().to_vec());
    }
    exec.close().unwrap();
    out
}

fn test_schema() -> Schema {
    Schema::new(vec![
        Column::new("id", DataType::Integer, false),
        Column::new("name", DataType::Text, false),
        Column::new("value", DataType::Integer, true),
    ])
}

fn make_ctx(schema: Schema) -> Arc<ExecutorContext> {
    let catalog = Arc::new(MockCatalog { schema });
    Arc::new(ExecutorContext::new(catalog))
}

// ---------------------------------------------------------------------------
// INSERT tests
// ---------------------------------------------------------------------------

#[test]
fn insert_single_row() {
    let schema = test_schema();
    let ctx = make_ctx(schema.clone());

    let rows = vec![vec![
        Datum::Integer(1),
        Datum::Text("Alice".into()),
        Datum::Integer(100),
    ]];
    let source = make_source(rows, schema.clone());

    let mut exec = ModifyExecutor::new_insert(
        ctx,
        source,
        "test_table".into(),
        vec!["id".into(), "name".into(), "value".into()],
    );
    let result = collect(&mut exec);

    assert_eq!(result.len(), 1);
    assert_eq!(result[0][0], Datum::Integer(1));
    assert_eq!(result[0][1], Datum::Text("Alice".into()));
    assert_eq!(result[0][2], Datum::Integer(100));
}

#[test]
fn insert_multiple_rows() {
    let schema = test_schema();
    let ctx = make_ctx(schema.clone());

    let rows = vec![
        vec![Datum::Integer(1), Datum::Text("A".into()), Datum::Integer(10)],
        vec![Datum::Integer(2), Datum::Text("B".into()), Datum::Integer(20)],
        vec![Datum::Integer(3), Datum::Text("C".into()), Datum::Integer(30)],
    ];
    let source = make_source(rows, schema.clone());

    let mut exec = ModifyExecutor::new_insert(
        ctx,
        source,
        "test_table".into(),
        vec!["id".into(), "name".into(), "value".into()],
    );
    let result = collect(&mut exec);

    assert_eq!(result.len(), 3);
    assert_eq!(result[0][0], Datum::Integer(1));
    assert_eq!(result[1][0], Datum::Integer(2));
    assert_eq!(result[2][0], Datum::Integer(3));
}

// ---------------------------------------------------------------------------
// UPDATE tests
// ---------------------------------------------------------------------------

#[test]
fn update_single_column() {
    // UPDATE test_table SET value = 999
    let schema = test_schema();
    let ctx = make_ctx(schema.clone());

    let rows = vec![
        vec![Datum::Integer(1), Datum::Text("Alice".into()), Datum::Integer(100)],
        vec![Datum::Integer(2), Datum::Text("Bob".into()),   Datum::Integer(200)],
    ];
    let source = make_source(rows, schema.clone());

    let assignments = vec![("value".into(), Expr::Integer(999))];

    let mut exec = ModifyExecutor::new_update(
        ctx,
        source,
        "test_table".into(),
        assignments,
    );
    let result = collect(&mut exec);

    assert_eq!(result.len(), 2);
    // id and name stay the same; value becomes 999
    assert_eq!(result[0][0], Datum::Integer(1));
    assert_eq!(result[0][1], Datum::Text("Alice".into()));
    assert_eq!(result[0][2], Datum::Integer(999));
    assert_eq!(result[1][0], Datum::Integer(2));
    assert_eq!(result[1][1], Datum::Text("Bob".into()));
    assert_eq!(result[1][2], Datum::Integer(999));
}

#[test]
fn update_multiple_columns() {
    // UPDATE test_table SET name = 'updated', value = 0
    let schema = test_schema();
    let ctx = make_ctx(schema.clone());

    let rows = vec![vec![
        Datum::Integer(1),
        Datum::Text("old".into()),
        Datum::Integer(42),
    ]];
    let source = make_source(rows, schema.clone());

    let assignments = vec![
        ("name".into(), Expr::String("updated".into())),
        ("value".into(), Expr::Integer(0)),
    ];

    let mut exec = ModifyExecutor::new_update(
        ctx,
        source,
        "test_table".into(),
        assignments,
    );
    let result = collect(&mut exec);

    assert_eq!(result.len(), 1);
    assert_eq!(result[0][0], Datum::Integer(1));       // id unchanged
    assert_eq!(result[0][1], Datum::Text("updated".into()));
    assert_eq!(result[0][2], Datum::Integer(0));
}

// ---------------------------------------------------------------------------
// DELETE tests
// ---------------------------------------------------------------------------

#[test]
fn delete_rows() {
    let schema = test_schema();
    let ctx = make_ctx(schema.clone());

    let rows = vec![
        vec![Datum::Integer(1), Datum::Text("A".into()), Datum::Integer(10)],
        vec![Datum::Integer(2), Datum::Text("B".into()), Datum::Integer(20)],
    ];
    let source = make_source(rows, schema.clone());

    let mut exec = ModifyExecutor::new_delete(ctx, source, "test_table".into());
    let result = collect(&mut exec);

    // Delete returns the deleted rows
    assert_eq!(result.len(), 2);
    assert_eq!(result[0][0], Datum::Integer(1));
    assert_eq!(result[1][0], Datum::Integer(2));
}

#[test]
fn delete_empty_input() {
    let schema = test_schema();
    let ctx = make_ctx(schema.clone());

    let source = make_source(vec![], schema.clone());

    let mut exec = ModifyExecutor::new_delete(ctx, source, "test_table".into());
    let result = collect(&mut exec);

    assert!(result.is_empty());
}

// ---------------------------------------------------------------------------
// Schema preservation
// ---------------------------------------------------------------------------

#[test]
fn insert_preserves_schema() {
    let schema = test_schema();
    let ctx = make_ctx(schema.clone());

    let source = make_source(vec![], schema.clone());
    let exec = ModifyExecutor::new_insert(
        ctx,
        source,
        "test_table".into(),
        vec!["id".into(), "name".into(), "value".into()],
    );

    let out_schema = exec.schema();
    assert_eq!(out_schema.column_count(), 3);
    assert_eq!(out_schema.columns()[0].name, "id");
    assert_eq!(out_schema.columns()[1].name, "name");
    assert_eq!(out_schema.columns()[2].name, "value");
}
