use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sqld::executor::executor::{CatalogProvider, Executor, ExecutorContext, intermediate_tuple};
use sqld::executor::seq_scan::SeqScanExecutor;
use sqld::planner::physical_plan::KeyRange;
use sqld::sql::ast::{BinaryOp, Expr};
use sqld::types::{Column, DataType, Datum, MvccHeader, Schema, Tuple};
use sqld::utils::error::Result;

// ===========================================================================
// Mock catalog provider
// ===========================================================================

struct MockCatalog {
    tables: Mutex<HashMap<String, (Schema, Vec<Tuple>)>>,
}

impl MockCatalog {
    fn new() -> Self {
        Self {
            tables: Mutex::new(HashMap::new()),
        }
    }

    fn add_table(&self, name: &str, schema: Schema, tuples: Vec<Tuple>) {
        self.tables
            .lock()
            .unwrap()
            .insert(name.to_string(), (schema, tuples));
    }
}

impl CatalogProvider for MockCatalog {
    fn table_schema(&self, table: &str) -> Result<Schema> {
        let tables = self.tables.lock().unwrap();
        tables
            .get(table)
            .map(|(s, _)| s.clone())
            .ok_or_else(|| {
                sqld::utils::error::SqlError::ExecutionError(format!(
                    "table not found: {table}"
                ))
                .into()
            })
    }

    fn scan_table(&self, table: &str) -> Result<Vec<Tuple>> {
        let tables = self.tables.lock().unwrap();
        tables
            .get(table)
            .map(|(_, t)| t.clone())
            .ok_or_else(|| {
                sqld::utils::error::SqlError::ExecutionError(format!(
                    "table not found: {table}"
                ))
                .into()
            })
    }

    fn scan_index(
        &self,
        table: &str,
        _index: &str,
        _ranges: &[KeyRange],
    ) -> Result<Vec<Tuple>> {
        self.scan_table(table)
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
        _old: &Tuple,
        new_values: Vec<Datum>,
    ) -> Result<Tuple> {
        Ok(intermediate_tuple(new_values))
    }
}

// ===========================================================================
// Helpers
// ===========================================================================

fn make_tuple(values: Vec<Datum>) -> Tuple {
    Tuple::new(MvccHeader::new(0, 0, 0), values)
}

fn employees_schema() -> Schema {
    Schema::new(vec![
        Column::new("id", DataType::Integer, false),
        Column::new("name", DataType::Text, false),
        Column::new("salary", DataType::Integer, true),
    ])
}

fn employees_data() -> Vec<Tuple> {
    vec![
        make_tuple(vec![
            Datum::Integer(1),
            Datum::Text("Alice".into()),
            Datum::Integer(50000),
        ]),
        make_tuple(vec![
            Datum::Integer(2),
            Datum::Text("Bob".into()),
            Datum::Integer(60000),
        ]),
        make_tuple(vec![
            Datum::Integer(3),
            Datum::Text("Charlie".into()),
            Datum::Integer(45000),
        ]),
        make_tuple(vec![
            Datum::Integer(4),
            Datum::Text("Diana".into()),
            Datum::Null,
        ]),
    ]
}

fn collect_all(exec: &mut dyn Executor) -> Vec<Tuple> {
    let mut rows = Vec::new();
    while let Some(tuple) = exec.next().expect("next() failed") {
        rows.push(tuple);
    }
    rows
}

fn setup_catalog() -> Arc<MockCatalog> {
    let catalog = Arc::new(MockCatalog::new());
    catalog.add_table("employees", employees_schema(), employees_data());
    catalog
}

// ===========================================================================
// Tests using SeqScanExecutor directly
// ===========================================================================

#[test]
fn seq_scan_returns_all_rows() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));
    let schema = employees_schema();

    let mut scan = SeqScanExecutor::new(ctx, "employees".into(), schema, None);
    scan.init().unwrap();

    let rows = collect_all(&mut scan);
    assert_eq!(rows.len(), 4, "should return all 4 employee rows");

    // Verify first and last row content
    assert_eq!(rows[0].get(0).unwrap(), &Datum::Integer(1));
    assert_eq!(rows[0].get(1).unwrap(), &Datum::Text("Alice".into()));
    assert_eq!(rows[3].get(0).unwrap(), &Datum::Integer(4));
    assert_eq!(rows[3].get(1).unwrap(), &Datum::Text("Diana".into()));

    scan.close().unwrap();
}

#[test]
fn seq_scan_with_equality_predicate() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));
    let schema = employees_schema();

    // WHERE id = 2
    let predicate = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("id".into())),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Integer(2)),
    };

    let mut scan = SeqScanExecutor::new(ctx, "employees".into(), schema, Some(predicate));
    scan.init().unwrap();

    let rows = collect_all(&mut scan);
    assert_eq!(rows.len(), 1, "should return exactly one row with id=2");
    assert_eq!(rows[0].get(1).unwrap(), &Datum::Text("Bob".into()));

    scan.close().unwrap();
}

#[test]
fn seq_scan_with_comparison_predicate() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));
    let schema = employees_schema();

    // WHERE salary > 48000
    let predicate = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("salary".into())),
        op: BinaryOp::Gt,
        right: Box::new(Expr::Integer(48000)),
    };

    let mut scan = SeqScanExecutor::new(ctx, "employees".into(), schema, Some(predicate));
    scan.init().unwrap();

    let rows = collect_all(&mut scan);
    // Alice (50000) and Bob (60000) pass; Charlie (45000) and Diana (NULL) do not
    assert_eq!(rows.len(), 2, "should return Alice and Bob");
    assert_eq!(rows[0].get(1).unwrap(), &Datum::Text("Alice".into()));
    assert_eq!(rows[1].get(1).unwrap(), &Datum::Text("Bob".into()));

    scan.close().unwrap();
}

#[test]
fn seq_scan_empty_table() {
    let catalog = Arc::new(MockCatalog::new());
    let schema = Schema::new(vec![Column::new("x", DataType::Integer, false)]);
    catalog.add_table("empty", schema.clone(), vec![]);
    let ctx = Arc::new(ExecutorContext::new(catalog));

    let mut scan = SeqScanExecutor::new(ctx, "empty".into(), schema, None);
    scan.init().unwrap();

    let result = scan.next().unwrap();
    assert!(result.is_none(), "scan on empty table should return None immediately");

    scan.close().unwrap();
}

#[test]
fn seq_scan_predicate_filters_all_rows() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));
    let schema = employees_schema();

    // WHERE salary > 100000 (no employee earns that much)
    let predicate = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("salary".into())),
        op: BinaryOp::Gt,
        right: Box::new(Expr::Integer(100000)),
    };

    let mut scan = SeqScanExecutor::new(ctx, "employees".into(), schema, Some(predicate));
    scan.init().unwrap();

    let rows = collect_all(&mut scan);
    assert_eq!(rows.len(), 0, "no rows should pass salary > 100000");

    scan.close().unwrap();
}

#[test]
fn seq_scan_null_predicate_handling() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));
    let schema = employees_schema();

    // WHERE salary IS NULL
    let predicate = Expr::IsNull {
        expr: Box::new(Expr::Identifier("salary".into())),
        negated: false,
    };

    let mut scan = SeqScanExecutor::new(ctx, "employees".into(), schema, Some(predicate));
    scan.init().unwrap();

    let rows = collect_all(&mut scan);
    // Only Diana has NULL salary
    assert_eq!(rows.len(), 1, "only Diana has NULL salary");
    assert_eq!(rows[0].get(1).unwrap(), &Datum::Text("Diana".into()));

    scan.close().unwrap();
}

#[test]
fn seq_scan_schema_accessor() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));
    let schema = employees_schema();

    let scan = SeqScanExecutor::new(ctx, "employees".into(), schema, None);
    let s = scan.schema();
    assert_eq!(s.column_count(), 3, "schema should have 3 columns");
    assert_eq!(s.columns()[0].name, "id");
    assert_eq!(s.columns()[1].name, "name");
    assert_eq!(s.columns()[2].name, "salary");
}

#[test]
fn seq_scan_with_compound_predicate() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));
    let schema = employees_schema();

    // WHERE salary >= 45000 AND salary <= 55000
    let predicate = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Identifier("salary".into())),
            op: BinaryOp::GtEq,
            right: Box::new(Expr::Integer(45000)),
        }),
        op: BinaryOp::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Identifier("salary".into())),
            op: BinaryOp::LtEq,
            right: Box::new(Expr::Integer(55000)),
        }),
    };

    let mut scan = SeqScanExecutor::new(ctx, "employees".into(), schema, Some(predicate));
    scan.init().unwrap();

    let rows = collect_all(&mut scan);
    // Alice (50000) and Charlie (45000) are in range; Bob (60000) is out; Diana (NULL) fails
    assert_eq!(rows.len(), 2, "Alice and Charlie should be in range [45000, 55000]");
    assert_eq!(rows[0].get(1).unwrap(), &Datum::Text("Alice".into()));
    assert_eq!(rows[1].get(1).unwrap(), &Datum::Text("Charlie".into()));

    scan.close().unwrap();
}

#[test]
fn seq_scan_close_clears_state() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));
    let schema = employees_schema();

    let mut scan = SeqScanExecutor::new(ctx, "employees".into(), schema, None);
    scan.init().unwrap();

    // Consume one row
    let first = scan.next().unwrap();
    assert!(first.is_some(), "should get at least one row");

    // Close resets internal state
    scan.close().unwrap();

    // After close and re-init, we should get all rows again
    scan.init().unwrap();
    let rows = collect_all(&mut scan);
    assert_eq!(rows.len(), 4, "re-init after close should rescan all rows");

    scan.close().unwrap();
}
