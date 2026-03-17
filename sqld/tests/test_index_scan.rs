use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sqld::executor::executor::{CatalogProvider, Executor, ExecutorContext, intermediate_tuple};
use sqld::executor::index_scan::IndexScanExecutor;
use sqld::planner::physical_plan::KeyRange;
use sqld::sql::ast::{BinaryOp, Expr};
use sqld::types::{Column, DataType, Datum, MvccHeader, Schema, Tuple};
use sqld::utils::error::Result;

// ===========================================================================
// Mock catalog provider that simulates index scan behavior
// ===========================================================================

struct MockCatalog {
    tables: Mutex<HashMap<String, (Schema, Vec<Tuple>)>>,
    /// Index data: (table, index_name) -> tuples that the index would return
    index_data: Mutex<HashMap<(String, String), Vec<Tuple>>>,
}

impl MockCatalog {
    fn new() -> Self {
        Self {
            tables: Mutex::new(HashMap::new()),
            index_data: Mutex::new(HashMap::new()),
        }
    }

    fn add_table(&self, name: &str, schema: Schema, tuples: Vec<Tuple>) {
        self.tables
            .lock()
            .unwrap()
            .insert(name.to_string(), (schema, tuples));
    }

    fn add_index_data(&self, table: &str, index: &str, tuples: Vec<Tuple>) {
        self.index_data
            .lock()
            .unwrap()
            .insert((table.to_string(), index.to_string()), tuples);
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
        index: &str,
        _ranges: &[KeyRange],
    ) -> Result<Vec<Tuple>> {
        let index_data = self.index_data.lock().unwrap();
        let key = (table.to_string(), index.to_string());
        Ok(index_data.get(&key).cloned().unwrap_or_default())
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

fn products_schema() -> Schema {
    Schema::new(vec![
        Column::new("id", DataType::Integer, false),
        Column::new("name", DataType::Text, false),
        Column::new("price", DataType::Integer, true),
        Column::new("category", DataType::Text, true),
    ])
}

fn products_data() -> Vec<Tuple> {
    vec![
        make_tuple(vec![
            Datum::Integer(1),
            Datum::Text("Widget".into()),
            Datum::Integer(100),
            Datum::Text("A".into()),
        ]),
        make_tuple(vec![
            Datum::Integer(2),
            Datum::Text("Gadget".into()),
            Datum::Integer(200),
            Datum::Text("B".into()),
        ]),
        make_tuple(vec![
            Datum::Integer(3),
            Datum::Text("Doohickey".into()),
            Datum::Integer(50),
            Datum::Text("A".into()),
        ]),
        make_tuple(vec![
            Datum::Integer(4),
            Datum::Text("Thingamajig".into()),
            Datum::Integer(300),
            Datum::Text("C".into()),
        ]),
        make_tuple(vec![
            Datum::Integer(5),
            Datum::Text("Whatchamacallit".into()),
            Datum::Integer(150),
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
    let all = products_data();
    catalog.add_table("products", products_schema(), all.clone());

    // Simulate index on "category": returns only category='A' rows
    let category_a_rows: Vec<Tuple> = all
        .iter()
        .filter(|t| t.get(3) == Some(&Datum::Text("A".into())))
        .cloned()
        .collect();
    catalog.add_index_data("products", "idx_category", category_a_rows);

    // Simulate index on "id": returns rows with id in [2, 4]
    let id_range_rows: Vec<Tuple> = all
        .iter()
        .filter(|t| {
            matches!(t.get(0), Some(Datum::Integer(v)) if *v >= 2 && *v <= 4)
        })
        .cloned()
        .collect();
    catalog.add_index_data("products", "idx_id", id_range_rows);

    // Index that returns all rows
    catalog.add_index_data("products", "idx_all", all);

    catalog
}

// ===========================================================================
// Tests using IndexScanExecutor directly
// ===========================================================================

#[test]
fn index_scan_returns_matching_rows() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));
    let schema = products_schema();

    // Index scan on category index (simulated: returns category='A' rows)
    let key_ranges = vec![KeyRange::eq(Expr::String("A".into()))];

    let mut scan = IndexScanExecutor::new(
        ctx,
        "products".into(),
        "idx_category".into(),
        schema,
        key_ranges,
        None,
    );
    scan.init().unwrap();

    let rows = collect_all(&mut scan);
    assert_eq!(rows.len(), 2, "index scan should return 2 rows for category=A");

    // Verify the returned rows are Widget and Doohickey
    assert_eq!(rows[0].get(1).unwrap(), &Datum::Text("Widget".into()));
    assert_eq!(rows[1].get(1).unwrap(), &Datum::Text("Doohickey".into()));

    scan.close().unwrap();
}

#[test]
fn index_scan_with_residual_predicate() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));
    let schema = products_schema();

    // Index returns id in [2, 4], then we filter with price > 100
    let predicate = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("price".into())),
        op: BinaryOp::Gt,
        right: Box::new(Expr::Integer(100)),
    };

    let mut scan = IndexScanExecutor::new(
        ctx,
        "products".into(),
        "idx_id".into(),
        schema,
        vec![KeyRange::full()],
        Some(predicate),
    );
    scan.init().unwrap();

    let rows = collect_all(&mut scan);
    // idx_id returns rows with id 2,3,4: Gadget(200), Doohickey(50), Thingamajig(300)
    // Residual predicate price > 100 keeps: Gadget(200) and Thingamajig(300)
    assert_eq!(rows.len(), 2, "residual predicate should filter to 2 rows");

    let names: Vec<String> = rows
        .iter()
        .map(|r| match r.get(1).unwrap() {
            Datum::Text(s) => s.clone(),
            other => panic!("expected Text, got {other:?}"),
        })
        .collect();

    assert!(names.contains(&"Gadget".to_string()), "Gadget should be in results");
    assert!(
        names.contains(&"Thingamajig".to_string()),
        "Thingamajig should be in results"
    );

    scan.close().unwrap();
}

#[test]
fn index_scan_empty_result() {
    let catalog = Arc::new(MockCatalog::new());
    let schema = products_schema();
    catalog.add_table("products", schema.clone(), vec![]);
    // Index returns no rows
    catalog.add_index_data("products", "idx_empty", vec![]);
    let ctx = Arc::new(ExecutorContext::new(catalog));

    let mut scan = IndexScanExecutor::new(
        ctx,
        "products".into(),
        "idx_empty".into(),
        schema,
        vec![KeyRange::full()],
        None,
    );
    scan.init().unwrap();

    let result = scan.next().unwrap();
    assert!(result.is_none(), "index scan with no matching rows should return None");

    scan.close().unwrap();
}

#[test]
fn index_scan_schema_accessor() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));
    let schema = products_schema();

    let scan = IndexScanExecutor::new(
        ctx,
        "products".into(),
        "idx_category".into(),
        schema,
        vec![],
        None,
    );

    let s = scan.schema();
    assert_eq!(s.column_count(), 4, "products schema should have 4 columns");
    assert_eq!(s.columns()[0].name, "id");
    assert_eq!(s.columns()[1].name, "name");
    assert_eq!(s.columns()[2].name, "price");
    assert_eq!(s.columns()[3].name, "category");
}

#[test]
fn index_scan_residual_filters_all() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));
    let schema = products_schema();

    // Index returns category='A' rows (Widget price=100, Doohickey price=50)
    // Residual: price > 500 (nothing passes)
    let predicate = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("price".into())),
        op: BinaryOp::Gt,
        right: Box::new(Expr::Integer(500)),
    };

    let mut scan = IndexScanExecutor::new(
        ctx,
        "products".into(),
        "idx_category".into(),
        schema,
        vec![KeyRange::eq(Expr::String("A".into()))],
        Some(predicate),
    );
    scan.init().unwrap();

    let rows = collect_all(&mut scan);
    assert_eq!(rows.len(), 0, "no rows should pass price > 500 residual");

    scan.close().unwrap();
}

#[test]
fn index_scan_close_and_reinit() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));
    let schema = products_schema();

    let mut scan = IndexScanExecutor::new(
        ctx,
        "products".into(),
        "idx_category".into(),
        schema,
        vec![KeyRange::eq(Expr::String("A".into()))],
        None,
    );

    // First scan
    scan.init().unwrap();
    let rows1 = collect_all(&mut scan);
    assert_eq!(rows1.len(), 2, "first scan should return 2 rows");
    scan.close().unwrap();

    // Re-init and scan again
    scan.init().unwrap();
    let rows2 = collect_all(&mut scan);
    assert_eq!(rows2.len(), 2, "second scan after reinit should return same 2 rows");
    scan.close().unwrap();
}

#[test]
fn index_scan_with_null_handling_in_predicate() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));
    let schema = products_schema();

    // Residual predicate: category IS NOT NULL
    let predicate = Expr::IsNull {
        expr: Box::new(Expr::Identifier("category".into())),
        negated: true,
    };

    let mut scan = IndexScanExecutor::new(
        ctx,
        "products".into(),
        "idx_all".into(),
        schema,
        vec![KeyRange::full()],
        Some(predicate),
    );
    scan.init().unwrap();

    let rows = collect_all(&mut scan);
    // 5 total rows, 1 has NULL category (Whatchamacallit), so 4 should pass
    assert_eq!(rows.len(), 4, "IS NOT NULL predicate should filter out 1 row");

    // Verify the NULL-category row is not present
    for row in &rows {
        let cat = row.get(3).unwrap();
        assert!(
            !cat.is_null(),
            "no row should have NULL category after IS NOT NULL filter"
        );
    }

    scan.close().unwrap();
}

#[test]
fn index_scan_with_equality_residual() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));
    let schema = products_schema();

    // Use the full index, then apply name = 'Gadget' as residual
    let predicate = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("name".into())),
        op: BinaryOp::Eq,
        right: Box::new(Expr::String("Gadget".into())),
    };

    let mut scan = IndexScanExecutor::new(
        ctx,
        "products".into(),
        "idx_all".into(),
        schema,
        vec![KeyRange::full()],
        Some(predicate),
    );
    scan.init().unwrap();

    let rows = collect_all(&mut scan);
    assert_eq!(rows.len(), 1, "only Gadget should match");
    assert_eq!(rows[0].get(0).unwrap(), &Datum::Integer(2));
    assert_eq!(rows[0].get(1).unwrap(), &Datum::Text("Gadget".into()));
    assert_eq!(rows[0].get(2).unwrap(), &Datum::Integer(200));

    scan.close().unwrap();
}

#[test]
fn index_scan_with_compound_residual() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));
    let schema = products_schema();

    // Residual: price >= 100 AND price <= 200
    let predicate = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Identifier("price".into())),
            op: BinaryOp::GtEq,
            right: Box::new(Expr::Integer(100)),
        }),
        op: BinaryOp::And,
        right: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Identifier("price".into())),
            op: BinaryOp::LtEq,
            right: Box::new(Expr::Integer(200)),
        }),
    };

    let mut scan = IndexScanExecutor::new(
        ctx,
        "products".into(),
        "idx_all".into(),
        schema,
        vec![KeyRange::full()],
        Some(predicate),
    );
    scan.init().unwrap();

    let rows = collect_all(&mut scan);
    // Widget(100), Gadget(200), Whatchamacallit(150) should match
    assert_eq!(rows.len(), 3, "3 products have price in [100, 200]");

    let prices: Vec<&Datum> = rows.iter().map(|r| r.get(2).unwrap()).collect();
    assert!(prices.contains(&&Datum::Integer(100)), "Widget at 100");
    assert!(prices.contains(&&Datum::Integer(200)), "Gadget at 200");
    assert!(prices.contains(&&Datum::Integer(150)), "Whatchamacallit at 150");

    scan.close().unwrap();
}
