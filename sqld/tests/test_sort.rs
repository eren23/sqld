use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sqld::executor::executor::{build_executor, CatalogProvider, ExecutorContext};
use sqld::planner::logical_plan::SortExpr;
use sqld::planner::physical_plan::{KeyRange, PhysicalPlan};
use sqld::sql::ast::Expr;
use sqld::types::{Column, DataType, Datum, MvccHeader, Schema, Tuple};
use sqld::utils::error::Result;

// ===========================================================================
// Mock catalog provider for tests
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

    fn insert_tuple(&self, table: &str, values: Vec<Datum>) -> Result<Tuple> {
        let mut tables = self.tables.lock().unwrap();
        let (_, ref mut tuples) = tables.get_mut(table).unwrap();
        let tuple = Tuple::new(MvccHeader::new_insert(1, 0), values);
        tuples.push(tuple.clone());
        Ok(tuple)
    }

    fn delete_tuple(&self, table: &str, tuple: &Tuple) -> Result<Tuple> {
        let mut tables = self.tables.lock().unwrap();
        let (_, ref mut tuples) = tables.get_mut(table).unwrap();
        tuples.retain(|t| t != tuple);
        Ok(tuple.clone())
    }

    fn update_tuple(
        &self,
        table: &str,
        old_tuple: &Tuple,
        new_values: Vec<Datum>,
    ) -> Result<Tuple> {
        let mut tables = self.tables.lock().unwrap();
        let (_, ref mut tuples) = tables.get_mut(table).unwrap();
        tuples.retain(|t| t != old_tuple);
        let new_tuple = Tuple::new(MvccHeader::new_insert(2, 0), new_values);
        tuples.push(new_tuple.clone());
        Ok(new_tuple)
    }
}

// ===========================================================================
// Test helpers
// ===========================================================================

fn make_tuple(values: Vec<Datum>) -> Tuple {
    Tuple::new(MvccHeader::new(0, 0, 0), values)
}

fn numbers_schema() -> Schema {
    Schema::new(vec![
        Column::new("id", DataType::Integer, false),
        Column::new("value", DataType::Integer, true),
        Column::new("name", DataType::Text, false),
    ])
}

fn numbers_data() -> Vec<Tuple> {
    vec![
        make_tuple(vec![
            Datum::Integer(3),
            Datum::Integer(30),
            Datum::Text("c".into()),
        ]),
        make_tuple(vec![
            Datum::Integer(1),
            Datum::Integer(10),
            Datum::Text("a".into()),
        ]),
        make_tuple(vec![
            Datum::Integer(2),
            Datum::Integer(20),
            Datum::Text("b".into()),
        ]),
        make_tuple(vec![
            Datum::Integer(4),
            Datum::Null,
            Datum::Text("d".into()),
        ]),
        make_tuple(vec![
            Datum::Integer(5),
            Datum::Integer(10),
            Datum::Text("e".into()),
        ]),
    ]
}

fn setup_catalog() -> Arc<MockCatalog> {
    let catalog = Arc::new(MockCatalog::new());
    catalog.add_table("numbers", numbers_schema(), numbers_data());
    catalog
}

fn seq_scan_plan() -> PhysicalPlan {
    PhysicalPlan::SeqScan {
        table: "numbers".into(),
        alias: None,
        schema: numbers_schema(),
        predicate: None,
    }
}

/// Execute a sort plan and collect all output rows.
fn execute_sort(plan: PhysicalPlan, ctx: Arc<ExecutorContext>) -> Vec<Tuple> {
    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();

    let mut rows = Vec::new();
    while let Some(tuple) = exec.next().unwrap() {
        rows.push(tuple);
    }
    exec.close().unwrap();
    rows
}

/// Extract the integer id (column 0) from each row for easy assertion.
fn ids(rows: &[Tuple]) -> Vec<Option<i32>> {
    rows.iter()
        .map(|t| match t.get(0).unwrap() {
            Datum::Integer(v) => Some(*v),
            Datum::Null => None,
            _ => panic!("unexpected datum type in id column"),
        })
        .collect()
}

/// Extract the integer value (column 1) from each row.
fn values(rows: &[Tuple]) -> Vec<Option<i32>> {
    rows.iter()
        .map(|t| match t.get(1).unwrap() {
            Datum::Integer(v) => Some(*v),
            Datum::Null => None,
            _ => panic!("unexpected datum type in value column"),
        })
        .collect()
}

// ===========================================================================
// Tests
// ===========================================================================

#[test]
fn sort_ascending() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));

    let plan = PhysicalPlan::ExternalSort {
        order_by: vec![SortExpr {
            expr: Expr::Identifier("value".into()),
            ascending: true,
            nulls_first: false,
        }],
        input: Box::new(seq_scan_plan()),
    };

    let rows = execute_sort(plan, ctx);

    assert_eq!(rows.len(), 5);
    // ASC with NULLS LAST: 10, 10, 20, 30, NULL
    let vals = values(&rows);
    assert_eq!(
        vals,
        vec![Some(10), Some(10), Some(20), Some(30), None]
    );
    // NULL row (id=4) should be last
    let last_id = match rows.last().unwrap().get(0).unwrap() {
        Datum::Integer(v) => *v,
        _ => panic!("expected integer"),
    };
    assert_eq!(last_id, 4);
}

#[test]
fn sort_descending() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));

    let plan = PhysicalPlan::ExternalSort {
        order_by: vec![SortExpr {
            expr: Expr::Identifier("value".into()),
            ascending: false,
            nulls_first: false,
        }],
        input: Box::new(seq_scan_plan()),
    };

    let rows = execute_sort(plan, ctx);

    assert_eq!(rows.len(), 5);
    // DESC with NULLS LAST: 30, 20, 10, 10, NULL
    let vals = values(&rows);
    assert_eq!(
        vals,
        vec![Some(30), Some(20), Some(10), Some(10), None]
    );
    // NULL row (id=4) should be last
    let last_id = match rows.last().unwrap().get(0).unwrap() {
        Datum::Integer(v) => *v,
        _ => panic!("expected integer"),
    };
    assert_eq!(last_id, 4);
}

#[test]
fn sort_nulls_first() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));

    let plan = PhysicalPlan::ExternalSort {
        order_by: vec![SortExpr {
            expr: Expr::Identifier("value".into()),
            ascending: true,
            nulls_first: true,
        }],
        input: Box::new(seq_scan_plan()),
    };

    let rows = execute_sort(plan, ctx);

    assert_eq!(rows.len(), 5);
    // NULLS FIRST, ASC: NULL, 10, 10, 20, 30
    let vals = values(&rows);
    assert_eq!(
        vals,
        vec![None, Some(10), Some(10), Some(20), Some(30)]
    );
    // First row should be the NULL row (id=4)
    let first_id = match rows.first().unwrap().get(0).unwrap() {
        Datum::Integer(v) => *v,
        _ => panic!("expected integer"),
    };
    assert_eq!(first_id, 4);
}

#[test]
fn sort_nulls_last() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));

    let plan = PhysicalPlan::ExternalSort {
        order_by: vec![SortExpr {
            expr: Expr::Identifier("value".into()),
            ascending: true,
            nulls_first: false,
        }],
        input: Box::new(seq_scan_plan()),
    };

    let rows = execute_sort(plan, ctx);

    assert_eq!(rows.len(), 5);
    // NULLS LAST, ASC: 10, 10, 20, 30, NULL
    let vals = values(&rows);
    assert_eq!(
        vals,
        vec![Some(10), Some(10), Some(20), Some(30), None]
    );
    // Last row should be the NULL row (id=4)
    let last_id = match rows.last().unwrap().get(0).unwrap() {
        Datum::Integer(v) => *v,
        _ => panic!("expected integer"),
    };
    assert_eq!(last_id, 4);
}

#[test]
fn sort_multi_key() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));

    // Sort by value ASC, then by id DESC
    let plan = PhysicalPlan::ExternalSort {
        order_by: vec![
            SortExpr {
                expr: Expr::Identifier("value".into()),
                ascending: true,
                nulls_first: false,
            },
            SortExpr {
                expr: Expr::Identifier("id".into()),
                ascending: false,
                nulls_first: false,
            },
        ],
        input: Box::new(seq_scan_plan()),
    };

    let rows = execute_sort(plan, ctx);

    assert_eq!(rows.len(), 5);
    // value ASC, id DESC:
    //   value=10, id=5  (id DESC among the two value=10 rows)
    //   value=10, id=1
    //   value=20, id=2
    //   value=30, id=3
    //   value=NULL, id=4
    let row_ids = ids(&rows);
    assert_eq!(row_ids, vec![Some(5), Some(1), Some(2), Some(3), Some(4)]);
}

#[test]
fn sort_already_sorted() {
    // Data that is already sorted: verify sort preserves order (stability).
    let catalog = Arc::new(MockCatalog::new());
    let schema = numbers_schema();
    let sorted_data = vec![
        make_tuple(vec![
            Datum::Integer(1),
            Datum::Integer(10),
            Datum::Text("a".into()),
        ]),
        make_tuple(vec![
            Datum::Integer(2),
            Datum::Integer(20),
            Datum::Text("b".into()),
        ]),
        make_tuple(vec![
            Datum::Integer(3),
            Datum::Integer(30),
            Datum::Text("c".into()),
        ]),
    ];
    catalog.add_table("numbers", schema.clone(), sorted_data);

    let ctx = Arc::new(ExecutorContext::new(catalog));

    let plan = PhysicalPlan::ExternalSort {
        order_by: vec![SortExpr {
            expr: Expr::Identifier("value".into()),
            ascending: true,
            nulls_first: false,
        }],
        input: Box::new(PhysicalPlan::SeqScan {
            table: "numbers".into(),
            alias: None,
            schema: numbers_schema(),
            predicate: None,
        }),
    };

    let rows = execute_sort(plan, ctx);

    assert_eq!(rows.len(), 3);
    let row_ids = ids(&rows);
    assert_eq!(row_ids, vec![Some(1), Some(2), Some(3)]);
    let vals = values(&rows);
    assert_eq!(vals, vec![Some(10), Some(20), Some(30)]);
}

#[test]
fn sort_single_row() {
    let catalog = Arc::new(MockCatalog::new());
    let schema = numbers_schema();
    let single_row = vec![make_tuple(vec![
        Datum::Integer(42),
        Datum::Integer(100),
        Datum::Text("only".into()),
    ])];
    catalog.add_table("numbers", schema.clone(), single_row);

    let ctx = Arc::new(ExecutorContext::new(catalog));

    let plan = PhysicalPlan::ExternalSort {
        order_by: vec![SortExpr {
            expr: Expr::Identifier("value".into()),
            ascending: true,
            nulls_first: false,
        }],
        input: Box::new(PhysicalPlan::SeqScan {
            table: "numbers".into(),
            alias: None,
            schema: numbers_schema(),
            predicate: None,
        }),
    };

    let rows = execute_sort(plan, ctx);

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0).unwrap(), &Datum::Integer(42));
    assert_eq!(rows[0].get(1).unwrap(), &Datum::Integer(100));
    assert_eq!(rows[0].get(2).unwrap(), &Datum::Text("only".into()));
}

#[test]
fn sort_empty() {
    let catalog = Arc::new(MockCatalog::new());
    let schema = numbers_schema();
    catalog.add_table("numbers", schema.clone(), vec![]);

    let ctx = Arc::new(ExecutorContext::new(catalog));

    let plan = PhysicalPlan::ExternalSort {
        order_by: vec![SortExpr {
            expr: Expr::Identifier("value".into()),
            ascending: true,
            nulls_first: false,
        }],
        input: Box::new(PhysicalPlan::SeqScan {
            table: "numbers".into(),
            alias: None,
            schema: numbers_schema(),
            predicate: None,
        }),
    };

    let rows = execute_sort(plan, ctx);

    assert_eq!(rows.len(), 0);
}

#[test]
fn sort_external_merge_sort() {
    // Use a very small work_mem to force the sort executor to spill into
    // multiple sorted runs and then merge them.
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog).with_work_mem(1));

    let plan = PhysicalPlan::ExternalSort {
        order_by: vec![SortExpr {
            expr: Expr::Identifier("value".into()),
            ascending: true,
            nulls_first: false,
        }],
        input: Box::new(seq_scan_plan()),
    };

    let rows = execute_sort(plan, ctx);

    // Even with external merge sort the result must be correctly sorted.
    assert_eq!(rows.len(), 5);
    let vals = values(&rows);
    assert_eq!(
        vals,
        vec![Some(10), Some(10), Some(20), Some(30), None]
    );
    // NULL row (id=4) should be last
    let last_id = match rows.last().unwrap().get(0).unwrap() {
        Datum::Integer(v) => *v,
        _ => panic!("expected integer"),
    };
    assert_eq!(last_id, 4);
}
