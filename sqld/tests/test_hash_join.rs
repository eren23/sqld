use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sqld::executor::executor::{build_executor, CatalogProvider, Executor, ExecutorContext};
use sqld::planner::physical_plan::{KeyRange, PhysicalPlan};
use sqld::sql::ast::{Expr, JoinType};
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
// Helper functions
// ===========================================================================

fn make_tuple(values: Vec<Datum>) -> Tuple {
    Tuple::new(MvccHeader::new(0, 0, 0), values)
}

fn employees_schema() -> Schema {
    Schema::new(vec![
        Column::new("id", DataType::Integer, false),
        Column::new("name", DataType::Text, false),
        Column::new("dept_id", DataType::Integer, true),
    ])
}

fn departments_schema() -> Schema {
    Schema::new(vec![
        Column::new("id", DataType::Integer, false),
        Column::new("dept_name", DataType::Text, false),
    ])
}

fn employees_data() -> Vec<Tuple> {
    vec![
        make_tuple(vec![
            Datum::Integer(1),
            Datum::Text("Alice".into()),
            Datum::Integer(10),
        ]),
        make_tuple(vec![
            Datum::Integer(2),
            Datum::Text("Bob".into()),
            Datum::Integer(20),
        ]),
        make_tuple(vec![
            Datum::Integer(3),
            Datum::Text("Charlie".into()),
            Datum::Integer(10),
        ]),
        make_tuple(vec![
            Datum::Integer(4),
            Datum::Text("Diana".into()),
            Datum::Null,
        ]),
    ]
}

fn departments_data() -> Vec<Tuple> {
    vec![
        make_tuple(vec![
            Datum::Integer(10),
            Datum::Text("Engineering".into()),
        ]),
        make_tuple(vec![
            Datum::Integer(20),
            Datum::Text("Sales".into()),
        ]),
        make_tuple(vec![
            Datum::Integer(30),
            Datum::Text("HR".into()),
        ]),
    ]
}

fn setup_catalog() -> Arc<MockCatalog> {
    let catalog = Arc::new(MockCatalog::new());
    catalog.add_table("employees", employees_schema(), employees_data());
    catalog.add_table("departments", departments_schema(), departments_data());
    catalog
}

fn merged_schema() -> Schema {
    employees_schema().merge(&departments_schema())
}

fn employees_seq_scan() -> PhysicalPlan {
    PhysicalPlan::SeqScan {
        table: "employees".into(),
        alias: None,
        schema: employees_schema(),
        predicate: None,
    }
}

fn departments_seq_scan() -> PhysicalPlan {
    PhysicalPlan::SeqScan {
        table: "departments".into(),
        alias: None,
        schema: departments_schema(),
        predicate: None,
    }
}

fn collect_rows(exec: &mut Box<dyn Executor>) -> Vec<Tuple> {
    let mut rows = Vec::new();
    while let Some(tuple) = exec.next().unwrap() {
        rows.push(tuple);
    }
    rows
}

// ===========================================================================
// HashJoin tests
// ===========================================================================

/// INNER JOIN employees.dept_id = departments.id
/// Expected: Alice->Engineering, Bob->Sales, Charlie->Engineering (3 rows).
/// Diana is excluded because her dept_id is NULL.
#[test]
fn hash_join_inner() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));

    let plan = PhysicalPlan::HashJoin {
        join_type: JoinType::Inner,
        left_keys: vec![Expr::Identifier("dept_id".into())],
        right_keys: vec![Expr::Identifier("id".into())],
        condition: None,
        left: Box::new(employees_seq_scan()),
        right: Box::new(departments_seq_scan()),
        schema: merged_schema(),
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();
    let rows = collect_rows(&mut exec);
    exec.close().unwrap();

    // Alice (dept_id=10) -> Engineering
    // Bob (dept_id=20) -> Sales
    // Charlie (dept_id=10) -> Engineering
    // Diana (dept_id=NULL) -> excluded
    assert_eq!(rows.len(), 3);

    // Merged schema columns:
    //   0: id, 1: name, 2: dept_id, 3: _right_id, 4: dept_name
    let mut pairs: Vec<(String, String)> = rows
        .iter()
        .map(|r| {
            let name = match r.get(1).unwrap() {
                Datum::Text(s) => s.clone(),
                other => panic!("expected Text for name, got {:?}", other),
            };
            let dept_name = match r.get(4).unwrap() {
                Datum::Text(s) => s.clone(),
                other => panic!("expected Text for dept_name, got {:?}", other),
            };
            (name, dept_name)
        })
        .collect();
    pairs.sort();

    assert_eq!(
        pairs,
        vec![
            ("Alice".to_string(), "Engineering".to_string()),
            ("Bob".to_string(), "Sales".to_string()),
            ("Charlie".to_string(), "Engineering".to_string()),
        ]
    );
}

/// LEFT JOIN employees.dept_id = departments.id
/// Expected: all 4 employees appear. Diana has NULLs for department columns.
#[test]
fn hash_join_left() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));

    let plan = PhysicalPlan::HashJoin {
        join_type: JoinType::Left,
        left_keys: vec![Expr::Identifier("dept_id".into())],
        right_keys: vec![Expr::Identifier("id".into())],
        condition: None,
        left: Box::new(employees_seq_scan()),
        right: Box::new(departments_seq_scan()),
        schema: merged_schema(),
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();
    let rows = collect_rows(&mut exec);
    exec.close().unwrap();

    // All 4 employees: 3 matched + Diana unmatched
    assert_eq!(rows.len(), 4);

    // Find Diana's row -- she should have NULLs for department columns
    let diana_row = rows
        .iter()
        .find(|r| r.get(1).unwrap() == &Datum::Text("Diana".into()))
        .expect("Diana should appear in LEFT JOIN results");

    // Columns: 0=id, 1=name, 2=dept_id, 3=_right_id, 4=dept_name
    assert_eq!(diana_row.get(2).unwrap(), &Datum::Null); // dept_id was NULL
    assert_eq!(diana_row.get(3).unwrap(), &Datum::Null); // _right_id
    assert_eq!(diana_row.get(4).unwrap(), &Datum::Null); // dept_name

    // Verify matched employees still have correct department data
    let alice_row = rows
        .iter()
        .find(|r| r.get(1).unwrap() == &Datum::Text("Alice".into()))
        .expect("Alice should appear");
    assert_eq!(
        alice_row.get(4).unwrap(),
        &Datum::Text("Engineering".into())
    );
}

/// RIGHT JOIN employees.dept_id = departments.id
/// Expected: 3 matched rows + HR (dept 30, unmatched) with NULLs for employee.
#[test]
fn hash_join_right() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));

    let plan = PhysicalPlan::HashJoin {
        join_type: JoinType::Right,
        left_keys: vec![Expr::Identifier("dept_id".into())],
        right_keys: vec![Expr::Identifier("id".into())],
        condition: None,
        left: Box::new(employees_seq_scan()),
        right: Box::new(departments_seq_scan()),
        schema: merged_schema(),
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();
    let rows = collect_rows(&mut exec);
    exec.close().unwrap();

    // 3 matched rows + HR (dept 30) unmatched = 4
    assert_eq!(rows.len(), 4);

    // Find the HR row -- should have NULLs for employee columns
    let hr_row = rows
        .iter()
        .find(|r| r.get(4).unwrap() == &Datum::Text("HR".into()))
        .expect("HR should appear in RIGHT JOIN results");

    // Employee columns should be NULL
    assert_eq!(hr_row.get(0).unwrap(), &Datum::Null); // id
    assert_eq!(hr_row.get(1).unwrap(), &Datum::Null); // name
    assert_eq!(hr_row.get(2).unwrap(), &Datum::Null); // dept_id
    // Department id should be present
    assert_eq!(hr_row.get(3).unwrap(), &Datum::Integer(30));
}

/// FULL JOIN employees.dept_id = departments.id
/// Expected: 3 matched + Diana (unmatched left) + HR (unmatched right) = 5 rows.
#[test]
fn hash_join_full() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));

    let plan = PhysicalPlan::HashJoin {
        join_type: JoinType::Full,
        left_keys: vec![Expr::Identifier("dept_id".into())],
        right_keys: vec![Expr::Identifier("id".into())],
        condition: None,
        left: Box::new(employees_seq_scan()),
        right: Box::new(departments_seq_scan()),
        schema: merged_schema(),
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();
    let rows = collect_rows(&mut exec);
    exec.close().unwrap();

    // 3 matched + Diana (unmatched left) + HR (unmatched right) = 5
    assert_eq!(rows.len(), 5);

    // Verify Diana appears with NULL department columns
    let diana_row = rows
        .iter()
        .find(|r| r.get(1).unwrap() == &Datum::Text("Diana".into()))
        .expect("Diana should appear in FULL JOIN results");
    assert_eq!(diana_row.get(3).unwrap(), &Datum::Null); // _right_id
    assert_eq!(diana_row.get(4).unwrap(), &Datum::Null); // dept_name

    // Verify HR appears with NULL employee columns
    let hr_row = rows
        .iter()
        .find(|r| r.get(4).unwrap() == &Datum::Text("HR".into()))
        .expect("HR should appear in FULL JOIN results");
    assert_eq!(hr_row.get(0).unwrap(), &Datum::Null); // id
    assert_eq!(hr_row.get(1).unwrap(), &Datum::Null); // name
    assert_eq!(hr_row.get(2).unwrap(), &Datum::Null); // dept_id

    // Verify matched rows are correct
    let mut matched_pairs: Vec<(String, String)> = rows
        .iter()
        .filter(|r| {
            !r.get(1).unwrap().is_null() && !r.get(4).unwrap().is_null()
        })
        .map(|r| {
            let name = match r.get(1).unwrap() {
                Datum::Text(s) => s.clone(),
                other => panic!("expected Text, got {:?}", other),
            };
            let dept = match r.get(4).unwrap() {
                Datum::Text(s) => s.clone(),
                other => panic!("expected Text, got {:?}", other),
            };
            (name, dept)
        })
        .collect();
    matched_pairs.sort();
    assert_eq!(
        matched_pairs,
        vec![
            ("Alice".to_string(), "Engineering".to_string()),
            ("Bob".to_string(), "Sales".to_string()),
            ("Charlie".to_string(), "Engineering".to_string()),
        ]
    );
}

/// CROSS JOIN via NestedLoopJoin (no condition) -- cartesian product.
/// Expected: 4 employees x 3 departments = 12 rows.
#[test]
fn hash_join_cross() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));

    let plan = PhysicalPlan::NestedLoopJoin {
        join_type: JoinType::Cross,
        condition: None,
        left: Box::new(employees_seq_scan()),
        right: Box::new(departments_seq_scan()),
        schema: merged_schema(),
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();
    let rows = collect_rows(&mut exec);
    exec.close().unwrap();

    // 4 employees x 3 departments = 12
    assert_eq!(rows.len(), 12);

    // Every employee should appear exactly 3 times
    let alice_count = rows
        .iter()
        .filter(|r| r.get(1).unwrap() == &Datum::Text("Alice".into()))
        .count();
    assert_eq!(alice_count, 3);

    let diana_count = rows
        .iter()
        .filter(|r| r.get(1).unwrap() == &Datum::Text("Diana".into()))
        .count();
    assert_eq!(diana_count, 3);

    // Every department should appear exactly 4 times
    let eng_count = rows
        .iter()
        .filter(|r| r.get(4).unwrap() == &Datum::Text("Engineering".into()))
        .count();
    assert_eq!(eng_count, 4);

    let hr_count = rows
        .iter()
        .filter(|r| r.get(4).unwrap() == &Datum::Text("HR".into()))
        .count();
    assert_eq!(hr_count, 4);
}

/// NULL join keys must never match, even when both sides have NULLs.
#[test]
fn hash_join_null_keys() {
    let catalog = Arc::new(MockCatalog::new());

    let left_schema = Schema::new(vec![
        Column::new("a", DataType::Integer, true),
        Column::new("val", DataType::Text, false),
    ]);
    let right_schema = Schema::new(vec![
        Column::new("b", DataType::Integer, true),
        Column::new("info", DataType::Text, false),
    ]);

    // Both sides have rows with NULL keys
    catalog.add_table(
        "left_t",
        left_schema.clone(),
        vec![
            make_tuple(vec![Datum::Null, Datum::Text("x".into())]),
            make_tuple(vec![Datum::Integer(1), Datum::Text("y".into())]),
        ],
    );
    catalog.add_table(
        "right_t",
        right_schema.clone(),
        vec![
            make_tuple(vec![Datum::Null, Datum::Text("p".into())]),
            make_tuple(vec![Datum::Integer(1), Datum::Text("q".into())]),
        ],
    );

    let ctx = Arc::new(ExecutorContext::new(catalog));
    let join_schema = left_schema.merge(&right_schema);

    let plan = PhysicalPlan::HashJoin {
        join_type: JoinType::Inner,
        left_keys: vec![Expr::Identifier("a".into())],
        right_keys: vec![Expr::Identifier("b".into())],
        condition: None,
        left: Box::new(PhysicalPlan::SeqScan {
            table: "left_t".into(),
            alias: None,
            schema: left_schema,
            predicate: None,
        }),
        right: Box::new(PhysicalPlan::SeqScan {
            table: "right_t".into(),
            alias: None,
            schema: right_schema,
            predicate: None,
        }),
        schema: join_schema,
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();
    let rows = collect_rows(&mut exec);
    exec.close().unwrap();

    // Only (1, "y") matches (1, "q"); NULL keys must NOT match each other
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(1).unwrap(), &Datum::Text("y".into()));
    assert_eq!(rows[0].get(3).unwrap(), &Datum::Text("q".into()));
}

/// INNER JOIN with empty left side produces zero rows.
#[test]
fn hash_join_empty_left() {
    let catalog = Arc::new(MockCatalog::new());
    catalog.add_table("empty_emp", employees_schema(), vec![]);
    catalog.add_table("departments", departments_schema(), departments_data());

    let ctx = Arc::new(ExecutorContext::new(catalog));

    let plan = PhysicalPlan::HashJoin {
        join_type: JoinType::Inner,
        left_keys: vec![Expr::Identifier("dept_id".into())],
        right_keys: vec![Expr::Identifier("id".into())],
        condition: None,
        left: Box::new(PhysicalPlan::SeqScan {
            table: "empty_emp".into(),
            alias: None,
            schema: employees_schema(),
            predicate: None,
        }),
        right: Box::new(departments_seq_scan()),
        schema: merged_schema(),
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();
    let rows = collect_rows(&mut exec);
    exec.close().unwrap();

    assert_eq!(rows.len(), 0);
}

/// INNER JOIN with empty right side produces zero rows.
#[test]
fn hash_join_empty_right() {
    let catalog = Arc::new(MockCatalog::new());
    catalog.add_table("employees", employees_schema(), employees_data());
    catalog.add_table("empty_dept", departments_schema(), vec![]);

    let ctx = Arc::new(ExecutorContext::new(catalog));

    let plan = PhysicalPlan::HashJoin {
        join_type: JoinType::Inner,
        left_keys: vec![Expr::Identifier("dept_id".into())],
        right_keys: vec![Expr::Identifier("id".into())],
        condition: None,
        left: Box::new(employees_seq_scan()),
        right: Box::new(PhysicalPlan::SeqScan {
            table: "empty_dept".into(),
            alias: None,
            schema: departments_schema(),
            predicate: None,
        }),
        schema: merged_schema(),
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();
    let rows = collect_rows(&mut exec);
    exec.close().unwrap();

    assert_eq!(rows.len(), 0);
}

/// INNER JOIN with duplicate keys: multiple rows per key produce the
/// full cartesian product for that key group.
#[test]
fn hash_join_duplicate_keys() {
    let catalog = Arc::new(MockCatalog::new());

    let left_schema = Schema::new(vec![
        Column::new("key", DataType::Integer, false),
        Column::new("lval", DataType::Text, false),
    ]);
    let right_schema = Schema::new(vec![
        Column::new("key", DataType::Integer, false),
        Column::new("rval", DataType::Text, false),
    ]);

    // 3 left rows with key=1, 2 right rows with key=1
    catalog.add_table(
        "left_dup",
        left_schema.clone(),
        vec![
            make_tuple(vec![Datum::Integer(1), Datum::Text("L1".into())]),
            make_tuple(vec![Datum::Integer(1), Datum::Text("L2".into())]),
            make_tuple(vec![Datum::Integer(1), Datum::Text("L3".into())]),
        ],
    );
    catalog.add_table(
        "right_dup",
        right_schema.clone(),
        vec![
            make_tuple(vec![Datum::Integer(1), Datum::Text("R1".into())]),
            make_tuple(vec![Datum::Integer(1), Datum::Text("R2".into())]),
        ],
    );

    let ctx = Arc::new(ExecutorContext::new(catalog));
    let join_schema = left_schema.merge(&right_schema);

    let plan = PhysicalPlan::HashJoin {
        join_type: JoinType::Inner,
        left_keys: vec![Expr::Identifier("key".into())],
        right_keys: vec![Expr::Identifier("key".into())],
        condition: None,
        left: Box::new(PhysicalPlan::SeqScan {
            table: "left_dup".into(),
            alias: None,
            schema: left_schema,
            predicate: None,
        }),
        right: Box::new(PhysicalPlan::SeqScan {
            table: "right_dup".into(),
            alias: None,
            schema: right_schema,
            predicate: None,
        }),
        schema: join_schema,
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();
    let rows = collect_rows(&mut exec);
    exec.close().unwrap();

    // 3 left x 2 right = 6 combinations
    assert_eq!(rows.len(), 6);

    // Merged schema: key, lval, _right_key, rval
    let mut pairs: Vec<(String, String)> = rows
        .iter()
        .map(|r| {
            let lval = match r.get(1).unwrap() {
                Datum::Text(s) => s.clone(),
                other => panic!("expected Text for lval, got {:?}", other),
            };
            let rval = match r.get(3).unwrap() {
                Datum::Text(s) => s.clone(),
                other => panic!("expected Text for rval, got {:?}", other),
            };
            (lval, rval)
        })
        .collect();
    pairs.sort();

    assert_eq!(
        pairs,
        vec![
            ("L1".to_string(), "R1".to_string()),
            ("L1".to_string(), "R2".to_string()),
            ("L2".to_string(), "R1".to_string()),
            ("L2".to_string(), "R2".to_string()),
            ("L3".to_string(), "R1".to_string()),
            ("L3".to_string(), "R2".to_string()),
        ]
    );
}

/// LEFT SEMI JOIN employees.dept_id = departments.id
/// Only left-side columns are emitted for rows that have at least one match.
/// Expected: Alice (dept_id=10), Bob (dept_id=20), Charlie (dept_id=10) -- 3 rows.
/// Diana (dept_id=NULL) is excluded because she has no match.
/// Each matching left row appears exactly once, even if multiple right rows match.
#[test]
fn hash_join_left_semi() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));

    let plan = PhysicalPlan::HashJoin {
        join_type: JoinType::LeftSemi,
        left_keys: vec![Expr::Identifier("dept_id".into())],
        right_keys: vec![Expr::Identifier("id".into())],
        condition: None,
        left: Box::new(employees_seq_scan()),
        right: Box::new(departments_seq_scan()),
        schema: employees_schema(),
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();
    let rows = collect_rows(&mut exec);
    exec.close().unwrap();

    // Alice (dept_id=10), Bob (dept_id=20), Charlie (dept_id=10) match.
    // Diana (dept_id=NULL) does not match.
    assert_eq!(rows.len(), 3);

    // Output schema is only the left side: 0=id, 1=name, 2=dept_id
    let mut names: Vec<String> = rows
        .iter()
        .map(|r| match r.get(1).unwrap() {
            Datum::Text(s) => s.clone(),
            other => panic!("expected Text for name, got {:?}", other),
        })
        .collect();
    names.sort();

    assert_eq!(
        names,
        vec![
            "Alice".to_string(),
            "Bob".to_string(),
            "Charlie".to_string(),
        ]
    );

    // Verify no right-side columns are present (only 3 columns per row)
    for row in &rows {
        assert!(
            row.get(3).is_none(),
            "SEMI join should not emit right-side columns"
        );
    }
}

/// LEFT ANTI JOIN employees.dept_id = departments.id
/// Only left-side columns are emitted for rows that have NO match on the right.
/// Expected: Diana (dept_id=NULL) -- 1 row.
#[test]
fn hash_join_left_anti() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog));

    let plan = PhysicalPlan::HashJoin {
        join_type: JoinType::LeftAnti,
        left_keys: vec![Expr::Identifier("dept_id".into())],
        right_keys: vec![Expr::Identifier("id".into())],
        condition: None,
        left: Box::new(employees_seq_scan()),
        right: Box::new(departments_seq_scan()),
        schema: employees_schema(),
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();
    let rows = collect_rows(&mut exec);
    exec.close().unwrap();

    // Only Diana (dept_id=NULL) has no matching department
    assert_eq!(rows.len(), 1);

    // Output schema is only the left side: 0=id, 1=name, 2=dept_id
    let name = match rows[0].get(1).unwrap() {
        Datum::Text(s) => s.clone(),
        other => panic!("expected Text for name, got {:?}", other),
    };
    assert_eq!(name, "Diana");
    assert_eq!(rows[0].get(2).unwrap(), &Datum::Null); // dept_id is NULL

    // Verify no right-side columns are present
    assert!(
        rows[0].get(3).is_none(),
        "ANTI join should not emit right-side columns"
    );
}

/// LEFT SEMI JOIN with duplicate keys: each matching left row should appear
/// exactly once, regardless of how many right-side matches exist.
#[test]
fn hash_join_left_semi_duplicate_keys() {
    let catalog = Arc::new(MockCatalog::new());

    let left_schema = Schema::new(vec![
        Column::new("key", DataType::Integer, false),
        Column::new("lval", DataType::Text, false),
    ]);
    let right_schema = Schema::new(vec![
        Column::new("key", DataType::Integer, false),
        Column::new("rval", DataType::Text, false),
    ]);

    // 2 left rows with key=1, 3 right rows with key=1
    catalog.add_table(
        "left_semi",
        left_schema.clone(),
        vec![
            make_tuple(vec![Datum::Integer(1), Datum::Text("L1".into())]),
            make_tuple(vec![Datum::Integer(1), Datum::Text("L2".into())]),
            make_tuple(vec![Datum::Integer(2), Datum::Text("L3".into())]),
        ],
    );
    catalog.add_table(
        "right_semi",
        right_schema.clone(),
        vec![
            make_tuple(vec![Datum::Integer(1), Datum::Text("R1".into())]),
            make_tuple(vec![Datum::Integer(1), Datum::Text("R2".into())]),
            make_tuple(vec![Datum::Integer(1), Datum::Text("R3".into())]),
        ],
    );

    let ctx = Arc::new(ExecutorContext::new(catalog));

    let plan = PhysicalPlan::HashJoin {
        join_type: JoinType::LeftSemi,
        left_keys: vec![Expr::Identifier("key".into())],
        right_keys: vec![Expr::Identifier("key".into())],
        condition: None,
        left: Box::new(PhysicalPlan::SeqScan {
            table: "left_semi".into(),
            alias: None,
            schema: left_schema.clone(),
            predicate: None,
        }),
        right: Box::new(PhysicalPlan::SeqScan {
            table: "right_semi".into(),
            alias: None,
            schema: right_schema,
            predicate: None,
        }),
        schema: left_schema,
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();
    let rows = collect_rows(&mut exec);
    exec.close().unwrap();

    // L1 and L2 match (key=1); L3 does not match (key=2, no right match).
    // Each should appear exactly once, NOT multiplied by right-side matches.
    assert_eq!(rows.len(), 2);

    let mut vals: Vec<String> = rows
        .iter()
        .map(|r| match r.get(1).unwrap() {
            Datum::Text(s) => s.clone(),
            other => panic!("expected Text, got {:?}", other),
        })
        .collect();
    vals.sort();

    assert_eq!(vals, vec!["L1".to_string(), "L2".to_string()]);
}

/// INNER JOIN with very small work_mem to force disk spill.
/// The result should be identical to the normal INNER JOIN test despite spilling.
#[test]
fn hash_join_disk_spill() {
    let catalog = setup_catalog();
    // 128 bytes is far too small to hold the hash table in memory,
    // forcing the executor to spill partitions to disk.
    let ctx = Arc::new(ExecutorContext::new(catalog).with_work_mem(128));

    let plan = PhysicalPlan::HashJoin {
        join_type: JoinType::Inner,
        left_keys: vec![Expr::Identifier("dept_id".into())],
        right_keys: vec![Expr::Identifier("id".into())],
        condition: None,
        left: Box::new(employees_seq_scan()),
        right: Box::new(departments_seq_scan()),
        schema: merged_schema(),
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();
    let rows = collect_rows(&mut exec);
    exec.close().unwrap();

    // Same result as hash_join_inner: 3 matched rows
    assert_eq!(rows.len(), 3);

    let mut pairs: Vec<(String, String)> = rows
        .iter()
        .map(|r| {
            let name = match r.get(1).unwrap() {
                Datum::Text(s) => s.clone(),
                other => panic!("expected Text for name, got {:?}", other),
            };
            let dept_name = match r.get(4).unwrap() {
                Datum::Text(s) => s.clone(),
                other => panic!("expected Text for dept_name, got {:?}", other),
            };
            (name, dept_name)
        })
        .collect();
    pairs.sort();

    assert_eq!(
        pairs,
        vec![
            ("Alice".to_string(), "Engineering".to_string()),
            ("Bob".to_string(), "Sales".to_string()),
            ("Charlie".to_string(), "Engineering".to_string()),
        ]
    );
}
