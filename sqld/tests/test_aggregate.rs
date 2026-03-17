use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sqld::executor::executor::{build_executor, CatalogProvider, Executor, ExecutorContext};
use sqld::planner::logical_plan::{AggregateExpr, AggregateFunc};
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
// Test data helpers
// ===========================================================================

fn make_tuple(values: Vec<Datum>) -> Tuple {
    Tuple::new(MvccHeader::new(0, 0, 0), values)
}

/// Sales table schema: (id INTEGER, product TEXT, amount INTEGER, region TEXT)
fn sales_schema() -> Schema {
    Schema::new(vec![
        Column::new("id", DataType::Integer, false),
        Column::new("product", DataType::Text, false),
        Column::new("amount", DataType::Integer, true),
        Column::new("region", DataType::Text, true),
    ])
}

/// Sales test data:
/// (1, "Widget", 100, "North"), (2, "Widget", 200, "South"),
/// (3, "Gadget", 150, "North"), (4, "Gadget", 250, "South"),
/// (5, "Widget", 300, "North"), (6, "Widget", 100, "North")
fn sales_data() -> Vec<Tuple> {
    vec![
        make_tuple(vec![
            Datum::Integer(1),
            Datum::Text("Widget".into()),
            Datum::Integer(100),
            Datum::Text("North".into()),
        ]),
        make_tuple(vec![
            Datum::Integer(2),
            Datum::Text("Widget".into()),
            Datum::Integer(200),
            Datum::Text("South".into()),
        ]),
        make_tuple(vec![
            Datum::Integer(3),
            Datum::Text("Gadget".into()),
            Datum::Integer(150),
            Datum::Text("North".into()),
        ]),
        make_tuple(vec![
            Datum::Integer(4),
            Datum::Text("Gadget".into()),
            Datum::Integer(250),
            Datum::Text("South".into()),
        ]),
        make_tuple(vec![
            Datum::Integer(5),
            Datum::Text("Widget".into()),
            Datum::Integer(300),
            Datum::Text("North".into()),
        ]),
        make_tuple(vec![
            Datum::Integer(6),
            Datum::Text("Widget".into()),
            Datum::Integer(100),
            Datum::Text("North".into()),
        ]),
    ]
}

fn setup_catalog() -> Arc<MockCatalog> {
    let catalog = Arc::new(MockCatalog::new());
    catalog.add_table("sales", sales_schema(), sales_data());
    catalog
}

/// Helper: build a SeqScan plan for the sales table.
fn sales_seq_scan() -> PhysicalPlan {
    PhysicalPlan::SeqScan {
        table: "sales".into(),
        alias: None,
        schema: sales_schema(),
        predicate: None,
    }
}

/// Collect all rows from an executor into a Vec.
fn collect_rows(exec: &mut Box<dyn Executor>) -> Vec<Tuple> {
    let mut rows = Vec::new();
    while let Some(tuple) = exec.next().unwrap() {
        rows.push(tuple);
    }
    rows
}

// ===========================================================================
// 1. hash_agg_count -- COUNT(*) with no group by
// ===========================================================================

#[test]
fn hash_agg_count() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog.clone()));

    // COUNT(*) implemented as COUNT(1) -- counting a non-null constant
    let agg_schema = Schema::new(vec![
        Column::new("cnt", DataType::BigInt, true),
    ]);

    let plan = PhysicalPlan::HashAggregate {
        group_by: vec![],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Count,
            arg: Expr::Integer(1),
            distinct: false,
            alias: "cnt".into(),
        }],
        input: Box::new(sales_seq_scan()),
        schema: agg_schema,
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();

    let rows = collect_rows(&mut exec);
    assert_eq!(rows.len(), 1, "COUNT with no GROUP BY should produce exactly one row");
    assert_eq!(rows[0].get(0).unwrap(), &Datum::BigInt(6));

    exec.close().unwrap();
}

// ===========================================================================
// 2. hash_agg_sum -- SUM(amount) with no group by
// ===========================================================================

#[test]
fn hash_agg_sum() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog.clone()));

    // SUM(amount): 100 + 200 + 150 + 250 + 300 + 100 = 1100
    let agg_schema = Schema::new(vec![
        Column::new("total", DataType::BigInt, true),
    ]);

    let plan = PhysicalPlan::HashAggregate {
        group_by: vec![],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Sum,
            arg: Expr::Identifier("amount".into()),
            distinct: false,
            alias: "total".into(),
        }],
        input: Box::new(sales_seq_scan()),
        schema: agg_schema,
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();

    let rows = collect_rows(&mut exec);
    assert_eq!(rows.len(), 1);
    // SUM of Integer values yields Integer via Datum::add
    assert_eq!(rows[0].get(0).unwrap(), &Datum::Integer(1100));

    exec.close().unwrap();
}

// ===========================================================================
// 3. hash_agg_avg -- AVG(amount) with no group by
// ===========================================================================

#[test]
fn hash_agg_avg() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog.clone()));

    // AVG(amount): 1100 / 6 = 183.333...
    let agg_schema = Schema::new(vec![
        Column::new("avg_amt", DataType::Float, true),
    ]);

    let plan = PhysicalPlan::HashAggregate {
        group_by: vec![],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Avg,
            arg: Expr::Identifier("amount".into()),
            distinct: false,
            alias: "avg_amt".into(),
        }],
        input: Box::new(sales_seq_scan()),
        schema: agg_schema,
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();

    let rows = collect_rows(&mut exec);
    assert_eq!(rows.len(), 1);

    match rows[0].get(0).unwrap() {
        Datum::Float(f) => {
            let expected = 1100.0 / 6.0;
            assert!(
                (f - expected).abs() < 0.001,
                "AVG should be ~{expected}, got {f}"
            );
        }
        other => panic!("expected Float for AVG, got {:?}", other),
    }

    exec.close().unwrap();
}

// ===========================================================================
// 4. hash_agg_min_max -- MIN(amount), MAX(amount) with no group by
// ===========================================================================

#[test]
fn hash_agg_min_max() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog.clone()));

    let agg_schema = Schema::new(vec![
        Column::new("min_amt", DataType::Integer, true),
        Column::new("max_amt", DataType::Integer, true),
    ]);

    let plan = PhysicalPlan::HashAggregate {
        group_by: vec![],
        aggregates: vec![
            AggregateExpr {
                func: AggregateFunc::Min,
                arg: Expr::Identifier("amount".into()),
                distinct: false,
                alias: "min_amt".into(),
            },
            AggregateExpr {
                func: AggregateFunc::Max,
                arg: Expr::Identifier("amount".into()),
                distinct: false,
                alias: "max_amt".into(),
            },
        ],
        input: Box::new(sales_seq_scan()),
        schema: agg_schema,
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();

    let rows = collect_rows(&mut exec);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0).unwrap(), &Datum::Integer(100), "MIN should be 100");
    assert_eq!(rows[0].get(1).unwrap(), &Datum::Integer(300), "MAX should be 300");

    exec.close().unwrap();
}

// ===========================================================================
// 5. hash_agg_group_by -- GROUP BY product, SUM(amount)
// ===========================================================================

#[test]
fn hash_agg_group_by() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog.clone()));

    let agg_schema = Schema::new(vec![
        Column::new("product", DataType::Text, true),
        Column::new("total", DataType::BigInt, true),
    ]);

    let plan = PhysicalPlan::HashAggregate {
        group_by: vec![Expr::Identifier("product".into())],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Sum,
            arg: Expr::Identifier("amount".into()),
            distinct: false,
            alias: "total".into(),
        }],
        input: Box::new(sales_seq_scan()),
        schema: agg_schema,
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();

    let rows = collect_rows(&mut exec);
    assert_eq!(rows.len(), 2, "should have two groups: Widget and Gadget");

    // Collect results into a map for order-independent comparison.
    let mut results: HashMap<String, Datum> = HashMap::new();
    for row in &rows {
        let product = match row.get(0).unwrap() {
            Datum::Text(s) => s.clone(),
            other => panic!("expected Text for product, got {:?}", other),
        };
        let total = row.get(1).unwrap().clone();
        results.insert(product, total);
    }

    // Widget: 100 + 200 + 300 + 100 = 700
    assert_eq!(results.get("Widget").unwrap(), &Datum::Integer(700));
    // Gadget: 150 + 250 = 400
    assert_eq!(results.get("Gadget").unwrap(), &Datum::Integer(400));

    exec.close().unwrap();
}

// ===========================================================================
// 6. hash_agg_count_distinct -- COUNT(DISTINCT region)
// ===========================================================================

#[test]
fn hash_agg_count_distinct() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog.clone()));

    // COUNT(DISTINCT region): "North" and "South" = 2
    let agg_schema = Schema::new(vec![
        Column::new("distinct_regions", DataType::BigInt, true),
    ]);

    let plan = PhysicalPlan::HashAggregate {
        group_by: vec![],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Count,
            arg: Expr::Identifier("region".into()),
            distinct: true,
            alias: "distinct_regions".into(),
        }],
        input: Box::new(sales_seq_scan()),
        schema: agg_schema,
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();

    let rows = collect_rows(&mut exec);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get(0).unwrap(),
        &Datum::BigInt(2),
        "COUNT(DISTINCT region) should be 2 (North, South)"
    );

    exec.close().unwrap();
}

// ===========================================================================
// 7. hash_agg_empty_input -- aggregate with no rows (COUNT=0, SUM=NULL)
// ===========================================================================

#[test]
fn hash_agg_empty_input() {
    let catalog = Arc::new(MockCatalog::new());
    let empty_schema = sales_schema();
    catalog.add_table("empty_sales", empty_schema.clone(), vec![]);
    let ctx = Arc::new(ExecutorContext::new(catalog));

    let agg_schema = Schema::new(vec![
        Column::new("cnt", DataType::BigInt, true),
        Column::new("total", DataType::BigInt, true),
    ]);

    let plan = PhysicalPlan::HashAggregate {
        group_by: vec![],
        aggregates: vec![
            AggregateExpr {
                func: AggregateFunc::Count,
                arg: Expr::Integer(1),
                distinct: false,
                alias: "cnt".into(),
            },
            AggregateExpr {
                func: AggregateFunc::Sum,
                arg: Expr::Identifier("amount".into()),
                distinct: false,
                alias: "total".into(),
            },
        ],
        input: Box::new(PhysicalPlan::SeqScan {
            table: "empty_sales".into(),
            alias: None,
            schema: empty_schema,
            predicate: None,
        }),
        schema: agg_schema,
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();

    let rows = collect_rows(&mut exec);
    // With no GROUP BY and no input rows, one row should be emitted with
    // default aggregate values: COUNT=0, SUM=NULL.
    assert_eq!(rows.len(), 1, "no GROUP BY on empty input should still emit one row");
    assert_eq!(rows[0].get(0).unwrap(), &Datum::BigInt(0), "COUNT on empty should be 0");
    assert_eq!(rows[0].get(1).unwrap(), &Datum::Null, "SUM on empty should be NULL");

    exec.close().unwrap();
}

// ===========================================================================
// 8. sort_agg_group_by -- GROUP BY product, SUM(amount) using SortAggregate
// ===========================================================================

#[test]
fn sort_agg_group_by() {
    // SortAggregate expects input sorted by the group-by keys.
    // Prepare data pre-sorted by product: Gadget first, then Widget.
    let catalog = Arc::new(MockCatalog::new());
    let schema = sales_schema();

    let sorted_data = vec![
        make_tuple(vec![
            Datum::Integer(3),
            Datum::Text("Gadget".into()),
            Datum::Integer(150),
            Datum::Text("North".into()),
        ]),
        make_tuple(vec![
            Datum::Integer(4),
            Datum::Text("Gadget".into()),
            Datum::Integer(250),
            Datum::Text("South".into()),
        ]),
        make_tuple(vec![
            Datum::Integer(1),
            Datum::Text("Widget".into()),
            Datum::Integer(100),
            Datum::Text("North".into()),
        ]),
        make_tuple(vec![
            Datum::Integer(2),
            Datum::Text("Widget".into()),
            Datum::Integer(200),
            Datum::Text("South".into()),
        ]),
        make_tuple(vec![
            Datum::Integer(5),
            Datum::Text("Widget".into()),
            Datum::Integer(300),
            Datum::Text("North".into()),
        ]),
        make_tuple(vec![
            Datum::Integer(6),
            Datum::Text("Widget".into()),
            Datum::Integer(100),
            Datum::Text("North".into()),
        ]),
    ];

    catalog.add_table("sorted_sales", schema.clone(), sorted_data);
    let ctx = Arc::new(ExecutorContext::new(catalog));

    let agg_schema = Schema::new(vec![
        Column::new("product", DataType::Text, true),
        Column::new("total", DataType::BigInt, true),
    ]);

    let plan = PhysicalPlan::SortAggregate {
        group_by: vec![Expr::Identifier("product".into())],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Sum,
            arg: Expr::Identifier("amount".into()),
            distinct: false,
            alias: "total".into(),
        }],
        input: Box::new(PhysicalPlan::SeqScan {
            table: "sorted_sales".into(),
            alias: None,
            schema,
            predicate: None,
        }),
        schema: agg_schema,
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();

    let rows = collect_rows(&mut exec);
    assert_eq!(rows.len(), 2, "should have two groups: Gadget and Widget");

    // SortAggregate preserves group order from sorted input.
    // First group: Gadget (150 + 250 = 400)
    assert_eq!(rows[0].get(0).unwrap(), &Datum::Text("Gadget".into()));
    assert_eq!(rows[0].get(1).unwrap(), &Datum::Integer(400));

    // Second group: Widget (100 + 200 + 300 + 100 = 700)
    assert_eq!(rows[1].get(0).unwrap(), &Datum::Text("Widget".into()));
    assert_eq!(rows[1].get(1).unwrap(), &Datum::Integer(700));

    exec.close().unwrap();
}

// ===========================================================================
// 9. hash_agg_null_handling -- NULLs excluded from SUM/AVG/MIN/MAX,
//    but COUNT(*) via constant counts all rows
// ===========================================================================

#[test]
fn hash_agg_null_handling() {
    // Create a table where some amount values are NULL.
    let catalog = Arc::new(MockCatalog::new());
    let schema = Schema::new(vec![
        Column::new("id", DataType::Integer, false),
        Column::new("product", DataType::Text, false),
        Column::new("amount", DataType::Integer, true),
        Column::new("region", DataType::Text, true),
    ]);

    let data = vec![
        make_tuple(vec![
            Datum::Integer(1),
            Datum::Text("Widget".into()),
            Datum::Integer(100),
            Datum::Text("North".into()),
        ]),
        make_tuple(vec![
            Datum::Integer(2),
            Datum::Text("Widget".into()),
            Datum::Null, // NULL amount
            Datum::Text("South".into()),
        ]),
        make_tuple(vec![
            Datum::Integer(3),
            Datum::Text("Widget".into()),
            Datum::Integer(300),
            Datum::Text("North".into()),
        ]),
        make_tuple(vec![
            Datum::Integer(4),
            Datum::Text("Gadget".into()),
            Datum::Null, // NULL amount
            Datum::Null, // NULL region
        ]),
    ];

    catalog.add_table("sales_with_nulls", schema.clone(), data);
    let ctx = Arc::new(ExecutorContext::new(catalog));

    // Aggregate without GROUP BY: COUNT(*), SUM(amount), AVG(amount),
    // MIN(amount), MAX(amount)
    let agg_schema = Schema::new(vec![
        Column::new("cnt_all", DataType::BigInt, true),
        Column::new("sum_amt", DataType::BigInt, true),
        Column::new("avg_amt", DataType::Float, true),
        Column::new("min_amt", DataType::Integer, true),
        Column::new("max_amt", DataType::Integer, true),
    ]);

    let plan = PhysicalPlan::HashAggregate {
        group_by: vec![],
        aggregates: vec![
            AggregateExpr {
                func: AggregateFunc::Count,
                arg: Expr::Integer(1), // COUNT(*) via non-null constant
                distinct: false,
                alias: "cnt_all".into(),
            },
            AggregateExpr {
                func: AggregateFunc::Sum,
                arg: Expr::Identifier("amount".into()),
                distinct: false,
                alias: "sum_amt".into(),
            },
            AggregateExpr {
                func: AggregateFunc::Avg,
                arg: Expr::Identifier("amount".into()),
                distinct: false,
                alias: "avg_amt".into(),
            },
            AggregateExpr {
                func: AggregateFunc::Min,
                arg: Expr::Identifier("amount".into()),
                distinct: false,
                alias: "min_amt".into(),
            },
            AggregateExpr {
                func: AggregateFunc::Max,
                arg: Expr::Identifier("amount".into()),
                distinct: false,
                alias: "max_amt".into(),
            },
        ],
        input: Box::new(PhysicalPlan::SeqScan {
            table: "sales_with_nulls".into(),
            alias: None,
            schema,
            predicate: None,
        }),
        schema: agg_schema,
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();

    let rows = collect_rows(&mut exec);
    assert_eq!(rows.len(), 1);

    // COUNT(1) counts all 4 rows (constant 1 is never null)
    assert_eq!(
        rows[0].get(0).unwrap(),
        &Datum::BigInt(4),
        "COUNT(*) should count all rows including those with NULL amounts"
    );

    // SUM(amount) only sums non-null values: 100 + 300 = 400
    assert_eq!(
        rows[0].get(1).unwrap(),
        &Datum::Integer(400),
        "SUM should skip NULL values"
    );

    // AVG(amount) = 400 / 2 = 200.0 (only 2 non-null values)
    match rows[0].get(2).unwrap() {
        Datum::Float(f) => {
            assert!(
                (f - 200.0).abs() < 0.001,
                "AVG should be 200.0, got {f}"
            );
        }
        other => panic!("expected Float for AVG, got {:?}", other),
    }

    // MIN(amount) = 100 (ignoring NULLs)
    assert_eq!(
        rows[0].get(3).unwrap(),
        &Datum::Integer(100),
        "MIN should skip NULL values"
    );

    // MAX(amount) = 300 (ignoring NULLs)
    assert_eq!(
        rows[0].get(4).unwrap(),
        &Datum::Integer(300),
        "MAX should skip NULL values"
    );

    exec.close().unwrap();
}

// ===========================================================================
// 10. hash_agg_string_agg -- STRING_AGG(region) with no group by
// ===========================================================================

#[test]
fn hash_agg_string_agg() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog.clone()));

    // STRING_AGG(region): "North,South,North,South,North,North"
    let agg_schema = Schema::new(vec![
        Column::new("regions", DataType::Text, true),
    ]);

    let plan = PhysicalPlan::HashAggregate {
        group_by: vec![],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::StringAgg,
            arg: Expr::Identifier("region".into()),
            distinct: false,
            alias: "regions".into(),
        }],
        input: Box::new(sales_seq_scan()),
        schema: agg_schema,
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();

    let rows = collect_rows(&mut exec);
    assert_eq!(rows.len(), 1, "STRING_AGG with no GROUP BY should produce one row");

    match rows[0].get(0).unwrap() {
        Datum::Text(s) => {
            // The values should be comma-separated.  The exact order depends on
            // scan order which is insertion order for the mock catalog.
            assert_eq!(s, "North,South,North,South,North,North");
        }
        other => panic!("expected Text for STRING_AGG, got {:?}", other),
    }

    exec.close().unwrap();
}

// ===========================================================================
// 11. hash_agg_array_agg -- ARRAY_AGG(region) with no group by
// ===========================================================================

#[test]
fn hash_agg_array_agg() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog.clone()));

    // ARRAY_AGG(region): "{North,South,North,South,North,North}"
    let agg_schema = Schema::new(vec![
        Column::new("regions_arr", DataType::Text, true),
    ]);

    let plan = PhysicalPlan::HashAggregate {
        group_by: vec![],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::ArrayAgg,
            arg: Expr::Identifier("region".into()),
            distinct: false,
            alias: "regions_arr".into(),
        }],
        input: Box::new(sales_seq_scan()),
        schema: agg_schema,
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();

    let rows = collect_rows(&mut exec);
    assert_eq!(rows.len(), 1, "ARRAY_AGG with no GROUP BY should produce one row");

    match rows[0].get(0).unwrap() {
        Datum::Text(s) => {
            assert_eq!(s, "{North,South,North,South,North,North}");
        }
        other => panic!("expected Text for ARRAY_AGG, got {:?}", other),
    }

    exec.close().unwrap();
}

// ===========================================================================
// 12. hash_agg_bool_and -- BOOL_AND: all true, some false, with NULLs
// ===========================================================================

/// Helper: boolean table schema (id INTEGER, flag BOOLEAN)
fn bool_schema() -> Schema {
    Schema::new(vec![
        Column::new("id", DataType::Integer, false),
        Column::new("flag", DataType::Boolean, true),
    ])
}

fn bool_seq_scan(table: &str, schema: Schema) -> PhysicalPlan {
    PhysicalPlan::SeqScan {
        table: table.into(),
        alias: None,
        schema,
        predicate: None,
    }
}

#[test]
fn hash_agg_bool_and() {
    // --- Case 1: all non-null values are true => BOOL_AND = true ---
    let catalog = Arc::new(MockCatalog::new());
    let schema = bool_schema();

    let all_true_data = vec![
        make_tuple(vec![Datum::Integer(1), Datum::Boolean(true)]),
        make_tuple(vec![Datum::Integer(2), Datum::Boolean(true)]),
        make_tuple(vec![Datum::Integer(3), Datum::Null]), // NULL is skipped
        make_tuple(vec![Datum::Integer(4), Datum::Boolean(true)]),
    ];
    catalog.add_table("bools_all_true", schema.clone(), all_true_data);
    let ctx = Arc::new(ExecutorContext::new(catalog));

    let agg_schema = Schema::new(vec![
        Column::new("result", DataType::Boolean, true),
    ]);

    let plan = PhysicalPlan::HashAggregate {
        group_by: vec![],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::BoolAnd,
            arg: Expr::Identifier("flag".into()),
            distinct: false,
            alias: "result".into(),
        }],
        input: Box::new(bool_seq_scan("bools_all_true", schema)),
        schema: agg_schema,
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();

    let rows = collect_rows(&mut exec);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get(0).unwrap(),
        &Datum::Boolean(true),
        "BOOL_AND of all-true (with NULLs) should be true"
    );
    exec.close().unwrap();

    // --- Case 2: some false values => BOOL_AND = false ---
    let catalog2 = Arc::new(MockCatalog::new());
    let schema2 = bool_schema();

    let some_false_data = vec![
        make_tuple(vec![Datum::Integer(1), Datum::Boolean(true)]),
        make_tuple(vec![Datum::Integer(2), Datum::Boolean(false)]),
        make_tuple(vec![Datum::Integer(3), Datum::Null]),
        make_tuple(vec![Datum::Integer(4), Datum::Boolean(true)]),
    ];
    catalog2.add_table("bools_some_false", schema2.clone(), some_false_data);
    let ctx2 = Arc::new(ExecutorContext::new(catalog2));

    let agg_schema2 = Schema::new(vec![
        Column::new("result", DataType::Boolean, true),
    ]);

    let plan2 = PhysicalPlan::HashAggregate {
        group_by: vec![],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::BoolAnd,
            arg: Expr::Identifier("flag".into()),
            distinct: false,
            alias: "result".into(),
        }],
        input: Box::new(bool_seq_scan("bools_some_false", schema2)),
        schema: agg_schema2,
    };

    let mut exec2 = build_executor(plan2, ctx2);
    exec2.init().unwrap();

    let rows2 = collect_rows(&mut exec2);
    assert_eq!(rows2.len(), 1);
    assert_eq!(
        rows2[0].get(0).unwrap(),
        &Datum::Boolean(false),
        "BOOL_AND with some false values should be false"
    );
    exec2.close().unwrap();
}

// ===========================================================================
// 13. hash_agg_bool_or -- BOOL_OR: all false, some true, with NULLs
// ===========================================================================

#[test]
fn hash_agg_bool_or() {
    // --- Case 1: all non-null values are false => BOOL_OR = false ---
    let catalog = Arc::new(MockCatalog::new());
    let schema = bool_schema();

    let all_false_data = vec![
        make_tuple(vec![Datum::Integer(1), Datum::Boolean(false)]),
        make_tuple(vec![Datum::Integer(2), Datum::Boolean(false)]),
        make_tuple(vec![Datum::Integer(3), Datum::Null]),
    ];
    catalog.add_table("bools_all_false", schema.clone(), all_false_data);
    let ctx = Arc::new(ExecutorContext::new(catalog));

    let agg_schema = Schema::new(vec![
        Column::new("result", DataType::Boolean, true),
    ]);

    let plan = PhysicalPlan::HashAggregate {
        group_by: vec![],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::BoolOr,
            arg: Expr::Identifier("flag".into()),
            distinct: false,
            alias: "result".into(),
        }],
        input: Box::new(bool_seq_scan("bools_all_false", schema)),
        schema: agg_schema,
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();

    let rows = collect_rows(&mut exec);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get(0).unwrap(),
        &Datum::Boolean(false),
        "BOOL_OR of all-false (with NULLs) should be false"
    );
    exec.close().unwrap();

    // --- Case 2: some true values => BOOL_OR = true ---
    let catalog2 = Arc::new(MockCatalog::new());
    let schema2 = bool_schema();

    let some_true_data = vec![
        make_tuple(vec![Datum::Integer(1), Datum::Boolean(false)]),
        make_tuple(vec![Datum::Integer(2), Datum::Boolean(true)]),
        make_tuple(vec![Datum::Integer(3), Datum::Null]),
        make_tuple(vec![Datum::Integer(4), Datum::Boolean(false)]),
    ];
    catalog2.add_table("bools_some_true", schema2.clone(), some_true_data);
    let ctx2 = Arc::new(ExecutorContext::new(catalog2));

    let agg_schema2 = Schema::new(vec![
        Column::new("result", DataType::Boolean, true),
    ]);

    let plan2 = PhysicalPlan::HashAggregate {
        group_by: vec![],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::BoolOr,
            arg: Expr::Identifier("flag".into()),
            distinct: false,
            alias: "result".into(),
        }],
        input: Box::new(bool_seq_scan("bools_some_true", schema2)),
        schema: agg_schema2,
    };

    let mut exec2 = build_executor(plan2, ctx2);
    exec2.init().unwrap();

    let rows2 = collect_rows(&mut exec2);
    assert_eq!(rows2.len(), 1);
    assert_eq!(
        rows2[0].get(0).unwrap(),
        &Datum::Boolean(true),
        "BOOL_OR with some true values should be true"
    );
    exec2.close().unwrap();
}

// ===========================================================================
// 14. hash_agg_string_agg_distinct -- STRING_AGG(DISTINCT region)
// ===========================================================================

#[test]
fn hash_agg_string_agg_distinct() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog.clone()));

    // STRING_AGG(DISTINCT region): only "North" and "South" (distinct values)
    let agg_schema = Schema::new(vec![
        Column::new("distinct_regions", DataType::Text, true),
    ]);

    let plan = PhysicalPlan::HashAggregate {
        group_by: vec![],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::StringAgg,
            arg: Expr::Identifier("region".into()),
            distinct: true,
            alias: "distinct_regions".into(),
        }],
        input: Box::new(sales_seq_scan()),
        schema: agg_schema,
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();

    let rows = collect_rows(&mut exec);
    assert_eq!(rows.len(), 1, "STRING_AGG DISTINCT should produce one row");

    match rows[0].get(0).unwrap() {
        Datum::Text(s) => {
            // The result should contain exactly "North" and "South" separated by comma.
            // Order may depend on insertion order, so check both parts are present.
            let mut parts: Vec<&str> = s.split(',').collect();
            parts.sort();
            assert_eq!(
                parts,
                vec!["North", "South"],
                "STRING_AGG(DISTINCT region) should contain exactly North and South"
            );
        }
        other => panic!("expected Text for STRING_AGG DISTINCT, got {:?}", other),
    }

    exec.close().unwrap();
}

// ===========================================================================
// 15. hash_agg_array_agg_group_by -- ARRAY_AGG(region) GROUP BY product
// ===========================================================================

#[test]
fn hash_agg_array_agg_group_by() {
    let catalog = setup_catalog();
    let ctx = Arc::new(ExecutorContext::new(catalog.clone()));

    let agg_schema = Schema::new(vec![
        Column::new("product", DataType::Text, true),
        Column::new("regions_arr", DataType::Text, true),
    ]);

    let plan = PhysicalPlan::HashAggregate {
        group_by: vec![Expr::Identifier("product".into())],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::ArrayAgg,
            arg: Expr::Identifier("region".into()),
            distinct: false,
            alias: "regions_arr".into(),
        }],
        input: Box::new(sales_seq_scan()),
        schema: agg_schema,
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();

    let rows = collect_rows(&mut exec);
    assert_eq!(rows.len(), 2, "should have two groups: Widget and Gadget");

    // Collect results into a map for order-independent comparison.
    let mut results: HashMap<String, String> = HashMap::new();
    for row in &rows {
        let product = match row.get(0).unwrap() {
            Datum::Text(s) => s.clone(),
            other => panic!("expected Text for product, got {:?}", other),
        };
        let arr = match row.get(1).unwrap() {
            Datum::Text(s) => s.clone(),
            other => panic!("expected Text for ARRAY_AGG, got {:?}", other),
        };
        results.insert(product, arr);
    }

    // Widget rows have regions: North, South, North, North
    assert_eq!(
        results.get("Widget").unwrap(),
        "{North,South,North,North}",
        "ARRAY_AGG for Widget group"
    );
    // Gadget rows have regions: North, South
    assert_eq!(
        results.get("Gadget").unwrap(),
        "{North,South}",
        "ARRAY_AGG for Gadget group"
    );

    exec.close().unwrap();
}

// ===========================================================================
// 16. hash_agg_new_aggregates_empty_input -- STRING_AGG/ARRAY_AGG/BOOL_AND/
//     BOOL_OR on empty input should all return NULL
// ===========================================================================

#[test]
fn hash_agg_new_aggregates_empty_input() {
    let catalog = Arc::new(MockCatalog::new());

    // Table with text and boolean columns, but no rows
    let schema = Schema::new(vec![
        Column::new("id", DataType::Integer, false),
        Column::new("name", DataType::Text, true),
        Column::new("flag", DataType::Boolean, true),
    ]);
    catalog.add_table("empty_table", schema.clone(), vec![]);
    let ctx = Arc::new(ExecutorContext::new(catalog));

    let agg_schema = Schema::new(vec![
        Column::new("str_agg", DataType::Text, true),
        Column::new("arr_agg", DataType::Text, true),
        Column::new("b_and", DataType::Boolean, true),
        Column::new("b_or", DataType::Boolean, true),
    ]);

    let plan = PhysicalPlan::HashAggregate {
        group_by: vec![],
        aggregates: vec![
            AggregateExpr {
                func: AggregateFunc::StringAgg,
                arg: Expr::Identifier("name".into()),
                distinct: false,
                alias: "str_agg".into(),
            },
            AggregateExpr {
                func: AggregateFunc::ArrayAgg,
                arg: Expr::Identifier("name".into()),
                distinct: false,
                alias: "arr_agg".into(),
            },
            AggregateExpr {
                func: AggregateFunc::BoolAnd,
                arg: Expr::Identifier("flag".into()),
                distinct: false,
                alias: "b_and".into(),
            },
            AggregateExpr {
                func: AggregateFunc::BoolOr,
                arg: Expr::Identifier("flag".into()),
                distinct: false,
                alias: "b_or".into(),
            },
        ],
        input: Box::new(PhysicalPlan::SeqScan {
            table: "empty_table".into(),
            alias: None,
            schema,
            predicate: None,
        }),
        schema: agg_schema,
    };

    let mut exec = build_executor(plan, ctx);
    exec.init().unwrap();

    let rows = collect_rows(&mut exec);
    assert_eq!(rows.len(), 1, "no GROUP BY on empty input should still emit one row");

    assert_eq!(
        rows[0].get(0).unwrap(),
        &Datum::Null,
        "STRING_AGG on empty input should be NULL"
    );
    assert_eq!(
        rows[0].get(1).unwrap(),
        &Datum::Null,
        "ARRAY_AGG on empty input should be NULL"
    );
    assert_eq!(
        rows[0].get(2).unwrap(),
        &Datum::Null,
        "BOOL_AND on empty input should be NULL"
    );
    assert_eq!(
        rows[0].get(3).unwrap(),
        &Datum::Null,
        "BOOL_OR on empty input should be NULL"
    );

    exec.close().unwrap();
}
