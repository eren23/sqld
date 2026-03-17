use sqld::planner::cost_model::{CostConstants, CostModel};
use sqld::planner::logical_plan::{AggregateExpr, AggregateFunc, ProjectionExpr, SortExpr};
use sqld::planner::physical_plan::*;
use sqld::planner::{Catalog, TableStats, ColumnStats};
use sqld::sql::ast::*;
use sqld::types::{Column, DataType, Schema};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Catalog helper
// ---------------------------------------------------------------------------

fn make_catalog() -> Catalog {
    let mut catalog = Catalog::new();

    catalog.add_table(
        "users",
        Schema::new(vec![
            Column::new("id", DataType::Integer, false),
            Column::new("name", DataType::Varchar(255), true),
            Column::new("age", DataType::Integer, true),
        ]),
    );
    catalog.set_stats(
        "users",
        TableStats {
            row_count: 10000.0,
            page_count: 100.0,
            column_stats: HashMap::new(),
        },
    );

    catalog.add_table(
        "orders",
        Schema::new(vec![
            Column::new("id", DataType::Integer, false),
            Column::new("user_id", DataType::Integer, true),
            Column::new("amount", DataType::Float, true),
        ]),
    );
    catalog.set_stats(
        "orders",
        TableStats {
            row_count: 50000.0,
            page_count: 500.0,
            column_stats: HashMap::new(),
        },
    );

    catalog
}

// ---------------------------------------------------------------------------
// Schema helpers
// ---------------------------------------------------------------------------

fn users_schema() -> Schema {
    Schema::new(vec![
        Column::new("id", DataType::Integer, false),
        Column::new("name", DataType::Varchar(255), true),
        Column::new("age", DataType::Integer, true),
    ])
}

fn orders_schema() -> Schema {
    Schema::new(vec![
        Column::new("id", DataType::Integer, false),
        Column::new("user_id", DataType::Integer, true),
        Column::new("amount", DataType::Float, true),
    ])
}

// ---------------------------------------------------------------------------
// Approximate equality helper
// ---------------------------------------------------------------------------

fn approx_eq(a: f64, b: f64, tolerance: f64) -> bool {
    (a - b).abs() <= tolerance
}

// ---------------------------------------------------------------------------
// 1. test_seq_scan_cost
//    SeqScan on "users" → seq_page_cost * pages + cpu_tuple_cost * rows
//    = 1.0*100 + 0.01*10000 = 200.0
// ---------------------------------------------------------------------------

#[test]
fn test_seq_scan_cost() {
    let catalog = make_catalog();
    let cost_model = CostModel::new(&catalog);

    let plan = PhysicalPlan::SeqScan {
        table: "users".into(),
        alias: None,
        schema: users_schema(),
        predicate: None,
    };

    let cost = cost_model.estimate_cost(&plan);
    // seq_page_cost(1.0) * 100 + cpu_tuple_cost(0.01) * 10000 = 100 + 100 = 200
    assert!(
        approx_eq(cost, 200.0, 1e-9),
        "expected 200.0, got {}",
        cost
    );
}

// ---------------------------------------------------------------------------
// 2. test_index_scan_cheaper_for_selective
//    IndexScan with 1 range on "users" → random_page_cost*(pages*0.1)
//    + cpu_index_tuple_cost*(rows*0.1) = 4.0*10 + 0.005*1000 = 45.0
//    which is cheaper than SeqScan at 200.0
// ---------------------------------------------------------------------------

#[test]
fn test_index_scan_cheaper_for_selective() {
    let catalog = make_catalog();
    let cost_model = CostModel::new(&catalog);

    let index_scan = PhysicalPlan::IndexScan {
        table: "users".into(),
        alias: None,
        index_name: "idx_users_id".into(),
        schema: users_schema(),
        key_ranges: vec![KeyRange::eq(Expr::Integer(1))],
        predicate: None,
    };

    let seq_scan = PhysicalPlan::SeqScan {
        table: "users".into(),
        alias: None,
        schema: users_schema(),
        predicate: None,
    };

    let idx_cost = cost_model.estimate_cost(&index_scan);
    let seq_cost = cost_model.estimate_cost(&seq_scan);

    // Expected index cost: random_page_cost(4.0) * (100*0.1=10) + cpu_index_tuple_cost(0.005) * (10000*0.1=1000)
    // = 40.0 + 5.0 = 45.0
    assert!(
        approx_eq(idx_cost, 45.0, 1e-9),
        "expected index scan cost 45.0, got {}",
        idx_cost
    );

    assert!(
        idx_cost < seq_cost,
        "index scan ({}) should be cheaper than seq scan ({}) for selective query",
        idx_cost,
        seq_cost
    );
}

// ---------------------------------------------------------------------------
// 3. test_hash_join_cost
//    HashJoin of users (left) and orders (right)
//    left_cost = 200.0 (users seq scan)
//    right_cost = 500.0 + 500.0 = 1000.0 (orders seq scan: 1.0*500 + 0.01*50000)
//    build_cost = hash_build_cost(0.02) * right_rows(50000) = 1000.0
//    probe_cost = cpu_tuple_cost(0.01) * left_rows(10000) = 100.0
//    total = 200.0 + 1000.0 + 1000.0 + 100.0 = 2300.0
// ---------------------------------------------------------------------------

#[test]
fn test_hash_join_cost() {
    let catalog = make_catalog();
    let cost_model = CostModel::new(&catalog);

    let left = PhysicalPlan::SeqScan {
        table: "users".into(),
        alias: None,
        schema: users_schema(),
        predicate: None,
    };
    let right = PhysicalPlan::SeqScan {
        table: "orders".into(),
        alias: None,
        schema: orders_schema(),
        predicate: None,
    };

    let join_schema = users_schema().merge(&orders_schema());

    let hash_join = PhysicalPlan::HashJoin {
        join_type: JoinType::Inner,
        left_keys: vec![Expr::Identifier("id".into())],
        right_keys: vec![Expr::Identifier("user_id".into())],
        condition: None,
        left: Box::new(left),
        right: Box::new(right),
        schema: join_schema,
    };

    let cost = cost_model.estimate_cost(&hash_join);

    // left_cost: 1.0*100 + 0.01*10000 = 200.0
    // right_cost: 1.0*500 + 0.01*50000 = 1000.0
    // build_cost: 0.02 * 50000 = 1000.0
    // probe_cost: 0.01 * 10000 = 100.0
    // total = 2300.0
    assert!(
        approx_eq(cost, 2300.0, 1e-9),
        "expected hash join cost 2300.0, got {}",
        cost
    );
}

// ---------------------------------------------------------------------------
// 4. test_nested_loop_join_expensive_for_large
//    NestedLoopJoin of users and orders
//    left_cost = 200.0
//    right_cost = 1000.0
//    total = left_cost + left_rows * right_cost
//          = 200.0 + 10000.0 * 1000.0 = 10,000,200.0
// ---------------------------------------------------------------------------

#[test]
fn test_nested_loop_join_expensive_for_large() {
    let catalog = make_catalog();
    let cost_model = CostModel::new(&catalog);

    let left = PhysicalPlan::SeqScan {
        table: "users".into(),
        alias: None,
        schema: users_schema(),
        predicate: None,
    };
    let right = PhysicalPlan::SeqScan {
        table: "orders".into(),
        alias: None,
        schema: orders_schema(),
        predicate: None,
    };

    let join_schema = users_schema().merge(&orders_schema());

    let nl_join = PhysicalPlan::NestedLoopJoin {
        join_type: JoinType::Inner,
        condition: Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier("id".into())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Identifier("user_id".into())),
        }),
        left: Box::new(left),
        right: Box::new(right),
        schema: join_schema,
    };

    let cost = cost_model.estimate_cost(&nl_join);

    // left_cost + left_rows * right_cost = 200.0 + 10000.0 * 1000.0
    let expected = 200.0 + 10000.0 * 1000.0;
    assert!(
        approx_eq(cost, expected, 1e-6),
        "expected nested loop join cost {}, got {}",
        expected,
        cost
    );

    // Also verify it is indeed very expensive
    assert!(
        cost > 1_000_000.0,
        "nested loop join on large tables should be very expensive, got {}",
        cost
    );
}

// ---------------------------------------------------------------------------
// 5. test_hash_join_cheaper_than_nested_loop
//    For an equi-join, HashJoin cost should be much less than NestedLoopJoin cost
// ---------------------------------------------------------------------------

#[test]
fn test_hash_join_cheaper_than_nested_loop() {
    let catalog = make_catalog();
    let cost_model = CostModel::new(&catalog);

    let users_left = PhysicalPlan::SeqScan {
        table: "users".into(),
        alias: None,
        schema: users_schema(),
        predicate: None,
    };
    let orders_right = PhysicalPlan::SeqScan {
        table: "orders".into(),
        alias: None,
        schema: orders_schema(),
        predicate: None,
    };

    let join_schema = users_schema().merge(&orders_schema());

    let hash_join = PhysicalPlan::HashJoin {
        join_type: JoinType::Inner,
        left_keys: vec![Expr::Identifier("id".into())],
        right_keys: vec![Expr::Identifier("user_id".into())],
        condition: None,
        left: Box::new(users_left.clone()),
        right: Box::new(orders_right.clone()),
        schema: join_schema.clone(),
    };

    let nl_join = PhysicalPlan::NestedLoopJoin {
        join_type: JoinType::Inner,
        condition: Some(Expr::BinaryOp {
            left: Box::new(Expr::Identifier("id".into())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Identifier("user_id".into())),
        }),
        left: Box::new(users_left),
        right: Box::new(orders_right),
        schema: join_schema,
    };

    let hash_cost = cost_model.estimate_cost(&hash_join);
    let nl_cost = cost_model.estimate_cost(&nl_join);

    assert!(
        hash_cost < nl_cost,
        "hash join ({}) should be cheaper than nested loop join ({}) for equi-join",
        hash_cost,
        nl_cost
    );
}

// ---------------------------------------------------------------------------
// 6. test_sort_cost
//    ExternalSort on SeqScan of "users"
//    input_cost = 200.0
//    sort_cost = sort_factor(1.0) * cpu_tuple_cost(0.01) * n(10000) * log2(10000)
//    log2(10000) ≈ 13.2877...
//    sort_cost ≈ 1.0 * 0.01 * 10000 * 13.2877 ≈ 1328.77
//    total ≈ 200.0 + 1328.77 = 1528.77
// ---------------------------------------------------------------------------

#[test]
fn test_sort_cost() {
    let catalog = make_catalog();
    let cost_model = CostModel::new(&catalog);

    let input = PhysicalPlan::SeqScan {
        table: "users".into(),
        alias: None,
        schema: users_schema(),
        predicate: None,
    };

    let sort_plan = PhysicalPlan::ExternalSort {
        order_by: vec![SortExpr {
            expr: Expr::Identifier("name".into()),
            ascending: true,
            nulls_first: false,
        }],
        input: Box::new(input),
    };

    let cost = cost_model.estimate_cost(&sort_plan);

    let n = 10000.0_f64;
    let input_cost = 200.0_f64;
    let sort_part = 1.0 * 0.01 * n * n.log2();
    let expected = input_cost + sort_part;

    assert!(
        approx_eq(cost, expected, 1e-9),
        "expected sort cost {}, got {}",
        expected,
        cost
    );
}

// ---------------------------------------------------------------------------
// 7. test_hash_aggregate_cost
//    HashAggregate on SeqScan of "users"
//    input_cost = 200.0
//    hash_cost = hash_build_cost(0.02) * rows(10000) = 200.0
//    total = 400.0
// ---------------------------------------------------------------------------

#[test]
fn test_hash_aggregate_cost() {
    let catalog = make_catalog();
    let cost_model = CostModel::new(&catalog);

    let input = PhysicalPlan::SeqScan {
        table: "users".into(),
        alias: None,
        schema: users_schema(),
        predicate: None,
    };

    let agg_schema = Schema::new(vec![
        Column::new("age", DataType::Integer, true),
        Column::new("count", DataType::BigInt, false),
    ]);

    let agg_plan = PhysicalPlan::HashAggregate {
        group_by: vec![Expr::Identifier("age".into())],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Count,
            arg: Expr::Star,
            distinct: false,
            alias: "count".into(),
        }],
        input: Box::new(input),
        schema: agg_schema,
    };

    let cost = cost_model.estimate_cost(&agg_plan);

    // input_cost + hash_build_cost * rows = 200.0 + 0.02 * 10000 = 400.0
    assert!(
        approx_eq(cost, 400.0, 1e-9),
        "expected hash aggregate cost 400.0, got {}",
        cost
    );
}

// ---------------------------------------------------------------------------
// 8. test_hash_aggregate_vs_sort_aggregate
//    Compare HashAggregate and SortAggregate costs for unordered input.
//    HashAggregate: input_cost + hash_build_cost * rows
//    SortAggregate: input_cost + sort_cost(rows) + cpu_tuple_cost * rows
//    For large tables, sort_cost typically dominates making SortAggregate more expensive.
// ---------------------------------------------------------------------------

#[test]
fn test_hash_aggregate_vs_sort_aggregate() {
    let catalog = make_catalog();
    let cost_model = CostModel::new(&catalog);

    let agg_schema = Schema::new(vec![
        Column::new("age", DataType::Integer, true),
        Column::new("count", DataType::BigInt, false),
    ]);

    let hash_agg = PhysicalPlan::HashAggregate {
        group_by: vec![Expr::Identifier("age".into())],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Count,
            arg: Expr::Star,
            distinct: false,
            alias: "count".into(),
        }],
        input: Box::new(PhysicalPlan::SeqScan {
            table: "users".into(),
            alias: None,
            schema: users_schema(),
            predicate: None,
        }),
        schema: agg_schema.clone(),
    };

    let sort_agg = PhysicalPlan::SortAggregate {
        group_by: vec![Expr::Identifier("age".into())],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Count,
            arg: Expr::Star,
            distinct: false,
            alias: "count".into(),
        }],
        input: Box::new(PhysicalPlan::SeqScan {
            table: "users".into(),
            alias: None,
            schema: users_schema(),
            predicate: None,
        }),
        schema: agg_schema,
    };

    let hash_cost = cost_model.estimate_cost(&hash_agg);
    let sort_cost = cost_model.estimate_cost(&sort_agg);

    // HashAggregate: 200.0 + 0.02*10000 = 400.0
    // SortAggregate: 200.0 + sort_cost(10000) + 0.01*10000
    //   sort_cost(10000) = 1.0 * 0.01 * 10000 * log2(10000) ≈ 1328.77
    //   total ≈ 200.0 + 1328.77 + 100.0 = 1628.77
    assert!(
        hash_cost < sort_cost,
        "HashAggregate ({}) should be cheaper than SortAggregate ({}) for unordered input",
        hash_cost,
        sort_cost
    );
}

// ---------------------------------------------------------------------------
// 9. test_values_cost
//    Values with 5 rows → 5 * cpu_tuple_cost(0.01) = 0.05
// ---------------------------------------------------------------------------

#[test]
fn test_values_cost() {
    let catalog = make_catalog();
    let cost_model = CostModel::new(&catalog);

    let plan = PhysicalPlan::Values {
        rows: vec![
            vec![Expr::Integer(1), Expr::String("a".into()), Expr::Integer(20)],
            vec![Expr::Integer(2), Expr::String("b".into()), Expr::Integer(25)],
            vec![Expr::Integer(3), Expr::String("c".into()), Expr::Integer(30)],
            vec![Expr::Integer(4), Expr::String("d".into()), Expr::Integer(35)],
            vec![Expr::Integer(5), Expr::String("e".into()), Expr::Integer(40)],
        ],
        schema: users_schema(),
    };

    let cost = cost_model.estimate_cost(&plan);

    // 5 rows * cpu_tuple_cost(0.01) = 0.05
    assert!(
        approx_eq(cost, 0.05, 1e-9),
        "expected values cost 0.05, got {}",
        cost
    );
}

// ---------------------------------------------------------------------------
// 10. test_empty_cost
//     Empty → 0.0
// ---------------------------------------------------------------------------

#[test]
fn test_empty_cost() {
    let catalog = make_catalog();
    let cost_model = CostModel::new(&catalog);

    let plan = PhysicalPlan::Empty {
        schema: Schema::empty(),
    };

    let cost = cost_model.estimate_cost(&plan);
    assert_eq!(cost, 0.0, "Empty plan should have zero cost");
}

// ---------------------------------------------------------------------------
// 11. test_custom_constants
//     Use CostModel::with_constants with modified seq_page_cost and verify
//     different (higher) cost for SeqScan.
// ---------------------------------------------------------------------------

#[test]
fn test_custom_constants() {
    let catalog = make_catalog();

    let default_model = CostModel::new(&catalog);
    let custom_constants = CostConstants {
        seq_page_cost: 10.0, // 10x more expensive than default 1.0
        ..CostConstants::default()
    };
    let custom_model = CostModel::with_constants(&catalog, custom_constants);

    let plan = PhysicalPlan::SeqScan {
        table: "users".into(),
        alias: None,
        schema: users_schema(),
        predicate: None,
    };

    let default_cost = default_model.estimate_cost(&plan);
    let custom_cost = custom_model.estimate_cost(&plan);

    // Default: 1.0*100 + 0.01*10000 = 200.0
    // Custom:  10.0*100 + 0.01*10000 = 1100.0
    assert!(
        approx_eq(default_cost, 200.0, 1e-9),
        "expected default cost 200.0, got {}",
        default_cost
    );
    assert!(
        approx_eq(custom_cost, 1100.0, 1e-9),
        "expected custom cost 1100.0, got {}",
        custom_cost
    );
    assert!(
        custom_cost > default_cost,
        "custom seq_page_cost=10.0 should produce higher cost than default: custom={}, default={}",
        custom_cost,
        default_cost
    );
}
