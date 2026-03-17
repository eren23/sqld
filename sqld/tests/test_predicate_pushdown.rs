use sqld::planner::logical_plan::*;
use sqld::planner::rules::predicate_pushdown::PredicatePushdown;
use sqld::planner::rules::OptimizationRule;
use sqld::planner::Catalog;
use sqld::sql::ast::*;
use sqld::types::{Column, DataType, Schema};

// ---------------------------------------------------------------------------
// Helpers
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
    catalog.add_table(
        "orders",
        Schema::new(vec![
            Column::new("id", DataType::Integer, false),
            Column::new("user_id", DataType::Integer, false),
            Column::new("amount", DataType::Float, true),
        ]),
    );
    catalog
}

fn scan(table: &str, catalog: &Catalog) -> LogicalPlan {
    let schema = catalog.get_schema(table).cloned().unwrap_or_else(Schema::empty);
    LogicalPlan::Scan {
        table: table.to_string(),
        alias: None,
        schema,
    }
}

fn filter(pred: Expr, input: LogicalPlan) -> LogicalPlan {
    LogicalPlan::Filter {
        predicate: pred,
        input: Box::new(input),
    }
}

fn col(name: &str) -> Expr {
    Expr::Identifier(name.to_string())
}

fn qcol(table: &str, column: &str) -> Expr {
    Expr::QualifiedIdentifier {
        table: table.to_string(),
        column: column.to_string(),
    }
}

fn eq(l: Expr, r: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(l),
        op: BinaryOp::Eq,
        right: Box::new(r),
    }
}

fn gt(l: Expr, r: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(l),
        op: BinaryOp::Gt,
        right: Box::new(r),
    }
}

fn lit_int(n: i64) -> Expr {
    Expr::Integer(n)
}

fn and(l: Expr, r: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(l),
        op: BinaryOp::And,
        right: Box::new(r),
    }
}

/// Build a trivial join schema from two scan schemas.
fn join_schema(left: &LogicalPlan, right: &LogicalPlan) -> Schema {
    left.schema().merge(&right.schema())
}

/// Count Filter nodes in the whole plan tree.
fn count_filters(plan: &LogicalPlan) -> usize {
    let self_count = if matches!(plan, LogicalPlan::Filter { .. }) { 1 } else { 0 };
    self_count + plan.children().iter().map(|c| count_filters(c)).sum::<usize>()
}

// ---------------------------------------------------------------------------
// 1. Filter on top of Project pushes below it
// ---------------------------------------------------------------------------

#[test]
fn test_push_filter_through_project() {
    let catalog = make_catalog();

    // Build: Filter(age > 30, Project([id, age], Scan(users)))
    let users_scan = scan("users", &catalog);
    let project = LogicalPlan::Project {
        expressions: vec![
            ProjectionExpr { expr: col("id"), alias: "id".to_string() },
            ProjectionExpr { expr: col("age"), alias: "age".to_string() },
        ],
        input: Box::new(users_scan),
    };
    let plan = filter(gt(col("age"), lit_int(30)), project);

    let optimized = PredicatePushdown.apply(plan);

    // After pushdown: Project should be on top, Filter below it (adjacent to Scan)
    match &optimized {
        LogicalPlan::Project { input, .. } => {
            assert!(
                matches!(input.as_ref(), LogicalPlan::Filter { .. }),
                "expected Filter directly below Project, got {}",
                input.node_name()
            );
            // The filter should sit above the scan
            match input.as_ref() {
                LogicalPlan::Filter { input: scan_node, .. } => {
                    assert!(
                        matches!(scan_node.as_ref(), LogicalPlan::Scan { .. }),
                        "expected Scan below Filter"
                    );
                }
                _ => unreachable!(),
            }
        }
        _ => panic!("expected Project at root, got {}", optimized.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 2. Filter referencing only left table pushes to left of inner join
// ---------------------------------------------------------------------------

#[test]
fn test_push_filter_into_inner_join_left() {
    let catalog = make_catalog();

    let users = scan("users", &catalog);
    let orders = scan("orders", &catalog);
    let schema = join_schema(&users, &orders);

    // Join(users, orders, users.id = orders.user_id)
    let join = LogicalPlan::Join {
        join_type: JoinType::Inner,
        condition: Some(eq(qcol("users", "id"), qcol("orders", "user_id"))),
        left: Box::new(users),
        right: Box::new(orders),
        schema,
    };

    // Filter: users.age > 18  (only references left side)
    let plan = filter(gt(qcol("users", "age"), lit_int(18)), join);

    let optimized = PredicatePushdown.apply(plan);

    // Expect: Join -> (Filter -> Scan(users), Scan(orders))
    match &optimized {
        LogicalPlan::Join { left, right, .. } => {
            assert!(
                matches!(left.as_ref(), LogicalPlan::Filter { .. }),
                "expected Filter pushed to left side, got {}",
                left.node_name()
            );
            assert!(
                !matches!(right.as_ref(), LogicalPlan::Filter { .. }),
                "right side should NOT have a filter, got {}",
                right.node_name()
            );
        }
        _ => panic!("expected Join at root, got {}", optimized.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 3. Filter referencing only right table pushes to right of inner join
// ---------------------------------------------------------------------------

#[test]
fn test_push_filter_into_inner_join_right() {
    let catalog = make_catalog();

    let users = scan("users", &catalog);
    let orders = scan("orders", &catalog);
    let schema = join_schema(&users, &orders);

    let join = LogicalPlan::Join {
        join_type: JoinType::Inner,
        condition: Some(eq(qcol("users", "id"), qcol("orders", "user_id"))),
        left: Box::new(users),
        right: Box::new(orders),
        schema,
    };

    // Filter: orders.amount > 100.0 (only references right side)
    let plan = filter(gt(qcol("orders", "amount"), lit_int(100)), join);

    let optimized = PredicatePushdown.apply(plan);

    match &optimized {
        LogicalPlan::Join { left, right, .. } => {
            assert!(
                !matches!(left.as_ref(), LogicalPlan::Filter { .. }),
                "left side should NOT have a filter, got {}",
                left.node_name()
            );
            assert!(
                matches!(right.as_ref(), LogicalPlan::Filter { .. }),
                "expected Filter pushed to right side, got {}",
                right.node_name()
            );
        }
        _ => panic!("expected Join at root, got {}", optimized.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 4. Cross-table predicate becomes join condition
// ---------------------------------------------------------------------------

#[test]
fn test_filter_referencing_both_sides_stays_as_join_condition() {
    let catalog = make_catalog();

    let users = scan("users", &catalog);
    let orders = scan("orders", &catalog);
    let schema = join_schema(&users, &orders);

    // Join with no initial condition (cross join style)
    let join = LogicalPlan::Join {
        join_type: JoinType::Inner,
        condition: None,
        left: Box::new(users),
        right: Box::new(orders),
        schema,
    };

    // Filter referencing both: users.id = orders.user_id
    let pred = eq(qcol("users", "id"), qcol("orders", "user_id"));
    let plan = filter(pred, join);

    let optimized = PredicatePushdown.apply(plan);

    // The cross-table predicate should be folded into the join condition,
    // and neither child should receive a standalone filter.
    match &optimized {
        LogicalPlan::Join { condition, left, right, .. } => {
            assert!(
                condition.is_some(),
                "cross-table predicate should become the join condition"
            );
            assert!(
                !matches!(left.as_ref(), LogicalPlan::Filter { .. }),
                "left should not have a filter"
            );
            assert!(
                !matches!(right.as_ref(), LogicalPlan::Filter { .. }),
                "right should not have a filter"
            );
        }
        _ => panic!("expected Join at root, got {}", optimized.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 5. Filter pushes below Sort
// ---------------------------------------------------------------------------

#[test]
fn test_push_through_sort() {
    let catalog = make_catalog();

    let users = scan("users", &catalog);
    let sort = LogicalPlan::Sort {
        order_by: vec![SortExpr {
            expr: col("name"),
            ascending: true,
            nulls_first: false,
        }],
        input: Box::new(users),
    };

    let plan = filter(gt(col("age"), lit_int(21)), sort);

    let optimized = PredicatePushdown.apply(plan);

    // Sort should be at the root; Filter should appear below it
    match &optimized {
        LogicalPlan::Sort { input, .. } => {
            assert!(
                count_filters(input) >= 1,
                "Filter should be pushed below Sort"
            );
        }
        _ => panic!("expected Sort at root, got {}", optimized.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 6. Filter above Limit stays — must NOT push below Limit
// ---------------------------------------------------------------------------

#[test]
fn test_filter_above_limit_stays() {
    let catalog = make_catalog();

    let users = scan("users", &catalog);
    let limit = LogicalPlan::Limit {
        count: Some(10),
        offset: 0,
        input: Box::new(users),
    };

    let plan = filter(gt(col("age"), lit_int(18)), limit);

    let optimized = PredicatePushdown.apply(plan);

    // Filter must NOT be pushed below Limit (would change semantics)
    match &optimized {
        LogicalPlan::Filter { input, .. } => {
            assert!(
                matches!(input.as_ref(), LogicalPlan::Limit { .. }),
                "Filter must stay above Limit, but inner node is {}",
                input.node_name()
            );
        }
        _ => panic!(
            "expected Filter to remain at root above Limit, got {}",
            optimized.node_name()
        ),
    }
}

// ---------------------------------------------------------------------------
// 7. Filter on group-by column pushes below Aggregate
// ---------------------------------------------------------------------------

#[test]
fn test_push_group_by_column_through_aggregate() {
    let catalog = make_catalog();

    let orders = scan("orders", &catalog);
    let agg_schema = Schema::new(vec![
        Column::new("user_id", DataType::Integer, false),
        Column::new("total", DataType::Float, true),
    ]);

    // Aggregate: GROUP BY user_id, SUM(amount) AS total
    let agg = LogicalPlan::Aggregate {
        group_by: vec![col("user_id")],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Sum,
            arg: col("amount"),
            distinct: false,
            alias: "total".to_string(),
        }],
        input: Box::new(orders),
        schema: agg_schema,
    };

    // Filter on the group-by column: user_id = 42
    let plan = filter(eq(col("user_id"), lit_int(42)), agg);

    let optimized = PredicatePushdown.apply(plan);

    // The filter on user_id (a group-by key) should be pushed below the Aggregate
    match &optimized {
        LogicalPlan::Aggregate { input, .. } => {
            assert!(
                count_filters(input) >= 1,
                "Filter on group-by column should be pushed below Aggregate"
            );
        }
        _ => panic!(
            "expected Aggregate at root after pushdown, got {}",
            optimized.node_name()
        ),
    }
}

// ---------------------------------------------------------------------------
// 8. Filter on aggregate result stays above Aggregate
// ---------------------------------------------------------------------------

#[test]
fn test_filter_on_aggregate_stays_above() {
    let catalog = make_catalog();

    let orders = scan("orders", &catalog);
    let agg_schema = Schema::new(vec![
        Column::new("user_id", DataType::Integer, false),
        Column::new("total", DataType::Float, true),
    ]);

    let agg = LogicalPlan::Aggregate {
        group_by: vec![col("user_id")],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Sum,
            arg: col("amount"),
            distinct: false,
            alias: "total".to_string(),
        }],
        input: Box::new(orders),
        schema: agg_schema,
    };

    // Filter on the aggregate result column: total > 500
    // "total" is not a group-by key, so this is a HAVING-like predicate
    let plan = filter(gt(col("total"), lit_int(500)), agg);

    let optimized = PredicatePushdown.apply(plan);

    // Filter on aggregate result must remain above the Aggregate
    match &optimized {
        LogicalPlan::Filter { input, .. } => {
            assert!(
                matches!(input.as_ref(), LogicalPlan::Aggregate { .. }),
                "Filter on aggregate result must stay above Aggregate, inner node is {}",
                input.node_name()
            );
        }
        _ => panic!(
            "expected Filter to remain above Aggregate, got {}",
            optimized.node_name()
        ),
    }
}

// ---------------------------------------------------------------------------
// 9. Conjunction splits and each part pushes to the correct join side
// ---------------------------------------------------------------------------

#[test]
fn test_conjunction_split_push() {
    let catalog = make_catalog();

    let users = scan("users", &catalog);
    let orders = scan("orders", &catalog);
    let schema = join_schema(&users, &orders);

    let join = LogicalPlan::Join {
        join_type: JoinType::Inner,
        condition: Some(eq(qcol("users", "id"), qcol("orders", "user_id"))),
        left: Box::new(users),
        right: Box::new(orders),
        schema,
    };

    // WHERE users.age > 18 AND orders.amount > 50
    let pred = and(
        gt(qcol("users", "age"), lit_int(18)),
        gt(qcol("orders", "amount"), lit_int(50)),
    );
    let plan = filter(pred, join);

    let optimized = PredicatePushdown.apply(plan);

    // Both filters should be pushed: one to left, one to right
    match &optimized {
        LogicalPlan::Join { left, right, .. } => {
            assert!(
                matches!(left.as_ref(), LogicalPlan::Filter { .. }),
                "users.age > 18 should push to left side, got {}",
                left.node_name()
            );
            assert!(
                matches!(right.as_ref(), LogicalPlan::Filter { .. }),
                "orders.amount > 50 should push to right side, got {}",
                right.node_name()
            );
        }
        _ => panic!("expected Join at root, got {}", optimized.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 10. LEFT JOIN: left-table predicate pushes down, right-table predicate does not
// ---------------------------------------------------------------------------

#[test]
fn test_left_join_push_left_only() {
    let catalog = make_catalog();

    let users = scan("users", &catalog);
    let orders = scan("orders", &catalog);
    let schema = join_schema(&users, &orders);

    let join = LogicalPlan::Join {
        join_type: JoinType::Left,
        condition: Some(eq(qcol("users", "id"), qcol("orders", "user_id"))),
        left: Box::new(users),
        right: Box::new(orders),
        schema,
    };

    // WHERE users.age > 18 AND orders.amount > 50
    // For a LEFT JOIN:
    //   - users.age > 18 (left side) CAN be pushed down
    //   - orders.amount > 50 (right side) CANNOT be pushed down (would turn LEFT JOIN into INNER JOIN)
    let pred = and(
        gt(qcol("users", "age"), lit_int(18)),
        gt(qcol("orders", "amount"), lit_int(50)),
    );
    let plan = filter(pred, join);

    let optimized = PredicatePushdown.apply(plan);

    match &optimized {
        LogicalPlan::Join { join_type, left, right, condition, .. } => {
            assert_eq!(*join_type, JoinType::Left, "join type should remain Left");

            // Left-side predicate should have been pushed below the join
            assert!(
                matches!(left.as_ref(), LogicalPlan::Filter { .. }),
                "users.age > 18 should push to left side in a LEFT JOIN, got {}",
                left.node_name()
            );

            // Right-side predicate must NOT be pushed below the join; it should
            // be absorbed into the join condition instead.
            assert!(
                !matches!(right.as_ref(), LogicalPlan::Filter { .. }),
                "orders.amount > 50 must NOT push to right side in a LEFT JOIN"
            );

            // The right-side predicate should appear in the join condition
            assert!(
                condition.is_some(),
                "right-side predicate should be merged into join condition"
            );
        }
        _ => panic!("expected Join at root, got {}", optimized.node_name()),
    }
}
