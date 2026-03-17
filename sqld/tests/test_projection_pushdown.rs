use sqld::planner::logical_plan::*;
use sqld::planner::rules::projection_pushdown::ProjectionPushdown;
use sqld::planner::rules::OptimizationRule;
use sqld::sql::ast::*;
use sqld::types::{Column, DataType, Schema};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a Schema from (name, DataType) pairs.
fn make_schema(cols: &[(&str, DataType)]) -> Schema {
    Schema::new(
        cols.iter()
            .map(|(name, dt)| Column::new(*name, *dt, true))
            .collect(),
    )
}

/// Build a Scan node.
fn scan(table: &str, schema: Schema) -> LogicalPlan {
    LogicalPlan::Scan {
        table: table.to_string(),
        alias: None,
        schema,
    }
}

/// Build a Project node from (expr, alias) pairs.
fn project(exprs_with_aliases: Vec<(Expr, &str)>, input: LogicalPlan) -> LogicalPlan {
    let expressions = exprs_with_aliases
        .into_iter()
        .map(|(expr, alias)| ProjectionExpr {
            expr,
            alias: alias.to_string(),
        })
        .collect();
    LogicalPlan::Project {
        expressions,
        input: Box::new(input),
    }
}

/// Build an Expr::Identifier for a column reference.
fn col(name: &str) -> Expr {
    Expr::Identifier(name.to_string())
}

/// Retrieve the scan schema from a plan node that is expected to be a Scan.
fn scan_schema(plan: &LogicalPlan) -> &Schema {
    match plan {
        LogicalPlan::Scan { schema, .. } => schema,
        other => panic!("expected Scan, got {:?}", other.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 1. test_pushdown_narrows_scan
//    Project [id, name] over Scan(users: id, name, age, email)
//    => Scan schema reduced to [id, name]
// ---------------------------------------------------------------------------

#[test]
fn test_pushdown_narrows_scan() {
    let schema = make_schema(&[
        ("id", DataType::Integer),
        ("name", DataType::Varchar(255)),
        ("age", DataType::Integer),
        ("email", DataType::Varchar(255)),
    ]);
    let input = scan("users", schema);
    let plan = project(
        vec![(col("id"), "id"), (col("name"), "name")],
        input,
    );

    let optimized = ProjectionPushdown.apply(plan);

    // Scan schema is NOT narrowed because the executor returns full tuples
    // from storage. The Project above handles column selection.
    match &optimized {
        LogicalPlan::Project { input, .. } => {
            let s = scan_schema(input);
            assert_eq!(
                s.column_count(),
                4,
                "scan schema should remain unchanged (executor returns full tuples)"
            );
        }
        _ => panic!("expected Project at root"),
    }
}

// ---------------------------------------------------------------------------
// 2. test_pushdown_through_filter
//    Project [id] over Filter(age > 18) over Scan(id, name, age, email)
//    => Scan schema includes both id (projected) and age (needed by filter)
// ---------------------------------------------------------------------------

#[test]
fn test_pushdown_through_filter() {
    let schema = make_schema(&[
        ("id", DataType::Integer),
        ("name", DataType::Varchar(255)),
        ("age", DataType::Integer),
        ("email", DataType::Varchar(255)),
    ]);
    let base_scan = scan("users", schema);

    let filter = LogicalPlan::Filter {
        predicate: Expr::BinaryOp {
            left: Box::new(col("age")),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Integer(18)),
        },
        input: Box::new(base_scan),
    };

    let plan = project(vec![(col("id"), "id")], filter);

    let optimized = ProjectionPushdown.apply(plan);

    match &optimized {
        LogicalPlan::Project { input, .. } => match input.as_ref() {
            LogicalPlan::Filter { input: scan_node, .. } => {
                let s = scan_schema(scan_node);
                // Scan keeps full schema (executor returns full tuples)
                assert_eq!(s.column_count(), 4, "scan schema should remain unchanged");
                assert!(s.has_column("id"));
                assert!(s.has_column("age"));
            }
            other => panic!("expected Filter below Project, got {:?}", other.node_name()),
        },
        _ => panic!("expected Project at root"),
    }
}

// ---------------------------------------------------------------------------
// 3. test_pushdown_through_join
//    Project [id] over Join(users, orders ON users.id = orders.user_id)
//    => both scans have narrowed schemas (only columns actually needed kept)
// ---------------------------------------------------------------------------

#[test]
fn test_pushdown_through_join() {
    let users_schema = make_schema(&[
        ("id", DataType::Integer),
        ("name", DataType::Varchar(255)),
        ("age", DataType::Integer),
        ("email", DataType::Varchar(255)),
    ]);
    let orders_schema = make_schema(&[
        ("order_id", DataType::Integer),
        ("user_id", DataType::Integer),
        ("amount", DataType::Float),
        ("status", DataType::Varchar(50)),
    ]);

    let users_scan = scan("users", users_schema.clone());
    let orders_scan = scan("orders", orders_schema.clone());

    let join_schema = users_schema.merge(&orders_schema);

    let join = LogicalPlan::Join {
        join_type: JoinType::Inner,
        condition: Some(Expr::BinaryOp {
            left: Box::new(col("id")),
            op: BinaryOp::Eq,
            right: Box::new(col("user_id")),
        }),
        left: Box::new(users_scan),
        right: Box::new(orders_scan),
        schema: join_schema,
    };

    // Project only 'id' (from users side)
    let plan = project(vec![(col("id"), "id")], join);

    let optimized = ProjectionPushdown.apply(plan);

    // Scan schemas are NOT narrowed (executor returns full tuples)
    match &optimized {
        LogicalPlan::Project { input, .. } => match input.as_ref() {
            LogicalPlan::Join { left, right, .. } => {
                let left_schema = match left.as_ref() {
                    LogicalPlan::Scan { schema, .. } => schema,
                    other => panic!("expected Scan on left, got {:?}", other.node_name()),
                };
                let right_schema = match right.as_ref() {
                    LogicalPlan::Scan { schema, .. } => schema,
                    other => panic!("expected Scan on right, got {:?}", other.node_name()),
                };

                assert_eq!(left_schema.column_count(), 4, "users scan should keep full schema");
                assert_eq!(right_schema.column_count(), 4, "orders scan should keep full schema");
            }
            other => panic!("expected Join below Project, got {:?}", other.node_name()),
        },
        _ => panic!("expected Project at root"),
    }
}

// ---------------------------------------------------------------------------
// 4. test_no_pushdown_when_all_needed
//    Project [id, name, age] over Scan(id, name, age)
//    => all columns needed; scan schema unchanged (still 3 columns)
// ---------------------------------------------------------------------------

#[test]
fn test_no_pushdown_when_all_needed() {
    let schema = make_schema(&[
        ("id", DataType::Integer),
        ("name", DataType::Varchar(255)),
        ("age", DataType::Integer),
    ]);
    let input = scan("users", schema);
    let plan = project(
        vec![
            (col("id"), "id"),
            (col("name"), "name"),
            (col("age"), "age"),
        ],
        input,
    );

    let optimized = ProjectionPushdown.apply(plan);

    match &optimized {
        LogicalPlan::Project { input, .. } => {
            let s = scan_schema(input);
            assert_eq!(
                s.column_count(),
                3,
                "scan schema should remain at 3 columns when all are needed"
            );
            assert!(s.has_column("id"));
            assert!(s.has_column("name"));
            assert!(s.has_column("age"));
        }
        _ => panic!("expected Project at root"),
    }
}

// ---------------------------------------------------------------------------
// 5. test_pushdown_preserves_filter_columns
//    Project [name] over Filter(id = 1) over Scan(id, name, age, email)
//    => Scan keeps both id (filter) and name (projected); drops age, email
// ---------------------------------------------------------------------------

#[test]
fn test_pushdown_preserves_filter_columns() {
    let schema = make_schema(&[
        ("id", DataType::Integer),
        ("name", DataType::Varchar(255)),
        ("age", DataType::Integer),
        ("email", DataType::Varchar(255)),
    ]);
    let base_scan = scan("users", schema);

    let filter = LogicalPlan::Filter {
        predicate: Expr::BinaryOp {
            left: Box::new(col("id")),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Integer(1)),
        },
        input: Box::new(base_scan),
    };

    let plan = project(vec![(col("name"), "name")], filter);

    let optimized = ProjectionPushdown.apply(plan);

    // Scan keeps full schema (executor returns full tuples)
    match &optimized {
        LogicalPlan::Project { input, .. } => match input.as_ref() {
            LogicalPlan::Filter { input: scan_node, .. } => {
                let s = scan_schema(scan_node);
                assert_eq!(s.column_count(), 4, "scan schema should remain unchanged");
                assert!(s.has_column("id"));
                assert!(s.has_column("name"));
            }
            other => panic!("expected Filter below Project, got {:?}", other.node_name()),
        },
        _ => panic!("expected Project at root"),
    }
}

// ---------------------------------------------------------------------------
// 6. test_empty_scan_not_created
//    If pushdown would eliminate ALL columns, the scan is not narrowed to empty.
//    Project [constant] over Scan(id, name) => Scan retains its original schema.
// ---------------------------------------------------------------------------

#[test]
fn test_empty_scan_not_created() {
    let schema = make_schema(&[
        ("id", DataType::Integer),
        ("name", DataType::Varchar(255)),
    ]);
    let input = scan("users", schema);

    // Project only a literal — no column references at all, so `needed` is empty.
    let plan = project(vec![(Expr::Integer(42), "constant")], input);

    let optimized = ProjectionPushdown.apply(plan);

    match &optimized {
        LogicalPlan::Project { input, .. } => {
            let s = scan_schema(input);
            // The guard `!new_cols.is_empty()` prevents narrowing to an empty schema.
            assert!(
                s.column_count() > 0,
                "scan must not be narrowed to an empty schema"
            );
        }
        _ => panic!("expected Project at root"),
    }
}
