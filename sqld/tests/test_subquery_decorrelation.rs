use sqld::planner::logical_plan::*;
use sqld::planner::rules::subquery_decorrelation::SubqueryDecorrelation;
use sqld::planner::rules::OptimizationRule;
use sqld::sql::ast::*;
use sqld::types::{Column, DataType, Schema};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn scan(table: &str) -> LogicalPlan {
    LogicalPlan::Scan {
        table: table.to_string(),
        alias: None,
        schema: Schema::empty(),
    }
}

fn col(name: &str) -> Expr {
    Expr::Identifier(name.to_string())
}

fn eq(l: Expr, r: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(l),
        op: BinaryOp::Eq,
        right: Box::new(r),
    }
}

fn orders_subquery() -> Select {
    Select {
        distinct: false,
        columns: vec![SelectColumn::Expr {
            expr: Expr::Identifier("user_id".into()),
            alias: None,
        }],
        from: Some(FromClause {
            table: TableRef::Table {
                name: "orders".into(),
                alias: None,
            },
            joins: vec![],
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        offset: None,
        set_op: None,
    }
}

fn orders_subquery_with_where(condition: Expr) -> Select {
    Select {
        distinct: false,
        columns: vec![SelectColumn::AllColumns],
        from: Some(FromClause {
            table: TableRef::Table {
                name: "orders".into(),
                alias: None,
            },
            joins: vec![],
        }),
        where_clause: Some(condition),
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        offset: None,
        set_op: None,
    }
}

fn contains_node(plan: &LogicalPlan, name: &str) -> bool {
    if plan.node_name() == name {
        return true;
    }
    plan.children().iter().any(|c| contains_node(c, name))
}

fn contains_in_subquery(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Filter { predicate, input } => {
            expr_has_in_subquery(predicate) || contains_in_subquery(input)
        }
        other => other.children().iter().any(|c| contains_in_subquery(c)),
    }
}

fn expr_has_in_subquery(expr: &Expr) -> bool {
    match expr {
        Expr::InSubquery { .. } => true,
        Expr::BinaryOp { left, right, .. } => {
            expr_has_in_subquery(left) || expr_has_in_subquery(right)
        }
        Expr::UnaryOp { expr, .. } => expr_has_in_subquery(expr),
        _ => false,
    }
}

fn contains_exists(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Filter { predicate, input } => {
            expr_has_exists(predicate) || contains_exists(input)
        }
        other => other.children().iter().any(|c| contains_exists(c)),
    }
}

fn expr_has_exists(expr: &Expr) -> bool {
    match expr {
        Expr::Exists { .. } => true,
        Expr::BinaryOp { left, right, .. } => {
            expr_has_exists(left) || expr_has_exists(right)
        }
        Expr::UnaryOp { expr, .. } => expr_has_exists(expr),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// 1. IN subquery → inner join (semi-join)
// ---------------------------------------------------------------------------

#[test]
fn test_in_subquery_to_join() {
    // Filter: id IN (SELECT user_id FROM orders)
    let plan = LogicalPlan::Filter {
        predicate: Expr::InSubquery {
            expr: Box::new(col("id")),
            subquery: Box::new(orders_subquery()),
            negated: false,
        },
        input: Box::new(scan("users")),
    };

    let optimized = SubqueryDecorrelation.apply(plan);

    // InSubquery should be gone and a Join node should appear
    assert!(
        !contains_in_subquery(&optimized),
        "InSubquery expression should have been removed from the plan"
    );
    assert!(
        contains_node(&optimized, "Join"),
        "expected a Join node in the optimized plan, got {:?}",
        optimized.node_name()
    );
    assert!(
        matches!(optimized, LogicalPlan::Join { join_type: JoinType::Inner, .. }),
        "IN subquery should become an Inner join, got {:?}",
        optimized.node_name()
    );
}

// ---------------------------------------------------------------------------
// 2. NOT IN subquery → anti-join: left join + IS NULL filter
// ---------------------------------------------------------------------------

#[test]
fn test_not_in_subquery_to_anti_join() {
    // Filter: id NOT IN (SELECT user_id FROM orders)
    let plan = LogicalPlan::Filter {
        predicate: Expr::InSubquery {
            expr: Box::new(col("id")),
            subquery: Box::new(orders_subquery()),
            negated: true,
        },
        input: Box::new(scan("users")),
    };

    let optimized = SubqueryDecorrelation.apply(plan);

    // InSubquery should be gone
    assert!(
        !contains_in_subquery(&optimized),
        "InSubquery expression should have been removed from the plan"
    );

    // Should be Filter(IS NULL) over Join(Left)
    match &optimized {
        LogicalPlan::Filter { predicate, input } => {
            assert!(
                matches!(predicate, Expr::IsNull { negated: false, .. }),
                "outer filter predicate should be IS NULL, got {:?}",
                predicate
            );
            assert!(
                matches!(input.as_ref(), LogicalPlan::Join { join_type: JoinType::Left, .. }),
                "inner node should be a Left join for anti-join, got {:?}",
                input.node_name()
            );
        }
        other => panic!(
            "expected Filter(IS NULL) -> Left Join for NOT IN, got {:?}",
            other.node_name()
        ),
    }
}

// ---------------------------------------------------------------------------
// 3. EXISTS → inner join (semi-join)
// ---------------------------------------------------------------------------

#[test]
fn test_exists_to_join() {
    // Filter: EXISTS (SELECT * FROM orders WHERE orders.user_id = users.id)
    let where_cond = eq(col("orders.user_id"), col("users.id"));
    let plan = LogicalPlan::Filter {
        predicate: Expr::Exists {
            subquery: Box::new(orders_subquery_with_where(where_cond)),
            negated: false,
        },
        input: Box::new(scan("users")),
    };

    let optimized = SubqueryDecorrelation.apply(plan);

    // EXISTS should be gone and a Join node should appear
    assert!(
        !contains_exists(&optimized),
        "Exists expression should have been removed from the plan"
    );
    assert!(
        contains_node(&optimized, "Join"),
        "expected a Join node in the optimized plan"
    );
    assert!(
        matches!(optimized, LogicalPlan::Join { join_type: JoinType::Inner, .. }),
        "EXISTS should become an Inner join, got {:?}",
        optimized.node_name()
    );
}

// ---------------------------------------------------------------------------
// 4. NOT EXISTS → anti-join: left join + IS NULL filter
// ---------------------------------------------------------------------------

#[test]
fn test_not_exists_to_anti_join() {
    // Filter: NOT EXISTS (SELECT * FROM orders WHERE orders.user_id = users.id)
    let where_cond = eq(col("orders.user_id"), col("users.id"));
    let plan = LogicalPlan::Filter {
        predicate: Expr::Exists {
            subquery: Box::new(orders_subquery_with_where(where_cond)),
            negated: true,
        },
        input: Box::new(scan("users")),
    };

    let optimized = SubqueryDecorrelation.apply(plan);

    // Exists should be gone
    assert!(
        !contains_exists(&optimized),
        "Exists expression should have been removed from the plan"
    );

    // Should be Filter(IS NULL) over Join(Left)
    match &optimized {
        LogicalPlan::Filter { predicate, input } => {
            assert!(
                matches!(predicate, Expr::IsNull { negated: false, .. }),
                "outer filter predicate should be IS NULL, got {:?}",
                predicate
            );
            assert!(
                matches!(input.as_ref(), LogicalPlan::Join { join_type: JoinType::Left, .. }),
                "inner node should be a Left join for anti-join, got {:?}",
                input.node_name()
            );
        }
        other => panic!(
            "expected Filter(IS NULL) -> Left Join for NOT EXISTS, got {:?}",
            other.node_name()
        ),
    }
}

// ---------------------------------------------------------------------------
// 5. Regular filter (no subquery) passes through unchanged
// ---------------------------------------------------------------------------

#[test]
fn test_regular_filter_preserved() {
    // Filter: age > 18  (a plain predicate with no subquery)
    let predicate = Expr::BinaryOp {
        left: Box::new(col("age")),
        op: BinaryOp::Gt,
        right: Box::new(Expr::Integer(18)),
    };

    let plan = LogicalPlan::Filter {
        predicate: predicate.clone(),
        input: Box::new(scan("users")),
    };

    let optimized = SubqueryDecorrelation.apply(plan);

    // The plan should still be a Filter (no conversion occurred)
    match &optimized {
        LogicalPlan::Filter { predicate: out_pred, input } => {
            assert_eq!(
                *out_pred, predicate,
                "predicate should be unchanged for a regular filter"
            );
            assert!(
                matches!(input.as_ref(), LogicalPlan::Scan { table, .. } if table == "users"),
                "input should still be the users scan"
            );
        }
        other => panic!(
            "regular Filter should pass through unchanged, got {:?}",
            other.node_name()
        ),
    }

    // No join should have been introduced
    assert!(
        !contains_node(&optimized, "Join"),
        "no join should appear for a plain filter predicate"
    );
}

// ---------------------------------------------------------------------------
// 6. Mixed predicates: age > 18 AND id IN (SELECT user_id FROM orders)
// ---------------------------------------------------------------------------

#[test]
fn test_mixed_predicates() {
    // Filter: age > 18 AND id IN (SELECT user_id FROM orders)
    let age_pred = Expr::BinaryOp {
        left: Box::new(col("age")),
        op: BinaryOp::Gt,
        right: Box::new(Expr::Integer(18)),
    };
    let in_pred = Expr::InSubquery {
        expr: Box::new(col("id")),
        subquery: Box::new(orders_subquery()),
        negated: false,
    };
    let combined = Expr::BinaryOp {
        left: Box::new(age_pred.clone()),
        op: BinaryOp::And,
        right: Box::new(in_pred),
    };

    let plan = LogicalPlan::Filter {
        predicate: combined,
        input: Box::new(scan("users")),
    };

    let optimized = SubqueryDecorrelation.apply(plan);

    // The IN subquery part should have been converted to a Join
    assert!(
        !contains_in_subquery(&optimized),
        "InSubquery expression should have been removed"
    );
    assert!(
        contains_node(&optimized, "Join"),
        "the IN subquery should have been converted to a Join"
    );

    // The age > 18 predicate should remain as a Filter wrapping the join
    match &optimized {
        LogicalPlan::Filter { predicate, .. } => {
            assert_eq!(
                *predicate, age_pred,
                "the remaining filter predicate should be age > 18"
            );
        }
        other => panic!(
            "expected a Filter node wrapping the join for the age > 18 predicate, got {:?}",
            other.node_name()
        ),
    }
}
