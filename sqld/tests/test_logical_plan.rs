use sqld::planner::logical_plan::*;
use sqld::planner::plan_builder::PlanBuilder;
use sqld::planner::Catalog;
use sqld::sql::parser::parse;
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
            Column::new("name", DataType::Varchar(255), false),
            Column::new("age", DataType::Integer, true),
            Column::new("email", DataType::Text, true),
        ]),
    );

    catalog.add_table(
        "orders",
        Schema::new(vec![
            Column::new("id", DataType::Integer, false),
            Column::new("user_id", DataType::Integer, false),
            Column::new("amount", DataType::Float, false),
            Column::new("status", DataType::Varchar(50), false),
        ]),
    );

    catalog.add_table(
        "products",
        Schema::new(vec![
            Column::new("id", DataType::Integer, false),
            Column::new("name", DataType::Varchar(255), false),
            Column::new("price", DataType::Float, false),
            Column::new("category", DataType::Varchar(100), false),
        ]),
    );

    catalog.add_table(
        "order_items",
        Schema::new(vec![
            Column::new("id", DataType::Integer, false),
            Column::new("order_id", DataType::Integer, false),
            Column::new("product_id", DataType::Integer, false),
            Column::new("quantity", DataType::Integer, false),
        ]),
    );

    catalog
}

fn build_plan(sql: &str) -> LogicalPlan {
    let catalog = make_catalog();
    let builder = PlanBuilder::new(&catalog);
    let result = parse(sql);
    assert!(result.errors.is_empty(), "parse errors: {:?}", result.errors);
    let stmt = result.statements.into_iter().next().unwrap();
    builder.build(&stmt).unwrap()
}

// ---------------------------------------------------------------------------
// 1. test_simple_select — SELECT * FROM users → Project over Scan
// ---------------------------------------------------------------------------

#[test]
fn test_simple_select() {
    let plan = build_plan("SELECT * FROM users");
    match &plan {
        LogicalPlan::Project { input, .. } => {
            assert!(
                matches!(input.as_ref(), LogicalPlan::Scan { .. }),
                "expected Scan under Project, got {}",
                input.node_name()
            );
        }
        other => panic!("expected Project at top level, got {}", other.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 2. test_select_specific_columns — SELECT id, name FROM users → Project with 2 exprs
// ---------------------------------------------------------------------------

#[test]
fn test_select_specific_columns() {
    let plan = build_plan("SELECT id, name FROM users");
    match &plan {
        LogicalPlan::Project { expressions, .. } => {
            assert_eq!(
                expressions.len(),
                2,
                "expected 2 projection expressions, got {}",
                expressions.len()
            );
            assert_eq!(expressions[0].alias, "id");
            assert_eq!(expressions[1].alias, "name");
        }
        other => panic!("expected Project, got {}", other.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 3. test_select_with_where — SELECT * FROM users WHERE age > 18 → Project → Filter → Scan
// ---------------------------------------------------------------------------

#[test]
fn test_select_with_where() {
    let plan = build_plan("SELECT * FROM users WHERE age > 18");
    match &plan {
        LogicalPlan::Project { input, .. } => match input.as_ref() {
            LogicalPlan::Filter { input: scan, .. } => {
                assert!(
                    matches!(scan.as_ref(), LogicalPlan::Scan { .. }),
                    "expected Scan under Filter, got {}",
                    scan.node_name()
                );
            }
            other => panic!("expected Filter under Project, got {}", other.node_name()),
        },
        other => panic!("expected Project at top level, got {}", other.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 4. test_select_with_join — INNER JOIN produces Join with Inner type
// ---------------------------------------------------------------------------

#[test]
fn test_select_with_join() {
    let plan = build_plan(
        "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id",
    );
    match &plan {
        LogicalPlan::Project { input, .. } => match input.as_ref() {
            LogicalPlan::Join { join_type, condition, .. } => {
                assert_eq!(
                    *join_type,
                    sqld::sql::ast::JoinType::Inner,
                    "expected Inner join type"
                );
                assert!(condition.is_some(), "expected ON condition to be present");
            }
            other => panic!("expected Join under Project, got {}", other.node_name()),
        },
        other => panic!("expected Project at top level, got {}", other.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 5. test_left_join — LEFT JOIN produces Join with Left type
// ---------------------------------------------------------------------------

#[test]
fn test_left_join() {
    let plan = build_plan(
        "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id",
    );
    match &plan {
        LogicalPlan::Project { input, .. } => match input.as_ref() {
            LogicalPlan::Join { join_type, .. } => {
                assert_eq!(
                    *join_type,
                    sqld::sql::ast::JoinType::Left,
                    "expected Left join type"
                );
            }
            other => panic!("expected Join under Project, got {}", other.node_name()),
        },
        other => panic!("expected Project at top level, got {}", other.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 6. test_select_with_aggregate — SELECT COUNT(*) FROM users → Aggregate node present
// ---------------------------------------------------------------------------

#[test]
fn test_select_with_aggregate() {
    let plan = build_plan("SELECT COUNT(*) FROM users");
    // The plan should contain an Aggregate node somewhere in the tree.
    fn find_aggregate(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Aggregate { .. } => true,
            other => other.children().iter().any(|c| find_aggregate(c)),
        }
    }
    assert!(
        find_aggregate(&plan),
        "expected an Aggregate node in the plan tree"
    );
}

// ---------------------------------------------------------------------------
// 7. test_select_with_group_by — SELECT status, COUNT(*) FROM orders GROUP BY status
//    → Aggregate with non-empty group_by
// ---------------------------------------------------------------------------

#[test]
fn test_select_with_group_by() {
    let plan = build_plan("SELECT status, COUNT(*) FROM orders GROUP BY status");
    fn find_aggregate_group_by(plan: &LogicalPlan) -> Option<(usize, usize)> {
        match plan {
            LogicalPlan::Aggregate { group_by, aggregates, .. } => {
                Some((group_by.len(), aggregates.len()))
            }
            other => other
                .children()
                .iter()
                .find_map(|c| find_aggregate_group_by(c)),
        }
    }
    let (group_by_len, agg_len) = find_aggregate_group_by(&plan)
        .expect("expected an Aggregate node in the plan tree");
    assert_eq!(group_by_len, 1, "expected 1 group_by expression");
    assert_eq!(agg_len, 1, "expected 1 aggregate function");
}

// ---------------------------------------------------------------------------
// 8. test_select_with_order_by — SELECT * FROM users ORDER BY name → Sort node
// ---------------------------------------------------------------------------

#[test]
fn test_select_with_order_by() {
    let plan = build_plan("SELECT * FROM users ORDER BY name");
    // Sort is applied before projection so the top-level node is Project
    match &plan {
        LogicalPlan::Project { input, .. } => match input.as_ref() {
            LogicalPlan::Sort { order_by, .. } => {
                assert_eq!(order_by.len(), 1, "expected 1 sort expression");
            }
            other => panic!("expected Sort inside Project, got {}", other.node_name()),
        },
        other => panic!("expected Project at top level, got {}", other.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 9. test_select_with_limit — SELECT * FROM users LIMIT 10 → Limit with count=Some(10)
// ---------------------------------------------------------------------------

#[test]
fn test_select_with_limit() {
    let plan = build_plan("SELECT * FROM users LIMIT 10");
    match &plan {
        LogicalPlan::Limit { count, offset, .. } => {
            assert_eq!(*count, Some(10), "expected count = Some(10)");
            assert_eq!(*offset, 0, "expected offset = 0 when no OFFSET clause");
        }
        other => panic!("expected Limit at top level, got {}", other.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 10. test_select_with_limit_offset — LIMIT 10 OFFSET 5 → Limit with offset=5
// ---------------------------------------------------------------------------

#[test]
fn test_select_with_limit_offset() {
    let plan = build_plan("SELECT * FROM users LIMIT 10 OFFSET 5");
    match &plan {
        LogicalPlan::Limit { count, offset, .. } => {
            assert_eq!(*count, Some(10), "expected count = Some(10)");
            assert_eq!(*offset, 5, "expected offset = 5");
        }
        other => panic!("expected Limit at top level, got {}", other.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 11. test_select_distinct — SELECT DISTINCT name FROM users → Distinct node
// ---------------------------------------------------------------------------

#[test]
fn test_select_distinct() {
    let plan = build_plan("SELECT DISTINCT name FROM users");
    match &plan {
        LogicalPlan::Distinct { input } => {
            assert!(
                matches!(input.as_ref(), LogicalPlan::Project { .. }),
                "expected Project under Distinct, got {}",
                input.node_name()
            );
        }
        other => panic!("expected Distinct at top level, got {}", other.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 12. test_union — UNION → Union node with all=false
// ---------------------------------------------------------------------------

#[test]
fn test_union() {
    let plan = build_plan("SELECT id FROM users UNION SELECT id FROM orders");
    match &plan {
        LogicalPlan::Union { all, left, right } => {
            assert!(!*all, "expected UNION (not ALL), so all should be false");
            assert!(
                matches!(left.as_ref(), LogicalPlan::Project { .. }),
                "expected Project on left side of Union, got {}",
                left.node_name()
            );
            assert!(
                matches!(right.as_ref(), LogicalPlan::Project { .. }),
                "expected Project on right side of Union, got {}",
                right.node_name()
            );
        }
        other => panic!("expected Union at top level, got {}", other.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 13. test_union_all — UNION ALL → Union with all=true
// ---------------------------------------------------------------------------

#[test]
fn test_union_all() {
    let plan = build_plan("SELECT id FROM users UNION ALL SELECT id FROM orders");
    match &plan {
        LogicalPlan::Union { all, .. } => {
            assert!(*all, "expected UNION ALL, so all should be true");
        }
        other => panic!("expected Union at top level, got {}", other.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 14. test_insert_values — INSERT INTO users (id, name) VALUES (1, 'Alice') → Insert over Values
// ---------------------------------------------------------------------------

#[test]
fn test_insert_values() {
    let plan = build_plan("INSERT INTO users (id, name) VALUES (1, 'Alice')");
    match &plan {
        LogicalPlan::Insert { table, columns, input } => {
            assert_eq!(table, "users", "expected table = 'users'");
            assert_eq!(
                columns,
                &["id".to_string(), "name".to_string()],
                "expected columns [id, name]"
            );
            assert!(
                matches!(input.as_ref(), LogicalPlan::Values { .. }),
                "expected Values under Insert, got {}",
                input.node_name()
            );
        }
        other => panic!("expected Insert at top level, got {}", other.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 15. test_update — UPDATE users SET name = 'Bob' WHERE id = 1 → Update node
// ---------------------------------------------------------------------------

#[test]
fn test_update() {
    let plan = build_plan("UPDATE users SET name = 'Bob' WHERE id = 1");
    match &plan {
        LogicalPlan::Update { table, assignments, input } => {
            assert_eq!(table, "users", "expected table = 'users'");
            assert_eq!(assignments.len(), 1, "expected 1 assignment");
            assert_eq!(assignments[0].0, "name", "expected column 'name'");
            // With WHERE clause, input should be Filter over Scan
            assert!(
                matches!(input.as_ref(), LogicalPlan::Filter { .. }),
                "expected Filter under Update (due to WHERE), got {}",
                input.node_name()
            );
        }
        other => panic!("expected Update at top level, got {}", other.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 16. test_delete — DELETE FROM users WHERE id = 1 → Delete over Filter
// ---------------------------------------------------------------------------

#[test]
fn test_delete() {
    let plan = build_plan("DELETE FROM users WHERE id = 1");
    match &plan {
        LogicalPlan::Delete { table, input } => {
            assert_eq!(table, "users", "expected table = 'users'");
            assert!(
                matches!(input.as_ref(), LogicalPlan::Filter { .. }),
                "expected Filter under Delete (due to WHERE), got {}",
                input.node_name()
            );
        }
        other => panic!("expected Delete at top level, got {}", other.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 17. test_subquery_in_from — SELECT * FROM (SELECT id, name FROM users) AS t → nested Project
// ---------------------------------------------------------------------------

#[test]
fn test_subquery_in_from() {
    let plan = build_plan("SELECT * FROM (SELECT id, name FROM users) AS t");
    // The outer Project should sit on top of an inner Project (the subquery).
    match &plan {
        LogicalPlan::Project { input, .. } => {
            assert!(
                matches!(input.as_ref(), LogicalPlan::Project { .. }),
                "expected nested Project (subquery) under outer Project, got {}",
                input.node_name()
            );
        }
        other => panic!("expected Project at top level, got {}", other.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 18. test_having — SELECT status, COUNT(*) FROM orders GROUP BY status HAVING COUNT(*) > 5
//     → Filter after Aggregate
// ---------------------------------------------------------------------------

#[test]
fn test_having() {
    let plan = build_plan(
        "SELECT status, COUNT(*) FROM orders GROUP BY status HAVING COUNT(*) > 5",
    );
    // Expected shape: Project → Filter → Aggregate → Scan
    fn find_filter_over_aggregate(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Filter { input, .. } => {
                matches!(input.as_ref(), LogicalPlan::Aggregate { .. })
            }
            other => other
                .children()
                .iter()
                .any(|c| find_filter_over_aggregate(c)),
        }
    }
    assert!(
        find_filter_over_aggregate(&plan),
        "expected a Filter node directly over an Aggregate node (for HAVING)"
    );
}

// ---------------------------------------------------------------------------
// 19. test_no_from_clause — SELECT 1 + 1 → Project over Empty
// ---------------------------------------------------------------------------

#[test]
fn test_no_from_clause() {
    let plan = build_plan("SELECT 1 + 1");
    match &plan {
        LogicalPlan::Project { input, .. } => {
            assert!(
                matches!(input.as_ref(), LogicalPlan::Empty { .. }),
                "expected Empty under Project when no FROM clause, got {}",
                input.node_name()
            );
        }
        other => panic!("expected Project at top level, got {}", other.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 20. test_schema_propagation — verify schema flows through plan nodes correctly
//     for a simple SELECT from a cataloged table
// ---------------------------------------------------------------------------

#[test]
fn test_schema_propagation() {
    let plan = build_plan("SELECT * FROM users");

    // The Scan node should carry all 4 columns from the catalog.
    let scan_schema = match &plan {
        LogicalPlan::Project { input, .. } => input.schema(),
        other => panic!("expected Project at top level, got {}", other.node_name()),
    };

    assert_eq!(
        scan_schema.column_count(),
        4,
        "Scan schema should have 4 columns (id, name, age, email)"
    );
    assert!(scan_schema.has_column("id"), "schema missing 'id'");
    assert!(scan_schema.has_column("name"), "schema missing 'name'");
    assert!(scan_schema.has_column("age"), "schema missing 'age'");
    assert!(scan_schema.has_column("email"), "schema missing 'email'");

    // Verify column types are preserved.
    let (_, id_col) = scan_schema.column_by_name("id").unwrap();
    assert_eq!(id_col.data_type, DataType::Integer, "id should be Integer");
    assert!(!id_col.nullable, "id should not be nullable");

    let (_, name_col) = scan_schema.column_by_name("name").unwrap();
    assert_eq!(
        name_col.data_type,
        DataType::Varchar(255),
        "name should be Varchar(255)"
    );
    assert!(!name_col.nullable, "name should not be nullable");

    let (_, age_col) = scan_schema.column_by_name("age").unwrap();
    assert_eq!(age_col.data_type, DataType::Integer, "age should be Integer");
    assert!(age_col.nullable, "age should be nullable");

    let (_, email_col) = scan_schema.column_by_name("email").unwrap();
    assert_eq!(email_col.data_type, DataType::Text, "email should be Text");
    assert!(email_col.nullable, "email should be nullable");

    // The top-level plan's schema() call should propagate upward without error.
    let top_schema = plan.schema();
    assert!(
        top_schema.column_count() > 0,
        "top-level plan schema should be non-empty"
    );
}
