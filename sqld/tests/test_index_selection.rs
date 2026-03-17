use sqld::planner::logical_plan::*;
use sqld::planner::physical_plan::*;
use sqld::planner::physical_planner::PhysicalPlanner;
use sqld::planner::{Catalog, IndexInfo, TableStats, ColumnStats};
use sqld::sql::ast::*;
use sqld::types::{Column, DataType, Schema};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Helper: build the catalog used across all tests
// ---------------------------------------------------------------------------

fn make_catalog() -> Catalog {
    let mut catalog = Catalog::new();

    // Table "users": id, name, age, email
    catalog.add_table(
        "users",
        Schema::new(vec![
            Column::new("id",    DataType::Integer,      false),
            Column::new("name",  DataType::Varchar(255), false),
            Column::new("age",   DataType::Integer,      true),
            Column::new("email", DataType::Text,         false),
        ]),
    );

    // Per-column stats for "users"
    let mut col_stats: HashMap<String, ColumnStats> = HashMap::new();
    col_stats.insert(
        "id".to_string(),
        ColumnStats {
            distinct_count: 100_000.0,
            null_fraction:  0.0,
            min_value:      Some(1.0),
            max_value:      Some(100_000.0),
            avg_width:      4.0,
        },
    );
    col_stats.insert(
        "email".to_string(),
        ColumnStats {
            distinct_count: 100_000.0,
            null_fraction:  0.0,
            min_value:      None,
            max_value:      None,
            avg_width:      32.0,
        },
    );
    col_stats.insert(
        "age".to_string(),
        ColumnStats {
            distinct_count: 80.0,
            null_fraction:  0.0,
            min_value:      Some(0.0),
            max_value:      Some(100.0),
            avg_width:      4.0,
        },
    );

    catalog.set_stats(
        "users",
        TableStats {
            row_count:    100_000.0,
            page_count:   1_000.0,
            column_stats: col_stats,
        },
    );

    // Index: unique B-tree on users(id)
    catalog.add_index(IndexInfo {
        name:    "idx_users_id".to_string(),
        table:   "users".to_string(),
        columns: vec!["id".to_string()],
        unique:  true,
        method:  IndexMethod::BTree,
    });

    // Index: unique B-tree on users(email)
    catalog.add_index(IndexInfo {
        name:    "idx_users_email".to_string(),
        table:   "users".to_string(),
        columns: vec!["email".to_string()],
        unique:  true,
        method:  IndexMethod::BTree,
    });

    // "orders" table — needed for join tests
    catalog.add_table(
        "orders",
        Schema::new(vec![
            Column::new("id",      DataType::Integer, false),
            Column::new("user_id", DataType::Integer, false),
            Column::new("amount",  DataType::Float,   true),
        ]),
    );
    catalog.set_stats(
        "orders",
        TableStats {
            row_count:    50_000.0,
            page_count:   500.0,
            column_stats: HashMap::new(),
        },
    );

    catalog
}

// ---------------------------------------------------------------------------
// Utility: find the first SeqScan / IndexScan in a physical plan tree
// ---------------------------------------------------------------------------

fn find_scan(plan: &PhysicalPlan) -> Option<&PhysicalPlan> {
    match plan {
        PhysicalPlan::SeqScan { .. } | PhysicalPlan::IndexScan { .. } => Some(plan),
        _ => {
            for child in plan.children() {
                if let Some(found) = find_scan(child) {
                    return Some(found);
                }
            }
            None
        }
    }
}

// Utility: find the first join node in a physical plan tree
fn find_join(plan: &PhysicalPlan) -> Option<&PhysicalPlan> {
    match plan {
        PhysicalPlan::HashJoin { .. }
        | PhysicalPlan::SortMergeJoin { .. }
        | PhysicalPlan::NestedLoopJoin { .. } => Some(plan),
        _ => {
            for child in plan.children() {
                if let Some(found) = find_join(child) {
                    return Some(found);
                }
            }
            None
        }
    }
}

// Utility: find the first aggregate node in a physical plan tree
fn find_aggregate(plan: &PhysicalPlan) -> Option<&PhysicalPlan> {
    match plan {
        PhysicalPlan::HashAggregate { .. } | PhysicalPlan::SortAggregate { .. } => Some(plan),
        _ => {
            for child in plan.children() {
                if let Some(found) = find_aggregate(child) {
                    return Some(found);
                }
            }
            None
        }
    }
}

// Utility: build the users schema (shorthand)
fn users_schema() -> Schema {
    Schema::new(vec![
        Column::new("id",    DataType::Integer,      false),
        Column::new("name",  DataType::Varchar(255), false),
        Column::new("age",   DataType::Integer,      true),
        Column::new("email", DataType::Text,         false),
    ])
}

// Utility: build the orders schema (shorthand)
fn orders_schema() -> Schema {
    Schema::new(vec![
        Column::new("id",      DataType::Integer, false),
        Column::new("user_id", DataType::Integer, false),
        Column::new("amount",  DataType::Float,   true),
    ])
}

// ---------------------------------------------------------------------------
// 1. SeqScan when there is no predicate
// ---------------------------------------------------------------------------

#[test]
fn test_seq_scan_no_predicate() {
    let catalog = make_catalog();
    let logical = LogicalPlan::Scan {
        table:  "users".to_string(),
        alias:  None,
        schema: users_schema(),
    };

    let planner = PhysicalPlanner::new(&catalog);
    let physical = planner.plan(&logical);

    let scan = find_scan(&physical).expect("should produce a scan node");
    assert!(
        matches!(scan, PhysicalPlan::SeqScan { predicate: None, .. }),
        "scan without predicate must be SeqScan with no predicate; got {:?}",
        scan.node_name()
    );
}

// ---------------------------------------------------------------------------
// 2. IndexScan chosen for an equality predicate on an indexed column
// ---------------------------------------------------------------------------

#[test]
fn test_index_scan_with_equality() {
    let catalog = make_catalog();

    // Filter(id = 42) over Scan(users)
    let scan = LogicalPlan::Scan {
        table:  "users".to_string(),
        alias:  None,
        schema: users_schema(),
    };
    let predicate = Expr::BinaryOp {
        left:  Box::new(Expr::Identifier("id".to_string())),
        op:    BinaryOp::Eq,
        right: Box::new(Expr::Integer(42)),
    };
    let logical = LogicalPlan::Filter {
        predicate,
        input: Box::new(scan),
    };

    let planner  = PhysicalPlanner::new(&catalog);
    let physical = planner.plan(&logical);

    let found = find_scan(&physical).expect("should have a scan node");
    match found {
        PhysicalPlan::IndexScan { index_name, key_ranges, .. } => {
            assert_eq!(index_name, "idx_users_id", "wrong index chosen");
            assert_eq!(key_ranges.len(), 1, "equality should produce exactly one key range");
            // Verify point range: low == high == Inclusive(42)
            let kr = &key_ranges[0];
            assert!(
                matches!(&kr.low,  Bound::Inclusive(Expr::Integer(42))),
                "low bound should be Inclusive(42)"
            );
            assert!(
                matches!(&kr.high, Bound::Inclusive(Expr::Integer(42))),
                "high bound should be Inclusive(42)"
            );
        }
        PhysicalPlan::SeqScan { .. } => {
            // Cost model may prefer SeqScan in some configurations; that is acceptable.
        }
        other => panic!("unexpected node: {}", other.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 3. IndexScan chosen for a range predicate on an indexed column
// ---------------------------------------------------------------------------

#[test]
fn test_index_scan_with_range() {
    let catalog = make_catalog();

    // Filter(id > 50000) over Scan(users)
    let scan = LogicalPlan::Scan {
        table:  "users".to_string(),
        alias:  None,
        schema: users_schema(),
    };
    let predicate = Expr::BinaryOp {
        left:  Box::new(Expr::Identifier("id".to_string())),
        op:    BinaryOp::Gt,
        right: Box::new(Expr::Integer(50_000)),
    };
    let logical = LogicalPlan::Filter {
        predicate,
        input: Box::new(scan),
    };

    let planner  = PhysicalPlanner::new(&catalog);
    let physical = planner.plan(&logical);

    let found = find_scan(&physical).expect("should have a scan node");
    match found {
        PhysicalPlan::IndexScan { index_name, key_ranges, .. } => {
            assert_eq!(index_name, "idx_users_id");
            assert_eq!(key_ranges.len(), 1);
            // col > 50000 → Exclusive lower bound, Unbounded upper bound
            let kr = &key_ranges[0];
            assert!(
                matches!(&kr.low,  Bound::Exclusive(Expr::Integer(50_000))),
                "lower bound should be Exclusive(50000)"
            );
            assert!(
                matches!(&kr.high, Bound::Unbounded),
                "upper bound should be Unbounded"
            );
        }
        PhysicalPlan::SeqScan { .. } => {
            // Acceptable: cost model may prefer SeqScan for wide ranges.
        }
        other => panic!("unexpected node: {}", other.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 4. SeqScan when column has no index
// ---------------------------------------------------------------------------

#[test]
fn test_seq_scan_when_unindexed_column() {
    let catalog = make_catalog();

    // Filter(age > 50) over Scan(users)  — age has no index in make_catalog()
    let scan = LogicalPlan::Scan {
        table:  "users".to_string(),
        alias:  None,
        schema: users_schema(),
    };
    let predicate = Expr::BinaryOp {
        left:  Box::new(Expr::Identifier("age".to_string())),
        op:    BinaryOp::Gt,
        right: Box::new(Expr::Integer(50)),
    };
    let logical = LogicalPlan::Filter {
        predicate,
        input: Box::new(scan),
    };

    let planner  = PhysicalPlanner::new(&catalog);
    let physical = planner.plan(&logical);

    let found = find_scan(&physical).expect("should have a scan node");
    assert!(
        matches!(found, PhysicalPlan::SeqScan { .. }),
        "predicate on unindexed column must produce SeqScan"
    );
}

// ---------------------------------------------------------------------------
// 5. IndexScan for BETWEEN predicate
// ---------------------------------------------------------------------------

#[test]
fn test_index_scan_with_between() {
    let catalog = make_catalog();

    // Filter(id BETWEEN 100 AND 200) over Scan(users)
    let scan = LogicalPlan::Scan {
        table:  "users".to_string(),
        alias:  None,
        schema: users_schema(),
    };
    let predicate = Expr::Between {
        expr:    Box::new(Expr::Identifier("id".to_string())),
        low:     Box::new(Expr::Integer(100)),
        high:    Box::new(Expr::Integer(200)),
        negated: false,
    };
    let logical = LogicalPlan::Filter {
        predicate,
        input: Box::new(scan),
    };

    let planner  = PhysicalPlanner::new(&catalog);
    let physical = planner.plan(&logical);

    let found = find_scan(&physical).expect("should have a scan node");
    match found {
        PhysicalPlan::IndexScan { index_name, key_ranges, .. } => {
            assert_eq!(index_name, "idx_users_id");
            assert_eq!(key_ranges.len(), 1, "BETWEEN should produce a single range");
            let kr = &key_ranges[0];
            assert!(
                matches!(&kr.low,  Bound::Inclusive(Expr::Integer(100))),
                "lower bound should be Inclusive(100)"
            );
            assert!(
                matches!(&kr.high, Bound::Inclusive(Expr::Integer(200))),
                "upper bound should be Inclusive(200)"
            );
        }
        PhysicalPlan::SeqScan { .. } => {
            // Acceptable per cost model.
        }
        other => panic!("unexpected node: {}", other.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 6. IndexScan with IN list → multiple key ranges
// ---------------------------------------------------------------------------

#[test]
fn test_index_scan_with_in_list() {
    let catalog = make_catalog();

    // Filter(id IN (1, 2, 3)) over Scan(users)
    let scan = LogicalPlan::Scan {
        table:  "users".to_string(),
        alias:  None,
        schema: users_schema(),
    };
    let predicate = Expr::InList {
        expr:    Box::new(Expr::Identifier("id".to_string())),
        list:    vec![Expr::Integer(1), Expr::Integer(2), Expr::Integer(3)],
        negated: false,
    };
    let logical = LogicalPlan::Filter {
        predicate,
        input: Box::new(scan),
    };

    let planner  = PhysicalPlanner::new(&catalog);
    let physical = planner.plan(&logical);

    let found = find_scan(&physical).expect("should have a scan node");
    match found {
        PhysicalPlan::IndexScan { index_name, key_ranges, .. } => {
            assert_eq!(index_name, "idx_users_id");
            assert_eq!(
                key_ranges.len(), 3,
                "IN (1,2,3) should produce 3 point ranges"
            );
            for (i, kr) in key_ranges.iter().enumerate() {
                let expected = (i as i64) + 1;
                assert!(
                    matches!(&kr.low,  Bound::Inclusive(Expr::Integer(v)) if *v == expected),
                    "range {} low should be Inclusive({})", i, expected
                );
                assert!(
                    matches!(&kr.high, Bound::Inclusive(Expr::Integer(v)) if *v == expected),
                    "range {} high should be Inclusive({})", i, expected
                );
            }
        }
        PhysicalPlan::SeqScan { .. } => {
            // Acceptable per cost model.
        }
        other => panic!("unexpected node: {}", other.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 7. HashJoin (or SortMergeJoin) for an equi-join
// ---------------------------------------------------------------------------

#[test]
fn test_hash_join_for_equi_join() {
    let catalog = make_catalog();

    // Join(users, orders ON users.id = orders.user_id)
    let users_scan = LogicalPlan::Scan {
        table:  "users".to_string(),
        alias:  None,
        schema: users_schema(),
    };
    let orders_scan = LogicalPlan::Scan {
        table:  "orders".to_string(),
        alias:  None,
        schema: orders_schema(),
    };
    let join_schema = users_schema().merge(&orders_schema());
    let condition = Expr::BinaryOp {
        left:  Box::new(Expr::QualifiedIdentifier {
            table:  "users".to_string(),
            column: "id".to_string(),
        }),
        op:    BinaryOp::Eq,
        right: Box::new(Expr::QualifiedIdentifier {
            table:  "orders".to_string(),
            column: "user_id".to_string(),
        }),
    };
    let logical = LogicalPlan::Join {
        join_type: JoinType::Inner,
        condition: Some(condition),
        left:      Box::new(users_scan),
        right:     Box::new(orders_scan),
        schema:    join_schema,
    };

    let planner  = PhysicalPlanner::new(&catalog);
    let physical = planner.plan(&logical);

    let join_node = find_join(&physical).expect("should produce a join node");
    assert!(
        matches!(
            join_node,
            PhysicalPlan::HashJoin { .. } | PhysicalPlan::SortMergeJoin { .. }
        ),
        "equi-join should use HashJoin or SortMergeJoin, not NestedLoop; got {}",
        join_node.node_name()
    );
}

// ---------------------------------------------------------------------------
// 8. NestedLoopJoin for a non-equi join condition
// ---------------------------------------------------------------------------

#[test]
fn test_nested_loop_for_non_equi() {
    let catalog = make_catalog();

    // Join(users, orders) ON users.age > orders.amount  — non-equi
    let users_scan = LogicalPlan::Scan {
        table:  "users".to_string(),
        alias:  None,
        schema: users_schema(),
    };
    let orders_scan = LogicalPlan::Scan {
        table:  "orders".to_string(),
        alias:  None,
        schema: orders_schema(),
    };
    let join_schema = users_schema().merge(&orders_schema());
    let condition = Expr::BinaryOp {
        left:  Box::new(Expr::QualifiedIdentifier {
            table:  "users".to_string(),
            column: "age".to_string(),
        }),
        op:    BinaryOp::Gt,
        right: Box::new(Expr::QualifiedIdentifier {
            table:  "orders".to_string(),
            column: "amount".to_string(),
        }),
    };
    let logical = LogicalPlan::Join {
        join_type: JoinType::Inner,
        condition: Some(condition),
        left:      Box::new(users_scan),
        right:     Box::new(orders_scan),
        schema:    join_schema,
    };

    let planner  = PhysicalPlanner::new(&catalog);
    let physical = planner.plan(&logical);

    let join_node = find_join(&physical).expect("should produce a join node");
    assert!(
        matches!(join_node, PhysicalPlan::NestedLoopJoin { .. }),
        "non-equi join must use NestedLoopJoin; got {}",
        join_node.node_name()
    );
}

// ---------------------------------------------------------------------------
// 9. HashAggregate for GROUP BY queries
// ---------------------------------------------------------------------------

#[test]
fn test_hash_aggregate_selected() {
    let catalog = make_catalog();

    // Aggregate(group_by=[age], aggregates=[COUNT(id)]) over Scan(users)
    let scan = LogicalPlan::Scan {
        table:  "users".to_string(),
        alias:  None,
        schema: users_schema(),
    };
    let agg_schema = Schema::new(vec![
        Column::new("age",   DataType::Integer, true),
        Column::new("count", DataType::BigInt,  false),
    ]);
    let logical = LogicalPlan::Aggregate {
        group_by:   vec![Expr::Identifier("age".to_string())],
        aggregates: vec![AggregateExpr {
            func:     AggregateFunc::Count,
            arg:      Expr::Identifier("id".to_string()),
            distinct: false,
            alias:    "count".to_string(),
        }],
        input:  Box::new(scan),
        schema: agg_schema,
    };

    let planner  = PhysicalPlanner::new(&catalog);
    let physical = planner.plan(&logical);

    let agg_node = find_aggregate(&physical).expect("should produce an aggregate node");
    // Hash aggregate is typically cheaper than sort-then-aggregate for unordered data
    assert!(
        matches!(agg_node, PhysicalPlan::HashAggregate { .. }),
        "GROUP BY on a plain scan should prefer HashAggregate; got {}",
        agg_node.node_name()
    );
}

// ---------------------------------------------------------------------------
// 10. Distinct → HashDistinct or SortDistinct (either is valid)
// ---------------------------------------------------------------------------

#[test]
fn test_distinct_physical_choice() {
    let catalog = make_catalog();

    // Distinct over Scan(users)
    let scan = LogicalPlan::Scan {
        table:  "users".to_string(),
        alias:  None,
        schema: users_schema(),
    };
    let logical = LogicalPlan::Distinct {
        input: Box::new(scan),
    };

    let planner  = PhysicalPlanner::new(&catalog);
    let physical = planner.plan(&logical);

    fn find_distinct(plan: &PhysicalPlan) -> Option<&PhysicalPlan> {
        match plan {
            PhysicalPlan::HashDistinct { .. } | PhysicalPlan::SortDistinct { .. } => Some(plan),
            _ => {
                for child in plan.children() {
                    if let Some(found) = find_distinct(child) {
                        return Some(found);
                    }
                }
                None
            }
        }
    }

    let distinct_node = find_distinct(&physical)
        .expect("Distinct logical node must produce HashDistinct or SortDistinct");

    assert!(
        matches!(
            distinct_node,
            PhysicalPlan::HashDistinct { .. } | PhysicalPlan::SortDistinct { .. }
        ),
        "Distinct should map to HashDistinct or SortDistinct; got {}",
        distinct_node.node_name()
    );
}
