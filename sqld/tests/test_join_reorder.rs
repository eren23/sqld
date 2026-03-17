use sqld::planner::logical_plan::*;
use sqld::planner::rules::join_reorder::JoinReorder;
use sqld::planner::rules::OptimizationRule;
use sqld::planner::{Catalog, TableStats, ColumnStats};
use sqld::sql::ast::*;
use sqld::types::{Column, DataType, Schema};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_catalog() -> Catalog {
    let mut catalog = Catalog::new();

    // small_table: 10 rows
    catalog.add_table(
        "small_table",
        Schema::new(vec![
            Column::new("id", DataType::Integer, false),
            Column::new("val", DataType::Integer, true),
        ]),
    );
    catalog.set_stats(
        "small_table",
        TableStats {
            row_count: 10.0,
            page_count: 1.0,
            column_stats: HashMap::new(),
        },
    );

    // medium_table: 1000 rows
    catalog.add_table(
        "medium_table",
        Schema::new(vec![
            Column::new("id", DataType::Integer, false),
            Column::new("val", DataType::Integer, true),
        ]),
    );
    catalog.set_stats(
        "medium_table",
        TableStats {
            row_count: 1000.0,
            page_count: 20.0,
            column_stats: HashMap::new(),
        },
    );

    // large_table: 100000 rows
    catalog.add_table(
        "large_table",
        Schema::new(vec![
            Column::new("id", DataType::Integer, false),
            Column::new("val", DataType::Integer, true),
        ]),
    );
    catalog.set_stats(
        "large_table",
        TableStats {
            row_count: 100000.0,
            page_count: 2000.0,
            column_stats: HashMap::new(),
        },
    );

    catalog
}

fn scan(table: &str, catalog: &Catalog) -> LogicalPlan {
    let schema = catalog
        .get_schema(table)
        .cloned()
        .unwrap_or_else(Schema::empty);
    LogicalPlan::Scan {
        table: table.to_string(),
        alias: None,
        schema,
    }
}

fn inner_join(
    left: LogicalPlan,
    right: LogicalPlan,
    condition: Option<Expr>,
    _catalog: &Catalog,
) -> LogicalPlan {
    let schema = left.schema().merge(&right.schema());
    LogicalPlan::Join {
        join_type: JoinType::Inner,
        condition,
        left: Box::new(left),
        right: Box::new(right),
        schema,
    }
}

fn qcol(table: &str, col: &str) -> Expr {
    Expr::QualifiedIdentifier {
        table: table.to_string(),
        column: col.to_string(),
    }
}

fn eq(l: Expr, r: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(l),
        op: BinaryOp::Eq,
        right: Box::new(r),
    }
}

// ---------------------------------------------------------------------------
// Collect all table names reachable through Scan nodes in a plan tree.
// ---------------------------------------------------------------------------

fn collect_tables(plan: &LogicalPlan) -> Vec<String> {
    let mut tables = Vec::new();
    collect_tables_inner(plan, &mut tables);
    tables
}

fn collect_tables_inner(plan: &LogicalPlan, out: &mut Vec<String>) {
    match plan {
        LogicalPlan::Scan { table, .. } => out.push(table.clone()),
        other => {
            for child in other.children() {
                collect_tables_inner(child, out);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Count Join nodes in a plan tree.
// ---------------------------------------------------------------------------

fn count_joins(plan: &LogicalPlan) -> usize {
    let mut count = 0;
    if matches!(plan, LogicalPlan::Join { .. }) {
        count += 1;
    }
    for child in plan.children() {
        count += count_joins(child);
    }
    count
}

// ---------------------------------------------------------------------------
// Collect all join conditions present in the plan tree.
// ---------------------------------------------------------------------------

fn collect_predicates(plan: &LogicalPlan) -> Vec<Expr> {
    let mut preds = Vec::new();
    collect_predicates_inner(plan, &mut preds);
    preds
}

fn collect_predicates_inner(plan: &LogicalPlan, out: &mut Vec<Expr>) {
    if let LogicalPlan::Join { condition, .. } = plan {
        if let Some(cond) = condition {
            out.extend(split_conjunction(cond));
        }
    }
    for child in plan.children() {
        collect_predicates_inner(child, out);
    }
}

// ---------------------------------------------------------------------------
// 1. test_two_table_reorder
//    Join small_table and large_table. After reorder the plan should still
//    be a valid Join containing both tables regardless of which side each
//    lands on.
// ---------------------------------------------------------------------------

#[test]
fn test_two_table_reorder() {
    let catalog = make_catalog();
    let condition = eq(qcol("small_table", "id"), qcol("large_table", "id"));
    let left = scan("small_table", &catalog);
    let right = scan("large_table", &catalog);
    let plan = inner_join(left, right, Some(condition), &catalog);

    let rule = JoinReorder::new(catalog);
    let optimized = rule.apply(plan);

    // The optimized plan must be a Join node.
    assert!(
        matches!(optimized, LogicalPlan::Join { .. }),
        "expected a Join node at the root, got {:?}",
        optimized.node_name()
    );

    // Both tables must still be present.
    let tables = collect_tables(&optimized);
    assert_eq!(tables.len(), 2, "expected exactly 2 scan nodes");
    assert!(
        tables.contains(&"small_table".to_string()),
        "small_table missing from plan"
    );
    assert!(
        tables.contains(&"large_table".to_string()),
        "large_table missing from plan"
    );
}

// ---------------------------------------------------------------------------
// 2. test_three_table_dp
//    Join small, medium, large (3 tables ≤ 6 → DP algorithm).
//    Verify the result is a valid join tree with all three tables present.
// ---------------------------------------------------------------------------

#[test]
fn test_three_table_dp() {
    let catalog = make_catalog();

    let cond_sm = eq(qcol("small_table", "id"), qcol("medium_table", "id"));
    let cond_ml = eq(qcol("medium_table", "id"), qcol("large_table", "id"));

    let s = scan("small_table", &catalog);
    let m = scan("medium_table", &catalog);
    let l = scan("large_table", &catalog);

    // Build small JOIN medium JOIN large (left-deep).
    let sm = inner_join(s, m, Some(cond_sm), &catalog);
    let plan = inner_join(sm, l, Some(cond_ml), &catalog);

    let rule = JoinReorder::new(catalog);
    let optimized = rule.apply(plan);

    // Must still be a join tree.
    assert!(
        matches!(optimized, LogicalPlan::Join { .. }),
        "expected a Join at root"
    );

    // All three tables must be present.
    let tables = collect_tables(&optimized);
    assert_eq!(tables.len(), 3, "expected exactly 3 scan nodes");
    assert!(tables.contains(&"small_table".to_string()));
    assert!(tables.contains(&"medium_table".to_string()));
    assert!(tables.contains(&"large_table".to_string()));

    // Exactly 2 join nodes for 3 tables.
    assert_eq!(count_joins(&optimized), 2, "expected 2 join nodes for 3 tables");
}

// ---------------------------------------------------------------------------
// 3. test_single_table_no_reorder
//    A single Scan should pass through the rule unchanged.
// ---------------------------------------------------------------------------

#[test]
fn test_single_table_no_reorder() {
    let catalog = make_catalog();
    let plan = scan("small_table", &catalog);

    let rule = JoinReorder::new(catalog);
    let optimized = rule.apply(plan);

    assert!(
        matches!(optimized, LogicalPlan::Scan { .. }),
        "expected Scan node unchanged"
    );
    let tables = collect_tables(&optimized);
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0], "small_table");
}

// ---------------------------------------------------------------------------
// 4. test_outer_join_not_reordered
//    A LEFT JOIN should NOT be reordered; original table order is preserved.
// ---------------------------------------------------------------------------

#[test]
fn test_outer_join_not_reordered() {
    let catalog = make_catalog();
    let condition = eq(qcol("large_table", "id"), qcol("small_table", "id"));

    // Build the join manually (large LEFT JOIN small).
    let large_schema = catalog.get_schema("large_table").cloned().unwrap();
    let small_schema = catalog.get_schema("small_table").cloned().unwrap();
    let schema = large_schema.merge(&small_schema);

    let plan = LogicalPlan::Join {
        join_type: JoinType::Left,
        condition: Some(condition),
        left: Box::new(scan("large_table", &catalog)),
        right: Box::new(scan("small_table", &catalog)),
        schema,
    };

    let rule = JoinReorder::new(catalog);
    let optimized = rule.apply(plan);

    // The result must still be a Left join.
    match &optimized {
        LogicalPlan::Join { join_type, left, right, .. } => {
            assert_eq!(
                *join_type,
                JoinType::Left,
                "LEFT JOIN should not be converted to INNER"
            );
            // Original order preserved: large on the left, small on the right.
            assert!(
                matches!(left.as_ref(), LogicalPlan::Scan { table, .. } if table == "large_table"),
                "large_table should remain on the left side"
            );
            assert!(
                matches!(right.as_ref(), LogicalPlan::Scan { table, .. } if table == "small_table"),
                "small_table should remain on the right side"
            );
        }
        other => panic!("expected Join node, got {:?}", other.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 5. test_many_tables_greedy
//    Create 8 tables to trigger the greedy algorithm (7 < n ≤ 12).
//    Verify the output is a valid join tree containing all 8 tables.
// ---------------------------------------------------------------------------

#[test]
fn test_many_tables_greedy() {
    // Build a catalog with 8 tables named t0..t7.
    let mut catalog = Catalog::new();
    for i in 0..8usize {
        let name = format!("t{}", i);
        catalog.add_table(
            name.clone(),
            Schema::new(vec![
                Column::new("id", DataType::Integer, false),
                Column::new("val", DataType::Integer, true),
            ]),
        );
        catalog.set_stats(
            name,
            TableStats {
                row_count: (10usize.pow(i as u32 % 5 + 1)) as f64,
                page_count: (i + 1) as f64,
                column_stats: HashMap::new(),
            },
        );
    }

    // Build a left-deep chain: t0 JOIN t1 JOIN ... JOIN t7.
    let first_scan = LogicalPlan::Scan {
        table: "t0".to_string(),
        alias: None,
        schema: catalog.get_schema("t0").cloned().unwrap(),
    };

    let mut plan = first_scan;
    for i in 1..8usize {
        let tname = format!("t{}", i);
        let prev = format!("t{}", i - 1);
        let cond = eq(qcol(&prev, "id"), qcol(&tname, "id"));
        let right = LogicalPlan::Scan {
            table: tname.clone(),
            alias: None,
            schema: catalog.get_schema(&tname).cloned().unwrap(),
        };
        let merged_schema = plan.schema().merge(&right.schema());
        plan = LogicalPlan::Join {
            join_type: JoinType::Inner,
            condition: Some(cond),
            left: Box::new(plan),
            right: Box::new(right),
            schema: merged_schema,
        };
    }

    let rule = JoinReorder::new(catalog);
    let optimized = rule.apply(plan);

    // Must be a join tree.
    assert!(
        matches!(optimized, LogicalPlan::Join { .. }),
        "expected a Join at root"
    );

    // All 8 tables must appear exactly once.
    let tables = collect_tables(&optimized);
    assert_eq!(tables.len(), 8, "expected 8 scan nodes, got {}", tables.len());
    for i in 0..8usize {
        let name = format!("t{}", i);
        assert!(tables.contains(&name), "table {} missing from output plan", name);
    }

    // 7 join nodes for 8 tables.
    assert_eq!(count_joins(&optimized), 7, "expected 7 join nodes for 8 tables");
}

// ---------------------------------------------------------------------------
// 6. test_predicates_preserved
//    After reordering, all join predicates must still be present in the
//    output plan's join conditions.
// ---------------------------------------------------------------------------

#[test]
fn test_predicates_preserved() {
    let catalog = make_catalog();

    let cond1 = eq(qcol("small_table", "id"), qcol("medium_table", "id"));
    let cond2 = eq(qcol("medium_table", "id"), qcol("large_table", "id"));

    let s = scan("small_table", &catalog);
    let m = scan("medium_table", &catalog);
    let l = scan("large_table", &catalog);

    // Build small JOIN medium JOIN large keeping both predicates.
    let sm = inner_join(s, m, Some(cond1.clone()), &catalog);
    let plan = inner_join(sm, l, Some(cond2.clone()), &catalog);

    let rule = JoinReorder::new(catalog);
    let optimized = rule.apply(plan);

    // Collect all predicates from the reordered plan.
    let output_preds = collect_predicates(&optimized);

    // The original two equality predicates must each appear somewhere in the
    // output join conditions.
    assert!(
        output_preds.contains(&cond1),
        "predicate small_table.id = medium_table.id missing after reorder; found: {:?}",
        output_preds
    );
    assert!(
        output_preds.contains(&cond2),
        "predicate medium_table.id = large_table.id missing after reorder; found: {:?}",
        output_preds
    );
}
