use sqld::planner::cardinality::CardinalityEstimator;
use sqld::planner::logical_plan::*;
use sqld::planner::{Catalog, ColumnStats, TableStats};
use sqld::sql::ast::*;
use sqld::types::{Column, DataType, Schema};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Catalog helper
// ---------------------------------------------------------------------------

fn make_catalog() -> Catalog {
    let mut catalog = Catalog::new();

    // ---- users (10 000 rows, 100 pages) ------------------------------------
    catalog.add_table(
        "users",
        Schema::new(vec![
            Column::new("id", DataType::Integer, false),
            Column::new("age", DataType::Integer, true),
            Column::new("status", DataType::Varchar(20), true),
        ]),
    );

    let mut user_stats = TableStats {
        row_count: 10000.0,
        page_count: 100.0,
        column_stats: HashMap::new(),
    };
    user_stats.column_stats.insert(
        "id".to_string(),
        ColumnStats {
            distinct_count: 10000.0,
            null_fraction: 0.0,
            min_value: Some(1.0),
            max_value: Some(10000.0),
            avg_width: 4.0,
        },
    );
    user_stats.column_stats.insert(
        "age".to_string(),
        ColumnStats {
            distinct_count: 80.0,
            null_fraction: 0.05,
            min_value: Some(0.0),
            max_value: Some(100.0),
            avg_width: 4.0,
        },
    );
    user_stats.column_stats.insert(
        "status".to_string(),
        ColumnStats {
            distinct_count: 5.0,
            null_fraction: 0.0,
            min_value: None,
            max_value: None,
            avg_width: 8.0,
        },
    );
    catalog.set_stats("users", user_stats);

    // ---- orders (50 000 rows, 500 pages) -----------------------------------
    catalog.add_table(
        "orders",
        Schema::new(vec![
            Column::new("id", DataType::Integer, false),
            Column::new("user_id", DataType::Integer, false),
            Column::new("amount", DataType::Float, false),
        ]),
    );

    let mut order_stats = TableStats {
        row_count: 50000.0,
        page_count: 500.0,
        column_stats: HashMap::new(),
    };
    order_stats.column_stats.insert(
        "id".to_string(),
        ColumnStats {
            distinct_count: 50000.0,
            null_fraction: 0.0,
            min_value: None,
            max_value: None,
            avg_width: 4.0,
        },
    );
    order_stats.column_stats.insert(
        "user_id".to_string(),
        ColumnStats {
            distinct_count: 10000.0,
            null_fraction: 0.0,
            min_value: None,
            max_value: None,
            avg_width: 4.0,
        },
    );
    order_stats.column_stats.insert(
        "amount".to_string(),
        ColumnStats {
            distinct_count: 1000.0,
            null_fraction: 0.0,
            min_value: Some(0.0),
            max_value: Some(10000.0),
            avg_width: 8.0,
        },
    );
    catalog.set_stats("orders", order_stats);

    catalog
}

// ---------------------------------------------------------------------------
// Plan helpers
// ---------------------------------------------------------------------------

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

fn filter(pred: Expr, input: LogicalPlan) -> LogicalPlan {
    LogicalPlan::Filter {
        predicate: pred,
        input: Box::new(input),
    }
}

// ---------------------------------------------------------------------------
// Expression helpers
// ---------------------------------------------------------------------------

fn col(name: &str) -> Expr {
    Expr::Identifier(name.to_string())
}

fn qcol(t: &str, c: &str) -> Expr {
    Expr::QualifiedIdentifier {
        table: t.to_string(),
        column: c.to_string(),
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

fn lt(l: Expr, r: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(l),
        op: BinaryOp::Lt,
        right: Box::new(r),
    }
}

fn lit_int(n: i64) -> Expr {
    Expr::Integer(n)
}

fn lit_float(n: f64) -> Expr {
    Expr::Float(n)
}

fn and(l: Expr, r: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(l),
        op: BinaryOp::And,
        right: Box::new(r),
    }
}

fn or(l: Expr, r: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(l),
        op: BinaryOp::Or,
        right: Box::new(r),
    }
}

// ---------------------------------------------------------------------------
// 1. test_scan_cardinality
// ---------------------------------------------------------------------------

#[test]
fn test_scan_cardinality() {
    let catalog = make_catalog();
    let estimator = CardinalityEstimator::new(&catalog);
    let plan = scan("users", &catalog);

    let result = estimator.estimate(&plan);
    assert!(
        (result - 10000.0).abs() < 1.0,
        "expected ~10000.0, got {}",
        result
    );
}

// ---------------------------------------------------------------------------
// 2. test_scan_cardinality_orders
// ---------------------------------------------------------------------------

#[test]
fn test_scan_cardinality_orders() {
    let catalog = make_catalog();
    let estimator = CardinalityEstimator::new(&catalog);
    let plan = scan("orders", &catalog);

    let result = estimator.estimate(&plan);
    assert!(
        (result - 50000.0).abs() < 1.0,
        "expected ~50000.0, got {}",
        result
    );
}

// ---------------------------------------------------------------------------
// 3. test_equality_selectivity
//    users.status = 'active'  → 1/5 = 0.2  → 10000 * 0.2 = 2000 rows
// ---------------------------------------------------------------------------

#[test]
fn test_equality_selectivity() {
    let catalog = make_catalog();
    let estimator = CardinalityEstimator::new(&catalog);

    let pred = eq(qcol("users", "status"), Expr::String("active".to_string()));
    let plan = filter(pred, scan("users", &catalog));

    let result = estimator.estimate(&plan);
    let expected = 2000.0;
    assert!(
        (result - expected).abs() < 100.0,
        "expected ~{}, got {}",
        expected,
        result
    );
}

// ---------------------------------------------------------------------------
// 4. test_range_selectivity
//    users.age > 50  → (100-50)/(100-0) = 0.5  → 10000 * 0.5 = 5000 rows
// ---------------------------------------------------------------------------

#[test]
fn test_range_selectivity() {
    let catalog = make_catalog();
    let estimator = CardinalityEstimator::new(&catalog);

    let pred = gt(qcol("users", "age"), lit_int(50));
    let plan = filter(pred, scan("users", &catalog));

    let result = estimator.estimate(&plan);
    let expected = 5000.0;
    assert!(
        (result - expected).abs() < 500.0,
        "expected ~{}, got {}",
        expected,
        result
    );
}

// ---------------------------------------------------------------------------
// 5. test_between_selectivity
//    users.age BETWEEN 20 AND 40  → (40-20)/(100-0) = 0.2  → 2000 rows
// ---------------------------------------------------------------------------

#[test]
fn test_between_selectivity() {
    let catalog = make_catalog();
    let estimator = CardinalityEstimator::new(&catalog);

    let pred = Expr::Between {
        expr: Box::new(qcol("users", "age")),
        low: Box::new(lit_int(20)),
        high: Box::new(lit_int(40)),
        negated: false,
    };
    let plan = filter(pred, scan("users", &catalog));

    let result = estimator.estimate(&plan);
    let expected = 2000.0;
    assert!(
        (result - expected).abs() < 200.0,
        "expected ~{}, got {}",
        expected,
        result
    );
}

// ---------------------------------------------------------------------------
// 6. test_null_selectivity
//    users.age IS NULL  → null_fraction=0.05  → 10000 * 0.05 = 500 rows
// ---------------------------------------------------------------------------

#[test]
fn test_null_selectivity() {
    let catalog = make_catalog();
    let estimator = CardinalityEstimator::new(&catalog);

    let pred = Expr::IsNull {
        expr: Box::new(qcol("users", "age")),
        negated: false,
    };
    let plan = filter(pred, scan("users", &catalog));

    let result = estimator.estimate(&plan);
    let expected = 500.0;
    assert!(
        (result - expected).abs() < 50.0,
        "expected ~{}, got {}",
        expected,
        result
    );
}

// ---------------------------------------------------------------------------
// 7. test_in_list_selectivity
//    users.status IN ('a','b','c')  → 3/5 = 0.6  → 6000 rows
// ---------------------------------------------------------------------------

#[test]
fn test_in_list_selectivity() {
    let catalog = make_catalog();
    let estimator = CardinalityEstimator::new(&catalog);

    let pred = Expr::InList {
        expr: Box::new(qcol("users", "status")),
        list: vec![
            Expr::String("a".to_string()),
            Expr::String("b".to_string()),
            Expr::String("c".to_string()),
        ],
        negated: false,
    };
    let plan = filter(pred, scan("users", &catalog));

    let result = estimator.estimate(&plan);
    let expected = 6000.0;
    assert!(
        (result - expected).abs() < 600.0,
        "expected ~{}, got {}",
        expected,
        result
    );
}

// ---------------------------------------------------------------------------
// 8. test_and_selectivity
//    age > 50 AND status = 'active'
//    → sel = 0.5 * 0.2 = 0.1  → 10000 * 0.1 = 1000 rows
// ---------------------------------------------------------------------------

#[test]
fn test_and_selectivity() {
    let catalog = make_catalog();
    let estimator = CardinalityEstimator::new(&catalog);

    let pred = and(
        gt(qcol("users", "age"), lit_int(50)),
        eq(qcol("users", "status"), Expr::String("active".to_string())),
    );
    let plan = filter(pred, scan("users", &catalog));

    let result = estimator.estimate(&plan);
    // product of individual selectivities: 0.5 * 0.2 = 0.1 → 1000 rows
    let expected = 1000.0;
    assert!(
        (result - expected).abs() < 200.0,
        "expected ~{}, got {}",
        expected,
        result
    );
}

// ---------------------------------------------------------------------------
// 9. test_or_selectivity
//    age > 50 OR status = 'active'
//    → sel = 0.5 + 0.2 - 0.5*0.2 = 0.6  → 6000 rows
// ---------------------------------------------------------------------------

#[test]
fn test_or_selectivity() {
    let catalog = make_catalog();
    let estimator = CardinalityEstimator::new(&catalog);

    let pred = or(
        gt(qcol("users", "age"), lit_int(50)),
        eq(qcol("users", "status"), Expr::String("active".to_string())),
    );
    let plan = filter(pred, scan("users", &catalog));

    let result = estimator.estimate(&plan);
    // union formula: 0.5 + 0.2 - 0.1 = 0.6 → 6000 rows
    let expected = 6000.0;
    assert!(
        (result - expected).abs() < 600.0,
        "expected ~{}, got {}",
        expected,
        result
    );
}

// ---------------------------------------------------------------------------
// 10. test_not_selectivity
//     NOT (age > 50)  → 1 - 0.5 = 0.5  → 5000 rows
// ---------------------------------------------------------------------------

#[test]
fn test_not_selectivity() {
    let catalog = make_catalog();
    let estimator = CardinalityEstimator::new(&catalog);

    let pred = Expr::UnaryOp {
        op: UnaryOp::Not,
        expr: Box::new(gt(qcol("users", "age"), lit_int(50))),
    };
    let plan = filter(pred, scan("users", &catalog));

    let result = estimator.estimate(&plan);
    let expected = 5000.0;
    assert!(
        (result - expected).abs() < 500.0,
        "expected ~{}, got {}",
        expected,
        result
    );
}

// ---------------------------------------------------------------------------
// 11. test_join_cardinality_equi
//     INNER JOIN users and orders ON users.id = orders.user_id
//     → |L| * |R| / max(ndv_L, ndv_R) = 10000 * 50000 / max(10000, 10000)
//     = 500_000_000 / 10000 = 50000 rows
// ---------------------------------------------------------------------------

#[test]
fn test_join_cardinality_equi() {
    let catalog = make_catalog();
    let estimator = CardinalityEstimator::new(&catalog);

    let left = scan("users", &catalog);
    let right = scan("orders", &catalog);
    let left_schema = left.schema();
    let right_schema = right.schema();
    let joined_schema = left_schema.merge(&right_schema);

    let condition = eq(qcol("users", "id"), qcol("orders", "user_id"));

    let plan = LogicalPlan::Join {
        join_type: JoinType::Inner,
        condition: Some(condition),
        left: Box::new(left),
        right: Box::new(right),
        schema: joined_schema,
    };

    let result = estimator.estimate(&plan);
    // 10000 * 50000 / max(10000, 10000) = 50000
    let expected = 50000.0;
    assert!(
        (result - expected).abs() < 5000.0,
        "expected ~{}, got {}",
        expected,
        result
    );
}

// ---------------------------------------------------------------------------
// 12. test_aggregate_cardinality
//     GROUP BY status  → distinct_count(status) = 5 groups
// ---------------------------------------------------------------------------

#[test]
fn test_aggregate_cardinality() {
    let catalog = make_catalog();
    let estimator = CardinalityEstimator::new(&catalog);

    let input = scan("users", &catalog);
    let schema = input.schema();

    let plan = LogicalPlan::Aggregate {
        group_by: vec![qcol("users", "status")],
        aggregates: vec![],
        input: Box::new(input),
        schema,
    };

    let result = estimator.estimate(&plan);
    let expected = 5.0;
    assert!(
        (result - expected).abs() < 2.0,
        "expected ~{} groups, got {}",
        expected,
        result
    );
}

// ---------------------------------------------------------------------------
// 13. test_limit_cardinality
//     LIMIT 10 on 10000 rows  → min(10000, 10) = 10
// ---------------------------------------------------------------------------

#[test]
fn test_limit_cardinality() {
    let catalog = make_catalog();
    let estimator = CardinalityEstimator::new(&catalog);

    let plan = LogicalPlan::Limit {
        count: Some(10),
        offset: 0,
        input: Box::new(scan("users", &catalog)),
    };

    let result = estimator.estimate(&plan);
    let expected = 10.0;
    assert!(
        (result - expected).abs() < 1.0,
        "expected ~{}, got {}",
        expected,
        result
    );
}

// ---------------------------------------------------------------------------
// 14. test_distinct_cardinality
//     Distinct on 10000 rows  → 10000 * 0.8 = 8000
// ---------------------------------------------------------------------------

#[test]
fn test_distinct_cardinality() {
    let catalog = make_catalog();
    let estimator = CardinalityEstimator::new(&catalog);

    let plan = LogicalPlan::Distinct {
        input: Box::new(scan("users", &catalog)),
    };

    let result = estimator.estimate(&plan);
    let expected = 8000.0;
    assert!(
        (result - expected).abs() < 500.0,
        "expected ~{}, got {}",
        expected,
        result
    );
}

// ---------------------------------------------------------------------------
// 15. test_values_cardinality
//     Values with 3 rows  → 3.0
// ---------------------------------------------------------------------------

#[test]
fn test_values_cardinality() {
    let catalog = make_catalog();
    let estimator = CardinalityEstimator::new(&catalog);

    let plan = LogicalPlan::Values {
        rows: vec![
            vec![lit_int(1), lit_int(10)],
            vec![lit_int(2), lit_int(20)],
            vec![lit_int(3), lit_int(30)],
        ],
        schema: Schema::new(vec![
            Column::new("id", DataType::Integer, false),
            Column::new("val", DataType::Integer, false),
        ]),
    };

    let result = estimator.estimate(&plan);
    assert!(
        (result - 3.0).abs() < 0.1,
        "expected 3.0, got {}",
        result
    );
}
