use sqld::planner::logical_plan::*;
use sqld::planner::rules::constant_folding::ConstantFolding;
use sqld::planner::rules::OptimizationRule;
use sqld::sql::ast::*;
use sqld::types::{Column, DataType, Schema};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_scan() -> LogicalPlan {
    LogicalPlan::Scan {
        table: "t".into(),
        alias: None,
        schema: Schema::new(vec![Column::new("x", DataType::Integer, true)]),
    }
}

/// Wrap `expr` in a Filter over a simple Scan, apply ConstantFolding, and
/// return the resulting plan so tests can inspect it.
fn apply_filter(expr: Expr) -> LogicalPlan {
    let plan = LogicalPlan::Filter {
        predicate: expr,
        input: Box::new(make_scan()),
    };
    ConstantFolding.apply(plan)
}

/// Extract the predicate from a Filter node, panicking if the plan is not a Filter.
fn predicate_of(plan: &LogicalPlan) -> &Expr {
    match plan {
        LogicalPlan::Filter { predicate, .. } => predicate,
        other => panic!("expected Filter, got {}", other.node_name()),
    }
}

// ---------------------------------------------------------------------------
// 1. 1 + 2 = 3 → true → Filter eliminated
// ---------------------------------------------------------------------------

#[test]
fn test_fold_integer_addition() {
    // Predicate: 1 + 2 = 3  →  3 = 3  →  true  →  Filter removed
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Integer(1)),
            op: BinaryOp::Add,
            right: Box::new(Expr::Integer(2)),
        }),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Integer(3)),
    };
    let result = apply_filter(expr);
    assert!(
        matches!(result, LogicalPlan::Scan { .. }),
        "Filter with 1+2=3 (folds to true) should be eliminated, got {}",
        result.node_name()
    );
}

// ---------------------------------------------------------------------------
// 2. 10 - 3 → 7
// ---------------------------------------------------------------------------

#[test]
fn test_fold_integer_subtraction() {
    // Predicate: (10 - 3) = 7  →  7 = 7  →  true  →  Filter removed
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Integer(10)),
            op: BinaryOp::Sub,
            right: Box::new(Expr::Integer(3)),
        }),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Integer(7)),
    };
    let result = apply_filter(expr);
    assert!(
        matches!(result, LogicalPlan::Scan { .. }),
        "10-3=7 folds to true; filter should be eliminated, got {}",
        result.node_name()
    );
}

// ---------------------------------------------------------------------------
// 3. 3 * 4 → 12
// ---------------------------------------------------------------------------

#[test]
fn test_fold_integer_multiplication() {
    // Predicate: (3 * 4) = 12  →  12 = 12  →  true  →  Filter removed
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Integer(3)),
            op: BinaryOp::Mul,
            right: Box::new(Expr::Integer(4)),
        }),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Integer(12)),
    };
    let result = apply_filter(expr);
    assert!(
        matches!(result, LogicalPlan::Scan { .. }),
        "3*4=12 folds to true; filter should be eliminated, got {}",
        result.node_name()
    );
}

// ---------------------------------------------------------------------------
// 4. 10 / 2 → 5
// ---------------------------------------------------------------------------

#[test]
fn test_fold_integer_division() {
    // Predicate: (10 / 2) = 5  →  5 = 5  →  true  →  Filter removed
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::Integer(10)),
            op: BinaryOp::Div,
            right: Box::new(Expr::Integer(2)),
        }),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Integer(5)),
    };
    let result = apply_filter(expr);
    assert!(
        matches!(result, LogicalPlan::Scan { .. }),
        "10/2=5 folds to true; filter should be eliminated, got {}",
        result.node_name()
    );
}

// ---------------------------------------------------------------------------
// 5. 5 > 3 → true
// ---------------------------------------------------------------------------

#[test]
fn test_fold_integer_comparison() {
    // Predicate: 5 > 3  →  true  →  Filter removed
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Integer(5)),
        op: BinaryOp::Gt,
        right: Box::new(Expr::Integer(3)),
    };
    let result = apply_filter(expr);
    assert!(
        matches!(result, LogicalPlan::Scan { .. }),
        "5>3 folds to true; filter should be eliminated, got {}",
        result.node_name()
    );
}

// ---------------------------------------------------------------------------
// 6. 'hello' || ' world' → 'hello world'
// ---------------------------------------------------------------------------

#[test]
fn test_fold_string_concat() {
    // The concat of two string literals should fold to a single string literal.
    // Wrap in a Filter so we can inspect the folded predicate via apply_filter.
    // Predicate: ('hello' || ' world') IS NULL  →  the concat folds to
    // Expr::String("hello world"), then IS NULL on a String literal → false → Empty.
    let expr = Expr::IsNull {
        expr: Box::new(Expr::BinaryOp {
            left: Box::new(Expr::String("hello".into())),
            op: BinaryOp::Concat,
            right: Box::new(Expr::String(" world".into())),
        }),
        negated: false,
    };
    // 'hello world' IS NULL → false → Empty
    let result = apply_filter(expr);
    assert!(
        matches!(result, LogicalPlan::Empty { .. }),
        "('hello'||' world') IS NULL should fold concat then IS NULL on literal to false (Empty), got {}",
        result.node_name()
    );
}

// ---------------------------------------------------------------------------
// 7. x AND true → x  (variable part kept)
// ---------------------------------------------------------------------------

#[test]
fn test_fold_boolean_and_true() {
    // x AND true → x  (Filter stays with predicate = Identifier("x"))
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("x".into())),
        op: BinaryOp::And,
        right: Box::new(Expr::Boolean(true)),
    };
    let result = apply_filter(expr);
    assert!(
        matches!(result, LogicalPlan::Filter { .. }),
        "x AND true should keep filter (predicate=x), got {}",
        result.node_name()
    );
    assert_eq!(
        predicate_of(&result),
        &Expr::Identifier("x".into()),
        "predicate after folding x AND true should be x"
    );
}

// ---------------------------------------------------------------------------
// 8. x AND false → false → Empty
// ---------------------------------------------------------------------------

#[test]
fn test_fold_boolean_and_false() {
    // x AND false → false  →  Empty
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("x".into())),
        op: BinaryOp::And,
        right: Box::new(Expr::Boolean(false)),
    };
    let result = apply_filter(expr);
    assert!(
        matches!(result, LogicalPlan::Empty { .. }),
        "x AND false folds to false; filter should become Empty, got {}",
        result.node_name()
    );
}

// ---------------------------------------------------------------------------
// 9. x OR true → true → Filter eliminated
// ---------------------------------------------------------------------------

#[test]
fn test_fold_boolean_or_true() {
    // x OR true → true  →  Filter removed
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("x".into())),
        op: BinaryOp::Or,
        right: Box::new(Expr::Boolean(true)),
    };
    let result = apply_filter(expr);
    assert!(
        matches!(result, LogicalPlan::Scan { .. }),
        "x OR true folds to true; filter should be eliminated, got {}",
        result.node_name()
    );
}

// ---------------------------------------------------------------------------
// 10. x OR false → x  (variable part kept)
// ---------------------------------------------------------------------------

#[test]
fn test_fold_boolean_or_false() {
    // x OR false → x  (Filter stays with predicate = Identifier("x"))
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("x".into())),
        op: BinaryOp::Or,
        right: Box::new(Expr::Boolean(false)),
    };
    let result = apply_filter(expr);
    assert!(
        matches!(result, LogicalPlan::Filter { .. }),
        "x OR false should keep filter (predicate=x), got {}",
        result.node_name()
    );
    assert_eq!(
        predicate_of(&result),
        &Expr::Identifier("x".into()),
        "predicate after folding x OR false should be x"
    );
}

// ---------------------------------------------------------------------------
// 11. NOT NOT x → x
// ---------------------------------------------------------------------------

#[test]
fn test_fold_not_not() {
    // NOT NOT x → x  (Filter stays with predicate = Identifier("x"))
    let expr = Expr::UnaryOp {
        op: UnaryOp::Not,
        expr: Box::new(Expr::UnaryOp {
            op: UnaryOp::Not,
            expr: Box::new(Expr::Identifier("x".into())),
        }),
    };
    let result = apply_filter(expr);
    assert!(
        matches!(result, LogicalPlan::Filter { .. }),
        "NOT NOT x should keep filter (predicate=x), got {}",
        result.node_name()
    );
    assert_eq!(
        predicate_of(&result),
        &Expr::Identifier("x".into()),
        "predicate after folding NOT NOT x should be x"
    );
}

// ---------------------------------------------------------------------------
// 12. 5 IS NULL → false → Empty
// ---------------------------------------------------------------------------

#[test]
fn test_fold_is_null_on_literal() {
    // 5 IS NULL → false  →  Empty
    let expr = Expr::IsNull {
        expr: Box::new(Expr::Integer(5)),
        negated: false,
    };
    let result = apply_filter(expr);
    assert!(
        matches!(result, LogicalPlan::Empty { .. }),
        "5 IS NULL folds to false; filter should become Empty, got {}",
        result.node_name()
    );
}

// ---------------------------------------------------------------------------
// 13. NULL IS NULL → true → Filter eliminated
// ---------------------------------------------------------------------------

#[test]
fn test_fold_is_null_on_null() {
    // NULL IS NULL → true  →  Filter removed
    let expr = Expr::IsNull {
        expr: Box::new(Expr::Null),
        negated: false,
    };
    let result = apply_filter(expr);
    assert!(
        matches!(result, LogicalPlan::Scan { .. }),
        "NULL IS NULL folds to true; filter should be eliminated, got {}",
        result.node_name()
    );
}

// ---------------------------------------------------------------------------
// 14. Filter with predicate that folds to true → Filter removed
// ---------------------------------------------------------------------------

#[test]
fn test_filter_true_eliminated() {
    let scan = make_scan();
    let plan = LogicalPlan::Filter {
        predicate: Expr::Boolean(true),
        input: Box::new(scan),
    };
    let result = ConstantFolding.apply(plan);
    assert!(
        matches!(result, LogicalPlan::Scan { .. }),
        "Filter(true) should be eliminated, leaving Scan, got {}",
        result.node_name()
    );
}

// ---------------------------------------------------------------------------
// 15. Filter with predicate that folds to false → Empty node
// ---------------------------------------------------------------------------

#[test]
fn test_filter_false_becomes_empty() {
    let scan = make_scan();
    let plan = LogicalPlan::Filter {
        predicate: Expr::Boolean(false),
        input: Box::new(scan),
    };
    let result = ConstantFolding.apply(plan);
    assert!(
        matches!(result, LogicalPlan::Empty { .. }),
        "Filter(false) should become Empty, got {}",
        result.node_name()
    );
}
