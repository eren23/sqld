use sqld::executor::expr_eval::{compile_expr, evaluate_expr, ExprOp};
use sqld::sql::ast::{BinaryOp, Expr, UnaryOp, WhenClause};
use sqld::types::{Column, DataType, Datum, MvccHeader, Schema, Tuple};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_schema() -> Schema {
    Schema::new(vec![
        Column::new("a", DataType::Integer, true),
        Column::new("b", DataType::Integer, true),
        Column::new("c", DataType::Text, true),
    ])
}

fn test_tuple() -> Tuple {
    Tuple::new(
        MvccHeader::new(0, 0, 0),
        vec![
            Datum::Integer(10),
            Datum::Integer(20),
            Datum::Text("hello".into()),
        ],
    )
}

/// Compile an expression against the test schema and evaluate it against the
/// test tuple, returning the resulting Datum.
fn run_expr(expr: &Expr) -> Datum {
    let schema = test_schema();
    let tuple = test_tuple();
    let ops = compile_expr(expr, &schema).expect("compile_expr failed");
    evaluate_expr(&ops, &tuple).expect("evaluate_expr failed")
}

// ===========================================================================
// 1. literal integer
// ===========================================================================

#[test]
fn eval_literal_integer() {
    let expr = Expr::Integer(42);
    assert_eq!(run_expr(&expr), Datum::Integer(42));
}

// ===========================================================================
// 2. literal string
// ===========================================================================

#[test]
fn eval_literal_string() {
    let expr = Expr::String("world".into());
    assert_eq!(run_expr(&expr), Datum::Text("world".into()));
}

// ===========================================================================
// 3. literal null
// ===========================================================================

#[test]
fn eval_literal_null() {
    let expr = Expr::Null;
    assert_eq!(run_expr(&expr), Datum::Null);
}

// ===========================================================================
// 4. column ref
// ===========================================================================

#[test]
fn eval_column_ref() {
    // Column "a" is at ordinal 0 and holds Integer(10)
    let expr = Expr::Identifier("a".into());
    assert_eq!(run_expr(&expr), Datum::Integer(10));

    // Column "c" is at ordinal 2 and holds Text("hello")
    let expr_c = Expr::Identifier("c".into());
    assert_eq!(run_expr(&expr_c), Datum::Text("hello".into()));
}

// ===========================================================================
// 5. add  (a + b = 30)
// ===========================================================================

#[test]
fn eval_add() {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("a".into())),
        op: BinaryOp::Add,
        right: Box::new(Expr::Identifier("b".into())),
    };
    assert_eq!(run_expr(&expr), Datum::Integer(30));
}

// ===========================================================================
// 6. sub  (b - a = 10)
// ===========================================================================

#[test]
fn eval_sub() {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("b".into())),
        op: BinaryOp::Sub,
        right: Box::new(Expr::Identifier("a".into())),
    };
    assert_eq!(run_expr(&expr), Datum::Integer(10));
}

// ===========================================================================
// 7. mul  (a * b = 200)
// ===========================================================================

#[test]
fn eval_mul() {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("a".into())),
        op: BinaryOp::Mul,
        right: Box::new(Expr::Identifier("b".into())),
    };
    assert_eq!(run_expr(&expr), Datum::Integer(200));
}

// ===========================================================================
// 8. div  (b / a = 2)
// ===========================================================================

#[test]
fn eval_div() {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("b".into())),
        op: BinaryOp::Div,
        right: Box::new(Expr::Identifier("a".into())),
    };
    assert_eq!(run_expr(&expr), Datum::Integer(2));
}

// ===========================================================================
// 9. mod  (b % a = 0)
// ===========================================================================

#[test]
fn eval_mod() {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("b".into())),
        op: BinaryOp::Mod,
        right: Box::new(Expr::Identifier("a".into())),
    };
    assert_eq!(run_expr(&expr), Datum::Integer(0));
}

// ===========================================================================
// 10. neg  (-a = -10)
// ===========================================================================

#[test]
fn eval_neg() {
    let expr = Expr::UnaryOp {
        op: UnaryOp::Minus,
        expr: Box::new(Expr::Identifier("a".into())),
    };
    assert_eq!(run_expr(&expr), Datum::Integer(-10));
}

// ===========================================================================
// 11. comparison eq  (a = 10 -> true)
// ===========================================================================

#[test]
fn eval_comparison_eq() {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("a".into())),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Integer(10)),
    };
    assert_eq!(run_expr(&expr), Datum::Boolean(true));
}

// ===========================================================================
// 12. comparison lt  (a < b -> true)
// ===========================================================================

#[test]
fn eval_comparison_lt() {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("a".into())),
        op: BinaryOp::Lt,
        right: Box::new(Expr::Identifier("b".into())),
    };
    assert_eq!(run_expr(&expr), Datum::Boolean(true));
}

// ===========================================================================
// 13. comparison gt  (b > a -> true)
// ===========================================================================

#[test]
fn eval_comparison_gt() {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("b".into())),
        op: BinaryOp::Gt,
        right: Box::new(Expr::Identifier("a".into())),
    };
    assert_eq!(run_expr(&expr), Datum::Boolean(true));
}

// ===========================================================================
// 14. and / or
// ===========================================================================

#[test]
fn eval_and_or() {
    // true AND false = false
    let and_expr = Expr::BinaryOp {
        left: Box::new(Expr::Boolean(true)),
        op: BinaryOp::And,
        right: Box::new(Expr::Boolean(false)),
    };
    assert_eq!(run_expr(&and_expr), Datum::Boolean(false));

    // true OR false = true
    let or_expr = Expr::BinaryOp {
        left: Box::new(Expr::Boolean(true)),
        op: BinaryOp::Or,
        right: Box::new(Expr::Boolean(false)),
    };
    assert_eq!(run_expr(&or_expr), Datum::Boolean(true));
}

// ===========================================================================
// 15. not
// ===========================================================================

#[test]
fn eval_not() {
    let expr = Expr::UnaryOp {
        op: UnaryOp::Not,
        expr: Box::new(Expr::Boolean(true)),
    };
    assert_eq!(run_expr(&expr), Datum::Boolean(false));

    let expr_false = Expr::UnaryOp {
        op: UnaryOp::Not,
        expr: Box::new(Expr::Boolean(false)),
    };
    assert_eq!(run_expr(&expr_false), Datum::Boolean(true));
}

// ===========================================================================
// 16. is null
// ===========================================================================

#[test]
fn eval_is_null() {
    // NULL IS NULL -> true
    let expr_null = Expr::IsNull {
        expr: Box::new(Expr::Null),
        negated: false,
    };
    assert_eq!(run_expr(&expr_null), Datum::Boolean(true));

    // 10 IS NULL -> false
    let expr_not_null = Expr::IsNull {
        expr: Box::new(Expr::Integer(10)),
        negated: false,
    };
    assert_eq!(run_expr(&expr_not_null), Datum::Boolean(false));
}

// ===========================================================================
// 17. is not null
// ===========================================================================

#[test]
fn eval_is_not_null() {
    // 10 IS NOT NULL -> true
    let expr = Expr::IsNull {
        expr: Box::new(Expr::Integer(10)),
        negated: true,
    };
    assert_eq!(run_expr(&expr), Datum::Boolean(true));

    // NULL IS NOT NULL -> false
    let expr_null = Expr::IsNull {
        expr: Box::new(Expr::Null),
        negated: true,
    };
    assert_eq!(run_expr(&expr_null), Datum::Boolean(false));
}

// ===========================================================================
// 18. between  (a BETWEEN 5 AND 15 -> true)
// ===========================================================================

#[test]
fn eval_between() {
    let expr = Expr::Between {
        expr: Box::new(Expr::Identifier("a".into())),
        low: Box::new(Expr::Integer(5)),
        high: Box::new(Expr::Integer(15)),
        negated: false,
    };
    assert_eq!(run_expr(&expr), Datum::Boolean(true));

    // a NOT BETWEEN 5 AND 15 -> false
    let expr_neg = Expr::Between {
        expr: Box::new(Expr::Identifier("a".into())),
        low: Box::new(Expr::Integer(5)),
        high: Box::new(Expr::Integer(15)),
        negated: true,
    };
    assert_eq!(run_expr(&expr_neg), Datum::Boolean(false));

    // a BETWEEN 11 AND 20 -> false (a=10, not in [11,20])
    let expr_out = Expr::Between {
        expr: Box::new(Expr::Identifier("a".into())),
        low: Box::new(Expr::Integer(11)),
        high: Box::new(Expr::Integer(20)),
        negated: false,
    };
    assert_eq!(run_expr(&expr_out), Datum::Boolean(false));
}

// ===========================================================================
// 19. in list  (a IN (5, 10, 15) -> true)
// ===========================================================================

#[test]
fn eval_in_list() {
    let expr = Expr::InList {
        expr: Box::new(Expr::Identifier("a".into())),
        list: vec![Expr::Integer(5), Expr::Integer(10), Expr::Integer(15)],
        negated: false,
    };
    assert_eq!(run_expr(&expr), Datum::Boolean(true));

    // a NOT IN (5, 10, 15) -> false
    let expr_neg = Expr::InList {
        expr: Box::new(Expr::Identifier("a".into())),
        list: vec![Expr::Integer(5), Expr::Integer(10), Expr::Integer(15)],
        negated: true,
    };
    assert_eq!(run_expr(&expr_neg), Datum::Boolean(false));

    // a IN (1, 2, 3) -> false
    let expr_miss = Expr::InList {
        expr: Box::new(Expr::Identifier("a".into())),
        list: vec![Expr::Integer(1), Expr::Integer(2), Expr::Integer(3)],
        negated: false,
    };
    assert_eq!(run_expr(&expr_miss), Datum::Boolean(false));
}

// ===========================================================================
// 20. like  (c LIKE 'hel%' -> true)
// ===========================================================================

#[test]
fn eval_like() {
    let expr = Expr::Like {
        expr: Box::new(Expr::Identifier("c".into())),
        pattern: Box::new(Expr::String("hel%".into())),
        negated: false,
        case_insensitive: false,
    };
    assert_eq!(run_expr(&expr), Datum::Boolean(true));

    // c LIKE 'world%' -> false
    let expr_no = Expr::Like {
        expr: Box::new(Expr::Identifier("c".into())),
        pattern: Box::new(Expr::String("world%".into())),
        negated: false,
        case_insensitive: false,
    };
    assert_eq!(run_expr(&expr_no), Datum::Boolean(false));

    // c LIKE 'h_llo' -> true (underscore matches single char)
    let expr_underscore = Expr::Like {
        expr: Box::new(Expr::Identifier("c".into())),
        pattern: Box::new(Expr::String("h_llo".into())),
        negated: false,
        case_insensitive: false,
    };
    assert_eq!(run_expr(&expr_underscore), Datum::Boolean(true));
}

// ===========================================================================
// 21. like case insensitive  (c ILIKE 'HEL%' -> true)
// ===========================================================================

#[test]
fn eval_like_case_insensitive() {
    let expr = Expr::Like {
        expr: Box::new(Expr::Identifier("c".into())),
        pattern: Box::new(Expr::String("HEL%".into())),
        negated: false,
        case_insensitive: true,
    };
    assert_eq!(run_expr(&expr), Datum::Boolean(true));

    // ILIKE 'HELLO' -> true (exact match, case insensitive)
    let expr_exact = Expr::Like {
        expr: Box::new(Expr::Identifier("c".into())),
        pattern: Box::new(Expr::String("HELLO".into())),
        negated: false,
        case_insensitive: true,
    };
    assert_eq!(run_expr(&expr_exact), Datum::Boolean(true));
}

// ===========================================================================
// 22. case  (CASE WHEN a > 5 THEN 'big' ELSE 'small' END)
// ===========================================================================

#[test]
fn eval_case() {
    // a=10 > 5, so result should be 'big'
    let expr = Expr::Case {
        operand: None,
        when_clauses: vec![WhenClause {
            condition: Expr::BinaryOp {
                left: Box::new(Expr::Identifier("a".into())),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Integer(5)),
            },
            result: Expr::String("big".into()),
        }],
        else_clause: Some(Box::new(Expr::String("small".into()))),
    };
    assert_eq!(run_expr(&expr), Datum::Text("big".into()));

    // CASE WHEN a > 100 THEN 'big' ELSE 'small' END -> 'small'
    let expr_else = Expr::Case {
        operand: None,
        when_clauses: vec![WhenClause {
            condition: Expr::BinaryOp {
                left: Box::new(Expr::Identifier("a".into())),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Integer(100)),
            },
            result: Expr::String("big".into()),
        }],
        else_clause: Some(Box::new(Expr::String("small".into()))),
    };
    assert_eq!(run_expr(&expr_else), Datum::Text("small".into()));
}

// ===========================================================================
// 23. cast  (CAST(a AS FLOAT))
// ===========================================================================

#[test]
fn eval_cast() {
    // a=10 (Integer) cast to Float -> Float(10.0)
    let expr = Expr::Cast {
        expr: Box::new(Expr::Identifier("a".into())),
        data_type: DataType::Float,
    };
    assert_eq!(run_expr(&expr), Datum::Float(10.0));

    // CAST(a AS BIGINT) -> BigInt(10)
    let expr_bigint = Expr::Cast {
        expr: Box::new(Expr::Identifier("a".into())),
        data_type: DataType::BigInt,
    };
    assert_eq!(run_expr(&expr_bigint), Datum::BigInt(10));
}

// ===========================================================================
// 24. coalesce  (COALESCE(NULL, a) = 10)
// ===========================================================================

#[test]
fn eval_coalesce() {
    let expr = Expr::Coalesce(vec![Expr::Null, Expr::Identifier("a".into())]);
    assert_eq!(run_expr(&expr), Datum::Integer(10));

    // All NULLs -> NULL
    let expr_all_null = Expr::Coalesce(vec![Expr::Null, Expr::Null]);
    assert_eq!(run_expr(&expr_all_null), Datum::Null);

    // First non-null wins
    let expr_first = Expr::Coalesce(vec![
        Expr::Integer(99),
        Expr::Identifier("a".into()),
    ]);
    assert_eq!(run_expr(&expr_first), Datum::Integer(99));
}

// ===========================================================================
// 25. nullif  (NULLIF(a, 10) = NULL)
// ===========================================================================

#[test]
fn eval_nullif() {
    // NULLIF(a, 10): a=10, equal => NULL
    let expr = Expr::Nullif(
        Box::new(Expr::Identifier("a".into())),
        Box::new(Expr::Integer(10)),
    );
    assert_eq!(run_expr(&expr), Datum::Null);

    // NULLIF(a, 20): a=10 != 20 => returns a (10)
    let expr_diff = Expr::Nullif(
        Box::new(Expr::Identifier("a".into())),
        Box::new(Expr::Integer(20)),
    );
    assert_eq!(run_expr(&expr_diff), Datum::Integer(10));
}

// ===========================================================================
// 26. null propagation  (NULL + 5 = NULL)
// ===========================================================================

#[test]
fn eval_null_propagation() {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Null),
        op: BinaryOp::Add,
        right: Box::new(Expr::Integer(5)),
    };
    assert_eq!(run_expr(&expr), Datum::Null);

    // Also test subtraction with NULL
    let expr_sub = Expr::BinaryOp {
        left: Box::new(Expr::Integer(5)),
        op: BinaryOp::Sub,
        right: Box::new(Expr::Null),
    };
    assert_eq!(run_expr(&expr_sub), Datum::Null);

    // NULL * anything = NULL
    let expr_mul = Expr::BinaryOp {
        left: Box::new(Expr::Null),
        op: BinaryOp::Mul,
        right: Box::new(Expr::Integer(5)),
    };
    assert_eq!(run_expr(&expr_mul), Datum::Null);
}

// ===========================================================================
// 27. concat  ('hello' || ' world')
// ===========================================================================

#[test]
fn eval_concat() {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::String("hello".into())),
        op: BinaryOp::Concat,
        right: Box::new(Expr::String(" world".into())),
    };
    assert_eq!(run_expr(&expr), Datum::Text("hello world".into()));

    // NULL || 'x' = NULL (concat propagates nulls)
    let expr_null = Expr::BinaryOp {
        left: Box::new(Expr::Null),
        op: BinaryOp::Concat,
        right: Box::new(Expr::String("x".into())),
    };
    assert_eq!(run_expr(&expr_null), Datum::Null);
}

// ===========================================================================
// 28. function call  (length('hello') = 5)
// ===========================================================================

#[test]
fn eval_function_call() {
    let expr = Expr::FunctionCall {
        name: "length".into(),
        args: vec![Expr::String("hello".into())],
        distinct: false,
    };
    assert_eq!(run_expr(&expr), Datum::Integer(5));

    // length of empty string = 0
    let expr_empty = Expr::FunctionCall {
        name: "length".into(),
        args: vec![Expr::String("".into())],
        distinct: false,
    };
    assert_eq!(run_expr(&expr_empty), Datum::Integer(0));
}

// ===========================================================================
// 29. greatest / least
//     GREATEST(a, b) = 20,  LEAST(a, b) = 10
// ===========================================================================

#[test]
fn eval_greatest_least() {
    // GREATEST(a, b) where a=10, b=20 -> 20
    let expr_greatest = Expr::Greatest(vec![
        Expr::Identifier("a".into()),
        Expr::Identifier("b".into()),
    ]);
    assert_eq!(run_expr(&expr_greatest), Datum::Integer(20));

    // LEAST(a, b) where a=10, b=20 -> 10
    let expr_least = Expr::Least(vec![
        Expr::Identifier("a".into()),
        Expr::Identifier("b".into()),
    ]);
    assert_eq!(run_expr(&expr_least), Datum::Integer(10));

    // GREATEST with a NULL: should skip NULLs
    let expr_greatest_null = Expr::Greatest(vec![
        Expr::Null,
        Expr::Identifier("a".into()),
        Expr::Identifier("b".into()),
    ]);
    assert_eq!(run_expr(&expr_greatest_null), Datum::Integer(20));

    // LEAST with a NULL: should skip NULLs
    let expr_least_null = Expr::Least(vec![
        Expr::Null,
        Expr::Identifier("a".into()),
        Expr::Identifier("b".into()),
    ]);
    assert_eq!(run_expr(&expr_least_null), Datum::Integer(10));

    // All NULLs -> NULL
    let expr_all_null = Expr::Greatest(vec![Expr::Null, Expr::Null]);
    assert_eq!(run_expr(&expr_all_null), Datum::Null);
}

// ===========================================================================
// Additional coverage: compilation produces expected ops
// ===========================================================================

#[test]
fn compile_produces_correct_ops() {
    let schema = test_schema();

    // Simple literal
    let ops = compile_expr(&Expr::Integer(42), &schema).unwrap();
    assert_eq!(ops.len(), 1);
    assert!(matches!(ops[0], ExprOp::PushLiteral(Datum::Integer(42))));

    // Column reference
    let ops = compile_expr(&Expr::Identifier("b".into()), &schema).unwrap();
    assert_eq!(ops.len(), 1);
    assert!(matches!(ops[0], ExprOp::PushColumn(1)));

    // Binary add: two pushes + one Add
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("a".into())),
        op: BinaryOp::Add,
        right: Box::new(Expr::Identifier("b".into())),
    };
    let ops = compile_expr(&expr, &schema).unwrap();
    assert_eq!(ops.len(), 3);
    assert!(matches!(ops[2], ExprOp::Add));
}

// ===========================================================================
// Additional coverage: CASE with operand (simple CASE form)
// ===========================================================================

#[test]
fn eval_case_with_operand() {
    // CASE a WHEN 10 THEN 'ten' WHEN 20 THEN 'twenty' ELSE 'other' END
    let expr = Expr::Case {
        operand: Some(Box::new(Expr::Identifier("a".into()))),
        when_clauses: vec![
            WhenClause {
                condition: Expr::Integer(10),
                result: Expr::String("ten".into()),
            },
            WhenClause {
                condition: Expr::Integer(20),
                result: Expr::String("twenty".into()),
            },
        ],
        else_clause: Some(Box::new(Expr::String("other".into()))),
    };
    assert_eq!(run_expr(&expr), Datum::Text("ten".into()));
}

// ===========================================================================
// Additional coverage: CASE without else falls through to NULL
// ===========================================================================

#[test]
fn eval_case_no_else() {
    // CASE WHEN a > 100 THEN 'big' END -> NULL (no match, no else)
    let expr = Expr::Case {
        operand: None,
        when_clauses: vec![WhenClause {
            condition: Expr::BinaryOp {
                left: Box::new(Expr::Identifier("a".into())),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Integer(100)),
            },
            result: Expr::String("big".into()),
        }],
        else_clause: None,
    };
    assert_eq!(run_expr(&expr), Datum::Null);
}

// ===========================================================================
// Additional coverage: comparison operators (NotEq, LtEq, GtEq)
// ===========================================================================

#[test]
fn eval_comparison_not_eq() {
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("a".into())),
        op: BinaryOp::NotEq,
        right: Box::new(Expr::Identifier("b".into())),
    };
    assert_eq!(run_expr(&expr), Datum::Boolean(true));
}

#[test]
fn eval_comparison_lt_eq() {
    // a <= b (10 <= 20) -> true
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("a".into())),
        op: BinaryOp::LtEq,
        right: Box::new(Expr::Identifier("b".into())),
    };
    assert_eq!(run_expr(&expr), Datum::Boolean(true));

    // a <= a (10 <= 10) -> true
    let expr_eq = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("a".into())),
        op: BinaryOp::LtEq,
        right: Box::new(Expr::Identifier("a".into())),
    };
    assert_eq!(run_expr(&expr_eq), Datum::Boolean(true));
}

#[test]
fn eval_comparison_gt_eq() {
    // b >= a (20 >= 10) -> true
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("b".into())),
        op: BinaryOp::GtEq,
        right: Box::new(Expr::Identifier("a".into())),
    };
    assert_eq!(run_expr(&expr), Datum::Boolean(true));
}

// ===========================================================================
// Additional coverage: boolean literal
// ===========================================================================

#[test]
fn eval_literal_boolean() {
    assert_eq!(run_expr(&Expr::Boolean(true)), Datum::Boolean(true));
    assert_eq!(run_expr(&Expr::Boolean(false)), Datum::Boolean(false));
}

// ===========================================================================
// Additional coverage: float literal
// ===========================================================================

#[test]
fn eval_literal_float() {
    let expr = Expr::Float(3.14);
    assert_eq!(run_expr(&expr), Datum::Float(3.14));
}

// ===========================================================================
// Additional coverage: negation of NULL
// ===========================================================================

#[test]
fn eval_neg_null() {
    let expr = Expr::UnaryOp {
        op: UnaryOp::Minus,
        expr: Box::new(Expr::Null),
    };
    assert_eq!(run_expr(&expr), Datum::Null);
}

// ===========================================================================
// Additional coverage: NOT NULL -> NULL
// ===========================================================================

#[test]
fn eval_not_null() {
    let expr = Expr::UnaryOp {
        op: UnaryOp::Not,
        expr: Box::new(Expr::Null),
    };
    assert_eq!(run_expr(&expr), Datum::Null);
}

// ===========================================================================
// Additional coverage: IN list with NULL in list
// ===========================================================================

#[test]
fn eval_in_list_with_null() {
    // a IN (NULL, 10) -> true (10 matches)
    let expr = Expr::InList {
        expr: Box::new(Expr::Identifier("a".into())),
        list: vec![Expr::Null, Expr::Integer(10)],
        negated: false,
    };
    assert_eq!(run_expr(&expr), Datum::Boolean(true));

    // a IN (NULL, 99) -> NULL (no match but NULL present)
    let expr_null = Expr::InList {
        expr: Box::new(Expr::Identifier("a".into())),
        list: vec![Expr::Null, Expr::Integer(99)],
        negated: false,
    };
    assert_eq!(run_expr(&expr_null), Datum::Null);
}

// ===========================================================================
// Additional coverage: LIKE with NULL pattern or value
// ===========================================================================

#[test]
fn eval_like_null() {
    // NULL LIKE 'x%' -> NULL
    let expr = Expr::Like {
        expr: Box::new(Expr::Null),
        pattern: Box::new(Expr::String("x%".into())),
        negated: false,
        case_insensitive: false,
    };
    assert_eq!(run_expr(&expr), Datum::Null);

    // 'hello' LIKE NULL -> NULL
    let expr2 = Expr::Like {
        expr: Box::new(Expr::String("hello".into())),
        pattern: Box::new(Expr::Null),
        negated: false,
        case_insensitive: false,
    };
    assert_eq!(run_expr(&expr2), Datum::Null);
}

// ===========================================================================
// Additional coverage: BigInt literal (outside i32 range)
// ===========================================================================

#[test]
fn eval_literal_bigint() {
    let expr = Expr::Integer(5_000_000_000); // exceeds i32::MAX
    assert_eq!(run_expr(&expr), Datum::BigInt(5_000_000_000));
}

// ===========================================================================
// Additional coverage: unary plus is a no-op
// ===========================================================================

#[test]
fn eval_unary_plus() {
    let expr = Expr::UnaryOp {
        op: UnaryOp::Plus,
        expr: Box::new(Expr::Integer(42)),
    };
    assert_eq!(run_expr(&expr), Datum::Integer(42));
}

// ===========================================================================
// Additional coverage: NULL comparison yields NULL
// ===========================================================================

#[test]
fn eval_null_comparison() {
    // NULL = 10 -> NULL
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Null),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Integer(10)),
    };
    assert_eq!(run_expr(&expr), Datum::Null);

    // 10 < NULL -> NULL
    let expr_lt = Expr::BinaryOp {
        left: Box::new(Expr::Integer(10)),
        op: BinaryOp::Lt,
        right: Box::new(Expr::Null),
    };
    assert_eq!(run_expr(&expr_lt), Datum::Null);
}

// ===========================================================================
// Additional coverage: BETWEEN with NULL yields NULL
// ===========================================================================

#[test]
fn eval_between_null() {
    let expr = Expr::Between {
        expr: Box::new(Expr::Null),
        low: Box::new(Expr::Integer(1)),
        high: Box::new(Expr::Integer(10)),
        negated: false,
    };
    assert_eq!(run_expr(&expr), Datum::Null);
}

// ===========================================================================
// Additional coverage: column not found is an error
// ===========================================================================

#[test]
fn compile_column_not_found() {
    let schema = test_schema();
    let expr = Expr::Identifier("nonexistent".into());
    let result = compile_expr(&expr, &schema);
    assert!(result.is_err());
}
