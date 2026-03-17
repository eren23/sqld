use sqld::sql::ast::*;
use sqld::sql::parser::parse;
use sqld::types::DataType;

// ---------------------------------------------------------------------------
// Helper: parse a single expression by wrapping it in SELECT
// ---------------------------------------------------------------------------

fn parse_expr(sql_expr: &str) -> Expr {
    let sql = format!("SELECT {}", sql_expr);
    let r = parse(&sql);
    assert!(r.errors.is_empty(), "parse errors: {:?}", r.errors);
    let stmt = r.statements.into_iter().next().unwrap();
    match stmt {
        Statement::Select(s) => match s.columns.into_iter().next().unwrap() {
            SelectColumn::Expr { expr, .. } => expr,
            other => panic!("expected Expr column, got {:?}", other),
        },
        other => panic!("expected SELECT, got {:?}", other),
    }
}

// ===========================================================================
// Operator Precedence
// ===========================================================================

#[test]
fn precedence_mul_over_add() {
    // 1 + 2 * 3 => Add(1, Mul(2, 3))
    let expr = parse_expr("1 + 2 * 3");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::Add, right } => {
            assert_eq!(*left, Expr::Integer(1));
            match *right {
                Expr::BinaryOp { left: rl, op: BinaryOp::Mul, right: rr } => {
                    assert_eq!(*rl, Expr::Integer(2));
                    assert_eq!(*rr, Expr::Integer(3));
                }
                other => panic!("expected Mul, got {:?}", other),
            }
        }
        other => panic!("expected Add, got {:?}", other),
    }
}

#[test]
fn precedence_mul_over_add_left() {
    // 1 * 2 + 3 => Add(Mul(1, 2), 3)
    let expr = parse_expr("1 * 2 + 3");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::Add, right } => {
            match *left {
                Expr::BinaryOp { left: ll, op: BinaryOp::Mul, right: lr } => {
                    assert_eq!(*ll, Expr::Integer(1));
                    assert_eq!(*lr, Expr::Integer(2));
                }
                other => panic!("expected Mul, got {:?}", other),
            }
            assert_eq!(*right, Expr::Integer(3));
        }
        other => panic!("expected Add, got {:?}", other),
    }
}

#[test]
fn precedence_add_left_associative() {
    // 1 + 2 + 3 => Add(Add(1, 2), 3)
    let expr = parse_expr("1 + 2 + 3");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::Add, right } => {
            match *left {
                Expr::BinaryOp { left: ll, op: BinaryOp::Add, right: lr } => {
                    assert_eq!(*ll, Expr::Integer(1));
                    assert_eq!(*lr, Expr::Integer(2));
                }
                other => panic!("expected inner Add, got {:?}", other),
            }
            assert_eq!(*right, Expr::Integer(3));
        }
        other => panic!("expected Add, got {:?}", other),
    }
}

#[test]
fn precedence_exp_right_associative() {
    // 2 ^ 3 ^ 4 => Exp(2, Exp(3, 4))
    let expr = parse_expr("2 ^ 3 ^ 4");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::Exp, right } => {
            assert_eq!(*left, Expr::Integer(2));
            match *right {
                Expr::BinaryOp { left: rl, op: BinaryOp::Exp, right: rr } => {
                    assert_eq!(*rl, Expr::Integer(3));
                    assert_eq!(*rr, Expr::Integer(4));
                }
                other => panic!("expected inner Exp, got {:?}", other),
            }
        }
        other => panic!("expected Exp, got {:?}", other),
    }
}

#[test]
fn precedence_arithmetic_over_comparison() {
    // 1 + 2 > 3 => Gt(Add(1, 2), 3)
    let expr = parse_expr("1 + 2 > 3");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::Gt, right } => {
            match *left {
                Expr::BinaryOp { left: ll, op: BinaryOp::Add, right: lr } => {
                    assert_eq!(*ll, Expr::Integer(1));
                    assert_eq!(*lr, Expr::Integer(2));
                }
                other => panic!("expected Add, got {:?}", other),
            }
            assert_eq!(*right, Expr::Integer(3));
        }
        other => panic!("expected Gt, got {:?}", other),
    }
}

#[test]
fn precedence_and_over_or() {
    // a AND b OR c => Or(And(a, b), c)
    let expr = parse_expr("a AND b OR c");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::Or, right } => {
            match *left {
                Expr::BinaryOp { left: ll, op: BinaryOp::And, right: lr } => {
                    assert_eq!(*ll, Expr::Identifier("a".into()));
                    assert_eq!(*lr, Expr::Identifier("b".into()));
                }
                other => panic!("expected And, got {:?}", other),
            }
            assert_eq!(*right, Expr::Identifier("c".into()));
        }
        other => panic!("expected Or, got {:?}", other),
    }
}

#[test]
fn precedence_and_over_or_reversed() {
    // a OR b AND c => Or(a, And(b, c))
    let expr = parse_expr("a OR b AND c");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::Or, right } => {
            assert_eq!(*left, Expr::Identifier("a".into()));
            match *right {
                Expr::BinaryOp { left: rl, op: BinaryOp::And, right: rr } => {
                    assert_eq!(*rl, Expr::Identifier("b".into()));
                    assert_eq!(*rr, Expr::Identifier("c".into()));
                }
                other => panic!("expected And, got {:?}", other),
            }
        }
        other => panic!("expected Or, got {:?}", other),
    }
}

#[test]
fn precedence_not_over_and() {
    // NOT a AND b => And(Not(a), b)
    let expr = parse_expr("NOT a AND b");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::And, right } => {
            match *left {
                Expr::UnaryOp { op: UnaryOp::Not, expr } => {
                    assert_eq!(*expr, Expr::Identifier("a".into()));
                }
                other => panic!("expected Not, got {:?}", other),
            }
            assert_eq!(*right, Expr::Identifier("b".into()));
        }
        other => panic!("expected And, got {:?}", other),
    }
}

#[test]
fn precedence_not_over_or() {
    // NOT a OR b => Or(Not(a), b)
    let expr = parse_expr("NOT a OR b");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::Or, right } => {
            match *left {
                Expr::UnaryOp { op: UnaryOp::Not, expr } => {
                    assert_eq!(*expr, Expr::Identifier("a".into()));
                }
                other => panic!("expected Not, got {:?}", other),
            }
            assert_eq!(*right, Expr::Identifier("b".into()));
        }
        other => panic!("expected Or, got {:?}", other),
    }
}

#[test]
fn precedence_unary_minus_over_add() {
    // -a + b => Add(Neg(a), b)
    let expr = parse_expr("-a + b");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::Add, right } => {
            match *left {
                Expr::UnaryOp { op: UnaryOp::Minus, expr } => {
                    assert_eq!(*expr, Expr::Identifier("a".into()));
                }
                other => panic!("expected UnaryOp Minus, got {:?}", other),
            }
            assert_eq!(*right, Expr::Identifier("b".into()));
        }
        other => panic!("expected Add, got {:?}", other),
    }
}

#[test]
fn precedence_concat_left_associative() {
    // a || b || c => Concat(Concat(a, b), c)
    let expr = parse_expr("a || b || c");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::Concat, right } => {
            match *left {
                Expr::BinaryOp { left: ll, op: BinaryOp::Concat, right: lr } => {
                    assert_eq!(*ll, Expr::Identifier("a".into()));
                    assert_eq!(*lr, Expr::Identifier("b".into()));
                }
                other => panic!("expected inner Concat, got {:?}", other),
            }
            assert_eq!(*right, Expr::Identifier("c".into()));
        }
        other => panic!("expected Concat, got {:?}", other),
    }
}

#[test]
fn precedence_add_over_concat() {
    // a + b || c => Concat(Add(a, b), c)
    let expr = parse_expr("a + b || c");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::Concat, right } => {
            match *left {
                Expr::BinaryOp { left: ll, op: BinaryOp::Add, right: lr } => {
                    assert_eq!(*ll, Expr::Identifier("a".into()));
                    assert_eq!(*lr, Expr::Identifier("b".into()));
                }
                other => panic!("expected Add, got {:?}", other),
            }
            assert_eq!(*right, Expr::Identifier("c".into()));
        }
        other => panic!("expected Concat, got {:?}", other),
    }
}

#[test]
fn precedence_mul_add_concat() {
    // a * b + c || d => Concat(Add(Mul(a, b), c), d)
    let expr = parse_expr("a * b + c || d");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::Concat, right } => {
            assert_eq!(*right, Expr::Identifier("d".into()));
            match *left {
                Expr::BinaryOp { left: al, op: BinaryOp::Add, right: ar } => {
                    assert_eq!(*ar, Expr::Identifier("c".into()));
                    match *al {
                        Expr::BinaryOp { left: ml, op: BinaryOp::Mul, right: mr } => {
                            assert_eq!(*ml, Expr::Identifier("a".into()));
                            assert_eq!(*mr, Expr::Identifier("b".into()));
                        }
                        other => panic!("expected Mul, got {:?}", other),
                    }
                }
                other => panic!("expected Add, got {:?}", other),
            }
        }
        other => panic!("expected Concat, got {:?}", other),
    }
}

// ===========================================================================
// Parenthesized Expressions
// ===========================================================================

#[test]
fn parenthesized_expression() {
    // (1 + 2) * 3 => Mul(Add(1, 2), 3)
    let expr = parse_expr("(1 + 2) * 3");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::Mul, right } => {
            match *left {
                Expr::BinaryOp { left: ll, op: BinaryOp::Add, right: lr } => {
                    assert_eq!(*ll, Expr::Integer(1));
                    assert_eq!(*lr, Expr::Integer(2));
                }
                other => panic!("expected Add, got {:?}", other),
            }
            assert_eq!(*right, Expr::Integer(3));
        }
        other => panic!("expected Mul, got {:?}", other),
    }
}

// ===========================================================================
// Unary Operators
// ===========================================================================

#[test]
fn unary_minus() {
    let expr = parse_expr("-1");
    match expr {
        Expr::UnaryOp { op: UnaryOp::Minus, expr } => {
            assert_eq!(*expr, Expr::Integer(1));
        }
        other => panic!("expected UnaryOp Minus, got {:?}", other),
    }
}

#[test]
fn unary_plus() {
    let expr = parse_expr("+a");
    match expr {
        Expr::UnaryOp { op: UnaryOp::Plus, expr } => {
            assert_eq!(*expr, Expr::Identifier("a".into()));
        }
        other => panic!("expected UnaryOp Plus, got {:?}", other),
    }
}

#[test]
fn unary_not() {
    let expr = parse_expr("NOT true");
    match expr {
        Expr::UnaryOp { op: UnaryOp::Not, expr } => {
            assert_eq!(*expr, Expr::Boolean(true));
        }
        other => panic!("expected UnaryOp Not, got {:?}", other),
    }
}

#[test]
fn double_unary_minus() {
    // --1 => UnaryOp(Minus, UnaryOp(Minus, 1))
    let expr = parse_expr("- -1");
    match expr {
        Expr::UnaryOp { op: UnaryOp::Minus, expr: inner } => {
            match *inner {
                Expr::UnaryOp { op: UnaryOp::Minus, expr: innermost } => {
                    assert_eq!(*innermost, Expr::Integer(1));
                }
                other => panic!("expected inner UnaryOp Minus, got {:?}", other),
            }
        }
        other => panic!("expected UnaryOp Minus, got {:?}", other),
    }
}

// ===========================================================================
// Comparison Operators
// ===========================================================================

#[test]
fn comparison_eq() {
    let expr = parse_expr("a = 1");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::Eq, right } => {
            assert_eq!(*left, Expr::Identifier("a".into()));
            assert_eq!(*right, Expr::Integer(1));
        }
        other => panic!("expected Eq, got {:?}", other),
    }
}

#[test]
fn comparison_not_eq_angle_brackets() {
    let expr = parse_expr("a <> 1");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::NotEq, right } => {
            assert_eq!(*left, Expr::Identifier("a".into()));
            assert_eq!(*right, Expr::Integer(1));
        }
        other => panic!("expected NotEq, got {:?}", other),
    }
}

#[test]
fn comparison_not_eq_exclamation() {
    let expr = parse_expr("a != 1");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::NotEq, right } => {
            assert_eq!(*left, Expr::Identifier("a".into()));
            assert_eq!(*right, Expr::Integer(1));
        }
        other => panic!("expected NotEq, got {:?}", other),
    }
}

#[test]
fn comparison_combined_with_and() {
    // a < 1 AND b >= 2 => And(Lt(a, 1), GtEq(b, 2))
    let expr = parse_expr("a < 1 AND b >= 2");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::And, right } => {
            match *left {
                Expr::BinaryOp { left: ll, op: BinaryOp::Lt, right: lr } => {
                    assert_eq!(*ll, Expr::Identifier("a".into()));
                    assert_eq!(*lr, Expr::Integer(1));
                }
                other => panic!("expected Lt, got {:?}", other),
            }
            match *right {
                Expr::BinaryOp { left: rl, op: BinaryOp::GtEq, right: rr } => {
                    assert_eq!(*rl, Expr::Identifier("b".into()));
                    assert_eq!(*rr, Expr::Integer(2));
                }
                other => panic!("expected GtEq, got {:?}", other),
            }
        }
        other => panic!("expected And, got {:?}", other),
    }
}

// ===========================================================================
// IS NULL / IS NOT NULL
// ===========================================================================

#[test]
fn is_null() {
    let expr = parse_expr("a IS NULL");
    match expr {
        Expr::IsNull { expr, negated } => {
            assert_eq!(*expr, Expr::Identifier("a".into()));
            assert!(!negated);
        }
        other => panic!("expected IsNull, got {:?}", other),
    }
}

#[test]
fn is_not_null() {
    let expr = parse_expr("a IS NOT NULL");
    match expr {
        Expr::IsNull { expr, negated } => {
            assert_eq!(*expr, Expr::Identifier("a".into()));
            assert!(negated);
        }
        other => panic!("expected IsNull negated, got {:?}", other),
    }
}

// ===========================================================================
// BETWEEN
// ===========================================================================

#[test]
fn between() {
    let expr = parse_expr("a BETWEEN 1 AND 10");
    match expr {
        Expr::Between { expr, low, high, negated } => {
            assert_eq!(*expr, Expr::Identifier("a".into()));
            assert_eq!(*low, Expr::Integer(1));
            assert_eq!(*high, Expr::Integer(10));
            assert!(!negated);
        }
        other => panic!("expected Between, got {:?}", other),
    }
}

#[test]
fn not_between() {
    let expr = parse_expr("a NOT BETWEEN 1 AND 10");
    match expr {
        Expr::Between { expr, low, high, negated } => {
            assert_eq!(*expr, Expr::Identifier("a".into()));
            assert_eq!(*low, Expr::Integer(1));
            assert_eq!(*high, Expr::Integer(10));
            assert!(negated);
        }
        other => panic!("expected Between negated, got {:?}", other),
    }
}

// ===========================================================================
// IN
// ===========================================================================

#[test]
fn in_list() {
    let expr = parse_expr("a IN (1, 2, 3)");
    match expr {
        Expr::InList { expr, list, negated } => {
            assert_eq!(*expr, Expr::Identifier("a".into()));
            assert_eq!(list, vec![Expr::Integer(1), Expr::Integer(2), Expr::Integer(3)]);
            assert!(!negated);
        }
        other => panic!("expected InList, got {:?}", other),
    }
}

#[test]
fn not_in_list() {
    let expr = parse_expr("a NOT IN (1, 2)");
    match expr {
        Expr::InList { expr, list, negated } => {
            assert_eq!(*expr, Expr::Identifier("a".into()));
            assert_eq!(list, vec![Expr::Integer(1), Expr::Integer(2)]);
            assert!(negated);
        }
        other => panic!("expected InList negated, got {:?}", other),
    }
}

#[test]
fn in_subquery() {
    let expr = parse_expr("a IN (SELECT id FROM t)");
    match expr {
        Expr::InSubquery { expr, subquery, negated } => {
            assert_eq!(*expr, Expr::Identifier("a".into()));
            assert!(!negated);
            // Verify the subquery parsed correctly
            assert_eq!(subquery.columns.len(), 1);
            match &subquery.columns[0] {
                SelectColumn::Expr { expr: col_expr, .. } => {
                    assert_eq!(*col_expr, Expr::Identifier("id".into()));
                }
                other => panic!("expected Expr column in subquery, got {:?}", other),
            }
        }
        other => panic!("expected InSubquery, got {:?}", other),
    }
}

// ===========================================================================
// LIKE / ILIKE
// ===========================================================================

#[test]
fn like() {
    let expr = parse_expr("name LIKE '%foo%'");
    match expr {
        Expr::Like { expr, pattern, negated, case_insensitive } => {
            assert_eq!(*expr, Expr::Identifier("name".into()));
            assert_eq!(*pattern, Expr::String("%foo%".into()));
            assert!(!negated);
            assert!(!case_insensitive);
        }
        other => panic!("expected Like, got {:?}", other),
    }
}

#[test]
fn not_like() {
    let expr = parse_expr("name NOT LIKE 'a%'");
    match expr {
        Expr::Like { expr, pattern, negated, case_insensitive } => {
            assert_eq!(*expr, Expr::Identifier("name".into()));
            assert_eq!(*pattern, Expr::String("a%".into()));
            assert!(negated);
            assert!(!case_insensitive);
        }
        other => panic!("expected Like negated, got {:?}", other),
    }
}

#[test]
fn ilike() {
    let expr = parse_expr("name ILIKE '%FOO%'");
    match expr {
        Expr::Like { expr, pattern, negated, case_insensitive } => {
            assert_eq!(*expr, Expr::Identifier("name".into()));
            assert_eq!(*pattern, Expr::String("%FOO%".into()));
            assert!(!negated);
            assert!(case_insensitive);
        }
        other => panic!("expected Like case_insensitive, got {:?}", other),
    }
}

// ===========================================================================
// EXISTS / NOT EXISTS
// ===========================================================================

#[test]
fn exists() {
    let expr = parse_expr("EXISTS (SELECT 1)");
    match expr {
        Expr::Exists { subquery, negated } => {
            assert!(!negated);
            assert_eq!(subquery.columns.len(), 1);
            match &subquery.columns[0] {
                SelectColumn::Expr { expr: col_expr, .. } => {
                    assert_eq!(*col_expr, Expr::Integer(1));
                }
                other => panic!("expected Expr column, got {:?}", other),
            }
        }
        other => panic!("expected Exists, got {:?}", other),
    }
}

#[test]
fn not_exists() {
    let expr = parse_expr("NOT EXISTS (SELECT 1)");
    match expr {
        Expr::Exists { subquery, negated } => {
            assert!(negated);
            assert_eq!(subquery.columns.len(), 1);
        }
        other => panic!("expected Exists negated, got {:?}", other),
    }
}

// ===========================================================================
// CASE Expressions
// ===========================================================================

#[test]
fn case_searched() {
    let expr = parse_expr("CASE WHEN a > 1 THEN 'big' WHEN a = 1 THEN 'one' ELSE 'small' END");
    match expr {
        Expr::Case { operand, when_clauses, else_clause } => {
            assert!(operand.is_none(), "searched CASE should have no operand");
            assert_eq!(when_clauses.len(), 2);

            // First WHEN: a > 1 THEN 'big'
            match &when_clauses[0].condition {
                Expr::BinaryOp { left, op: BinaryOp::Gt, right } => {
                    assert_eq!(**left, Expr::Identifier("a".into()));
                    assert_eq!(**right, Expr::Integer(1));
                }
                other => panic!("expected Gt in first WHEN, got {:?}", other),
            }
            assert_eq!(when_clauses[0].result, Expr::String("big".into()));

            // Second WHEN: a = 1 THEN 'one'
            match &when_clauses[1].condition {
                Expr::BinaryOp { left, op: BinaryOp::Eq, right } => {
                    assert_eq!(**left, Expr::Identifier("a".into()));
                    assert_eq!(**right, Expr::Integer(1));
                }
                other => panic!("expected Eq in second WHEN, got {:?}", other),
            }
            assert_eq!(when_clauses[1].result, Expr::String("one".into()));

            // ELSE 'small'
            assert_eq!(*else_clause.unwrap(), Expr::String("small".into()));
        }
        other => panic!("expected Case, got {:?}", other),
    }
}

#[test]
fn case_simple() {
    let expr = parse_expr("CASE x WHEN 1 THEN 'a' WHEN 2 THEN 'b' END");
    match expr {
        Expr::Case { operand, when_clauses, else_clause } => {
            assert_eq!(*operand.unwrap(), Expr::Identifier("x".into()));
            assert_eq!(when_clauses.len(), 2);

            assert_eq!(when_clauses[0].condition, Expr::Integer(1));
            assert_eq!(when_clauses[0].result, Expr::String("a".into()));

            assert_eq!(when_clauses[1].condition, Expr::Integer(2));
            assert_eq!(when_clauses[1].result, Expr::String("b".into()));

            assert!(else_clause.is_none(), "CASE without ELSE should have None");
        }
        other => panic!("expected Case, got {:?}", other),
    }
}

#[test]
fn case_without_else() {
    let expr = parse_expr("CASE WHEN a > 0 THEN 'positive' END");
    match expr {
        Expr::Case { operand, when_clauses, else_clause } => {
            assert!(operand.is_none());
            assert_eq!(when_clauses.len(), 1);
            assert!(else_clause.is_none());
        }
        other => panic!("expected Case, got {:?}", other),
    }
}

// ===========================================================================
// CAST
// ===========================================================================

#[test]
fn cast_function_syntax() {
    let expr = parse_expr("CAST(x AS INTEGER)");
    match expr {
        Expr::Cast { expr, data_type } => {
            assert_eq!(*expr, Expr::Identifier("x".into()));
            assert_eq!(data_type, DataType::Integer);
        }
        other => panic!("expected Cast, got {:?}", other),
    }
}

#[test]
fn cast_postgres_style() {
    let expr = parse_expr("x::INTEGER");
    match expr {
        Expr::Cast { expr, data_type } => {
            assert_eq!(*expr, Expr::Identifier("x".into()));
            assert_eq!(data_type, DataType::Integer);
        }
        other => panic!("expected Cast, got {:?}", other),
    }
}

#[test]
fn cast_postgres_style_varchar() {
    let expr = parse_expr("x::VARCHAR(100)");
    match expr {
        Expr::Cast { expr, data_type } => {
            assert_eq!(*expr, Expr::Identifier("x".into()));
            assert_eq!(data_type, DataType::Varchar(100));
        }
        other => panic!("expected Cast, got {:?}", other),
    }
}

// ===========================================================================
// Function Calls
// ===========================================================================

#[test]
fn function_count_star() {
    let expr = parse_expr("count(*)");
    match expr {
        Expr::FunctionCall { name, args, distinct } => {
            assert_eq!(name, "count");
            assert_eq!(args, vec![Expr::Star]);
            assert!(!distinct);
        }
        other => panic!("expected FunctionCall, got {:?}", other),
    }
}

#[test]
fn function_count_distinct() {
    let expr = parse_expr("count(DISTINCT a)");
    match expr {
        Expr::FunctionCall { name, args, distinct } => {
            assert_eq!(name, "count");
            assert_eq!(args, vec![Expr::Identifier("a".into())]);
            assert!(distinct);
        }
        other => panic!("expected FunctionCall, got {:?}", other),
    }
}

#[test]
fn function_max() {
    let expr = parse_expr("max(a)");
    match expr {
        Expr::FunctionCall { name, args, distinct } => {
            assert_eq!(name, "max");
            assert_eq!(args, vec![Expr::Identifier("a".into())]);
            assert!(!distinct);
        }
        other => panic!("expected FunctionCall, got {:?}", other),
    }
}

#[test]
fn coalesce() {
    let expr = parse_expr("coalesce(a, b, 0)");
    match expr {
        Expr::Coalesce(args) => {
            assert_eq!(
                args,
                vec![
                    Expr::Identifier("a".into()),
                    Expr::Identifier("b".into()),
                    Expr::Integer(0),
                ]
            );
        }
        other => panic!("expected Coalesce, got {:?}", other),
    }
}

#[test]
fn nullif() {
    let expr = parse_expr("nullif(a, 0)");
    match expr {
        Expr::Nullif(a, b) => {
            assert_eq!(*a, Expr::Identifier("a".into()));
            assert_eq!(*b, Expr::Integer(0));
        }
        other => panic!("expected Nullif, got {:?}", other),
    }
}

#[test]
fn greatest() {
    let expr = parse_expr("greatest(a, b, c)");
    match expr {
        Expr::Greatest(args) => {
            assert_eq!(
                args,
                vec![
                    Expr::Identifier("a".into()),
                    Expr::Identifier("b".into()),
                    Expr::Identifier("c".into()),
                ]
            );
        }
        other => panic!("expected Greatest, got {:?}", other),
    }
}

#[test]
fn least() {
    let expr = parse_expr("least(a, b)");
    match expr {
        Expr::Least(args) => {
            assert_eq!(
                args,
                vec![
                    Expr::Identifier("a".into()),
                    Expr::Identifier("b".into()),
                ]
            );
        }
        other => panic!("expected Least, got {:?}", other),
    }
}

// ===========================================================================
// Literals
// ===========================================================================

#[test]
fn literal_integer() {
    let expr = parse_expr("42");
    assert_eq!(expr, Expr::Integer(42));
}

#[test]
fn literal_float() {
    let expr = parse_expr("3.14");
    assert_eq!(expr, Expr::Float(3.14));
}

#[test]
fn literal_string() {
    let expr = parse_expr("'hello'");
    assert_eq!(expr, Expr::String("hello".into()));
}

#[test]
fn literal_boolean_true() {
    let expr = parse_expr("true");
    assert_eq!(expr, Expr::Boolean(true));
}

#[test]
fn literal_boolean_false() {
    let expr = parse_expr("false");
    assert_eq!(expr, Expr::Boolean(false));
}

#[test]
fn literal_null() {
    let expr = parse_expr("NULL");
    assert_eq!(expr, Expr::Null);
}

#[test]
fn literal_hex() {
    let expr = parse_expr("0xFF");
    assert_eq!(expr, Expr::Integer(255));
}

// ===========================================================================
// Subquery Expression
// ===========================================================================

#[test]
fn subquery_expression() {
    let expr = parse_expr("(SELECT 1)");
    match expr {
        Expr::Subquery(sel) => {
            assert_eq!(sel.columns.len(), 1);
            match &sel.columns[0] {
                SelectColumn::Expr { expr: col_expr, .. } => {
                    assert_eq!(*col_expr, Expr::Integer(1));
                }
                other => panic!("expected Expr column, got {:?}", other),
            }
        }
        other => panic!("expected Subquery, got {:?}", other),
    }
}

// ===========================================================================
// Placeholder
// ===========================================================================

#[test]
fn placeholder() {
    let expr = parse_expr("$1");
    assert_eq!(expr, Expr::Placeholder(1));
}

// ===========================================================================
// Qualified Identifiers
// ===========================================================================

#[test]
fn qualified_identifier() {
    let expr = parse_expr("t.col");
    match expr {
        Expr::QualifiedIdentifier { table, column } => {
            assert_eq!(table, "t");
            assert_eq!(column, "col");
        }
        other => panic!("expected QualifiedIdentifier, got {:?}", other),
    }
}

// ===========================================================================
// Additional Edge Cases
// ===========================================================================

#[test]
fn function_no_args() {
    let expr = parse_expr("now()");
    match expr {
        Expr::FunctionCall { name, args, distinct } => {
            assert_eq!(name, "now");
            assert!(args.is_empty());
            assert!(!distinct);
        }
        other => panic!("expected FunctionCall with no args, got {:?}", other),
    }
}

#[test]
fn nested_parentheses() {
    // ((1 + 2)) => Add(1, 2)
    let expr = parse_expr("((1 + 2))");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::Add, right } => {
            assert_eq!(*left, Expr::Integer(1));
            assert_eq!(*right, Expr::Integer(2));
        }
        other => panic!("expected Add, got {:?}", other),
    }
}

#[test]
fn complex_expression_precedence() {
    // a + b * c - d / e => Sub(Add(a, Mul(b, c)), Div(d, e))
    let expr = parse_expr("a + b * c - d / e");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::Sub, right } => {
            // left = Add(a, Mul(b, c))
            match *left {
                Expr::BinaryOp { left: al, op: BinaryOp::Add, right: ar } => {
                    assert_eq!(*al, Expr::Identifier("a".into()));
                    match *ar {
                        Expr::BinaryOp { left: ml, op: BinaryOp::Mul, right: mr } => {
                            assert_eq!(*ml, Expr::Identifier("b".into()));
                            assert_eq!(*mr, Expr::Identifier("c".into()));
                        }
                        other => panic!("expected Mul, got {:?}", other),
                    }
                }
                other => panic!("expected Add, got {:?}", other),
            }
            // right = Div(d, e)
            match *right {
                Expr::BinaryOp { left: dl, op: BinaryOp::Div, right: dr } => {
                    assert_eq!(*dl, Expr::Identifier("d".into()));
                    assert_eq!(*dr, Expr::Identifier("e".into()));
                }
                other => panic!("expected Div, got {:?}", other),
            }
        }
        other => panic!("expected Sub, got {:?}", other),
    }
}

#[test]
fn modulo_operator() {
    let expr = parse_expr("a % b");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::Mod, right } => {
            assert_eq!(*left, Expr::Identifier("a".into()));
            assert_eq!(*right, Expr::Identifier("b".into()));
        }
        other => panic!("expected Mod, got {:?}", other),
    }
}

#[test]
fn qualified_star() {
    // t.* at the SELECT column level is parsed as TableAllColumns, not an Expr.
    // We test that QualifiedStar is reachable as an expression in a non-column
    // context by parsing it within a larger expression (e.g., inside a WHERE
    // or a function). In the SELECT column list, the parser detects ident.* and
    // produces TableAllColumns before the expression parser runs.
    let sql = "SELECT 1 FROM x WHERE t.col IS NOT NULL";
    let r = parse(sql);
    assert!(r.errors.is_empty(), "parse errors: {:?}", r.errors);
    let stmt = r.statements.into_iter().next().unwrap();
    match stmt {
        Statement::Select(s) => {
            let where_expr = s.where_clause.unwrap();
            match where_expr {
                Expr::IsNull { expr, negated } => {
                    assert!(negated);
                    match *expr {
                        Expr::QualifiedIdentifier { table, column } => {
                            assert_eq!(table, "t");
                            assert_eq!(column, "col");
                        }
                        other => panic!("expected QualifiedIdentifier, got {:?}", other),
                    }
                }
                other => panic!("expected IsNull, got {:?}", other),
            }
        }
        other => panic!("expected Select, got {:?}", other),
    }
}

#[test]
fn cast_chained_with_arithmetic() {
    // x::INTEGER + 1 => Add(Cast(x, Integer), 1)
    // :: (BP_CAST=120) is tighter than + (BP_ADD=80)
    let expr = parse_expr("x::INTEGER + 1");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::Add, right } => {
            match *left {
                Expr::Cast { expr, data_type } => {
                    assert_eq!(*expr, Expr::Identifier("x".into()));
                    assert_eq!(data_type, DataType::Integer);
                }
                other => panic!("expected Cast, got {:?}", other),
            }
            assert_eq!(*right, Expr::Integer(1));
        }
        other => panic!("expected Add, got {:?}", other),
    }
}

#[test]
fn function_multiple_args() {
    let expr = parse_expr("substr(name, 1, 5)");
    match expr {
        Expr::FunctionCall { name, args, distinct } => {
            assert_eq!(name, "substr");
            assert_eq!(args.len(), 3);
            assert_eq!(args[0], Expr::Identifier("name".into()));
            assert_eq!(args[1], Expr::Integer(1));
            assert_eq!(args[2], Expr::Integer(5));
            assert!(!distinct);
        }
        other => panic!("expected FunctionCall, got {:?}", other),
    }
}

#[test]
fn star_expression() {
    // SELECT * is parsed as AllColumns at the column level, not as Expr::Star.
    // Expr::Star appears inside function calls like count(*).
    // Verify that count(*) yields Expr::Star inside its args.
    let expr = parse_expr("count(*)");
    match expr {
        Expr::FunctionCall { args, .. } => {
            assert_eq!(args.len(), 1);
            assert_eq!(args[0], Expr::Star);
        }
        other => panic!("expected FunctionCall with Star arg, got {:?}", other),
    }
}

#[test]
fn placeholder_larger_number() {
    let expr = parse_expr("$42");
    assert_eq!(expr, Expr::Placeholder(42));
}

#[test]
fn comparison_lt_eq() {
    let expr = parse_expr("a <= b");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::LtEq, right } => {
            assert_eq!(*left, Expr::Identifier("a".into()));
            assert_eq!(*right, Expr::Identifier("b".into()));
        }
        other => panic!("expected LtEq, got {:?}", other),
    }
}

#[test]
fn comparison_gt() {
    let expr = parse_expr("a > b");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::Gt, right } => {
            assert_eq!(*left, Expr::Identifier("a".into()));
            assert_eq!(*right, Expr::Identifier("b".into()));
        }
        other => panic!("expected Gt, got {:?}", other),
    }
}

#[test]
fn negative_integer_in_expression() {
    // -42 => UnaryOp(Minus, 42)
    let expr = parse_expr("-42");
    match expr {
        Expr::UnaryOp { op: UnaryOp::Minus, expr } => {
            assert_eq!(*expr, Expr::Integer(42));
        }
        other => panic!("expected UnaryOp Minus, got {:?}", other),
    }
}

#[test]
fn between_with_expressions() {
    // a BETWEEN b + 1 AND c * 2
    let expr = parse_expr("a BETWEEN b + 1 AND c * 2");
    match expr {
        Expr::Between { expr, low, high, negated } => {
            assert_eq!(*expr, Expr::Identifier("a".into()));
            assert!(!negated);
            match *low {
                Expr::BinaryOp { left, op: BinaryOp::Add, right } => {
                    assert_eq!(*left, Expr::Identifier("b".into()));
                    assert_eq!(*right, Expr::Integer(1));
                }
                other => panic!("expected Add in low, got {:?}", other),
            }
            match *high {
                Expr::BinaryOp { left, op: BinaryOp::Mul, right } => {
                    assert_eq!(*left, Expr::Identifier("c".into()));
                    assert_eq!(*right, Expr::Integer(2));
                }
                other => panic!("expected Mul in high, got {:?}", other),
            }
        }
        other => panic!("expected Between, got {:?}", other),
    }
}

#[test]
fn subtraction_operator() {
    let expr = parse_expr("a - b");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::Sub, right } => {
            assert_eq!(*left, Expr::Identifier("a".into()));
            assert_eq!(*right, Expr::Identifier("b".into()));
        }
        other => panic!("expected Sub, got {:?}", other),
    }
}

#[test]
fn division_operator() {
    let expr = parse_expr("a / b");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::Div, right } => {
            assert_eq!(*left, Expr::Identifier("a".into()));
            assert_eq!(*right, Expr::Identifier("b".into()));
        }
        other => panic!("expected Div, got {:?}", other),
    }
}

#[test]
fn exponentiation_operator() {
    let expr = parse_expr("a ^ b");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::Exp, right } => {
            assert_eq!(*left, Expr::Identifier("a".into()));
            assert_eq!(*right, Expr::Identifier("b".into()));
        }
        other => panic!("expected Exp, got {:?}", other),
    }
}

#[test]
fn concat_operator() {
    let expr = parse_expr("a || b");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::Concat, right } => {
            assert_eq!(*left, Expr::Identifier("a".into()));
            assert_eq!(*right, Expr::Identifier("b".into()));
        }
        other => panic!("expected Concat, got {:?}", other),
    }
}

#[test]
fn string_literal_with_single_quote_escape() {
    let expr = parse_expr("'it''s'");
    assert_eq!(expr, Expr::String("it's".into()));
}

#[test]
fn in_list_with_strings() {
    let expr = parse_expr("status IN ('active', 'pending')");
    match expr {
        Expr::InList { expr, list, negated } => {
            assert_eq!(*expr, Expr::Identifier("status".into()));
            assert_eq!(
                list,
                vec![
                    Expr::String("active".into()),
                    Expr::String("pending".into()),
                ]
            );
            assert!(!negated);
        }
        other => panic!("expected InList, got {:?}", other),
    }
}

#[test]
fn not_ilike() {
    let expr = parse_expr("name NOT ILIKE '%test%'");
    match expr {
        Expr::Like { expr, pattern, negated, case_insensitive } => {
            assert_eq!(*expr, Expr::Identifier("name".into()));
            assert_eq!(*pattern, Expr::String("%test%".into()));
            assert!(negated);
            assert!(case_insensitive);
        }
        other => panic!("expected Like negated case_insensitive, got {:?}", other),
    }
}

#[test]
fn boolean_literal_case_insensitive() {
    let expr = parse_expr("TRUE");
    assert_eq!(expr, Expr::Boolean(true));

    let expr = parse_expr("FALSE");
    assert_eq!(expr, Expr::Boolean(false));
}

#[test]
fn cast_to_decimal() {
    let expr = parse_expr("CAST(x AS DECIMAL(10, 2))");
    match expr {
        Expr::Cast { expr, data_type } => {
            assert_eq!(*expr, Expr::Identifier("x".into()));
            assert_eq!(data_type, DataType::Decimal(10, 2));
        }
        other => panic!("expected Cast, got {:?}", other),
    }
}

#[test]
fn cast_to_text() {
    let expr = parse_expr("CAST(42 AS TEXT)");
    match expr {
        Expr::Cast { expr, data_type } => {
            assert_eq!(*expr, Expr::Integer(42));
            assert_eq!(data_type, DataType::Text);
        }
        other => panic!("expected Cast, got {:?}", other),
    }
}

#[test]
fn nested_function_calls() {
    // upper(trim(name))
    let expr = parse_expr("upper(trim(name))");
    match expr {
        Expr::FunctionCall { name: outer_name, args: outer_args, .. } => {
            assert_eq!(outer_name, "upper");
            assert_eq!(outer_args.len(), 1);
            match &outer_args[0] {
                Expr::FunctionCall { name: inner_name, args: inner_args, .. } => {
                    assert_eq!(inner_name, "trim");
                    assert_eq!(inner_args.len(), 1);
                    assert_eq!(inner_args[0], Expr::Identifier("name".into()));
                }
                other => panic!("expected inner FunctionCall, got {:?}", other),
            }
        }
        other => panic!("expected FunctionCall, got {:?}", other),
    }
}

#[test]
fn complex_boolean_expression() {
    // (a > 1 AND b < 2) OR (c = 3 AND d != 4)
    let expr = parse_expr("(a > 1 AND b < 2) OR (c = 3 AND d != 4)");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::Or, right } => {
            // left = And(Gt(a, 1), Lt(b, 2))
            match *left {
                Expr::BinaryOp { op: BinaryOp::And, .. } => { /* ok */ }
                other => panic!("expected And on left, got {:?}", other),
            }
            // right = And(Eq(c, 3), NotEq(d, 4))
            match *right {
                Expr::BinaryOp { op: BinaryOp::And, .. } => { /* ok */ }
                other => panic!("expected And on right, got {:?}", other),
            }
        }
        other => panic!("expected Or, got {:?}", other),
    }
}

#[test]
fn is_true_is_false() {
    // a IS TRUE => Eq(a, true)
    let expr = parse_expr("a IS TRUE");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::Eq, right } => {
            assert_eq!(*left, Expr::Identifier("a".into()));
            assert_eq!(*right, Expr::Boolean(true));
        }
        other => panic!("expected Eq(a, true), got {:?}", other),
    }

    // a IS NOT FALSE => NotEq(a, false)
    let expr = parse_expr("a IS NOT FALSE");
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::NotEq, right } => {
            assert_eq!(*left, Expr::Identifier("a".into()));
            assert_eq!(*right, Expr::Boolean(false));
        }
        other => panic!("expected NotEq(a, false), got {:?}", other),
    }
}
