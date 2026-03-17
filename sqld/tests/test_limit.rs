use sqld::executor::executor::Executor;
use sqld::executor::limit::LimitExecutor;
use sqld::executor::values::ValuesExecutor;
use sqld::sql::ast::Expr;
use sqld::types::{Column, DataType, Datum, Schema};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn datum_to_expr(d: Datum) -> Expr {
    match d {
        Datum::Integer(v) => Expr::Integer(v as i64),
        Datum::BigInt(v) => Expr::Integer(v),
        Datum::Float(v) => Expr::Float(v),
        Datum::Text(s) | Datum::Varchar(s) => Expr::String(s),
        Datum::Boolean(b) => Expr::Boolean(b),
        Datum::Null => Expr::Null,
        _ => Expr::Null,
    }
}

fn make_source(rows: Vec<Vec<Datum>>, schema: Schema) -> Box<dyn Executor> {
    let expr_rows: Vec<Vec<Expr>> = rows
        .into_iter()
        .map(|row| row.into_iter().map(datum_to_expr).collect())
        .collect();
    Box::new(ValuesExecutor::new(expr_rows, schema))
}

fn collect(exec: &mut dyn Executor) -> Vec<Vec<Datum>> {
    exec.init().unwrap();
    let mut out = Vec::new();
    while let Some(t) = exec.next().unwrap() {
        out.push(t.values().to_vec());
    }
    exec.close().unwrap();
    out
}

fn int_schema() -> Schema {
    Schema::new(vec![Column::new("x", DataType::Integer, false)])
}

/// Build 5 rows: [1], [2], [3], [4], [5]
fn five_rows() -> Vec<Vec<Datum>> {
    (1..=5).map(|i| vec![Datum::Integer(i)]).collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn limit_only() {
    // LIMIT 2 from 5 rows -> first 2
    let source = make_source(five_rows(), int_schema());
    let mut exec = LimitExecutor::new(source, Some(2), 0);
    let result = collect(&mut exec);

    assert_eq!(result.len(), 2);
    assert_eq!(result[0][0], Datum::Integer(1));
    assert_eq!(result[1][0], Datum::Integer(2));
}

#[test]
fn offset_only() {
    // OFFSET 2 from 5 rows -> rows 3, 4, 5
    let source = make_source(five_rows(), int_schema());
    let mut exec = LimitExecutor::new(source, None, 2);
    let result = collect(&mut exec);

    assert_eq!(result.len(), 3);
    assert_eq!(result[0][0], Datum::Integer(3));
    assert_eq!(result[1][0], Datum::Integer(4));
    assert_eq!(result[2][0], Datum::Integer(5));
}

#[test]
fn limit_and_offset_combined() {
    // LIMIT 2 OFFSET 1 from 5 rows -> rows 2, 3
    let source = make_source(five_rows(), int_schema());
    let mut exec = LimitExecutor::new(source, Some(2), 1);
    let result = collect(&mut exec);

    assert_eq!(result.len(), 2);
    assert_eq!(result[0][0], Datum::Integer(2));
    assert_eq!(result[1][0], Datum::Integer(3));
}

#[test]
fn limit_larger_than_input() {
    // LIMIT 100 from 5 rows -> all 5
    let source = make_source(five_rows(), int_schema());
    let mut exec = LimitExecutor::new(source, Some(100), 0);
    let result = collect(&mut exec);

    assert_eq!(result.len(), 5);
    for (i, row) in result.iter().enumerate() {
        assert_eq!(row[0], Datum::Integer((i + 1) as i32));
    }
}

#[test]
fn limit_zero() {
    // LIMIT 0 -> no rows
    let source = make_source(five_rows(), int_schema());
    let mut exec = LimitExecutor::new(source, Some(0), 0);
    let result = collect(&mut exec);

    assert!(result.is_empty());
}

#[test]
fn offset_larger_than_input() {
    // OFFSET 10 from 5 rows -> nothing
    let source = make_source(five_rows(), int_schema());
    let mut exec = LimitExecutor::new(source, None, 10);
    let result = collect(&mut exec);

    assert!(result.is_empty());
}

#[test]
fn limit_on_empty_input() {
    // LIMIT 5 from 0 rows -> nothing
    let source = make_source(vec![], int_schema());
    let mut exec = LimitExecutor::new(source, Some(5), 0);
    let result = collect(&mut exec);

    assert!(result.is_empty());
}

#[test]
fn offset_and_limit_exhaust_remaining() {
    // OFFSET 3 LIMIT 10 from 5 rows -> rows 4, 5 (only 2 remain)
    let source = make_source(five_rows(), int_schema());
    let mut exec = LimitExecutor::new(source, Some(10), 3);
    let result = collect(&mut exec);

    assert_eq!(result.len(), 2);
    assert_eq!(result[0][0], Datum::Integer(4));
    assert_eq!(result[1][0], Datum::Integer(5));
}
