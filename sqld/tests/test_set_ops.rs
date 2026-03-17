use sqld::executor::executor::Executor;
use sqld::executor::set_ops::{ExceptExecutor, IntersectExecutor, UnionExecutor};
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

fn row(v: i32) -> Vec<Datum> {
    vec![Datum::Integer(v)]
}

// ---------------------------------------------------------------------------
// UNION tests
// ---------------------------------------------------------------------------

#[test]
fn union_all() {
    // Left: {1, 2, 3}  Right: {2, 3, 4}
    // UNION ALL -> {1, 2, 3, 2, 3, 4}
    let left = make_source(vec![row(1), row(2), row(3)], int_schema());
    let right = make_source(vec![row(2), row(3), row(4)], int_schema());

    let mut exec = UnionExecutor::new(left, right, true);
    let result = collect(&mut exec);

    assert_eq!(result.len(), 6);
    let vals: Vec<i32> = result.iter().map(|r| match r[0] { Datum::Integer(v) => v, _ => panic!() }).collect();
    assert_eq!(vals, vec![1, 2, 3, 2, 3, 4]);
}

#[test]
fn union_distinct() {
    // Left: {1, 2, 3}  Right: {2, 3, 4}
    // UNION DISTINCT -> {1, 2, 3, 4}
    let left = make_source(vec![row(1), row(2), row(3)], int_schema());
    let right = make_source(vec![row(2), row(3), row(4)], int_schema());

    let mut exec = UnionExecutor::new(left, right, false);
    let result = collect(&mut exec);

    assert_eq!(result.len(), 4);
    let mut vals: Vec<i32> = result.iter().map(|r| match r[0] { Datum::Integer(v) => v, _ => panic!() }).collect();
    vals.sort();
    assert_eq!(vals, vec![1, 2, 3, 4]);
}

// ---------------------------------------------------------------------------
// INTERSECT tests
// ---------------------------------------------------------------------------

#[test]
fn intersect_all() {
    // Left: {1, 2, 2, 3}  Right: {2, 2, 3, 3}
    // INTERSECT ALL -> {2, 2, 3} (min of counts for each value)
    let left = make_source(vec![row(1), row(2), row(2), row(3)], int_schema());
    let right = make_source(vec![row(2), row(2), row(3), row(3)], int_schema());

    let mut exec = IntersectExecutor::new(left, right, true);
    let result = collect(&mut exec);

    assert_eq!(result.len(), 3);
    let mut vals: Vec<i32> = result.iter().map(|r| match r[0] { Datum::Integer(v) => v, _ => panic!() }).collect();
    vals.sort();
    assert_eq!(vals, vec![2, 2, 3]);
}

#[test]
fn intersect_distinct() {
    // Left: {1, 2, 2, 3}  Right: {2, 3, 3, 4}
    // INTERSECT DISTINCT -> {2, 3}
    let left = make_source(vec![row(1), row(2), row(2), row(3)], int_schema());
    let right = make_source(vec![row(2), row(3), row(3), row(4)], int_schema());

    let mut exec = IntersectExecutor::new(left, right, false);
    let result = collect(&mut exec);

    assert_eq!(result.len(), 2);
    let mut vals: Vec<i32> = result.iter().map(|r| match r[0] { Datum::Integer(v) => v, _ => panic!() }).collect();
    vals.sort();
    assert_eq!(vals, vec![2, 3]);
}

// ---------------------------------------------------------------------------
// EXCEPT tests
// ---------------------------------------------------------------------------

#[test]
fn except_all() {
    // Left: {1, 2, 2, 3, 3}  Right: {2, 3}
    // EXCEPT ALL -> {1, 2, 3}  (subtract one copy of 2 and one copy of 3)
    let left = make_source(vec![row(1), row(2), row(2), row(3), row(3)], int_schema());
    let right = make_source(vec![row(2), row(3)], int_schema());

    let mut exec = ExceptExecutor::new(left, right, true);
    let result = collect(&mut exec);

    assert_eq!(result.len(), 3);
    let mut vals: Vec<i32> = result.iter().map(|r| match r[0] { Datum::Integer(v) => v, _ => panic!() }).collect();
    vals.sort();
    assert_eq!(vals, vec![1, 2, 3]);
}

#[test]
fn except_distinct() {
    // Left: {1, 2, 2, 3}  Right: {2, 4}
    // EXCEPT DISTINCT -> {1, 3} (remove value 2 entirely, deduplicate)
    let left = make_source(vec![row(1), row(2), row(2), row(3)], int_schema());
    let right = make_source(vec![row(2), row(4)], int_schema());

    let mut exec = ExceptExecutor::new(left, right, false);
    let result = collect(&mut exec);

    assert_eq!(result.len(), 2);
    let mut vals: Vec<i32> = result.iter().map(|r| match r[0] { Datum::Integer(v) => v, _ => panic!() }).collect();
    vals.sort();
    assert_eq!(vals, vec![1, 3]);
}

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

#[test]
fn union_all_with_empty_right() {
    let left = make_source(vec![row(1), row(2)], int_schema());
    let right = make_source(vec![], int_schema());

    let mut exec = UnionExecutor::new(left, right, true);
    let result = collect(&mut exec);

    assert_eq!(result.len(), 2);
}

#[test]
fn intersect_disjoint_sets() {
    // No common elements -> empty result
    let left = make_source(vec![row(1), row(2)], int_schema());
    let right = make_source(vec![row(3), row(4)], int_schema());

    let mut exec = IntersectExecutor::new(left, right, false);
    let result = collect(&mut exec);

    assert!(result.is_empty());
}
