use sqld::executor::executor::Executor;
use sqld::executor::nested_loop_join::NestedLoopJoinExecutor;
use sqld::executor::values::ValuesExecutor;
use sqld::sql::ast::{BinaryOp, Expr, JoinType};
use sqld::types::{Column, DataType, Datum, Schema, Tuple};

// ---------------------------------------------------------------------------
// Helper: build a ValuesExecutor from Datum rows
// ---------------------------------------------------------------------------

fn make_source(rows: Vec<Vec<Datum>>, schema: Schema) -> Box<dyn Executor> {
    let expr_rows: Vec<Vec<Expr>> = rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|d| match d {
                    Datum::Integer(v) => Expr::Integer(v as i64),
                    Datum::BigInt(v) => Expr::Integer(v),
                    Datum::Float(v) => Expr::Float(v),
                    Datum::Text(s) | Datum::Varchar(s) => Expr::String(s),
                    Datum::Boolean(b) => Expr::Boolean(b),
                    Datum::Null => Expr::Null,
                    _ => Expr::Null,
                })
                .collect()
        })
        .collect();
    Box::new(ValuesExecutor::new(expr_rows, schema))
}

fn collect_all(exec: &mut Box<dyn Executor>) -> Vec<Tuple> {
    let mut rows = Vec::new();
    while let Some(tuple) = exec.next().unwrap() {
        rows.push(tuple);
    }
    rows
}

// ---------------------------------------------------------------------------
// Schemas
// ---------------------------------------------------------------------------

fn left_schema() -> Schema {
    Schema::new(vec![
        Column::new("id", DataType::Integer, false),
        Column::new("name", DataType::Text, false),
    ])
}

fn right_schema() -> Schema {
    Schema::new(vec![
        Column::new("id", DataType::Integer, false),
        Column::new("dept", DataType::Text, false),
    ])
}

fn left_data() -> Vec<Vec<Datum>> {
    vec![
        vec![Datum::Integer(1), Datum::Text("Alice".into())],
        vec![Datum::Integer(2), Datum::Text("Bob".into())],
        vec![Datum::Integer(3), Datum::Text("Charlie".into())],
    ]
}

fn right_data() -> Vec<Vec<Datum>> {
    vec![
        vec![Datum::Integer(2), Datum::Text("Engineering".into())],
        vec![Datum::Integer(3), Datum::Text("Sales".into())],
        vec![Datum::Integer(4), Datum::Text("Marketing".into())],
    ]
}

/// Equi-join condition: id = _right_id (using the merged schema column names)
fn equi_condition() -> Expr {
    Expr::BinaryOp {
        left: Box::new(Expr::Identifier("id".into())),
        op: BinaryOp::Eq,
        right: Box::new(Expr::Identifier("_right_id".into())),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn nested_loop_inner_equi_join() {
    let ls = left_schema();
    let rs = right_schema();
    let merged = ls.merge(&rs);

    let left = make_source(left_data(), ls.clone());
    let right = make_source(right_data(), rs.clone());

    let mut exec: Box<dyn Executor> = Box::new(NestedLoopJoinExecutor::new(
        left,
        right,
        JoinType::Inner,
        Some(equi_condition()),
        merged,
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // IDs 2 and 3 match
    assert_eq!(rows.len(), 2);

    let mut ids: Vec<i32> = rows
        .iter()
        .map(|t| match t.get(0).unwrap() {
            Datum::Integer(v) => *v,
            _ => panic!("expected integer"),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![2, 3]);

    // All rows should have 4 columns (merged schema: id, name, _right_id, dept)
    for row in &rows {
        assert_eq!(row.column_count(), 4);
        // Left id should equal right id for matched rows
        assert_eq!(row.get(0).unwrap(), row.get(2).unwrap());
    }
}

#[test]
fn nested_loop_cross_join() {
    let ls = Schema::new(vec![Column::new("a", DataType::Integer, false)]);
    let rs = Schema::new(vec![Column::new("b", DataType::Integer, false)]);
    let merged = ls.merge(&rs);

    let left = make_source(
        vec![vec![Datum::Integer(1)], vec![Datum::Integer(2)]],
        ls.clone(),
    );
    let right = make_source(
        vec![
            vec![Datum::Integer(10)],
            vec![Datum::Integer(20)],
            vec![Datum::Integer(30)],
        ],
        rs.clone(),
    );

    // CROSS JOIN: no condition
    let mut exec: Box<dyn Executor> = Box::new(NestedLoopJoinExecutor::new(
        left,
        right,
        JoinType::Cross,
        None,
        merged,
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // Cartesian product: 2 x 3 = 6 rows
    assert_eq!(rows.len(), 6);

    let mut combos: Vec<(i32, i32)> = rows
        .iter()
        .map(|t| {
            let a = match t.get(0).unwrap() {
                Datum::Integer(v) => *v,
                _ => panic!("expected integer"),
            };
            let b = match t.get(1).unwrap() {
                Datum::Integer(v) => *v,
                _ => panic!("expected integer"),
            };
            (a, b)
        })
        .collect();
    combos.sort();
    assert_eq!(
        combos,
        vec![(1, 10), (1, 20), (1, 30), (2, 10), (2, 20), (2, 30)]
    );
}

#[test]
fn nested_loop_left_join_with_no_matches() {
    let ls = left_schema();
    let rs = right_schema();
    let merged = ls.merge(&rs);

    // Left has ids 10, 20 -- no overlap with right ids 2, 3, 4
    let left = make_source(
        vec![
            vec![Datum::Integer(10), Datum::Text("X".into())],
            vec![Datum::Integer(20), Datum::Text("Y".into())],
        ],
        ls.clone(),
    );
    let right = make_source(right_data(), rs.clone());

    let mut exec: Box<dyn Executor> = Box::new(NestedLoopJoinExecutor::new(
        left,
        right,
        JoinType::Left,
        Some(equi_condition()),
        merged,
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // Both left rows are unmatched, so 2 rows with NULL-padded right side
    assert_eq!(rows.len(), 2);

    for row in &rows {
        assert_eq!(row.column_count(), 4);
        assert!(row.get(2).unwrap().is_null());
        assert!(row.get(3).unwrap().is_null());
    }

    // Verify the left ids are preserved
    let mut ids: Vec<i32> = rows
        .iter()
        .map(|t| match t.get(0).unwrap() {
            Datum::Integer(v) => *v,
            _ => panic!("expected integer"),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![10, 20]);
}

#[test]
fn nested_loop_left_join_mixed() {
    let ls = left_schema();
    let rs = right_schema();
    let merged = ls.merge(&rs);

    let left = make_source(left_data(), ls.clone());
    let right = make_source(right_data(), rs.clone());

    let mut exec: Box<dyn Executor> = Box::new(NestedLoopJoinExecutor::new(
        left,
        right,
        JoinType::Left,
        Some(equi_condition()),
        merged,
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // 2 matched (id=2, id=3) + 1 unmatched (id=1) = 3
    assert_eq!(rows.len(), 3);

    // The unmatched row should have id=1 with NULL right side
    let unmatched: Vec<&Tuple> = rows
        .iter()
        .filter(|t| *t.get(0).unwrap() == Datum::Integer(1))
        .collect();
    assert_eq!(unmatched.len(), 1);
    assert!(unmatched[0].get(2).unwrap().is_null());
    assert!(unmatched[0].get(3).unwrap().is_null());
}

#[test]
fn nested_loop_right_join() {
    let ls = left_schema();
    let rs = right_schema();
    let merged = ls.merge(&rs);

    let left = make_source(left_data(), ls.clone());
    let right = make_source(right_data(), rs.clone());

    let mut exec: Box<dyn Executor> = Box::new(NestedLoopJoinExecutor::new(
        left,
        right,
        JoinType::Right,
        Some(equi_condition()),
        merged,
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // 2 matched (id=2, id=3) + 1 unmatched right (id=4) = 3
    assert_eq!(rows.len(), 3);

    // The unmatched right row has _right_id=4 with NULL left side
    let unmatched: Vec<&Tuple> = rows
        .iter()
        .filter(|t| *t.get(2).unwrap() == Datum::Integer(4))
        .collect();
    assert_eq!(unmatched.len(), 1);
    assert!(unmatched[0].get(0).unwrap().is_null());
    assert!(unmatched[0].get(1).unwrap().is_null());
}

#[test]
fn nested_loop_full_join() {
    let ls = left_schema();
    let rs = right_schema();
    let merged = ls.merge(&rs);

    let left = make_source(left_data(), ls.clone());
    let right = make_source(right_data(), rs.clone());

    let mut exec: Box<dyn Executor> = Box::new(NestedLoopJoinExecutor::new(
        left,
        right,
        JoinType::Full,
        Some(equi_condition()),
        merged,
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // 2 matched + 1 unmatched left (id=1) + 1 unmatched right (id=4) = 4
    assert_eq!(rows.len(), 4);

    // Left-only: id=1 with NULL right
    let left_only: Vec<&Tuple> = rows
        .iter()
        .filter(|t| *t.get(0).unwrap() == Datum::Integer(1))
        .collect();
    assert_eq!(left_only.len(), 1);
    assert!(left_only[0].get(2).unwrap().is_null());

    // Right-only: _right_id=4 with NULL left
    let right_only: Vec<&Tuple> = rows
        .iter()
        .filter(|t| *t.get(2).unwrap() == Datum::Integer(4))
        .collect();
    assert_eq!(right_only.len(), 1);
    assert!(right_only[0].get(0).unwrap().is_null());
}

#[test]
fn nested_loop_theta_join() {
    let ls = Schema::new(vec![Column::new("a", DataType::Integer, false)]);
    let rs = Schema::new(vec![Column::new("b", DataType::Integer, false)]);
    let merged = ls.merge(&rs);

    let left = make_source(
        vec![
            vec![Datum::Integer(1)],
            vec![Datum::Integer(2)],
            vec![Datum::Integer(3)],
        ],
        ls.clone(),
    );
    let right = make_source(
        vec![
            vec![Datum::Integer(2)],
            vec![Datum::Integer(3)],
            vec![Datum::Integer(4)],
        ],
        rs.clone(),
    );

    // Theta condition: a < b
    let cond = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("a".into())),
        op: BinaryOp::Lt,
        right: Box::new(Expr::Identifier("b".into())),
    };

    let mut exec: Box<dyn Executor> = Box::new(NestedLoopJoinExecutor::new(
        left,
        right,
        JoinType::Inner,
        Some(cond),
        merged,
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // Pairs where a < b: (1,2), (1,3), (1,4), (2,3), (2,4), (3,4) = 6
    assert_eq!(rows.len(), 6);
}

#[test]
fn nested_loop_empty_left() {
    let ls = left_schema();
    let rs = right_schema();
    let merged = ls.merge(&rs);

    let left = make_source(vec![], ls.clone());
    let right = make_source(right_data(), rs.clone());

    let mut exec: Box<dyn Executor> = Box::new(NestedLoopJoinExecutor::new(
        left,
        right,
        JoinType::Inner,
        Some(equi_condition()),
        merged,
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    assert_eq!(rows.len(), 0);
}

#[test]
fn nested_loop_empty_right() {
    let ls = left_schema();
    let rs = right_schema();
    let merged = ls.merge(&rs);

    let left = make_source(left_data(), ls.clone());
    let right = make_source(vec![], rs.clone());

    let mut exec: Box<dyn Executor> = Box::new(NestedLoopJoinExecutor::new(
        left,
        right,
        JoinType::Inner,
        Some(equi_condition()),
        merged,
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    assert_eq!(rows.len(), 0);
}

#[test]
fn nested_loop_left_semi_join() {
    let ls = left_schema();
    let rs = right_schema();
    let merged = ls.merge(&rs);

    let left = make_source(left_data(), ls.clone());
    let right = make_source(right_data(), rs.clone());

    // SEMI join: condition needs merged schema for column resolution,
    // but output tuples contain only left-side columns.
    let mut exec: Box<dyn Executor> = Box::new(NestedLoopJoinExecutor::new(
        left,
        right,
        JoinType::LeftSemi,
        Some(equi_condition()),
        merged,
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // Left rows with id=2 and id=3 have matches on the right side
    assert_eq!(rows.len(), 2);

    // Only left-side columns should be emitted (id, name)
    for row in &rows {
        assert_eq!(row.column_count(), 2);
    }

    let mut ids: Vec<i32> = rows
        .iter()
        .map(|t| match t.get(0).unwrap() {
            Datum::Integer(v) => *v,
            _ => panic!("expected integer"),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![2, 3]);

    // Verify names are preserved
    let mut names: Vec<String> = rows
        .iter()
        .map(|t| match t.get(1).unwrap() {
            Datum::Text(s) => s.clone(),
            _ => panic!("expected text"),
        })
        .collect();
    names.sort();
    assert_eq!(names, vec!["Bob", "Charlie"]);
}

#[test]
fn nested_loop_left_semi_join_no_matches() {
    let ls = left_schema();
    let rs = right_schema();
    let merged = ls.merge(&rs);

    // Left has ids 10, 20 -- no overlap with right ids 2, 3, 4
    let left = make_source(
        vec![
            vec![Datum::Integer(10), Datum::Text("X".into())],
            vec![Datum::Integer(20), Datum::Text("Y".into())],
        ],
        ls.clone(),
    );
    let right = make_source(right_data(), rs.clone());

    let mut exec: Box<dyn Executor> = Box::new(NestedLoopJoinExecutor::new(
        left,
        right,
        JoinType::LeftSemi,
        Some(equi_condition()),
        merged,
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // No left rows match any right row, so result is empty
    assert_eq!(rows.len(), 0);
}

#[test]
fn nested_loop_left_semi_join_all_match() {
    let ls = left_schema();
    let rs = right_schema();
    let merged = ls.merge(&rs);

    // Left has ids 2, 3 which all exist on the right
    let left = make_source(
        vec![
            vec![Datum::Integer(2), Datum::Text("Bob".into())],
            vec![Datum::Integer(3), Datum::Text("Charlie".into())],
        ],
        ls.clone(),
    );
    let right = make_source(right_data(), rs.clone());

    let mut exec: Box<dyn Executor> = Box::new(NestedLoopJoinExecutor::new(
        left,
        right,
        JoinType::LeftSemi,
        Some(equi_condition()),
        merged,
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // All left rows match, so both are emitted
    assert_eq!(rows.len(), 2);

    for row in &rows {
        assert_eq!(row.column_count(), 2);
    }
}

#[test]
fn nested_loop_left_anti_join() {
    let ls = left_schema();
    let rs = right_schema();
    let merged = ls.merge(&rs);

    let left = make_source(left_data(), ls.clone());
    let right = make_source(right_data(), rs.clone());

    // ANTI join: condition needs merged schema for column resolution,
    // but output tuples contain only left-side columns (non-matching rows).
    let mut exec: Box<dyn Executor> = Box::new(NestedLoopJoinExecutor::new(
        left,
        right,
        JoinType::LeftAnti,
        Some(equi_condition()),
        merged,
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // Only left row with id=1 has NO match on the right side
    assert_eq!(rows.len(), 1);

    // Only left-side columns should be emitted (id, name)
    assert_eq!(rows[0].column_count(), 2);

    match rows[0].get(0).unwrap() {
        Datum::Integer(v) => assert_eq!(*v, 1),
        _ => panic!("expected integer"),
    }

    match rows[0].get(1).unwrap() {
        Datum::Text(s) => assert_eq!(s, "Alice"),
        _ => panic!("expected text"),
    }
}

#[test]
fn nested_loop_left_anti_join_no_matches() {
    let ls = left_schema();
    let rs = right_schema();
    let merged = ls.merge(&rs);

    // Left has ids 10, 20 -- no overlap with right ids 2, 3, 4
    let left = make_source(
        vec![
            vec![Datum::Integer(10), Datum::Text("X".into())],
            vec![Datum::Integer(20), Datum::Text("Y".into())],
        ],
        ls.clone(),
    );
    let right = make_source(right_data(), rs.clone());

    let mut exec: Box<dyn Executor> = Box::new(NestedLoopJoinExecutor::new(
        left,
        right,
        JoinType::LeftAnti,
        Some(equi_condition()),
        merged,
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // No left rows match, so ALL left rows are emitted
    assert_eq!(rows.len(), 2);

    for row in &rows {
        assert_eq!(row.column_count(), 2);
    }

    let mut ids: Vec<i32> = rows
        .iter()
        .map(|t| match t.get(0).unwrap() {
            Datum::Integer(v) => *v,
            _ => panic!("expected integer"),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![10, 20]);
}

#[test]
fn nested_loop_left_anti_join_all_match() {
    let ls = left_schema();
    let rs = right_schema();
    let merged = ls.merge(&rs);

    // Left has ids 2, 3 which all exist on the right
    let left = make_source(
        vec![
            vec![Datum::Integer(2), Datum::Text("Bob".into())],
            vec![Datum::Integer(3), Datum::Text("Charlie".into())],
        ],
        ls.clone(),
    );
    let right = make_source(right_data(), rs.clone());

    let mut exec: Box<dyn Executor> = Box::new(NestedLoopJoinExecutor::new(
        left,
        right,
        JoinType::LeftAnti,
        Some(equi_condition()),
        merged,
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // All left rows match, so none are emitted for ANTI join
    assert_eq!(rows.len(), 0);
}
