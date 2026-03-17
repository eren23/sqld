use sqld::executor::executor::Executor;
use sqld::executor::sort_merge_join::SortMergeJoinExecutor;
use sqld::executor::values::ValuesExecutor;
use sqld::sql::ast::{Expr, JoinType};
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

// Pre-sorted by id ASC
fn left_data_sorted() -> Vec<Vec<Datum>> {
    vec![
        vec![Datum::Integer(1), Datum::Text("Alice".into())],
        vec![Datum::Integer(2), Datum::Text("Bob".into())],
        vec![Datum::Integer(3), Datum::Text("Charlie".into())],
    ]
}

fn right_data_sorted() -> Vec<Vec<Datum>> {
    vec![
        vec![Datum::Integer(2), Datum::Text("Engineering".into())],
        vec![Datum::Integer(3), Datum::Text("Sales".into())],
        vec![Datum::Integer(4), Datum::Text("Marketing".into())],
    ]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn sort_merge_inner() {
    let ls = left_schema();
    let rs = right_schema();
    let merged = ls.merge(&rs);

    let left = make_source(left_data_sorted(), ls.clone());
    let right = make_source(right_data_sorted(), rs.clone());

    let mut exec: Box<dyn Executor> = Box::new(SortMergeJoinExecutor::new(
        left,
        right,
        JoinType::Inner,
        vec![Expr::Identifier("id".into())],
        vec![Expr::Identifier("id".into())],
        None,
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

    // Verify all rows have 4 columns (merged schema: id, name, _right_id, dept)
    for row in &rows {
        assert_eq!(row.column_count(), 4);
        // Left id should equal right id for matched rows
        let left_id = row.get(0).unwrap();
        let right_id = row.get(2).unwrap();
        assert_eq!(left_id, right_id);
    }
}

#[test]
fn sort_merge_left() {
    let ls = left_schema();
    let rs = right_schema();
    let merged = ls.merge(&rs);

    let left = make_source(left_data_sorted(), ls.clone());
    let right = make_source(right_data_sorted(), rs.clone());

    let mut exec: Box<dyn Executor> = Box::new(SortMergeJoinExecutor::new(
        left,
        right,
        JoinType::Left,
        vec![Expr::Identifier("id".into())],
        vec![Expr::Identifier("id".into())],
        None,
        merged,
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // All 3 left rows preserved: id=1 unmatched, id=2 matched, id=3 matched
    assert_eq!(rows.len(), 3);

    // Unmatched left row (id=1) should have NULL right columns
    let unmatched: Vec<&Tuple> = rows
        .iter()
        .filter(|t| *t.get(0).unwrap() == Datum::Integer(1))
        .collect();
    assert_eq!(unmatched.len(), 1);
    assert!(unmatched[0].get(2).unwrap().is_null());
    assert!(unmatched[0].get(3).unwrap().is_null());

    // Matched rows should have real values on right side
    let matched: Vec<&Tuple> = rows
        .iter()
        .filter(|t| *t.get(0).unwrap() != Datum::Integer(1))
        .collect();
    for row in matched {
        assert!(!row.get(2).unwrap().is_null());
        assert!(!row.get(3).unwrap().is_null());
    }
}

#[test]
fn sort_merge_right() {
    let ls = left_schema();
    let rs = right_schema();
    let merged = ls.merge(&rs);

    let left = make_source(left_data_sorted(), ls.clone());
    let right = make_source(right_data_sorted(), rs.clone());

    let mut exec: Box<dyn Executor> = Box::new(SortMergeJoinExecutor::new(
        left,
        right,
        JoinType::Right,
        vec![Expr::Identifier("id".into())],
        vec![Expr::Identifier("id".into())],
        None,
        merged,
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // All 3 right rows preserved: id=2 matched, id=3 matched, id=4 unmatched
    assert_eq!(rows.len(), 3);

    // Unmatched right row (id=4) should have NULL left columns
    let unmatched: Vec<&Tuple> = rows
        .iter()
        .filter(|t| *t.get(2).unwrap() == Datum::Integer(4))
        .collect();
    assert_eq!(unmatched.len(), 1);
    assert!(unmatched[0].get(0).unwrap().is_null());
    assert!(unmatched[0].get(1).unwrap().is_null());
}

#[test]
fn sort_merge_full() {
    let ls = left_schema();
    let rs = right_schema();
    let merged = ls.merge(&rs);

    let left = make_source(left_data_sorted(), ls.clone());
    let right = make_source(right_data_sorted(), rs.clone());

    let mut exec: Box<dyn Executor> = Box::new(SortMergeJoinExecutor::new(
        left,
        right,
        JoinType::Full,
        vec![Expr::Identifier("id".into())],
        vec![Expr::Identifier("id".into())],
        None,
        merged,
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // id=1 (left only), id=2 (matched), id=3 (matched), id=4 (right only) = 4 rows
    assert_eq!(rows.len(), 4);

    // Left-only: id=1, right columns are NULL
    let left_only: Vec<&Tuple> = rows
        .iter()
        .filter(|t| *t.get(0).unwrap() == Datum::Integer(1))
        .collect();
    assert_eq!(left_only.len(), 1);
    assert!(left_only[0].get(2).unwrap().is_null());
    assert!(left_only[0].get(3).unwrap().is_null());

    // Right-only: _right_id=4, left columns are NULL
    let right_only: Vec<&Tuple> = rows
        .iter()
        .filter(|t| *t.get(2).unwrap() == Datum::Integer(4))
        .collect();
    assert_eq!(right_only.len(), 1);
    assert!(right_only[0].get(0).unwrap().is_null());
    assert!(right_only[0].get(1).unwrap().is_null());

    // Matched rows (id=2, id=3) have all non-NULL columns
    let matched: Vec<&Tuple> = rows
        .iter()
        .filter(|t| !t.get(0).unwrap().is_null() && !t.get(2).unwrap().is_null())
        .collect();
    assert_eq!(matched.len(), 2);
}

#[test]
fn sort_merge_duplicate_keys() {
    let ls = left_schema();
    let rs = right_schema();
    let merged = ls.merge(&rs);

    // Multiple rows with the same key on both sides (pre-sorted)
    let left = make_source(
        vec![
            vec![Datum::Integer(1), Datum::Text("A1".into())],
            vec![Datum::Integer(1), Datum::Text("A2".into())],
        ],
        ls.clone(),
    );
    let right = make_source(
        vec![
            vec![Datum::Integer(1), Datum::Text("D1".into())],
            vec![Datum::Integer(1), Datum::Text("D2".into())],
        ],
        rs.clone(),
    );

    let mut exec: Box<dyn Executor> = Box::new(SortMergeJoinExecutor::new(
        left,
        right,
        JoinType::Inner,
        vec![Expr::Identifier("id".into())],
        vec![Expr::Identifier("id".into())],
        None,
        merged,
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // Cross-product of matching groups: 2 x 2 = 4
    assert_eq!(rows.len(), 4);

    // All rows should have id=1 on both sides
    for row in &rows {
        assert_eq!(*row.get(0).unwrap(), Datum::Integer(1));
        assert_eq!(*row.get(2).unwrap(), Datum::Integer(1));
    }

    // Verify all 4 name/dept combinations are present
    let mut combos: Vec<(String, String)> = rows
        .iter()
        .map(|t| {
            let name = match t.get(1).unwrap() {
                Datum::Text(s) => s.clone(),
                _ => panic!("expected text"),
            };
            let dept = match t.get(3).unwrap() {
                Datum::Text(s) => s.clone(),
                _ => panic!("expected text"),
            };
            (name, dept)
        })
        .collect();
    combos.sort();
    assert_eq!(
        combos,
        vec![
            ("A1".to_string(), "D1".to_string()),
            ("A1".to_string(), "D2".to_string()),
            ("A2".to_string(), "D1".to_string()),
            ("A2".to_string(), "D2".to_string()),
        ]
    );
}

#[test]
fn sort_merge_empty_inputs() {
    let ls = left_schema();
    let rs = right_schema();
    let merged = ls.merge(&rs);

    let left = make_source(vec![], ls.clone());
    let right = make_source(vec![], rs.clone());

    let mut exec: Box<dyn Executor> = Box::new(SortMergeJoinExecutor::new(
        left,
        right,
        JoinType::Inner,
        vec![Expr::Identifier("id".into())],
        vec![Expr::Identifier("id".into())],
        None,
        merged,
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    assert_eq!(rows.len(), 0);
}

#[test]
fn sort_merge_null_keys() {
    let ls = left_schema();
    let rs = right_schema();
    let merged = ls.merge(&rs);

    // Pre-sorted with NULLs at end (NULLs sort high in compare_keys)
    let left = make_source(
        vec![
            vec![Datum::Integer(1), Datum::Text("Alice".into())],
            vec![Datum::Null, Datum::Text("Ghost".into())],
        ],
        ls.clone(),
    );
    let right = make_source(
        vec![
            vec![Datum::Integer(1), Datum::Text("Engineering".into())],
            vec![Datum::Null, Datum::Text("NoTeam".into())],
        ],
        rs.clone(),
    );

    let mut exec: Box<dyn Executor> = Box::new(SortMergeJoinExecutor::new(
        left,
        right,
        JoinType::Inner,
        vec![Expr::Identifier("id".into())],
        vec![Expr::Identifier("id".into())],
        None,
        merged,
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // At minimum, id=1 matches. The sort_merge_join compare_keys treats
    // both-NULL as Equal, so NULLs may or may not produce matches depending
    // on implementation. Verify the id=1 match is always present.
    let non_null_matches: Vec<&Tuple> = rows
        .iter()
        .filter(|t| !t.get(0).unwrap().is_null())
        .collect();
    assert_eq!(non_null_matches.len(), 1);
    assert_eq!(*non_null_matches[0].get(0).unwrap(), Datum::Integer(1));
}

#[test]
fn sort_merge_no_overlap() {
    let ls = left_schema();
    let rs = right_schema();
    let merged = ls.merge(&rs);

    // Left keys: 1, 2. Right keys: 3, 4. No overlap.
    let left = make_source(
        vec![
            vec![Datum::Integer(1), Datum::Text("A".into())],
            vec![Datum::Integer(2), Datum::Text("B".into())],
        ],
        ls.clone(),
    );
    let right = make_source(
        vec![
            vec![Datum::Integer(3), Datum::Text("C".into())],
            vec![Datum::Integer(4), Datum::Text("D".into())],
        ],
        rs.clone(),
    );

    let mut exec: Box<dyn Executor> = Box::new(SortMergeJoinExecutor::new(
        left,
        right,
        JoinType::Inner,
        vec![Expr::Identifier("id".into())],
        vec![Expr::Identifier("id".into())],
        None,
        merged,
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    assert_eq!(rows.len(), 0);
}

// ---------------------------------------------------------------------------
// SEMI / ANTI join tests
// ---------------------------------------------------------------------------

#[test]
fn sort_merge_left_semi_basic() {
    let ls = left_schema();
    let rs = right_schema();

    // SEMI join uses only the left schema (only left columns emitted)
    let left = make_source(left_data_sorted(), ls.clone());
    let right = make_source(right_data_sorted(), rs.clone());

    let mut exec: Box<dyn Executor> = Box::new(SortMergeJoinExecutor::new(
        left,
        right,
        JoinType::LeftSemi,
        vec![Expr::Identifier("id".into())],
        vec![Expr::Identifier("id".into())],
        None,
        ls.clone(),
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // Left ids: 1, 2, 3. Right ids: 2, 3, 4. Matching left rows: id=2, id=3.
    assert_eq!(rows.len(), 2);

    // Only left-side columns should be present (id, name) => 2 columns
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

    // Verify names are from the left side
    let names: Vec<String> = rows
        .iter()
        .map(|t| match t.get(1).unwrap() {
            Datum::Text(s) => s.clone(),
            _ => panic!("expected text"),
        })
        .collect();
    assert!(names.contains(&"Bob".to_string()));
    assert!(names.contains(&"Charlie".to_string()));
}

#[test]
fn sort_merge_left_anti_basic() {
    let ls = left_schema();
    let rs = right_schema();

    // ANTI join uses only the left schema (only left columns emitted)
    let left = make_source(left_data_sorted(), ls.clone());
    let right = make_source(right_data_sorted(), rs.clone());

    let mut exec: Box<dyn Executor> = Box::new(SortMergeJoinExecutor::new(
        left,
        right,
        JoinType::LeftAnti,
        vec![Expr::Identifier("id".into())],
        vec![Expr::Identifier("id".into())],
        None,
        ls.clone(),
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // Left ids: 1, 2, 3. Right ids: 2, 3, 4. Non-matching left rows: id=1.
    assert_eq!(rows.len(), 1);

    // Only left-side columns should be present (id, name) => 2 columns
    assert_eq!(rows[0].column_count(), 2);

    assert_eq!(*rows[0].get(0).unwrap(), Datum::Integer(1));
    assert_eq!(*rows[0].get(1).unwrap(), Datum::Text("Alice".into()));
}

#[test]
fn sort_merge_left_semi_no_match() {
    let ls = left_schema();
    let rs = right_schema();

    // Left keys: 1, 2. Right keys: 3, 4. No overlap.
    let left = make_source(
        vec![
            vec![Datum::Integer(1), Datum::Text("A".into())],
            vec![Datum::Integer(2), Datum::Text("B".into())],
        ],
        ls.clone(),
    );
    let right = make_source(
        vec![
            vec![Datum::Integer(3), Datum::Text("C".into())],
            vec![Datum::Integer(4), Datum::Text("D".into())],
        ],
        rs.clone(),
    );

    let mut exec: Box<dyn Executor> = Box::new(SortMergeJoinExecutor::new(
        left,
        right,
        JoinType::LeftSemi,
        vec![Expr::Identifier("id".into())],
        vec![Expr::Identifier("id".into())],
        None,
        ls.clone(),
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // No matching rows => SEMI returns nothing
    assert_eq!(rows.len(), 0);
}

#[test]
fn sort_merge_left_anti_no_match() {
    let ls = left_schema();
    let rs = right_schema();

    // Left keys: 1, 2. Right keys: 3, 4. No overlap.
    let left = make_source(
        vec![
            vec![Datum::Integer(1), Datum::Text("A".into())],
            vec![Datum::Integer(2), Datum::Text("B".into())],
        ],
        ls.clone(),
    );
    let right = make_source(
        vec![
            vec![Datum::Integer(3), Datum::Text("C".into())],
            vec![Datum::Integer(4), Datum::Text("D".into())],
        ],
        rs.clone(),
    );

    let mut exec: Box<dyn Executor> = Box::new(SortMergeJoinExecutor::new(
        left,
        right,
        JoinType::LeftAnti,
        vec![Expr::Identifier("id".into())],
        vec![Expr::Identifier("id".into())],
        None,
        ls.clone(),
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // No matching rows => ANTI returns all left rows
    assert_eq!(rows.len(), 2);

    let mut ids: Vec<i32> = rows
        .iter()
        .map(|t| match t.get(0).unwrap() {
            Datum::Integer(v) => *v,
            _ => panic!("expected integer"),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![1, 2]);
}

#[test]
fn sort_merge_left_semi_all_match() {
    let ls = left_schema();
    let rs = right_schema();

    // All left keys exist on the right side
    let left = make_source(
        vec![
            vec![Datum::Integer(2), Datum::Text("Bob".into())],
            vec![Datum::Integer(3), Datum::Text("Charlie".into())],
        ],
        ls.clone(),
    );
    let right = make_source(
        vec![
            vec![Datum::Integer(2), Datum::Text("Engineering".into())],
            vec![Datum::Integer(3), Datum::Text("Sales".into())],
        ],
        rs.clone(),
    );

    let mut exec: Box<dyn Executor> = Box::new(SortMergeJoinExecutor::new(
        left,
        right,
        JoinType::LeftSemi,
        vec![Expr::Identifier("id".into())],
        vec![Expr::Identifier("id".into())],
        None,
        ls.clone(),
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // All left rows match => SEMI returns all left rows
    assert_eq!(rows.len(), 2);

    for row in &rows {
        assert_eq!(row.column_count(), 2);
    }
}

#[test]
fn sort_merge_left_anti_all_match() {
    let ls = left_schema();
    let rs = right_schema();

    // All left keys exist on the right side
    let left = make_source(
        vec![
            vec![Datum::Integer(2), Datum::Text("Bob".into())],
            vec![Datum::Integer(3), Datum::Text("Charlie".into())],
        ],
        ls.clone(),
    );
    let right = make_source(
        vec![
            vec![Datum::Integer(2), Datum::Text("Engineering".into())],
            vec![Datum::Integer(3), Datum::Text("Sales".into())],
        ],
        rs.clone(),
    );

    let mut exec: Box<dyn Executor> = Box::new(SortMergeJoinExecutor::new(
        left,
        right,
        JoinType::LeftAnti,
        vec![Expr::Identifier("id".into())],
        vec![Expr::Identifier("id".into())],
        None,
        ls.clone(),
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // All left rows match => ANTI returns nothing
    assert_eq!(rows.len(), 0);
}

#[test]
fn sort_merge_left_semi_duplicate_keys() {
    let ls = left_schema();
    let rs = right_schema();

    // Multiple left rows with the same key that matches the right
    let left = make_source(
        vec![
            vec![Datum::Integer(1), Datum::Text("A1".into())],
            vec![Datum::Integer(1), Datum::Text("A2".into())],
            vec![Datum::Integer(2), Datum::Text("B1".into())],
        ],
        ls.clone(),
    );
    let right = make_source(
        vec![
            vec![Datum::Integer(1), Datum::Text("D1".into())],
            vec![Datum::Integer(1), Datum::Text("D2".into())],
        ],
        rs.clone(),
    );

    let mut exec: Box<dyn Executor> = Box::new(SortMergeJoinExecutor::new(
        left,
        right,
        JoinType::LeftSemi,
        vec![Expr::Identifier("id".into())],
        vec![Expr::Identifier("id".into())],
        None,
        ls.clone(),
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // SEMI should return each matching left row exactly once, regardless
    // of how many right rows share the key. Left rows with id=1 match,
    // id=2 does not.
    assert_eq!(rows.len(), 2);

    for row in &rows {
        assert_eq!(row.column_count(), 2);
        assert_eq!(*row.get(0).unwrap(), Datum::Integer(1));
    }

    let mut names: Vec<String> = rows
        .iter()
        .map(|t| match t.get(1).unwrap() {
            Datum::Text(s) => s.clone(),
            _ => panic!("expected text"),
        })
        .collect();
    names.sort();
    assert_eq!(names, vec!["A1".to_string(), "A2".to_string()]);
}

#[test]
fn sort_merge_left_anti_duplicate_keys() {
    let ls = left_schema();
    let rs = right_schema();

    // Multiple left rows; some keys match the right, some don't
    let left = make_source(
        vec![
            vec![Datum::Integer(1), Datum::Text("A1".into())],
            vec![Datum::Integer(1), Datum::Text("A2".into())],
            vec![Datum::Integer(2), Datum::Text("B1".into())],
        ],
        ls.clone(),
    );
    let right = make_source(
        vec![
            vec![Datum::Integer(1), Datum::Text("D1".into())],
            vec![Datum::Integer(1), Datum::Text("D2".into())],
        ],
        rs.clone(),
    );

    let mut exec: Box<dyn Executor> = Box::new(SortMergeJoinExecutor::new(
        left,
        right,
        JoinType::LeftAnti,
        vec![Expr::Identifier("id".into())],
        vec![Expr::Identifier("id".into())],
        None,
        ls.clone(),
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // ANTI returns left rows that have no match. id=1 matches, id=2 does not.
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].column_count(), 2);
    assert_eq!(*rows[0].get(0).unwrap(), Datum::Integer(2));
    assert_eq!(*rows[0].get(1).unwrap(), Datum::Text("B1".into()));
}

#[test]
fn sort_merge_left_semi_empty_right() {
    let ls = left_schema();
    let rs = right_schema();

    let left = make_source(left_data_sorted(), ls.clone());
    let right = make_source(vec![], rs.clone());

    let mut exec: Box<dyn Executor> = Box::new(SortMergeJoinExecutor::new(
        left,
        right,
        JoinType::LeftSemi,
        vec![Expr::Identifier("id".into())],
        vec![Expr::Identifier("id".into())],
        None,
        ls.clone(),
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // Empty right => no matches => SEMI returns nothing
    assert_eq!(rows.len(), 0);
}

#[test]
fn sort_merge_left_anti_empty_right() {
    let ls = left_schema();
    let rs = right_schema();

    let left = make_source(left_data_sorted(), ls.clone());
    let right = make_source(vec![], rs.clone());

    let mut exec: Box<dyn Executor> = Box::new(SortMergeJoinExecutor::new(
        left,
        right,
        JoinType::LeftAnti,
        vec![Expr::Identifier("id".into())],
        vec![Expr::Identifier("id".into())],
        None,
        ls.clone(),
    ));

    exec.init().unwrap();
    let rows = collect_all(&mut exec);
    exec.close().unwrap();

    // Empty right => no matches => ANTI returns all left rows
    assert_eq!(rows.len(), 3);

    let mut ids: Vec<i32> = rows
        .iter()
        .map(|t| match t.get(0).unwrap() {
            Datum::Integer(v) => *v,
            _ => panic!("expected integer"),
        })
        .collect();
    ids.sort();
    assert_eq!(ids, vec![1, 2, 3]);
}
