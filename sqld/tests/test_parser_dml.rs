use sqld::sql::ast::*;
use sqld::sql::parser::parse;

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

fn first_stmt(sql: &str) -> Statement {
    let r = parse(sql);
    assert!(r.errors.is_empty(), "parse errors: {:?}", r.errors);
    r.statements.into_iter().next().unwrap()
}

// ===========================================================================
// INSERT
// ===========================================================================

// ---- 1. Simple VALUES ----

#[test]
fn insert_simple_values() {
    let stmt = first_stmt("INSERT INTO t VALUES (1, 'a', true)");
    let Statement::Insert(ins) = stmt else {
        panic!("expected Insert, got {:?}", stmt);
    };

    assert_eq!(ins.table, "t");
    assert!(ins.columns.is_none());
    assert!(ins.returning.is_none());

    let InsertSource::Values(rows) = &ins.source else {
        panic!("expected Values source, got {:?}", ins.source);
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 3);
    assert_eq!(rows[0][0], Expr::Integer(1));
    assert_eq!(rows[0][1], Expr::String("a".to_string()));
    assert_eq!(rows[0][2], Expr::Boolean(true));
}

// ---- 2. With column list ----

#[test]
fn insert_with_column_list() {
    let stmt = first_stmt("INSERT INTO t (a, b) VALUES (1, 2)");
    let Statement::Insert(ins) = stmt else {
        panic!("expected Insert, got {:?}", stmt);
    };

    assert_eq!(ins.table, "t");
    let cols = ins.columns.as_ref().expect("expected column list");
    assert_eq!(cols, &["a".to_string(), "b".to_string()]);

    let InsertSource::Values(rows) = &ins.source else {
        panic!("expected Values source");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], vec![Expr::Integer(1), Expr::Integer(2)]);
}

// ---- 3. Multi-row VALUES ----

#[test]
fn insert_multi_row_values() {
    let stmt = first_stmt("INSERT INTO t VALUES (1, 2), (3, 4)");
    let Statement::Insert(ins) = stmt else {
        panic!("expected Insert, got {:?}", stmt);
    };

    assert_eq!(ins.table, "t");
    assert!(ins.columns.is_none());

    let InsertSource::Values(rows) = &ins.source else {
        panic!("expected Values source");
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![Expr::Integer(1), Expr::Integer(2)]);
    assert_eq!(rows[1], vec![Expr::Integer(3), Expr::Integer(4)]);
}

// ---- 4. INSERT...SELECT ----

#[test]
fn insert_select() {
    let stmt = first_stmt("INSERT INTO t SELECT * FROM u");
    let Statement::Insert(ins) = stmt else {
        panic!("expected Insert, got {:?}", stmt);
    };

    assert_eq!(ins.table, "t");
    assert!(ins.columns.is_none());
    assert!(ins.returning.is_none());

    let InsertSource::Select(sel) = &ins.source else {
        panic!("expected Select source, got {:?}", ins.source);
    };
    // The select should have AllColumns and FROM u
    assert_eq!(sel.columns.len(), 1);
    assert_eq!(sel.columns[0], SelectColumn::AllColumns);
    let from = sel.from.as_ref().expect("expected FROM clause");
    match &from.table {
        TableRef::Table { name, alias } => {
            assert_eq!(name, "u");
            assert!(alias.is_none());
        }
        other => panic!("expected TableRef::Table, got {:?}", other),
    }
}

// ---- 5. With placeholder ----

#[test]
fn insert_with_placeholder() {
    let stmt = first_stmt("INSERT INTO t (a) VALUES ($1)");
    let Statement::Insert(ins) = stmt else {
        panic!("expected Insert, got {:?}", stmt);
    };

    assert_eq!(ins.table, "t");
    let cols = ins.columns.as_ref().expect("expected column list");
    assert_eq!(cols, &["a".to_string()]);

    let InsertSource::Values(rows) = &ins.source else {
        panic!("expected Values source");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 1);
    assert_eq!(rows[0][0], Expr::Placeholder(1));
}

// ---- 6. RETURNING star ----

#[test]
fn insert_returning_star() {
    let stmt = first_stmt("INSERT INTO t (a) VALUES (1) RETURNING *");
    let Statement::Insert(ins) = stmt else {
        panic!("expected Insert, got {:?}", stmt);
    };

    assert_eq!(ins.table, "t");
    let ret = ins.returning.as_ref().expect("expected RETURNING clause");
    assert_eq!(ret.len(), 1);
    assert_eq!(ret[0], SelectColumn::AllColumns);
}

// ---- 7. RETURNING columns with alias ----

#[test]
fn insert_returning_columns_with_alias() {
    let stmt = first_stmt("INSERT INTO t (a) VALUES (1) RETURNING a, b AS x");
    let Statement::Insert(ins) = stmt else {
        panic!("expected Insert, got {:?}", stmt);
    };

    let ret = ins.returning.as_ref().expect("expected RETURNING clause");
    assert_eq!(ret.len(), 2);

    match &ret[0] {
        SelectColumn::Expr { expr, alias } => {
            assert_eq!(*expr, Expr::Identifier("a".to_string()));
            assert!(alias.is_none());
        }
        other => panic!("expected Expr column, got {:?}", other),
    }

    match &ret[1] {
        SelectColumn::Expr { expr, alias } => {
            assert_eq!(*expr, Expr::Identifier("b".to_string()));
            assert_eq!(alias.as_deref(), Some("x"));
        }
        other => panic!("expected Expr column with alias, got {:?}", other),
    }
}

// ===========================================================================
// UPDATE
// ===========================================================================

// ---- 8. Simple SET ----

#[test]
fn update_simple_set() {
    let stmt = first_stmt("UPDATE t SET a = 1");
    let Statement::Update(upd) = stmt else {
        panic!("expected Update, got {:?}", stmt);
    };

    assert_eq!(upd.table, "t");
    assert!(upd.where_clause.is_none());
    assert!(upd.returning.is_none());
    assert_eq!(upd.assignments.len(), 1);
    assert_eq!(upd.assignments[0].column, "a");
    assert_eq!(upd.assignments[0].value, Expr::Integer(1));
}

// ---- 9. Multiple assignments ----

#[test]
fn update_multiple_assignments() {
    let stmt = first_stmt("UPDATE t SET a = 1, b = 'hello'");
    let Statement::Update(upd) = stmt else {
        panic!("expected Update, got {:?}", stmt);
    };

    assert_eq!(upd.table, "t");
    assert_eq!(upd.assignments.len(), 2);

    assert_eq!(upd.assignments[0].column, "a");
    assert_eq!(upd.assignments[0].value, Expr::Integer(1));

    assert_eq!(upd.assignments[1].column, "b");
    assert_eq!(upd.assignments[1].value, Expr::String("hello".to_string()));
}

// ---- 10. Expression in value with WHERE ----

#[test]
fn update_expression_value_with_where() {
    let stmt = first_stmt("UPDATE t SET a = a + 1 WHERE id = 5");
    let Statement::Update(upd) = stmt else {
        panic!("expected Update, got {:?}", stmt);
    };

    assert_eq!(upd.table, "t");
    assert_eq!(upd.assignments.len(), 1);
    assert_eq!(upd.assignments[0].column, "a");

    // Value should be BinaryOp: a + 1
    match &upd.assignments[0].value {
        Expr::BinaryOp { left, op, right } => {
            assert_eq!(**left, Expr::Identifier("a".to_string()));
            assert_eq!(*op, BinaryOp::Add);
            assert_eq!(**right, Expr::Integer(1));
        }
        other => panic!("expected BinaryOp for assignment value, got {:?}", other),
    }

    // WHERE clause: id = 5
    let where_expr = upd.where_clause.as_ref().expect("expected WHERE clause");
    match where_expr {
        Expr::BinaryOp { left, op, right } => {
            assert_eq!(**left, Expr::Identifier("id".to_string()));
            assert_eq!(*op, BinaryOp::Eq);
            assert_eq!(**right, Expr::Integer(5));
        }
        other => panic!("expected BinaryOp for WHERE, got {:?}", other),
    }
}

// ---- 11. UPDATE RETURNING * ----

#[test]
fn update_returning_star() {
    let stmt = first_stmt("UPDATE t SET a = 1 RETURNING *");
    let Statement::Update(upd) = stmt else {
        panic!("expected Update, got {:?}", stmt);
    };

    assert_eq!(upd.table, "t");
    assert!(upd.where_clause.is_none());

    let ret = upd.returning.as_ref().expect("expected RETURNING clause");
    assert_eq!(ret.len(), 1);
    assert_eq!(ret[0], SelectColumn::AllColumns);
}

// ---- 12. WHERE + RETURNING columns ----

#[test]
fn update_where_and_returning_columns() {
    let stmt = first_stmt("UPDATE t SET a = 1 WHERE b > 2 RETURNING a, b");
    let Statement::Update(upd) = stmt else {
        panic!("expected Update, got {:?}", stmt);
    };

    assert_eq!(upd.table, "t");

    // WHERE: b > 2
    let where_expr = upd.where_clause.as_ref().expect("expected WHERE clause");
    match where_expr {
        Expr::BinaryOp { left, op, right } => {
            assert_eq!(**left, Expr::Identifier("b".to_string()));
            assert_eq!(*op, BinaryOp::Gt);
            assert_eq!(**right, Expr::Integer(2));
        }
        other => panic!("expected BinaryOp for WHERE, got {:?}", other),
    }

    // RETURNING a, b
    let ret = upd.returning.as_ref().expect("expected RETURNING clause");
    assert_eq!(ret.len(), 2);

    match &ret[0] {
        SelectColumn::Expr { expr, alias } => {
            assert_eq!(*expr, Expr::Identifier("a".to_string()));
            assert!(alias.is_none());
        }
        other => panic!("expected Expr column, got {:?}", other),
    }
    match &ret[1] {
        SelectColumn::Expr { expr, alias } => {
            assert_eq!(*expr, Expr::Identifier("b".to_string()));
            assert!(alias.is_none());
        }
        other => panic!("expected Expr column, got {:?}", other),
    }
}

// ===========================================================================
// DELETE
// ===========================================================================

// ---- 13. Simple delete all ----

#[test]
fn delete_simple() {
    let stmt = first_stmt("DELETE FROM t");
    let Statement::Delete(del) = stmt else {
        panic!("expected Delete, got {:?}", stmt);
    };

    assert_eq!(del.table, "t");
    assert!(del.where_clause.is_none());
    assert!(del.returning.is_none());
}

// ---- 14. DELETE with WHERE ----

#[test]
fn delete_with_where() {
    let stmt = first_stmt("DELETE FROM t WHERE id = 1");
    let Statement::Delete(del) = stmt else {
        panic!("expected Delete, got {:?}", stmt);
    };

    assert_eq!(del.table, "t");
    let where_expr = del.where_clause.as_ref().expect("expected WHERE clause");
    match where_expr {
        Expr::BinaryOp { left, op, right } => {
            assert_eq!(**left, Expr::Identifier("id".to_string()));
            assert_eq!(*op, BinaryOp::Eq);
            assert_eq!(**right, Expr::Integer(1));
        }
        other => panic!("expected BinaryOp for WHERE, got {:?}", other),
    }

    assert!(del.returning.is_none());
}

// ---- 15. Compound WHERE (AND) ----

#[test]
fn delete_compound_where() {
    let stmt = first_stmt("DELETE FROM t WHERE a > 1 AND b < 2");
    let Statement::Delete(del) = stmt else {
        panic!("expected Delete, got {:?}", stmt);
    };

    assert_eq!(del.table, "t");
    let where_expr = del.where_clause.as_ref().expect("expected WHERE clause");

    // Top-level should be AND
    match where_expr {
        Expr::BinaryOp { left, op, right } => {
            assert_eq!(*op, BinaryOp::And);

            // left: a > 1
            match left.as_ref() {
                Expr::BinaryOp { left: ll, op: lop, right: lr } => {
                    assert_eq!(**ll, Expr::Identifier("a".to_string()));
                    assert_eq!(*lop, BinaryOp::Gt);
                    assert_eq!(**lr, Expr::Integer(1));
                }
                other => panic!("expected BinaryOp for left side of AND, got {:?}", other),
            }

            // right: b < 2
            match right.as_ref() {
                Expr::BinaryOp { left: rl, op: rop, right: rr } => {
                    assert_eq!(**rl, Expr::Identifier("b".to_string()));
                    assert_eq!(*rop, BinaryOp::Lt);
                    assert_eq!(**rr, Expr::Integer(2));
                }
                other => panic!("expected BinaryOp for right side of AND, got {:?}", other),
            }
        }
        other => panic!("expected BinaryOp (AND) for WHERE, got {:?}", other),
    }
}

// ---- 16. DELETE RETURNING * ----

#[test]
fn delete_returning_star() {
    let stmt = first_stmt("DELETE FROM t RETURNING *");
    let Statement::Delete(del) = stmt else {
        panic!("expected Delete, got {:?}", stmt);
    };

    assert_eq!(del.table, "t");
    assert!(del.where_clause.is_none());

    let ret = del.returning.as_ref().expect("expected RETURNING clause");
    assert_eq!(ret.len(), 1);
    assert_eq!(ret[0], SelectColumn::AllColumns);
}

// ---- 17. DELETE WHERE + RETURNING columns ----

#[test]
fn delete_where_and_returning_columns() {
    let stmt = first_stmt("DELETE FROM t WHERE id = 1 RETURNING id, name");
    let Statement::Delete(del) = stmt else {
        panic!("expected Delete, got {:?}", stmt);
    };

    assert_eq!(del.table, "t");

    // WHERE: id = 1
    let where_expr = del.where_clause.as_ref().expect("expected WHERE clause");
    match where_expr {
        Expr::BinaryOp { left, op, right } => {
            assert_eq!(**left, Expr::Identifier("id".to_string()));
            assert_eq!(*op, BinaryOp::Eq);
            assert_eq!(**right, Expr::Integer(1));
        }
        other => panic!("expected BinaryOp for WHERE, got {:?}", other),
    }

    // RETURNING id, name
    let ret = del.returning.as_ref().expect("expected RETURNING clause");
    assert_eq!(ret.len(), 2);

    match &ret[0] {
        SelectColumn::Expr { expr, alias } => {
            assert_eq!(*expr, Expr::Identifier("id".to_string()));
            assert!(alias.is_none());
        }
        other => panic!("expected Expr column, got {:?}", other),
    }
    match &ret[1] {
        SelectColumn::Expr { expr, alias } => {
            assert_eq!(*expr, Expr::Identifier("name".to_string()));
            assert!(alias.is_none());
        }
        other => panic!("expected Expr column, got {:?}", other),
    }
}
