use sqld::sql::ast::*;
use sqld::sql::parser::parse;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn first_stmt(sql: &str) -> Statement {
    let r = parse(sql);
    assert!(r.errors.is_empty(), "parse errors: {:?}", r.errors);
    r.statements.into_iter().next().unwrap()
}

fn first_select(sql: &str) -> Select {
    match first_stmt(sql) {
        Statement::Select(s) => s,
        other => panic!("expected SELECT, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// 1. SELECT 1 — simple literal expression, no FROM
// ---------------------------------------------------------------------------

#[test]
fn test_select_literal_no_from() {
    let sel = first_select("SELECT 1");
    assert!(!sel.distinct);
    assert_eq!(sel.columns.len(), 1);
    assert!(matches!(
        &sel.columns[0],
        SelectColumn::Expr { expr: Expr::Integer(1), alias: None }
    ));
    assert!(sel.from.is_none());
    assert!(sel.where_clause.is_none());
    assert!(sel.group_by.is_empty());
    assert!(sel.having.is_none());
    assert!(sel.order_by.is_empty());
    assert!(sel.limit.is_none());
    assert!(sel.offset.is_none());
    assert!(sel.set_op.is_none());
}

// ---------------------------------------------------------------------------
// 2. SELECT * — star column
// ---------------------------------------------------------------------------

#[test]
fn test_select_star() {
    let sel = first_select("SELECT *");
    assert_eq!(sel.columns.len(), 1);
    assert!(matches!(&sel.columns[0], SelectColumn::AllColumns));
}

// ---------------------------------------------------------------------------
// 3. SELECT DISTINCT a, b FROM t — distinct, multiple columns
// ---------------------------------------------------------------------------

#[test]
fn test_select_distinct_multiple_columns() {
    let sel = first_select("SELECT DISTINCT a, b FROM t");
    assert!(sel.distinct);
    assert_eq!(sel.columns.len(), 2);
    assert!(matches!(
        &sel.columns[0],
        SelectColumn::Expr { expr: Expr::Identifier(name), alias: None } if name == "a"
    ));
    assert!(matches!(
        &sel.columns[1],
        SelectColumn::Expr { expr: Expr::Identifier(name), alias: None } if name == "b"
    ));
    assert!(sel.from.is_some());
    let from = sel.from.unwrap();
    assert!(matches!(
        &from.table,
        TableRef::Table { name, alias: None } if name == "t"
    ));
}

// ---------------------------------------------------------------------------
// 4. SELECT a AS x, b y FROM t — aliases (explicit AS + implicit)
// ---------------------------------------------------------------------------

#[test]
fn test_select_aliases() {
    let sel = first_select("SELECT a AS x, b y FROM t");
    assert_eq!(sel.columns.len(), 2);
    // Explicit alias: a AS x
    assert!(matches!(
        &sel.columns[0],
        SelectColumn::Expr { expr: Expr::Identifier(name), alias: Some(a) }
            if name == "a" && a == "x"
    ));
    // Implicit alias: b y
    assert!(matches!(
        &sel.columns[1],
        SelectColumn::Expr { expr: Expr::Identifier(name), alias: Some(a) }
            if name == "b" && a == "y"
    ));
}

// ---------------------------------------------------------------------------
// 5. SELECT t.* FROM t — table qualified star
// ---------------------------------------------------------------------------

#[test]
fn test_select_table_qualified_star() {
    let sel = first_select("SELECT t.* FROM t");
    assert_eq!(sel.columns.len(), 1);
    assert!(matches!(
        &sel.columns[0],
        SelectColumn::TableAllColumns(table) if table == "t"
    ));
}

// ---------------------------------------------------------------------------
// 6. SELECT t.col FROM t — qualified column
// ---------------------------------------------------------------------------

#[test]
fn test_select_qualified_column() {
    let sel = first_select("SELECT t.col FROM t");
    assert_eq!(sel.columns.len(), 1);
    match &sel.columns[0] {
        SelectColumn::Expr {
            expr: Expr::QualifiedIdentifier { table, column },
            alias: None,
        } => {
            assert_eq!(table, "t");
            assert_eq!(column, "col");
        }
        other => panic!("expected QualifiedIdentifier, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// 7. SELECT * FROM t WHERE a > 1 — simple WHERE
// ---------------------------------------------------------------------------

#[test]
fn test_select_where_simple() {
    let sel = first_select("SELECT * FROM t WHERE a > 1");
    assert!(sel.where_clause.is_some());
    match sel.where_clause.unwrap() {
        Expr::BinaryOp { left, op, right } => {
            assert!(matches!(*left, Expr::Identifier(ref name) if name == "a"));
            assert_eq!(op, BinaryOp::Gt);
            assert!(matches!(*right, Expr::Integer(1)));
        }
        other => panic!("expected BinaryOp, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// 8. SELECT * FROM t WHERE a > 1 AND b < 2 — compound WHERE
// ---------------------------------------------------------------------------

#[test]
fn test_select_where_compound() {
    let sel = first_select("SELECT * FROM t WHERE a > 1 AND b < 2");
    match sel.where_clause.unwrap() {
        Expr::BinaryOp { left, op, right } => {
            assert_eq!(op, BinaryOp::And);
            // Left: a > 1
            match *left {
                Expr::BinaryOp {
                    left: ll,
                    op: lop,
                    right: lr,
                } => {
                    assert!(matches!(*ll, Expr::Identifier(ref n) if n == "a"));
                    assert_eq!(lop, BinaryOp::Gt);
                    assert!(matches!(*lr, Expr::Integer(1)));
                }
                other => panic!("expected left BinaryOp, got {:?}", other),
            }
            // Right: b < 2
            match *right {
                Expr::BinaryOp {
                    left: rl,
                    op: rop,
                    right: rr,
                } => {
                    assert!(matches!(*rl, Expr::Identifier(ref n) if n == "b"));
                    assert_eq!(rop, BinaryOp::Lt);
                    assert!(matches!(*rr, Expr::Integer(2)));
                }
                other => panic!("expected right BinaryOp, got {:?}", other),
            }
        }
        other => panic!("expected AND BinaryOp, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// 9. SELECT * FROM users u — implicit table alias
// ---------------------------------------------------------------------------

#[test]
fn test_select_implicit_table_alias() {
    let sel = first_select("SELECT * FROM users u");
    let from = sel.from.unwrap();
    match &from.table {
        TableRef::Table { name, alias } => {
            assert_eq!(name, "users");
            assert_eq!(alias.as_deref(), Some("u"));
        }
        other => panic!("expected Table, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// 10. SELECT * FROM (SELECT 1) AS sub — subquery in FROM
// ---------------------------------------------------------------------------

#[test]
fn test_select_subquery_in_from() {
    let sel = first_select("SELECT * FROM (SELECT 1) AS sub");
    let from = sel.from.unwrap();
    match &from.table {
        TableRef::Subquery { query, alias } => {
            assert_eq!(alias, "sub");
            assert_eq!(query.columns.len(), 1);
            assert!(matches!(
                &query.columns[0],
                SelectColumn::Expr { expr: Expr::Integer(1), alias: None }
            ));
        }
        other => panic!("expected Subquery, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// 11. INNER JOIN
// ---------------------------------------------------------------------------

#[test]
fn test_inner_join() {
    let sel = first_select("SELECT * FROM a INNER JOIN b ON a.id = b.id");
    let from = sel.from.unwrap();
    assert_eq!(from.joins.len(), 1);
    let join = &from.joins[0];
    assert_eq!(join.join_type, JoinType::Inner);
    assert!(!join.natural);
    match &join.table {
        TableRef::Table { name, alias: None } => assert_eq!(name, "b"),
        other => panic!("expected Table 'b', got {:?}", other),
    }
    match &join.condition {
        Some(JoinCondition::On(Expr::BinaryOp { left, op, right })) => {
            assert_eq!(*op, BinaryOp::Eq);
            assert!(matches!(
                left.as_ref(),
                Expr::QualifiedIdentifier { table, column } if table == "a" && column == "id"
            ));
            assert!(matches!(
                right.as_ref(),
                Expr::QualifiedIdentifier { table, column } if table == "b" && column == "id"
            ));
        }
        other => panic!("expected ON condition, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// 12. LEFT JOIN
// ---------------------------------------------------------------------------

#[test]
fn test_left_join() {
    let sel = first_select("SELECT * FROM a LEFT JOIN b ON a.id = b.id");
    let from = sel.from.unwrap();
    assert_eq!(from.joins.len(), 1);
    assert_eq!(from.joins[0].join_type, JoinType::Left);
    assert!(!from.joins[0].natural);
}

// ---------------------------------------------------------------------------
// 13. RIGHT OUTER JOIN
// ---------------------------------------------------------------------------

#[test]
fn test_right_outer_join() {
    let sel = first_select("SELECT * FROM a RIGHT OUTER JOIN b ON a.id = b.id");
    let from = sel.from.unwrap();
    assert_eq!(from.joins.len(), 1);
    assert_eq!(from.joins[0].join_type, JoinType::Right);
}

// ---------------------------------------------------------------------------
// 14. FULL JOIN
// ---------------------------------------------------------------------------

#[test]
fn test_full_join() {
    let sel = first_select("SELECT * FROM a FULL JOIN b ON a.id = b.id");
    let from = sel.from.unwrap();
    assert_eq!(from.joins.len(), 1);
    assert_eq!(from.joins[0].join_type, JoinType::Full);
}

// ---------------------------------------------------------------------------
// 15. CROSS JOIN
// ---------------------------------------------------------------------------

#[test]
fn test_cross_join() {
    let sel = first_select("SELECT * FROM a CROSS JOIN b");
    let from = sel.from.unwrap();
    assert_eq!(from.joins.len(), 1);
    assert_eq!(from.joins[0].join_type, JoinType::Cross);
    assert!(from.joins[0].condition.is_none());
}

// ---------------------------------------------------------------------------
// 16. NATURAL JOIN
// ---------------------------------------------------------------------------

#[test]
fn test_natural_join() {
    let sel = first_select("SELECT * FROM a NATURAL JOIN b");
    let from = sel.from.unwrap();
    assert_eq!(from.joins.len(), 1);
    assert!(from.joins[0].natural);
    assert!(from.joins[0].condition.is_none());
}

// ---------------------------------------------------------------------------
// 17. JOIN USING
// ---------------------------------------------------------------------------

#[test]
fn test_join_using() {
    let sel = first_select("SELECT * FROM a JOIN b USING (id)");
    let from = sel.from.unwrap();
    assert_eq!(from.joins.len(), 1);
    match &from.joins[0].condition {
        Some(JoinCondition::Using(cols)) => {
            assert_eq!(cols, &["id".to_string()]);
        }
        other => panic!("expected USING condition, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// 18. Multiple joins
// ---------------------------------------------------------------------------

#[test]
fn test_multiple_joins() {
    let sel = first_select(
        "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id",
    );
    let from = sel.from.unwrap();
    assert!(matches!(
        &from.table,
        TableRef::Table { name, alias: None } if name == "a"
    ));
    assert_eq!(from.joins.len(), 2);

    // First join: b
    match &from.joins[0].table {
        TableRef::Table { name, alias: None } => assert_eq!(name, "b"),
        other => panic!("expected Table 'b', got {:?}", other),
    }

    // Second join: c
    match &from.joins[1].table {
        TableRef::Table { name, alias: None } => assert_eq!(name, "c"),
        other => panic!("expected Table 'c', got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// 19. GROUP BY
// ---------------------------------------------------------------------------

#[test]
fn test_group_by() {
    let sel = first_select("SELECT * FROM t GROUP BY a, b");
    assert_eq!(sel.group_by.len(), 2);
    assert!(matches!(&sel.group_by[0], Expr::Identifier(n) if n == "a"));
    assert!(matches!(&sel.group_by[1], Expr::Identifier(n) if n == "b"));
}

// ---------------------------------------------------------------------------
// 20. HAVING
// ---------------------------------------------------------------------------

#[test]
fn test_having() {
    let sel = first_select("SELECT a, count(*) FROM t GROUP BY a HAVING count(*) > 1");
    assert_eq!(sel.group_by.len(), 1);
    assert!(sel.having.is_some());
    match sel.having.unwrap() {
        Expr::BinaryOp { left, op, right } => {
            assert_eq!(op, BinaryOp::Gt);
            // left should be count(*)
            match *left {
                Expr::FunctionCall { ref name, ref args, distinct } => {
                    assert_eq!(name, "count");
                    assert_eq!(args.len(), 1);
                    assert!(matches!(args[0], Expr::Star));
                    assert!(!distinct);
                }
                ref other => panic!("expected FunctionCall count(*), got {:?}", other),
            }
            assert!(matches!(*right, Expr::Integer(1)));
        }
        other => panic!("expected BinaryOp for HAVING, got {:?}", other),
    }
}

// ---------------------------------------------------------------------------
// 21. ORDER BY with directions
// ---------------------------------------------------------------------------

#[test]
fn test_order_by_directions() {
    let sel = first_select("SELECT * FROM t ORDER BY a ASC, b DESC");
    assert_eq!(sel.order_by.len(), 2);
    assert!(matches!(&sel.order_by[0].expr, Expr::Identifier(n) if n == "a"));
    assert_eq!(sel.order_by[0].direction, Some(OrderDirection::Asc));
    assert!(sel.order_by[0].nulls.is_none());

    assert!(matches!(&sel.order_by[1].expr, Expr::Identifier(n) if n == "b"));
    assert_eq!(sel.order_by[1].direction, Some(OrderDirection::Desc));
    assert!(sel.order_by[1].nulls.is_none());
}

// ---------------------------------------------------------------------------
// 22. ORDER BY DESC NULLS FIRST
// ---------------------------------------------------------------------------

#[test]
fn test_order_by_nulls_first() {
    let sel = first_select("SELECT * FROM t ORDER BY a DESC NULLS FIRST");
    assert_eq!(sel.order_by.len(), 1);
    assert_eq!(sel.order_by[0].direction, Some(OrderDirection::Desc));
    assert_eq!(sel.order_by[0].nulls, Some(NullsOrder::First));
}

// ---------------------------------------------------------------------------
// 23. ORDER BY NULLS LAST (no direction)
// ---------------------------------------------------------------------------

#[test]
fn test_order_by_nulls_last_no_direction() {
    let sel = first_select("SELECT * FROM t ORDER BY a NULLS LAST");
    assert_eq!(sel.order_by.len(), 1);
    assert!(matches!(&sel.order_by[0].expr, Expr::Identifier(n) if n == "a"));
    assert_eq!(sel.order_by[0].direction, None);
    assert_eq!(sel.order_by[0].nulls, Some(NullsOrder::Last));
}

// ---------------------------------------------------------------------------
// 24. LIMIT
// ---------------------------------------------------------------------------

#[test]
fn test_limit() {
    let sel = first_select("SELECT * FROM t LIMIT 10");
    assert!(matches!(sel.limit, Some(Expr::Integer(10))));
    assert!(sel.offset.is_none());
}

// ---------------------------------------------------------------------------
// 25. LIMIT + OFFSET
// ---------------------------------------------------------------------------

#[test]
fn test_limit_offset() {
    let sel = first_select("SELECT * FROM t LIMIT 10 OFFSET 5");
    assert!(matches!(sel.limit, Some(Expr::Integer(10))));
    assert!(matches!(sel.offset, Some(Expr::Integer(5))));
}

// ---------------------------------------------------------------------------
// 26. UNION
// ---------------------------------------------------------------------------

#[test]
fn test_union() {
    let sel = first_select("SELECT a FROM t UNION SELECT b FROM u");
    assert!(sel.set_op.is_some());
    let set_op = sel.set_op.unwrap();
    assert_eq!(set_op.op, SetOperator::Union);
    assert!(!set_op.all);

    // Left side
    assert_eq!(sel.columns.len(), 1);
    assert!(matches!(
        &sel.columns[0],
        SelectColumn::Expr { expr: Expr::Identifier(n), alias: None } if n == "a"
    ));
    let left_from = sel.from.unwrap();
    assert!(matches!(
        &left_from.table,
        TableRef::Table { name, alias: None } if name == "t"
    ));

    // Right side
    let right = &set_op.right;
    assert_eq!(right.columns.len(), 1);
    assert!(matches!(
        &right.columns[0],
        SelectColumn::Expr { expr: Expr::Identifier(n), alias: None } if n == "b"
    ));
    let right_from = right.from.as_ref().unwrap();
    assert!(matches!(
        &right_from.table,
        TableRef::Table { name, alias: None } if name == "u"
    ));
}

// ---------------------------------------------------------------------------
// 27. UNION ALL
// ---------------------------------------------------------------------------

#[test]
fn test_union_all() {
    let sel = first_select("SELECT a FROM t UNION ALL SELECT b FROM u");
    let set_op = sel.set_op.unwrap();
    assert_eq!(set_op.op, SetOperator::Union);
    assert!(set_op.all);
}

// ---------------------------------------------------------------------------
// 28. INTERSECT
// ---------------------------------------------------------------------------

#[test]
fn test_intersect() {
    let sel = first_select("SELECT a FROM t INTERSECT SELECT b FROM u");
    let set_op = sel.set_op.unwrap();
    assert_eq!(set_op.op, SetOperator::Intersect);
    assert!(!set_op.all);
}

// ---------------------------------------------------------------------------
// 29. EXCEPT
// ---------------------------------------------------------------------------

#[test]
fn test_except() {
    let sel = first_select("SELECT a FROM t EXCEPT SELECT b FROM u");
    let set_op = sel.set_op.unwrap();
    assert_eq!(set_op.op, SetOperator::Except);
    assert!(!set_op.all);
}

// ---------------------------------------------------------------------------
// 30. Multiple statements: SELECT 1; SELECT 2;
// ---------------------------------------------------------------------------

#[test]
fn test_multiple_statements() {
    let r = parse("SELECT 1; SELECT 2;");
    assert!(r.errors.is_empty(), "parse errors: {:?}", r.errors);
    assert_eq!(r.statements.len(), 2);

    match &r.statements[0] {
        Statement::Select(s) => {
            assert_eq!(s.columns.len(), 1);
            assert!(matches!(
                &s.columns[0],
                SelectColumn::Expr { expr: Expr::Integer(1), alias: None }
            ));
        }
        other => panic!("expected first SELECT, got {:?}", other),
    }

    match &r.statements[1] {
        Statement::Select(s) => {
            assert_eq!(s.columns.len(), 1);
            assert!(matches!(
                &s.columns[0],
                SelectColumn::Expr { expr: Expr::Integer(2), alias: None }
            ));
        }
        other => panic!("expected second SELECT, got {:?}", other),
    }
}
