use sqld::planner::explain::*;
use sqld::planner::logical_plan::*;
use sqld::planner::physical_plan::*;
use sqld::planner::Catalog;
use sqld::sql::ast::*;
use sqld::types::{Column, DataType, Schema};

fn make_catalog() -> Catalog {
    let mut catalog = Catalog::new();
    catalog.add_table(
        "users",
        Schema::new(vec![
            Column::new("id", DataType::Integer, false),
            Column::new("name", DataType::Varchar(255), false),
            Column::new("age", DataType::Integer, true),
        ]),
    );
    catalog
}

fn users_schema() -> Schema {
    make_catalog().get_schema("users").unwrap().clone()
}

// ---------------------------------------------------------------------------
// 1. explain_logical of a Scan → output contains "Scan: users"
// ---------------------------------------------------------------------------

#[test]
fn test_explain_logical_scan() {
    let plan = LogicalPlan::Scan {
        table: "users".to_string(),
        alias: None,
        schema: users_schema(),
    };
    let output = explain_logical(&plan);
    assert!(
        output.contains("Scan: users"),
        "output should contain 'Scan: users':\n{}",
        output
    );
}

// ---------------------------------------------------------------------------
// 2. Filter over Scan → output contains "Filter:" and the expression
// ---------------------------------------------------------------------------

#[test]
fn test_explain_logical_filter() {
    let predicate = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("age".to_string())),
        op: BinaryOp::Gt,
        right: Box::new(Expr::Integer(18)),
    };
    let plan = LogicalPlan::Filter {
        predicate,
        input: Box::new(LogicalPlan::Scan {
            table: "users".to_string(),
            alias: None,
            schema: users_schema(),
        }),
    };
    let output = explain_logical(&plan);
    assert!(
        output.contains("Filter:"),
        "output should contain 'Filter:':\n{}",
        output
    );
    assert!(
        output.contains("age"),
        "output should contain the filter column 'age':\n{}",
        output
    );
    assert!(
        output.contains("18"),
        "output should contain the filter value '18':\n{}",
        output
    );
}

// ---------------------------------------------------------------------------
// 3. Project over Scan → output contains "Project:" and column names
// ---------------------------------------------------------------------------

#[test]
fn test_explain_logical_project() {
    let plan = LogicalPlan::Project {
        expressions: vec![
            ProjectionExpr {
                expr: Expr::Identifier("id".to_string()),
                alias: "id".to_string(),
            },
            ProjectionExpr {
                expr: Expr::Identifier("name".to_string()),
                alias: "name".to_string(),
            },
        ],
        input: Box::new(LogicalPlan::Scan {
            table: "users".to_string(),
            alias: None,
            schema: users_schema(),
        }),
    };
    let output = explain_logical(&plan);
    assert!(
        output.contains("Project:"),
        "output should contain 'Project:':\n{}",
        output
    );
    assert!(
        output.contains("id"),
        "output should contain column name 'id':\n{}",
        output
    );
    assert!(
        output.contains("name"),
        "output should contain column name 'name':\n{}",
        output
    );
}

// ---------------------------------------------------------------------------
// 4. Join → output contains "Join" and "ON" with condition
// ---------------------------------------------------------------------------

#[test]
fn test_explain_logical_join() {
    let condition = Expr::BinaryOp {
        left: Box::new(Expr::QualifiedIdentifier {
            table: "users".to_string(),
            column: "id".to_string(),
        }),
        op: BinaryOp::Eq,
        right: Box::new(Expr::QualifiedIdentifier {
            table: "orders".to_string(),
            column: "user_id".to_string(),
        }),
    };
    let left_schema = users_schema();
    let right_schema = Schema::new(vec![
        Column::new("id", DataType::Integer, false),
        Column::new("user_id", DataType::Integer, false),
        Column::new("amount", DataType::Float, false),
    ]);
    let joined_schema = left_schema.merge(&right_schema);
    let plan = LogicalPlan::Join {
        join_type: JoinType::Inner,
        condition: Some(condition),
        left: Box::new(LogicalPlan::Scan {
            table: "users".to_string(),
            alias: None,
            schema: users_schema(),
        }),
        right: Box::new(LogicalPlan::Scan {
            table: "orders".to_string(),
            alias: None,
            schema: right_schema,
        }),
        schema: joined_schema,
    };
    let output = explain_logical(&plan);
    assert!(
        output.contains("Join"),
        "output should contain 'Join':\n{}",
        output
    );
    assert!(
        output.contains("ON"),
        "output should contain 'ON':\n{}",
        output
    );
    assert!(
        output.contains("users.id"),
        "output should contain join condition column 'users.id':\n{}",
        output
    );
}

// ---------------------------------------------------------------------------
// 5. Aggregate → output contains "Aggregate:" with group_by and aggs
// ---------------------------------------------------------------------------

#[test]
fn test_explain_logical_aggregate() {
    let group_col = Expr::Identifier("age".to_string());
    let agg_expr = AggregateExpr {
        func: AggregateFunc::Count,
        arg: Expr::Star,
        distinct: false,
        alias: "count".to_string(),
    };
    let agg_schema = Schema::new(vec![
        Column::new("age", DataType::Integer, true),
        Column::new("count", DataType::BigInt, false),
    ]);
    let plan = LogicalPlan::Aggregate {
        group_by: vec![group_col],
        aggregates: vec![agg_expr],
        input: Box::new(LogicalPlan::Scan {
            table: "users".to_string(),
            alias: None,
            schema: users_schema(),
        }),
        schema: agg_schema,
    };
    let output = explain_logical(&plan);
    assert!(
        output.contains("Aggregate:"),
        "output should contain 'Aggregate:':\n{}",
        output
    );
    assert!(
        output.contains("group_by="),
        "output should contain 'group_by=':\n{}",
        output
    );
    assert!(
        output.contains("aggs="),
        "output should contain 'aggs=':\n{}",
        output
    );
    assert!(
        output.contains("age"),
        "output should contain group column 'age':\n{}",
        output
    );
    assert!(
        output.contains("count"),
        "output should contain aggregate function 'count':\n{}",
        output
    );
}

// ---------------------------------------------------------------------------
// 6. Sort → output contains "Sort:" with ASC/DESC
// ---------------------------------------------------------------------------

#[test]
fn test_explain_logical_sort() {
    let plan = LogicalPlan::Sort {
        order_by: vec![
            SortExpr {
                expr: Expr::Identifier("name".to_string()),
                ascending: true,
                nulls_first: false,
            },
            SortExpr {
                expr: Expr::Identifier("age".to_string()),
                ascending: false,
                nulls_first: true,
            },
        ],
        input: Box::new(LogicalPlan::Scan {
            table: "users".to_string(),
            alias: None,
            schema: users_schema(),
        }),
    };
    let output = explain_logical(&plan);
    assert!(
        output.contains("Sort:"),
        "output should contain 'Sort:':\n{}",
        output
    );
    assert!(
        output.contains("ASC"),
        "output should contain 'ASC':\n{}",
        output
    );
    assert!(
        output.contains("DESC"),
        "output should contain 'DESC':\n{}",
        output
    );
}

// ---------------------------------------------------------------------------
// 7. Limit → output contains "Limit:" with count
// ---------------------------------------------------------------------------

#[test]
fn test_explain_logical_limit() {
    let plan = LogicalPlan::Limit {
        count: Some(25),
        offset: 0,
        input: Box::new(LogicalPlan::Scan {
            table: "users".to_string(),
            alias: None,
            schema: users_schema(),
        }),
    };
    let output = explain_logical(&plan);
    assert!(
        output.contains("Limit:"),
        "output should contain 'Limit:':\n{}",
        output
    );
    assert!(
        output.contains("25"),
        "output should contain count '25':\n{}",
        output
    );
}

// ---------------------------------------------------------------------------
// 8. Project over Filter over Join over (Scan, Scan) → indented tree output
// ---------------------------------------------------------------------------

#[test]
fn test_explain_logical_nested() {
    let right_schema = Schema::new(vec![
        Column::new("id", DataType::Integer, false),
        Column::new("user_id", DataType::Integer, false),
    ]);
    let joined_schema = users_schema().merge(&right_schema);

    let join = LogicalPlan::Join {
        join_type: JoinType::Inner,
        condition: Some(Expr::BinaryOp {
            left: Box::new(Expr::QualifiedIdentifier {
                table: "users".to_string(),
                column: "id".to_string(),
            }),
            op: BinaryOp::Eq,
            right: Box::new(Expr::QualifiedIdentifier {
                table: "orders".to_string(),
                column: "user_id".to_string(),
            }),
        }),
        left: Box::new(LogicalPlan::Scan {
            table: "users".to_string(),
            alias: None,
            schema: users_schema(),
        }),
        right: Box::new(LogicalPlan::Scan {
            table: "orders".to_string(),
            alias: None,
            schema: right_schema,
        }),
        schema: joined_schema,
    };

    let filter = LogicalPlan::Filter {
        predicate: Expr::BinaryOp {
            left: Box::new(Expr::Identifier("age".to_string())),
            op: BinaryOp::GtEq,
            right: Box::new(Expr::Integer(21)),
        },
        input: Box::new(join),
    };

    let plan = LogicalPlan::Project {
        expressions: vec![ProjectionExpr {
            expr: Expr::Identifier("name".to_string()),
            alias: "name".to_string(),
        }],
        input: Box::new(filter),
    };

    let output = explain_logical(&plan);

    assert!(
        output.contains("Project:"),
        "output should contain 'Project:':\n{}",
        output
    );
    assert!(
        output.contains("Filter:"),
        "output should contain 'Filter:':\n{}",
        output
    );
    assert!(
        output.contains("Join"),
        "output should contain 'Join':\n{}",
        output
    );
    assert!(
        output.contains("Scan: users"),
        "output should contain 'Scan: users':\n{}",
        output
    );

    // Verify indentation exists (children are indented relative to parents)
    let lines: Vec<&str> = output.lines().collect();
    assert!(
        lines.len() >= 5,
        "nested plan should produce at least 5 lines:\n{}",
        output
    );
    let has_deep_indent = lines.iter().any(|l| l.starts_with("    "));
    assert!(
        has_deep_indent,
        "nested plan should have deeply indented lines:\n{}",
        output
    );
}

// ---------------------------------------------------------------------------
// 9. explain_physical of SeqScan → contains "SeqScan:"
// ---------------------------------------------------------------------------

#[test]
fn test_explain_physical_seq_scan() {
    let plan = PhysicalPlan::SeqScan {
        table: "users".to_string(),
        alias: None,
        schema: users_schema(),
        predicate: None,
    };
    let output = explain_physical(&plan);
    assert!(
        output.contains("SeqScan:"),
        "output should contain 'SeqScan:':\n{}",
        output
    );
    assert!(
        output.contains("users"),
        "output should contain table name 'users':\n{}",
        output
    );
}

// ---------------------------------------------------------------------------
// 10. IndexScan → contains "IndexScan:" and "USING"
// ---------------------------------------------------------------------------

#[test]
fn test_explain_physical_index_scan() {
    let plan = PhysicalPlan::IndexScan {
        table: "users".to_string(),
        alias: None,
        index_name: "users_id_idx".to_string(),
        schema: users_schema(),
        key_ranges: vec![KeyRange::eq(Expr::Integer(42))],
        predicate: None,
    };
    let output = explain_physical(&plan);
    assert!(
        output.contains("IndexScan:"),
        "output should contain 'IndexScan:':\n{}",
        output
    );
    assert!(
        output.contains("USING"),
        "output should contain 'USING':\n{}",
        output
    );
    assert!(
        output.contains("users_id_idx"),
        "output should contain index name 'users_id_idx':\n{}",
        output
    );
}

// ---------------------------------------------------------------------------
// 11. HashJoin → contains "HashJoin:"
// ---------------------------------------------------------------------------

#[test]
fn test_explain_physical_hash_join() {
    let right_schema = Schema::new(vec![
        Column::new("id", DataType::Integer, false),
        Column::new("user_id", DataType::Integer, false),
    ]);
    let joined_schema = users_schema().merge(&right_schema);

    let plan = PhysicalPlan::HashJoin {
        join_type: JoinType::Inner,
        left_keys: vec![Expr::QualifiedIdentifier {
            table: "users".to_string(),
            column: "id".to_string(),
        }],
        right_keys: vec![Expr::QualifiedIdentifier {
            table: "orders".to_string(),
            column: "user_id".to_string(),
        }],
        condition: None,
        left: Box::new(PhysicalPlan::SeqScan {
            table: "users".to_string(),
            alias: None,
            schema: users_schema(),
            predicate: None,
        }),
        right: Box::new(PhysicalPlan::SeqScan {
            table: "orders".to_string(),
            alias: None,
            schema: right_schema,
            predicate: None,
        }),
        schema: joined_schema,
    };
    let output = explain_physical(&plan);
    assert!(
        output.contains("HashJoin:"),
        "output should contain 'HashJoin:':\n{}",
        output
    );
}

// ---------------------------------------------------------------------------
// 12. NestedLoopJoin → contains "NestedLoopJoin:"
// ---------------------------------------------------------------------------

#[test]
fn test_explain_physical_nested_loop() {
    let right_schema = Schema::new(vec![
        Column::new("id", DataType::Integer, false),
        Column::new("user_id", DataType::Integer, false),
    ]);
    let joined_schema = users_schema().merge(&right_schema);

    let condition = Expr::BinaryOp {
        left: Box::new(Expr::QualifiedIdentifier {
            table: "users".to_string(),
            column: "id".to_string(),
        }),
        op: BinaryOp::Eq,
        right: Box::new(Expr::QualifiedIdentifier {
            table: "orders".to_string(),
            column: "user_id".to_string(),
        }),
    };
    let plan = PhysicalPlan::NestedLoopJoin {
        join_type: JoinType::Left,
        condition: Some(condition),
        left: Box::new(PhysicalPlan::SeqScan {
            table: "users".to_string(),
            alias: None,
            schema: users_schema(),
            predicate: None,
        }),
        right: Box::new(PhysicalPlan::SeqScan {
            table: "orders".to_string(),
            alias: None,
            schema: right_schema,
            predicate: None,
        }),
        schema: joined_schema,
    };
    let output = explain_physical(&plan);
    assert!(
        output.contains("NestedLoopJoin:"),
        "output should contain 'NestedLoopJoin:':\n{}",
        output
    );
}

// ---------------------------------------------------------------------------
// 13. explain_analyze with costs → contains "cost=" and "rows="
// ---------------------------------------------------------------------------

#[test]
fn test_explain_analyze_format() {
    let right_schema = Schema::new(vec![
        Column::new("id", DataType::Integer, false),
        Column::new("user_id", DataType::Integer, false),
    ]);
    let joined_schema = users_schema().merge(&right_schema);

    let plan = PhysicalPlan::HashJoin {
        join_type: JoinType::Inner,
        left_keys: vec![Expr::Identifier("id".to_string())],
        right_keys: vec![Expr::Identifier("user_id".to_string())],
        condition: None,
        left: Box::new(PhysicalPlan::SeqScan {
            table: "users".to_string(),
            alias: None,
            schema: users_schema(),
            predicate: None,
        }),
        right: Box::new(PhysicalPlan::SeqScan {
            table: "orders".to_string(),
            alias: None,
            schema: right_schema,
            predicate: None,
        }),
        schema: joined_schema,
    };

    // costs: (estimated_rows, estimated_cost) per node (hash join + 2 seq scans)
    let costs = vec![(500.0, 120.50), (1000.0, 10.0), (2000.0, 20.0)];
    let output = explain_analyze(&plan, &costs);

    assert!(
        output.contains("cost="),
        "output should contain 'cost=':\n{}",
        output
    );
    assert!(
        output.contains("rows="),
        "output should contain 'rows=':\n{}",
        output
    );
}

// ---------------------------------------------------------------------------
// 14. format_expr for various expressions
// ---------------------------------------------------------------------------

#[test]
fn test_explain_format_expr() {
    // Identifier
    let expr = Expr::Identifier("age".to_string());
    assert_eq!(format_expr(&expr), "age");

    // QualifiedIdentifier
    let expr = Expr::QualifiedIdentifier {
        table: "users".to_string(),
        column: "id".to_string(),
    };
    assert_eq!(format_expr(&expr), "users.id");

    // BinaryOp
    let expr = Expr::BinaryOp {
        left: Box::new(Expr::Identifier("age".to_string())),
        op: BinaryOp::Gt,
        right: Box::new(Expr::Integer(18)),
    };
    let result = format_expr(&expr);
    assert!(result.contains("age"), "binary op should include left operand");
    assert!(result.contains(">"), "binary op should include operator");
    assert!(result.contains("18"), "binary op should include right operand");

    // IsNull
    let expr = Expr::IsNull {
        expr: Box::new(Expr::Identifier("name".to_string())),
        negated: false,
    };
    let result = format_expr(&expr);
    assert!(result.contains("name"), "IS NULL should include column name");
    assert!(result.contains("IS NULL"), "IS NULL should include 'IS NULL'");

    // IsNull negated (IS NOT NULL)
    let expr = Expr::IsNull {
        expr: Box::new(Expr::Identifier("name".to_string())),
        negated: true,
    };
    let result = format_expr(&expr);
    assert!(
        result.contains("IS NOT NULL"),
        "negated IS NULL should produce 'IS NOT NULL': {}",
        result
    );

    // Integer literal
    let expr = Expr::Integer(42);
    assert_eq!(format_expr(&expr), "42");

    // String literal
    let expr = Expr::String("hello".to_string());
    assert_eq!(format_expr(&expr), "'hello'");

    // Boolean literal
    let expr = Expr::Boolean(true);
    assert_eq!(format_expr(&expr), "true");

    // Null literal
    let expr = Expr::Null;
    assert_eq!(format_expr(&expr), "NULL");

    // Star
    let expr = Expr::Star;
    assert_eq!(format_expr(&expr), "*");

    // FunctionCall
    let expr = Expr::FunctionCall {
        name: "count".to_string(),
        args: vec![Expr::Star],
        distinct: false,
    };
    let result = format_expr(&expr);
    assert!(result.contains("count"), "function call should include function name");
    assert!(result.contains("*"), "function call should include args");
}

// ---------------------------------------------------------------------------
// 15. Empty node → contains "Empty"
// ---------------------------------------------------------------------------

#[test]
fn test_explain_empty() {
    let logical_plan = LogicalPlan::Empty {
        schema: Schema::empty(),
    };
    let output = explain_logical(&logical_plan);
    assert!(
        output.contains("Empty"),
        "logical Empty output should contain 'Empty':\n{}",
        output
    );

    let physical_plan = PhysicalPlan::Empty {
        schema: Schema::empty(),
    };
    let output = explain_physical(&physical_plan);
    assert!(
        output.contains("Empty"),
        "physical Empty output should contain 'Empty':\n{}",
        output
    );
}
