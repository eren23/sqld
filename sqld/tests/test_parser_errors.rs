use sqld::sql::ast::*;
use sqld::sql::parser::{parse, ParseResult};

// ===========================================================================
// Helpers
// ===========================================================================

/// Parse SQL and return the result without any assertions.
fn parse_sql(sql: &str) -> ParseResult {
    parse(sql)
}


// ===========================================================================
// 1. Error recovery: missing semicolon between statements
// ===========================================================================

#[test]
fn error_recovery_missing_semicolon_between_selects() {
    // "SELECT 1 SELECT 2" — the parser should parse the first SELECT 1
    // successfully. When it encounters the second SELECT where it expects
    // a semicolon or EOF, it will either produce an error or treat SELECT
    // as a statement boundary. Either way, we should get at least one
    // valid statement and recovery should happen.
    let r = parse_sql("SELECT 1 SELECT 2");
    // The parser should produce at least one valid statement.
    assert!(
        r.statements.len() >= 1,
        "expected at least 1 statement, got {}",
        r.statements.len()
    );
    // The exact behavior depends on whether the parser treats SELECT as an
    // infix token or errors. Verify something was parsed.
    // If both parse correctly, great; if one errors, at least one should succeed.
    assert!(
        r.statements.len() + r.errors.len() >= 2,
        "expected the parser to attempt both statements"
    );
}

// ===========================================================================
// 2. Error recovery: garbage followed by valid statement
// ===========================================================================

#[test]
fn error_recovery_garbage_then_valid() {
    let r = parse_sql("GARBAGE; SELECT 1");
    // "GARBAGE" is an identifier, not a valid statement start — errors.
    // After synchronize (skips to ;), "SELECT 1" should parse OK.
    assert!(r.has_errors());
    assert!(r.errors.len() >= 1, "garbage should produce at least 1 error");
    assert!(
        r.statements.len() >= 1,
        "SELECT 1 after semicolon should parse successfully"
    );
    // Verify the successful statement is SELECT 1
    match &r.statements[0] {
        Statement::Select(sel) => {
            assert_eq!(sel.columns.len(), 1);
            assert!(matches!(
                &sel.columns[0],
                SelectColumn::Expr {
                    expr: Expr::Integer(1),
                    alias: None
                }
            ));
        }
        other => panic!("expected SELECT, got {:?}", other),
    }
}

// ===========================================================================
// 3. Error recovery: multiple errors followed by valid statement
// ===========================================================================

#[test]
fn error_recovery_multiple_errors_then_valid() {
    let r = parse_sql("GARBAGE1; GARBAGE2; SELECT 1");
    assert!(r.has_errors());
    assert!(
        r.errors.len() >= 2,
        "expected at least 2 errors for GARBAGE1 and GARBAGE2, got {}",
        r.errors.len()
    );
    assert!(
        r.statements.len() >= 1,
        "SELECT 1 should parse successfully after errors"
    );
}

// ===========================================================================
// 4. Error recovery: valid + invalid + valid
// ===========================================================================

#[test]
fn error_recovery_valid_invalid_valid() {
    let r = parse_sql("SELECT 1; BLAH; SELECT 2");
    assert!(r.has_errors());
    assert_eq!(
        r.errors.len(), 1,
        "only BLAH should error, got {:?}",
        r.errors
    );
    assert_eq!(
        r.statements.len(), 2,
        "both SELECTs should parse, got {:?}",
        r.statements
    );

    // Verify first statement is SELECT 1
    match &r.statements[0] {
        Statement::Select(sel) => {
            assert!(matches!(
                &sel.columns[0],
                SelectColumn::Expr {
                    expr: Expr::Integer(1),
                    ..
                }
            ));
        }
        other => panic!("expected SELECT 1, got {:?}", other),
    }

    // Verify second statement is SELECT 2
    match &r.statements[1] {
        Statement::Select(sel) => {
            assert!(matches!(
                &sel.columns[0],
                SelectColumn::Expr {
                    expr: Expr::Integer(2),
                    ..
                }
            ));
        }
        other => panic!("expected SELECT 2, got {:?}", other),
    }
}

// ===========================================================================
// 5. Completely invalid input
// ===========================================================================

#[test]
fn completely_invalid_input() {
    // "!!!" produces Error tokens from the lexer (lone '!' is not a valid
    // operator). The parser should produce error(s) and no valid statements.
    let r = parse_sql("!!!");
    assert!(r.has_errors());
    assert!(
        r.statements.is_empty(),
        "completely invalid input should produce no statements, got {:?}",
        r.statements
    );
}

// ===========================================================================
// 6. Error position tracking: line 1
// ===========================================================================

#[test]
fn error_position_line_1() {
    let r = parse_sql("GARBAGE");
    assert!(r.has_errors());
    let err = &r.errors[0];
    assert_eq!(err.line, 1, "error should be on line 1");
    assert!(
        err.col >= 1,
        "col should be at least 1 (1-based), got {}",
        err.col
    );
}

// ===========================================================================
// 7. Error position tracking: multiline input
// ===========================================================================

#[test]
fn error_position_multiline() {
    // Put a valid statement on line 1, garbage on line 2.
    let r = parse_sql("SELECT 1;\nGARBAGE");
    assert!(r.has_errors());
    let err = &r.errors[0];
    assert_eq!(
        err.line, 2,
        "error should be on line 2, got line {}",
        err.line
    );
}

// ===========================================================================
// 8. Expected tokens: invalid statement start lists expected keywords
// ===========================================================================

#[test]
fn expected_tokens_for_invalid_start() {
    let r = parse_sql("GARBAGE");
    assert!(r.has_errors());
    let err = &r.errors[0];
    // The parser's default error path provides expected statement keywords.
    // GARBAGE is tokenized as an Identifier, which hits the _ arm in
    // parse_statement and lists SELECT, INSERT, UPDATE, DELETE, CREATE, DROP.
    assert!(
        !err.expected.is_empty(),
        "expected tokens list should not be empty"
    );
    assert!(
        err.expected.contains(&"SELECT".to_string()),
        "expected tokens should contain SELECT, got {:?}",
        err.expected
    );
    assert!(
        err.expected.contains(&"INSERT".to_string()),
        "expected tokens should contain INSERT, got {:?}",
        err.expected
    );
}

// ===========================================================================
// 9. DELETE missing FROM: `DELETE WHERE ...`
// ===========================================================================

#[test]
fn delete_missing_from() {
    // DELETE expects FROM after it. "DELETE WHERE" will error because
    // WHERE is not KwFrom.
    let r = parse_sql("DELETE WHERE id = 1");
    assert!(r.has_errors());
    let err = &r.errors[0];
    // The error message from expect(KwFrom) says "expected KwFrom, found ..."
    assert!(
        err.message.contains("KwFrom") || err.message.contains("FROM"),
        "error should mention FROM/KwFrom, got: {}",
        err.message
    );
}

// ===========================================================================
// 10. INSERT missing INTO: `INSERT t VALUES (1)`
// ===========================================================================

#[test]
fn insert_missing_into() {
    // INSERT expects INTO. "INSERT t" will fail at expect(KwInto).
    let r = parse_sql("INSERT t VALUES (1)");
    assert!(r.has_errors());
    let err = &r.errors[0];
    assert!(
        err.message.contains("KwInto") || err.message.contains("INTO"),
        "error should mention INTO/KwInto, got: {}",
        err.message
    );
}

// ===========================================================================
// 11. CREATE missing object type: `CREATE t (id INT)`
// ===========================================================================

#[test]
fn create_missing_object_type() {
    // CREATE expects TABLE, INDEX, VIEW, or UNIQUE. "CREATE t" (where t is
    // an identifier) should error with expected list.
    let r = parse_sql("CREATE t (id INT)");
    assert!(r.has_errors());
    let err = &r.errors[0];
    assert!(
        err.message.contains("TABLE") || err.message.contains("INDEX") || err.message.contains("VIEW"),
        "error should mention TABLE/INDEX/VIEW, got: {}",
        err.message
    );
    // Verify expected list
    assert!(
        err.expected.contains(&"TABLE".to_string()),
        "expected list should contain TABLE, got {:?}",
        err.expected
    );
    assert!(
        err.expected.contains(&"INDEX".to_string()),
        "expected list should contain INDEX, got {:?}",
        err.expected
    );
    assert!(
        err.expected.contains(&"VIEW".to_string()),
        "expected list should contain VIEW, got {:?}",
        err.expected
    );
    assert!(
        err.expected.contains(&"UNIQUE".to_string()),
        "expected list should contain UNIQUE, got {:?}",
        err.expected
    );
}

// ===========================================================================
// 12. Common mistake detection: FROM without SELECT
// ===========================================================================

#[test]
fn common_mistake_from_without_select() {
    let r = parse_sql("FROM users");
    assert!(r.has_errors());
    let err = &r.errors[0];
    assert!(
        err.message.contains("unexpected FROM without SELECT"),
        "error should detect FROM without SELECT, got: {}",
        err.message
    );
    assert!(
        err.message.contains("did you mean SELECT ... FROM?"),
        "error should suggest SELECT ... FROM, got: {}",
        err.message
    );
}

// ===========================================================================
// 13. Common mistake detection: SET without UPDATE
// ===========================================================================

#[test]
fn common_mistake_set_without_update() {
    let r = parse_sql("SET a = 1");
    assert!(r.has_errors());
    let err = &r.errors[0];
    assert!(
        err.message.contains("unexpected SET without UPDATE"),
        "error should detect SET without UPDATE, got: {}",
        err.message
    );
    assert!(
        err.message.contains("did you mean UPDATE ... SET?"),
        "error should suggest UPDATE ... SET, got: {}",
        err.message
    );
}

// ===========================================================================
// 14. Common mistake detection: VALUES without INSERT
// ===========================================================================

#[test]
fn common_mistake_values_without_insert() {
    let r = parse_sql("VALUES (1, 2)");
    assert!(r.has_errors());
    let err = &r.errors[0];
    assert!(
        err.message.contains("unexpected VALUES without INSERT"),
        "error should detect VALUES without INSERT, got: {}",
        err.message
    );
    assert!(
        err.message.contains("did you mean INSERT INTO ... VALUES?"),
        "error should suggest INSERT INTO ... VALUES, got: {}",
        err.message
    );
}

// ===========================================================================
// 15. Partial parse success: valid + error + valid
// ===========================================================================

#[test]
fn partial_parse_success_with_from_error() {
    let r = parse_sql("SELECT 1; FROM users; SELECT 2");
    assert!(r.has_errors());
    assert_eq!(
        r.statements.len(), 2,
        "should parse 2 valid statements around the FROM error"
    );
    assert_eq!(
        r.errors.len(), 1,
        "should have 1 error for FROM"
    );

    // Verify the first valid statement is SELECT 1
    match &r.statements[0] {
        Statement::Select(sel) => {
            assert!(matches!(
                &sel.columns[0],
                SelectColumn::Expr {
                    expr: Expr::Integer(1),
                    ..
                }
            ));
        }
        other => panic!("expected SELECT 1, got {:?}", other),
    }

    // Verify the second valid statement is SELECT 2
    match &r.statements[1] {
        Statement::Select(sel) => {
            assert!(matches!(
                &sel.columns[0],
                SelectColumn::Expr {
                    expr: Expr::Integer(2),
                    ..
                }
            ));
        }
        other => panic!("expected SELECT 2, got {:?}", other),
    }

    // Verify the error mentions FROM without SELECT
    assert!(
        r.errors[0].message.contains("FROM without SELECT"),
        "error should mention FROM without SELECT, got: {}",
        r.errors[0].message
    );
}

// ===========================================================================
// 16. Expression error: `SELECT 1 +` (trailing operator)
// ===========================================================================

#[test]
fn expression_error_trailing_operator() {
    let r = parse_sql("SELECT 1 +");
    assert!(r.has_errors());
    assert!(
        !r.errors.is_empty(),
        "trailing + should produce a parse error"
    );
}

// ===========================================================================
// 17. Expression error: `SELECT (1 + 2` (missing closing paren)
// ===========================================================================

#[test]
fn expression_error_missing_closing_paren() {
    let r = parse_sql("SELECT (1 + 2");
    assert!(r.has_errors());
    // The error should mention the missing right paren
    let has_paren_error = r.errors.iter().any(|e| {
        e.message.contains("RightParen")
            || e.message.contains(")")
            || e.message.contains("paren")
    });
    assert!(
        has_paren_error,
        "should error about missing closing paren, got: {:?}",
        r.errors
    );
}

// ===========================================================================
// 18. DDL error: `CREATE TABLE` with no name or body
// ===========================================================================

#[test]
fn ddl_error_create_table_missing_name() {
    // "CREATE TABLE" followed by EOF — should error because table name is missing.
    let r = parse_sql("CREATE TABLE");
    assert!(r.has_errors());
    assert!(
        !r.errors.is_empty(),
        "CREATE TABLE without name should error"
    );
}

// ===========================================================================
// 19. DDL error: `DROP TABLE` with no name
// ===========================================================================

#[test]
fn ddl_error_drop_table_missing_name() {
    let r = parse_sql("DROP TABLE");
    assert!(r.has_errors());
    assert!(
        !r.errors.is_empty(),
        "DROP TABLE without name should error"
    );
}

// ===========================================================================
// 20. DDL error: `ALTER TABLE t` with no action
// ===========================================================================

#[test]
fn ddl_error_alter_table_missing_action() {
    let r = parse_sql("ALTER TABLE t");
    assert!(r.has_errors());
    let err = &r.errors[0];
    // Should expect ADD, DROP, or RENAME
    assert!(
        err.message.contains("ADD") || err.message.contains("DROP") || err.message.contains("RENAME"),
        "error should mention ADD/DROP/RENAME, got: {}",
        err.message
    );
    assert!(
        err.expected.contains(&"ADD".to_string()),
        "expected list should contain ADD, got {:?}",
        err.expected
    );
    assert!(
        err.expected.contains(&"DROP".to_string()),
        "expected list should contain DROP, got {:?}",
        err.expected
    );
    assert!(
        err.expected.contains(&"RENAME".to_string()),
        "expected list should contain RENAME, got {:?}",
        err.expected
    );
}

// ===========================================================================
// 21. Complex error recovery: multiple errors interspersed with valid statements
// ===========================================================================

#[test]
fn complex_error_recovery_mixed() {
    let r = parse_sql("SELECT 1; CREATE; SELECT 2; INSERT; SELECT 3");
    // "CREATE" alone should error (missing TABLE/INDEX/VIEW/UNIQUE).
    // "INSERT" alone should error (missing INTO).
    // Each SELECT should parse OK.
    assert_eq!(
        r.statements.len(), 3,
        "should parse 3 valid SELECT statements, got {:?}",
        r.statements
    );
    assert_eq!(
        r.errors.len(), 2,
        "should have 2 errors (CREATE and INSERT), got {:?}",
        r.errors
    );

    // Verify all three statements are SELECTs with correct values
    for (i, expected_val) in [(0, 1i64), (1, 2), (2, 3)] {
        match &r.statements[i] {
            Statement::Select(sel) => {
                assert!(matches!(
                    &sel.columns[0],
                    SelectColumn::Expr {
                        expr: Expr::Integer(v),
                        ..
                    } if *v == expected_val
                ), "statement {} should be SELECT {}, got {:?}", i, expected_val, sel);
            }
            other => panic!("statement {} should be SELECT, got {:?}", i, other),
        }
    }
}

// ===========================================================================
// 22. Error message quality: human-readable messages
// ===========================================================================

#[test]
fn error_messages_are_human_readable() {
    let r = parse_sql("FROM users");
    assert!(r.has_errors());
    let err = &r.errors[0];
    // The message should be descriptive, not just a token kind dump
    assert!(
        err.message.len() > 10,
        "error message should be descriptive, got: {}",
        err.message
    );
    // Should contain guidance words
    assert!(
        err.message.contains("did you mean"),
        "error message should provide a suggestion, got: {}",
        err.message
    );
}

#[test]
fn error_messages_contain_descriptive_text() {
    // Generic unknown statement error
    let r = parse_sql("GARBAGE");
    assert!(r.has_errors());
    let err = &r.errors[0];
    assert!(
        err.message.contains("expected") || err.message.contains("unexpected"),
        "error message should contain 'expected' or 'unexpected', got: {}",
        err.message
    );
}

// ===========================================================================
// 23. has_errors() correctness
// ===========================================================================

#[test]
fn has_errors_returns_false_for_valid_input() {
    let r = parse_sql("SELECT 1");
    assert!(
        !r.has_errors(),
        "valid input should not have errors"
    );
    assert!(r.errors.is_empty());
}

#[test]
fn has_errors_returns_true_for_invalid_input() {
    let r = parse_sql("GARBAGE");
    assert!(
        r.has_errors(),
        "invalid input should have errors"
    );
    assert!(!r.errors.is_empty());
}

// ===========================================================================
// 24. Empty input
// ===========================================================================

#[test]
fn empty_input_no_statements_no_errors() {
    let r = parse_sql("");
    assert!(
        r.statements.is_empty(),
        "empty input should produce no statements"
    );
    assert!(
        r.errors.is_empty(),
        "empty input should produce no errors"
    );
    assert!(!r.has_errors());
}

// ===========================================================================
// 25. Whitespace-only input
// ===========================================================================

#[test]
fn whitespace_only_no_statements_no_errors() {
    let r = parse_sql("   ");
    assert!(
        r.statements.is_empty(),
        "whitespace-only input should produce no statements"
    );
    assert!(
        r.errors.is_empty(),
        "whitespace-only input should produce no errors"
    );
    assert!(!r.has_errors());
}

// ===========================================================================
// 26. Semicolons-only input
// ===========================================================================

#[test]
fn semicolons_only_no_statements_no_errors() {
    let r = parse_sql(";;;");
    assert!(
        r.statements.is_empty(),
        "semicolons-only input should produce no statements"
    );
    assert!(
        r.errors.is_empty(),
        "semicolons-only input should produce no errors"
    );
    assert!(!r.has_errors());
}

// ===========================================================================
// 27. Error offset is within source bounds
// ===========================================================================

#[test]
fn error_offset_within_bounds() {
    let input = "SELECT 1; GARBAGE; SELECT 2";
    let r = parse_sql(input);
    assert!(r.has_errors());
    for err in &r.errors {
        assert!(
            err.offset <= input.len(),
            "error offset {} should be <= source length {}",
            err.offset,
            input.len()
        );
    }
}

// ===========================================================================
// 28. Error position col tracking
// ===========================================================================

#[test]
fn error_col_matches_position_in_line() {
    // "GARBAGE" starts at column 1
    let r = parse_sql("GARBAGE");
    assert!(r.has_errors());
    let err = &r.errors[0];
    assert_eq!(err.line, 1);
    assert_eq!(err.col, 1, "GARBAGE starts at column 1");
}

#[test]
fn error_col_after_semicolon() {
    // After "SELECT 1; " (10 chars), GARBAGE starts at col 11
    let r = parse_sql("SELECT 1; GARBAGE");
    assert!(r.has_errors());
    let err = &r.errors[0];
    assert_eq!(err.line, 1);
    assert!(
        err.col > 1,
        "error col should be past the first statement, got col={}",
        err.col
    );
}

// ===========================================================================
// 29. Multiple semicolons between statements are harmless
// ===========================================================================

#[test]
fn multiple_semicolons_between_valid_statements() {
    let r = parse_sql("SELECT 1;;; SELECT 2");
    assert!(!r.has_errors(), "multiple semicolons should not cause errors");
    assert_eq!(
        r.statements.len(), 2,
        "should parse 2 statements separated by multiple semicolons"
    );
}

// ===========================================================================
// 30. Error recovery across different statement types
// ===========================================================================

#[test]
fn error_recovery_across_statement_types() {
    let r = parse_sql("GARBAGE; INSERT INTO t VALUES (1); GARBAGE2; DELETE FROM t");
    assert!(r.has_errors());
    assert_eq!(
        r.errors.len(), 2,
        "should have 2 errors for GARBAGE and GARBAGE2, got {:?}",
        r.errors
    );
    assert_eq!(
        r.statements.len(), 2,
        "should parse INSERT and DELETE, got {:?}",
        r.statements
    );
    assert!(matches!(&r.statements[0], Statement::Insert(_)));
    assert!(matches!(&r.statements[1], Statement::Delete(_)));
}

// ===========================================================================
// 31. Error recovery via statement-starting keyword (no semicolon)
// ===========================================================================

#[test]
fn error_recovery_via_keyword_no_semicolon() {
    // "GARBAGE SELECT 1" — GARBAGE errors, synchronize scans forward and
    // stops at SELECT (a statement-starting keyword).
    let r = parse_sql("GARBAGE SELECT 1");
    assert!(r.has_errors());
    // The parser should recover at SELECT and parse it.
    assert!(
        r.statements.len() >= 1,
        "should recover at SELECT keyword and parse it, got {} statements",
        r.statements.len()
    );
}

// ===========================================================================
// 32. Verify error Display impl
// ===========================================================================

#[test]
fn error_display_format() {
    let r = parse_sql("GARBAGE");
    assert!(r.has_errors());
    let err = &r.errors[0];
    let display = format!("{}", err);
    // Display format is "line:col: message"
    assert!(
        display.contains(':'),
        "Display should contain line:col: format, got: {}",
        display
    );
    assert!(
        display.starts_with(&format!("{}:{}", err.line, err.col)),
        "Display should start with line:col, got: {}",
        display
    );
}

// ===========================================================================
// 33. FROM hint expected list
// ===========================================================================

#[test]
fn from_hint_expected_contains_select() {
    let r = parse_sql("FROM users");
    assert!(r.has_errors());
    let err = &r.errors[0];
    assert!(
        err.expected.contains(&"SELECT".to_string()),
        "FROM hint should list SELECT as expected, got {:?}",
        err.expected
    );
}

// ===========================================================================
// 34. SET hint expected list
// ===========================================================================

#[test]
fn set_hint_expected_contains_update() {
    let r = parse_sql("SET a = 1");
    assert!(r.has_errors());
    let err = &r.errors[0];
    assert!(
        err.expected.contains(&"UPDATE".to_string()),
        "SET hint should list UPDATE as expected, got {:?}",
        err.expected
    );
}

// ===========================================================================
// 35. VALUES hint expected list
// ===========================================================================

#[test]
fn values_hint_expected_contains_insert() {
    let r = parse_sql("VALUES (1, 2)");
    assert!(r.has_errors());
    let err = &r.errors[0];
    assert!(
        err.expected.contains(&"INSERT".to_string()),
        "VALUES hint should list INSERT as expected, got {:?}",
        err.expected
    );
}

// ===========================================================================
// 36. DROP without object type
// ===========================================================================

#[test]
fn drop_without_object_type() {
    let r = parse_sql("DROP stuff");
    assert!(r.has_errors());
    let err = &r.errors[0];
    assert!(
        err.message.contains("TABLE") || err.message.contains("INDEX") || err.message.contains("VIEW"),
        "error should mention TABLE/INDEX/VIEW after DROP, got: {}",
        err.message
    );
}

// ===========================================================================
// 37. Multiline error on second line
// ===========================================================================

#[test]
fn multiline_error_second_line_col() {
    let r = parse_sql("SELECT 1;\n  GARBAGE");
    assert!(r.has_errors());
    let err = &r.errors[0];
    assert_eq!(err.line, 2, "error should be on line 2, got {}", err.line);
    // "  GARBAGE" — the identifier starts at col 3 (after 2 spaces)
    assert_eq!(err.col, 3, "error should be at col 3 (after 2 spaces), got {}", err.col);
}

// ===========================================================================
// 38. Multiple common mistake errors
// ===========================================================================

#[test]
fn multiple_common_mistakes() {
    let r = parse_sql("FROM t; SET a = 1; VALUES (1)");
    assert_eq!(r.errors.len(), 3, "should have 3 errors, got {:?}", r.errors);
    assert!(r.statements.is_empty(), "no valid statements should parse");

    assert!(r.errors[0].message.contains("FROM without SELECT"));
    assert!(r.errors[1].message.contains("SET without UPDATE"));
    assert!(r.errors[2].message.contains("VALUES without INSERT"));
}

// ===========================================================================
// 39. Transaction statements still parse around errors
// ===========================================================================

#[test]
fn transaction_statements_parse_around_errors() {
    let r = parse_sql("BEGIN; GARBAGE; COMMIT");
    assert!(r.has_errors());
    assert_eq!(r.errors.len(), 1);
    assert_eq!(r.statements.len(), 2);
    assert!(matches!(&r.statements[0], Statement::Begin));
    assert!(matches!(&r.statements[1], Statement::Commit));
}

// ===========================================================================
// 40. INSERT missing source (VALUES or SELECT)
// ===========================================================================

#[test]
fn insert_missing_source() {
    let r = parse_sql("INSERT INTO t");
    assert!(r.has_errors());
    let err = &r.errors[0];
    assert!(
        err.message.contains("VALUES") || err.message.contains("SELECT"),
        "error should mention VALUES or SELECT, got: {}",
        err.message
    );
}

// ===========================================================================
// 41. Verify ParseError fields are populated
// ===========================================================================

#[test]
fn parse_error_fields_populated() {
    let r = parse_sql("GARBAGE");
    assert!(r.has_errors());
    let err = &r.errors[0];

    // message is non-empty
    assert!(!err.message.is_empty(), "error message should not be empty");

    // line and col are at least 1 (1-based)
    assert!(err.line >= 1, "line should be >= 1, got {}", err.line);
    assert!(err.col >= 1, "col should be >= 1, got {}", err.col);

    // offset is valid
    assert_eq!(err.offset, 0, "GARBAGE starts at offset 0");
}

// ===========================================================================
// 42. Error after valid DDL
// ===========================================================================

#[test]
fn error_after_valid_ddl() {
    let r = parse_sql("CREATE TABLE t (id INT); GARBAGE");
    assert!(r.has_errors());
    assert_eq!(r.statements.len(), 1, "CREATE TABLE should parse");
    assert_eq!(r.errors.len(), 1, "GARBAGE should error");
    assert!(matches!(&r.statements[0], Statement::CreateTable(_)));
}

// ===========================================================================
// 43. Nested expression errors do not crash
// ===========================================================================

#[test]
fn nested_expression_error_no_crash() {
    // Deeply nested unclosed parens — should error, not crash
    let r = parse_sql("SELECT ((((1 + 2");
    assert!(r.has_errors());
}

#[test]
fn mismatched_parens_no_crash() {
    let r = parse_sql("SELECT 1)");
    // This may or may not error (the `)` might just be seen as junk after
    // the statement), but it should not crash.
    let _ = r;
}

// ===========================================================================
// 44. Large number of errors does not crash
// ===========================================================================

#[test]
fn many_errors_do_not_crash() {
    // 50 garbage tokens separated by semicolons
    let input: String = (0..50)
        .map(|i| format!("GARBAGE{}", i))
        .collect::<Vec<_>>()
        .join("; ");
    let r = parse_sql(&input);
    assert!(r.has_errors());
    assert_eq!(r.errors.len(), 50, "should collect 50 errors");
    assert!(r.statements.is_empty());
}

// ===========================================================================
// 45. ALTER TABLE DROP without COLUMN or CONSTRAINT
// ===========================================================================

#[test]
fn alter_table_drop_without_target() {
    let r = parse_sql("ALTER TABLE t DROP something_else");
    // "DROP" followed by neither COLUMN nor CONSTRAINT.
    // "something_else" is an identifier, not COLUMN or CONSTRAINT keyword.
    // However, parse_name might accept it as a name after DROP.
    // Let's check what actually happens: the parser does `eat(KwColumn)` then
    // `eat(KwConstraint)` — if neither matches, it errors.
    // But wait: the code does `if self.eat(KwDrop)` then `if self.eat(KwColumn)`.
    // "something_else" is an Identifier, not KwColumn, so eat(KwColumn) fails.
    // Then eat(KwConstraint) also fails. So it hits the error path.
    assert!(r.has_errors());
    let err = &r.errors[0];
    assert!(
        err.expected.contains(&"COLUMN".to_string())
            || err.expected.contains(&"CONSTRAINT".to_string()),
        "should expect COLUMN or CONSTRAINT, got {:?}",
        err.expected
    );
}

// ===========================================================================
// 46. Error recovery preserves position for subsequent valid parse
// ===========================================================================

#[test]
fn error_recovery_preserves_subsequent_statement_correctness() {
    let r = parse_sql("GARBAGE; SELECT 42 AS answer");
    assert!(r.has_errors());
    assert_eq!(r.statements.len(), 1);
    match &r.statements[0] {
        Statement::Select(sel) => {
            assert_eq!(sel.columns.len(), 1);
            match &sel.columns[0] {
                SelectColumn::Expr { expr, alias } => {
                    assert_eq!(*expr, Expr::Integer(42));
                    assert_eq!(alias.as_deref(), Some("answer"));
                }
                other => panic!("expected expr column, got {:?}", other),
            }
        }
        other => panic!("expected SELECT, got {:?}", other),
    }
}

// ===========================================================================
// 47. Common mistake hint positions
// ===========================================================================

#[test]
fn common_mistake_from_position() {
    let r = parse_sql("FROM users");
    assert!(r.has_errors());
    let err = &r.errors[0];
    // FROM starts at offset 0, line 1, col 1
    assert_eq!(err.offset, 0);
    assert_eq!(err.line, 1);
    assert_eq!(err.col, 1);
}

#[test]
fn common_mistake_set_position() {
    let r = parse_sql("  SET a = 1");
    assert!(r.has_errors());
    let err = &r.errors[0];
    // SET starts at offset 2 (after 2 spaces), col 3
    assert_eq!(err.offset, 2);
    assert_eq!(err.line, 1);
    assert_eq!(err.col, 3);
}

// ===========================================================================
// 48. Verify synchronize stops at statement-starting keywords
// ===========================================================================

#[test]
fn synchronize_stops_at_insert() {
    let r = parse_sql("GARBAGE INSERT INTO t VALUES (1)");
    assert!(r.has_errors());
    assert!(
        r.statements.len() >= 1,
        "should recover at INSERT keyword"
    );
    if r.statements.len() >= 1 {
        assert!(
            matches!(&r.statements[0], Statement::Insert(_)),
            "recovered statement should be INSERT, got {:?}",
            r.statements[0]
        );
    }
}

#[test]
fn synchronize_stops_at_delete() {
    let r = parse_sql("GARBAGE DELETE FROM t");
    assert!(r.has_errors());
    assert!(
        r.statements.len() >= 1,
        "should recover at DELETE keyword"
    );
    if r.statements.len() >= 1 {
        assert!(
            matches!(&r.statements[0], Statement::Delete(_)),
            "recovered statement should be DELETE"
        );
    }
}

#[test]
fn synchronize_stops_at_update() {
    let r = parse_sql("GARBAGE UPDATE t SET a = 1");
    assert!(r.has_errors());
    assert!(
        r.statements.len() >= 1,
        "should recover at UPDATE keyword"
    );
    if r.statements.len() >= 1 {
        assert!(
            matches!(&r.statements[0], Statement::Update(_)),
            "recovered statement should be UPDATE"
        );
    }
}

#[test]
fn synchronize_stops_at_create() {
    let r = parse_sql("GARBAGE CREATE TABLE t (id INT)");
    assert!(r.has_errors());
    assert!(
        r.statements.len() >= 1,
        "should recover at CREATE keyword"
    );
    if r.statements.len() >= 1 {
        assert!(
            matches!(&r.statements[0], Statement::CreateTable(_)),
            "recovered statement should be CREATE TABLE"
        );
    }
}

#[test]
fn synchronize_stops_at_begin() {
    let r = parse_sql("GARBAGE BEGIN");
    assert!(r.has_errors());
    assert!(
        r.statements.len() >= 1,
        "should recover at BEGIN keyword"
    );
    if r.statements.len() >= 1 {
        assert!(
            matches!(&r.statements[0], Statement::Begin),
            "recovered statement should be BEGIN"
        );
    }
}

// ===========================================================================
// 49. Error after expression in SELECT
// ===========================================================================

#[test]
fn error_in_select_from_clause() {
    // SELECT * FROM — missing table name
    let r = parse_sql("SELECT * FROM");
    assert!(r.has_errors());
}

// ===========================================================================
// 50. Tab and newline handling in error positions
// ===========================================================================

#[test]
fn error_on_third_line() {
    let r = parse_sql("\n\nGARBAGE");
    assert!(r.has_errors());
    let err = &r.errors[0];
    assert_eq!(err.line, 3, "error should be on line 3, got {}", err.line);
}

// ===========================================================================
// 51. Error count consistency with has_errors
// ===========================================================================

#[test]
fn has_errors_consistent_with_error_count() {
    let inputs = vec![
        ("SELECT 1", false),
        ("GARBAGE", true),
        ("", false),
        (";;;", false),
        ("SELECT 1; GARBAGE", true),
        ("FROM users", true),
    ];

    for (input, should_have_errors) in inputs {
        let r = parse_sql(input);
        assert_eq!(
            r.has_errors(),
            should_have_errors,
            "has_errors() mismatch for input {:?}: expected {}, errors={:?}",
            input,
            should_have_errors,
            r.errors
        );
        assert_eq!(
            !r.errors.is_empty(),
            should_have_errors,
            "errors.is_empty() mismatch for input {:?}",
            input
        );
    }
}
