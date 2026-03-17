use sqld::sql::error::DEFAULT_MAX_ERRORS;
use sqld::sql::token::Span;
use sqld::sql::{tokenize, tokenize_with_limit, TokenKind};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn kinds_no_eof(input: &str) -> Vec<TokenKind> {
    tokenize(input)
        .tokens
        .iter()
        .map(|t| t.kind)
        .filter(|k| *k != TokenKind::Eof)
        .collect()
}

fn first_kind(input: &str) -> TokenKind {
    tokenize(input).tokens[0].kind
}

fn first_span(input: &str) -> Span {
    tokenize(input).tokens[0].span
}

// ---------------------------------------------------------------------------
// Empty input / EOF
// ---------------------------------------------------------------------------

#[test]
fn test_empty_input() {
    let res = tokenize("");
    assert_eq!(res.tokens.len(), 1);
    assert_eq!(res.tokens[0].kind, TokenKind::Eof);
    assert_eq!(res.tokens[0].span, Span::new(0, 0));
    assert!(!res.has_errors());
}

#[test]
fn test_whitespace_only() {
    let res = tokenize("   \n\t\r\n  ");
    assert_eq!(res.tokens.len(), 1);
    assert_eq!(res.tokens[0].kind, TokenKind::Eof);
}

// ---------------------------------------------------------------------------
// Keywords (representative subset + case insensitivity)
// ---------------------------------------------------------------------------

#[test]
fn test_keywords_lowercase() {
    assert_eq!(first_kind("select"), TokenKind::KwSelect);
    assert_eq!(first_kind("from"), TokenKind::KwFrom);
    assert_eq!(first_kind("where"), TokenKind::KwWhere);
    assert_eq!(first_kind("insert"), TokenKind::KwInsert);
    assert_eq!(first_kind("into"), TokenKind::KwInto);
    assert_eq!(first_kind("values"), TokenKind::KwValues);
    assert_eq!(first_kind("update"), TokenKind::KwUpdate);
    assert_eq!(first_kind("delete"), TokenKind::KwDelete);
    assert_eq!(first_kind("create"), TokenKind::KwCreate);
    assert_eq!(first_kind("table"), TokenKind::KwTable);
    assert_eq!(first_kind("drop"), TokenKind::KwDrop);
    assert_eq!(first_kind("alter"), TokenKind::KwAlter);
    assert_eq!(first_kind("join"), TokenKind::KwJoin);
    assert_eq!(first_kind("inner"), TokenKind::KwInner);
    assert_eq!(first_kind("left"), TokenKind::KwLeft);
    assert_eq!(first_kind("right"), TokenKind::KwRight);
    assert_eq!(first_kind("full"), TokenKind::KwFull);
    assert_eq!(first_kind("outer"), TokenKind::KwOuter);
    assert_eq!(first_kind("cross"), TokenKind::KwCross);
    assert_eq!(first_kind("natural"), TokenKind::KwNatural);
    assert_eq!(first_kind("order"), TokenKind::KwOrder);
    assert_eq!(first_kind("by"), TokenKind::KwBy);
    assert_eq!(first_kind("group"), TokenKind::KwGroup);
    assert_eq!(first_kind("having"), TokenKind::KwHaving);
    assert_eq!(first_kind("limit"), TokenKind::KwLimit);
    assert_eq!(first_kind("offset"), TokenKind::KwOffset);
    assert_eq!(first_kind("distinct"), TokenKind::KwDistinct);
    assert_eq!(first_kind("union"), TokenKind::KwUnion);
    assert_eq!(first_kind("intersect"), TokenKind::KwIntersect);
    assert_eq!(first_kind("except"), TokenKind::KwExcept);
    assert_eq!(first_kind("with"), TokenKind::KwWith);
    assert_eq!(first_kind("recursive"), TokenKind::KwRecursive);
    assert_eq!(first_kind("case"), TokenKind::KwCase);
    assert_eq!(first_kind("when"), TokenKind::KwWhen);
    assert_eq!(first_kind("then"), TokenKind::KwThen);
    assert_eq!(first_kind("else"), TokenKind::KwElse);
    assert_eq!(first_kind("end"), TokenKind::KwEnd);
    assert_eq!(first_kind("cast"), TokenKind::KwCast);
    assert_eq!(first_kind("primary"), TokenKind::KwPrimary);
    assert_eq!(first_kind("key"), TokenKind::KwKey);
    assert_eq!(first_kind("foreign"), TokenKind::KwForeign);
    assert_eq!(first_kind("references"), TokenKind::KwReferences);
    assert_eq!(first_kind("unique"), TokenKind::KwUnique);
    assert_eq!(first_kind("begin"), TokenKind::KwBegin);
    assert_eq!(first_kind("commit"), TokenKind::KwCommit);
    assert_eq!(first_kind("rollback"), TokenKind::KwRollback);
    assert_eq!(first_kind("explain"), TokenKind::KwExplain);
    assert_eq!(first_kind("analyze"), TokenKind::KwAnalyze);
    assert_eq!(first_kind("vacuum"), TokenKind::KwVacuum);
    assert_eq!(first_kind("truncate"), TokenKind::KwTruncate);
    assert_eq!(first_kind("returning"), TokenKind::KwReturning);
    assert_eq!(first_kind("coalesce"), TokenKind::KwCoalesce);
    assert_eq!(first_kind("nullif"), TokenKind::KwNullif);
    assert_eq!(first_kind("greatest"), TokenKind::KwGreatest);
    assert_eq!(first_kind("least"), TokenKind::KwLeast);
    assert_eq!(first_kind("any"), TokenKind::KwAny);
    assert_eq!(first_kind("some"), TokenKind::KwSome);
    assert_eq!(first_kind("array"), TokenKind::KwArray);
    assert_eq!(first_kind("row"), TokenKind::KwRow);
}

#[test]
fn test_keywords_case_insensitive() {
    assert_eq!(first_kind("SELECT"), TokenKind::KwSelect);
    assert_eq!(first_kind("Select"), TokenKind::KwSelect);
    assert_eq!(first_kind("sElEcT"), TokenKind::KwSelect);
    assert_eq!(first_kind("FROM"), TokenKind::KwFrom);
    assert_eq!(first_kind("Where"), TokenKind::KwWhere);
    assert_eq!(first_kind("INSERT"), TokenKind::KwInsert);
    assert_eq!(first_kind("CREATE"), TokenKind::KwCreate);
    assert_eq!(first_kind("TABLE"), TokenKind::KwTable);
    assert_eq!(first_kind("JOIN"), TokenKind::KwJoin);
    assert_eq!(first_kind("ORDER"), TokenKind::KwOrder);
    assert_eq!(first_kind("COALESCE"), TokenKind::KwCoalesce);
    assert_eq!(first_kind("NULLIF"), TokenKind::KwNullif);
    assert_eq!(first_kind("ROW"), TokenKind::KwRow);
}

// ---------------------------------------------------------------------------
// Identifiers
// ---------------------------------------------------------------------------

#[test]
fn test_identifiers() {
    assert_eq!(first_kind("my_table"), TokenKind::Identifier);
    assert_eq!(first_kind("_private"), TokenKind::Identifier);
    assert_eq!(first_kind("col1"), TokenKind::Identifier);
    assert_eq!(first_kind("a"), TokenKind::Identifier);
    assert_eq!(first_kind("_"), TokenKind::Identifier);
    assert_eq!(first_kind("x123_abc"), TokenKind::Identifier);
}

#[test]
fn test_identifier_span() {
    let input = "my_table";
    let span = first_span(input);
    assert_eq!(span.text(input), "my_table");
}

#[test]
fn test_identifier_not_keyword() {
    // Words that look like keywords but aren't
    assert_eq!(first_kind("selects"), TokenKind::Identifier);
    assert_eq!(first_kind("fromage"), TokenKind::Identifier);
    assert_eq!(first_kind("whereas"), TokenKind::Identifier);
    assert_eq!(first_kind("creating"), TokenKind::Identifier);
}

// ---------------------------------------------------------------------------
// Quoted identifiers
// ---------------------------------------------------------------------------

#[test]
fn test_quoted_identifier() {
    assert_eq!(first_kind(r#""MyTable""#), TokenKind::QuotedIdentifier);
    assert_eq!(first_kind(r#""select""#), TokenKind::QuotedIdentifier);
    assert_eq!(first_kind(r#""has space""#), TokenKind::QuotedIdentifier);
}

#[test]
fn test_quoted_identifier_case_preserved() {
    let input = r#""CamelCase""#;
    let span = first_span(input);
    assert_eq!(span.text(input), r#""CamelCase""#);
}

#[test]
fn test_quoted_identifier_double_quote_escape() {
    let input = r#""has""quote""#;
    assert_eq!(first_kind(input), TokenKind::QuotedIdentifier);
    let span = first_span(input);
    assert_eq!(span.text(input), input);
}

#[test]
fn test_unterminated_quoted_identifier() {
    let res = tokenize(r#""unterminated"#);
    assert_eq!(res.tokens[0].kind, TokenKind::QuotedIdentifier);
    assert!(res.has_errors());
    assert_eq!(res.errors[0].message, "unterminated quoted identifier");
}

// ---------------------------------------------------------------------------
// Boolean & null literals
// ---------------------------------------------------------------------------

#[test]
fn test_boolean_literals() {
    assert_eq!(first_kind("true"), TokenKind::BooleanLiteral);
    assert_eq!(first_kind("false"), TokenKind::BooleanLiteral);
    assert_eq!(first_kind("TRUE"), TokenKind::BooleanLiteral);
    assert_eq!(first_kind("FALSE"), TokenKind::BooleanLiteral);
    assert_eq!(first_kind("True"), TokenKind::BooleanLiteral);
    assert_eq!(first_kind("False"), TokenKind::BooleanLiteral);
}

#[test]
fn test_null_literal() {
    assert_eq!(first_kind("null"), TokenKind::NullLiteral);
    assert_eq!(first_kind("NULL"), TokenKind::NullLiteral);
    assert_eq!(first_kind("Null"), TokenKind::NullLiteral);
}

// ---------------------------------------------------------------------------
// Integer literals
// ---------------------------------------------------------------------------

#[test]
fn test_integer_literals() {
    assert_eq!(first_kind("0"), TokenKind::IntegerLiteral);
    assert_eq!(first_kind("42"), TokenKind::IntegerLiteral);
    assert_eq!(first_kind("123456789"), TokenKind::IntegerLiteral);
}

#[test]
fn test_integer_span() {
    let input = "42";
    let span = first_span(input);
    assert_eq!(span.text(input), "42");
}

// ---------------------------------------------------------------------------
// Float literals
// ---------------------------------------------------------------------------

#[test]
fn test_float_literals() {
    assert_eq!(first_kind("3.14"), TokenKind::FloatLiteral);
    assert_eq!(first_kind("0.5"), TokenKind::FloatLiteral);
    assert_eq!(first_kind("100.0"), TokenKind::FloatLiteral);
}

#[test]
fn test_float_span() {
    let input = "3.14";
    let span = first_span(input);
    assert_eq!(span.text(input), "3.14");
}

// ---------------------------------------------------------------------------
// Scientific notation
// ---------------------------------------------------------------------------

#[test]
fn test_scientific_notation() {
    assert_eq!(first_kind("1e10"), TokenKind::FloatLiteral);
    assert_eq!(first_kind("1E10"), TokenKind::FloatLiteral);
    assert_eq!(first_kind("1e+10"), TokenKind::FloatLiteral);
    assert_eq!(first_kind("1e-3"), TokenKind::FloatLiteral);
    assert_eq!(first_kind("3.14e2"), TokenKind::FloatLiteral);
    assert_eq!(first_kind("2.5E-4"), TokenKind::FloatLiteral);
}

#[test]
fn test_scientific_span() {
    let input = "1.5e-3";
    let span = first_span(input);
    assert_eq!(span.text(input), "1.5e-3");
}

#[test]
fn test_not_scientific_without_digits() {
    // 1e followed by non-digit: should be IntegerLiteral(1) then Identifier(e_var)
    // but `1e` alone: IntegerLiteral(1) then Identifier(e)
    let res = tokenize("1e");
    assert_eq!(res.tokens[0].kind, TokenKind::IntegerLiteral);
    assert_eq!(res.tokens[0].span.text("1e"), "1");
    assert_eq!(res.tokens[1].kind, TokenKind::Identifier);
}

// ---------------------------------------------------------------------------
// Hex literals
// ---------------------------------------------------------------------------

#[test]
fn test_hex_literals() {
    assert_eq!(first_kind("0x1A"), TokenKind::IntegerLiteral);
    assert_eq!(first_kind("0xFF"), TokenKind::IntegerLiteral);
    assert_eq!(first_kind("0X00"), TokenKind::IntegerLiteral);
    assert_eq!(first_kind("0xDEAD"), TokenKind::IntegerLiteral);
    assert_eq!(first_kind("0xabcdef"), TokenKind::IntegerLiteral);
}

#[test]
fn test_hex_span() {
    let input = "0xFF";
    let span = first_span(input);
    assert_eq!(span.text(input), "0xFF");
}

#[test]
fn test_hex_no_digits_error() {
    let res = tokenize("0x ");
    assert_eq!(res.tokens[0].kind, TokenKind::Error);
    assert!(res.has_errors());
    assert!(res.errors[0].message.contains("hex literal"));
}

// ---------------------------------------------------------------------------
// String literals
// ---------------------------------------------------------------------------

#[test]
fn test_string_literals() {
    assert_eq!(first_kind("'hello'"), TokenKind::StringLiteral);
    assert_eq!(first_kind("''"), TokenKind::StringLiteral);
    assert_eq!(first_kind("'hello world'"), TokenKind::StringLiteral);
}

#[test]
fn test_string_span() {
    let input = "'hello'";
    let span = first_span(input);
    assert_eq!(span.text(input), "'hello'");
}

#[test]
fn test_string_single_quote_escape() {
    // 'it''s' is a string containing it's
    let input = "'it''s'";
    let res = tokenize(input);
    assert_eq!(res.tokens[0].kind, TokenKind::StringLiteral);
    assert_eq!(res.tokens[0].span.text(input), "'it''s'");
    assert!(!res.has_errors());
}

#[test]
fn test_string_multiple_escapes() {
    let input = "'a''b''c'";
    let res = tokenize(input);
    assert_eq!(res.tokens[0].kind, TokenKind::StringLiteral);
    assert_eq!(res.tokens[0].span.text(input), input);
}

#[test]
fn test_unterminated_string() {
    let res = tokenize("'unterminated");
    assert_eq!(res.tokens[0].kind, TokenKind::StringLiteral);
    assert!(res.has_errors());
    assert!(res.errors[0].message.contains("unterminated string"));
}

#[test]
fn test_empty_string() {
    let input = "''";
    let res = tokenize(input);
    assert_eq!(res.tokens[0].kind, TokenKind::StringLiteral);
    assert_eq!(res.tokens[0].span.text(input), "''");
    assert!(!res.has_errors());
}

// ---------------------------------------------------------------------------
// E-string literals (C-style escapes)
// ---------------------------------------------------------------------------

#[test]
fn test_e_string_basic() {
    assert_eq!(first_kind("E'hello'"), TokenKind::StringLiteral);
    assert_eq!(first_kind("e'hello'"), TokenKind::StringLiteral);
}

#[test]
fn test_e_string_backslash_escapes() {
    // E'it\'s' — backslash-escaped quote does not end the string
    let input = r"E'it\'s'";
    let res = tokenize(input);
    assert_eq!(res.tokens[0].kind, TokenKind::StringLiteral);
    assert_eq!(res.tokens[0].span.text(input), input);
    assert!(!res.has_errors());
}

#[test]
fn test_e_string_backslash_n() {
    let input = r"E'line1\nline2'";
    let res = tokenize(input);
    assert_eq!(res.tokens[0].kind, TokenKind::StringLiteral);
    assert_eq!(res.tokens[0].span.text(input), input);
}

#[test]
fn test_e_string_double_backslash() {
    let input = r"E'path\\to\\file'";
    let res = tokenize(input);
    assert_eq!(res.tokens[0].kind, TokenKind::StringLiteral);
    assert_eq!(res.tokens[0].span.text(input), input);
}

#[test]
fn test_e_string_double_quote_escape() {
    // E-strings also support '' escaping
    let input = "E'it''s'";
    let res = tokenize(input);
    assert_eq!(res.tokens[0].kind, TokenKind::StringLiteral);
    assert_eq!(res.tokens[0].span.text(input), input);
    assert!(!res.has_errors());
}

#[test]
fn test_unterminated_e_string() {
    let res = tokenize("E'unterminated");
    assert!(res.has_errors());
    assert!(res.errors[0].message.contains("unterminated"));
}

#[test]
fn test_e_not_followed_by_quote_is_identifier() {
    // Plain E without quote is an identifier
    assert_eq!(first_kind("E"), TokenKind::Identifier);
    assert_eq!(first_kind("EXISTS"), TokenKind::KwExists);
}

// ---------------------------------------------------------------------------
// Operators
// ---------------------------------------------------------------------------

#[test]
fn test_operators() {
    assert_eq!(first_kind("+"), TokenKind::Plus);
    assert_eq!(first_kind("-"), TokenKind::Minus);
    assert_eq!(first_kind("*"), TokenKind::Star);
    assert_eq!(first_kind("/"), TokenKind::Slash);
    assert_eq!(first_kind("%"), TokenKind::Percent);
    assert_eq!(first_kind("^"), TokenKind::Caret);
    assert_eq!(first_kind("="), TokenKind::Eq);
    assert_eq!(first_kind("<"), TokenKind::Lt);
    assert_eq!(first_kind(">"), TokenKind::Gt);
}

#[test]
fn test_multi_char_operators() {
    assert_eq!(first_kind("<>"), TokenKind::NotEq);
    assert_eq!(first_kind("<="), TokenKind::LtEq);
    assert_eq!(first_kind(">="), TokenKind::GtEq);
    assert_eq!(first_kind("||"), TokenKind::Concat);
    assert_eq!(first_kind("!="), TokenKind::NotEq);
}

// ---------------------------------------------------------------------------
// Operator disambiguation (longest match)
// ---------------------------------------------------------------------------

#[test]
fn test_lt_gt_disambiguation() {
    // <> should be NotEq, not Lt then Gt
    let k = kinds_no_eof("<>");
    assert_eq!(k, vec![TokenKind::NotEq]);
}

#[test]
fn test_lt_eq_disambiguation() {
    let k = kinds_no_eof("<=");
    assert_eq!(k, vec![TokenKind::LtEq]);
}

#[test]
fn test_gt_eq_disambiguation() {
    let k = kinds_no_eof(">=");
    assert_eq!(k, vec![TokenKind::GtEq]);
}

#[test]
fn test_concat_disambiguation() {
    // || should be Concat, not two errors
    let k = kinds_no_eof("||");
    assert_eq!(k, vec![TokenKind::Concat]);
}

#[test]
fn test_colon_colon_disambiguation() {
    let k = kinds_no_eof("::");
    assert_eq!(k, vec![TokenKind::ColonColon]);
}

#[test]
fn test_lt_followed_by_something_else() {
    let k = kinds_no_eof("< 5");
    assert_eq!(k, vec![TokenKind::Lt, TokenKind::IntegerLiteral]);
}

#[test]
fn test_gt_followed_by_something_else() {
    let k = kinds_no_eof("> 5");
    assert_eq!(k, vec![TokenKind::Gt, TokenKind::IntegerLiteral]);
}

// ---------------------------------------------------------------------------
// Punctuation
// ---------------------------------------------------------------------------

#[test]
fn test_punctuation() {
    assert_eq!(first_kind("("), TokenKind::LeftParen);
    assert_eq!(first_kind(")"), TokenKind::RightParen);
    assert_eq!(first_kind(","), TokenKind::Comma);
    assert_eq!(first_kind(";"), TokenKind::Semicolon);
    assert_eq!(first_kind("."), TokenKind::Dot);
    assert_eq!(first_kind("::"), TokenKind::ColonColon);
}

// ---------------------------------------------------------------------------
// Placeholder
// ---------------------------------------------------------------------------

#[test]
fn test_placeholder() {
    assert_eq!(first_kind("$1"), TokenKind::Placeholder);
    assert_eq!(first_kind("$42"), TokenKind::Placeholder);
    assert_eq!(first_kind("$100"), TokenKind::Placeholder);
}

#[test]
fn test_placeholder_span() {
    let input = "$1";
    let span = first_span(input);
    assert_eq!(span.text(input), "$1");

    let input2 = "$42";
    let span2 = first_span(input2);
    assert_eq!(span2.text(input2), "$42");
}

// ---------------------------------------------------------------------------
// Comments
// ---------------------------------------------------------------------------

#[test]
fn test_single_line_comment() {
    let k = kinds_no_eof("-- this is a comment\nSELECT");
    assert_eq!(k, vec![TokenKind::KwSelect]);
}

#[test]
fn test_single_line_comment_at_end() {
    let k = kinds_no_eof("SELECT -- trailing comment");
    assert_eq!(k, vec![TokenKind::KwSelect]);
}

#[test]
fn test_block_comment() {
    let k = kinds_no_eof("/* block comment */ SELECT");
    assert_eq!(k, vec![TokenKind::KwSelect]);
}

#[test]
fn test_block_comment_multiline() {
    let k = kinds_no_eof("/* line1\nline2\nline3 */ SELECT");
    assert_eq!(k, vec![TokenKind::KwSelect]);
}

#[test]
fn test_nested_block_comment() {
    let k = kinds_no_eof("/* outer /* inner */ still comment */ SELECT");
    assert_eq!(k, vec![TokenKind::KwSelect]);
}

#[test]
fn test_deeply_nested_block_comment() {
    let k = kinds_no_eof("/* a /* b /* c */ b */ a */ SELECT");
    assert_eq!(k, vec![TokenKind::KwSelect]);
}

#[test]
fn test_unterminated_block_comment() {
    let res = tokenize("/* unterminated");
    assert!(res.has_errors());
    assert!(res.errors[0].message.contains("unterminated block comment"));
}

#[test]
fn test_comment_does_not_affect_strings() {
    // String containing -- should not be treated as comment
    let input = "'hello -- world'";
    let k = kinds_no_eof(input);
    assert_eq!(k, vec![TokenKind::StringLiteral]);
}

// ---------------------------------------------------------------------------
// Error reporting with position
// ---------------------------------------------------------------------------

#[test]
fn test_error_position_line_col() {
    let res = tokenize("SELECT @");
    assert!(res.has_errors());
    let err = &res.errors[0];
    assert_eq!(err.line, 1);
    assert_eq!(err.col, 8);
    assert_eq!(err.offset, 7);
}

#[test]
fn test_error_position_multiline() {
    let res = tokenize("SELECT\n  @");
    assert!(res.has_errors());
    let err = &res.errors[0];
    assert_eq!(err.line, 2);
    assert_eq!(err.col, 3);
}

#[test]
fn test_error_token_in_stream() {
    let res = tokenize("SELECT @ FROM");
    let k: Vec<TokenKind> = res.tokens.iter().map(|t| t.kind).collect();
    assert_eq!(
        k,
        vec![
            TokenKind::KwSelect,
            TokenKind::Error,
            TokenKind::KwFrom,
            TokenKind::Eof,
        ]
    );
}

#[test]
fn test_single_pipe_error() {
    let res = tokenize("a | b");
    assert!(res.has_errors());
    assert!(res.errors[0].message.contains("||"));
}

#[test]
fn test_single_colon_error() {
    let res = tokenize("a : b");
    assert!(res.has_errors());
    assert!(res.errors[0].message.contains("::"));
}

// ---------------------------------------------------------------------------
// Error limit
// ---------------------------------------------------------------------------

#[test]
fn test_error_limit_default() {
    // Generate more than DEFAULT_MAX_ERRORS errors
    let bad_chars: String = "@".repeat(DEFAULT_MAX_ERRORS + 20);
    let res = tokenize(&bad_chars);
    assert_eq!(res.errors.len(), DEFAULT_MAX_ERRORS);
    // But all error tokens still appear in the stream
    let error_count = res
        .tokens
        .iter()
        .filter(|t| t.kind == TokenKind::Error)
        .count();
    assert_eq!(error_count, DEFAULT_MAX_ERRORS + 20);
}

#[test]
fn test_error_limit_custom() {
    let bad_chars: String = "@".repeat(10);
    let res = tokenize_with_limit(&bad_chars, 3);
    assert_eq!(res.errors.len(), 3);
    let error_count = res
        .tokens
        .iter()
        .filter(|t| t.kind == TokenKind::Error)
        .count();
    assert_eq!(error_count, 10);
}

// ---------------------------------------------------------------------------
// Consecutive strings
// ---------------------------------------------------------------------------

#[test]
fn test_consecutive_strings() {
    let input = "'a' 'b' 'c'";
    let k = kinds_no_eof(input);
    assert_eq!(
        k,
        vec![
            TokenKind::StringLiteral,
            TokenKind::StringLiteral,
            TokenKind::StringLiteral,
        ]
    );
}

#[test]
fn test_consecutive_strings_no_space() {
    // Two adjacent strings: 'a''b' is actually one string with escaped quote
    // But 'a' 'b' (with space) is two strings
    let input = "'a' 'b'";
    let res = tokenize(input);
    assert_eq!(res.tokens.len(), 3); // two strings + eof
}

// ---------------------------------------------------------------------------
// Token spans & positions
// ---------------------------------------------------------------------------

#[test]
fn test_token_span_offsets() {
    let input = "SELECT * FROM t";
    let res = tokenize(input);
    let t = &res.tokens;

    assert_eq!(t[0].span.text(input), "SELECT");
    assert_eq!(t[0].span, Span::new(0, 6));
    assert_eq!(t[0].line, 1);
    assert_eq!(t[0].col, 1);

    assert_eq!(t[1].span.text(input), "*");
    assert_eq!(t[1].span, Span::new(7, 8));
    assert_eq!(t[1].line, 1);
    assert_eq!(t[1].col, 8);

    assert_eq!(t[2].span.text(input), "FROM");
    assert_eq!(t[2].span, Span::new(9, 13));
    assert_eq!(t[2].line, 1);
    assert_eq!(t[2].col, 10);

    assert_eq!(t[3].span.text(input), "t");
    assert_eq!(t[3].span, Span::new(14, 15));
    assert_eq!(t[3].line, 1);
    assert_eq!(t[3].col, 15);
}

#[test]
fn test_multiline_positions() {
    let input = "SELECT\n  *\nFROM\n  t";
    let res = tokenize(input);
    let t = &res.tokens;

    assert_eq!(t[0].kind, TokenKind::KwSelect);
    assert_eq!(t[0].line, 1);
    assert_eq!(t[0].col, 1);

    assert_eq!(t[1].kind, TokenKind::Star);
    assert_eq!(t[1].line, 2);
    assert_eq!(t[1].col, 3);

    assert_eq!(t[2].kind, TokenKind::KwFrom);
    assert_eq!(t[2].line, 3);
    assert_eq!(t[2].col, 1);

    assert_eq!(t[3].kind, TokenKind::Identifier);
    assert_eq!(t[3].line, 4);
    assert_eq!(t[3].col, 3);
}

// ---------------------------------------------------------------------------
// Complex statements
// ---------------------------------------------------------------------------

#[test]
fn test_select_statement() {
    let input = "SELECT a, b FROM t WHERE a > 10 ORDER BY b ASC;";
    let k = kinds_no_eof(input);
    assert_eq!(
        k,
        vec![
            TokenKind::KwSelect,
            TokenKind::Identifier,
            TokenKind::Comma,
            TokenKind::Identifier,
            TokenKind::KwFrom,
            TokenKind::Identifier,
            TokenKind::KwWhere,
            TokenKind::Identifier,
            TokenKind::Gt,
            TokenKind::IntegerLiteral,
            TokenKind::KwOrder,
            TokenKind::KwBy,
            TokenKind::Identifier,
            TokenKind::KwAsc,
            TokenKind::Semicolon,
        ]
    );
}

#[test]
fn test_insert_statement() {
    let input = "INSERT INTO users (name, age) VALUES ('Alice', 30);";
    let k = kinds_no_eof(input);
    assert_eq!(
        k,
        vec![
            TokenKind::KwInsert,
            TokenKind::KwInto,
            TokenKind::Identifier,
            TokenKind::LeftParen,
            TokenKind::Identifier,
            TokenKind::Comma,
            TokenKind::Identifier,
            TokenKind::RightParen,
            TokenKind::KwValues,
            TokenKind::LeftParen,
            TokenKind::StringLiteral,
            TokenKind::Comma,
            TokenKind::IntegerLiteral,
            TokenKind::RightParen,
            TokenKind::Semicolon,
        ]
    );
}

#[test]
fn test_cast_expression() {
    let input = "x::integer";
    let k = kinds_no_eof(input);
    assert_eq!(
        k,
        vec![
            TokenKind::Identifier,
            TokenKind::ColonColon,
            TokenKind::Identifier, // "integer" is not a keyword
        ]
    );
}

#[test]
fn test_concat_expression() {
    let input = "'hello' || ' ' || 'world'";
    let k = kinds_no_eof(input);
    assert_eq!(
        k,
        vec![
            TokenKind::StringLiteral,
            TokenKind::Concat,
            TokenKind::StringLiteral,
            TokenKind::Concat,
            TokenKind::StringLiteral,
        ]
    );
}

#[test]
fn test_placeholder_in_query() {
    let input = "SELECT * FROM t WHERE id = $1 AND name = $2";
    let k = kinds_no_eof(input);
    assert_eq!(
        k,
        vec![
            TokenKind::KwSelect,
            TokenKind::Star,
            TokenKind::KwFrom,
            TokenKind::Identifier,
            TokenKind::KwWhere,
            TokenKind::Identifier,
            TokenKind::Eq,
            TokenKind::Placeholder,
            TokenKind::KwAnd,
            TokenKind::Identifier,
            TokenKind::Eq,
            TokenKind::Placeholder,
        ]
    );
}

#[test]
fn test_qualified_name() {
    let input = "schema.table.column";
    let k = kinds_no_eof(input);
    assert_eq!(
        k,
        vec![
            TokenKind::KwSchema,
            TokenKind::Dot,
            TokenKind::KwTable,
            TokenKind::Dot,
            TokenKind::KwColumn,
        ]
    );
}

#[test]
fn test_create_table() {
    let input = "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);";
    let k = kinds_no_eof(input);
    assert_eq!(
        k,
        vec![
            TokenKind::KwCreate,
            TokenKind::KwTable,
            TokenKind::KwIf,
            TokenKind::KwNot,
            TokenKind::KwExists,
            TokenKind::Identifier,
            TokenKind::LeftParen,
            TokenKind::Identifier,
            TokenKind::Identifier, // INTEGER is not a keyword in our set
            TokenKind::KwPrimary,
            TokenKind::KwKey,
            TokenKind::Comma,
            TokenKind::Identifier,
            TokenKind::Identifier, // TEXT is not a keyword in our set
            TokenKind::KwNot,
            TokenKind::NullLiteral,
            TokenKind::RightParen,
            TokenKind::Semicolon,
        ]
    );
}

#[test]
fn test_is_null_expression() {
    let input = "x IS NULL";
    let k = kinds_no_eof(input);
    assert_eq!(
        k,
        vec![TokenKind::Identifier, TokenKind::KwIs, TokenKind::NullLiteral]
    );
}

#[test]
fn test_is_not_null_expression() {
    let input = "x IS NOT NULL";
    let k = kinds_no_eof(input);
    assert_eq!(
        k,
        vec![
            TokenKind::Identifier,
            TokenKind::KwIs,
            TokenKind::KwNot,
            TokenKind::NullLiteral,
        ]
    );
}

// ---------------------------------------------------------------------------
// Numeric edge cases
// ---------------------------------------------------------------------------

#[test]
fn test_integer_followed_by_dot_no_digit() {
    // "1." should be IntegerLiteral then Dot
    let k = kinds_no_eof("1.");
    assert_eq!(k, vec![TokenKind::IntegerLiteral, TokenKind::Dot]);
}

#[test]
fn test_dot_followed_by_digit() {
    // ".5" should be Dot then IntegerLiteral
    let k = kinds_no_eof(".5");
    assert_eq!(k, vec![TokenKind::Dot, TokenKind::IntegerLiteral]);
}

#[test]
fn test_number_dot_identifier() {
    // "1.name" should be IntegerLiteral, Dot, Identifier
    let k = kinds_no_eof("1.name");
    assert_eq!(
        k,
        vec![
            TokenKind::IntegerLiteral,
            TokenKind::Dot,
            TokenKind::Identifier,
        ]
    );
}

// ---------------------------------------------------------------------------
// Span helper
// ---------------------------------------------------------------------------

#[test]
fn test_span_len_and_is_empty() {
    let s = Span::new(5, 10);
    assert_eq!(s.len(), 5);
    assert!(!s.is_empty());

    let empty = Span::new(3, 3);
    assert_eq!(empty.len(), 0);
    assert!(empty.is_empty());
}
