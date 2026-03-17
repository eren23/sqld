use crate::sql::ast::*;
use crate::sql::lexer::tokenize;
use crate::sql::token::{Token, TokenKind};
use crate::types::DataType;

// ---------------------------------------------------------------------------
// Binding powers (14 levels: OR lowest → function call highest)
// ---------------------------------------------------------------------------

const BP_OR: u8 = 10;
const BP_AND: u8 = 20;
const BP_NOT: u8 = 30; // prefix only
const BP_IS: u8 = 40;
const BP_COMPARISON: u8 = 50;
const BP_RANGE: u8 = 60; // BETWEEN, IN, LIKE, ILIKE
const BP_CONCAT: u8 = 70;
const BP_ADD: u8 = 80;
const BP_MUL: u8 = 90;
const BP_EXP: u8 = 100;
const BP_UNARY: u8 = 110; // prefix +, -
const BP_CAST: u8 = 120; // ::
const BP_FIELD: u8 = 130; // .
const BP_CALL: u8 = 140; // function()

// ---------------------------------------------------------------------------
// Parse error
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct ParseError {
    pub message: String,
    pub line: usize,
    pub col: usize,
    pub offset: usize,
    pub expected: Vec<String>,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}: {}", self.line, self.col, self.message)
    }
}

impl std::error::Error for ParseError {}

// ---------------------------------------------------------------------------
// Parse result
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct ParseResult {
    pub statements: Vec<Statement>,
    pub errors: Vec<ParseError>,
}

impl ParseResult {
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

pub fn parse(source: &str) -> ParseResult {
    let lex = tokenize(source);
    let parser = Parser::new(source, lex.tokens);
    parser.parse_all()
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

pub struct Parser<'a> {
    source: &'a str,
    tokens: Vec<Token>,
    pos: usize,
    errors: Vec<ParseError>,
}

impl<'a> Parser<'a> {
    pub fn new(source: &'a str, tokens: Vec<Token>) -> Self {
        Self {
            source,
            tokens,
            pos: 0,
            errors: Vec::new(),
        }
    }

    pub fn parse_all(mut self) -> ParseResult {
        let mut stmts = Vec::new();
        while !self.at_end() {
            self.skip_semicolons();
            if self.at_end() {
                break;
            }
            match self.parse_statement() {
                Ok(stmt) => stmts.push(stmt),
                Err(e) => {
                    self.errors.push(e);
                    self.synchronize();
                }
            }
            self.skip_semicolons();
        }
        ParseResult {
            statements: stmts,
            errors: self.errors,
        }
    }

    // -------------------------------------------------------------------
    // Token helpers
    // -------------------------------------------------------------------

    fn at_end(&self) -> bool {
        self.peek().kind == TokenKind::Eof
    }

    fn peek(&self) -> &Token {
        &self.tokens[self.pos]
    }

    fn peek_ahead(&self, n: usize) -> &Token {
        let idx = self.pos + n;
        if idx < self.tokens.len() {
            &self.tokens[idx]
        } else {
            self.tokens.last().unwrap() // Eof
        }
    }

    fn advance(&mut self) -> Token {
        let tok = self.tokens[self.pos];
        if self.pos + 1 < self.tokens.len() {
            self.pos += 1;
        }
        tok
    }

    fn check(&self, kind: TokenKind) -> bool {
        self.peek().kind == kind
    }

    fn eat(&mut self, kind: TokenKind) -> bool {
        if self.check(kind) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, kind: TokenKind) -> Result<Token, ParseError> {
        if self.check(kind) {
            Ok(self.advance())
        } else {
            let tok = self.peek();
            Err(ParseError {
                message: format!("expected {:?}, found {:?}", kind, tok.kind),
                line: tok.line,
                col: tok.col,
                offset: tok.span.start,
                expected: vec![format!("{:?}", kind)],
            })
        }
    }

    fn expect_identifier(&mut self) -> Result<String, ParseError> {
        let tok = self.peek();
        match tok.kind {
            TokenKind::Identifier => {
                let text = tok.span.text(self.source).to_string();
                self.advance();
                Ok(text)
            }
            TokenKind::QuotedIdentifier => {
                let raw = tok.span.text(self.source);
                // Strip surrounding quotes and unescape ""
                let inner = &raw[1..raw.len() - 1];
                let text = inner.replace("\"\"", "\"");
                self.advance();
                Ok(text)
            }
            _ => Err(ParseError {
                message: format!("expected identifier, found {:?}", tok.kind),
                line: tok.line,
                col: tok.col,
                offset: tok.span.start,
                expected: vec!["identifier".into()],
            }),
        }
    }

    /// Accept an identifier or certain keywords that can serve as identifiers.
    fn parse_name(&mut self) -> Result<String, ParseError> {
        let tok = self.peek();
        match tok.kind {
            TokenKind::Identifier | TokenKind::QuotedIdentifier => self.expect_identifier(),
            // Allow many keywords as names in non-reserved contexts
            kind if Self::is_unreserved_keyword(kind) => {
                let text = tok.span.text(self.source).to_lowercase();
                self.advance();
                Ok(text)
            }
            _ => self.expect_identifier(),
        }
    }

    fn is_unreserved_keyword(kind: TokenKind) -> bool {
        matches!(
            kind,
            TokenKind::KwKey
                | TokenKind::KwIndex
                | TokenKind::KwView
                | TokenKind::KwColumn
                | TokenKind::KwConstraint
                | TokenKind::KwCascade
                | TokenKind::KwRestrict
                | TokenKind::KwRename
                | TokenKind::KwTo
                | TokenKind::KwNo
                | TokenKind::KwFirst
                | TokenKind::KwLast
                | TokenKind::KwShow
                | TokenKind::KwCopy
                | TokenKind::KwDatabase
                | TokenKind::KwSchema
                | TokenKind::KwAnalyze
                | TokenKind::KwVacuum
                | TokenKind::KwSavepoint
                | TokenKind::KwRelease
                | TokenKind::KwReturning
                | TokenKind::KwOffset
                | TokenKind::KwLimit
        )
    }

    fn skip_semicolons(&mut self) {
        while self.check(TokenKind::Semicolon) {
            self.advance();
        }
    }

    fn error_at_current(&self, msg: &str) -> ParseError {
        let tok = self.peek();
        ParseError {
            message: msg.to_string(),
            line: tok.line,
            col: tok.col,
            offset: tok.span.start,
            expected: vec![],
        }
    }

    fn error_with_expected(&self, msg: &str, expected: Vec<String>) -> ParseError {
        let tok = self.peek();
        ParseError {
            message: msg.to_string(),
            line: tok.line,
            col: tok.col,
            offset: tok.span.start,
            expected,
        }
    }

    /// Error recovery: skip to next statement boundary.
    fn synchronize(&mut self) {
        loop {
            let kind = self.peek().kind;
            if kind == TokenKind::Eof || kind == TokenKind::Semicolon {
                break;
            }
            // Statement-starting keywords
            if matches!(
                kind,
                TokenKind::KwSelect
                    | TokenKind::KwInsert
                    | TokenKind::KwUpdate
                    | TokenKind::KwDelete
                    | TokenKind::KwCreate
                    | TokenKind::KwDrop
                    | TokenKind::KwAlter
                    | TokenKind::KwBegin
                    | TokenKind::KwCommit
                    | TokenKind::KwRollback
                    | TokenKind::KwSavepoint
                    | TokenKind::KwExplain
                    | TokenKind::KwShow
                    | TokenKind::KwAnalyze
                    | TokenKind::KwVacuum
                    | TokenKind::KwCopy
            ) {
                break;
            }
            self.advance();
        }
    }

    // ===================================================================
    // Statement dispatch
    // ===================================================================

    fn parse_statement(&mut self) -> Result<Statement, ParseError> {
        match self.peek().kind {
            TokenKind::KwSelect => self.parse_select_stmt(),
            TokenKind::KwInsert => self.parse_insert(),
            TokenKind::KwUpdate => self.parse_update(),
            TokenKind::KwDelete => self.parse_delete(),
            TokenKind::KwCreate => self.parse_create(),
            TokenKind::KwDrop => self.parse_drop(),
            TokenKind::KwAlter => self.parse_alter(),
            TokenKind::KwBegin => {
                self.advance();
                Ok(Statement::Begin)
            }
            TokenKind::KwCommit => {
                self.advance();
                Ok(Statement::Commit)
            }
            TokenKind::KwRollback => self.parse_rollback(),
            TokenKind::KwSavepoint => self.parse_savepoint(),
            TokenKind::KwExplain => self.parse_explain(),
            TokenKind::KwShow => self.parse_show(),
            TokenKind::KwAnalyze => self.parse_analyze_stmt(),
            TokenKind::KwVacuum => self.parse_vacuum(),
            TokenKind::KwCopy => self.parse_copy(),
            _ => {
                let tok = self.peek();
                // Common mistake detection
                if tok.kind == TokenKind::KwFrom {
                    Err(self.error_with_expected(
                        "unexpected FROM without SELECT — did you mean SELECT ... FROM?",
                        vec!["SELECT".into()],
                    ))
                } else if tok.kind == TokenKind::KwSet {
                    Err(self.error_with_expected(
                        "unexpected SET without UPDATE — did you mean UPDATE ... SET?",
                        vec!["UPDATE".into()],
                    ))
                } else if tok.kind == TokenKind::KwValues {
                    Err(self.error_with_expected(
                        "unexpected VALUES without INSERT — did you mean INSERT INTO ... VALUES?",
                        vec!["INSERT".into()],
                    ))
                } else {
                    Err(self.error_with_expected(
                        &format!("expected statement, found {:?}", tok.kind),
                        vec![
                            "SELECT".into(),
                            "INSERT".into(),
                            "UPDATE".into(),
                            "DELETE".into(),
                            "CREATE".into(),
                            "DROP".into(),
                        ],
                    ))
                }
            }
        }
    }

    // ===================================================================
    // SELECT
    // ===================================================================

    fn parse_select_stmt(&mut self) -> Result<Statement, ParseError> {
        let sel = self.parse_select()?;
        Ok(Statement::Select(sel))
    }

    fn parse_select(&mut self) -> Result<Select, ParseError> {
        self.expect(TokenKind::KwSelect)?;

        let distinct = self.eat(TokenKind::KwDistinct);
        if !distinct {
            self.eat(TokenKind::KwAll); // optional ALL
        }

        let columns = self.parse_select_columns()?;

        let from = if self.eat(TokenKind::KwFrom) {
            Some(self.parse_from_clause()?)
        } else {
            None
        };

        let where_clause = if self.eat(TokenKind::KwWhere) {
            Some(self.parse_expr(0)?)
        } else {
            None
        };

        let group_by = if self.check(TokenKind::KwGroup) {
            self.advance();
            self.expect(TokenKind::KwBy)?;
            self.parse_expr_list()?
        } else {
            vec![]
        };

        let having = if self.eat(TokenKind::KwHaving) {
            Some(self.parse_expr(0)?)
        } else {
            None
        };

        // Set operations (UNION / INTERSECT / EXCEPT) — parse with precedence
        let set_op = self.parse_set_operation()?;

        let order_by = if self.check(TokenKind::KwOrder) {
            self.advance();
            self.expect(TokenKind::KwBy)?;
            self.parse_order_by_list()?
        } else {
            vec![]
        };

        let limit = if self.eat(TokenKind::KwLimit) {
            Some(self.parse_expr(0)?)
        } else {
            None
        };

        let offset = if self.eat(TokenKind::KwOffset) {
            Some(self.parse_expr(0)?)
        } else {
            None
        };

        Ok(Select {
            distinct,
            columns,
            from,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            set_op,
        })
    }

    fn parse_select_columns(&mut self) -> Result<Vec<SelectColumn>, ParseError> {
        let mut cols = vec![self.parse_select_column()?];
        while self.eat(TokenKind::Comma) {
            cols.push(self.parse_select_column()?);
        }
        Ok(cols)
    }

    fn parse_select_column(&mut self) -> Result<SelectColumn, ParseError> {
        // Check for * or table.*
        if self.check(TokenKind::Star) {
            self.advance();
            return Ok(SelectColumn::AllColumns);
        }

        // Check for table.*
        if self.check(TokenKind::Identifier) || self.check(TokenKind::QuotedIdentifier) {
            if self.peek_ahead(1).kind == TokenKind::Dot
                && self.peek_ahead(2).kind == TokenKind::Star
            {
                let name = self.expect_identifier()?;
                self.advance(); // .
                self.advance(); // *
                return Ok(SelectColumn::TableAllColumns(name));
            }
        }

        let expr = self.parse_expr(0)?;
        let alias = self.parse_optional_alias()?;
        Ok(SelectColumn::Expr { expr, alias })
    }

    fn parse_optional_alias(&mut self) -> Result<Option<String>, ParseError> {
        if self.eat(TokenKind::KwAs) {
            Ok(Some(self.parse_name()?))
        } else if self.check(TokenKind::Identifier) || self.check(TokenKind::QuotedIdentifier) {
            // Implicit alias (no AS keyword) but only if it looks like a name
            // Don't consume keywords that start new clauses
            Ok(Some(self.parse_name()?))
        } else if Self::is_unreserved_keyword(self.peek().kind) && self.peek().kind != TokenKind::KwOffset && self.peek().kind != TokenKind::KwLimit {
            Ok(Some(self.parse_name()?))
        } else {
            Ok(None)
        }
    }

    // -------------------------------------------------------------------
    // FROM clause
    // -------------------------------------------------------------------

    fn parse_from_clause(&mut self) -> Result<FromClause, ParseError> {
        let table = self.parse_table_ref()?;
        let mut joins = Vec::new();
        loop {
            if let Some(join) = self.try_parse_join()? {
                joins.push(join);
            } else {
                break;
            }
        }
        Ok(FromClause { table, joins })
    }

    fn parse_table_ref(&mut self) -> Result<TableRef, ParseError> {
        if self.check(TokenKind::LeftParen) {
            // Subquery: (SELECT ...) AS alias
            self.advance();
            let query = self.parse_select()?;
            self.expect(TokenKind::RightParen)?;
            self.eat(TokenKind::KwAs);
            let alias = self.parse_name()?;
            Ok(TableRef::Subquery {
                query: Box::new(query),
                alias,
            })
        } else {
            let name = self.parse_name()?;
            let alias = if self.eat(TokenKind::KwAs) {
                Some(self.parse_name()?)
            } else if self.check(TokenKind::Identifier) || self.check(TokenKind::QuotedIdentifier) {
                // Implicit alias — but not if it looks like a keyword that starts a clause
                Some(self.parse_name()?)
            } else {
                None
            };
            Ok(TableRef::Table { name, alias })
        }
    }

    fn try_parse_join(&mut self) -> Result<Option<Join>, ParseError> {
        let mut natural = false;
        let mut join_type = None;

        if self.eat(TokenKind::KwNatural) {
            natural = true;
        }

        if self.eat(TokenKind::KwCross) {
            self.expect(TokenKind::KwJoin)?;
            join_type = Some(JoinType::Cross);
        } else if self.eat(TokenKind::KwInner) {
            self.expect(TokenKind::KwJoin)?;
            join_type = Some(JoinType::Inner);
        } else if self.eat(TokenKind::KwLeft) {
            self.eat(TokenKind::KwOuter);
            self.expect(TokenKind::KwJoin)?;
            join_type = Some(JoinType::Left);
        } else if self.eat(TokenKind::KwRight) {
            self.eat(TokenKind::KwOuter);
            self.expect(TokenKind::KwJoin)?;
            join_type = Some(JoinType::Right);
        } else if self.eat(TokenKind::KwFull) {
            self.eat(TokenKind::KwOuter);
            self.expect(TokenKind::KwJoin)?;
            join_type = Some(JoinType::Full);
        } else if self.eat(TokenKind::KwJoin) {
            join_type = Some(JoinType::Inner);
        }

        let jt = match join_type {
            Some(jt) => jt,
            None => {
                if natural {
                    return Err(self.error_at_current("expected JOIN after NATURAL"));
                }
                return Ok(None);
            }
        };

        let table = self.parse_table_ref()?;

        let condition = if self.eat(TokenKind::KwOn) {
            Some(JoinCondition::On(self.parse_expr(0)?))
        } else if self.eat(TokenKind::KwUsing) {
            self.expect(TokenKind::LeftParen)?;
            let cols = self.parse_name_list()?;
            self.expect(TokenKind::RightParen)?;
            Some(JoinCondition::Using(cols))
        } else {
            None
        };

        Ok(Some(Join {
            join_type: jt,
            natural,
            table,
            condition,
        }))
    }

    // -------------------------------------------------------------------
    // ORDER BY
    // -------------------------------------------------------------------

    fn parse_order_by_list(&mut self) -> Result<Vec<OrderByItem>, ParseError> {
        let mut items = vec![self.parse_order_by_item()?];
        while self.eat(TokenKind::Comma) {
            items.push(self.parse_order_by_item()?);
        }
        Ok(items)
    }

    fn parse_order_by_item(&mut self) -> Result<OrderByItem, ParseError> {
        let expr = self.parse_expr(0)?;
        let direction = if self.eat(TokenKind::KwAsc) {
            Some(OrderDirection::Asc)
        } else if self.eat(TokenKind::KwDesc) {
            Some(OrderDirection::Desc)
        } else {
            None
        };
        let nulls = if self.eat(TokenKind::KwNulls) {
            if self.eat(TokenKind::KwFirst) {
                Some(NullsOrder::First)
            } else if self.eat(TokenKind::KwLast) {
                Some(NullsOrder::Last)
            } else {
                return Err(self.error_with_expected(
                    "expected FIRST or LAST after NULLS",
                    vec!["FIRST".into(), "LAST".into()],
                ));
            }
        } else {
            None
        };
        Ok(OrderByItem {
            expr,
            direction,
            nulls,
        })
    }

    // -------------------------------------------------------------------
    // Set operations
    // -------------------------------------------------------------------

    fn parse_set_operation(&mut self) -> Result<Option<Box<SetOperation>>, ParseError> {
        let op = if self.check(TokenKind::KwUnion) {
            self.advance();
            SetOperator::Union
        } else if self.check(TokenKind::KwIntersect) {
            self.advance();
            SetOperator::Intersect
        } else if self.check(TokenKind::KwExcept) {
            self.advance();
            SetOperator::Except
        } else {
            return Ok(None);
        };

        let all = self.eat(TokenKind::KwAll);
        if !all {
            self.eat(TokenKind::KwDistinct); // optional DISTINCT (default)
        }

        let right = self.parse_select()?;
        Ok(Some(Box::new(SetOperation { op, all, right })))
    }

    // ===================================================================
    // INSERT
    // ===================================================================

    fn parse_insert(&mut self) -> Result<Statement, ParseError> {
        self.expect(TokenKind::KwInsert)?;
        self.expect(TokenKind::KwInto)?;
        let table = self.parse_name()?;

        let columns = if self.check(TokenKind::LeftParen) {
            // Could be column list or VALUES — peek to distinguish
            if self.peek_ahead(1).kind == TokenKind::KwSelect {
                None // It's INSERT INTO t (SELECT ...)
            } else {
                self.advance(); // (
                let cols = self.parse_name_list()?;
                self.expect(TokenKind::RightParen)?;
                Some(cols)
            }
        } else {
            None
        };

        let source = if self.eat(TokenKind::KwValues) {
            let mut rows = vec![self.parse_value_row()?];
            while self.eat(TokenKind::Comma) {
                rows.push(self.parse_value_row()?);
            }
            InsertSource::Values(rows)
        } else if self.check(TokenKind::KwSelect) {
            InsertSource::Select(Box::new(self.parse_select()?))
        } else if self.check(TokenKind::LeftParen) && self.peek_ahead(1).kind == TokenKind::KwSelect
        {
            self.advance(); // (
            let sel = self.parse_select()?;
            self.expect(TokenKind::RightParen)?;
            InsertSource::Select(Box::new(sel))
        } else {
            return Err(self.error_with_expected(
                "expected VALUES or SELECT",
                vec!["VALUES".into(), "SELECT".into()],
            ));
        };

        let returning = self.parse_returning()?;

        Ok(Statement::Insert(Insert {
            table,
            columns,
            source,
            returning,
        }))
    }

    fn parse_value_row(&mut self) -> Result<Vec<Expr>, ParseError> {
        self.expect(TokenKind::LeftParen)?;
        let exprs = self.parse_expr_list()?;
        self.expect(TokenKind::RightParen)?;
        Ok(exprs)
    }

    fn parse_returning(&mut self) -> Result<Option<Vec<SelectColumn>>, ParseError> {
        if self.eat(TokenKind::KwReturning) {
            Ok(Some(self.parse_select_columns()?))
        } else {
            Ok(None)
        }
    }

    // ===================================================================
    // UPDATE
    // ===================================================================

    fn parse_update(&mut self) -> Result<Statement, ParseError> {
        self.expect(TokenKind::KwUpdate)?;
        let table = self.parse_name()?;
        self.expect(TokenKind::KwSet)?;

        let mut assignments = vec![self.parse_assignment()?];
        while self.eat(TokenKind::Comma) {
            assignments.push(self.parse_assignment()?);
        }

        let where_clause = if self.eat(TokenKind::KwWhere) {
            Some(self.parse_expr(0)?)
        } else {
            None
        };

        let returning = self.parse_returning()?;

        Ok(Statement::Update(Update {
            table,
            assignments,
            where_clause,
            returning,
        }))
    }

    fn parse_assignment(&mut self) -> Result<Assignment, ParseError> {
        let column = self.parse_name()?;
        self.expect(TokenKind::Eq)?;
        let value = self.parse_expr(0)?;
        Ok(Assignment { column, value })
    }

    // ===================================================================
    // DELETE
    // ===================================================================

    fn parse_delete(&mut self) -> Result<Statement, ParseError> {
        self.expect(TokenKind::KwDelete)?;
        self.expect(TokenKind::KwFrom)?;
        let table = self.parse_name()?;

        let where_clause = if self.eat(TokenKind::KwWhere) {
            Some(self.parse_expr(0)?)
        } else {
            None
        };

        let returning = self.parse_returning()?;

        Ok(Statement::Delete(Delete {
            table,
            where_clause,
            returning,
        }))
    }

    // ===================================================================
    // CREATE
    // ===================================================================

    fn parse_create(&mut self) -> Result<Statement, ParseError> {
        self.expect(TokenKind::KwCreate)?;

        if self.eat(TokenKind::KwUnique) {
            // CREATE UNIQUE INDEX
            return self.parse_create_index(true);
        }

        match self.peek().kind {
            TokenKind::KwTable => self.parse_create_table(),
            TokenKind::KwIndex => self.parse_create_index(false),
            TokenKind::KwView => self.parse_create_view(),
            _ => Err(self.error_with_expected(
                "expected TABLE, INDEX, VIEW, or UNIQUE after CREATE",
                vec!["TABLE".into(), "INDEX".into(), "VIEW".into(), "UNIQUE".into()],
            )),
        }
    }

    fn parse_create_table(&mut self) -> Result<Statement, ParseError> {
        self.expect(TokenKind::KwTable)?;

        let if_not_exists = if self.eat(TokenKind::KwIf) {
            self.expect(TokenKind::KwNot)?;
            self.expect(TokenKind::KwExists)?;
            true
        } else {
            false
        };

        let name = self.parse_name()?;
        self.expect(TokenKind::LeftParen)?;

        let mut columns = Vec::new();
        let mut constraints = Vec::new();

        loop {
            if self.check(TokenKind::RightParen) {
                break;
            }

            // Table constraint starts with CONSTRAINT, PRIMARY, UNIQUE, CHECK, FOREIGN
            if self.check(TokenKind::KwConstraint)
                || self.check(TokenKind::KwPrimary)
                || self.check(TokenKind::KwUnique)
                || self.check(TokenKind::KwCheck)
                || self.check(TokenKind::KwForeign)
            {
                constraints.push(self.parse_table_constraint()?);
            } else {
                columns.push(self.parse_column_def()?);
            }

            if !self.eat(TokenKind::Comma) {
                break;
            }
        }

        self.expect(TokenKind::RightParen)?;

        Ok(Statement::CreateTable(CreateTable {
            if_not_exists,
            name,
            columns,
            constraints,
        }))
    }

    fn parse_column_def(&mut self) -> Result<ColumnDef, ParseError> {
        let name = self.parse_name()?;
        let data_type = self.parse_data_type()?;
        let mut constraints = Vec::new();

        loop {
            if let Some(c) = self.try_parse_column_constraint()? {
                constraints.push(c);
            } else {
                break;
            }
        }

        Ok(ColumnDef {
            name,
            data_type,
            constraints,
        })
    }

    fn try_parse_column_constraint(&mut self) -> Result<Option<ColumnConstraint>, ParseError> {
        if self.check(TokenKind::KwNot) && self.peek_ahead(1).kind == TokenKind::NullLiteral {
            self.advance(); // NOT
            self.advance(); // NULL
            return Ok(Some(ColumnConstraint::NotNull));
        }
        if self.eat(TokenKind::NullLiteral) {
            return Ok(Some(ColumnConstraint::Null));
        }
        if self.eat(TokenKind::KwDefault) {
            let expr = self.parse_expr(0)?;
            return Ok(Some(ColumnConstraint::Default(expr)));
        }
        if self.check(TokenKind::KwPrimary) {
            self.advance();
            self.expect(TokenKind::KwKey)?;
            return Ok(Some(ColumnConstraint::PrimaryKey));
        }
        if self.eat(TokenKind::KwUnique) {
            return Ok(Some(ColumnConstraint::Unique));
        }
        if self.eat(TokenKind::KwCheck) {
            self.expect(TokenKind::LeftParen)?;
            let expr = self.parse_expr(0)?;
            self.expect(TokenKind::RightParen)?;
            return Ok(Some(ColumnConstraint::Check(expr)));
        }
        if self.eat(TokenKind::KwReferences) {
            let table = self.parse_name()?;
            let column = if self.eat(TokenKind::LeftParen) {
                let col = self.parse_name()?;
                self.expect(TokenKind::RightParen)?;
                Some(col)
            } else {
                None
            };
            let on_delete = self.parse_on_action(TokenKind::KwDelete)?;
            let on_update = self.parse_on_action(TokenKind::KwUpdate)?;
            return Ok(Some(ColumnConstraint::References {
                table,
                column,
                on_delete,
                on_update,
            }));
        }
        Ok(None)
    }

    fn parse_on_action(
        &mut self,
        trigger: TokenKind,
    ) -> Result<Option<ReferentialAction>, ParseError> {
        if self.check(TokenKind::KwOn) && self.peek_ahead(1).kind == trigger {
            self.advance(); // ON
            self.advance(); // DELETE or UPDATE
            let action = self.parse_referential_action()?;
            Ok(Some(action))
        } else {
            Ok(None)
        }
    }

    fn parse_referential_action(&mut self) -> Result<ReferentialAction, ParseError> {
        if self.eat(TokenKind::KwCascade) {
            Ok(ReferentialAction::Cascade)
        } else if self.eat(TokenKind::KwRestrict) {
            Ok(ReferentialAction::Restrict)
        } else if self.eat(TokenKind::KwSet) {
            if self.eat(TokenKind::NullLiteral) {
                Ok(ReferentialAction::SetNull)
            } else if self.eat(TokenKind::KwDefault) {
                Ok(ReferentialAction::SetDefault)
            } else {
                Err(self.error_with_expected(
                    "expected NULL or DEFAULT after SET",
                    vec!["NULL".into(), "DEFAULT".into()],
                ))
            }
        } else if self.eat(TokenKind::KwNo) {
            // NO ACTION — "action" may be an identifier
            let tok = self.peek();
            let text = tok.span.text(self.source).to_lowercase();
            if text == "action" {
                self.advance();
                Ok(ReferentialAction::NoAction)
            } else {
                Err(self.error_at_current("expected ACTION after NO"))
            }
        } else {
            Err(self.error_with_expected(
                "expected referential action",
                vec![
                    "CASCADE".into(),
                    "RESTRICT".into(),
                    "SET NULL".into(),
                    "SET DEFAULT".into(),
                    "NO ACTION".into(),
                ],
            ))
        }
    }

    fn parse_table_constraint(&mut self) -> Result<TableConstraint, ParseError> {
        let name = if self.eat(TokenKind::KwConstraint) {
            Some(self.parse_name()?)
        } else {
            None
        };

        if self.check(TokenKind::KwPrimary) {
            self.advance();
            self.expect(TokenKind::KwKey)?;
            self.expect(TokenKind::LeftParen)?;
            let columns = self.parse_name_list()?;
            self.expect(TokenKind::RightParen)?;
            Ok(TableConstraint::PrimaryKey { name, columns })
        } else if self.eat(TokenKind::KwUnique) {
            self.expect(TokenKind::LeftParen)?;
            let columns = self.parse_name_list()?;
            self.expect(TokenKind::RightParen)?;
            Ok(TableConstraint::Unique { name, columns })
        } else if self.eat(TokenKind::KwCheck) {
            self.expect(TokenKind::LeftParen)?;
            let expr = self.parse_expr(0)?;
            self.expect(TokenKind::RightParen)?;
            Ok(TableConstraint::Check { name, expr })
        } else if self.eat(TokenKind::KwForeign) {
            self.expect(TokenKind::KwKey)?;
            self.expect(TokenKind::LeftParen)?;
            let columns = self.parse_name_list()?;
            self.expect(TokenKind::RightParen)?;
            self.expect(TokenKind::KwReferences)?;
            let ref_table = self.parse_name()?;
            self.expect(TokenKind::LeftParen)?;
            let ref_columns = self.parse_name_list()?;
            self.expect(TokenKind::RightParen)?;
            let on_delete = self.parse_on_action(TokenKind::KwDelete)?;
            let on_update = self.parse_on_action(TokenKind::KwUpdate)?;
            Ok(TableConstraint::ForeignKey {
                name,
                columns,
                ref_table,
                ref_columns,
                on_delete,
                on_update,
            })
        } else {
            Err(self.error_with_expected(
                "expected PRIMARY KEY, UNIQUE, CHECK, or FOREIGN KEY",
                vec![
                    "PRIMARY KEY".into(),
                    "UNIQUE".into(),
                    "CHECK".into(),
                    "FOREIGN KEY".into(),
                ],
            ))
        }
    }

    fn parse_data_type(&mut self) -> Result<DataType, ParseError> {
        let tok = self.peek();
        let text = tok.span.text(self.source).to_lowercase();
        match text.as_str() {
            "integer" | "int" => {
                self.advance();
                Ok(DataType::Integer)
            }
            "bigint" => {
                self.advance();
                Ok(DataType::BigInt)
            }
            "float" | "double" | "real" => {
                self.advance();
                Ok(DataType::Float)
            }
            "boolean" | "bool" => {
                self.advance();
                Ok(DataType::Boolean)
            }
            "varchar" => {
                self.advance();
                if self.eat(TokenKind::LeftParen) {
                    let n = self.parse_integer_literal()? as u32;
                    self.expect(TokenKind::RightParen)?;
                    Ok(DataType::Varchar(n))
                } else {
                    Ok(DataType::Varchar(255))
                }
            }
            "text" => {
                self.advance();
                Ok(DataType::Text)
            }
            "timestamp" => {
                self.advance();
                Ok(DataType::Timestamp)
            }
            "date" => {
                self.advance();
                Ok(DataType::Date)
            }
            "decimal" | "numeric" => {
                self.advance();
                if self.eat(TokenKind::LeftParen) {
                    let p = self.parse_integer_literal()? as u8;
                    self.expect(TokenKind::Comma)?;
                    let s = self.parse_integer_literal()? as u8;
                    self.expect(TokenKind::RightParen)?;
                    Ok(DataType::Decimal(p, s))
                } else {
                    Ok(DataType::Decimal(38, 0))
                }
            }
            "blob" | "bytea" => {
                self.advance();
                Ok(DataType::Blob)
            }
            _ => Err(self.error_with_expected(
                &format!("expected data type, found '{}'", text),
                vec![
                    "INTEGER".into(),
                    "VARCHAR".into(),
                    "TEXT".into(),
                    "BOOLEAN".into(),
                    "FLOAT".into(),
                    "TIMESTAMP".into(),
                ],
            )),
        }
    }

    fn parse_integer_literal(&mut self) -> Result<i64, ParseError> {
        let tok = self.peek();
        if tok.kind == TokenKind::IntegerLiteral {
            let text = tok.span.text(self.source);
            let val = text.parse::<i64>().map_err(|_| {
                self.error_at_current(&format!("invalid integer literal: {}", text))
            })?;
            self.advance();
            Ok(val)
        } else {
            Err(self.error_with_expected(
                "expected integer literal",
                vec!["integer".into()],
            ))
        }
    }

    // ===================================================================
    // DROP
    // ===================================================================

    fn parse_drop(&mut self) -> Result<Statement, ParseError> {
        self.expect(TokenKind::KwDrop)?;
        match self.peek().kind {
            TokenKind::KwTable => self.parse_drop_table(),
            TokenKind::KwIndex => self.parse_drop_index(),
            TokenKind::KwView => self.parse_drop_view(),
            _ => Err(self.error_with_expected(
                "expected TABLE, INDEX, or VIEW after DROP",
                vec!["TABLE".into(), "INDEX".into(), "VIEW".into()],
            )),
        }
    }

    fn parse_drop_table(&mut self) -> Result<Statement, ParseError> {
        self.expect(TokenKind::KwTable)?;
        let if_exists = if self.eat(TokenKind::KwIf) {
            self.expect(TokenKind::KwExists)?;
            true
        } else {
            false
        };
        let name = self.parse_name()?;
        let cascade = self.eat(TokenKind::KwCascade);
        if !cascade {
            self.eat(TokenKind::KwRestrict); // optional RESTRICT (default)
        }
        Ok(Statement::DropTable(DropTable {
            if_exists,
            name,
            cascade,
        }))
    }

    fn parse_drop_index(&mut self) -> Result<Statement, ParseError> {
        self.expect(TokenKind::KwIndex)?;
        let if_exists = if self.eat(TokenKind::KwIf) {
            self.expect(TokenKind::KwExists)?;
            true
        } else {
            false
        };
        let name = self.parse_name()?;
        Ok(Statement::DropIndex(DropIndex { if_exists, name }))
    }

    fn parse_drop_view(&mut self) -> Result<Statement, ParseError> {
        self.expect(TokenKind::KwView)?;
        let if_exists = if self.eat(TokenKind::KwIf) {
            self.expect(TokenKind::KwExists)?;
            true
        } else {
            false
        };
        let name = self.parse_name()?;
        Ok(Statement::DropView(DropView { if_exists, name }))
    }

    // ===================================================================
    // ALTER TABLE
    // ===================================================================

    fn parse_alter(&mut self) -> Result<Statement, ParseError> {
        self.expect(TokenKind::KwAlter)?;
        self.expect(TokenKind::KwTable)?;
        let name = self.parse_name()?;

        let action = if self.eat(TokenKind::KwAdd) {
            if self.check(TokenKind::KwColumn) {
                self.advance();
                AlterTableAction::AddColumn(self.parse_column_def()?)
            } else if self.check(TokenKind::KwConstraint)
                || self.check(TokenKind::KwPrimary)
                || self.check(TokenKind::KwUnique)
                || self.check(TokenKind::KwCheck)
                || self.check(TokenKind::KwForeign)
            {
                AlterTableAction::AddConstraint(self.parse_table_constraint()?)
            } else {
                // Default to ADD COLUMN if next looks like a column definition
                AlterTableAction::AddColumn(self.parse_column_def()?)
            }
        } else if self.eat(TokenKind::KwDrop) {
            if self.eat(TokenKind::KwColumn) {
                let col_name = self.parse_name()?;
                AlterTableAction::DropColumn { name: col_name }
            } else if self.eat(TokenKind::KwConstraint) {
                let cname = self.parse_name()?;
                AlterTableAction::DropConstraint { name: cname }
            } else {
                return Err(self.error_with_expected(
                    "expected COLUMN or CONSTRAINT after DROP",
                    vec!["COLUMN".into(), "CONSTRAINT".into()],
                ));
            }
        } else if self.eat(TokenKind::KwRename) {
            self.eat(TokenKind::KwColumn);
            let old_name = self.parse_name()?;
            self.expect(TokenKind::KwTo)?;
            let new_name = self.parse_name()?;
            AlterTableAction::RenameColumn { old_name, new_name }
        } else {
            return Err(self.error_with_expected(
                "expected ADD, DROP, or RENAME",
                vec!["ADD".into(), "DROP".into(), "RENAME".into()],
            ));
        };

        Ok(Statement::AlterTable(AlterTable { name, action }))
    }

    // ===================================================================
    // CREATE INDEX
    // ===================================================================

    fn parse_create_index(&mut self, unique: bool) -> Result<Statement, ParseError> {
        self.expect(TokenKind::KwIndex)?;
        let name = self.parse_name()?;
        self.expect(TokenKind::KwOn)?;
        let table = self.parse_name()?;

        // USING method (optional, before columns)
        let using_method = if self.eat(TokenKind::KwUsing) {
            let method_tok = self.peek();
            let method_text = method_tok.span.text(self.source).to_lowercase();
            match method_text.as_str() {
                "hash" => {
                    self.advance();
                    Some(IndexMethod::Hash)
                }
                "btree" => {
                    self.advance();
                    Some(IndexMethod::BTree)
                }
                _ => {
                    return Err(self.error_with_expected(
                        &format!("unknown index method: {}", method_text),
                        vec!["HASH".into(), "BTREE".into()],
                    ));
                }
            }
        } else {
            None
        };

        self.expect(TokenKind::LeftParen)?;
        let mut columns = vec![self.parse_index_column()?];
        while self.eat(TokenKind::Comma) {
            columns.push(self.parse_index_column()?);
        }
        self.expect(TokenKind::RightParen)?;

        Ok(Statement::CreateIndex(CreateIndex {
            unique,
            name,
            table,
            columns,
            using_method,
        }))
    }

    fn parse_index_column(&mut self) -> Result<IndexColumn, ParseError> {
        let name = self.parse_name()?;
        let direction = if self.eat(TokenKind::KwAsc) {
            Some(OrderDirection::Asc)
        } else if self.eat(TokenKind::KwDesc) {
            Some(OrderDirection::Desc)
        } else {
            None
        };
        Ok(IndexColumn { name, direction })
    }

    // ===================================================================
    // CREATE VIEW
    // ===================================================================

    fn parse_create_view(&mut self) -> Result<Statement, ParseError> {
        self.expect(TokenKind::KwView)?;
        let name = self.parse_name()?;

        let columns = if self.check(TokenKind::LeftParen) {
            self.advance();
            let cols = self.parse_name_list()?;
            self.expect(TokenKind::RightParen)?;
            Some(cols)
        } else {
            None
        };

        self.expect(TokenKind::KwAs)?;
        let query = self.parse_select()?;

        Ok(Statement::CreateView(CreateView {
            name,
            columns,
            query,
        }))
    }

    // ===================================================================
    // Transaction statements
    // ===================================================================

    fn parse_rollback(&mut self) -> Result<Statement, ParseError> {
        self.expect(TokenKind::KwRollback)?;
        let savepoint = if self.eat(TokenKind::KwTo) {
            self.eat(TokenKind::KwSavepoint); // optional SAVEPOINT keyword
            Some(self.parse_name()?)
        } else {
            None
        };
        Ok(Statement::Rollback { savepoint })
    }

    fn parse_savepoint(&mut self) -> Result<Statement, ParseError> {
        self.expect(TokenKind::KwSavepoint)?;
        let name = self.parse_name()?;
        Ok(Statement::Savepoint { name })
    }

    // ===================================================================
    // EXPLAIN
    // ===================================================================

    fn parse_explain(&mut self) -> Result<Statement, ParseError> {
        self.expect(TokenKind::KwExplain)?;
        let analyze = self.eat(TokenKind::KwAnalyze);
        let stmt = self.parse_statement()?;
        Ok(Statement::Explain {
            analyze,
            statement: Box::new(stmt),
        })
    }

    // ===================================================================
    // SHOW
    // ===================================================================

    fn parse_show(&mut self) -> Result<Statement, ParseError> {
        self.expect(TokenKind::KwShow)?;
        let tok = self.peek();
        let text = tok.span.text(self.source).to_lowercase();
        match text.as_str() {
            "tables" => {
                self.advance();
                Ok(Statement::ShowTables)
            }
            "columns" => {
                self.advance();
                self.expect(TokenKind::KwFrom)?;
                let table = self.parse_name()?;
                Ok(Statement::ShowColumns { table })
            }
            _ => Err(self.error_with_expected(
                "expected TABLES or COLUMNS after SHOW",
                vec!["TABLES".into(), "COLUMNS".into()],
            )),
        }
    }

    // ===================================================================
    // ANALYZE / VACUUM
    // ===================================================================

    fn parse_analyze_stmt(&mut self) -> Result<Statement, ParseError> {
        self.expect(TokenKind::KwAnalyze)?;
        let table = if !self.at_end()
            && !self.check(TokenKind::Semicolon)
            && !self.is_statement_start()
        {
            Some(self.parse_name()?)
        } else {
            None
        };
        Ok(Statement::Analyze { table })
    }

    fn parse_vacuum(&mut self) -> Result<Statement, ParseError> {
        self.expect(TokenKind::KwVacuum)?;
        let table = if !self.at_end()
            && !self.check(TokenKind::Semicolon)
            && !self.is_statement_start()
        {
            Some(self.parse_name()?)
        } else {
            None
        };
        Ok(Statement::Vacuum { table })
    }

    fn is_statement_start(&self) -> bool {
        matches!(
            self.peek().kind,
            TokenKind::KwSelect
                | TokenKind::KwInsert
                | TokenKind::KwUpdate
                | TokenKind::KwDelete
                | TokenKind::KwCreate
                | TokenKind::KwDrop
                | TokenKind::KwAlter
                | TokenKind::KwBegin
                | TokenKind::KwCommit
                | TokenKind::KwRollback
                | TokenKind::KwSavepoint
                | TokenKind::KwExplain
                | TokenKind::KwShow
                | TokenKind::KwAnalyze
                | TokenKind::KwVacuum
                | TokenKind::KwCopy
        )
    }

    // ===================================================================
    // COPY
    // ===================================================================

    fn parse_copy(&mut self) -> Result<Statement, ParseError> {
        self.expect(TokenKind::KwCopy)?;
        let table = self.parse_name()?;

        let columns = if self.check(TokenKind::LeftParen) {
            self.advance();
            let cols = self.parse_name_list()?;
            self.expect(TokenKind::RightParen)?;
            Some(cols)
        } else {
            None
        };

        let direction = if self.eat(TokenKind::KwFrom) {
            let path = self.parse_string_literal()?;
            CopyDirection::From(path)
        } else if self.eat(TokenKind::KwTo) {
            let path = self.parse_string_literal()?;
            CopyDirection::To(path)
        } else {
            return Err(self.error_with_expected(
                "expected FROM or TO",
                vec!["FROM".into(), "TO".into()],
            ));
        };

        Ok(Statement::Copy(Copy {
            table,
            columns,
            direction,
        }))
    }

    fn parse_string_literal(&mut self) -> Result<String, ParseError> {
        let tok = self.peek();
        if tok.kind == TokenKind::StringLiteral {
            let raw = tok.span.text(self.source);
            // Strip surrounding quotes and unescape ''
            let inner = &raw[1..raw.len() - 1];
            let text = inner.replace("''", "'");
            self.advance();
            Ok(text)
        } else {
            Err(self.error_with_expected(
                "expected string literal",
                vec!["string".into()],
            ))
        }
    }

    // -------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------

    fn parse_name_list(&mut self) -> Result<Vec<String>, ParseError> {
        let mut names = vec![self.parse_name()?];
        while self.eat(TokenKind::Comma) {
            names.push(self.parse_name()?);
        }
        Ok(names)
    }

    fn parse_expr_list(&mut self) -> Result<Vec<Expr>, ParseError> {
        let mut exprs = vec![self.parse_expr(0)?];
        while self.eat(TokenKind::Comma) {
            exprs.push(self.parse_expr(0)?);
        }
        Ok(exprs)
    }

    // ===================================================================
    // Pratt expression parser
    // ===================================================================

    pub fn parse_expr(&mut self, min_bp: u8) -> Result<Expr, ParseError> {
        // ---- prefix / nud ----
        let mut lhs = self.parse_prefix()?;

        // ---- infix / led loop ----
        loop {
            let kind = self.peek().kind;

            // Postfix-like: field access (.)
            if kind == TokenKind::Dot && BP_FIELD >= min_bp {
                self.advance(); // .
                if self.check(TokenKind::Star) {
                    self.advance();
                    if let Expr::Identifier(table) = lhs {
                        lhs = Expr::QualifiedStar(table);
                    } else {
                        return Err(self.error_at_current("expected identifier before .*"));
                    }
                } else {
                    let col = self.expect_identifier()?;
                    if let Expr::Identifier(table) = lhs {
                        lhs = Expr::QualifiedIdentifier { table, column: col };
                    } else {
                        return Err(
                            self.error_at_current("expected identifier before .column"),
                        );
                    }
                }
                continue;
            }

            // Postfix-like: function call — only when LHS is Identifier
            if kind == TokenKind::LeftParen && BP_CALL >= min_bp {
                if let Expr::Identifier(ref _name) = lhs {
                    let name = if let Expr::Identifier(n) = lhs {
                        n
                    } else {
                        unreachable!()
                    };
                    self.advance(); // (

                    // Special case: func(*)
                    if self.check(TokenKind::Star) {
                        self.advance();
                        self.expect(TokenKind::RightParen)?;
                        lhs = Expr::FunctionCall {
                            name,
                            args: vec![Expr::Star],
                            distinct: false,
                        };
                        continue;
                    }

                    // func() with no args
                    if self.check(TokenKind::RightParen) {
                        self.advance();
                        lhs = Expr::FunctionCall {
                            name,
                            args: vec![],
                            distinct: false,
                        };
                        continue;
                    }

                    let distinct = self.eat(TokenKind::KwDistinct);
                    let args = self.parse_expr_list()?;
                    self.expect(TokenKind::RightParen)?;
                    lhs = Expr::FunctionCall {
                        name,
                        args,
                        distinct,
                    };
                    continue;
                }
            }

            // TypeCast ::
            if kind == TokenKind::ColonColon && BP_CAST >= min_bp {
                self.advance();
                let data_type = self.parse_data_type()?;
                lhs = Expr::Cast {
                    expr: Box::new(lhs),
                    data_type,
                };
                continue;
            }

            // IS [NOT] NULL / IS [NOT] TRUE / IS [NOT] FALSE
            if kind == TokenKind::KwIs && BP_IS >= min_bp {
                self.advance(); // IS
                let negated = self.eat(TokenKind::KwNot);
                if self.eat(TokenKind::NullLiteral) {
                    lhs = Expr::IsNull {
                        expr: Box::new(lhs),
                        negated,
                    };
                    continue;
                }
                // IS [NOT] TRUE / FALSE — model as comparison with boolean
                let tok = self.peek();
                if tok.kind == TokenKind::BooleanLiteral {
                    let val = tok.span.text(self.source).to_lowercase() == "true";
                    self.advance();
                    let bool_expr = Expr::Boolean(val);
                    let cmp = if negated {
                        BinaryOp::NotEq
                    } else {
                        BinaryOp::Eq
                    };
                    lhs = Expr::BinaryOp {
                        left: Box::new(lhs),
                        op: cmp,
                        right: Box::new(bool_expr),
                    };
                    continue;
                }
                return Err(self.error_with_expected(
                    "expected NULL, TRUE, or FALSE after IS [NOT]",
                    vec!["NULL".into(), "TRUE".into(), "FALSE".into()],
                ));
            }

            // NOT IN / NOT BETWEEN / NOT LIKE — negated forms
            if kind == TokenKind::KwNot {
                let next = self.peek_ahead(1).kind;
                if matches!(
                    next,
                    TokenKind::KwIn | TokenKind::KwBetween | TokenKind::KwLike | TokenKind::KwIlike
                ) && BP_RANGE >= min_bp
                {
                    self.advance(); // NOT
                    match self.peek().kind {
                        TokenKind::KwIn => {
                            self.advance();
                            lhs = self.parse_in_expr(lhs, true)?;
                        }
                        TokenKind::KwBetween => {
                            self.advance();
                            lhs = self.parse_between_expr(lhs, true)?;
                        }
                        TokenKind::KwLike => {
                            self.advance();
                            let pattern = self.parse_expr(BP_RANGE + 1)?;
                            lhs = Expr::Like {
                                expr: Box::new(lhs),
                                pattern: Box::new(pattern),
                                negated: true,
                                case_insensitive: false,
                            };
                        }
                        TokenKind::KwIlike => {
                            self.advance();
                            let pattern = self.parse_expr(BP_RANGE + 1)?;
                            lhs = Expr::Like {
                                expr: Box::new(lhs),
                                pattern: Box::new(pattern),
                                negated: true,
                                case_insensitive: true,
                            };
                        }
                        _ => unreachable!(),
                    }
                    continue;
                }
            }

            // IN
            if kind == TokenKind::KwIn && BP_RANGE >= min_bp {
                self.advance();
                lhs = self.parse_in_expr(lhs, false)?;
                continue;
            }

            // BETWEEN
            if kind == TokenKind::KwBetween && BP_RANGE >= min_bp {
                self.advance();
                lhs = self.parse_between_expr(lhs, false)?;
                continue;
            }

            // LIKE / ILIKE
            if (kind == TokenKind::KwLike || kind == TokenKind::KwIlike) && BP_RANGE >= min_bp {
                let case_insensitive = kind == TokenKind::KwIlike;
                self.advance();
                let pattern = self.parse_expr(BP_RANGE + 1)?;
                lhs = Expr::Like {
                    expr: Box::new(lhs),
                    pattern: Box::new(pattern),
                    negated: false,
                    case_insensitive,
                };
                continue;
            }

            // Standard binary infix operators
            if let Some((l_bp, r_bp)) = Self::infix_binding_power(kind) {
                if l_bp < min_bp {
                    break;
                }
                self.advance();
                let op = Self::token_to_binary_op(kind);
                let rhs = self.parse_expr(r_bp)?;
                lhs = Expr::BinaryOp {
                    left: Box::new(lhs),
                    op,
                    right: Box::new(rhs),
                };
                continue;
            }

            break;
        }

        Ok(lhs)
    }

    // -------------------------------------------------------------------
    // Prefix (nud)
    // -------------------------------------------------------------------

    fn parse_prefix(&mut self) -> Result<Expr, ParseError> {
        let tok = self.peek();
        match tok.kind {
            // Literals
            TokenKind::IntegerLiteral => {
                let text = tok.span.text(self.source);
                let val = if text.starts_with("0x") || text.starts_with("0X") {
                    i64::from_str_radix(&text[2..], 16).map_err(|_| {
                        self.error_at_current(&format!("invalid hex literal: {}", text))
                    })?
                } else {
                    text.parse::<i64>().map_err(|_| {
                        self.error_at_current(&format!("invalid integer literal: {}", text))
                    })?
                };
                self.advance();
                Ok(Expr::Integer(val))
            }
            TokenKind::FloatLiteral => {
                let text = tok.span.text(self.source);
                let val = text.parse::<f64>().map_err(|_| {
                    self.error_at_current(&format!("invalid float literal: {}", text))
                })?;
                self.advance();
                Ok(Expr::Float(val))
            }
            TokenKind::StringLiteral => {
                let s = self.parse_string_literal()?;
                Ok(Expr::String(s))
            }
            TokenKind::BooleanLiteral => {
                let val = tok.span.text(self.source).to_lowercase() == "true";
                self.advance();
                Ok(Expr::Boolean(val))
            }
            TokenKind::NullLiteral => {
                self.advance();
                Ok(Expr::Null)
            }

            // Placeholder
            TokenKind::Placeholder => {
                let text = tok.span.text(self.source);
                let n = text[1..].parse::<u32>().map_err(|_| {
                    self.error_at_current(&format!("invalid placeholder: {}", text))
                })?;
                self.advance();
                Ok(Expr::Placeholder(n))
            }

            // Star
            TokenKind::Star => {
                self.advance();
                Ok(Expr::Star)
            }

            // Identifier (may be followed by . or ()
            TokenKind::Identifier | TokenKind::QuotedIdentifier => {
                let name = self.expect_identifier()?;
                Ok(Expr::Identifier(name))
            }

            // Grouped expression or subquery
            TokenKind::LeftParen => {
                self.advance();
                if self.check(TokenKind::KwSelect) {
                    let sel = self.parse_select()?;
                    self.expect(TokenKind::RightParen)?;
                    Ok(Expr::Subquery(Box::new(sel)))
                } else {
                    let expr = self.parse_expr(0)?;
                    self.expect(TokenKind::RightParen)?;
                    Ok(expr)
                }
            }

            // Prefix NOT
            TokenKind::KwNot => {
                self.advance();
                // NOT EXISTS
                if self.check(TokenKind::KwExists) {
                    self.advance();
                    self.expect(TokenKind::LeftParen)?;
                    let sel = self.parse_select()?;
                    self.expect(TokenKind::RightParen)?;
                    return Ok(Expr::Exists {
                        subquery: Box::new(sel),
                        negated: true,
                    });
                }
                let expr = self.parse_expr(BP_NOT)?;
                Ok(Expr::UnaryOp {
                    op: UnaryOp::Not,
                    expr: Box::new(expr),
                })
            }

            // Unary + / -
            TokenKind::Plus => {
                self.advance();
                let expr = self.parse_expr(BP_UNARY)?;
                Ok(Expr::UnaryOp {
                    op: UnaryOp::Plus,
                    expr: Box::new(expr),
                })
            }
            TokenKind::Minus => {
                self.advance();
                let expr = self.parse_expr(BP_UNARY)?;
                Ok(Expr::UnaryOp {
                    op: UnaryOp::Minus,
                    expr: Box::new(expr),
                })
            }

            // EXISTS
            TokenKind::KwExists => {
                self.advance();
                self.expect(TokenKind::LeftParen)?;
                let sel = self.parse_select()?;
                self.expect(TokenKind::RightParen)?;
                Ok(Expr::Exists {
                    subquery: Box::new(sel),
                    negated: false,
                })
            }

            // CASE
            TokenKind::KwCase => self.parse_case_expr(),

            // CAST(expr AS type)
            TokenKind::KwCast => {
                self.advance();
                self.expect(TokenKind::LeftParen)?;
                let expr = self.parse_expr(0)?;
                self.expect(TokenKind::KwAs)?;
                let data_type = self.parse_data_type()?;
                self.expect(TokenKind::RightParen)?;
                Ok(Expr::Cast {
                    expr: Box::new(expr),
                    data_type,
                })
            }

            // COALESCE
            TokenKind::KwCoalesce => {
                self.advance();
                self.expect(TokenKind::LeftParen)?;
                let args = self.parse_expr_list()?;
                self.expect(TokenKind::RightParen)?;
                Ok(Expr::Coalesce(args))
            }

            // NULLIF
            TokenKind::KwNullif => {
                self.advance();
                self.expect(TokenKind::LeftParen)?;
                let a = self.parse_expr(0)?;
                self.expect(TokenKind::Comma)?;
                let b = self.parse_expr(0)?;
                self.expect(TokenKind::RightParen)?;
                Ok(Expr::Nullif(Box::new(a), Box::new(b)))
            }

            // GREATEST
            TokenKind::KwGreatest => {
                self.advance();
                self.expect(TokenKind::LeftParen)?;
                let args = self.parse_expr_list()?;
                self.expect(TokenKind::RightParen)?;
                Ok(Expr::Greatest(args))
            }

            // LEAST
            TokenKind::KwLeast => {
                self.advance();
                self.expect(TokenKind::LeftParen)?;
                let args = self.parse_expr_list()?;
                self.expect(TokenKind::RightParen)?;
                Ok(Expr::Least(args))
            }

            // Unreserved keywords can be used as identifiers in expressions
            kind if Self::is_unreserved_keyword(kind) => {
                let text = tok.span.text(self.source).to_lowercase();
                self.advance();
                Ok(Expr::Identifier(text))
            }

            _ => Err(self.error_with_expected(
                &format!("expected expression, found {:?}", tok.kind),
                vec![
                    "literal".into(),
                    "identifier".into(),
                    "(".into(),
                    "NOT".into(),
                    "CASE".into(),
                ],
            )),
        }
    }

    // -------------------------------------------------------------------
    // Special infix expression helpers
    // -------------------------------------------------------------------

    fn parse_in_expr(&mut self, lhs: Expr, negated: bool) -> Result<Expr, ParseError> {
        self.expect(TokenKind::LeftParen)?;
        if self.check(TokenKind::KwSelect) {
            let sel = self.parse_select()?;
            self.expect(TokenKind::RightParen)?;
            Ok(Expr::InSubquery {
                expr: Box::new(lhs),
                subquery: Box::new(sel),
                negated,
            })
        } else {
            let list = self.parse_expr_list()?;
            self.expect(TokenKind::RightParen)?;
            Ok(Expr::InList {
                expr: Box::new(lhs),
                list,
                negated,
            })
        }
    }

    fn parse_between_expr(&mut self, lhs: Expr, negated: bool) -> Result<Expr, ParseError> {
        let low = self.parse_expr(BP_RANGE + 1)?;
        self.expect(TokenKind::KwAnd)?;
        let high = self.parse_expr(BP_RANGE + 1)?;
        Ok(Expr::Between {
            expr: Box::new(lhs),
            low: Box::new(low),
            high: Box::new(high),
            negated,
        })
    }

    fn parse_case_expr(&mut self) -> Result<Expr, ParseError> {
        self.expect(TokenKind::KwCase)?;

        // Simple CASE (CASE expr WHEN ...) vs searched CASE (CASE WHEN ...)
        let operand = if !self.check(TokenKind::KwWhen) {
            Some(Box::new(self.parse_expr(0)?))
        } else {
            None
        };

        let mut when_clauses = Vec::new();
        while self.eat(TokenKind::KwWhen) {
            let condition = self.parse_expr(0)?;
            self.expect(TokenKind::KwThen)?;
            let result = self.parse_expr(0)?;
            when_clauses.push(WhenClause { condition, result });
        }

        let else_clause = if self.eat(TokenKind::KwElse) {
            Some(Box::new(self.parse_expr(0)?))
        } else {
            None
        };

        self.expect(TokenKind::KwEnd)?;

        Ok(Expr::Case {
            operand,
            when_clauses,
            else_clause,
        })
    }

    // -------------------------------------------------------------------
    // Binding power tables
    // -------------------------------------------------------------------

    /// Returns (left_bp, right_bp) for standard binary infix operators.
    fn infix_binding_power(kind: TokenKind) -> Option<(u8, u8)> {
        match kind {
            // Logical
            TokenKind::KwOr => Some((BP_OR, BP_OR + 1)),
            TokenKind::KwAnd => Some((BP_AND, BP_AND + 1)),

            // Comparison
            TokenKind::Eq
            | TokenKind::NotEq
            | TokenKind::Lt
            | TokenKind::Gt
            | TokenKind::LtEq
            | TokenKind::GtEq => Some((BP_COMPARISON, BP_COMPARISON + 1)),

            // Concatenation
            TokenKind::Concat => Some((BP_CONCAT, BP_CONCAT + 1)),

            // Additive
            TokenKind::Plus | TokenKind::Minus => Some((BP_ADD, BP_ADD + 1)),

            // Multiplicative
            TokenKind::Star | TokenKind::Slash | TokenKind::Percent => {
                Some((BP_MUL, BP_MUL + 1))
            }

            // Exponentiation (right-associative)
            TokenKind::Caret => Some((BP_EXP, BP_EXP)),

            _ => None,
        }
    }

    fn token_to_binary_op(kind: TokenKind) -> BinaryOp {
        match kind {
            TokenKind::Plus => BinaryOp::Add,
            TokenKind::Minus => BinaryOp::Sub,
            TokenKind::Star => BinaryOp::Mul,
            TokenKind::Slash => BinaryOp::Div,
            TokenKind::Percent => BinaryOp::Mod,
            TokenKind::Caret => BinaryOp::Exp,
            TokenKind::Concat => BinaryOp::Concat,
            TokenKind::Eq => BinaryOp::Eq,
            TokenKind::NotEq => BinaryOp::NotEq,
            TokenKind::Lt => BinaryOp::Lt,
            TokenKind::Gt => BinaryOp::Gt,
            TokenKind::LtEq => BinaryOp::LtEq,
            TokenKind::GtEq => BinaryOp::GtEq,
            TokenKind::KwAnd => BinaryOp::And,
            TokenKind::KwOr => BinaryOp::Or,
            _ => unreachable!("not a binary op: {:?}", kind),
        }
    }
}
