use crate::sql::error::{LexerError, DEFAULT_MAX_ERRORS};
use crate::sql::token::{lookup_keyword, Span, Token, TokenKind};

// ---------------------------------------------------------------------------
// Lexer output
// ---------------------------------------------------------------------------

/// Result of tokenising a SQL source string.
pub struct LexResult {
    /// Token stream, always terminated by [`TokenKind::Eof`].
    pub tokens: Vec<Token>,
    /// Errors collected during lexing (up to `max_errors`).
    pub errors: Vec<LexerError>,
}

impl LexResult {
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Public convenience functions
// ---------------------------------------------------------------------------

pub fn tokenize(source: &str) -> LexResult {
    Lexer::new(source).tokenize_all()
}

pub fn tokenize_with_limit(source: &str, max_errors: usize) -> LexResult {
    Lexer::with_max_errors(source, max_errors).tokenize_all()
}

// ---------------------------------------------------------------------------
// Lexer
// ---------------------------------------------------------------------------

pub struct Lexer<'a> {
    source: &'a str,
    bytes: &'a [u8],
    pos: usize,
    line: usize,
    col: usize,
    errors: Vec<LexerError>,
    max_errors: usize,
}

impl<'a> Lexer<'a> {
    pub fn new(source: &'a str) -> Self {
        Self {
            source,
            bytes: source.as_bytes(),
            pos: 0,
            line: 1,
            col: 1,
            errors: Vec::new(),
            max_errors: DEFAULT_MAX_ERRORS,
        }
    }

    pub fn with_max_errors(source: &'a str, max_errors: usize) -> Self {
        let mut lex = Self::new(source);
        lex.max_errors = max_errors;
        lex
    }

    pub fn tokenize_all(mut self) -> LexResult {
        let mut tokens = Vec::new();
        loop {
            let tok = self.next_token();
            let is_eof = tok.kind == TokenKind::Eof;
            tokens.push(tok);
            if is_eof {
                break;
            }
        }
        LexResult {
            tokens,
            errors: self.errors,
        }
    }

    // -----------------------------------------------------------------------
    // Core helpers
    // -----------------------------------------------------------------------

    fn at_end(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    fn peek(&self) -> u8 {
        self.bytes[self.pos]
    }

    fn peek_at(&self, offset: usize) -> Option<u8> {
        self.bytes.get(self.pos + offset).copied()
    }

    fn advance(&mut self) {
        if self.pos < self.bytes.len() {
            if self.bytes[self.pos] == b'\n' {
                self.line += 1;
                self.col = 1;
            } else {
                self.col += 1;
            }
            self.pos += 1;
        }
    }

    fn add_error(&mut self, message: String, offset: usize, line: usize, col: usize) {
        if self.errors.len() < self.max_errors {
            self.errors.push(LexerError {
                message,
                offset,
                line,
                col,
            });
        }
    }

    fn make_token(&self, kind: TokenKind, start: usize, line: usize, col: usize) -> Token {
        Token {
            kind,
            span: Span::new(start, self.pos),
            line,
            col,
        }
    }

    // -----------------------------------------------------------------------
    // Whitespace & comments
    // -----------------------------------------------------------------------

    fn skip_whitespace_and_comments(&mut self) {
        loop {
            // Whitespace
            while !self.at_end() && self.peek().is_ascii_whitespace() {
                self.advance();
            }

            if self.at_end() {
                break;
            }

            // Single-line comment: --
            if self.peek() == b'-' && self.peek_at(1) == Some(b'-') {
                while !self.at_end() && self.peek() != b'\n' {
                    self.advance();
                }
                continue;
            }

            // Block comment: /* ... */ (nestable)
            if self.peek() == b'/' && self.peek_at(1) == Some(b'*') {
                self.skip_block_comment();
                continue;
            }

            break;
        }
    }

    fn skip_block_comment(&mut self) {
        let start_line = self.line;
        let start_col = self.col;
        let start_offset = self.pos;
        self.advance(); // /
        self.advance(); // *
        let mut depth: u32 = 1;
        while !self.at_end() && depth > 0 {
            if self.peek() == b'/' && self.peek_at(1) == Some(b'*') {
                depth += 1;
                self.advance();
                self.advance();
            } else if self.peek() == b'*' && self.peek_at(1) == Some(b'/') {
                depth -= 1;
                self.advance();
                self.advance();
            } else {
                self.advance();
            }
        }
        if depth > 0 {
            self.add_error(
                "unterminated block comment".into(),
                start_offset,
                start_line,
                start_col,
            );
        }
    }

    // -----------------------------------------------------------------------
    // Main dispatch
    // -----------------------------------------------------------------------

    fn next_token(&mut self) -> Token {
        self.skip_whitespace_and_comments();

        if self.at_end() {
            return Token {
                kind: TokenKind::Eof,
                span: Span::new(self.pos, self.pos),
                line: self.line,
                col: self.col,
            };
        }

        let start = self.pos;
        let start_line = self.line;
        let start_col = self.col;
        let b = self.peek();

        // E-string literal: E'...' or e'...'
        if (b == b'E' || b == b'e') && self.peek_at(1) == Some(b'\'') {
            return self.scan_e_string(start, start_line, start_col);
        }

        // Identifier or keyword
        if b.is_ascii_alphabetic() || b == b'_' {
            return self.scan_identifier(start, start_line, start_col);
        }

        // Numeric literal
        if b.is_ascii_digit() {
            return self.scan_number(start, start_line, start_col);
        }

        // String literal
        if b == b'\'' {
            return self.scan_string(start, start_line, start_col);
        }

        // Quoted identifier
        if b == b'"' {
            return self.scan_quoted_identifier(start, start_line, start_col);
        }

        // Placeholder $N
        if b == b'$' && self.peek_at(1).map_or(false, |c| c.is_ascii_digit()) {
            return self.scan_placeholder(start, start_line, start_col);
        }

        // Operators and punctuation
        if let Some(kind) = self.scan_operator(start, start_line, start_col) {
            return self.make_token(kind, start, start_line, start_col);
        }

        // Unknown character
        let ch = self.source[self.pos..].chars().next().unwrap();
        self.advance();
        self.add_error(
            format!("unexpected character '{ch}'"),
            start,
            start_line,
            start_col,
        );
        self.make_token(TokenKind::Error, start, start_line, start_col)
    }

    // -----------------------------------------------------------------------
    // Identifiers & keywords
    // -----------------------------------------------------------------------

    fn scan_identifier(&mut self, start: usize, line: usize, col: usize) -> Token {
        while !self.at_end()
            && (self.peek().is_ascii_alphanumeric() || self.peek() == b'_')
        {
            self.advance();
        }

        let text = &self.source[start..self.pos];
        let lower = text.to_ascii_lowercase();
        let kind = lookup_keyword(&lower).unwrap_or(TokenKind::Identifier);
        self.make_token(kind, start, line, col)
    }

    // -----------------------------------------------------------------------
    // Quoted identifiers  "..."
    // -----------------------------------------------------------------------

    fn scan_quoted_identifier(&mut self, start: usize, line: usize, col: usize) -> Token {
        self.advance(); // opening "
        loop {
            if self.at_end() {
                self.add_error(
                    "unterminated quoted identifier".into(),
                    start,
                    line,
                    col,
                );
                break;
            }
            if self.peek() == b'"' {
                self.advance();
                // "" escape
                if !self.at_end() && self.peek() == b'"' {
                    self.advance();
                    continue;
                }
                break;
            }
            self.advance();
        }
        self.make_token(TokenKind::QuotedIdentifier, start, line, col)
    }

    // -----------------------------------------------------------------------
    // String literals  '...'
    // -----------------------------------------------------------------------

    fn scan_string(&mut self, start: usize, line: usize, col: usize) -> Token {
        self.advance(); // opening '
        loop {
            if self.at_end() {
                self.add_error(
                    "unterminated string literal".into(),
                    start,
                    line,
                    col,
                );
                break;
            }
            if self.peek() == b'\'' {
                self.advance();
                // '' escape
                if !self.at_end() && self.peek() == b'\'' {
                    self.advance();
                    continue;
                }
                break;
            }
            self.advance();
        }
        self.make_token(TokenKind::StringLiteral, start, line, col)
    }

    // -----------------------------------------------------------------------
    // E-string literals  E'...' with C-style escapes
    // -----------------------------------------------------------------------

    fn scan_e_string(&mut self, start: usize, line: usize, col: usize) -> Token {
        self.advance(); // E
        self.advance(); // opening '
        loop {
            if self.at_end() {
                self.add_error(
                    "unterminated E-string literal".into(),
                    start,
                    line,
                    col,
                );
                break;
            }
            match self.peek() {
                b'\\' => {
                    self.advance(); // backslash
                    if !self.at_end() {
                        self.advance(); // escaped char
                    }
                }
                b'\'' => {
                    self.advance();
                    // '' escape also valid inside E-strings
                    if !self.at_end() && self.peek() == b'\'' {
                        self.advance();
                        continue;
                    }
                    break;
                }
                _ => {
                    self.advance();
                }
            }
        }
        self.make_token(TokenKind::StringLiteral, start, line, col)
    }

    // -----------------------------------------------------------------------
    // Numeric literals
    // -----------------------------------------------------------------------

    fn scan_number(&mut self, start: usize, line: usize, col: usize) -> Token {
        // Hex: 0x...
        if self.peek() == b'0' && self.peek_at(1).map_or(false, |c| c == b'x' || c == b'X') {
            return self.scan_hex(start, line, col);
        }

        // Integer part
        while !self.at_end() && self.peek().is_ascii_digit() {
            self.advance();
        }

        let mut is_float = false;

        // Fractional part: digits followed by '.' then digit
        if !self.at_end()
            && self.peek() == b'.'
            && self.peek_at(1).map_or(false, |c| c.is_ascii_digit())
        {
            is_float = true;
            self.advance(); // .
            while !self.at_end() && self.peek().is_ascii_digit() {
                self.advance();
            }
        }

        // Exponent: e/E followed by optional sign and digits
        if !self.at_end() && (self.peek() == b'e' || self.peek() == b'E') {
            let has_exponent = if self.peek_at(1).map_or(false, |c| c.is_ascii_digit()) {
                true
            } else if self
                .peek_at(1)
                .map_or(false, |c| c == b'+' || c == b'-')
                && self.peek_at(2).map_or(false, |c| c.is_ascii_digit())
            {
                true
            } else {
                false
            };

            if has_exponent {
                is_float = true;
                self.advance(); // e/E
                if !self.at_end() && (self.peek() == b'+' || self.peek() == b'-') {
                    self.advance(); // sign
                }
                while !self.at_end() && self.peek().is_ascii_digit() {
                    self.advance();
                }
            }
        }

        let kind = if is_float {
            TokenKind::FloatLiteral
        } else {
            TokenKind::IntegerLiteral
        };
        self.make_token(kind, start, line, col)
    }

    fn scan_hex(&mut self, start: usize, line: usize, col: usize) -> Token {
        self.advance(); // 0
        self.advance(); // x/X

        if self.at_end() || !self.peek().is_ascii_hexdigit() {
            self.add_error(
                "hex literal requires at least one digit".into(),
                start,
                line,
                col,
            );
            return self.make_token(TokenKind::Error, start, line, col);
        }

        while !self.at_end() && self.peek().is_ascii_hexdigit() {
            self.advance();
        }
        self.make_token(TokenKind::IntegerLiteral, start, line, col)
    }

    // -----------------------------------------------------------------------
    // Placeholder  $N
    // -----------------------------------------------------------------------

    fn scan_placeholder(&mut self, start: usize, line: usize, col: usize) -> Token {
        self.advance(); // $
        while !self.at_end() && self.peek().is_ascii_digit() {
            self.advance();
        }
        self.make_token(TokenKind::Placeholder, start, line, col)
    }

    // -----------------------------------------------------------------------
    // Operators & punctuation (longest-match disambiguation)
    // -----------------------------------------------------------------------

    fn scan_operator(
        &mut self,
        start: usize,
        start_line: usize,
        start_col: usize,
    ) -> Option<TokenKind> {
        let b = self.peek();
        match b {
            b'+' => {
                self.advance();
                Some(TokenKind::Plus)
            }
            b'-' => {
                self.advance();
                Some(TokenKind::Minus)
            }
            b'*' => {
                self.advance();
                Some(TokenKind::Star)
            }
            b'/' => {
                self.advance();
                Some(TokenKind::Slash)
            }
            b'%' => {
                self.advance();
                Some(TokenKind::Percent)
            }
            b'^' => {
                self.advance();
                Some(TokenKind::Caret)
            }
            b'=' => {
                self.advance();
                Some(TokenKind::Eq)
            }
            b'<' => {
                self.advance();
                if !self.at_end() {
                    match self.peek() {
                        b'>' => {
                            self.advance();
                            Some(TokenKind::NotEq)
                        }
                        b'=' => {
                            self.advance();
                            Some(TokenKind::LtEq)
                        }
                        _ => Some(TokenKind::Lt),
                    }
                } else {
                    Some(TokenKind::Lt)
                }
            }
            b'>' => {
                self.advance();
                if !self.at_end() && self.peek() == b'=' {
                    self.advance();
                    Some(TokenKind::GtEq)
                } else {
                    Some(TokenKind::Gt)
                }
            }
            b'!' => {
                self.advance();
                if !self.at_end() && self.peek() == b'=' {
                    self.advance();
                    Some(TokenKind::NotEq)
                } else {
                    self.add_error(
                        "expected '!=' operator, found lone '!'".into(),
                        start,
                        start_line,
                        start_col,
                    );
                    Some(TokenKind::Error)
                }
            }
            b'|' => {
                self.advance();
                if !self.at_end() && self.peek() == b'|' {
                    self.advance();
                    Some(TokenKind::Concat)
                } else {
                    self.add_error(
                        "expected '||' for concatenation, found single '|'".into(),
                        start,
                        start_line,
                        start_col,
                    );
                    Some(TokenKind::Error)
                }
            }
            b':' => {
                self.advance();
                if !self.at_end() && self.peek() == b':' {
                    self.advance();
                    Some(TokenKind::ColonColon)
                } else {
                    self.add_error(
                        "expected '::' for type cast, found single ':'".into(),
                        start,
                        start_line,
                        start_col,
                    );
                    Some(TokenKind::Error)
                }
            }
            b'(' => {
                self.advance();
                Some(TokenKind::LeftParen)
            }
            b')' => {
                self.advance();
                Some(TokenKind::RightParen)
            }
            b',' => {
                self.advance();
                Some(TokenKind::Comma)
            }
            b';' => {
                self.advance();
                Some(TokenKind::Semicolon)
            }
            b'.' => {
                self.advance();
                Some(TokenKind::Dot)
            }
            _ => None,
        }
    }
}
