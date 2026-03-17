use std::fmt;

// ---------------------------------------------------------------------------
// Lexer error
// ---------------------------------------------------------------------------

/// An error encountered during lexical analysis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LexerError {
    pub message: String,
    pub offset: usize,
    pub line: usize,
    pub col: usize,
}

impl fmt::Display for LexerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}: {}", self.line, self.col, self.message)
    }
}

impl std::error::Error for LexerError {}

/// Default maximum number of errors collected before the lexer stops recording.
pub const DEFAULT_MAX_ERRORS: usize = 50;
