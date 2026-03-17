// ---------------------------------------------------------------------------
// Span
// ---------------------------------------------------------------------------

/// Byte-offset range within the source string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    pub fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    pub fn len(&self) -> usize {
        self.end - self.start
    }

    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }

    pub fn text<'a>(&self, source: &'a str) -> &'a str {
        &source[self.start..self.end]
    }
}

// ---------------------------------------------------------------------------
// Token
// ---------------------------------------------------------------------------

/// A single lexical token produced by the lexer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Token {
    pub kind: TokenKind,
    pub span: Span,
    pub line: usize,
    pub col: usize,
}

// ---------------------------------------------------------------------------
// TokenKind
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TokenKind {
    // Literals
    IntegerLiteral,
    FloatLiteral,
    StringLiteral,
    BooleanLiteral,
    NullLiteral,

    // Identifiers
    Identifier,
    QuotedIdentifier,

    // ---- SQL keywords (89 variants) ----

    // DML
    KwSelect,
    KwFrom,
    KwWhere,
    KwInsert,
    KwInto,
    KwValues,
    KwUpdate,
    KwSet,
    KwDelete,
    KwReturning,

    // DDL
    KwCreate,
    KwTable,
    KwDrop,
    KwAlter,
    KwAdd,
    KwColumn,
    KwIndex,
    KwView,
    KwDatabase,
    KwSchema,
    KwTruncate,

    // Conditionals / existence
    KwIf,
    KwExists,

    // Logical
    KwNot,
    KwAnd,
    KwOr,
    KwIn,
    KwBetween,
    KwLike,
    KwIlike,
    KwIs,

    // Aliases & joins
    KwAs,
    KwOn,
    KwJoin,
    KwInner,
    KwLeft,
    KwRight,
    KwFull,
    KwOuter,
    KwCross,
    KwNatural,
    KwUsing,

    // ORDER BY
    KwOrder,
    KwBy,
    KwAsc,
    KwDesc,
    KwNulls,
    KwFirst,
    KwLast,

    // LIMIT / OFFSET
    KwLimit,
    KwOffset,

    // GROUP BY
    KwGroup,
    KwHaving,

    // DISTINCT / ALL
    KwDistinct,
    KwAll,

    // Set operations
    KwUnion,
    KwIntersect,
    KwExcept,

    // CTE
    KwWith,
    KwRecursive,

    // CASE
    KwCase,
    KwWhen,
    KwThen,
    KwElse,
    KwEnd,

    // CAST
    KwCast,

    // Constraints
    KwPrimary,
    KwKey,
    KwForeign,
    KwReferences,
    KwUnique,
    KwCheck,
    KwDefault,
    KwConstraint,

    // Transactions
    KwBegin,
    KwCommit,
    KwRollback,
    KwSavepoint,
    KwRelease,

    // Privileges
    KwGrant,
    KwRevoke,

    // Admin / utility
    KwExplain,
    KwAnalyze,
    KwVacuum,
    KwShow,
    KwCopy,

    // Additional DDL/DML
    KwCascade,
    KwRestrict,
    KwRename,
    KwTo,
    KwNo,

    // Functions / expressions
    KwCoalesce,
    KwNullif,
    KwGreatest,
    KwLeast,
    KwAny,
    KwSome,
    KwArray,
    KwRow,

    // ---- Operators ----
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Caret,
    Eq,
    NotEq,
    Lt,
    Gt,
    LtEq,
    GtEq,
    Concat,

    // ---- Punctuation ----
    LeftParen,
    RightParen,
    Comma,
    Semicolon,
    Dot,
    ColonColon,

    // ---- Placeholder ----
    Placeholder,

    // ---- Special ----
    Eof,
    Error,
}

// ---------------------------------------------------------------------------
// Keyword lookup (case-insensitive, input must be lowercase)
// ---------------------------------------------------------------------------

pub fn lookup_keyword(word: &str) -> Option<TokenKind> {
    match word {
        // DML
        "select" => Some(TokenKind::KwSelect),
        "from" => Some(TokenKind::KwFrom),
        "where" => Some(TokenKind::KwWhere),
        "insert" => Some(TokenKind::KwInsert),
        "into" => Some(TokenKind::KwInto),
        "values" => Some(TokenKind::KwValues),
        "update" => Some(TokenKind::KwUpdate),
        "set" => Some(TokenKind::KwSet),
        "delete" => Some(TokenKind::KwDelete),
        "returning" => Some(TokenKind::KwReturning),

        // DDL
        "create" => Some(TokenKind::KwCreate),
        "table" => Some(TokenKind::KwTable),
        "drop" => Some(TokenKind::KwDrop),
        "alter" => Some(TokenKind::KwAlter),
        "add" => Some(TokenKind::KwAdd),
        "column" => Some(TokenKind::KwColumn),
        "index" => Some(TokenKind::KwIndex),
        "view" => Some(TokenKind::KwView),
        "database" => Some(TokenKind::KwDatabase),
        "schema" => Some(TokenKind::KwSchema),
        "truncate" => Some(TokenKind::KwTruncate),

        // Conditionals
        "if" => Some(TokenKind::KwIf),
        "exists" => Some(TokenKind::KwExists),

        // Logical
        "not" => Some(TokenKind::KwNot),
        "and" => Some(TokenKind::KwAnd),
        "or" => Some(TokenKind::KwOr),
        "in" => Some(TokenKind::KwIn),
        "between" => Some(TokenKind::KwBetween),
        "like" => Some(TokenKind::KwLike),
        "ilike" => Some(TokenKind::KwIlike),
        "is" => Some(TokenKind::KwIs),

        // Aliases & joins
        "as" => Some(TokenKind::KwAs),
        "on" => Some(TokenKind::KwOn),
        "join" => Some(TokenKind::KwJoin),
        "inner" => Some(TokenKind::KwInner),
        "left" => Some(TokenKind::KwLeft),
        "right" => Some(TokenKind::KwRight),
        "full" => Some(TokenKind::KwFull),
        "outer" => Some(TokenKind::KwOuter),
        "cross" => Some(TokenKind::KwCross),
        "natural" => Some(TokenKind::KwNatural),
        "using" => Some(TokenKind::KwUsing),

        // ORDER BY
        "order" => Some(TokenKind::KwOrder),
        "by" => Some(TokenKind::KwBy),
        "asc" => Some(TokenKind::KwAsc),
        "desc" => Some(TokenKind::KwDesc),
        "nulls" => Some(TokenKind::KwNulls),
        "first" => Some(TokenKind::KwFirst),
        "last" => Some(TokenKind::KwLast),

        // LIMIT / OFFSET
        "limit" => Some(TokenKind::KwLimit),
        "offset" => Some(TokenKind::KwOffset),

        // GROUP BY
        "group" => Some(TokenKind::KwGroup),
        "having" => Some(TokenKind::KwHaving),

        // DISTINCT / ALL
        "distinct" => Some(TokenKind::KwDistinct),
        "all" => Some(TokenKind::KwAll),

        // Set operations
        "union" => Some(TokenKind::KwUnion),
        "intersect" => Some(TokenKind::KwIntersect),
        "except" => Some(TokenKind::KwExcept),

        // CTE
        "with" => Some(TokenKind::KwWith),
        "recursive" => Some(TokenKind::KwRecursive),

        // CASE
        "case" => Some(TokenKind::KwCase),
        "when" => Some(TokenKind::KwWhen),
        "then" => Some(TokenKind::KwThen),
        "else" => Some(TokenKind::KwElse),
        "end" => Some(TokenKind::KwEnd),

        // CAST
        "cast" => Some(TokenKind::KwCast),

        // Constraints
        "primary" => Some(TokenKind::KwPrimary),
        "key" => Some(TokenKind::KwKey),
        "foreign" => Some(TokenKind::KwForeign),
        "references" => Some(TokenKind::KwReferences),
        "unique" => Some(TokenKind::KwUnique),
        "check" => Some(TokenKind::KwCheck),
        "default" => Some(TokenKind::KwDefault),
        "constraint" => Some(TokenKind::KwConstraint),

        // Transactions
        "begin" => Some(TokenKind::KwBegin),
        "commit" => Some(TokenKind::KwCommit),
        "rollback" => Some(TokenKind::KwRollback),
        "savepoint" => Some(TokenKind::KwSavepoint),
        "release" => Some(TokenKind::KwRelease),

        // Privileges
        "grant" => Some(TokenKind::KwGrant),
        "revoke" => Some(TokenKind::KwRevoke),

        // Admin
        "explain" => Some(TokenKind::KwExplain),
        "analyze" => Some(TokenKind::KwAnalyze),
        "vacuum" => Some(TokenKind::KwVacuum),
        "show" => Some(TokenKind::KwShow),
        "copy" => Some(TokenKind::KwCopy),

        // Additional DDL/DML
        "cascade" => Some(TokenKind::KwCascade),
        "restrict" => Some(TokenKind::KwRestrict),
        "rename" => Some(TokenKind::KwRename),
        "to" => Some(TokenKind::KwTo),
        "no" => Some(TokenKind::KwNo),

        // Functions / expressions
        "coalesce" => Some(TokenKind::KwCoalesce),
        "nullif" => Some(TokenKind::KwNullif),
        "greatest" => Some(TokenKind::KwGreatest),
        "least" => Some(TokenKind::KwLeast),
        "any" => Some(TokenKind::KwAny),
        "some" => Some(TokenKind::KwSome),
        "array" => Some(TokenKind::KwArray),
        "row" => Some(TokenKind::KwRow),

        // Literals (keyword forms)
        "true" => Some(TokenKind::BooleanLiteral),
        "false" => Some(TokenKind::BooleanLiteral),
        "null" => Some(TokenKind::NullLiteral),

        _ => None,
    }
}
