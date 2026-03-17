use crate::types::DataType;

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    // Literals
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,

    // Identifiers
    Identifier(String),
    QualifiedIdentifier { table: String, column: String },

    // Wildcard
    Star,
    QualifiedStar(String),

    // Operators
    UnaryOp { op: UnaryOp, expr: Box<Expr> },
    BinaryOp { left: Box<Expr>, op: BinaryOp, right: Box<Expr> },

    // Special comparison forms
    IsNull { expr: Box<Expr>, negated: bool },
    InList { expr: Box<Expr>, list: Vec<Expr>, negated: bool },
    InSubquery { expr: Box<Expr>, subquery: Box<Select>, negated: bool },
    Between { expr: Box<Expr>, low: Box<Expr>, high: Box<Expr>, negated: bool },
    Like { expr: Box<Expr>, pattern: Box<Expr>, negated: bool, case_insensitive: bool },
    Exists { subquery: Box<Select>, negated: bool },

    // CASE
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<WhenClause>,
        else_clause: Option<Box<Expr>>,
    },

    // CAST (both CAST(expr AS type) and expr::type)
    Cast { expr: Box<Expr>, data_type: DataType },

    // Function call
    FunctionCall { name: String, args: Vec<Expr>, distinct: bool },

    // Special built-in functions
    Coalesce(Vec<Expr>),
    Nullif(Box<Expr>, Box<Expr>),
    Greatest(Vec<Expr>),
    Least(Vec<Expr>),

    // Subquery expression
    Subquery(Box<Select>),

    // Placeholder ($1, $2, ...)
    Placeholder(u32),
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhenClause {
    pub condition: Expr,
    pub result: Expr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Plus,
    Minus,
    Not,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Exp,
    Concat,
    Eq,
    NotEq,
    Lt,
    Gt,
    LtEq,
    GtEq,
    And,
    Or,
}

// ---------------------------------------------------------------------------
// Statements
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(Select),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    CreateTable(CreateTable),
    DropTable(DropTable),
    AlterTable(AlterTable),
    CreateIndex(CreateIndex),
    DropIndex(DropIndex),
    CreateView(CreateView),
    DropView(DropView),
    Begin,
    Commit,
    Rollback { savepoint: Option<String> },
    Savepoint { name: String },
    Explain { analyze: bool, statement: Box<Statement> },
    ShowTables,
    ShowColumns { table: String },
    Analyze { table: Option<String> },
    Vacuum { table: Option<String> },
    Copy(Copy),
}

// ---------------------------------------------------------------------------
// SELECT
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct Select {
    pub distinct: bool,
    pub columns: Vec<SelectColumn>,
    pub from: Option<FromClause>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
    pub set_op: Option<Box<SetOperation>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectColumn {
    Expr { expr: Expr, alias: Option<String> },
    AllColumns,
    TableAllColumns(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct FromClause {
    pub table: TableRef,
    pub joins: Vec<Join>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableRef {
    Table { name: String, alias: Option<String> },
    Subquery { query: Box<Select>, alias: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    pub join_type: JoinType,
    pub natural: bool,
    pub table: TableRef,
    pub condition: Option<JoinCondition>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    LeftSemi,
    LeftAnti,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinCondition {
    On(Expr),
    Using(Vec<String>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    pub expr: Expr,
    pub direction: Option<OrderDirection>,
    pub nulls: Option<NullsOrder>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullsOrder {
    First,
    Last,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SetOperation {
    pub op: SetOperator,
    pub all: bool,
    pub right: Select,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOperator {
    Union,
    Intersect,
    Except,
}

// ---------------------------------------------------------------------------
// INSERT
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct Insert {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub source: InsertSource,
    pub returning: Option<Vec<SelectColumn>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InsertSource {
    Values(Vec<Vec<Expr>>),
    Select(Box<Select>),
}

// ---------------------------------------------------------------------------
// UPDATE
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct Update {
    pub table: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<Expr>,
    pub returning: Option<Vec<SelectColumn>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: Expr,
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct Delete {
    pub table: String,
    pub where_clause: Option<Expr>,
    pub returning: Option<Vec<SelectColumn>>,
}

// ---------------------------------------------------------------------------
// CREATE TABLE
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTable {
    pub if_not_exists: bool,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraint {
    NotNull,
    Null,
    Default(Expr),
    PrimaryKey,
    Unique,
    Check(Expr),
    References {
        table: String,
        column: Option<String>,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferentialAction {
    Cascade,
    Restrict,
    NoAction,
    SetNull,
    SetDefault,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraint {
    PrimaryKey { name: Option<String>, columns: Vec<String> },
    Unique { name: Option<String>, columns: Vec<String> },
    Check { name: Option<String>, expr: Expr },
    ForeignKey {
        name: Option<String>,
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
}

// ---------------------------------------------------------------------------
// DROP TABLE
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct DropTable {
    pub if_exists: bool,
    pub name: String,
    pub cascade: bool,
}

// ---------------------------------------------------------------------------
// ALTER TABLE
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct AlterTable {
    pub name: String,
    pub action: AlterTableAction,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableAction {
    AddColumn(ColumnDef),
    DropColumn { name: String },
    RenameColumn { old_name: String, new_name: String },
    AddConstraint(TableConstraint),
    DropConstraint { name: String },
}

// ---------------------------------------------------------------------------
// CREATE / DROP INDEX
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndex {
    pub unique: bool,
    pub name: String,
    pub table: String,
    pub columns: Vec<IndexColumn>,
    pub using_method: Option<IndexMethod>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexColumn {
    pub name: String,
    pub direction: Option<OrderDirection>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexMethod {
    BTree,
    Hash,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropIndex {
    pub if_exists: bool,
    pub name: String,
}

// ---------------------------------------------------------------------------
// CREATE / DROP VIEW
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct CreateView {
    pub name: String,
    pub columns: Option<Vec<String>>,
    pub query: Select,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropView {
    pub if_exists: bool,
    pub name: String,
}

// ---------------------------------------------------------------------------
// COPY
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct Copy {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub direction: CopyDirection,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CopyDirection {
    From(String),
    To(String),
}
