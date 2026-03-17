use crate::sql::ast::{self, BinaryOp, Expr, JoinType};
use crate::types::{Column, DataType, Schema};

// ---------------------------------------------------------------------------
// Aggregate function enum
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregateFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    StringAgg,
    ArrayAgg,
    BoolAnd,
    BoolOr,
}

impl AggregateFunc {
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "count" => Some(AggregateFunc::Count),
            "sum" => Some(AggregateFunc::Sum),
            "avg" => Some(AggregateFunc::Avg),
            "min" => Some(AggregateFunc::Min),
            "max" => Some(AggregateFunc::Max),
            "string_agg" => Some(AggregateFunc::StringAgg),
            "array_agg" => Some(AggregateFunc::ArrayAgg),
            "bool_and" | "every" => Some(AggregateFunc::BoolAnd),
            "bool_or" => Some(AggregateFunc::BoolOr),
            _ => None,
        }
    }

    pub fn return_type(&self, input_type: DataType) -> DataType {
        match self {
            AggregateFunc::Count => DataType::BigInt,
            AggregateFunc::Sum => match input_type {
                DataType::Integer => DataType::BigInt,
                DataType::BigInt => DataType::BigInt,
                DataType::Float => DataType::Float,
                DataType::Decimal(p, s) => DataType::Decimal(p, s),
                other => other,
            },
            AggregateFunc::Avg => DataType::Float,
            AggregateFunc::Min | AggregateFunc::Max => input_type,
            AggregateFunc::StringAgg => DataType::Text,
            AggregateFunc::ArrayAgg => DataType::Text, // serialized as text for now
            AggregateFunc::BoolAnd | AggregateFunc::BoolOr => DataType::Boolean,
        }
    }
}

impl std::fmt::Display for AggregateFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregateFunc::Count => write!(f, "count"),
            AggregateFunc::Sum => write!(f, "sum"),
            AggregateFunc::Avg => write!(f, "avg"),
            AggregateFunc::Min => write!(f, "min"),
            AggregateFunc::Max => write!(f, "max"),
            AggregateFunc::StringAgg => write!(f, "string_agg"),
            AggregateFunc::ArrayAgg => write!(f, "array_agg"),
            AggregateFunc::BoolAnd => write!(f, "bool_and"),
            AggregateFunc::BoolOr => write!(f, "bool_or"),
        }
    }
}

// ---------------------------------------------------------------------------
// Aggregate expression
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct AggregateExpr {
    pub func: AggregateFunc,
    pub arg: Expr,
    pub distinct: bool,
    pub alias: String,
}

// ---------------------------------------------------------------------------
// Sort expression
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct SortExpr {
    pub expr: Expr,
    pub ascending: bool,
    pub nulls_first: bool,
}

// ---------------------------------------------------------------------------
// Projection expression
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionExpr {
    pub expr: Expr,
    pub alias: String,
}

// ---------------------------------------------------------------------------
// Logical plan
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Table scan. `schema` contains all columns of the table.
    Scan {
        table: String,
        alias: Option<String>,
        schema: Schema,
    },

    /// Predicate filter.
    Filter {
        predicate: Expr,
        input: Box<LogicalPlan>,
    },

    /// Column projection / expression evaluation.
    Project {
        expressions: Vec<ProjectionExpr>,
        input: Box<LogicalPlan>,
    },

    /// Join of two relations.
    Join {
        join_type: JoinType,
        condition: Option<Expr>,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        schema: Schema,
    },

    /// Grouping with aggregate functions.
    Aggregate {
        group_by: Vec<Expr>,
        aggregates: Vec<AggregateExpr>,
        input: Box<LogicalPlan>,
        schema: Schema,
    },

    /// Order-preserving sort.
    Sort {
        order_by: Vec<SortExpr>,
        input: Box<LogicalPlan>,
    },

    /// Row limit with optional offset.
    Limit {
        count: Option<usize>,
        offset: usize,
        input: Box<LogicalPlan>,
    },

    /// Duplicate elimination.
    Distinct {
        input: Box<LogicalPlan>,
    },

    /// Set union.
    Union {
        all: bool,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    },

    /// Set intersection.
    Intersect {
        all: bool,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    },

    /// Set difference.
    Except {
        all: bool,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    },

    /// INSERT target.
    Insert {
        table: String,
        columns: Vec<String>,
        input: Box<LogicalPlan>,
    },

    /// UPDATE with assignments.
    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        input: Box<LogicalPlan>,
    },

    /// DELETE from table.
    Delete {
        table: String,
        input: Box<LogicalPlan>,
    },

    /// Literal row set (INSERT VALUES, inline tables).
    Values {
        rows: Vec<Vec<Expr>>,
        schema: Schema,
    },

    /// Zero-row relation (used as leaf when no FROM clause).
    Empty {
        schema: Schema,
    },
}

impl LogicalPlan {
    /// Returns the output schema of this plan node.
    pub fn schema(&self) -> Schema {
        match self {
            LogicalPlan::Scan { schema, .. } => schema.clone(),
            LogicalPlan::Filter { input, .. } => input.schema(),
            LogicalPlan::Project { expressions, .. } => {
                let cols = expressions
                    .iter()
                    .map(|pe| {
                        let dt = Self::infer_expr_type(&pe.expr);
                        Column::new(pe.alias.clone(), dt, true)
                    })
                    .collect();
                Schema::new(cols)
            }
            LogicalPlan::Join { schema, .. } => schema.clone(),
            LogicalPlan::Aggregate { schema, .. } => schema.clone(),
            LogicalPlan::Sort { input, .. } => input.schema(),
            LogicalPlan::Limit { input, .. } => input.schema(),
            LogicalPlan::Distinct { input } => input.schema(),
            LogicalPlan::Union { left, .. } => left.schema(),
            LogicalPlan::Intersect { left, .. } => left.schema(),
            LogicalPlan::Except { left, .. } => left.schema(),
            LogicalPlan::Insert { input, .. } => input.schema(),
            LogicalPlan::Update { input, .. } => input.schema(),
            LogicalPlan::Delete { input, .. } => input.schema(),
            LogicalPlan::Values { schema, .. } => schema.clone(),
            LogicalPlan::Empty { schema } => schema.clone(),
        }
    }

    /// Returns the name of this plan node for display.
    pub fn node_name(&self) -> &'static str {
        match self {
            LogicalPlan::Scan { .. } => "Scan",
            LogicalPlan::Filter { .. } => "Filter",
            LogicalPlan::Project { .. } => "Project",
            LogicalPlan::Join { .. } => "Join",
            LogicalPlan::Aggregate { .. } => "Aggregate",
            LogicalPlan::Sort { .. } => "Sort",
            LogicalPlan::Limit { .. } => "Limit",
            LogicalPlan::Distinct { .. } => "Distinct",
            LogicalPlan::Union { .. } => "Union",
            LogicalPlan::Intersect { .. } => "Intersect",
            LogicalPlan::Except { .. } => "Except",
            LogicalPlan::Insert { .. } => "Insert",
            LogicalPlan::Update { .. } => "Update",
            LogicalPlan::Delete { .. } => "Delete",
            LogicalPlan::Values { .. } => "Values",
            LogicalPlan::Empty { .. } => "Empty",
        }
    }

    /// Returns child plan nodes.
    pub fn children(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::Scan { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::Empty { .. } => vec![],
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Distinct { input }
            | LogicalPlan::Insert { input, .. }
            | LogicalPlan::Update { input, .. }
            | LogicalPlan::Delete { input, .. } => vec![input],
            LogicalPlan::Join { left, right, .. }
            | LogicalPlan::Union { left, right, .. }
            | LogicalPlan::Intersect { left, right, .. }
            | LogicalPlan::Except { left, right, .. } => vec![left, right],
        }
    }

    /// Simple type inference for expressions. Returns a best-guess DataType.
    pub fn infer_expr_type(expr: &Expr) -> DataType {
        match expr {
            Expr::Integer(_) => DataType::BigInt,
            Expr::Float(_) => DataType::Float,
            Expr::String(_) => DataType::Text,
            Expr::Boolean(_) => DataType::Boolean,
            Expr::Null => DataType::Integer, // unknown, nullable
            Expr::BinaryOp { left, op, .. } => match op {
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod | BinaryOp::Exp => {
                    Self::infer_expr_type(left)
                }
                BinaryOp::Concat => DataType::Text,
                _ => DataType::Boolean, // comparisons
            },
            Expr::UnaryOp { op: ast::UnaryOp::Not, .. } => DataType::Boolean,
            Expr::UnaryOp { expr, .. } => Self::infer_expr_type(expr),
            Expr::IsNull { .. }
            | Expr::InList { .. }
            | Expr::InSubquery { .. }
            | Expr::Between { .. }
            | Expr::Like { .. }
            | Expr::Exists { .. } => DataType::Boolean,
            Expr::Cast { data_type, .. } => *data_type,
            Expr::FunctionCall { name, args, .. } => {
                if let Some(agg) = AggregateFunc::from_name(name) {
                    let input_type = args.first()
                        .map(|a| Self::infer_expr_type(a))
                        .unwrap_or(DataType::BigInt);
                    agg.return_type(input_type)
                } else {
                    DataType::Text // default for unknown functions
                }
            }
            _ => DataType::Text, // fallback
        }
    }
}

// ---------------------------------------------------------------------------
// Expression helpers
// ---------------------------------------------------------------------------

/// Collect all column references from an expression.
pub fn collect_columns(expr: &Expr) -> Vec<(Option<String>, String)> {
    let mut cols = Vec::new();
    collect_columns_inner(expr, &mut cols);
    cols
}

fn collect_columns_inner(expr: &Expr, out: &mut Vec<(Option<String>, String)>) {
    match expr {
        Expr::Identifier(name) => out.push((None, name.clone())),
        Expr::QualifiedIdentifier { table, column } => {
            out.push((Some(table.clone()), column.clone()));
        }
        Expr::BinaryOp { left, right, .. } => {
            collect_columns_inner(left, out);
            collect_columns_inner(right, out);
        }
        Expr::UnaryOp { expr, .. } => collect_columns_inner(expr, out),
        Expr::IsNull { expr, .. } => collect_columns_inner(expr, out),
        Expr::InList { expr, list, .. } => {
            collect_columns_inner(expr, out);
            for item in list {
                collect_columns_inner(item, out);
            }
        }
        Expr::Between { expr, low, high, .. } => {
            collect_columns_inner(expr, out);
            collect_columns_inner(low, out);
            collect_columns_inner(high, out);
        }
        Expr::Like { expr, pattern, .. } => {
            collect_columns_inner(expr, out);
            collect_columns_inner(pattern, out);
        }
        Expr::Case { operand, when_clauses, else_clause } => {
            if let Some(op) = operand {
                collect_columns_inner(op, out);
            }
            for wc in when_clauses {
                collect_columns_inner(&wc.condition, out);
                collect_columns_inner(&wc.result, out);
            }
            if let Some(ec) = else_clause {
                collect_columns_inner(ec, out);
            }
        }
        Expr::Cast { expr, .. } => collect_columns_inner(expr, out),
        Expr::FunctionCall { args, .. } => {
            for arg in args {
                collect_columns_inner(arg, out);
            }
        }
        Expr::Coalesce(args) | Expr::Greatest(args) | Expr::Least(args) => {
            for arg in args {
                collect_columns_inner(arg, out);
            }
        }
        Expr::Nullif(a, b) => {
            collect_columns_inner(a, out);
            collect_columns_inner(b, out);
        }
        _ => {}
    }
}

/// Check if an expression only references columns from a given set of table names.
pub fn expr_references_only(expr: &Expr, tables: &[&str]) -> bool {
    let cols = collect_columns(expr);
    cols.iter().all(|(tbl, _)| {
        match tbl {
            Some(t) => tables.contains(&t.as_str()),
            None => true, // unqualified columns are ambiguous, allow them
        }
    })
}

/// Split a conjunction (AND chain) into individual predicates.
pub fn split_conjunction(expr: &Expr) -> Vec<Expr> {
    let mut parts = Vec::new();
    split_conjunction_inner(expr, &mut parts);
    parts
}

fn split_conjunction_inner(expr: &Expr, out: &mut Vec<Expr>) {
    match expr {
        Expr::BinaryOp { left, op: BinaryOp::And, right } => {
            split_conjunction_inner(left, out);
            split_conjunction_inner(right, out);
        }
        other => out.push(other.clone()),
    }
}

/// Combine predicates with AND.
pub fn combine_conjunction(preds: &[Expr]) -> Option<Expr> {
    if preds.is_empty() {
        return None;
    }
    let mut result = preds[0].clone();
    for pred in &preds[1..] {
        result = Expr::BinaryOp {
            left: Box::new(result),
            op: BinaryOp::And,
            right: Box::new(pred.clone()),
        };
    }
    Some(result)
}

/// Get the set of table names referenced in an expression.
pub fn referenced_tables(expr: &Expr) -> Vec<String> {
    let cols = collect_columns(expr);
    let mut tables: Vec<String> = cols
        .into_iter()
        .filter_map(|(t, _)| t)
        .collect();
    tables.sort();
    tables.dedup();
    tables
}
