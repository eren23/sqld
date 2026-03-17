use std::cmp::Ordering;

use crate::sql::ast::{BinaryOp, Expr, UnaryOp, WhenClause};
use crate::types::{DataType, Datum, Schema, Tuple};
use crate::utils::error::{Result, SqlError, TypeError};

use super::scalar_functions::call_scalar_function;

// ---------------------------------------------------------------------------
// ExprOp — stack-based bytecode for expression evaluation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum ExprOp {
    // Push values
    PushLiteral(Datum),
    PushColumn(usize),

    // Arithmetic (pop 2 → push 1)
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Exp,
    Concat,

    // Unary (pop 1 → push 1)
    Neg,
    Not,

    // Comparison (pop 2 → push 1)
    Eq,
    NotEq,
    Lt,
    Gt,
    LtEq,
    GtEq,

    // Logical (pop 2 → push 1)
    And,
    Or,

    // Null checks (pop 1 → push 1)
    IsNull,
    IsNotNull,

    // Type cast (pop 1 → push 1)
    Cast(DataType),

    // Scalar function call (pop `arity` → push 1)
    CallScalar { name: String, arity: usize },

    // COALESCE (pop `n` → push 1)
    Coalesce(usize),

    // NULLIF (pop 2 → push 1)
    Nullif,

    // GREATEST / LEAST (pop `n` → push 1)
    Greatest(usize),
    Least(usize),

    // CASE (pop items → push 1)
    //   Stack layout bottom→top:
    //     [operand?] [cond1, result1, cond2, result2, ...] [else?]
    Case {
        when_count: usize,
        has_else: bool,
        has_operand: bool,
    },

    // BETWEEN (pop 3: value, low, high → push 1)
    Between { negated: bool },

    // IN list (pop 1+len → push 1)
    InList { len: usize, negated: bool },

    // LIKE (pop 2: value, pattern → push 1)
    Like {
        negated: bool,
        case_insensitive: bool,
    },
}

// ---------------------------------------------------------------------------
// Compilation: Expr → Vec<ExprOp>
// ---------------------------------------------------------------------------

/// Compile an AST expression to stack-based bytecode.
///
/// Column references are resolved against `schema`.
pub fn compile_expr(expr: &Expr, schema: &Schema) -> Result<Vec<ExprOp>> {
    let mut ops = Vec::new();
    compile_inner(expr, schema, &mut ops)?;
    Ok(ops)
}

fn compile_inner(expr: &Expr, schema: &Schema, ops: &mut Vec<ExprOp>) -> Result<()> {
    match expr {
        // --- Literals ---
        Expr::Integer(v) => {
            if *v >= i32::MIN as i64 && *v <= i32::MAX as i64 {
                ops.push(ExprOp::PushLiteral(Datum::Integer(*v as i32)));
            } else {
                ops.push(ExprOp::PushLiteral(Datum::BigInt(*v)));
            }
        }
        Expr::Float(v) => ops.push(ExprOp::PushLiteral(Datum::Float(*v))),
        Expr::String(s) => ops.push(ExprOp::PushLiteral(Datum::Text(s.clone()))),
        Expr::Boolean(b) => ops.push(ExprOp::PushLiteral(Datum::Boolean(*b))),
        Expr::Null => ops.push(ExprOp::PushLiteral(Datum::Null)),

        // --- Column references ---
        Expr::Identifier(name) => {
            let ordinal = resolve_column(name, None, schema)?;
            ops.push(ExprOp::PushColumn(ordinal));
        }
        Expr::QualifiedIdentifier { table, column } => {
            let ordinal = resolve_column(column, Some(table), schema)?;
            ops.push(ExprOp::PushColumn(ordinal));
        }

        // --- Binary operators ---
        Expr::BinaryOp { left, op, right } => {
            compile_inner(left, schema, ops)?;
            compile_inner(right, schema, ops)?;
            ops.push(match op {
                BinaryOp::Add => ExprOp::Add,
                BinaryOp::Sub => ExprOp::Sub,
                BinaryOp::Mul => ExprOp::Mul,
                BinaryOp::Div => ExprOp::Div,
                BinaryOp::Mod => ExprOp::Mod,
                BinaryOp::Exp => ExprOp::Exp,
                BinaryOp::Concat => ExprOp::Concat,
                BinaryOp::Eq => ExprOp::Eq,
                BinaryOp::NotEq => ExprOp::NotEq,
                BinaryOp::Lt => ExprOp::Lt,
                BinaryOp::Gt => ExprOp::Gt,
                BinaryOp::LtEq => ExprOp::LtEq,
                BinaryOp::GtEq => ExprOp::GtEq,
                BinaryOp::And => ExprOp::And,
                BinaryOp::Or => ExprOp::Or,
            });
        }

        // --- Unary operators ---
        Expr::UnaryOp { op, expr } => {
            compile_inner(expr, schema, ops)?;
            match op {
                UnaryOp::Minus => ops.push(ExprOp::Neg),
                UnaryOp::Plus => {} // no-op
                UnaryOp::Not => ops.push(ExprOp::Not),
            }
        }

        // --- IS NULL / IS NOT NULL ---
        Expr::IsNull { expr, negated } => {
            compile_inner(expr, schema, ops)?;
            if *negated {
                ops.push(ExprOp::IsNotNull);
            } else {
                ops.push(ExprOp::IsNull);
            }
        }

        // --- IN list ---
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            compile_inner(expr, schema, ops)?;
            for item in list {
                compile_inner(item, schema, ops)?;
            }
            ops.push(ExprOp::InList {
                len: list.len(),
                negated: *negated,
            });
        }

        // --- BETWEEN ---
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            compile_inner(expr, schema, ops)?;
            compile_inner(low, schema, ops)?;
            compile_inner(high, schema, ops)?;
            ops.push(ExprOp::Between { negated: *negated });
        }

        // --- LIKE ---
        Expr::Like {
            expr,
            pattern,
            negated,
            case_insensitive,
        } => {
            compile_inner(expr, schema, ops)?;
            compile_inner(pattern, schema, ops)?;
            ops.push(ExprOp::Like {
                negated: *negated,
                case_insensitive: *case_insensitive,
            });
        }

        // --- CASE ---
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            let has_operand = operand.is_some();
            if let Some(op) = operand {
                compile_inner(op, schema, ops)?;
            }
            for WhenClause { condition, result } in when_clauses {
                compile_inner(condition, schema, ops)?;
                compile_inner(result, schema, ops)?;
            }
            let has_else = else_clause.is_some();
            if let Some(ec) = else_clause {
                compile_inner(ec, schema, ops)?;
            }
            ops.push(ExprOp::Case {
                when_count: when_clauses.len(),
                has_else,
                has_operand,
            });
        }

        // --- CAST ---
        Expr::Cast { expr, data_type } => {
            compile_inner(expr, schema, ops)?;
            ops.push(ExprOp::Cast(*data_type));
        }

        // --- Function call ---
        Expr::FunctionCall {
            name,
            args,
            distinct: _,
        } => {
            for arg in args {
                compile_inner(arg, schema, ops)?;
            }
            ops.push(ExprOp::CallScalar {
                name: name.to_ascii_lowercase(),
                arity: args.len(),
            });
        }

        // --- COALESCE ---
        Expr::Coalesce(args) => {
            for arg in args {
                compile_inner(arg, schema, ops)?;
            }
            ops.push(ExprOp::Coalesce(args.len()));
        }

        // --- NULLIF ---
        Expr::Nullif(a, b) => {
            compile_inner(a, schema, ops)?;
            compile_inner(b, schema, ops)?;
            ops.push(ExprOp::Nullif);
        }

        // --- GREATEST / LEAST ---
        Expr::Greatest(args) => {
            for arg in args {
                compile_inner(arg, schema, ops)?;
            }
            ops.push(ExprOp::Greatest(args.len()));
        }
        Expr::Least(args) => {
            for arg in args {
                compile_inner(arg, schema, ops)?;
            }
            ops.push(ExprOp::Least(args.len()));
        }

        // --- Star / wildcards (should be resolved before reaching executor) ---
        Expr::Star | Expr::QualifiedStar(_) => {
            return Err(SqlError::ExecutionError(
                "unresolved wildcard in expression".into(),
            )
            .into());
        }

        // --- Subqueries (not supported in expression evaluator) ---
        Expr::Subquery(_)
        | Expr::Exists { .. }
        | Expr::InSubquery { .. } => {
            return Err(SqlError::ExecutionError(
                "subqueries in expressions are not yet supported".into(),
            )
            .into());
        }

        // --- Placeholders ---
        Expr::Placeholder(n) => {
            return Err(SqlError::ExecutionError(
                format!("unbound placeholder ${n}"),
            )
            .into());
        }
    }
    Ok(())
}

/// Resolve a column name to an ordinal in the schema.
fn resolve_column(name: &str, table: Option<&str>, schema: &Schema) -> Result<usize> {
    // 1. Try exact name
    if let Some((idx, _)) = schema.column_by_name(name) {
        return Ok(idx);
    }

    if let Some(tbl) = table {
        // 2. Try "table.column"
        let qualified = format!("{tbl}.{name}");
        if let Some((idx, _)) = schema.column_by_name(&qualified) {
            return Ok(idx);
        }
        // 3. Try "_right_column" (join schema convention)
        let right = format!("_right_{name}");
        if let Some((idx, _)) = schema.column_by_name(&right) {
            return Ok(idx);
        }
    }

    // 4. Case-insensitive fallback
    for (i, col) in schema.columns().iter().enumerate() {
        if col.name.eq_ignore_ascii_case(name) {
            return Ok(i);
        }
    }

    Err(SqlError::ExecutionError(format!(
        "column '{}' not found in schema with columns: {:?}",
        table
            .map(|t| format!("{t}.{name}"))
            .unwrap_or_else(|| name.to_string()),
        schema
            .columns()
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>()
    ))
    .into())
}

// ---------------------------------------------------------------------------
// Evaluation: Vec<ExprOp> + Tuple → Datum
// ---------------------------------------------------------------------------

/// Evaluate compiled bytecode against a tuple, returning a single Datum.
pub fn evaluate_expr(ops: &[ExprOp], tuple: &Tuple) -> Result<Datum> {
    let mut stack: Vec<Datum> = Vec::with_capacity(ops.len());

    for op in ops {
        match op {
            ExprOp::PushLiteral(d) => stack.push(d.clone()),

            ExprOp::PushColumn(idx) => {
                let val = tuple
                    .get(*idx)
                    .cloned()
                    .unwrap_or(Datum::Null);
                stack.push(val);
            }

            // --- Arithmetic ---
            ExprOp::Add => {
                let b = pop(&mut stack)?;
                let a = pop(&mut stack)?;
                stack.push(a.add(&b)?);
            }
            ExprOp::Sub => {
                let b = pop(&mut stack)?;
                let a = pop(&mut stack)?;
                stack.push(a.sub(&b)?);
            }
            ExprOp::Mul => {
                let b = pop(&mut stack)?;
                let a = pop(&mut stack)?;
                stack.push(a.mul(&b)?);
            }
            ExprOp::Div => {
                let b = pop(&mut stack)?;
                let a = pop(&mut stack)?;
                stack.push(a.div(&b)?);
            }
            ExprOp::Mod => {
                let b = pop(&mut stack)?;
                let a = pop(&mut stack)?;
                stack.push(eval_mod(&a, &b)?);
            }
            ExprOp::Exp => {
                let b = pop(&mut stack)?;
                let a = pop(&mut stack)?;
                stack.push(eval_exp(&a, &b)?);
            }
            ExprOp::Concat => {
                let b = pop(&mut stack)?;
                let a = pop(&mut stack)?;
                stack.push(eval_concat(&a, &b));
            }
            ExprOp::Neg => {
                let a = pop(&mut stack)?;
                stack.push(a.neg()?);
            }
            ExprOp::Not => {
                let a = pop(&mut stack)?;
                stack.push(eval_not(&a));
            }

            // --- Comparison ---
            ExprOp::Eq => {
                let b = pop(&mut stack)?;
                let a = pop(&mut stack)?;
                stack.push(eval_cmp(&a, &b, Ordering::Equal, false)?);
            }
            ExprOp::NotEq => {
                let b = pop(&mut stack)?;
                let a = pop(&mut stack)?;
                stack.push(eval_cmp(&a, &b, Ordering::Equal, true)?);
            }
            ExprOp::Lt => {
                let b = pop(&mut stack)?;
                let a = pop(&mut stack)?;
                stack.push(eval_cmp(&a, &b, Ordering::Less, false)?);
            }
            ExprOp::Gt => {
                let b = pop(&mut stack)?;
                let a = pop(&mut stack)?;
                stack.push(eval_cmp(&a, &b, Ordering::Greater, false)?);
            }
            ExprOp::LtEq => {
                let b = pop(&mut stack)?;
                let a = pop(&mut stack)?;
                stack.push(eval_cmp_le(&a, &b)?);
            }
            ExprOp::GtEq => {
                let b = pop(&mut stack)?;
                let a = pop(&mut stack)?;
                stack.push(eval_cmp_ge(&a, &b)?);
            }

            // --- Logical ---
            ExprOp::And => {
                let b = pop(&mut stack)?;
                let a = pop(&mut stack)?;
                stack.push(eval_and(&a, &b));
            }
            ExprOp::Or => {
                let b = pop(&mut stack)?;
                let a = pop(&mut stack)?;
                stack.push(eval_or(&a, &b));
            }

            // --- Null checks ---
            ExprOp::IsNull => {
                let a = pop(&mut stack)?;
                stack.push(Datum::Boolean(a.is_null()));
            }
            ExprOp::IsNotNull => {
                let a = pop(&mut stack)?;
                stack.push(Datum::Boolean(!a.is_null()));
            }

            // --- Cast ---
            ExprOp::Cast(dt) => {
                let a = pop(&mut stack)?;
                stack.push(eval_cast(a, dt)?);
            }

            // --- Scalar function ---
            ExprOp::CallScalar { name, arity } => {
                let n = *arity;
                if stack.len() < n {
                    return Err(
                        SqlError::ExecutionError("stack underflow".into()).into()
                    );
                }
                let args: Vec<Datum> = stack.drain(stack.len() - n..).collect();
                stack.push(call_scalar_function(name, args)?);
            }

            // --- COALESCE ---
            ExprOp::Coalesce(n) => {
                let args: Vec<Datum> = stack.drain(stack.len() - n..).collect();
                let result = args
                    .into_iter()
                    .find(|d| !d.is_null())
                    .unwrap_or(Datum::Null);
                stack.push(result);
            }

            // --- NULLIF ---
            ExprOp::Nullif => {
                let b = pop(&mut stack)?;
                let a = pop(&mut stack)?;
                let cmp = a.sql_cmp(&b)?;
                if cmp == Some(Ordering::Equal) {
                    stack.push(Datum::Null);
                } else {
                    stack.push(a);
                }
            }

            // --- GREATEST / LEAST ---
            ExprOp::Greatest(n) => {
                let args: Vec<Datum> = stack.drain(stack.len() - n..).collect();
                stack.push(eval_greatest(args)?);
            }
            ExprOp::Least(n) => {
                let args: Vec<Datum> = stack.drain(stack.len() - n..).collect();
                stack.push(eval_least(args)?);
            }

            // --- CASE ---
            ExprOp::Case {
                when_count,
                has_else,
                has_operand,
            } => {
                let else_val = if *has_else {
                    Some(pop(&mut stack)?)
                } else {
                    None
                };
                let mut whens = Vec::with_capacity(*when_count);
                for _ in 0..*when_count {
                    let result = pop(&mut stack)?;
                    let condition = pop(&mut stack)?;
                    whens.push((condition, result));
                }
                whens.reverse();

                let operand = if *has_operand {
                    Some(pop(&mut stack)?)
                } else {
                    None
                };

                let mut matched = None;
                for (cond, result) in &whens {
                    let is_match = if let Some(ref op) = operand {
                        op.sql_cmp(cond)? == Some(Ordering::Equal)
                    } else {
                        is_truthy(cond)
                    };
                    if is_match {
                        matched = Some(result.clone());
                        break;
                    }
                }
                stack.push(
                    matched
                        .or(else_val)
                        .unwrap_or(Datum::Null),
                );
            }

            // --- BETWEEN ---
            ExprOp::Between { negated } => {
                let high = pop(&mut stack)?;
                let low = pop(&mut stack)?;
                let val = pop(&mut stack)?;
                let ge_low = val.sql_cmp(&low)?;
                let le_high = val.sql_cmp(&high)?;
                let in_range = match (ge_low, le_high) {
                    (Some(Ordering::Greater | Ordering::Equal), Some(Ordering::Less | Ordering::Equal)) => {
                        true
                    }
                    (Some(_), Some(_)) => false,
                    _ => {
                        stack.push(Datum::Null);
                        continue;
                    }
                };
                let result = if *negated { !in_range } else { in_range };
                stack.push(Datum::Boolean(result));
            }

            // --- IN list ---
            ExprOp::InList { len, negated } => {
                let items: Vec<Datum> = stack.drain(stack.len() - len..).collect();
                let val = pop(&mut stack)?;
                if val.is_null() {
                    stack.push(Datum::Null);
                } else {
                    let mut found = false;
                    let mut has_null = false;
                    for item in &items {
                        if item.is_null() {
                            has_null = true;
                            continue;
                        }
                        if val.sql_cmp(item)? == Some(Ordering::Equal) {
                            found = true;
                            break;
                        }
                    }
                    if found {
                        stack.push(Datum::Boolean(!*negated));
                    } else if has_null {
                        stack.push(Datum::Null);
                    } else {
                        stack.push(Datum::Boolean(*negated));
                    }
                }
            }

            // --- LIKE ---
            ExprOp::Like {
                negated,
                case_insensitive,
            } => {
                let pattern = pop(&mut stack)?;
                let val = pop(&mut stack)?;
                stack.push(eval_like(&val, &pattern, *negated, *case_insensitive)?);
            }
        }
    }

    stack.pop().ok_or_else(|| {
        SqlError::ExecutionError("expression produced no result".into()).into()
    })
}

// ---------------------------------------------------------------------------
// Helper: evaluate an expression against a tuple (convenience wrapper)
// ---------------------------------------------------------------------------

/// Compile and evaluate in one step.
pub fn eval_expr_direct(expr: &Expr, tuple: &Tuple, schema: &Schema) -> Result<Datum> {
    let ops = compile_expr(expr, schema)?;
    evaluate_expr(&ops, tuple)
}

/// Check if a datum is truthy (non-null, non-false).
pub fn is_truthy(d: &Datum) -> bool {
    match d {
        Datum::Boolean(b) => *b,
        Datum::Null => false,
        _ => true,
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn pop(stack: &mut Vec<Datum>) -> Result<Datum> {
    stack
        .pop()
        .ok_or_else(|| SqlError::ExecutionError("expression stack underflow".into()).into())
}

fn eval_cmp(a: &Datum, b: &Datum, target: Ordering, negate: bool) -> Result<Datum> {
    let cmp = a.sql_cmp(b)?;
    match cmp {
        None => Ok(Datum::Null), // NULL comparison
        Some(ord) => {
            let matched = ord == target;
            Ok(Datum::Boolean(if negate { !matched } else { matched }))
        }
    }
}

fn eval_cmp_le(a: &Datum, b: &Datum) -> Result<Datum> {
    match a.sql_cmp(b)? {
        None => Ok(Datum::Null),
        Some(ord) => Ok(Datum::Boolean(ord != Ordering::Greater)),
    }
}

fn eval_cmp_ge(a: &Datum, b: &Datum) -> Result<Datum> {
    match a.sql_cmp(b)? {
        None => Ok(Datum::Null),
        Some(ord) => Ok(Datum::Boolean(ord != Ordering::Less)),
    }
}

fn eval_and(a: &Datum, b: &Datum) -> Datum {
    let ab = to_bool(a);
    let bb = to_bool(b);
    match (ab, bb) {
        (Some(false), _) | (_, Some(false)) => Datum::Boolean(false),
        (Some(true), Some(true)) => Datum::Boolean(true),
        _ => Datum::Null,
    }
}

fn eval_or(a: &Datum, b: &Datum) -> Datum {
    let ab = to_bool(a);
    let bb = to_bool(b);
    match (ab, bb) {
        (Some(true), _) | (_, Some(true)) => Datum::Boolean(true),
        (Some(false), Some(false)) => Datum::Boolean(false),
        _ => Datum::Null,
    }
}

fn eval_not(a: &Datum) -> Datum {
    match to_bool(a) {
        Some(v) => Datum::Boolean(!v),
        None => Datum::Null,
    }
}

fn to_bool(d: &Datum) -> Option<bool> {
    match d {
        Datum::Boolean(b) => Some(*b),
        Datum::Null => None,
        Datum::Integer(v) => Some(*v != 0),
        Datum::BigInt(v) => Some(*v != 0),
        _ => Some(true),
    }
}

fn eval_mod(a: &Datum, b: &Datum) -> Result<Datum> {
    if a.is_null() || b.is_null() {
        return Ok(Datum::Null);
    }
    let (ca, cb) = Datum::coerce_pair(a, b)?;
    match (&ca, &cb) {
        (Datum::Integer(x), Datum::Integer(y)) => {
            if *y == 0 {
                return Err(TypeError::DivisionByZero.into());
            }
            Ok(Datum::Integer(x % y))
        }
        (Datum::BigInt(x), Datum::BigInt(y)) => {
            if *y == 0 {
                return Err(TypeError::DivisionByZero.into());
            }
            Ok(Datum::BigInt(x % y))
        }
        (Datum::Float(x), Datum::Float(y)) => {
            if *y == 0.0 {
                return Err(TypeError::DivisionByZero.into());
            }
            Ok(Datum::Float(x % y))
        }
        _ => Err(TypeError::TypeMismatch {
            expected: "numeric".into(),
            found: ca.type_name().into(),
        }
        .into()),
    }
}

fn eval_exp(a: &Datum, b: &Datum) -> Result<Datum> {
    if a.is_null() || b.is_null() {
        return Ok(Datum::Null);
    }
    let fa = datum_to_f64(a)?;
    let fb = datum_to_f64(b)?;
    Ok(Datum::Float(fa.powf(fb)))
}

fn eval_concat(a: &Datum, b: &Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::Text(format!("{a}{b}"))
}

fn eval_cast(d: Datum, target: &DataType) -> Result<Datum> {
    if d.is_null() {
        return Ok(Datum::Null);
    }
    // Try implicit coercion first
    if let Ok(coerced) = d.coerce_to(target) {
        return Ok(coerced);
    }
    // Explicit casts via string conversion
    let s = format!("{d}");
    match target {
        DataType::Integer => s
            .trim()
            .parse::<i32>()
            .map(Datum::Integer)
            .map_err(|_| {
                TypeError::InvalidCoercion {
                    from: d.type_name().into(),
                    to: target.to_string(),
                }
                .into()
            }),
        DataType::BigInt => s
            .trim()
            .parse::<i64>()
            .map(Datum::BigInt)
            .map_err(|_| {
                TypeError::InvalidCoercion {
                    from: d.type_name().into(),
                    to: target.to_string(),
                }
                .into()
            }),
        DataType::Float => s
            .trim()
            .parse::<f64>()
            .map(Datum::Float)
            .map_err(|_| {
                TypeError::InvalidCoercion {
                    from: d.type_name().into(),
                    to: target.to_string(),
                }
                .into()
            }),
        DataType::Boolean => match s.trim().to_ascii_lowercase().as_str() {
            "true" | "t" | "1" | "yes" => Ok(Datum::Boolean(true)),
            "false" | "f" | "0" | "no" => Ok(Datum::Boolean(false)),
            _ => Err(TypeError::InvalidCoercion {
                from: d.type_name().into(),
                to: "BOOLEAN".into(),
            }
            .into()),
        },
        DataType::Text => Ok(Datum::Text(s)),
        DataType::Varchar(n) => {
            let truncated: String = s.chars().take(*n as usize).collect();
            Ok(Datum::Varchar(truncated))
        }
        _ => d.coerce_to(target),
    }
}

pub fn datum_to_f64(d: &Datum) -> Result<f64> {
    match d {
        Datum::Integer(v) => Ok(*v as f64),
        Datum::BigInt(v) => Ok(*v as f64),
        Datum::Float(v) => Ok(*v),
        Datum::Decimal { mantissa, scale } => {
            Ok(*mantissa as f64 / 10f64.powi(*scale as i32))
        }
        Datum::Null => Ok(f64::NAN),
        _ => Err(TypeError::TypeMismatch {
            expected: "numeric".into(),
            found: d.type_name().into(),
        }
        .into()),
    }
}

fn eval_greatest(args: Vec<Datum>) -> Result<Datum> {
    let mut best: Option<Datum> = None;
    for d in args {
        if d.is_null() {
            continue;
        }
        best = Some(match best {
            None => d,
            Some(ref cur) => {
                if d.sql_cmp(cur)? == Some(Ordering::Greater) {
                    d
                } else {
                    cur.clone()
                }
            }
        });
    }
    Ok(best.unwrap_or(Datum::Null))
}

fn eval_least(args: Vec<Datum>) -> Result<Datum> {
    let mut best: Option<Datum> = None;
    for d in args {
        if d.is_null() {
            continue;
        }
        best = Some(match best {
            None => d,
            Some(ref cur) => {
                if d.sql_cmp(cur)? == Some(Ordering::Less) {
                    d
                } else {
                    cur.clone()
                }
            }
        });
    }
    Ok(best.unwrap_or(Datum::Null))
}

fn eval_like(
    val: &Datum,
    pattern: &Datum,
    negated: bool,
    case_insensitive: bool,
) -> Result<Datum> {
    if val.is_null() || pattern.is_null() {
        return Ok(Datum::Null);
    }
    let s = format!("{val}");
    let p = format!("{pattern}");

    let s_cmp = if case_insensitive { s.to_lowercase() } else { s };
    let p_cmp = if case_insensitive { p.to_lowercase() } else { p };
    let matched = like_match(&s_cmp, &p_cmp);
    Ok(Datum::Boolean(if negated { !matched } else { matched }))
}

/// SQL LIKE pattern matching: `%` = any sequence, `_` = any single char.
fn like_match(s: &str, pattern: &str) -> bool {
    let sb = s.as_bytes();
    let pb = pattern.as_bytes();
    let (sn, pn) = (sb.len(), pb.len());

    // dp[j] = can pattern[0..j] match s[0..current_i]
    let mut dp = vec![false; pn + 1];
    dp[0] = true;
    // Initialize for leading %
    for j in 1..=pn {
        if pb[j - 1] == b'%' {
            dp[j] = dp[j - 1];
        }
    }

    for i in 1..=sn {
        let mut prev = dp[0]; // dp_prev[i-1][j-1]
        dp[0] = false;
        for j in 1..=pn {
            let tmp = dp[j];
            if pb[j - 1] == b'%' {
                dp[j] = dp[j - 1] || dp[j]; // dp[j] is dp_prev row (match 0+ chars)
            } else if pb[j - 1] == b'_' || pb[j - 1] == sb[i - 1] {
                dp[j] = prev;
            } else {
                dp[j] = false;
            }
            prev = tmp;
        }
    }

    dp[pn]
}
