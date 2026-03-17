use std::fmt::Write;

use crate::sql::ast::{BinaryOp, Expr, JoinType};

use super::logical_plan::LogicalPlan;
use super::physical_plan::PhysicalPlan;

/// Format a logical plan as EXPLAIN output.
pub fn explain_logical(plan: &LogicalPlan) -> String {
    let mut output = String::new();
    format_logical(plan, 0, &mut output);
    output
}

/// Format a physical plan as EXPLAIN output.
pub fn explain_physical(plan: &PhysicalPlan) -> String {
    let mut output = String::new();
    format_physical(plan, 0, &mut output);
    output
}

/// Format a physical plan with EXPLAIN ANALYZE output (includes estimated costs).
pub fn explain_analyze(
    plan: &PhysicalPlan,
    costs: &[(f64, f64)], // (estimated_rows, estimated_cost) per node
) -> String {
    let mut output = String::new();
    format_analyze(plan, 0, costs, &mut 0, &mut output);
    output
}

// ---------------------------------------------------------------------------
// Logical plan formatting
// ---------------------------------------------------------------------------

fn format_logical(plan: &LogicalPlan, indent: usize, out: &mut String) {
    let prefix = "  ".repeat(indent);

    match plan {
        LogicalPlan::Scan { table, alias, schema } => {
            let _ = write!(out, "{}Scan: {}", prefix, table);
            if let Some(a) = alias {
                let _ = write!(out, " AS {}", a);
            }
            let _ = writeln!(out, " (cols: {})", schema.column_count());
        }
        LogicalPlan::Filter { predicate, input } => {
            let _ = writeln!(out, "{}Filter: {}", prefix, format_expr(predicate));
            format_logical(input, indent + 1, out);
        }
        LogicalPlan::Project { expressions, input } => {
            let cols: Vec<&str> = expressions.iter().map(|e| e.alias.as_str()).collect();
            let _ = writeln!(out, "{}Project: [{}]", prefix, cols.join(", "));
            format_logical(input, indent + 1, out);
        }
        LogicalPlan::Join {
            join_type,
            condition,
            left,
            right,
            ..
        } => {
            let _ = write!(out, "{}{} Join", prefix, format_join_type(*join_type));
            if let Some(cond) = condition {
                let _ = write!(out, " ON {}", format_expr(cond));
            }
            let _ = writeln!(out);
            format_logical(left, indent + 1, out);
            format_logical(right, indent + 1, out);
        }
        LogicalPlan::Aggregate {
            group_by,
            aggregates,
            input,
            ..
        } => {
            let gb: Vec<String> = group_by.iter().map(|e| format_expr(e)).collect();
            let aggs: Vec<String> = aggregates
                .iter()
                .map(|a| format!("{}({})", a.func, format_expr(&a.arg)))
                .collect();
            let _ = writeln!(
                out,
                "{}Aggregate: group_by=[{}], aggs=[{}]",
                prefix,
                gb.join(", "),
                aggs.join(", ")
            );
            format_logical(input, indent + 1, out);
        }
        LogicalPlan::Sort { order_by, input } => {
            let keys: Vec<String> = order_by
                .iter()
                .map(|s| {
                    format!(
                        "{} {}",
                        format_expr(&s.expr),
                        if s.ascending { "ASC" } else { "DESC" }
                    )
                })
                .collect();
            let _ = writeln!(out, "{}Sort: [{}]", prefix, keys.join(", "));
            format_logical(input, indent + 1, out);
        }
        LogicalPlan::Limit { count, offset, input } => {
            let _ = write!(out, "{}Limit:", prefix);
            if let Some(c) = count {
                let _ = write!(out, " count={}", c);
            }
            if *offset > 0 {
                let _ = write!(out, " offset={}", offset);
            }
            let _ = writeln!(out);
            format_logical(input, indent + 1, out);
        }
        LogicalPlan::Distinct { input } => {
            let _ = writeln!(out, "{}Distinct", prefix);
            format_logical(input, indent + 1, out);
        }
        LogicalPlan::Union { all, left, right } => {
            let _ = writeln!(
                out,
                "{}Union{}",
                prefix,
                if *all { " All" } else { "" }
            );
            format_logical(left, indent + 1, out);
            format_logical(right, indent + 1, out);
        }
        LogicalPlan::Intersect { all, left, right } => {
            let _ = writeln!(
                out,
                "{}Intersect{}",
                prefix,
                if *all { " All" } else { "" }
            );
            format_logical(left, indent + 1, out);
            format_logical(right, indent + 1, out);
        }
        LogicalPlan::Except { all, left, right } => {
            let _ = writeln!(
                out,
                "{}Except{}",
                prefix,
                if *all { " All" } else { "" }
            );
            format_logical(left, indent + 1, out);
            format_logical(right, indent + 1, out);
        }
        LogicalPlan::Insert { table, columns, input } => {
            let _ = writeln!(
                out,
                "{}Insert: {} ({})",
                prefix,
                table,
                columns.join(", ")
            );
            format_logical(input, indent + 1, out);
        }
        LogicalPlan::Update {
            table,
            assignments,
            input,
        } => {
            let sets: Vec<String> = assignments
                .iter()
                .map(|(c, e)| format!("{} = {}", c, format_expr(e)))
                .collect();
            let _ = writeln!(out, "{}Update: {} SET {}", prefix, table, sets.join(", "));
            format_logical(input, indent + 1, out);
        }
        LogicalPlan::Delete { table, input } => {
            let _ = writeln!(out, "{}Delete: {}", prefix, table);
            format_logical(input, indent + 1, out);
        }
        LogicalPlan::Values { rows, .. } => {
            let _ = writeln!(out, "{}Values: {} rows", prefix, rows.len());
        }
        LogicalPlan::Empty { .. } => {
            let _ = writeln!(out, "{}Empty", prefix);
        }
    }
}

// ---------------------------------------------------------------------------
// Physical plan formatting
// ---------------------------------------------------------------------------

fn format_physical(plan: &PhysicalPlan, indent: usize, out: &mut String) {
    let prefix = "  ".repeat(indent);

    match plan {
        PhysicalPlan::SeqScan {
            table,
            alias,
            predicate,
            ..
        } => {
            let _ = write!(out, "{}SeqScan: {}", prefix, table);
            if let Some(a) = alias {
                let _ = write!(out, " AS {}", a);
            }
            if let Some(pred) = predicate {
                let _ = write!(out, " WHERE {}", format_expr(pred));
            }
            let _ = writeln!(out);
        }
        PhysicalPlan::IndexScan {
            table,
            alias,
            index_name,
            key_ranges,
            predicate,
            ..
        } => {
            let _ = write!(
                out,
                "{}IndexScan: {} USING {} ({} ranges)",
                prefix,
                table,
                index_name,
                key_ranges.len()
            );
            if let Some(a) = alias {
                let _ = write!(out, " AS {}", a);
            }
            if let Some(pred) = predicate {
                let _ = write!(out, " WHERE {}", format_expr(pred));
            }
            let _ = writeln!(out);
        }
        PhysicalPlan::HashJoin {
            join_type,
            left_keys,
            right_keys,
            left,
            right,
            ..
        } => {
            let lk: Vec<String> = left_keys.iter().map(|e| format_expr(e)).collect();
            let rk: Vec<String> = right_keys.iter().map(|e| format_expr(e)).collect();
            let _ = writeln!(
                out,
                "{}HashJoin: {} ON [{}] = [{}]",
                prefix,
                format_join_type(*join_type),
                lk.join(", "),
                rk.join(", ")
            );
            format_physical(left, indent + 1, out);
            format_physical(right, indent + 1, out);
        }
        PhysicalPlan::SortMergeJoin {
            join_type,
            left_keys,
            right_keys,
            left,
            right,
            ..
        } => {
            let lk: Vec<String> = left_keys.iter().map(|e| format_expr(e)).collect();
            let rk: Vec<String> = right_keys.iter().map(|e| format_expr(e)).collect();
            let _ = writeln!(
                out,
                "{}SortMergeJoin: {} ON [{}] = [{}]",
                prefix,
                format_join_type(*join_type),
                lk.join(", "),
                rk.join(", ")
            );
            format_physical(left, indent + 1, out);
            format_physical(right, indent + 1, out);
        }
        PhysicalPlan::NestedLoopJoin {
            join_type,
            condition,
            left,
            right,
            ..
        } => {
            let _ = write!(
                out,
                "{}NestedLoopJoin: {}",
                prefix,
                format_join_type(*join_type)
            );
            if let Some(cond) = condition {
                let _ = write!(out, " ON {}", format_expr(cond));
            }
            let _ = writeln!(out);
            format_physical(left, indent + 1, out);
            format_physical(right, indent + 1, out);
        }
        PhysicalPlan::HashAggregate {
            group_by,
            aggregates,
            input,
            ..
        } => {
            let gb: Vec<String> = group_by.iter().map(|e| format_expr(e)).collect();
            let aggs: Vec<String> = aggregates
                .iter()
                .map(|a| format!("{}({})", a.func, format_expr(&a.arg)))
                .collect();
            let _ = writeln!(
                out,
                "{}HashAggregate: group_by=[{}], aggs=[{}]",
                prefix,
                gb.join(", "),
                aggs.join(", ")
            );
            format_physical(input, indent + 1, out);
        }
        PhysicalPlan::SortAggregate {
            group_by,
            aggregates,
            input,
            ..
        } => {
            let gb: Vec<String> = group_by.iter().map(|e| format_expr(e)).collect();
            let aggs: Vec<String> = aggregates
                .iter()
                .map(|a| format!("{}({})", a.func, format_expr(&a.arg)))
                .collect();
            let _ = writeln!(
                out,
                "{}SortAggregate: group_by=[{}], aggs=[{}]",
                prefix,
                gb.join(", "),
                aggs.join(", ")
            );
            format_physical(input, indent + 1, out);
        }
        PhysicalPlan::ExternalSort { order_by, input } => {
            let keys: Vec<String> = order_by
                .iter()
                .map(|s| {
                    format!(
                        "{} {}",
                        format_expr(&s.expr),
                        if s.ascending { "ASC" } else { "DESC" }
                    )
                })
                .collect();
            let _ = writeln!(out, "{}ExternalSort: [{}]", prefix, keys.join(", "));
            format_physical(input, indent + 1, out);
        }
        PhysicalPlan::HashDistinct { input } => {
            let _ = writeln!(out, "{}HashDistinct", prefix);
            format_physical(input, indent + 1, out);
        }
        PhysicalPlan::SortDistinct { input } => {
            let _ = writeln!(out, "{}SortDistinct", prefix);
            format_physical(input, indent + 1, out);
        }
        PhysicalPlan::Project { expressions, input } => {
            let cols: Vec<&str> = expressions.iter().map(|e| e.alias.as_str()).collect();
            let _ = writeln!(out, "{}Project: [{}]", prefix, cols.join(", "));
            format_physical(input, indent + 1, out);
        }
        PhysicalPlan::Filter { predicate, input } => {
            let _ = writeln!(out, "{}Filter: {}", prefix, format_expr(predicate));
            format_physical(input, indent + 1, out);
        }
        PhysicalPlan::Limit { count, offset, input } => {
            let _ = write!(out, "{}Limit:", prefix);
            if let Some(c) = count {
                let _ = write!(out, " count={}", c);
            }
            if *offset > 0 {
                let _ = write!(out, " offset={}", offset);
            }
            let _ = writeln!(out);
            format_physical(input, indent + 1, out);
        }
        PhysicalPlan::Union { all, left, right } => {
            let _ = writeln!(
                out,
                "{}Union{}",
                prefix,
                if *all { " All" } else { "" }
            );
            format_physical(left, indent + 1, out);
            format_physical(right, indent + 1, out);
        }
        PhysicalPlan::Intersect { all, left, right } => {
            let _ = writeln!(
                out,
                "{}Intersect{}",
                prefix,
                if *all { " All" } else { "" }
            );
            format_physical(left, indent + 1, out);
            format_physical(right, indent + 1, out);
        }
        PhysicalPlan::Except { all, left, right } => {
            let _ = writeln!(
                out,
                "{}Except{}",
                prefix,
                if *all { " All" } else { "" }
            );
            format_physical(left, indent + 1, out);
            format_physical(right, indent + 1, out);
        }
        PhysicalPlan::Insert { table, columns, input } => {
            let _ = writeln!(
                out,
                "{}Insert: {} ({})",
                prefix,
                table,
                columns.join(", ")
            );
            format_physical(input, indent + 1, out);
        }
        PhysicalPlan::Update {
            table,
            assignments,
            input,
        } => {
            let sets: Vec<String> = assignments
                .iter()
                .map(|(c, e)| format!("{} = {}", c, format_expr(e)))
                .collect();
            let _ = writeln!(out, "{}Update: {} SET {}", prefix, table, sets.join(", "));
            format_physical(input, indent + 1, out);
        }
        PhysicalPlan::Delete { table, input } => {
            let _ = writeln!(out, "{}Delete: {}", prefix, table);
            format_physical(input, indent + 1, out);
        }
        PhysicalPlan::Values { rows, .. } => {
            let _ = writeln!(out, "{}Values: {} rows", prefix, rows.len());
        }
        PhysicalPlan::Empty { .. } => {
            let _ = writeln!(out, "{}Empty", prefix);
        }
    }
}

// ---------------------------------------------------------------------------
// EXPLAIN ANALYZE formatting
// ---------------------------------------------------------------------------

fn format_analyze(
    plan: &PhysicalPlan,
    indent: usize,
    costs: &[(f64, f64)],
    node_idx: &mut usize,
    out: &mut String,
) {
    let prefix = "  ".repeat(indent);
    let (est_rows, est_cost) = costs.get(*node_idx).copied().unwrap_or((0.0, 0.0));
    *node_idx += 1;

    let _ = write!(out, "{}{}", prefix, plan.node_name());
    let _ = write!(out, "  (cost={:.2} rows={:.0})", est_cost, est_rows);

    // Add node-specific detail
    match plan {
        PhysicalPlan::SeqScan { table, .. } => {
            let _ = write!(out, " on {}", table);
        }
        PhysicalPlan::IndexScan {
            table, index_name, ..
        } => {
            let _ = write!(out, " on {} using {}", table, index_name);
        }
        PhysicalPlan::HashJoin { join_type, .. }
        | PhysicalPlan::SortMergeJoin { join_type, .. }
        | PhysicalPlan::NestedLoopJoin { join_type, .. } => {
            let _ = write!(out, " {}", format_join_type(*join_type));
        }
        _ => {}
    }

    let _ = writeln!(out);

    for child in plan.children() {
        format_analyze(child, indent + 1, costs, node_idx, out);
    }
}

// ---------------------------------------------------------------------------
// Expression formatting
// ---------------------------------------------------------------------------

pub fn format_expr(expr: &Expr) -> String {
    match expr {
        Expr::Integer(n) => n.to_string(),
        Expr::Float(n) => format!("{}", n),
        Expr::String(s) => format!("'{}'", s),
        Expr::Boolean(b) => b.to_string(),
        Expr::Null => "NULL".to_string(),
        Expr::Identifier(name) => name.clone(),
        Expr::QualifiedIdentifier { table, column } => format!("{}.{}", table, column),
        Expr::Star => "*".to_string(),
        Expr::QualifiedStar(table) => format!("{}.*", table),
        Expr::BinaryOp { left, op, right } => {
            format!("{} {} {}", format_expr(left), format_binop(*op), format_expr(right))
        }
        Expr::UnaryOp { op, expr } => {
            let op_str = match op {
                crate::sql::ast::UnaryOp::Plus => "+",
                crate::sql::ast::UnaryOp::Minus => "-",
                crate::sql::ast::UnaryOp::Not => "NOT ",
            };
            format!("{}{}", op_str, format_expr(expr))
        }
        Expr::IsNull { expr, negated } => {
            if *negated {
                format!("{} IS NOT NULL", format_expr(expr))
            } else {
                format!("{} IS NULL", format_expr(expr))
            }
        }
        Expr::InList { expr, list, negated } => {
            let items: Vec<String> = list.iter().map(|e| format_expr(e)).collect();
            format!(
                "{} {}IN ({})",
                format_expr(expr),
                if *negated { "NOT " } else { "" },
                items.join(", ")
            )
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            format!(
                "{} {}BETWEEN {} AND {}",
                format_expr(expr),
                if *negated { "NOT " } else { "" },
                format_expr(low),
                format_expr(high)
            )
        }
        Expr::Like {
            expr,
            pattern,
            negated,
            ..
        } => {
            format!(
                "{} {}LIKE {}",
                format_expr(expr),
                if *negated { "NOT " } else { "" },
                format_expr(pattern)
            )
        }
        Expr::FunctionCall { name, args, distinct } => {
            let arg_strs: Vec<String> = args.iter().map(|a| format_expr(a)).collect();
            if *distinct {
                format!("{}(DISTINCT {})", name, arg_strs.join(", "))
            } else {
                format!("{}({})", name, arg_strs.join(", "))
            }
        }
        Expr::Cast { expr, data_type } => {
            format!("CAST({} AS {})", format_expr(expr), data_type)
        }
        Expr::Exists { negated, .. } => {
            if *negated {
                "NOT EXISTS (...)".to_string()
            } else {
                "EXISTS (...)".to_string()
            }
        }
        _ => format!("{:?}", expr),
    }
}

fn format_binop(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Add => "+",
        BinaryOp::Sub => "-",
        BinaryOp::Mul => "*",
        BinaryOp::Div => "/",
        BinaryOp::Mod => "%",
        BinaryOp::Exp => "^",
        BinaryOp::Concat => "||",
        BinaryOp::Eq => "=",
        BinaryOp::NotEq => "!=",
        BinaryOp::Lt => "<",
        BinaryOp::Gt => ">",
        BinaryOp::LtEq => "<=",
        BinaryOp::GtEq => ">=",
        BinaryOp::And => "AND",
        BinaryOp::Or => "OR",
    }
}

fn format_join_type(jt: JoinType) -> &'static str {
    match jt {
        JoinType::Inner => "Inner",
        JoinType::Left => "Left",
        JoinType::Right => "Right",
        JoinType::Full => "Full",
        JoinType::Cross => "Cross",
        JoinType::LeftSemi => "LeftSemi",
        JoinType::LeftAnti => "LeftAnti",
    }
}
