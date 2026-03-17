use crate::sql::ast::*;
use crate::types::{Column, DataType, Schema};
use crate::utils::error::{Error, SqlError};

use super::logical_plan::*;
use super::Catalog;

/// Converts parsed AST statements into logical plan trees.
pub struct PlanBuilder<'a> {
    catalog: &'a Catalog,
}

impl<'a> PlanBuilder<'a> {
    pub fn new(catalog: &'a Catalog) -> Self {
        Self { catalog }
    }

    /// Build a logical plan from a parsed statement.
    pub fn build(&self, stmt: &Statement) -> Result<LogicalPlan, Error> {
        match stmt {
            Statement::Select(sel) => self.build_select(sel),
            Statement::Insert(ins) => self.build_insert(ins),
            Statement::Update(upd) => self.build_update(upd),
            Statement::Delete(del) => self.build_delete(del),
            Statement::Explain { analyze: _, statement } => {
                // For plan building, we just build the inner statement's plan.
                self.build(statement)
            }
            _ => Err(Error::Sql(SqlError::PlanError(format!(
                "unsupported statement for planning: {:?}",
                std::mem::discriminant(stmt)
            )))),
        }
    }

    // -----------------------------------------------------------------------
    // SELECT
    // -----------------------------------------------------------------------

    fn build_select(&self, sel: &Select) -> Result<LogicalPlan, Error> {
        // 1. FROM clause → base relation (scans + joins)
        let mut plan = self.build_from(&sel.from)?;

        // 2. WHERE → Filter
        if let Some(ref where_clause) = sel.where_clause {
            plan = LogicalPlan::Filter {
                predicate: where_clause.clone(),
                input: Box::new(plan),
            };
        }

        // 3. GROUP BY / aggregates → Aggregate
        if !sel.group_by.is_empty() || self.has_aggregates(&sel.columns) {
            plan = self.build_aggregate(&sel.group_by, &sel.columns, plan)?;

            // 4. HAVING → Filter (after aggregation)
            if let Some(ref having) = sel.having {
                plan = LogicalPlan::Filter {
                    predicate: having.clone(),
                    input: Box::new(plan),
                };
            }
        }

        // 5. ORDER BY → Sort (before projection so sort can see all source columns)
        if !sel.order_by.is_empty() {
            plan = self.build_sort(&sel.order_by, plan);
        }

        // 6. SELECT list → Project
        plan = self.build_projection(&sel.columns, plan)?;

        // 7. DISTINCT → Distinct
        if sel.distinct {
            plan = LogicalPlan::Distinct {
                input: Box::new(plan),
            };
        }

        // 8. LIMIT/OFFSET → Limit
        if sel.limit.is_some() || sel.offset.is_some() {
            let count = sel.limit.as_ref().and_then(|e| self.expr_to_usize(e));
            let offset = sel
                .offset
                .as_ref()
                .and_then(|e| self.expr_to_usize(e))
                .unwrap_or(0);
            plan = LogicalPlan::Limit {
                count,
                offset,
                input: Box::new(plan),
            };
        }

        // 9. Set operations (UNION / INTERSECT / EXCEPT)
        if let Some(ref set_op) = sel.set_op {
            let right = self.build_select(&set_op.right)?;
            plan = match set_op.op {
                SetOperator::Union => LogicalPlan::Union {
                    all: set_op.all,
                    left: Box::new(plan),
                    right: Box::new(right),
                },
                SetOperator::Intersect => LogicalPlan::Intersect {
                    all: set_op.all,
                    left: Box::new(plan),
                    right: Box::new(right),
                },
                SetOperator::Except => LogicalPlan::Except {
                    all: set_op.all,
                    left: Box::new(plan),
                    right: Box::new(right),
                },
            };
        }

        Ok(plan)
    }

    // -----------------------------------------------------------------------
    // FROM clause
    // -----------------------------------------------------------------------

    fn build_from(&self, from: &Option<FromClause>) -> Result<LogicalPlan, Error> {
        let from = match from {
            Some(f) => f,
            None => {
                return Ok(LogicalPlan::Empty {
                    schema: Schema::empty(),
                });
            }
        };

        let mut plan = self.build_table_ref(&from.table)?;

        for join in &from.joins {
            let right = self.build_table_ref(&join.table)?;
            let condition = match &join.condition {
                Some(JoinCondition::On(expr)) => Some(expr.clone()),
                Some(JoinCondition::Using(cols)) => {
                    Some(self.using_to_on_condition(cols, &plan, &right))
                }
                None => None,
            };

            let schema = if join.join_type == JoinType::Cross || matches!(join.join_type, JoinType::Inner) {
                plan.schema().merge(&right.schema())
            } else {
                // For outer joins, all columns are nullable
                let left_schema = plan.schema();
                let right_schema = right.schema();
                left_schema.merge(&right_schema)
            };

            plan = LogicalPlan::Join {
                join_type: join.join_type,
                condition,
                left: Box::new(plan),
                right: Box::new(right),
                schema,
            };
        }

        Ok(plan)
    }

    fn build_table_ref(&self, table_ref: &TableRef) -> Result<LogicalPlan, Error> {
        match table_ref {
            TableRef::Table { name, alias } => {
                let schema = self
                    .catalog
                    .get_schema(name)
                    .cloned()
                    .unwrap_or_else(|| {
                        // If table not in catalog, create a minimal schema
                        Schema::empty()
                    });
                Ok(LogicalPlan::Scan {
                    table: name.clone(),
                    alias: alias.clone(),
                    schema,
                })
            }
            TableRef::Subquery { query, alias: _ } => {
                let sub_plan = self.build_select(query)?;
                // The subquery's output becomes the "scan" schema
                Ok(sub_plan)
            }
        }
    }

    fn using_to_on_condition(
        &self,
        cols: &[String],
        _left: &LogicalPlan,
        _right: &LogicalPlan,
    ) -> Expr {
        let conditions: Vec<Expr> = cols
            .iter()
            .map(|col| {
                Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(col.clone())),
                    op: BinaryOp::Eq,
                    right: Box::new(Expr::Identifier(col.clone())),
                }
            })
            .collect();
        combine_conjunction(&conditions).unwrap()
    }

    // -----------------------------------------------------------------------
    // Projection
    // -----------------------------------------------------------------------

    fn build_projection(
        &self,
        columns: &[SelectColumn],
        input: LogicalPlan,
    ) -> Result<LogicalPlan, Error> {
        let input_schema = input.schema();
        let mut expressions = Vec::new();

        for col in columns {
            match col {
                SelectColumn::Expr { expr, alias } => {
                    let name = alias.clone().unwrap_or_else(|| self.expr_name(expr));
                    expressions.push(ProjectionExpr {
                        expr: expr.clone(),
                        alias: name,
                    });
                }
                SelectColumn::AllColumns => {
                    for c in input_schema.columns() {
                        expressions.push(ProjectionExpr {
                            expr: Expr::Identifier(c.name.clone()),
                            alias: c.name.clone(),
                        });
                    }
                }
                SelectColumn::TableAllColumns(table) => {
                    for c in input_schema.columns() {
                        // Include columns from this table
                        expressions.push(ProjectionExpr {
                            expr: Expr::QualifiedIdentifier {
                                table: table.clone(),
                                column: c.name.clone(),
                            },
                            alias: c.name.clone(),
                        });
                    }
                }
            }
        }

        Ok(LogicalPlan::Project {
            expressions,
            input: Box::new(input),
        })
    }

    // -----------------------------------------------------------------------
    // Aggregation
    // -----------------------------------------------------------------------

    fn build_aggregate(
        &self,
        group_by: &[Expr],
        columns: &[SelectColumn],
        input: LogicalPlan,
    ) -> Result<LogicalPlan, Error> {
        let mut aggregates = Vec::new();
        let mut schema_cols = Vec::new();

        // Group-by columns become output columns
        for gb_expr in group_by {
            let name = self.expr_name(gb_expr);
            let dt = LogicalPlan::infer_expr_type(gb_expr);
            schema_cols.push(Column::new(name, dt, true));
        }

        // Extract aggregate functions from SELECT columns
        for col in columns {
            if let SelectColumn::Expr { expr, alias } = col {
                self.extract_aggregates(expr, alias, &mut aggregates, &mut schema_cols);
            }
        }

        let schema = Schema::new(schema_cols);
        Ok(LogicalPlan::Aggregate {
            group_by: group_by.to_vec(),
            aggregates,
            input: Box::new(input),
            schema,
        })
    }

    fn extract_aggregates(
        &self,
        expr: &Expr,
        alias: &Option<String>,
        aggregates: &mut Vec<AggregateExpr>,
        schema_cols: &mut Vec<Column>,
    ) {
        match expr {
            Expr::FunctionCall { name, args, distinct } => {
                if let Some(func) = AggregateFunc::from_name(name) {
                    let arg = args.first().cloned().unwrap_or(Expr::Star);
                    let alias_name = alias
                        .clone()
                        .unwrap_or_else(|| format!("{}({})", name, self.expr_name(&arg)));
                    let input_type = LogicalPlan::infer_expr_type(&arg);
                    let return_type = func.return_type(input_type);
                    aggregates.push(AggregateExpr {
                        func,
                        arg,
                        distinct: *distinct,
                        alias: alias_name.clone(),
                    });
                    schema_cols.push(Column::new(alias_name, return_type, true));
                }
            }
            _ => {
                // Non-aggregate expression in SELECT with GROUP BY is allowed
                // if it's a group-by column (validation happens elsewhere)
            }
        }
    }

    fn has_aggregates(&self, columns: &[SelectColumn]) -> bool {
        for col in columns {
            if let SelectColumn::Expr { expr, .. } = col {
                if self.expr_has_aggregate(expr) {
                    return true;
                }
            }
        }
        false
    }

    fn expr_has_aggregate(&self, expr: &Expr) -> bool {
        match expr {
            Expr::FunctionCall { name, .. } => AggregateFunc::from_name(name).is_some(),
            Expr::BinaryOp { left, right, .. } => {
                self.expr_has_aggregate(left) || self.expr_has_aggregate(right)
            }
            Expr::UnaryOp { expr, .. } => self.expr_has_aggregate(expr),
            Expr::Cast { expr, .. } => self.expr_has_aggregate(expr),
            _ => false,
        }
    }

    // -----------------------------------------------------------------------
    // Sort
    // -----------------------------------------------------------------------

    fn build_sort(&self, order_by: &[OrderByItem], input: LogicalPlan) -> LogicalPlan {
        let sort_exprs: Vec<SortExpr> = order_by
            .iter()
            .map(|item| {
                let ascending = match item.direction {
                    Some(OrderDirection::Desc) => false,
                    _ => true,
                };
                let nulls_first = match item.nulls {
                    Some(NullsOrder::First) => true,
                    Some(NullsOrder::Last) => false,
                    None => !ascending, // default: NULLS LAST for ASC, FIRST for DESC
                };
                SortExpr {
                    expr: item.expr.clone(),
                    ascending,
                    nulls_first,
                }
            })
            .collect();

        LogicalPlan::Sort {
            order_by: sort_exprs,
            input: Box::new(input),
        }
    }

    // -----------------------------------------------------------------------
    // INSERT
    // -----------------------------------------------------------------------

    fn build_insert(&self, ins: &Insert) -> Result<LogicalPlan, Error> {
        let columns = ins.columns.clone().unwrap_or_default();
        let input = match &ins.source {
            InsertSource::Values(rows) => {
                let schema = self.values_schema(&columns, rows);
                LogicalPlan::Values {
                    rows: rows.clone(),
                    schema,
                }
            }
            InsertSource::Select(query) => self.build_select(query)?,
        };

        Ok(LogicalPlan::Insert {
            table: ins.table.clone(),
            columns,
            input: Box::new(input),
        })
    }

    fn values_schema(&self, columns: &[String], rows: &[Vec<Expr>]) -> Schema {
        if columns.is_empty() && rows.is_empty() {
            return Schema::empty();
        }
        // Try to infer column types from the first row
        let first_row = rows.first();
        let num_cols = columns
            .len()
            .max(first_row.map(|r| r.len()).unwrap_or(0));

        let cols: Vec<Column> = (0..num_cols)
            .map(|i| {
                let name = columns
                    .get(i)
                    .cloned()
                    .unwrap_or_else(|| format!("column{}", i));
                let dt = first_row
                    .and_then(|r| r.get(i))
                    .map(|e| LogicalPlan::infer_expr_type(e))
                    .unwrap_or(DataType::Text);
                Column::new(name, dt, true)
            })
            .collect();
        Schema::new(cols)
    }

    // -----------------------------------------------------------------------
    // UPDATE
    // -----------------------------------------------------------------------

    fn build_update(&self, upd: &Update) -> Result<LogicalPlan, Error> {
        let schema = self
            .catalog
            .get_schema(&upd.table)
            .cloned()
            .unwrap_or_else(Schema::empty);

        let mut plan: LogicalPlan = LogicalPlan::Scan {
            table: upd.table.clone(),
            alias: None,
            schema,
        };

        if let Some(ref where_clause) = upd.where_clause {
            plan = LogicalPlan::Filter {
                predicate: where_clause.clone(),
                input: Box::new(plan),
            };
        }

        let assignments: Vec<(String, Expr)> = upd
            .assignments
            .iter()
            .map(|a| (a.column.clone(), a.value.clone()))
            .collect();

        Ok(LogicalPlan::Update {
            table: upd.table.clone(),
            assignments,
            input: Box::new(plan),
        })
    }

    // -----------------------------------------------------------------------
    // DELETE
    // -----------------------------------------------------------------------

    fn build_delete(&self, del: &Delete) -> Result<LogicalPlan, Error> {
        let schema = self
            .catalog
            .get_schema(&del.table)
            .cloned()
            .unwrap_or_else(Schema::empty);

        let mut plan: LogicalPlan = LogicalPlan::Scan {
            table: del.table.clone(),
            alias: None,
            schema,
        };

        if let Some(ref where_clause) = del.where_clause {
            plan = LogicalPlan::Filter {
                predicate: where_clause.clone(),
                input: Box::new(plan),
            };
        }

        Ok(LogicalPlan::Delete {
            table: del.table.clone(),
            input: Box::new(plan),
        })
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn expr_name(&self, expr: &Expr) -> String {
        match expr {
            Expr::Identifier(name) => name.clone(),
            Expr::QualifiedIdentifier { table, column } => format!("{}.{}", table, column),
            Expr::Integer(n) => n.to_string(),
            Expr::Float(n) => n.to_string(),
            Expr::String(s) => format!("'{}'", s),
            Expr::Boolean(b) => b.to_string(),
            Expr::Star => "*".to_string(),
            Expr::FunctionCall { name, args, .. } => {
                let arg_names: Vec<String> = args.iter().map(|a| self.expr_name(a)).collect();
                format!("{}({})", name, arg_names.join(", "))
            }
            _ => "?expr".to_string(),
        }
    }

    fn expr_to_usize(&self, expr: &Expr) -> Option<usize> {
        match expr {
            Expr::Integer(n) if *n >= 0 => Some(*n as usize),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::parse;

    fn make_catalog() -> Catalog {
        let mut catalog = Catalog::new();
        catalog.add_table(
            "users",
            Schema::new(vec![
                Column::new("id", DataType::Integer, false),
                Column::new("name", DataType::Varchar(255), false),
                Column::new("age", DataType::Integer, true),
            ]),
        );
        catalog.add_table(
            "orders",
            Schema::new(vec![
                Column::new("id", DataType::Integer, false),
                Column::new("user_id", DataType::Integer, false),
                Column::new("amount", DataType::Float, false),
            ]),
        );
        catalog
    }

    fn build_plan(sql: &str) -> LogicalPlan {
        let catalog = make_catalog();
        let builder = PlanBuilder::new(&catalog);
        let result = parse(sql);
        assert!(result.errors.is_empty(), "parse errors: {:?}", result.errors);
        let stmt = result.statements.into_iter().next().unwrap();
        builder.build(&stmt).unwrap()
    }

    #[test]
    fn test_simple_select() {
        let plan = build_plan("SELECT * FROM users");
        assert!(matches!(plan, LogicalPlan::Project { .. }));
    }

    #[test]
    fn test_select_with_where() {
        let plan = build_plan("SELECT * FROM users WHERE age > 18");
        // Should be Project -> Filter -> Scan
        match plan {
            LogicalPlan::Project { input, .. } => {
                assert!(matches!(*input, LogicalPlan::Filter { .. }));
            }
            _ => panic!("expected Project"),
        }
    }

    #[test]
    fn test_join() {
        let plan = build_plan(
            "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id",
        );
        // Project -> Join -> (Scan, Scan)
        match plan {
            LogicalPlan::Project { input, .. } => match *input {
                LogicalPlan::Join { join_type, .. } => {
                    assert_eq!(join_type, JoinType::Inner);
                }
                _ => panic!("expected Join"),
            },
            _ => panic!("expected Project"),
        }
    }

    #[test]
    fn test_insert_values() {
        let plan = build_plan("INSERT INTO users (id, name) VALUES (1, 'Alice')");
        assert!(matches!(plan, LogicalPlan::Insert { .. }));
    }
}
