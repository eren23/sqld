use std::sync::Arc;

use crate::executor::executor::{build_executor, ExecutorContext};
use crate::planner::optimizer::Optimizer;
use crate::planner::physical_planner::PhysicalPlanner;
use crate::planner::plan_builder::PlanBuilder;
use crate::protocol::connection::Session;
use crate::protocol::messages::{
    datum_to_text, BackendMessage, ErrorFields, FieldDescription, Severity, TransactionState,
};
use crate::sql::ast::Statement;
use crate::sql::parser;
use crate::types::DataType;

/// Handle a simple query: parse, plan, execute, and return messages.
///
/// Returns a list of backend messages to send to the client.
pub fn handle_simple_query(sql: &str, session: &mut Session) -> Vec<BackendMessage> {
    let mut messages = Vec::new();

    // Empty query
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        messages.push(BackendMessage::EmptyQueryResponse);
        messages.push(BackendMessage::ReadyForQuery {
            state: session.txn_state,
        });
        return messages;
    }

    // Parse
    let parse_result = parser::parse(sql);
    if parse_result.has_errors() {
        session.txn_state = if session.txn_state == TransactionState::InBlock {
            TransactionState::Failed
        } else {
            session.txn_state
        };
        let err_msg = parse_result
            .errors
            .iter()
            .map(|e| format!("{e}"))
            .collect::<Vec<_>>()
            .join("; ");
        messages.push(BackendMessage::ErrorResponse(
            ErrorFields::syntax_error(err_msg).with_position(1),
        ));
        messages.push(BackendMessage::ReadyForQuery {
            state: session.txn_state,
        });
        return messages;
    }

    for stmt in &parse_result.statements {
        let result = execute_statement(&stmt, session);
        match result {
            Ok(mut msgs) => messages.append(&mut msgs),
            Err(err) => {
                session.txn_state = if session.txn_state == TransactionState::InBlock {
                    TransactionState::Failed
                } else {
                    session.txn_state
                };
                messages.push(BackendMessage::ErrorResponse(err));
                break;
            }
        }
    }

    messages.push(BackendMessage::ReadyForQuery {
        state: session.txn_state,
    });
    messages
}

/// Execute a single parsed statement and return backend messages.
pub fn execute_statement(
    stmt: &Statement,
    session: &mut Session,
) -> Result<Vec<BackendMessage>, ErrorFields> {
    match stmt {
        Statement::Begin => {
            if session.txn_state == TransactionState::InBlock {
                return Err(ErrorFields::invalid_transaction_state(
                    "there is already a transaction in progress",
                ));
            }
            session.txn_state = TransactionState::InBlock;
            Ok(vec![BackendMessage::CommandComplete {
                tag: "BEGIN".to_string(),
            }])
        }
        Statement::Commit => {
            if session.txn_state == TransactionState::Idle {
                return Err(ErrorFields::invalid_transaction_state(
                    "there is no transaction in progress",
                ));
            }
            session.txn_state = TransactionState::Idle;
            Ok(vec![BackendMessage::CommandComplete {
                tag: "COMMIT".to_string(),
            }])
        }
        Statement::Rollback { savepoint } => {
            if session.txn_state == TransactionState::Idle && savepoint.is_none() {
                return Err(ErrorFields::invalid_transaction_state(
                    "there is no transaction in progress",
                ));
            }
            if savepoint.is_some() {
                Ok(vec![BackendMessage::CommandComplete {
                    tag: "ROLLBACK".to_string(),
                }])
            } else {
                session.txn_state = TransactionState::Idle;
                Ok(vec![BackendMessage::CommandComplete {
                    tag: "ROLLBACK".to_string(),
                }])
            }
        }
        Statement::Savepoint { name: _name } => {
            if session.txn_state != TransactionState::InBlock {
                return Err(ErrorFields::invalid_transaction_state(
                    "SAVEPOINT can only be used in transaction blocks",
                ));
            }
            Ok(vec![BackendMessage::CommandComplete {
                tag: format!("SAVEPOINT"),
            }])
        }
        Statement::ShowTables => {
            let catalog = session.catalog.lock().unwrap();
            let mut table_names: Vec<&String> = catalog.tables.keys().collect();
            table_names.sort();

            let fields = vec![FieldDescription::new("table_name", &DataType::Text)];
            let mut messages = vec![BackendMessage::RowDescription { fields }];

            for name in &table_names {
                messages.push(BackendMessage::DataRow {
                    values: vec![Some(name.as_bytes().to_vec())],
                });
            }

            messages.push(BackendMessage::CommandComplete {
                tag: format!("SELECT {}", table_names.len()),
            });
            Ok(messages)
        }
        Statement::ShowColumns { table } => {
            let catalog = session.catalog.lock().unwrap();
            let schema = catalog.get_schema(table).ok_or_else(|| {
                ErrorFields::undefined_table(format!("table \"{table}\" does not exist"))
            })?;

            let fields = vec![
                FieldDescription::new("column_name", &DataType::Text),
                FieldDescription::new("data_type", &DataType::Text),
            ];
            let mut messages = vec![BackendMessage::RowDescription { fields }];

            let col_count = schema.columns().len();
            for col in schema.columns() {
                messages.push(BackendMessage::DataRow {
                    values: vec![
                        Some(col.name.as_bytes().to_vec()),
                        Some(col.data_type.to_string().into_bytes()),
                    ],
                });
            }

            messages.push(BackendMessage::CommandComplete {
                tag: format!("SELECT {col_count}"),
            });
            Ok(messages)
        }
        Statement::Select(_select) => execute_query_plan(stmt, session),
        Statement::Insert(_) => execute_query_plan(stmt, session),
        Statement::Update(_) => execute_query_plan(stmt, session),
        Statement::Delete(_) => execute_query_plan(stmt, session),
        Statement::CreateTable(ct) => {
            let mut catalog = session.catalog.lock().unwrap();
            if catalog.tables.contains_key(&ct.name) {
                if ct.if_not_exists {
                    return Ok(vec![BackendMessage::NoticeResponse(
                        ErrorFields::new(
                            Severity::Notice,
                            "42P07",
                            format!("relation \"{}\" already exists, skipping", ct.name),
                        ),
                    ), BackendMessage::CommandComplete {
                        tag: "CREATE TABLE".to_string(),
                    }]);
                }
                return Err(ErrorFields::new(
                    Severity::Error,
                    "42P07",
                    format!("relation \"{}\" already exists", ct.name),
                ));
            }
            let schema = crate::types::Schema::new(
                ct.columns
                    .iter()
                    .map(|c| crate::types::Column::new(
                        c.name.clone(),
                        c.data_type,
                        c.constraints.iter().any(|con| {
                            matches!(con, crate::sql::ast::ColumnConstraint::Null)
                        }) || !c.constraints.iter().any(|con| {
                            matches!(con, crate::sql::ast::ColumnConstraint::NotNull
                                | crate::sql::ast::ColumnConstraint::PrimaryKey)
                        }),
                    ))
                    .collect(),
            );
            catalog.add_table(ct.name.clone(), schema);
            Ok(vec![BackendMessage::CommandComplete {
                tag: "CREATE TABLE".to_string(),
            }])
        }
        Statement::DropTable(dt) => {
            let mut catalog = session.catalog.lock().unwrap();
            if !catalog.tables.contains_key(&dt.name) {
                if dt.if_exists {
                    return Ok(vec![BackendMessage::NoticeResponse(
                        ErrorFields::new(
                            Severity::Notice,
                            "00000",
                            format!("table \"{}\" does not exist, skipping", dt.name),
                        ),
                    ), BackendMessage::CommandComplete {
                        tag: "DROP TABLE".to_string(),
                    }]);
                }
                return Err(ErrorFields::undefined_table(format!(
                    "table \"{}\" does not exist",
                    dt.name
                )));
            }
            catalog.tables.remove(&dt.name);
            Ok(vec![BackendMessage::CommandComplete {
                tag: "DROP TABLE".to_string(),
            }])
        }
        Statement::CreateView(cv) => {
            // Store view as a table entry with special handling
            let mut catalog = session.catalog.lock().unwrap();
            if catalog.tables.contains_key(&cv.name) {
                return Err(ErrorFields::new(
                    Severity::Error,
                    "42P07",
                    format!("relation \"{}\" already exists", cv.name),
                ));
            }
            // We need to resolve the view query to determine its schema
            // For simplicity, store an empty schema and resolve on use
            let schema = crate::types::Schema::empty();
            catalog.add_table(cv.name.clone(), schema);
            Ok(vec![BackendMessage::CommandComplete {
                tag: "CREATE VIEW".to_string(),
            }])
        }
        Statement::DropView(dv) => {
            let mut catalog = session.catalog.lock().unwrap();
            if !catalog.tables.contains_key(&dv.name) {
                if dv.if_exists {
                    return Ok(vec![BackendMessage::CommandComplete {
                        tag: "DROP VIEW".to_string(),
                    }]);
                }
                return Err(ErrorFields::undefined_table(format!(
                    "view \"{}\" does not exist",
                    dv.name
                )));
            }
            catalog.tables.remove(&dv.name);
            Ok(vec![BackendMessage::CommandComplete {
                tag: "DROP VIEW".to_string(),
            }])
        }
        Statement::CreateIndex(ci) => {
            let catalog = session.catalog.lock().unwrap();
            if !catalog.tables.contains_key(&ci.table) {
                return Err(ErrorFields::undefined_table(format!(
                    "table \"{}\" does not exist",
                    ci.table
                )));
            }
            drop(catalog);
            let mut catalog = session.catalog.lock().unwrap();
            let idx_info = crate::planner::IndexInfo {
                name: ci.name.clone(),
                table: ci.table.clone(),
                columns: ci.columns.iter().map(|c| c.name.clone()).collect(),
                unique: ci.unique,
                method: ci
                    .using_method
                    .unwrap_or(crate::sql::ast::IndexMethod::BTree),
            };
            catalog.add_index(idx_info);
            Ok(vec![BackendMessage::CommandComplete {
                tag: "CREATE INDEX".to_string(),
            }])
        }
        Statement::DropIndex(di) => {
            let mut catalog = session.catalog.lock().unwrap();
            let before = catalog.indexes.len();
            catalog.indexes.retain(|i| i.name != di.name);
            if catalog.indexes.len() == before && !di.if_exists {
                return Err(ErrorFields::new(
                    Severity::Error,
                    "42704",
                    format!("index \"{}\" does not exist", di.name),
                ));
            }
            Ok(vec![BackendMessage::CommandComplete {
                tag: "DROP INDEX".to_string(),
            }])
        }
        Statement::Explain { analyze: _analyze, statement } => {
            let catalog = session.catalog.lock().unwrap();
            let plan_builder = PlanBuilder::new(&catalog);

            let logical = plan_builder.build(statement).map_err(|e| {
                ErrorFields::internal(format!("plan error: {e}"))
            })?;

            let explain_text = crate::planner::explain::explain_logical(&logical);

            let fields = vec![FieldDescription::new("QUERY PLAN", &DataType::Text)];
            let mut messages = vec![BackendMessage::RowDescription { fields }];

            for line in explain_text.lines() {
                messages.push(BackendMessage::DataRow {
                    values: vec![Some(line.as_bytes().to_vec())],
                });
            }

            messages.push(BackendMessage::CommandComplete {
                tag: format!("EXPLAIN"),
            });
            Ok(messages)
        }
        Statement::Analyze { table: _table } => {
            Ok(vec![BackendMessage::CommandComplete {
                tag: "ANALYZE".to_string(),
            }])
        }
        Statement::Vacuum { table: _table } => {
            Ok(vec![BackendMessage::CommandComplete {
                tag: "VACUUM".to_string(),
            }])
        }
        Statement::AlterTable(_) => {
            Ok(vec![BackendMessage::CommandComplete {
                tag: "ALTER TABLE".to_string(),
            }])
        }
        Statement::Copy(copy) => {
            // COPY in simple query mode is handled specially
            super::copy::handle_copy_statement(copy, session)
        }
    }
}

/// Plan and execute a query, returning RowDescription + DataRow* + CommandComplete.
fn execute_query_plan(
    stmt: &Statement,
    session: &mut Session,
) -> Result<Vec<BackendMessage>, ErrorFields> {
    // Build the plan while holding the catalog lock, then drop it before
    // execution so the executor can re-acquire it (e.g. for UPDATE/DELETE
    // which call back into the CatalogProvider).
    let (schema, mut executor) = {
        let catalog = session.catalog.lock().unwrap();
        let plan_builder = PlanBuilder::new(&catalog);

        let logical = plan_builder.build(stmt).map_err(|e| {
            ErrorFields::internal(format!("plan error: {e}"))
        })?;

        let optimizer = Optimizer::new(&catalog);
        let optimized = optimizer.optimize(logical);

        let physical_planner = PhysicalPlanner::new(&catalog);
        let physical = physical_planner.plan(&optimized);

        let schema = physical.schema();
        let exec_ctx = Arc::new(ExecutorContext::new(session.catalog_provider.clone()));

        let executor = build_executor(physical, exec_ctx);
        (schema, executor)
    }; // catalog lock dropped here

    executor.init().map_err(|e| {
        ErrorFields::internal(format!("executor init error: {e}"))
    })?;

    let mut messages = Vec::new();

    // Build RowDescription from schema
    let fields: Vec<FieldDescription> = schema
        .columns()
        .iter()
        .map(|col| FieldDescription::new(&col.name, &col.data_type))
        .collect();

    let is_dml = matches!(stmt, Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_));

    if !is_dml {
        messages.push(BackendMessage::RowDescription {
            fields: fields.clone(),
        });
    }

    let mut row_count = 0u64;
    loop {
        match executor.next() {
            Ok(Some(tuple)) => {
                row_count += 1;
                if !is_dml {
                    let values: Vec<Option<Vec<u8>>> = tuple
                        .values()
                        .iter()
                        .map(|d| datum_to_text(d))
                        .collect();
                    messages.push(BackendMessage::DataRow { values });
                }
            }
            Ok(None) => break,
            Err(e) => {
                let _ = executor.close();
                return Err(ErrorFields::internal(format!("execution error: {e}")));
            }
        }
    }

    let _ = executor.close();

    let tag = match stmt {
        Statement::Select(_) => format!("SELECT {row_count}"),
        Statement::Insert(_) => format!("INSERT 0 {row_count}"),
        Statement::Update(_) => format!("UPDATE {row_count}"),
        Statement::Delete(_) => format!("DELETE {row_count}"),
        _ => format!("OK"),
    };

    messages.push(BackendMessage::CommandComplete { tag });
    Ok(messages)
}
