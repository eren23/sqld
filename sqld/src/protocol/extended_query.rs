use crate::planner::plan_builder::PlanBuilder;
use crate::protocol::connection::Session;
use crate::protocol::messages::{
    pg_oid_to_datatype, text_to_datum,
    BackendMessage, DescribeTarget, ErrorFields, FieldDescription, TransactionState,
};
use crate::sql::ast::Statement;
use crate::sql::parser;
use crate::types::{DataType, Datum, Schema};

// ---------------------------------------------------------------------------
// Prepared statement
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct PreparedStatement {
    pub name: String,
    pub query: String,
    pub param_types: Vec<DataType>,
    pub statements: Vec<Statement>,
    pub result_schema: Option<Schema>,
}

// ---------------------------------------------------------------------------
// Portal — a bound prepared statement ready for execution
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Portal {
    pub name: String,
    pub statement_name: String,
    pub param_values: Vec<Datum>,
    pub result_formats: Vec<i16>,
    pub statements: Vec<Statement>,
    pub result_schema: Option<Schema>,
}

// ---------------------------------------------------------------------------
// Extended query handler
// ---------------------------------------------------------------------------

/// Handle Parse message.
pub fn handle_parse(
    name: &str,
    query: &str,
    param_type_oids: &[i32],
    session: &mut Session,
) -> BackendMessage {
    // Parse the query text
    let parse_result = parser::parse(query);
    if parse_result.has_errors() {
        let err_msg = parse_result
            .errors
            .iter()
            .map(|e| format!("{e}"))
            .collect::<Vec<_>>()
            .join("; ");
        return BackendMessage::ErrorResponse(
            ErrorFields::syntax_error(err_msg),
        );
    }
    let stmts = parse_result.statements;

    let param_types: Vec<DataType> = param_type_oids
        .iter()
        .map(|oid| pg_oid_to_datatype(*oid).unwrap_or(DataType::Text))
        .collect();

    // Try to determine result schema for the first statement
    let result_schema = if let Some(stmt) = stmts.first() {
        resolve_result_schema(stmt, session).ok()
    } else {
        None
    };

    let prepared = PreparedStatement {
        name: name.to_string(),
        query: query.to_string(),
        param_types,
        statements: stmts,
        result_schema,
    };

    session.prepared_statements.insert(name.to_string(), prepared);

    BackendMessage::ParseComplete
}

/// Handle Bind message.
pub fn handle_bind(
    portal_name: &str,
    statement_name: &str,
    param_formats: &[i16],
    param_values: &[Option<Vec<u8>>],
    result_formats: &[i16],
    session: &mut Session,
) -> BackendMessage {
    let prepared = match session.prepared_statements.get(statement_name) {
        Some(p) => p.clone(),
        None => {
            return BackendMessage::ErrorResponse(ErrorFields::internal(format!(
                "prepared statement \"{}\" does not exist",
                statement_name
            )));
        }
    };

    // Bind parameter values
    let mut bound_params = Vec::with_capacity(param_values.len());
    for (i, val) in param_values.iter().enumerate() {
        match val {
            None => bound_params.push(Datum::Null),
            Some(data) => {
                let format = if i < param_formats.len() {
                    param_formats[i]
                } else if param_formats.len() == 1 {
                    param_formats[0]
                } else {
                    0 // text
                };

                let target_type = if i < prepared.param_types.len() {
                    &prepared.param_types[i]
                } else {
                    &DataType::Text
                };

                if format == 0 {
                    // Text format
                    match text_to_datum(data, target_type) {
                        Ok(d) => bound_params.push(d),
                        Err(e) => {
                            return BackendMessage::ErrorResponse(
                                ErrorFields::data_exception(format!(
                                    "invalid input for parameter ${}: {e}",
                                    i + 1
                                )),
                            );
                        }
                    }
                } else {
                    // Binary format — simplified: treat as text
                    match text_to_datum(data, target_type) {
                        Ok(d) => bound_params.push(d),
                        Err(e) => {
                            return BackendMessage::ErrorResponse(
                                ErrorFields::data_exception(format!(
                                    "invalid binary input for parameter ${}: {e}",
                                    i + 1
                                )),
                            );
                        }
                    }
                }
            }
        }
    }

    let portal = Portal {
        name: portal_name.to_string(),
        statement_name: statement_name.to_string(),
        param_values: bound_params,
        result_formats: result_formats.to_vec(),
        statements: prepared.statements.clone(),
        result_schema: prepared.result_schema.clone(),
    };

    session.portals.insert(portal_name.to_string(), portal);

    BackendMessage::BindComplete
}

/// Handle Describe message.
pub fn handle_describe(
    target: DescribeTarget,
    name: &str,
    session: &Session,
) -> Vec<BackendMessage> {
    match target {
        DescribeTarget::Statement => {
            match session.prepared_statements.get(name) {
                Some(prepared) => {
                    let mut messages = Vec::new();

                    // ParameterDescription
                    let type_oids: Vec<i32> = prepared
                        .param_types
                        .iter()
                        .map(|dt| crate::protocol::messages::pg_type_info(dt).0)
                        .collect();
                    messages.push(BackendMessage::ParameterDescription { type_oids });

                    // RowDescription or NoData
                    match &prepared.result_schema {
                        Some(schema) if !schema.columns().is_empty() => {
                            let fields: Vec<FieldDescription> = schema
                                .columns()
                                .iter()
                                .map(|col| FieldDescription::new(&col.name, &col.data_type))
                                .collect();
                            messages.push(BackendMessage::RowDescription { fields });
                        }
                        _ => {
                            messages.push(BackendMessage::NoData);
                        }
                    }
                    messages
                }
                None => {
                    vec![BackendMessage::ErrorResponse(ErrorFields::internal(
                        format!("prepared statement \"{name}\" does not exist"),
                    ))]
                }
            }
        }
        DescribeTarget::Portal => {
            match session.portals.get(name) {
                Some(portal) => {
                    match &portal.result_schema {
                        Some(schema) if !schema.columns().is_empty() => {
                            let fields: Vec<FieldDescription> = schema
                                .columns()
                                .iter()
                                .map(|col| FieldDescription::new(&col.name, &col.data_type))
                                .collect();
                            vec![BackendMessage::RowDescription { fields }]
                        }
                        _ => vec![BackendMessage::NoData],
                    }
                }
                None => {
                    vec![BackendMessage::ErrorResponse(ErrorFields::internal(
                        format!("portal \"{name}\" does not exist"),
                    ))]
                }
            }
        }
    }
}

/// Handle Execute message.
pub fn handle_execute(
    portal_name: &str,
    _max_rows: i32,
    session: &mut Session,
) -> Vec<BackendMessage> {
    let portal = match session.portals.get(portal_name) {
        Some(p) => p.clone(),
        None => {
            return vec![BackendMessage::ErrorResponse(ErrorFields::internal(
                format!("portal \"{portal_name}\" does not exist"),
            ))];
        }
    };

    let mut all_messages = Vec::new();

    for stmt in &portal.statements {
        match super::simple_query::execute_statement(stmt, session) {
            Ok(mut msgs) => all_messages.append(&mut msgs),
            Err(err) => {
                session.txn_state = if session.txn_state == TransactionState::InBlock {
                    TransactionState::Failed
                } else {
                    session.txn_state
                };
                all_messages.push(BackendMessage::ErrorResponse(err));
                break;
            }
        }
    }

    all_messages
}

/// Handle Close message.
pub fn handle_close(
    target: DescribeTarget,
    name: &str,
    session: &mut Session,
) -> BackendMessage {
    match target {
        DescribeTarget::Statement => {
            session.prepared_statements.remove(name);
        }
        DescribeTarget::Portal => {
            session.portals.remove(name);
        }
    }
    BackendMessage::CloseComplete
}

/// Handle Sync — end of extended query protocol cycle.
pub fn handle_sync(session: &Session) -> BackendMessage {
    BackendMessage::ReadyForQuery {
        state: session.txn_state,
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn resolve_result_schema(stmt: &Statement, session: &Session) -> Result<Schema, String> {
    let catalog = session.catalog.lock().unwrap();
    let plan_builder = PlanBuilder::new(&catalog);
    let logical = plan_builder
        .build(stmt)
        .map_err(|e| format!("{e}"))?;
    Ok(logical.schema())
}
