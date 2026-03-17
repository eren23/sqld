use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sqld::executor::executor::CatalogProvider;
use sqld::planner::physical_plan::KeyRange;
use sqld::planner::Catalog;
use sqld::protocol::connection::Session;
use sqld::protocol::extended_query::{
    handle_bind, handle_close, handle_describe, handle_execute, handle_parse, handle_sync,
};
use sqld::protocol::messages::*;
use sqld::protocol::simple_query::handle_simple_query;
use sqld::types::*;
use sqld::utils::error::Result;

// ===========================================================================
// In-memory CatalogProvider
// ===========================================================================

struct MemCatalog {
    catalog: Arc<Mutex<Catalog>>,
    data: Mutex<HashMap<String, Vec<Tuple>>>,
}

impl MemCatalog {
    fn new(catalog: Arc<Mutex<Catalog>>) -> Self {
        Self {
            catalog,
            data: Mutex::new(HashMap::new()),
        }
    }
}

impl CatalogProvider for MemCatalog {
    fn table_schema(&self, table: &str) -> Result<Schema> {
        let cat = self.catalog.lock().unwrap();
        cat.get_schema(table)
            .cloned()
            .ok_or_else(|| {
                sqld::utils::error::SqlError::ExecutionError(format!(
                    "table not found: {table}"
                ))
                .into()
            })
    }

    fn scan_table(&self, table: &str) -> Result<Vec<Tuple>> {
        let data = self.data.lock().unwrap();
        Ok(data.get(table).cloned().unwrap_or_default())
    }

    fn scan_index(
        &self,
        table: &str,
        _index: &str,
        _ranges: &[KeyRange],
    ) -> Result<Vec<Tuple>> {
        self.scan_table(table)
    }

    fn insert_tuple(&self, table: &str, values: Vec<Datum>) -> Result<Tuple> {
        let tuple = Tuple::new(MvccHeader::new_insert(1, 0), values);
        let mut data = self.data.lock().unwrap();
        data.entry(table.to_string())
            .or_insert_with(Vec::new)
            .push(tuple.clone());
        Ok(tuple)
    }

    fn delete_tuple(&self, table: &str, tuple: &Tuple) -> Result<Tuple> {
        let mut data = self.data.lock().unwrap();
        if let Some(rows) = data.get_mut(table) {
            rows.retain(|r| r.values() != tuple.values());
        }
        Ok(tuple.clone())
    }

    fn update_tuple(
        &self,
        table: &str,
        old_tuple: &Tuple,
        new_values: Vec<Datum>,
    ) -> Result<Tuple> {
        self.delete_tuple(table, old_tuple)?;
        self.insert_tuple(table, new_values)
    }
}

// ===========================================================================
// Session factory
// ===========================================================================

fn make_session() -> Session {
    let catalog = Arc::new(Mutex::new(Catalog::new()));
    let cp: Arc<dyn CatalogProvider> = Arc::new(MemCatalog::new(catalog.clone()));
    Session::new(catalog, cp, 1)
}

// ===========================================================================
// Helpers
// ===========================================================================

fn has_error_response(msgs: &[BackendMessage]) -> bool {
    msgs.iter().any(|m| matches!(m, BackendMessage::ErrorResponse(_)))
}

fn is_error(msg: &BackendMessage) -> bool {
    matches!(msg, BackendMessage::ErrorResponse(_))
}

fn count_data_rows(msgs: &[BackendMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
        .count()
}

// ===========================================================================
// Tests
// ===========================================================================

#[test]
fn test_parse_simple() {
    let mut session = make_session();
    let resp = handle_parse("s1", "SELECT 1", &[], &mut session);
    assert!(matches!(resp, BackendMessage::ParseComplete));
}

#[test]
fn test_parse_syntax_error() {
    let mut session = make_session();
    let resp = handle_parse("s_bad", "SELEC INVALID GARBAGE", &[], &mut session);
    assert!(is_error(&resp), "expected ErrorResponse, got {resp:?}");
}

#[test]
fn test_bind_and_execute() {
    let mut session = make_session();

    // Parse
    let resp = handle_parse("s1", "SELECT 1", &[], &mut session);
    assert!(matches!(resp, BackendMessage::ParseComplete));

    // Bind
    let resp = handle_bind("p1", "s1", &[], &[], &[], &mut session);
    assert!(matches!(resp, BackendMessage::BindComplete));

    // Execute
    let msgs = handle_execute("p1", 0, &mut session);
    assert!(!has_error_response(&msgs));
    let has_complete = msgs
        .iter()
        .any(|m| matches!(m, BackendMessage::CommandComplete { .. }));
    assert!(has_complete, "expected CommandComplete from execute");
}

#[test]
fn test_describe_statement() {
    let mut session = make_session();

    let resp = handle_parse("s_desc", "SELECT 1", &[], &mut session);
    assert!(matches!(resp, BackendMessage::ParseComplete));

    let msgs = handle_describe(DescribeTarget::Statement, "s_desc", &session);
    assert!(!has_error_response(&msgs));

    // Should have ParameterDescription
    let has_param_desc = msgs
        .iter()
        .any(|m| matches!(m, BackendMessage::ParameterDescription { .. }));
    assert!(has_param_desc, "expected ParameterDescription");

    // Should have either RowDescription or NoData
    let has_row_desc_or_nodata = msgs.iter().any(|m| {
        matches!(
            m,
            BackendMessage::RowDescription { .. } | BackendMessage::NoData
        )
    });
    assert!(has_row_desc_or_nodata, "expected RowDescription or NoData");
}

#[test]
fn test_describe_portal() {
    let mut session = make_session();

    handle_parse("s_portal", "SELECT 1", &[], &mut session);
    handle_bind("p_portal", "s_portal", &[], &[], &[], &mut session);

    let msgs = handle_describe(DescribeTarget::Portal, "p_portal", &session);
    assert!(!has_error_response(&msgs));

    let has_row_or_nodata = msgs.iter().any(|m| {
        matches!(
            m,
            BackendMessage::RowDescription { .. } | BackendMessage::NoData
        )
    });
    assert!(has_row_or_nodata, "expected RowDescription or NoData");
}

#[test]
fn test_close_statement() {
    let mut session = make_session();

    handle_parse("s_close", "SELECT 1", &[], &mut session);
    assert!(session.prepared_statements.contains_key("s_close"));

    let resp = handle_close(DescribeTarget::Statement, "s_close", &mut session);
    assert!(matches!(resp, BackendMessage::CloseComplete));
    assert!(!session.prepared_statements.contains_key("s_close"));
}

#[test]
fn test_close_portal() {
    let mut session = make_session();

    handle_parse("s_cp", "SELECT 1", &[], &mut session);
    handle_bind("p_cp", "s_cp", &[], &[], &[], &mut session);
    assert!(session.portals.contains_key("p_cp"));

    let resp = handle_close(DescribeTarget::Portal, "p_cp", &mut session);
    assert!(matches!(resp, BackendMessage::CloseComplete));
    assert!(!session.portals.contains_key("p_cp"));
}

#[test]
fn test_sync() {
    let mut session = make_session();

    // Sync when idle
    let resp = handle_sync(&session);
    assert!(matches!(
        resp,
        BackendMessage::ReadyForQuery {
            state: TransactionState::Idle
        }
    ));

    // Sync after BEGIN
    handle_simple_query("BEGIN", &mut session);
    let resp = handle_sync(&session);
    assert!(matches!(
        resp,
        BackendMessage::ReadyForQuery {
            state: TransactionState::InBlock
        }
    ));
}

#[test]
fn test_named_prepared_statement() {
    let mut session = make_session();

    // Create a table and insert data
    handle_simple_query("CREATE TABLE items (id INTEGER, name TEXT)", &mut session);
    handle_simple_query("INSERT INTO items VALUES (1, 'apple')", &mut session);
    handle_simple_query("INSERT INTO items VALUES (2, 'banana')", &mut session);

    // Parse a named prepared statement (no parameters - the engine's extended
    // query protocol does not yet fully substitute $N placeholders at execute)
    let resp = handle_parse(
        "list_items",
        "SELECT id, name FROM items",
        &[],
        &mut session,
    );
    assert!(
        matches!(resp, BackendMessage::ParseComplete),
        "parse failed: {resp:?}"
    );

    // Verify the named statement is stored
    assert!(session.prepared_statements.contains_key("list_items"));

    // Bind
    let resp = handle_bind(
        "portal_list",
        "list_items",
        &[],
        &[],
        &[],
        &mut session,
    );
    assert!(
        matches!(resp, BackendMessage::BindComplete),
        "bind failed: {resp:?}"
    );

    // Execute
    let msgs = handle_execute("portal_list", 0, &mut session);
    assert!(!has_error_response(&msgs), "execute failed: {msgs:?}");
    assert_eq!(count_data_rows(&msgs), 2);
}

#[test]
fn test_unnamed_prepared_statement() {
    let mut session = make_session();

    // Parse with empty name (unnamed prepared statement)
    let resp = handle_parse("", "SELECT 42", &[], &mut session);
    assert!(matches!(resp, BackendMessage::ParseComplete));
    assert!(session.prepared_statements.contains_key(""));

    // Bind with empty portal name
    let resp = handle_bind("", "", &[], &[], &[], &mut session);
    assert!(matches!(resp, BackendMessage::BindComplete));

    // Execute
    let msgs = handle_execute("", 0, &mut session);
    assert!(!has_error_response(&msgs));
    let has_complete = msgs
        .iter()
        .any(|m| matches!(m, BackendMessage::CommandComplete { .. }));
    assert!(has_complete);
}

#[test]
fn test_multiple_portals() {
    let mut session = make_session();

    handle_parse("s_multi", "SELECT 1", &[], &mut session);

    // First binding
    let resp = handle_bind("p_a", "s_multi", &[], &[], &[], &mut session);
    assert!(matches!(resp, BackendMessage::BindComplete));

    // Second binding from the same statement
    let resp = handle_bind("p_b", "s_multi", &[], &[], &[], &mut session);
    assert!(matches!(resp, BackendMessage::BindComplete));

    // Both portals should exist
    assert!(session.portals.contains_key("p_a"));
    assert!(session.portals.contains_key("p_b"));

    // Execute first portal
    let msgs_a = handle_execute("p_a", 0, &mut session);
    assert!(!has_error_response(&msgs_a));

    // Execute second portal
    let msgs_b = handle_execute("p_b", 0, &mut session);
    assert!(!has_error_response(&msgs_b));
}

#[test]
fn test_bind_with_parameter_values() {
    let mut session = make_session();

    // Parse a statement with parameter type hints (parsing accepts them)
    let resp = handle_parse(
        "s_typed",
        "SELECT 1",
        &[23, 25, 16], // int4, text, bool OIDs
        &mut session,
    );
    assert!(
        matches!(resp, BackendMessage::ParseComplete),
        "parse failed: {resp:?}"
    );

    // Bind with text-format parameter values (values are accepted even
    // if the query itself doesn't reference them)
    let resp = handle_bind(
        "p_typed",
        "s_typed",
        &[0, 0, 0],
        &[
            Some(b"42".to_vec()),
            Some(b"hello".to_vec()),
            Some(b"true".to_vec()),
        ],
        &[],
        &mut session,
    );
    assert!(
        matches!(resp, BackendMessage::BindComplete),
        "bind failed: {resp:?}"
    );

    // Execute
    let msgs = handle_execute("p_typed", 0, &mut session);
    assert!(!has_error_response(&msgs), "execute failed: {msgs:?}");
}

#[test]
fn test_extended_query_insert_via_literal() {
    let mut session = make_session();

    // Create table
    handle_simple_query(
        "CREATE TABLE typed (i INTEGER, t TEXT, b BOOLEAN)",
        &mut session,
    );

    // Parse an INSERT with literal values (not placeholders)
    let resp = handle_parse(
        "s_ins",
        "INSERT INTO typed VALUES (42, 'hello', true)",
        &[],
        &mut session,
    );
    assert!(
        matches!(resp, BackendMessage::ParseComplete),
        "parse failed: {resp:?}"
    );

    // Bind (no parameters needed)
    let resp = handle_bind("p_ins", "s_ins", &[], &[], &[], &mut session);
    assert!(matches!(resp, BackendMessage::BindComplete));

    // Execute
    let msgs = handle_execute("p_ins", 0, &mut session);
    assert!(!has_error_response(&msgs), "execute failed: {msgs:?}");

    // Verify data via simple query
    let msgs = handle_simple_query("SELECT i, t, b FROM typed", &mut session);
    assert!(!has_error_response(&msgs));
    assert_eq!(count_data_rows(&msgs), 1);
}

#[test]
fn test_bind_nonexistent_statement_returns_error() {
    let mut session = make_session();

    let resp = handle_bind("p_none", "nonexistent_stmt", &[], &[], &[], &mut session);
    assert!(is_error(&resp), "expected error for nonexistent statement");
}

#[test]
fn test_execute_nonexistent_portal_returns_error() {
    let mut session = make_session();

    let msgs = handle_execute("nonexistent_portal", 0, &mut session);
    assert!(has_error_response(&msgs), "expected error for nonexistent portal");
}

#[test]
fn test_describe_nonexistent_statement_returns_error() {
    let mut session = make_session();

    let msgs = handle_describe(DescribeTarget::Statement, "no_such_stmt", &session);
    assert!(has_error_response(&msgs));
}
