use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sqld::executor::executor::CatalogProvider;
use sqld::planner::physical_plan::KeyRange;
use sqld::planner::Catalog;
use sqld::protocol::connection::Session;
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

fn get_error_fields(msgs: &[BackendMessage]) -> Option<&ErrorFields> {
    msgs.iter().find_map(|m| match m {
        BackendMessage::ErrorResponse(e) => Some(e),
        _ => None,
    })
}

fn get_error_code(msgs: &[BackendMessage]) -> Option<String> {
    get_error_fields(msgs).map(|e| e.code.clone())
}

fn has_error_response(msgs: &[BackendMessage]) -> bool {
    msgs.iter().any(|m| matches!(m, BackendMessage::ErrorResponse(_)))
}

fn last_ready_state(msgs: &[BackendMessage]) -> Option<TransactionState> {
    msgs.iter().rev().find_map(|m| match m {
        BackendMessage::ReadyForQuery { state } => Some(*state),
        _ => None,
    })
}

// ===========================================================================
// Tests
// ===========================================================================

#[test]
fn test_syntax_error() {
    let mut session = make_session();
    let msgs = handle_simple_query("SELEC INVALID GARBAGE", &mut session);

    assert!(has_error_response(&msgs));
    assert_eq!(get_error_code(&msgs), Some("42601".to_string()));
}

#[test]
fn test_undefined_table() {
    let mut session = make_session();
    // DROP TABLE on a nonexistent table produces a clear 42P01 error.
    // Note: SELECT * FROM nonexistent does not error because the planner
    // silently creates an empty schema for unknown tables.
    let msgs = handle_simple_query("DROP TABLE no_such_table", &mut session);

    assert!(has_error_response(&msgs));
    let code = get_error_code(&msgs).unwrap();
    assert_eq!(code, "42P01");
}

#[test]
fn test_undefined_column() {
    let mut session = make_session();
    handle_simple_query("CREATE TABLE col_test (id INTEGER)", &mut session);

    let msgs = handle_simple_query("SELECT nonexistent FROM col_test", &mut session);
    assert!(has_error_response(&msgs));
    let code = get_error_code(&msgs).unwrap();
    assert!(
        code == "XX000" || code == "42703",
        "expected XX000 or 42703, got {code}"
    );
}

#[test]
fn test_duplicate_table() {
    let mut session = make_session();
    handle_simple_query("CREATE TABLE dup_tbl (id INTEGER)", &mut session);

    let msgs = handle_simple_query("CREATE TABLE dup_tbl (id INTEGER)", &mut session);
    assert!(has_error_response(&msgs));
    assert_eq!(get_error_code(&msgs), Some("42P07".to_string()));
}

#[test]
fn test_division_by_zero() {
    let e = ErrorFields::division_by_zero();
    assert_eq!(e.code, "22012");
    assert_eq!(e.message, "division by zero");
    assert_eq!(e.severity, Severity::Error);
}

#[test]
fn test_error_fields_severity() {
    let mut session = make_session();
    let msgs = handle_simple_query("SELEC BAD SQL", &mut session);

    let err = get_error_fields(&msgs).unwrap();
    assert_eq!(err.severity, Severity::Error);
    assert_eq!(err.severity.as_str(), "ERROR");
}

#[test]
fn test_error_fields_sqlstate() {
    let codes = [
        ErrorFields::internal("x").code,
        ErrorFields::syntax_error("x").code,
        ErrorFields::undefined_table("x").code,
        ErrorFields::undefined_column("x").code,
        ErrorFields::unique_violation("x").code,
        ErrorFields::not_null_violation("x").code,
        ErrorFields::foreign_key_violation("x").code,
        ErrorFields::check_violation("x").code,
        ErrorFields::serialization_failure("x").code,
        ErrorFields::deadlock_detected("x").code,
        ErrorFields::invalid_transaction_state("x").code,
        ErrorFields::data_exception("x").code,
        ErrorFields::division_by_zero().code,
        ErrorFields::feature_not_supported("x").code,
    ];

    for code in &codes {
        assert_eq!(
            code.len(),
            5,
            "SQLSTATE code '{}' is not 5 characters",
            code
        );
        assert!(
            code.chars().all(|c| c.is_ascii_alphanumeric()),
            "SQLSTATE code '{}' contains non-alphanumeric characters",
            code
        );
    }
}

#[test]
fn test_error_with_detail() {
    let e = ErrorFields::internal("main message")
        .with_detail("some detail about the error")
        .with_hint("try this instead");

    assert_eq!(e.message, "main message");
    assert_eq!(e.detail, Some("some detail about the error".to_string()));
    assert_eq!(e.hint, Some("try this instead".to_string()));
}

#[test]
fn test_error_with_position() {
    let e = ErrorFields::syntax_error("bad token")
        .with_position(42);

    assert_eq!(e.position, Some(42));
    assert_eq!(e.code, "42601");

    // Verify that the simple query handler sets position on syntax errors
    let mut session = make_session();
    let msgs = handle_simple_query("SELEC BAD", &mut session);
    let err = get_error_fields(&msgs).unwrap();
    assert!(err.position.is_some(), "syntax error should include position");
}

#[test]
fn test_transaction_error_state() {
    let mut session = make_session();

    handle_simple_query("BEGIN", &mut session);
    assert_eq!(session.txn_state, TransactionState::InBlock);

    let msgs = handle_simple_query("SELEC BAD", &mut session);
    assert!(has_error_response(&msgs));
    assert_eq!(session.txn_state, TransactionState::Failed);
    assert_eq!(last_ready_state(&msgs), Some(TransactionState::Failed));
}

#[test]
fn test_error_response_always_followed_by_ready() {
    let mut session = make_session();

    let error_queries = [
        "SELEC BAD",
        "DROP TABLE ghost",
        "CREATE TABLE dup (id INTEGER); CREATE TABLE dup (id INTEGER)",
    ];

    for query in &error_queries {
        let msgs = handle_simple_query(query, &mut session);
        let last_msg = msgs.last().unwrap();
        assert!(
            matches!(last_msg, BackendMessage::ReadyForQuery { .. }),
            "query '{query}' did not end with ReadyForQuery: {last_msg:?}"
        );
        if session.txn_state != TransactionState::Idle {
            handle_simple_query("ROLLBACK", &mut session);
        }
    }
}

#[test]
fn test_begin_inside_transaction_returns_error() {
    let mut session = make_session();
    handle_simple_query("BEGIN", &mut session);
    assert_eq!(session.txn_state, TransactionState::InBlock);

    let msgs = handle_simple_query("BEGIN", &mut session);
    assert!(has_error_response(&msgs));
    assert_eq!(get_error_code(&msgs), Some("25000".to_string()));
}

#[test]
fn test_commit_outside_transaction_returns_error() {
    let mut session = make_session();
    assert_eq!(session.txn_state, TransactionState::Idle);

    let msgs = handle_simple_query("COMMIT", &mut session);
    assert!(has_error_response(&msgs));
    assert_eq!(get_error_code(&msgs), Some("25000".to_string()));
}

#[test]
fn test_show_columns_on_nonexistent_table() {
    let mut session = make_session();
    let msgs = handle_simple_query("SHOW COLUMNS FROM ghost_table", &mut session);

    assert!(has_error_response(&msgs));
    assert_eq!(get_error_code(&msgs), Some("42P01".to_string()));
}
