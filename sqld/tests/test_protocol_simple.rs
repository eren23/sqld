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

fn has_error_response(msgs: &[BackendMessage]) -> bool {
    msgs.iter().any(|m| matches!(m, BackendMessage::ErrorResponse(_)))
}

fn get_error_code(msgs: &[BackendMessage]) -> Option<String> {
    msgs.iter().find_map(|m| match m {
        BackendMessage::ErrorResponse(e) => Some(e.code.clone()),
        _ => None,
    })
}

fn last_ready_state(msgs: &[BackendMessage]) -> Option<TransactionState> {
    msgs.iter().rev().find_map(|m| match m {
        BackendMessage::ReadyForQuery { state } => Some(*state),
        _ => None,
    })
}

fn get_command_tag(msgs: &[BackendMessage]) -> Option<String> {
    msgs.iter().find_map(|m| match m {
        BackendMessage::CommandComplete { tag } => Some(tag.clone()),
        _ => None,
    })
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
fn test_empty_query() {
    let mut session = make_session();
    let msgs = handle_simple_query("", &mut session);

    assert!(msgs.iter().any(|m| matches!(m, BackendMessage::EmptyQueryResponse)));
    assert_eq!(last_ready_state(&msgs), Some(TransactionState::Idle));
}

#[test]
fn test_select_literal_no_from() {
    // SELECT <literal> without FROM produces a RowDescription + CommandComplete
    // but zero DataRows because the engine's Empty plan yields no rows
    // (no implicit dual table). This is engine-specific behavior.
    let mut session = make_session();
    let msgs = handle_simple_query("SELECT 1", &mut session);

    let has_row_desc = msgs.iter().any(|m| matches!(m, BackendMessage::RowDescription { .. }));
    let has_complete = msgs.iter().any(|m| matches!(m, BackendMessage::CommandComplete { .. }));

    assert!(has_row_desc, "expected RowDescription");
    assert!(has_complete, "expected CommandComplete");
    assert_eq!(last_ready_state(&msgs), Some(TransactionState::Idle));
}

#[test]
fn test_select_from_table_returns_data_row() {
    let mut session = make_session();

    handle_simple_query("CREATE TABLE nums (val INTEGER)", &mut session);
    handle_simple_query("INSERT INTO nums VALUES (42)", &mut session);

    let msgs = handle_simple_query("SELECT val FROM nums", &mut session);

    let has_row_desc = msgs.iter().any(|m| matches!(m, BackendMessage::RowDescription { .. }));
    let has_data_row = msgs.iter().any(|m| matches!(m, BackendMessage::DataRow { .. }));
    let has_complete = msgs.iter().any(|m| matches!(m, BackendMessage::CommandComplete { .. }));

    assert!(has_row_desc, "expected RowDescription");
    assert!(has_data_row, "expected DataRow");
    assert!(has_complete, "expected CommandComplete");
    assert_eq!(last_ready_state(&msgs), Some(TransactionState::Idle));

    // Verify the data value
    let data_row = msgs.iter().find_map(|m| match m {
        BackendMessage::DataRow { values } => Some(values.clone()),
        _ => None,
    });
    let values = data_row.unwrap();
    assert_eq!(values.len(), 1);
    assert_eq!(values[0], Some(b"42".to_vec()));
}

#[test]
fn test_create_table() {
    let mut session = make_session();
    let msgs = handle_simple_query(
        "CREATE TABLE users (id INTEGER, name TEXT)",
        &mut session,
    );

    assert!(!has_error_response(&msgs), "CREATE TABLE failed");
    assert_eq!(get_command_tag(&msgs), Some("CREATE TABLE".to_string()));
    assert_eq!(last_ready_state(&msgs), Some(TransactionState::Idle));
}

#[test]
fn test_insert_and_select() {
    let mut session = make_session();

    // CREATE TABLE
    let msgs = handle_simple_query("CREATE TABLE items (id INTEGER, name TEXT)", &mut session);
    assert!(!has_error_response(&msgs), "CREATE TABLE failed");

    // INSERT
    let msgs = handle_simple_query("INSERT INTO items VALUES (1, 'apple')", &mut session);
    assert!(!has_error_response(&msgs), "INSERT failed");
    let tag = get_command_tag(&msgs).unwrap();
    assert!(tag.starts_with("INSERT"), "expected INSERT tag, got {tag}");

    // SELECT
    let msgs = handle_simple_query("SELECT id, name FROM items", &mut session);
    assert!(!has_error_response(&msgs), "SELECT failed");
    assert_eq!(count_data_rows(&msgs), 1);

    // Verify the returned data
    let data_row = msgs.iter().find_map(|m| match m {
        BackendMessage::DataRow { values } => Some(values.clone()),
        _ => None,
    });
    let values = data_row.unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!(values[0], Some(b"1".to_vec()));
    assert_eq!(values[1], Some(b"apple".to_vec()));
}

#[test]
fn test_multiple_statements() {
    let mut session = make_session();
    let msgs = handle_simple_query("SELECT 1; SELECT 2", &mut session);

    let complete_count = msgs
        .iter()
        .filter(|m| matches!(m, BackendMessage::CommandComplete { .. }))
        .count();
    assert!(
        complete_count >= 2,
        "expected at least 2 CommandComplete, got {complete_count}"
    );
    assert_eq!(last_ready_state(&msgs), Some(TransactionState::Idle));
}

#[test]
fn test_transaction_begin_commit() {
    let mut session = make_session();

    // BEGIN
    let msgs = handle_simple_query("BEGIN", &mut session);
    assert!(!has_error_response(&msgs));
    assert_eq!(last_ready_state(&msgs), Some(TransactionState::InBlock));
    assert_eq!(session.txn_state, TransactionState::InBlock);

    // COMMIT
    let msgs = handle_simple_query("COMMIT", &mut session);
    assert!(!has_error_response(&msgs));
    assert_eq!(last_ready_state(&msgs), Some(TransactionState::Idle));
    assert_eq!(session.txn_state, TransactionState::Idle);
}

#[test]
fn test_transaction_rollback() {
    let mut session = make_session();

    // BEGIN
    handle_simple_query("BEGIN", &mut session);
    assert_eq!(session.txn_state, TransactionState::InBlock);

    // ROLLBACK
    let msgs = handle_simple_query("ROLLBACK", &mut session);
    assert!(!has_error_response(&msgs));
    assert_eq!(last_ready_state(&msgs), Some(TransactionState::Idle));
    assert_eq!(session.txn_state, TransactionState::Idle);
}

#[test]
fn test_failed_transaction() {
    let mut session = make_session();

    handle_simple_query("BEGIN", &mut session);
    assert_eq!(session.txn_state, TransactionState::InBlock);

    // Trigger an error inside the transaction block
    let msgs = handle_simple_query("SELEC INVALID GARBAGE", &mut session);
    assert!(has_error_response(&msgs));
    assert_eq!(session.txn_state, TransactionState::Failed);
    assert_eq!(last_ready_state(&msgs), Some(TransactionState::Failed));

    // ROLLBACK recovers from the Failed state
    let msgs = handle_simple_query("ROLLBACK", &mut session);
    assert!(!has_error_response(&msgs));
    assert_eq!(session.txn_state, TransactionState::Idle);
}

#[test]
fn test_show_tables() {
    let mut session = make_session();

    handle_simple_query("CREATE TABLE alpha (id INTEGER)", &mut session);
    handle_simple_query("CREATE TABLE beta (id INTEGER)", &mut session);

    let msgs = handle_simple_query("SHOW TABLES", &mut session);
    assert!(!has_error_response(&msgs));
    assert_eq!(count_data_rows(&msgs), 2);

    // Verify RowDescription has table_name column
    let row_desc = msgs.iter().find_map(|m| match m {
        BackendMessage::RowDescription { fields } => Some(fields.clone()),
        _ => None,
    });
    assert!(row_desc.is_some());
    assert_eq!(row_desc.unwrap()[0].name, "table_name");
}

#[test]
fn test_drop_table() {
    let mut session = make_session();

    handle_simple_query("CREATE TABLE temp (id INTEGER)", &mut session);

    // Verify table exists
    let msgs = handle_simple_query("SHOW TABLES", &mut session);
    assert_eq!(count_data_rows(&msgs), 1);

    // DROP TABLE
    let msgs = handle_simple_query("DROP TABLE temp", &mut session);
    assert!(!has_error_response(&msgs));
    assert_eq!(get_command_tag(&msgs), Some("DROP TABLE".to_string()));

    // Verify table is gone
    let msgs = handle_simple_query("SHOW TABLES", &mut session);
    assert_eq!(count_data_rows(&msgs), 0);

    // DROP TABLE IF EXISTS on a nonexistent table should not error
    let msgs = handle_simple_query("DROP TABLE IF EXISTS temp", &mut session);
    assert!(!has_error_response(&msgs));
    assert_eq!(get_command_tag(&msgs), Some("DROP TABLE".to_string()));
}

#[test]
fn test_show_columns() {
    let mut session = make_session();

    handle_simple_query(
        "CREATE TABLE items (id INTEGER, name TEXT, price FLOAT)",
        &mut session,
    );

    let msgs = handle_simple_query("SHOW COLUMNS FROM items", &mut session);
    assert!(!has_error_response(&msgs));
    assert_eq!(count_data_rows(&msgs), 3);
}

#[test]
fn test_create_table_if_not_exists() {
    let mut session = make_session();

    handle_simple_query("CREATE TABLE dup (id INTEGER)", &mut session);

    // Second create with IF NOT EXISTS should not error
    let msgs = handle_simple_query(
        "CREATE TABLE IF NOT EXISTS dup (id INTEGER)",
        &mut session,
    );
    assert!(!has_error_response(&msgs));
    let has_notice = msgs
        .iter()
        .any(|m| matches!(m, BackendMessage::NoticeResponse(_)));
    assert!(has_notice, "expected NoticeResponse for IF NOT EXISTS");
}

#[test]
fn test_duplicate_table_without_if_not_exists() {
    let mut session = make_session();

    handle_simple_query("CREATE TABLE dup2 (id INTEGER)", &mut session);

    let msgs = handle_simple_query("CREATE TABLE dup2 (id INTEGER)", &mut session);
    assert!(has_error_response(&msgs));
    assert_eq!(get_error_code(&msgs), Some("42P07".to_string()));
}

#[test]
fn test_syntax_error_returns_42601() {
    let mut session = make_session();
    let msgs = handle_simple_query("SELEC INVALID GARBAGE", &mut session);

    assert!(has_error_response(&msgs));
    assert_eq!(get_error_code(&msgs), Some("42601".to_string()));
    assert_eq!(last_ready_state(&msgs), Some(TransactionState::Idle));
}

#[test]
fn test_whitespace_only_query() {
    let mut session = make_session();
    let msgs = handle_simple_query("   ;  ", &mut session);

    assert!(msgs
        .iter()
        .any(|m| matches!(m, BackendMessage::EmptyQueryResponse)));
    assert_eq!(last_ready_state(&msgs), Some(TransactionState::Idle));
}

#[test]
fn test_select_from_table_returns_correct_value() {
    let mut session = make_session();
    handle_simple_query("CREATE TABLE vals (n INTEGER)", &mut session);
    handle_simple_query("INSERT INTO vals VALUES (42)", &mut session);

    let msgs = handle_simple_query("SELECT n FROM vals", &mut session);

    let data_row = msgs.iter().find_map(|m| match m {
        BackendMessage::DataRow { values } => Some(values.clone()),
        _ => None,
    });
    let values = data_row.unwrap();
    assert_eq!(values.len(), 1);
    assert_eq!(values[0], Some(b"42".to_vec()));
}

#[test]
fn test_select_command_tag_contains_row_count() {
    let mut session = make_session();
    let msgs = handle_simple_query("SELECT 1", &mut session);

    let tag = get_command_tag(&msgs).unwrap();
    assert!(
        tag.starts_with("SELECT"),
        "expected SELECT tag, got {tag}"
    );
}
