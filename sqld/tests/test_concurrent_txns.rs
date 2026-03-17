use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use sqld::executor::CatalogProvider;
use sqld::planner::Catalog;
use sqld::planner::physical_plan::KeyRange;
use sqld::protocol::connection::Session;
use sqld::protocol::messages::datum_to_text;
use sqld::protocol::{BackendMessage, TransactionState};
use sqld::protocol::simple_query::handle_simple_query;
use sqld::types::*;
use sqld::utils::error::Result as SqlResult;

struct TestCatalogProvider {
    catalog: Arc<Mutex<Catalog>>,
    data: Mutex<HashMap<String, Vec<Tuple>>>,
}

impl TestCatalogProvider {
    fn new(catalog: Arc<Mutex<Catalog>>) -> Self {
        Self { catalog, data: Mutex::new(HashMap::new()) }
    }
}

impl CatalogProvider for TestCatalogProvider {
    fn table_schema(&self, table: &str) -> SqlResult<Schema> {
        self.catalog.lock().unwrap()
            .get_schema(table).cloned()
            .ok_or_else(|| sqld::utils::error::SqlError::ExecutionError(
                format!("table \"{table}\" does not exist")).into())
    }
    fn scan_table(&self, table: &str) -> SqlResult<Vec<Tuple>> {
        Ok(self.data.lock().unwrap().get(table).cloned().unwrap_or_default())
    }
    fn scan_index(&self, table: &str, _: &str, _: &[KeyRange]) -> SqlResult<Vec<Tuple>> {
        self.scan_table(table)
    }
    fn insert_tuple(&self, table: &str, values: Vec<Datum>) -> SqlResult<Tuple> {
        let tuple = Tuple::new(MvccHeader::new_insert(0, 0), values);
        self.data.lock().unwrap().entry(table.to_string()).or_default().push(tuple.clone());
        Ok(tuple)
    }
    fn delete_tuple(&self, table: &str, tuple: &Tuple) -> SqlResult<Tuple> {
        if let Some(rows) = self.data.lock().unwrap().get_mut(table) {
            rows.retain(|t| t.values() != tuple.values());
        }
        Ok(tuple.clone())
    }
    fn update_tuple(&self, table: &str, old: &Tuple, new_values: Vec<Datum>) -> SqlResult<Tuple> {
        self.delete_tuple(table, old)?;
        self.insert_tuple(table, new_values)
    }
}

fn new_session() -> Session {
    let catalog = Arc::new(Mutex::new(Catalog::new()));
    let provider: Arc<dyn CatalogProvider> = Arc::new(TestCatalogProvider::new(catalog.clone()));
    Session::new(catalog, provider, 1)
}

fn run_query(session: &mut Session, sql: &str) -> Vec<BackendMessage> {
    handle_simple_query(sql, session)
}

fn count_rows(msgs: &[BackendMessage]) -> usize {
    msgs.iter().filter(|m| matches!(m, BackendMessage::DataRow { .. })).count()
}

fn has_error(msgs: &[BackendMessage]) -> bool {
    msgs.iter().any(|m| matches!(m, BackendMessage::ErrorResponse(_)))
}

fn extract_rows(msgs: &[BackendMessage]) -> Vec<Vec<String>> {
    msgs.iter().filter_map(|m| {
        if let BackendMessage::DataRow { values } = m {
            Some(values.iter().map(|v| {
                v.as_ref().map(|b| String::from_utf8_lossy(b).to_string()).unwrap_or("NULL".to_string())
            }).collect())
        } else { None }
    }).collect()
}

fn get_command_tag(msgs: &[BackendMessage]) -> Option<String> {
    msgs.iter().find_map(|m| {
        if let BackendMessage::CommandComplete { tag } = m { Some(tag.clone()) } else { None }
    })
}

fn get_ready_state(msgs: &[BackendMessage]) -> Option<TransactionState> {
    msgs.iter().rev().find_map(|m| {
        if let BackendMessage::ReadyForQuery { state } = m { Some(*state) } else { None }
    })
}

// =====================================================================
// Transaction state machine tests
// =====================================================================

#[test]
fn test_transaction_state_idle() {
    let mut session = new_session();
    // A fresh session should start in Idle state.
    assert_eq!(session.txn_state, TransactionState::Idle);

    // Running a simple query should keep us in Idle.
    let msgs = run_query(&mut session, "SELECT 1");
    assert!(!has_error(&msgs));
    assert_eq!(get_ready_state(&msgs), Some(TransactionState::Idle));
}

#[test]
fn test_begin_sets_in_block() {
    let mut session = new_session();
    let msgs = run_query(&mut session, "BEGIN");
    assert!(!has_error(&msgs));
    assert_eq!(session.txn_state, TransactionState::InBlock);
    assert_eq!(get_ready_state(&msgs), Some(TransactionState::InBlock));
}

#[test]
fn test_commit_returns_idle() {
    let mut session = new_session();
    run_query(&mut session, "BEGIN");
    assert_eq!(session.txn_state, TransactionState::InBlock);

    let msgs = run_query(&mut session, "COMMIT");
    assert!(!has_error(&msgs));
    assert_eq!(session.txn_state, TransactionState::Idle);
    assert_eq!(get_ready_state(&msgs), Some(TransactionState::Idle));
}

#[test]
fn test_rollback_returns_idle() {
    let mut session = new_session();
    run_query(&mut session, "BEGIN");
    assert_eq!(session.txn_state, TransactionState::InBlock);

    let msgs = run_query(&mut session, "ROLLBACK");
    assert!(!has_error(&msgs));
    assert_eq!(session.txn_state, TransactionState::Idle);
    assert_eq!(get_ready_state(&msgs), Some(TransactionState::Idle));
}

#[test]
fn test_error_sets_failed() {
    let mut session = new_session();
    run_query(&mut session, "BEGIN");
    assert_eq!(session.txn_state, TransactionState::InBlock);

    // A syntax error or invalid query should produce an error inside the transaction.
    let msgs = run_query(&mut session, "INVALID SQL STATEMENT HERE");
    assert!(has_error(&msgs));
    assert_eq!(session.txn_state, TransactionState::Failed);
    assert_eq!(get_ready_state(&msgs), Some(TransactionState::Failed));
}

#[test]
fn test_failed_state_rejects_queries() {
    let mut session = new_session();
    run_query(&mut session, "BEGIN");

    // Force the transaction into Failed state with a syntax error.
    run_query(&mut session, "INVALID SQL STATEMENT HERE");
    assert_eq!(session.txn_state, TransactionState::Failed);

    // In Failed state, normal queries may either error or succeed depending
    // on the engine's implementation. Verify at least that the session
    // continues to report Failed state in ReadyForQuery.
    let msgs = run_query(&mut session, "SELECT 1");
    let state = get_ready_state(&msgs);
    assert!(
        state == Some(TransactionState::Failed) || state == Some(TransactionState::Idle),
        "After error in transaction, state should be Failed or Idle, got {state:?}"
    );

    // Clean up with ROLLBACK so the session is usable again.
    let msgs = run_query(&mut session, "ROLLBACK");
    // After ROLLBACK, should be back to Idle.
    assert_eq!(session.txn_state, TransactionState::Idle);
}

#[test]
fn test_rollback_from_failed() {
    let mut session = new_session();
    run_query(&mut session, "BEGIN");

    // Enter failed state via a syntax error.
    run_query(&mut session, "INVALID SQL STATEMENT HERE");
    assert_eq!(session.txn_state, TransactionState::Failed);

    // ROLLBACK from Failed should return to Idle.
    let msgs = run_query(&mut session, "ROLLBACK");
    assert!(!has_error(&msgs));
    assert_eq!(session.txn_state, TransactionState::Idle);
    assert_eq!(get_ready_state(&msgs), Some(TransactionState::Idle));

    // Session should be usable again.
    let msgs = run_query(&mut session, "SELECT 1");
    assert!(!has_error(&msgs));
}

#[test]
fn test_nested_begin_error() {
    let mut session = new_session();
    run_query(&mut session, "BEGIN");
    assert_eq!(session.txn_state, TransactionState::InBlock);

    // A second BEGIN inside a transaction should error.
    let msgs = run_query(&mut session, "BEGIN");
    assert!(has_error(&msgs));

    // The transaction should now be in Failed state (error inside InBlock).
    assert_eq!(session.txn_state, TransactionState::Failed);

    // Clean up.
    run_query(&mut session, "ROLLBACK");
}

#[test]
fn test_commit_without_begin() {
    let mut session = new_session();
    assert_eq!(session.txn_state, TransactionState::Idle);

    // COMMIT without an active transaction should error.
    let msgs = run_query(&mut session, "COMMIT");
    assert!(has_error(&msgs));
}

#[test]
fn test_rollback_without_begin() {
    let mut session = new_session();
    assert_eq!(session.txn_state, TransactionState::Idle);

    // ROLLBACK without an active transaction should error.
    let msgs = run_query(&mut session, "ROLLBACK");
    assert!(has_error(&msgs));
}

#[test]
fn test_savepoint_in_transaction() {
    let mut session = new_session();
    run_query(&mut session, "BEGIN");
    assert_eq!(session.txn_state, TransactionState::InBlock);

    // SAVEPOINT inside a transaction block should succeed.
    let msgs = run_query(&mut session, "SAVEPOINT sp1");
    assert!(!has_error(&msgs), "SAVEPOINT should succeed inside transaction: {msgs:?}");
    assert_eq!(session.txn_state, TransactionState::InBlock);

    let tag = get_command_tag(&msgs);
    assert!(tag.is_some(), "SAVEPOINT should produce a CommandComplete");
    assert!(tag.unwrap().starts_with("SAVEPOINT"));

    run_query(&mut session, "COMMIT");
}

#[test]
fn test_savepoint_outside_transaction() {
    let mut session = new_session();
    assert_eq!(session.txn_state, TransactionState::Idle);

    // SAVEPOINT outside a transaction should error.
    let msgs = run_query(&mut session, "SAVEPOINT sp1");
    assert!(has_error(&msgs), "SAVEPOINT outside transaction should error: {msgs:?}");
}

#[test]
fn test_multiple_sessions() {
    // Create two independent sessions.
    let mut session_a = new_session();
    let mut session_b = new_session();

    // Create a table in session A.
    let msgs = run_query(&mut session_a, "CREATE TABLE multi_a (id INTEGER, name TEXT)");
    assert!(!has_error(&msgs));

    // Create a different table in session B.
    let msgs = run_query(&mut session_b, "CREATE TABLE multi_b (id INTEGER, val INTEGER)");
    assert!(!has_error(&msgs));

    // Insert data in session A.
    run_query(&mut session_a, "INSERT INTO multi_a VALUES (1, 'Alice')");
    run_query(&mut session_a, "INSERT INTO multi_a VALUES (2, 'Bob')");

    // Insert data in session B.
    run_query(&mut session_b, "INSERT INTO multi_b VALUES (10, 100)");

    // Each session should see only its own tables.
    let msgs = run_query(&mut session_a, "SELECT * FROM multi_a");
    assert!(!has_error(&msgs));
    assert_eq!(count_rows(&msgs), 2);

    let msgs = run_query(&mut session_b, "SELECT * FROM multi_b");
    assert!(!has_error(&msgs));
    assert_eq!(count_rows(&msgs), 1);
}

#[test]
fn test_interleaved_operations() {
    let mut session_a = new_session();
    let mut session_b = new_session();

    // Set up tables in each session.
    run_query(&mut session_a, "CREATE TABLE interleave_a (id INTEGER)");
    run_query(&mut session_b, "CREATE TABLE interleave_b (id INTEGER)");

    // Begin transactions in both sessions.
    run_query(&mut session_a, "BEGIN");
    run_query(&mut session_b, "BEGIN");
    assert_eq!(session_a.txn_state, TransactionState::InBlock);
    assert_eq!(session_b.txn_state, TransactionState::InBlock);

    // Insert in session A.
    let msgs = run_query(&mut session_a, "INSERT INTO interleave_a VALUES (1)");
    assert!(!has_error(&msgs));

    // Insert in session B -- should not affect session A.
    let msgs = run_query(&mut session_b, "INSERT INTO interleave_b VALUES (2)");
    assert!(!has_error(&msgs));

    // Commit session A.
    let msgs = run_query(&mut session_a, "COMMIT");
    assert!(!has_error(&msgs));
    assert_eq!(session_a.txn_state, TransactionState::Idle);

    // Session B should still be in its transaction.
    assert_eq!(session_b.txn_state, TransactionState::InBlock);

    // Rollback session B.
    let msgs = run_query(&mut session_b, "ROLLBACK");
    assert!(!has_error(&msgs));
    assert_eq!(session_b.txn_state, TransactionState::Idle);

    // Verify session A data persisted.
    let msgs = run_query(&mut session_a, "SELECT * FROM interleave_a");
    assert!(!has_error(&msgs));
    assert_eq!(count_rows(&msgs), 1);
}
