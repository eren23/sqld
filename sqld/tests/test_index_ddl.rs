use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use sqld::planner::Catalog;
use sqld::planner::physical_plan::KeyRange;
use sqld::types::*;
use sqld::protocol::messages::*;
use sqld::protocol::connection::Session;
use sqld::protocol::simple_query::handle_simple_query;

struct MemCatalog {
    catalog: Arc<Mutex<Catalog>>,
    data: Mutex<HashMap<String, Vec<Tuple>>>,
}
impl MemCatalog {
    fn new(catalog: Arc<Mutex<Catalog>>) -> Self {
        Self { catalog, data: Mutex::new(HashMap::new()) }
    }
}
impl sqld::executor::CatalogProvider for MemCatalog {
    fn table_schema(&self, table: &str) -> sqld::utils::error::Result<Schema> {
        self.catalog.lock().unwrap().get_schema(table).cloned()
            .ok_or_else(|| sqld::utils::error::SqlError::ExecutionError(format!("table not found: {table}")).into())
    }
    fn scan_table(&self, table: &str) -> sqld::utils::error::Result<Vec<Tuple>> {
        Ok(self.data.lock().unwrap().get(table).cloned().unwrap_or_default())
    }
    fn scan_index(&self, table: &str, _: &str, _: &[KeyRange]) -> sqld::utils::error::Result<Vec<Tuple>> {
        self.scan_table(table)
    }
    fn insert_tuple(&self, table: &str, values: Vec<Datum>) -> sqld::utils::error::Result<Tuple> {
        let tuple = Tuple::new(MvccHeader::new_insert(0, 0), values);
        self.data.lock().unwrap().entry(table.to_string()).or_default().push(tuple.clone());
        Ok(tuple)
    }
    fn delete_tuple(&self, table: &str, tuple: &Tuple) -> sqld::utils::error::Result<Tuple> {
        self.data.lock().unwrap().entry(table.to_string()).or_default().retain(|t| t.values() != tuple.values());
        Ok(tuple.clone())
    }
    fn update_tuple(&self, table: &str, old: &Tuple, new_values: Vec<Datum>) -> sqld::utils::error::Result<Tuple> {
        self.delete_tuple(table, old)?;
        self.insert_tuple(table, new_values)
    }
}

fn make_session() -> Session {
    let catalog = Arc::new(Mutex::new(Catalog::new()));
    let cp: Arc<dyn sqld::executor::CatalogProvider> = Arc::new(MemCatalog::new(catalog.clone()));
    Session::new(catalog, cp, 1)
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

fn has_error(msgs: &[BackendMessage]) -> bool {
    msgs.iter().any(|m| matches!(m, BackendMessage::ErrorResponse(_)))
}

fn row_count(msgs: &[BackendMessage]) -> usize {
    msgs.iter().filter(|m| matches!(m, BackendMessage::DataRow { .. })).count()
}

fn has_command_complete(msgs: &[BackendMessage], prefix: &str) -> bool {
    msgs.iter().any(|m| {
        if let BackendMessage::CommandComplete { tag } = m {
            tag.starts_with(prefix)
        } else {
            false
        }
    })
}

fn get_error_code(msgs: &[BackendMessage]) -> Option<String> {
    msgs.iter().find_map(|m| {
        if let BackendMessage::ErrorResponse(e) = m {
            Some(e.code.clone())
        } else {
            None
        }
    })
}

// =====================================================================
// Tests
// =====================================================================

#[test]
fn test_create_index_on_table() {
    let mut session = make_session();
    handle_simple_query("CREATE TABLE idx_tbl (id INTEGER, name TEXT, val INTEGER)", &mut session);

    let msgs = handle_simple_query("CREATE INDEX idx_name ON idx_tbl (name)", &mut session);
    assert!(!has_error(&msgs), "CREATE INDEX should succeed: {msgs:?}");
    assert!(has_command_complete(&msgs, "CREATE INDEX"));
}

#[test]
fn test_create_unique_index() {
    let mut session = make_session();
    handle_simple_query("CREATE TABLE uidx_tbl (id INTEGER, email TEXT)", &mut session);

    let msgs = handle_simple_query("CREATE UNIQUE INDEX uidx_email ON uidx_tbl (email)", &mut session);
    assert!(!has_error(&msgs), "CREATE UNIQUE INDEX should succeed: {msgs:?}");
    assert!(has_command_complete(&msgs, "CREATE INDEX"));
}

#[test]
fn test_drop_index() {
    let mut session = make_session();
    handle_simple_query("CREATE TABLE didx_tbl (id INTEGER, val INTEGER)", &mut session);
    handle_simple_query("CREATE INDEX didx_val ON didx_tbl (val)", &mut session);

    let msgs = handle_simple_query("DROP INDEX didx_val", &mut session);
    assert!(!has_error(&msgs), "DROP INDEX should succeed: {msgs:?}");
    assert!(has_command_complete(&msgs, "DROP INDEX"));
}

#[test]
fn test_drop_index_nonexistent_errors() {
    let mut session = make_session();

    let msgs = handle_simple_query("DROP INDEX no_such_index", &mut session);
    assert!(has_error(&msgs), "DROP nonexistent INDEX should error");
}

#[test]
fn test_drop_index_if_exists() {
    let mut session = make_session();

    let msgs = handle_simple_query("DROP INDEX IF EXISTS no_such_index", &mut session);
    assert!(!has_error(&msgs), "DROP INDEX IF EXISTS should not error: {msgs:?}");
    assert!(has_command_complete(&msgs, "DROP INDEX"));
}

#[test]
fn test_create_index_on_nonexistent_table() {
    let mut session = make_session();

    let msgs = handle_simple_query("CREATE INDEX idx_bad ON nonexistent_table (col)", &mut session);
    assert!(has_error(&msgs), "CREATE INDEX on nonexistent table should error");
}

#[test]
fn test_explain_select() {
    let mut session = make_session();
    handle_simple_query("CREATE TABLE explain_tbl (id INTEGER, name TEXT)", &mut session);

    let msgs = handle_simple_query("EXPLAIN SELECT * FROM explain_tbl", &mut session);
    assert!(!has_error(&msgs), "EXPLAIN should succeed: {msgs:?}");
    assert!(has_command_complete(&msgs, "EXPLAIN"));
    // EXPLAIN should produce at least one DataRow with the query plan
    assert!(row_count(&msgs) > 0, "EXPLAIN should produce plan output");
}

#[test]
fn test_show_columns() {
    let mut session = make_session();
    handle_simple_query("CREATE TABLE showcol_tbl (id INTEGER, name TEXT, active BOOLEAN)", &mut session);

    let msgs = handle_simple_query("SHOW COLUMNS FROM showcol_tbl", &mut session);
    assert!(!has_error(&msgs), "SHOW COLUMNS should succeed: {msgs:?}");
    let rows = extract_rows(&msgs);
    assert_eq!(rows.len(), 3);
    // Check column names are present
    let col_names: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(col_names.contains(&"id"));
    assert!(col_names.contains(&"name"));
    assert!(col_names.contains(&"active"));
}

#[test]
fn test_analyze_statement() {
    let mut session = make_session();
    handle_simple_query("CREATE TABLE analyze_tbl (id INTEGER)", &mut session);

    let msgs = handle_simple_query("ANALYZE analyze_tbl", &mut session);
    assert!(!has_error(&msgs), "ANALYZE should succeed: {msgs:?}");
    assert!(has_command_complete(&msgs, "ANALYZE"));
}

#[test]
fn test_vacuum_statement() {
    let mut session = make_session();

    let msgs = handle_simple_query("VACUUM", &mut session);
    assert!(!has_error(&msgs), "VACUUM should succeed: {msgs:?}");
    assert!(has_command_complete(&msgs, "VACUUM"));
}

#[test]
fn test_drop_table_then_recreate() {
    let mut session = make_session();
    handle_simple_query("CREATE TABLE recreate_tbl (id INTEGER)", &mut session);

    let msgs = handle_simple_query("DROP TABLE recreate_tbl", &mut session);
    assert!(!has_error(&msgs));

    // Recreate with different schema
    let msgs = handle_simple_query("CREATE TABLE recreate_tbl (id INTEGER, name TEXT, score INTEGER)", &mut session);
    assert!(!has_error(&msgs), "Recreating table should succeed: {msgs:?}");

    // Insert with new schema
    let msgs = handle_simple_query("INSERT INTO recreate_tbl VALUES (1, 'Alice', 100)", &mut session);
    assert!(!has_error(&msgs));

    let msgs = handle_simple_query("SELECT id, name, score FROM recreate_tbl", &mut session);
    let rows = extract_rows(&msgs);
    assert!(rows.len() >= 1, "Should have at least the inserted row");
    // Verify the last inserted row is present
    let last_row = rows.last().unwrap();
    assert_eq!(last_row[0], "1");
    assert_eq!(last_row[1], "Alice");
    assert_eq!(last_row[2], "100");
}
