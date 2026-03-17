use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use sqld::executor::CatalogProvider;
use sqld::planner::Catalog;
use sqld::planner::physical_plan::KeyRange;
use sqld::protocol::connection::Session;
use sqld::protocol::BackendMessage;
use sqld::protocol::simple_query::handle_simple_query;
use sqld::types::*;
use sqld::utils::error::Result as SqlResult;

// ---------------------------------------------------------------------------
// In-memory CatalogProvider for integration tests
// ---------------------------------------------------------------------------

struct TestCatalogProvider {
    catalog: Arc<Mutex<Catalog>>,
    data: Mutex<HashMap<String, Vec<Tuple>>>,
}

impl TestCatalogProvider {
    fn new(catalog: Arc<Mutex<Catalog>>) -> Self {
        Self {
            catalog,
            data: Mutex::new(HashMap::new()),
        }
    }
}

impl CatalogProvider for TestCatalogProvider {
    fn table_schema(&self, table: &str) -> SqlResult<Schema> {
        self.catalog
            .lock()
            .unwrap()
            .get_schema(table)
            .cloned()
            .ok_or_else(|| {
                sqld::utils::error::SqlError::ExecutionError(format!(
                    "table \"{table}\" does not exist"
                ))
                .into()
            })
    }

    fn scan_table(&self, table: &str) -> SqlResult<Vec<Tuple>> {
        Ok(self
            .data
            .lock()
            .unwrap()
            .get(table)
            .cloned()
            .unwrap_or_default())
    }

    fn scan_index(&self, table: &str, _: &str, _: &[KeyRange]) -> SqlResult<Vec<Tuple>> {
        self.scan_table(table)
    }

    fn insert_tuple(&self, table: &str, values: Vec<Datum>) -> SqlResult<Tuple> {
        let tuple = Tuple::new(MvccHeader::new_insert(0, 0), values);
        self.data
            .lock()
            .unwrap()
            .entry(table.to_string())
            .or_default()
            .push(tuple.clone());
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn new_session() -> Session {
    let catalog = Arc::new(Mutex::new(Catalog::new()));
    let provider: Arc<dyn CatalogProvider> =
        Arc::new(TestCatalogProvider::new(catalog.clone()));
    Session::new(catalog, provider, 1)
}

fn run(session: &mut Session, sql: &str) -> Vec<BackendMessage> {
    handle_simple_query(sql, session)
}

fn has_error(msgs: &[BackendMessage]) -> bool {
    msgs.iter()
        .any(|m| matches!(m, BackendMessage::ErrorResponse(_)))
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

fn extract_rows(msgs: &[BackendMessage]) -> Vec<Vec<String>> {
    msgs.iter()
        .filter_map(|m| {
            if let BackendMessage::DataRow { values } = m {
                Some(
                    values
                        .iter()
                        .map(|v| {
                            v.as_ref()
                                .map(|b| String::from_utf8_lossy(b).to_string())
                                .unwrap_or("NULL".to_string())
                        })
                        .collect(),
                )
            } else {
                None
            }
        })
        .collect()
}

// =====================================================================
// View tests
// =====================================================================

#[test]
fn test_create_view() {
    let mut s = new_session();
    run(&mut s, "CREATE TABLE products (id INTEGER, name TEXT, price INTEGER)");

    let msgs = run(
        &mut s,
        "CREATE VIEW cheap_products AS SELECT id, name FROM products WHERE price < 100",
    );
    assert!(!has_error(&msgs), "CREATE VIEW should succeed: {msgs:?}");
    assert!(has_command_complete(&msgs, "CREATE VIEW"));
}

#[test]
fn test_drop_view() {
    let mut s = new_session();
    run(&mut s, "CREATE TABLE base_tbl (id INTEGER)");
    run(&mut s, "CREATE VIEW v_drop AS SELECT id FROM base_tbl");

    let msgs = run(&mut s, "DROP VIEW v_drop");
    assert!(!has_error(&msgs), "DROP VIEW should succeed: {msgs:?}");
    assert!(has_command_complete(&msgs, "DROP VIEW"));

    // Dropping again without IF EXISTS should error
    let msgs = run(&mut s, "DROP VIEW v_drop");
    assert!(has_error(&msgs), "DROP VIEW on missing view should error");
}

#[test]
fn test_drop_view_if_exists() {
    let mut s = new_session();

    // Drop a view that never existed -- IF EXISTS prevents error
    let msgs = run(&mut s, "DROP VIEW IF EXISTS nonexistent_view");
    assert!(
        !has_error(&msgs),
        "DROP VIEW IF EXISTS on nonexistent should not error: {msgs:?}"
    );
    assert!(has_command_complete(&msgs, "DROP VIEW"));
}

#[test]
fn test_duplicate_view() {
    let mut s = new_session();
    run(&mut s, "CREATE TABLE dup_base (id INTEGER)");

    let msgs = run(&mut s, "CREATE VIEW dup_view AS SELECT id FROM dup_base");
    assert!(!has_error(&msgs), "First CREATE VIEW should succeed: {msgs:?}");

    // Creating the same view again should produce an error (relation already exists)
    let msgs = run(&mut s, "CREATE VIEW dup_view AS SELECT id FROM dup_base");
    assert!(has_error(&msgs), "Duplicate CREATE VIEW should error: {msgs:?}");
    assert_eq!(
        get_error_code(&msgs),
        Some("42P07".to_string()),
        "Duplicate relation error code should be 42P07"
    );
}

#[test]
fn test_view_lifecycle() {
    let mut s = new_session();
    run(&mut s, "CREATE TABLE life_base (id INTEGER, val TEXT)");
    run(&mut s, "INSERT INTO life_base VALUES (1, 'hello')");
    run(&mut s, "INSERT INTO life_base VALUES (2, 'world')");

    // Create view
    let msgs = run(
        &mut s,
        "CREATE VIEW life_view AS SELECT id, val FROM life_base",
    );
    assert!(!has_error(&msgs), "CREATE VIEW should succeed: {msgs:?}");
    assert!(has_command_complete(&msgs, "CREATE VIEW"));

    // The view is registered in the catalog (SHOW TABLES lists it alongside tables)
    let msgs = run(&mut s, "SHOW TABLES");
    assert!(!has_error(&msgs));
    let rows = extract_rows(&msgs);
    let names: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(
        names.contains(&"life_view"),
        "View should appear in SHOW TABLES: {names:?}"
    );

    // Drop view
    let msgs = run(&mut s, "DROP VIEW life_view");
    assert!(!has_error(&msgs), "DROP VIEW should succeed: {msgs:?}");
    assert!(has_command_complete(&msgs, "DROP VIEW"));

    // Confirm view is gone from catalog
    let msgs = run(&mut s, "SHOW TABLES");
    let rows = extract_rows(&msgs);
    let names: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(
        !names.contains(&"life_view"),
        "View should no longer appear after DROP: {names:?}"
    );
}
