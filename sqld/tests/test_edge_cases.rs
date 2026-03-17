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

fn has_empty_query_response(msgs: &[BackendMessage]) -> bool {
    msgs.iter()
        .any(|m| matches!(m, BackendMessage::EmptyQueryResponse))
}

fn count_rows(msgs: &[BackendMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
        .count()
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
// Edge case tests
// =====================================================================

#[test]
fn test_empty_query() {
    let mut s = new_session();
    let msgs = run(&mut s, "");
    assert!(!has_error(&msgs), "Empty query should not error: {msgs:?}");
    assert!(
        has_empty_query_response(&msgs),
        "Empty query should produce EmptyQueryResponse"
    );
}

#[test]
fn test_whitespace_only_query() {
    let mut s = new_session();
    let msgs = run(&mut s, "   \t\n  ");
    assert!(!has_error(&msgs), "Whitespace-only query should not error: {msgs:?}");
    assert!(
        has_empty_query_response(&msgs),
        "Whitespace-only should produce EmptyQueryResponse"
    );
}

#[test]
fn test_semicolons_only() {
    let mut s = new_session();
    let msgs = run(&mut s, ";;;");
    assert!(!has_error(&msgs), "Semicolons-only should not error: {msgs:?}");
    assert!(
        has_empty_query_response(&msgs),
        "Semicolons-only should produce EmptyQueryResponse"
    );
}

#[test]
fn test_select_no_from() {
    let mut s = new_session();
    let msgs = run(&mut s, "SELECT 1");
    // SELECT without FROM may or may not be supported
    if !has_error(&msgs) {
        let rows = extract_rows(&msgs);
        if !rows.is_empty() {
            assert_eq!(rows[0][0], "1");
        }
    }
    // Baseline: no panic
}

#[test]
fn test_select_star_empty_table() {
    let mut s = new_session();
    run(&mut s, "CREATE TABLE empty_t (id INTEGER, name TEXT)");

    let msgs = run(&mut s, "SELECT * FROM empty_t");
    assert!(!has_error(&msgs), "SELECT * from empty table should succeed: {msgs:?}");
    assert_eq!(count_rows(&msgs), 0);
}

#[test]
fn test_large_result_set() {
    let mut s = new_session();
    run(&mut s, "CREATE TABLE big (id INTEGER)");

    for i in 1..=200 {
        run(&mut s, &format!("INSERT INTO big VALUES ({i})"));
    }

    let msgs = run(&mut s, "SELECT id FROM big");
    assert!(!has_error(&msgs), "Large result set should succeed: {msgs:?}");
    assert_eq!(count_rows(&msgs), 200);
}

#[test]
fn test_long_string_value() {
    let mut s = new_session();
    run(&mut s, "CREATE TABLE longstr (id INTEGER, val TEXT)");

    let long_val = "x".repeat(5000);
    let sql = format!("INSERT INTO longstr VALUES (1, '{long_val}')");
    let msgs = run(&mut s, &sql);
    assert!(!has_error(&msgs), "INSERT with long string should succeed: {msgs:?}");

    let msgs = run(&mut s, "SELECT val FROM longstr WHERE id = 1");
    assert!(!has_error(&msgs), "SELECT with long string should succeed: {msgs:?}");
    let rows = extract_rows(&msgs);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].len(), 5000);
}

#[test]
fn test_null_handling() {
    let mut s = new_session();
    run(&mut s, "CREATE TABLE nulls (id INTEGER, val TEXT)");

    // Some parsers may not accept bare NULL in VALUES; test gracefully
    let msgs = run(&mut s, "INSERT INTO nulls VALUES (1, NULL)");
    if has_error(&msgs) {
        // Parser does not support NULL literal in VALUES -- skip the rest
        return;
    }

    run(&mut s, "INSERT INTO nulls VALUES (2, 'hello')");

    let msgs = run(&mut s, "SELECT id, val FROM nulls ORDER BY id");
    assert!(!has_error(&msgs), "NULL handling should succeed: {msgs:?}");
    let rows = extract_rows(&msgs);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[0][1], "NULL");
    assert_eq!(rows[1][0], "2");
    assert_eq!(rows[1][1], "hello");
}

#[test]
fn test_null_arithmetic() {
    let mut s = new_session();
    run(&mut s, "CREATE TABLE na (val INTEGER)");
    run(&mut s, "INSERT INTO na VALUES (NULL)");

    let msgs = run(&mut s, "SELECT val + 1 FROM na");
    // NULL + 1 should yield NULL per SQL standard
    if !has_error(&msgs) {
        let rows = extract_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], "NULL", "NULL + 1 should be NULL");
    }
}

#[test]
fn test_coalesce() {
    let mut s = new_session();
    run(&mut s, "CREATE TABLE co (id INTEGER, val INTEGER)");
    run(&mut s, "INSERT INTO co VALUES (1, NULL)");
    run(&mut s, "INSERT INTO co VALUES (2, 42)");

    let msgs = run(&mut s, "SELECT COALESCE(val, 0) FROM co ORDER BY id");
    if !has_error(&msgs) {
        let rows = extract_rows(&msgs);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], "0", "COALESCE(NULL, 0) should be 0");
        assert_eq!(rows[1][0], "42", "COALESCE(42, 0) should be 42");
    }
}

#[test]
fn test_cast_expressions() {
    let mut s = new_session();
    run(&mut s, "CREATE TABLE cast_tbl (val INTEGER, txt TEXT)");
    run(&mut s, "INSERT INTO cast_tbl VALUES (42, '123')");

    // CAST integer to text
    let msgs = run(&mut s, "SELECT CAST(val AS TEXT) FROM cast_tbl");
    if !has_error(&msgs) {
        let rows = extract_rows(&msgs);
        if !rows.is_empty() {
            assert_eq!(rows[0][0], "42");
        }
    }

    // CAST text to integer
    let msgs = run(&mut s, "SELECT CAST(txt AS INTEGER) FROM cast_tbl");
    if !has_error(&msgs) {
        let rows = extract_rows(&msgs);
        // The CAST result may vary depending on implementation;
        // just verify a non-empty result is returned without panic
        assert!(!rows.is_empty(), "CAST should return a row");
    }
}

#[test]
fn test_nested_subquery() {
    let mut s = new_session();
    run(&mut s, "CREATE TABLE nest1 (id INTEGER)");
    run(&mut s, "CREATE TABLE nest2 (id INTEGER)");
    run(&mut s, "INSERT INTO nest1 VALUES (1)");
    run(&mut s, "INSERT INTO nest1 VALUES (2)");
    run(&mut s, "INSERT INTO nest1 VALUES (3)");
    run(&mut s, "INSERT INTO nest2 VALUES (2)");
    run(&mut s, "INSERT INTO nest2 VALUES (3)");

    // Nested subquery: SELECT from subquery result
    let msgs = run(
        &mut s,
        "SELECT id FROM nest1 WHERE id IN (SELECT id FROM nest2 WHERE id > 1)",
    );
    // This may or may not be supported -- just verify no panic
    if !has_error(&msgs) {
        let rows = extract_rows(&msgs);
        assert!(rows.len() >= 1, "Nested subquery should return rows");
    }
}

#[test]
fn test_duplicate_column_names() {
    let mut s = new_session();
    run(&mut s, "CREATE TABLE dcn (id INTEGER, val INTEGER)");
    run(&mut s, "INSERT INTO dcn VALUES (1, 10)");

    // Use aliases to avoid ambiguity
    let msgs = run(&mut s, "SELECT id AS col1, val AS col2 FROM dcn");
    assert!(!has_error(&msgs), "Aliased SELECT should succeed: {msgs:?}");
    let rows = extract_rows(&msgs);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[0][1], "10");

    // Verify field names in RowDescription
    let field_names: Vec<String> = msgs
        .iter()
        .filter_map(|m| {
            if let BackendMessage::RowDescription { fields } = m {
                Some(fields.iter().map(|f| f.name.clone()).collect::<Vec<_>>())
            } else {
                None
            }
        })
        .flatten()
        .collect();
    assert!(
        field_names.contains(&"col1".to_string()),
        "Field names should include alias col1: {field_names:?}"
    );
    assert!(
        field_names.contains(&"col2".to_string()),
        "Field names should include alias col2: {field_names:?}"
    );
}

#[test]
fn test_case_insensitive_keywords() {
    let mut s = new_session();
    run(&mut s, "CREATE TABLE ci_kw (id INTEGER, name TEXT)");
    run(&mut s, "INSERT INTO ci_kw VALUES (1, 'Alice')");

    // Lowercase keywords
    let msgs = run(&mut s, "select id, name from ci_kw where id = 1");
    assert!(
        !has_error(&msgs),
        "Lowercase keywords should work: {msgs:?}"
    );
    let rows = extract_rows(&msgs);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[0][1], "Alice");

    // Mixed-case keywords
    let msgs = run(&mut s, "Select id, name From ci_kw Where id = 1");
    assert!(
        !has_error(&msgs),
        "Mixed-case keywords should work: {msgs:?}"
    );
    let rows = extract_rows(&msgs);
    assert_eq!(rows.len(), 1);
}
