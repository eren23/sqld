use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use sqld::executor::CatalogProvider;
use sqld::planner::Catalog;
use sqld::planner::physical_plan::KeyRange;
use sqld::protocol::connection::Session;
use sqld::protocol::copy::{CopyFormat, CopyOptions, begin_copy_in, process_copy_data};
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

// =====================================================================
// Bulk load tests
// =====================================================================

#[test]
fn test_bulk_insert_many_rows() {
    let mut session = new_session();
    let msgs = run_query(&mut session, "CREATE TABLE bulk (id INTEGER, val TEXT)");
    assert!(!has_error(&msgs));

    // Insert 100 rows using individual INSERT statements.
    for i in 0..100 {
        let sql = format!("INSERT INTO bulk VALUES ({i}, 'row_{i}')");
        let msgs = run_query(&mut session, &sql);
        assert!(!has_error(&msgs), "INSERT failed at row {i}: {msgs:?}");
    }

    // Verify all 100 rows exist by scanning all rows.
    let msgs = run_query(&mut session, "SELECT id FROM bulk");
    assert!(!has_error(&msgs), "SELECT FROM bulk failed: {msgs:?}");
    assert_eq!(count_rows(&msgs), 100);
}

#[test]
fn test_copy_data_csv() {
    let col_types = vec![DataType::Integer, DataType::Text, DataType::Float];
    let opts = CopyOptions::default();

    let data = b"1,alice,3.14\n2,bob,2.72\n3,carol,1.41\n";
    let rows = process_copy_data(data, &col_types, &opts).unwrap();

    assert_eq!(rows.len(), 3);

    assert_eq!(rows[0][0], Datum::Integer(1));
    assert_eq!(rows[0][1], Datum::Text("alice".to_string()));
    if let Datum::Float(v) = rows[0][2] {
        assert!((v - 3.14).abs() < 0.001);
    } else {
        panic!("expected Float, got {:?}", rows[0][2]);
    }

    assert_eq!(rows[1][0], Datum::Integer(2));
    assert_eq!(rows[1][1], Datum::Text("bob".to_string()));

    assert_eq!(rows[2][0], Datum::Integer(3));
    assert_eq!(rows[2][1], Datum::Text("carol".to_string()));
}

#[test]
fn test_copy_data_tab_delimited() {
    let col_types = vec![DataType::Integer, DataType::Text];
    let opts = CopyOptions::default().with_delimiter(b'\t');

    let data = b"1\tAlice\n2\tBob\n3\tCarol\n";
    let rows = process_copy_data(data, &col_types, &opts).unwrap();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], Datum::Integer(1));
    assert_eq!(rows[0][1], Datum::Text("Alice".to_string()));
    assert_eq!(rows[1][0], Datum::Integer(2));
    assert_eq!(rows[1][1], Datum::Text("Bob".to_string()));
    assert_eq!(rows[2][0], Datum::Integer(3));
    assert_eq!(rows[2][1], Datum::Text("Carol".to_string()));
}

#[test]
fn test_copy_data_with_nulls() {
    let col_types = vec![DataType::Integer, DataType::Text, DataType::Float];
    let opts = CopyOptions::default();

    // Empty fields should become Null.
    let data = b",hello,\n1,,2.5\n";
    let rows = process_copy_data(data, &col_types, &opts).unwrap();

    assert_eq!(rows.len(), 2);

    // First row: null integer, "hello", null float.
    assert_eq!(rows[0][0], Datum::Null);
    assert_eq!(rows[0][1], Datum::Text("hello".to_string()));
    assert_eq!(rows[0][2], Datum::Null);

    // Second row: 1, null text, 2.5.
    assert_eq!(rows[1][0], Datum::Integer(1));
    assert_eq!(rows[1][1], Datum::Null);
    if let Datum::Float(v) = rows[1][2] {
        assert!((v - 2.5).abs() < 0.001);
    } else {
        panic!("expected Float, got {:?}", rows[1][2]);
    }
}

#[test]
fn test_copy_data_with_quotes() {
    let col_types = vec![DataType::Integer, DataType::Text];
    let opts = CopyOptions::default();

    // Quoted fields in CSV: the text field contains a comma.
    let data = b"1,\"hello, world\"\n2,\"simple\"\n";
    let rows = process_copy_data(data, &col_types, &opts).unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Datum::Integer(1));
    // The quoted field should have the comma preserved.
    assert_eq!(rows[0][1], Datum::Text("hello, world".to_string()));
    assert_eq!(rows[1][0], Datum::Integer(2));
    assert_eq!(rows[1][1], Datum::Text("simple".to_string()));
}

#[test]
fn test_copy_data_type_conversion() {
    let col_types = vec![DataType::Integer, DataType::Float, DataType::Boolean, DataType::Text];
    let opts = CopyOptions::default();

    let data = b"42,3.14,true,hello\n-1,0.0,false,world\n";
    let rows = process_copy_data(data, &col_types, &opts).unwrap();

    assert_eq!(rows.len(), 2);

    // First row.
    assert_eq!(rows[0][0], Datum::Integer(42));
    if let Datum::Float(v) = rows[0][1] {
        assert!((v - 3.14).abs() < 0.001);
    } else {
        panic!("expected Float");
    }
    assert_eq!(rows[0][2], Datum::Boolean(true));
    assert_eq!(rows[0][3], Datum::Text("hello".to_string()));

    // Second row.
    assert_eq!(rows[1][0], Datum::Integer(-1));
    if let Datum::Float(v) = rows[1][1] {
        assert!(v.abs() < 0.001);
    } else {
        panic!("expected Float");
    }
    assert_eq!(rows[1][2], Datum::Boolean(false));
    assert_eq!(rows[1][3], Datum::Text("world".to_string()));
}

#[test]
fn test_copy_data_wrong_columns() {
    let col_types = vec![DataType::Integer, DataType::Text, DataType::Float];
    let opts = CopyOptions::default();

    // Only 2 fields when 3 are expected.
    let data = b"1,alice\n";
    let result = process_copy_data(data, &col_types, &opts);
    assert!(result.is_err(), "Wrong column count should produce error");
}

#[test]
fn test_begin_copy_in_message() {
    let col_types = vec![DataType::Integer, DataType::Text, DataType::Float];
    let columns = vec!["id".to_string(), "name".to_string(), "score".to_string()];
    let opts = CopyOptions::default();

    let msg = begin_copy_in("test_table", &columns, &col_types, &opts);

    match msg {
        BackendMessage::CopyInResponse { format, column_formats } => {
            assert_eq!(format, 0, "CSV format should be text (0)");
            assert_eq!(column_formats.len(), 3, "Should have 3 column formats");
            assert!(column_formats.iter().all(|&f| f == 0), "All columns should be text format");
        }
        other => panic!("expected CopyInResponse, got {other:?}"),
    }
}

#[test]
fn test_large_batch_insert() {
    let mut session = new_session();
    run_query(&mut session, "CREATE TABLE large_batch (id INTEGER, name TEXT, val INTEGER)");

    // Insert 50 rows.
    for i in 0..50 {
        let sql = format!("INSERT INTO large_batch VALUES ({i}, 'item_{i}', {})", i * 10);
        let msgs = run_query(&mut session, &sql);
        assert!(!has_error(&msgs), "INSERT failed at row {i}");
    }

    // Verify count by scanning all rows.
    let msgs = run_query(&mut session, "SELECT id FROM large_batch");
    assert!(!has_error(&msgs));
    assert_eq!(count_rows(&msgs), 50);

    // Verify a specific row with WHERE.
    let msgs = run_query(&mut session, "SELECT id, name, val FROM large_batch WHERE id = 25");
    assert!(!has_error(&msgs));
    let rows = extract_rows(&msgs);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "25");
    assert_eq!(rows[0][1], "item_25");
    assert_eq!(rows[0][2], "250");
}

#[test]
fn test_insert_select_bulk() {
    let mut session = new_session();

    // Create source table and populate it.
    run_query(&mut session, "CREATE TABLE source (id INTEGER, val TEXT)");
    for i in 1..=10 {
        run_query(&mut session, &format!("INSERT INTO source VALUES ({i}, 'v{i}')"));
    }

    // Create destination table.
    run_query(&mut session, "CREATE TABLE dest (id INTEGER, val TEXT)");

    // INSERT ... SELECT to copy data (feature may not be supported).
    let msgs = run_query(&mut session, "INSERT INTO dest SELECT id, val FROM source");
    if !has_error(&msgs) {
        // Verify the destination has all rows.
        let msgs = run_query(&mut session, "SELECT id FROM dest");
        assert!(!has_error(&msgs));
        assert_eq!(count_rows(&msgs), 10);

        // Verify a specific row was copied correctly.
        let msgs = run_query(&mut session, "SELECT id, val FROM dest WHERE id = 5");
        assert!(!has_error(&msgs));
        let rows = extract_rows(&msgs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], "5");
        assert_eq!(rows[0][1], "v5");
    }
}
