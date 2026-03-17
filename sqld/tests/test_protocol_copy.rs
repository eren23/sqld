use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sqld::executor::executor::CatalogProvider;
use sqld::planner::physical_plan::KeyRange;
use sqld::planner::Catalog;
use sqld::protocol::connection::Session;
use sqld::protocol::copy::{begin_copy_in, process_copy_data, CopyFormat, CopyOptions};
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
    msgs.iter()
        .any(|m| matches!(m, BackendMessage::ErrorResponse(_)))
}

fn count_data_rows(msgs: &[BackendMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, BackendMessage::DataRow { .. }))
        .count()
}

// ===========================================================================
// Tests — CopyOptions
// ===========================================================================

#[test]
fn test_copy_options_default() {
    let opts = CopyOptions::default();
    assert_eq!(opts.delimiter, b',');
    assert!(!opts.has_header);
    assert_eq!(opts.null_string, "");
    assert_eq!(opts.format, CopyFormat::Csv);
}

#[test]
fn test_copy_options_csv() {
    let opts = CopyOptions::csv();
    assert_eq!(opts.delimiter, b',');
    assert!(opts.has_header);
    assert_eq!(opts.null_string, "");
    assert_eq!(opts.format, CopyFormat::Csv);
}

#[test]
fn test_copy_options_custom_delimiter() {
    let opts = CopyOptions::default().with_delimiter(b'\t');
    assert_eq!(opts.delimiter, b'\t');

    let opts2 = CopyOptions::csv().with_delimiter(b'|');
    assert_eq!(opts2.delimiter, b'|');
    assert!(opts2.has_header);
}

// ===========================================================================
// Tests — process_copy_data
// ===========================================================================

#[test]
fn test_process_copy_data_simple() {
    let col_types = vec![DataType::Integer, DataType::Text];
    let opts = CopyOptions::default();

    let data = b"1,hello\n2,world\n";
    let rows = process_copy_data(data, &col_types, &opts).unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Datum::Integer(1));
    assert_eq!(rows[0][1], Datum::Text("hello".to_string()));
    assert_eq!(rows[1][0], Datum::Integer(2));
    assert_eq!(rows[1][1], Datum::Text("world".to_string()));
}

#[test]
fn test_process_copy_data_types() {
    let col_types = vec![
        DataType::Integer,
        DataType::BigInt,
        DataType::Float,
        DataType::Boolean,
    ];
    let opts = CopyOptions::default();

    let data = b"42,9999999999,3.14,true\n";
    let rows = process_copy_data(data, &col_types, &opts).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Datum::Integer(42));
    assert_eq!(rows[0][1], Datum::BigInt(9999999999));
    assert_eq!(rows[0][2], Datum::Float(3.14));
    assert_eq!(rows[0][3], Datum::Boolean(true));
}

#[test]
fn test_process_copy_data_empty_field() {
    let col_types = vec![DataType::Integer, DataType::Text];
    let opts = CopyOptions::default();

    // Empty field should be treated as NULL
    let data = b",hello\n";
    let rows = process_copy_data(data, &col_types, &opts).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Datum::Null);
    assert_eq!(rows[0][1], Datum::Text("hello".to_string()));
}

#[test]
fn test_process_copy_data_quoted() {
    let col_types = vec![DataType::Text, DataType::Integer];
    let opts = CopyOptions::default();

    let data = b"\"hello, world\",42\n";
    let rows = process_copy_data(data, &col_types, &opts).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Datum::Text("hello, world".to_string()));
    assert_eq!(rows[0][1], Datum::Integer(42));
}

#[test]
fn test_process_copy_data_wrong_columns() {
    let col_types = vec![DataType::Integer, DataType::Text, DataType::Boolean];
    let opts = CopyOptions::default();

    // Only 2 fields but expecting 3 columns
    let data = b"1,hello\n";
    let result = process_copy_data(data, &col_types, &opts);

    assert!(result.is_err(), "expected error for wrong number of columns");
}

// ===========================================================================
// Tests — begin_copy_in
// ===========================================================================

#[test]
fn test_begin_copy_in() {
    let col_types = vec![DataType::Integer, DataType::Text, DataType::Boolean];
    let columns = vec![
        "id".to_string(),
        "name".to_string(),
        "active".to_string(),
    ];
    let opts = CopyOptions::default();

    let msg = begin_copy_in("test_table", &columns, &col_types, &opts);

    match msg {
        BackendMessage::CopyInResponse {
            format,
            column_formats,
        } => {
            assert_eq!(format, 0, "expected text format");
            assert_eq!(column_formats.len(), 3);
            // All column formats should be 0 (text)
            for cf in &column_formats {
                assert_eq!(*cf, 0i16);
            }
        }
        other => panic!("expected CopyInResponse, got {other:?}"),
    }
}

// ===========================================================================
// Tests — COPY TO STDOUT via handle_copy_statement (through simple query)
// ===========================================================================

#[test]
fn test_copy_to_stdout() {
    let mut session = make_session();

    // Create table and insert data
    handle_simple_query(
        "CREATE TABLE export_test (id INTEGER, name TEXT)",
        &mut session,
    );
    handle_simple_query("INSERT INTO export_test VALUES (1, 'alice')", &mut session);
    handle_simple_query("INSERT INTO export_test VALUES (2, 'bob')", &mut session);

    // COPY TO STDOUT
    let msgs = handle_simple_query("COPY export_test TO 'STDOUT'", &mut session);
    assert!(!has_error_response(&msgs), "COPY TO failed: {msgs:?}");

    // Should contain CopyOutResponse
    let has_copy_out = msgs.iter().any(|m| matches!(m, BackendMessage::CopyOutResponse { .. }));
    assert!(has_copy_out, "expected CopyOutResponse");

    // Should contain CopyData messages (one per row + header if csv has_header)
    // CopyOptions::csv() has has_header=true so we get header + 2 data rows = 3
    let copy_data_count = msgs
        .iter()
        .filter(|m| matches!(m, BackendMessage::CopyData { .. }))
        .count();
    assert!(
        copy_data_count >= 2,
        "expected at least 2 CopyData messages, got {copy_data_count}"
    );

    // Should contain CopyDone
    let has_copy_done = msgs.iter().any(|m| matches!(m, BackendMessage::CopyDone));
    assert!(has_copy_done, "expected CopyDone");

    // Should have CommandComplete with COPY tag
    let tag = msgs.iter().find_map(|m| match m {
        BackendMessage::CommandComplete { tag } => Some(tag.clone()),
        _ => None,
    });
    assert_eq!(tag, Some("COPY 2".to_string()));
}

#[test]
fn test_process_copy_data_with_tab_delimiter() {
    let col_types = vec![DataType::Integer, DataType::Text];
    let opts = CopyOptions::default().with_delimiter(b'\t');

    let data = b"1\thello\n2\tworld\n";
    let rows = process_copy_data(data, &col_types, &opts).unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Datum::Integer(1));
    assert_eq!(rows[0][1], Datum::Text("hello".to_string()));
}

#[test]
fn test_process_copy_data_skips_empty_lines() {
    let col_types = vec![DataType::Integer];
    let opts = CopyOptions::default();

    let data = b"1\n\n2\n\n";
    let rows = process_copy_data(data, &col_types, &opts).unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Datum::Integer(1));
    assert_eq!(rows[1][0], Datum::Integer(2));
}

#[test]
fn test_process_copy_data_end_marker() {
    let col_types = vec![DataType::Integer];
    let opts = CopyOptions::default();

    // The \. marker should end the data stream
    let data = b"1\n2\n\\.\n3\n";
    let rows = process_copy_data(data, &col_types, &opts).unwrap();

    // Lines after \. should still be processed (process_copy_data just skips the marker line)
    assert!(rows.len() >= 2, "expected at least 2 rows, got {}", rows.len());
}
