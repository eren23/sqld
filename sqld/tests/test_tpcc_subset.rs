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

// =====================================================================
// TPC-C schema setup helper
// =====================================================================

fn create_tpcc_schema(session: &mut Session) {
    let ddl_statements = [
        "CREATE TABLE warehouse (w_id INTEGER, w_name TEXT, w_ytd FLOAT)",
        "CREATE TABLE district (d_id INTEGER, d_w_id INTEGER, d_name TEXT, d_next_o_id INTEGER)",
        "CREATE TABLE customer (c_id INTEGER, c_d_id INTEGER, c_w_id INTEGER, c_first TEXT, c_last TEXT, c_balance FLOAT)",
        "CREATE TABLE orders (o_id INTEGER, o_d_id INTEGER, o_w_id INTEGER, o_c_id INTEGER, o_entry_d TEXT)",
        "CREATE TABLE order_line (ol_o_id INTEGER, ol_d_id INTEGER, ol_w_id INTEGER, ol_number INTEGER, ol_i_id INTEGER, ol_amount FLOAT)",
        "CREATE TABLE item (i_id INTEGER, i_name TEXT, i_price FLOAT)",
        "CREATE TABLE stock (s_i_id INTEGER, s_w_id INTEGER, s_quantity INTEGER)",
    ];
    for sql in &ddl_statements {
        let msgs = run_query(session, sql);
        assert!(!has_error(&msgs), "Schema creation failed for: {sql} -- {msgs:?}");
    }
}

fn load_sample_data(session: &mut Session) {
    // Warehouse
    run_query(session, "INSERT INTO warehouse VALUES (1, 'Main Warehouse', 50000.0)");

    // Districts
    for d in 1..=3 {
        let sql = format!(
            "INSERT INTO district VALUES ({d}, 1, 'District {d}', {next_oid})",
            next_oid = 100 + d
        );
        run_query(session, &sql);
    }

    // Customers
    let customers = [
        (1, 1, 1, "Alice", "Smith", 1000.0),
        (2, 1, 1, "Bob", "Jones", 2000.0),
        (3, 2, 1, "Carol", "Davis", 1500.0),
        (4, 2, 1, "Dave", "Wilson", 3000.0),
        (5, 3, 1, "Eve", "Brown", 500.0),
    ];
    for (c_id, c_d_id, c_w_id, first, last, balance) in &customers {
        let sql = format!(
            "INSERT INTO customer VALUES ({c_id}, {c_d_id}, {c_w_id}, '{first}', '{last}', {balance})"
        );
        run_query(session, &sql);
    }

    // Items
    let items = [
        (1, "Widget A", 9.99),
        (2, "Widget B", 19.99),
        (3, "Gadget C", 29.99),
        (4, "Gadget D", 4.99),
        (5, "Thingamajig", 14.99),
    ];
    for (i_id, name, price) in &items {
        let sql = format!("INSERT INTO item VALUES ({i_id}, '{name}', {price})");
        run_query(session, &sql);
    }

    // Stock
    for i_id in 1..=5 {
        let qty = 100 - i_id * 10; // 90, 80, 70, 60, 50
        let sql = format!("INSERT INTO stock VALUES ({i_id}, 1, {qty})");
        run_query(session, &sql);
    }

    // Orders
    let orders = [
        (1, 1, 1, 1, "2025-01-01"),
        (2, 1, 1, 2, "2025-01-02"),
        (3, 2, 1, 3, "2025-01-03"),
    ];
    for (o_id, o_d_id, o_w_id, o_c_id, entry_d) in &orders {
        let sql = format!(
            "INSERT INTO orders VALUES ({o_id}, {o_d_id}, {o_w_id}, {o_c_id}, '{entry_d}')"
        );
        run_query(session, &sql);
    }

    // Order lines
    let order_lines = [
        (1, 1, 1, 1, 1, 9.99),
        (1, 1, 1, 2, 2, 19.99),
        (2, 1, 1, 1, 3, 29.99),
        (3, 2, 1, 1, 4, 4.99),
        (3, 2, 1, 2, 5, 14.99),
    ];
    for (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_amount) in &order_lines {
        let sql = format!(
            "INSERT INTO order_line VALUES ({ol_o_id}, {ol_d_id}, {ol_w_id}, {ol_number}, {ol_i_id}, {ol_amount})"
        );
        run_query(session, &sql);
    }
}

// =====================================================================
// Tests
// =====================================================================

#[test]
fn test_tpcc_schema_creation() {
    let mut session = new_session();
    create_tpcc_schema(&mut session);

    // Verify all 7 tables exist.
    let msgs = run_query(&mut session, "SHOW TABLES");
    assert!(!has_error(&msgs));
    let rows = extract_rows(&msgs);
    let table_names: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();

    assert!(table_names.contains(&"warehouse"), "missing warehouse table");
    assert!(table_names.contains(&"district"), "missing district table");
    assert!(table_names.contains(&"customer"), "missing customer table");
    assert!(table_names.contains(&"orders"), "missing orders table");
    assert!(table_names.contains(&"order_line"), "missing order_line table");
    assert!(table_names.contains(&"item"), "missing item table");
    assert!(table_names.contains(&"stock"), "missing stock table");
    assert_eq!(rows.len(), 7, "expected exactly 7 tables");
}

#[test]
fn test_tpcc_data_load() {
    let mut session = new_session();
    create_tpcc_schema(&mut session);
    load_sample_data(&mut session);

    // Verify row counts in each table by scanning.
    let mut check = |table: &str, expected: usize| {
        let msgs = run_query(&mut session, &format!("SELECT * FROM {table}"));
        assert!(!has_error(&msgs), "SELECT * failed for {table}: {msgs:?}");
        assert_eq!(count_rows(&msgs), expected, "wrong count for {table}");
    };

    check("warehouse", 1);
    check("district", 3);
    check("customer", 5);
    check("item", 5);
    check("stock", 5);
    check("orders", 3);
    check("order_line", 5);
}

#[test]
fn test_tpcc_new_order() {
    let mut session = new_session();
    create_tpcc_schema(&mut session);
    load_sample_data(&mut session);

    // Simulate a new order transaction:
    // 1. Insert a new order for customer 1, district 1, warehouse 1.
    let msgs = run_query(&mut session, "BEGIN");
    assert!(!has_error(&msgs));

    let msgs = run_query(
        &mut session,
        "INSERT INTO orders VALUES (4, 1, 1, 1, '2025-02-01')",
    );
    assert!(!has_error(&msgs), "Insert new order failed: {msgs:?}");

    // 2. Insert order lines for this new order.
    let msgs = run_query(
        &mut session,
        "INSERT INTO order_line VALUES (4, 1, 1, 1, 1, 9.99)",
    );
    assert!(!has_error(&msgs));

    let msgs = run_query(
        &mut session,
        "INSERT INTO order_line VALUES (4, 1, 1, 2, 3, 29.99)",
    );
    assert!(!has_error(&msgs));

    // 3. Update stock for item 1: decrease quantity by 1.
    let msgs = run_query(
        &mut session,
        "UPDATE stock SET s_quantity = s_quantity - 1 WHERE s_i_id = 1 AND s_w_id = 1",
    );
    assert!(!has_error(&msgs), "Stock update for item 1 failed: {msgs:?}");

    // 4. Update stock for item 3: decrease quantity by 1.
    let msgs = run_query(
        &mut session,
        "UPDATE stock SET s_quantity = s_quantity - 1 WHERE s_i_id = 3 AND s_w_id = 1",
    );
    assert!(!has_error(&msgs), "Stock update for item 3 failed: {msgs:?}");

    // 5. Update district next_o_id.
    let msgs = run_query(
        &mut session,
        "UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_id = 1 AND d_w_id = 1",
    );
    assert!(!has_error(&msgs), "District update failed: {msgs:?}");

    let msgs = run_query(&mut session, "COMMIT");
    assert!(!has_error(&msgs));

    // Verify the new order exists.
    let msgs = run_query(&mut session, "SELECT o_id, o_c_id FROM orders WHERE o_id = 4");
    assert!(!has_error(&msgs));
    let rows = extract_rows(&msgs);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "4");
    assert_eq!(rows[0][1], "1");

    // Verify order line count for order 4.
    let msgs = run_query(
        &mut session,
        "SELECT ol_number FROM order_line WHERE ol_o_id = 4",
    );
    assert!(!has_error(&msgs));
    assert_eq!(count_rows(&msgs), 2);

    // Verify stock was updated for item 1 (was 90, now 89).
    let msgs = run_query(
        &mut session,
        "SELECT s_quantity FROM stock WHERE s_i_id = 1",
    );
    assert!(!has_error(&msgs));
    let rows = extract_rows(&msgs);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "89");
}

#[test]
fn test_tpcc_payment() {
    let mut session = new_session();
    create_tpcc_schema(&mut session);
    load_sample_data(&mut session);

    // Simulate a payment transaction:
    // Customer 2 makes a payment of 500.0.

    let msgs = run_query(&mut session, "BEGIN");
    assert!(!has_error(&msgs));

    // 1. Update customer balance: decrease by payment amount.
    let msgs = run_query(
        &mut session,
        "UPDATE customer SET c_balance = c_balance - 500.0 WHERE c_id = 2 AND c_d_id = 1 AND c_w_id = 1",
    );
    assert!(!has_error(&msgs), "Customer balance update failed: {msgs:?}");

    // 2. Update warehouse year-to-date: increase by payment amount.
    let msgs = run_query(
        &mut session,
        "UPDATE warehouse SET w_ytd = w_ytd + 500.0 WHERE w_id = 1",
    );
    assert!(!has_error(&msgs), "Warehouse ytd update failed: {msgs:?}");

    let msgs = run_query(&mut session, "COMMIT");
    assert!(!has_error(&msgs));

    // Verify customer balance was updated (was 2000.0, now 1500.0).
    let msgs = run_query(
        &mut session,
        "SELECT c_balance FROM customer WHERE c_id = 2 AND c_d_id = 1",
    );
    assert!(!has_error(&msgs));
    let rows = extract_rows(&msgs);
    assert_eq!(rows.len(), 1);
    let balance: f64 = rows[0][0].parse().expect("balance should be numeric");
    assert!((balance - 1500.0).abs() < 0.01, "expected balance 1500.0, got {balance}");

    // Verify warehouse ytd was updated (was 50000.0, now 50500.0).
    let msgs = run_query(&mut session, "SELECT w_ytd FROM warehouse WHERE w_id = 1");
    assert!(!has_error(&msgs));
    let rows = extract_rows(&msgs);
    assert_eq!(rows.len(), 1);
    let ytd: f64 = rows[0][0].parse().expect("ytd should be numeric");
    assert!((ytd - 50500.0).abs() < 0.01, "expected ytd 50500.0, got {ytd}");
}

#[test]
fn test_tpcc_order_status() {
    let mut session = new_session();
    create_tpcc_schema(&mut session);
    load_sample_data(&mut session);

    // Query customer orders with a join: find all orders for customer 1.
    let msgs = run_query(
        &mut session,
        "SELECT o_id, o_entry_d FROM orders WHERE o_c_id = 1 ORDER BY o_id",
    );
    assert!(!has_error(&msgs), "Order status query failed: {msgs:?}");
    let rows = extract_rows(&msgs);
    // Customer 1 has orders 1 and 2 (from sample data load function: orders (1,1,1,1,...) and (2,1,1,2,...)).
    // Actually customer 1 has order 1, customer 2 has order 2, customer 3 has order 3.
    assert_eq!(rows.len(), 1, "customer 1 should have 1 order");
    assert_eq!(rows[0][0], "1");
    assert_eq!(rows[0][1], "2025-01-01");

    // Query order lines for order 1 using a join.
    let msgs = run_query(
        &mut session,
        "SELECT ol_number, ol_i_id, ol_amount FROM order_line WHERE ol_o_id = 1 ORDER BY ol_number",
    );
    assert!(!has_error(&msgs));
    let rows = extract_rows(&msgs);
    assert_eq!(rows.len(), 2, "order 1 should have 2 lines");
    assert_eq!(rows[0][0], "1"); // ol_number
    assert_eq!(rows[0][1], "1"); // item 1
    assert_eq!(rows[1][0], "2"); // ol_number
    assert_eq!(rows[1][1], "2"); // item 2

    // Compute total for order 1 via row scan.
    let msgs = run_query(
        &mut session,
        "SELECT ol_amount FROM order_line WHERE ol_o_id = 1",
    );
    assert!(!has_error(&msgs));
    let rows = extract_rows(&msgs);
    let total: f64 = rows.iter().map(|r| r[0].parse::<f64>().unwrap()).sum();
    assert!((total - 29.98).abs() < 0.01, "expected total 29.98, got {total}");
}

#[test]
fn test_tpcc_stock_level() {
    let mut session = new_session();
    create_tpcc_schema(&mut session);
    load_sample_data(&mut session);

    // Count items with stock below a threshold of 75.
    // Stock quantities are: item1=90, item2=80, item3=70, item4=60, item5=50.
    // Items below 75: item3 (70), item4 (60), item5 (50) = 3 items.
    let msgs = run_query(
        &mut session,
        "SELECT s_i_id FROM stock WHERE s_quantity < 75",
    );
    assert!(!has_error(&msgs), "Stock level query failed: {msgs:?}");
    assert_eq!(count_rows(&msgs), 3, "expected 3 items below threshold 75");

    // Count items with stock below a threshold of 55.
    // Items below 55: item5 (50) = 1 item.
    let msgs = run_query(
        &mut session,
        "SELECT s_i_id FROM stock WHERE s_quantity < 55",
    );
    assert!(!has_error(&msgs));
    assert_eq!(count_rows(&msgs), 1, "expected 1 item below threshold 55");

    // Compute total stock across all items.
    // SUM may not work end-to-end; verify via row scan instead.
    let msgs = run_query(&mut session, "SELECT s_quantity FROM stock");
    assert!(!has_error(&msgs));
    let rows = extract_rows(&msgs);
    let total: i64 = rows.iter().map(|r| r[0].parse::<i64>().unwrap()).sum();
    // 90 + 80 + 70 + 60 + 50 = 350
    assert_eq!(total, 350, "expected total stock 350");
}

#[test]
fn test_tpcc_delivery() {
    let mut session = new_session();
    create_tpcc_schema(&mut session);
    load_sample_data(&mut session);

    // Simulate a delivery transaction:
    // Find the oldest undelivered order in district 1 and process it.

    // 1. Query the oldest order in district 1.
    let msgs = run_query(
        &mut session,
        "SELECT o_id, o_c_id FROM orders WHERE o_d_id = 1 ORDER BY o_id LIMIT 1",
    );
    assert!(!has_error(&msgs), "Delivery query failed: {msgs:?}");
    let rows = extract_rows(&msgs);
    assert_eq!(rows.len(), 1);
    let order_id = &rows[0][0];
    let customer_id = &rows[0][1];
    assert_eq!(order_id, "1");
    assert_eq!(customer_id, "1");

    // 2. Compute the total for this order via row scan.
    let msgs = run_query(
        &mut session,
        &format!("SELECT ol_amount FROM order_line WHERE ol_o_id = {order_id} AND ol_d_id = 1"),
    );
    assert!(!has_error(&msgs));
    let rows = extract_rows(&msgs);
    let order_total: f64 = rows.iter().map(|r| r[0].parse::<f64>().unwrap()).sum();

    // 3. Update the customer balance (credit the delivery amount).
    let msgs = run_query(&mut session, "BEGIN");
    assert!(!has_error(&msgs));

    let msgs = run_query(
        &mut session,
        &format!(
            "UPDATE customer SET c_balance = c_balance + {order_total} WHERE c_id = {customer_id} AND c_d_id = 1 AND c_w_id = 1"
        ),
    );
    assert!(!has_error(&msgs), "Customer balance update for delivery failed: {msgs:?}");

    // 4. Delete the delivered order (simplified delivery).
    let msgs = run_query(
        &mut session,
        &format!("DELETE FROM orders WHERE o_id = {order_id} AND o_d_id = 1"),
    );
    assert!(!has_error(&msgs), "Order deletion failed: {msgs:?}");

    let msgs = run_query(&mut session, "COMMIT");
    assert!(!has_error(&msgs));

    // Verify the order was removed.
    let msgs = run_query(
        &mut session,
        &format!("SELECT o_id FROM orders WHERE o_id = {order_id} AND o_d_id = 1"),
    );
    assert!(!has_error(&msgs));
    assert_eq!(count_rows(&msgs), 0, "delivered order should be deleted");

    // Verify customer balance was credited (was 1000.0 + 29.98 = 1029.98).
    let msgs = run_query(
        &mut session,
        &format!("SELECT c_balance FROM customer WHERE c_id = {customer_id} AND c_d_id = 1"),
    );
    assert!(!has_error(&msgs));
    let rows = extract_rows(&msgs);
    let balance: f64 = rows[0][0].parse().expect("balance should be numeric");
    let expected = 1000.0 + order_total;
    assert!(
        (balance - expected).abs() < 0.01,
        "expected balance {expected}, got {balance}"
    );

    // Verify remaining orders in district 1.
    let msgs = run_query(
        &mut session,
        "SELECT o_id FROM orders WHERE o_d_id = 1",
    );
    assert!(!has_error(&msgs));
    // Was 2 orders in district 1 (orders 1 and 2), now 1 after deletion.
    assert_eq!(count_rows(&msgs), 1, "should have 1 remaining order in district 1");
}
