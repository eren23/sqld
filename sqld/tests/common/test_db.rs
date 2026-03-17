use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sqld::executor::executor::{intermediate_tuple, CatalogProvider};
use sqld::planner::physical_plan::KeyRange;
use sqld::planner::Catalog;
use sqld::protocol::connection::Session;
use sqld::protocol::messages::BackendMessage;
use sqld::protocol::simple_query::handle_simple_query;
use sqld::types::{Column, DataType, Datum, MvccHeader, Schema, Tuple};
use sqld::utils::error::{Result, SqlError};

// ---------------------------------------------------------------------------
// In-memory catalog provider
// ---------------------------------------------------------------------------

/// An in-memory implementation of [`CatalogProvider`] that stores table data
/// as `HashMap<String, Vec<Tuple>>`. Suitable for integration tests that need
/// to exercise the full parse-plan-execute pipeline without touching disk.
pub struct MemoryCatalogProvider {
    tables: Mutex<HashMap<String, (Schema, Vec<Tuple>)>>,
    next_xmin: Mutex<u64>,
}

impl MemoryCatalogProvider {
    pub fn new() -> Self {
        Self {
            tables: Mutex::new(HashMap::new()),
            next_xmin: Mutex::new(1),
        }
    }

    /// Register a table with the given schema and no rows.
    pub fn register_table(&self, name: &str, schema: Schema) {
        self.tables
            .lock()
            .unwrap()
            .insert(name.to_string(), (schema, Vec::new()));
    }

    /// Register a table with pre-populated rows.
    pub fn register_table_with_data(&self, name: &str, schema: Schema, rows: Vec<Tuple>) {
        self.tables
            .lock()
            .unwrap()
            .insert(name.to_string(), (schema, rows));
    }

    fn alloc_xmin(&self) -> u64 {
        let mut xmin = self.next_xmin.lock().unwrap();
        let val = *xmin;
        *xmin += 1;
        val
    }
}

impl CatalogProvider for MemoryCatalogProvider {
    fn table_schema(&self, table: &str) -> Result<Schema> {
        let tables = self.tables.lock().unwrap();
        tables
            .get(table)
            .map(|(s, _)| s.clone())
            .ok_or_else(|| {
                SqlError::ExecutionError(format!("table not found: {table}")).into()
            })
    }

    fn scan_table(&self, table: &str) -> Result<Vec<Tuple>> {
        let tables = self.tables.lock().unwrap();
        tables
            .get(table)
            .map(|(_, rows)| {
                rows.iter()
                    .filter(|t| !t.header.is_deleted())
                    .cloned()
                    .collect()
            })
            .ok_or_else(|| {
                SqlError::ExecutionError(format!("table not found: {table}")).into()
            })
    }

    fn scan_index(
        &self,
        table: &str,
        _index: &str,
        _ranges: &[KeyRange],
    ) -> Result<Vec<Tuple>> {
        // For in-memory tests, fall back to a full table scan.
        self.scan_table(table)
    }

    fn insert_tuple(&self, table: &str, values: Vec<Datum>) -> Result<Tuple> {
        let xmin = self.alloc_xmin();
        let tuple = Tuple::new(MvccHeader::new_insert(xmin, 0), values);
        let mut tables = self.tables.lock().unwrap();
        if let Some((_, rows)) = tables.get_mut(table) {
            rows.push(tuple.clone());
            Ok(tuple)
        } else {
            Err(SqlError::ExecutionError(format!("table not found: {table}")).into())
        }
    }

    fn delete_tuple(&self, table: &str, tuple: &Tuple) -> Result<Tuple> {
        let mut tables = self.tables.lock().unwrap();
        if let Some((_, rows)) = tables.get_mut(table) {
            // Find and mark the matching tuple as deleted by removing it.
            if let Some(pos) = rows.iter().position(|r| r.values() == tuple.values()) {
                let removed = rows.remove(pos);
                return Ok(removed);
            }
            // If no exact match found, still return the tuple (best-effort).
            Ok(tuple.clone())
        } else {
            Err(SqlError::ExecutionError(format!("table not found: {table}")).into())
        }
    }

    fn update_tuple(
        &self,
        table: &str,
        old_tuple: &Tuple,
        new_values: Vec<Datum>,
    ) -> Result<Tuple> {
        // Delete the old version, then insert the new one.
        self.delete_tuple(table, old_tuple)?;
        self.insert_tuple(table, new_values)
    }
}

// ---------------------------------------------------------------------------
// TestDb — convenient wrapper for integration tests
// ---------------------------------------------------------------------------

/// A self-contained test database that wires together an in-memory catalog
/// provider, a planner [`Catalog`], and a [`Session`]. Tests create one of
/// these and call [`TestDb::execute`] (or the convenience wrappers) to run
/// SQL through the full pipeline.
pub struct TestDb {
    pub session: Session,
    pub catalog_provider: Arc<MemoryCatalogProvider>,
    pub catalog: Arc<Mutex<Catalog>>,
}

impl TestDb {
    /// Create a fresh, empty test database.
    pub fn new() -> Self {
        let catalog_provider = Arc::new(MemoryCatalogProvider::new());
        let catalog = Arc::new(Mutex::new(Catalog::new()));

        let session = Session::new(
            catalog.clone(),
            catalog_provider.clone() as Arc<dyn CatalogProvider>,
            1, // process_id
        );

        Self {
            session,
            catalog_provider,
            catalog,
        }
    }

    /// Create a test database pre-populated with the given tables.
    ///
    /// Each entry is `(table_name, schema)`. The table is registered in both
    /// the planner catalog and the in-memory catalog provider.
    pub fn with_tables(tables: &[(&str, Schema)]) -> Self {
        let db = Self::new();
        for (name, schema) in tables {
            db.catalog.lock().unwrap().add_table(name.to_string(), schema.clone());
            db.catalog_provider.register_table(name, schema.clone());
        }
        db
    }

    /// Return a mutable reference to the underlying [`Session`].
    pub fn session(&mut self) -> &mut Session {
        &mut self.session
    }

    /// Run a SQL statement through `handle_simple_query` and return the raw
    /// backend messages.
    pub fn execute(&mut self, sql: &str) -> Vec<BackendMessage> {
        handle_simple_query(sql, &mut self.session)
    }

    /// Run a SQL statement and panic if any of the returned messages is an
    /// `ErrorResponse`.
    pub fn execute_expect_ok(&mut self, sql: &str) -> Vec<BackendMessage> {
        let messages = self.execute(sql);
        for msg in &messages {
            if let BackendMessage::ErrorResponse(err) = msg {
                panic!(
                    "expected OK but got ErrorResponse: [{}] {}",
                    err.code, err.message
                );
            }
        }
        messages
    }

    /// Run a SQL query and return only the data rows, with each column
    /// converted to a `String`. NULL values become the string `"NULL"`.
    ///
    /// Panics if the query produces an error.
    pub fn execute_query(&mut self, sql: &str) -> Vec<Vec<String>> {
        let messages = self.execute_expect_ok(sql);
        let mut rows = Vec::new();
        for msg in &messages {
            if let BackendMessage::DataRow { values } = msg {
                let row: Vec<String> = values
                    .iter()
                    .map(|v| match v {
                        Some(bytes) => String::from_utf8_lossy(bytes).to_string(),
                        None => "NULL".to_string(),
                    })
                    .collect();
                rows.push(row);
            }
        }
        rows
    }

    /// Create a table by executing `CREATE TABLE` SQL and also registering it
    /// in the memory catalog provider.
    ///
    /// `columns` is a slice of `(name, DataType, nullable)` triples.
    pub fn create_table(&mut self, name: &str, columns: &[(&str, DataType, bool)]) {
        let col_defs: Vec<String> = columns
            .iter()
            .map(|(cname, dtype, nullable)| {
                let null_clause = if *nullable { "" } else { " NOT NULL" };
                format!("{cname} {dtype}{null_clause}")
            })
            .collect();

        let sql = format!("CREATE TABLE {name} ({})", col_defs.join(", "));
        self.execute_expect_ok(&sql);

        let schema = Schema::new(
            columns
                .iter()
                .map(|(cname, dtype, nullable)| Column::new(*cname, *dtype, *nullable))
                .collect(),
        );
        self.catalog_provider.register_table(name, schema);
    }

    /// Insert a row into a table using an `INSERT` statement built from the
    /// provided SQL literal values.
    ///
    /// `values` should be SQL literal strings, e.g. `&["1", "'Alice'", "NULL"]`.
    pub fn insert_row(&mut self, table: &str, values: &[&str]) {
        let vals = values.join(", ");
        let sql = format!("INSERT INTO {table} VALUES ({vals})");
        self.execute_expect_ok(&sql);
    }

    /// Execute a query and return the result as `Vec<Vec<Datum>>`.
    ///
    /// Only SELECT queries make sense here. Each inner Vec corresponds to one
    /// row, with `Datum::Text` for non-NULL values and `Datum::Null` for NULLs.
    /// Since the wire protocol transfers values in text format, all non-NULL
    /// values are returned as `Datum::Text`.
    ///
    /// Panics if the query produces an error.
    pub fn query(&mut self, sql: &str) -> Vec<Vec<Datum>> {
        let messages = self.execute_expect_ok(sql);
        let mut rows = Vec::new();
        for msg in &messages {
            if let BackendMessage::DataRow { values } = msg {
                let row: Vec<Datum> = values
                    .iter()
                    .map(|v| match v {
                        Some(bytes) => {
                            Datum::Text(String::from_utf8_lossy(bytes).to_string())
                        }
                        None => Datum::Null,
                    })
                    .collect();
                rows.push(row);
            }
        }
        rows
    }

    /// Create a standard set of test tables: `users`, `orders`, `products`.
    ///
    /// Schemas:
    /// - `users(id INTEGER, name VARCHAR(255), email VARCHAR(255))`
    /// - `orders(id INTEGER, user_id INTEGER, amount INTEGER, status VARCHAR(50))`
    /// - `products(id INTEGER, name VARCHAR(255), price INTEGER, stock INTEGER)`
    pub fn create_test_tables(&mut self) {
        self.execute_expect_ok(
            "CREATE TABLE users (
                id INTEGER NOT NULL,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255)
            )",
        );

        // Also register the table in the memory catalog provider so
        // scan_table / insert_tuple work.
        self.catalog_provider.register_table(
            "users",
            Schema::new(vec![
                Column::new("id", DataType::Integer, false),
                Column::new("name", DataType::Varchar(255), false),
                Column::new("email", DataType::Varchar(255), true),
            ]),
        );

        self.execute_expect_ok(
            "CREATE TABLE orders (
                id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                status VARCHAR(50)
            )",
        );

        self.catalog_provider.register_table(
            "orders",
            Schema::new(vec![
                Column::new("id", DataType::Integer, false),
                Column::new("user_id", DataType::Integer, false),
                Column::new("amount", DataType::Integer, false),
                Column::new("status", DataType::Varchar(50), true),
            ]),
        );

        self.execute_expect_ok(
            "CREATE TABLE products (
                id INTEGER NOT NULL,
                name VARCHAR(255) NOT NULL,
                price INTEGER NOT NULL,
                stock INTEGER NOT NULL
            )",
        );

        self.catalog_provider.register_table(
            "products",
            Schema::new(vec![
                Column::new("id", DataType::Integer, false),
                Column::new("name", DataType::Varchar(255), false),
                Column::new("price", DataType::Integer, false),
                Column::new("stock", DataType::Integer, false),
            ]),
        );
    }

    /// Insert sample rows into the standard test tables created by
    /// [`create_test_tables`].
    ///
    /// Users: Alice (1), Bob (2), Charlie (3)
    /// Orders: 3 orders across users 1 and 2
    /// Products: Widget, Gadget, Doohickey
    pub fn insert_test_data(&mut self) {
        // Users
        self.execute_expect_ok(
            "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')",
        );
        self.execute_expect_ok(
            "INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com')",
        );
        self.execute_expect_ok(
            "INSERT INTO users (id, name, email) VALUES (3, 'Charlie', 'charlie@example.com')",
        );

        // Orders
        self.execute_expect_ok(
            "INSERT INTO orders (id, user_id, amount, status) VALUES (101, 1, 500, 'completed')",
        );
        self.execute_expect_ok(
            "INSERT INTO orders (id, user_id, amount, status) VALUES (102, 1, 300, 'pending')",
        );
        self.execute_expect_ok(
            "INSERT INTO orders (id, user_id, amount, status) VALUES (103, 2, 750, 'completed')",
        );

        // Products
        self.execute_expect_ok(
            "INSERT INTO products (id, name, price, stock) VALUES (1, 'Widget', 1000, 50)",
        );
        self.execute_expect_ok(
            "INSERT INTO products (id, name, price, stock) VALUES (2, 'Gadget', 2500, 30)",
        );
        self.execute_expect_ok(
            "INSERT INTO products (id, name, price, stock) VALUES (3, 'Doohickey', 500, 100)",
        );
    }
}
