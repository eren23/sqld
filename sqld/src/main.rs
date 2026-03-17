use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use sqld::config::Config;
use sqld::executor::CatalogProvider;
use sqld::planner::Catalog;
use sqld::planner::physical_plan::KeyRange;
use sqld::protocol::server::Server;
use sqld::types::{Datum, MvccHeader, Schema, Tuple};
use sqld::utils::error::Result;

// ---------------------------------------------------------------------------
// In-memory catalog provider for bootstrap
// ---------------------------------------------------------------------------

struct MemoryCatalogProvider {
    catalog: Arc<Mutex<Catalog>>,
    data: Mutex<HashMap<String, Vec<Tuple>>>,
}

impl MemoryCatalogProvider {
    fn new(catalog: Arc<Mutex<Catalog>>) -> Self {
        Self {
            catalog,
            data: Mutex::new(HashMap::new()),
        }
    }
}

impl CatalogProvider for MemoryCatalogProvider {
    fn table_schema(&self, table: &str) -> Result<Schema> {
        let catalog = self.catalog.lock().unwrap();
        catalog
            .get_schema(table)
            .cloned()
            .ok_or_else(|| {
                sqld::utils::error::SqlError::ExecutionError(format!(
                    "table \"{table}\" does not exist"
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
        // Fallback to full scan for in-memory provider
        self.scan_table(table)
    }

    fn insert_tuple(&self, table: &str, values: Vec<Datum>) -> Result<Tuple> {
        let tuple = Tuple::new(MvccHeader::new_insert(0, 0), values);
        let mut data = self.data.lock().unwrap();
        data.entry(table.to_string())
            .or_insert_with(Vec::new)
            .push(tuple.clone());
        Ok(tuple)
    }

    fn delete_tuple(&self, table: &str, tuple: &Tuple) -> Result<Tuple> {
        let mut data = self.data.lock().unwrap();
        if let Some(rows) = data.get_mut(table) {
            rows.retain(|t| t.values() != tuple.values());
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

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let args: Vec<String> = env::args().collect();

    // Parse config path from args or use default
    let config_path = if args.len() > 1 {
        PathBuf::from(&args[1])
    } else {
        PathBuf::from("sqld_config.toml")
    };

    let config = if config_path.exists() {
        match Config::from_file(&config_path) {
            Ok(c) => {
                eprintln!("Loaded config from {}", config_path.display());
                c
            }
            Err(e) => {
                eprintln!("Error loading config: {e}");
                std::process::exit(1);
            }
        }
    } else {
        eprintln!("Using default configuration");
        Config::default()
    };

    // Bootstrap catalog
    let catalog = Arc::new(Mutex::new(Catalog::new()));

    // Create storage provider
    let catalog_provider: Arc<dyn CatalogProvider> =
        Arc::new(MemoryCatalogProvider::new(catalog.clone()));

    // WAL recovery would happen here in a full implementation
    // For now, we start with a clean state
    eprintln!("WAL recovery: skipped (in-memory mode)");

    // Create data directory if needed
    let data_dir = &config.storage.data_dir;
    if !std::path::Path::new(data_dir).exists() {
        if let Err(e) = std::fs::create_dir_all(data_dir) {
            eprintln!("Warning: could not create data directory '{data_dir}': {e}");
        }
    }

    // Start TCP server
    let server = Server::new(config, catalog, catalog_provider);
    if let Err(e) = server.run() {
        eprintln!("Server error: {e}");
        std::process::exit(1);
    }
}
