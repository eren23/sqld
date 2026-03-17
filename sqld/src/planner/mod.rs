pub mod cardinality;
pub mod cost_model;
pub mod explain;
pub mod logical_plan;
pub mod optimizer;
pub mod physical_plan;
pub mod physical_planner;
pub mod plan_builder;
pub mod rules;

use std::collections::HashMap;

use crate::sql::ast::IndexMethod;
use crate::types::Schema;

// ---------------------------------------------------------------------------
// Catalog — metadata the planner needs about tables and indexes
// ---------------------------------------------------------------------------

/// Metadata for a single index.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub method: IndexMethod,
}

/// Per-column statistics collected by ANALYZE.
#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub distinct_count: f64,
    pub null_fraction: f64,
    pub min_value: Option<f64>,
    pub max_value: Option<f64>,
    pub avg_width: f64,
}

impl Default for ColumnStats {
    fn default() -> Self {
        Self {
            distinct_count: 100.0,
            null_fraction: 0.0,
            min_value: None,
            max_value: None,
            avg_width: 8.0,
        }
    }
}

/// Per-table statistics.
#[derive(Debug, Clone)]
pub struct TableStats {
    pub row_count: f64,
    pub page_count: f64,
    pub column_stats: HashMap<String, ColumnStats>,
}

impl Default for TableStats {
    fn default() -> Self {
        Self {
            row_count: 1000.0,
            page_count: 10.0,
            column_stats: HashMap::new(),
        }
    }
}

/// The catalog provides schema and statistics information to the planner.
#[derive(Debug, Clone, Default)]
pub struct Catalog {
    pub tables: HashMap<String, Schema>,
    pub indexes: Vec<IndexInfo>,
    pub stats: HashMap<String, TableStats>,
}

impl Catalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_table(&mut self, name: impl Into<String>, schema: Schema) {
        self.tables.insert(name.into(), schema);
    }

    pub fn add_index(&mut self, info: IndexInfo) {
        self.indexes.push(info);
    }

    pub fn set_stats(&mut self, table: impl Into<String>, stats: TableStats) {
        self.stats.insert(table.into(), stats);
    }

    pub fn get_schema(&self, table: &str) -> Option<&Schema> {
        self.tables.get(table)
    }

    pub fn get_indexes(&self, table: &str) -> Vec<&IndexInfo> {
        self.indexes.iter().filter(|i| i.table == table).collect()
    }

    pub fn get_stats(&self, table: &str) -> TableStats {
        self.stats.get(table).cloned().unwrap_or_default()
    }

    pub fn get_column_stats(&self, table: &str, column: &str) -> ColumnStats {
        self.stats
            .get(table)
            .and_then(|ts| ts.column_stats.get(column).cloned())
            .unwrap_or_default()
    }
}
