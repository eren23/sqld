use std::collections::HashMap;

use super::data_type::DataType;

/// A single column definition within a [`Schema`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
        }
    }
}

/// An ordered set of column definitions that describes the shape of a
/// relation (table, intermediate result, etc.).
#[derive(Debug, Clone)]
pub struct Schema {
    columns: Vec<Column>,
    /// name → ordinal index (case-sensitive)
    name_index: HashMap<String, usize>,
}

impl Schema {
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    pub fn new(columns: Vec<Column>) -> Self {
        let name_index = columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
        Self {
            columns,
            name_index,
        }
    }

    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            name_index: HashMap::new(),
        }
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Look up a column by name.  Returns `(ordinal, &Column)`.
    pub fn column_by_name(&self, name: &str) -> Option<(usize, &Column)> {
        self.name_index
            .get(name)
            .map(|&idx| (idx, &self.columns[idx]))
    }

    /// Look up a column by zero-based ordinal position.
    pub fn column_by_ordinal(&self, ordinal: usize) -> Option<&Column> {
        self.columns.get(ordinal)
    }

    /// Returns true if the schema contains a column with the given name.
    pub fn has_column(&self, name: &str) -> bool {
        self.name_index.contains_key(name)
    }

    // -----------------------------------------------------------------------
    // Mutation
    // -----------------------------------------------------------------------

    /// Add a column to the end of the schema.
    pub fn add_column(&mut self, col: Column) {
        let idx = self.columns.len();
        self.name_index.insert(col.name.clone(), idx);
        self.columns.push(col);
    }

    // -----------------------------------------------------------------------
    // Combinators
    // -----------------------------------------------------------------------

    /// Create a new schema by concatenating `self` and `other`.
    ///
    /// Used for join results.  Duplicate column names from `other` are
    /// prefixed with `_right_` to avoid collisions.
    pub fn merge(&self, other: &Schema) -> Schema {
        let mut columns = self.columns.clone();
        for col in &other.columns {
            let name = if self.has_column(&col.name) {
                format!("_right_{}", col.name)
            } else {
                col.name.clone()
            };
            columns.push(Column {
                name,
                data_type: col.data_type,
                nullable: col.nullable,
            });
        }
        Schema::new(columns)
    }

    /// Project a subset of columns by ordinal positions.
    pub fn project(&self, ordinals: &[usize]) -> Schema {
        let columns = ordinals
            .iter()
            .filter_map(|&i| self.columns.get(i).cloned())
            .collect();
        Schema::new(columns)
    }
}

impl PartialEq for Schema {
    fn eq(&self, other: &Self) -> bool {
        self.columns == other.columns
    }
}

impl Eq for Schema {}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", DataType::Integer, false),
            Column::new("name", DataType::Varchar(255), false),
            Column::new("score", DataType::Float, true),
        ])
    }

    #[test]
    fn column_count() {
        let s = sample_schema();
        assert_eq!(s.column_count(), 3);
    }

    #[test]
    fn lookup_by_name() {
        let s = sample_schema();
        let (idx, col) = s.column_by_name("name").unwrap();
        assert_eq!(idx, 1);
        assert_eq!(col.data_type, DataType::Varchar(255));
    }

    #[test]
    fn lookup_by_ordinal() {
        let s = sample_schema();
        let col = s.column_by_ordinal(2).unwrap();
        assert_eq!(col.name, "score");
        assert!(col.nullable);
    }

    #[test]
    fn lookup_missing() {
        let s = sample_schema();
        assert!(s.column_by_name("nonexistent").is_none());
        assert!(s.column_by_ordinal(99).is_none());
    }

    #[test]
    fn merge_no_conflicts() {
        let left = Schema::new(vec![Column::new("a", DataType::Integer, false)]);
        let right = Schema::new(vec![Column::new("b", DataType::BigInt, true)]);
        let merged = left.merge(&right);
        assert_eq!(merged.column_count(), 2);
        assert!(merged.has_column("a"));
        assert!(merged.has_column("b"));
    }

    #[test]
    fn merge_with_conflict() {
        let left = Schema::new(vec![Column::new("id", DataType::Integer, false)]);
        let right = Schema::new(vec![Column::new("id", DataType::Integer, false)]);
        let merged = left.merge(&right);
        assert_eq!(merged.column_count(), 2);
        assert!(merged.has_column("id"));
        assert!(merged.has_column("_right_id"));
    }

    #[test]
    fn project() {
        let s = sample_schema();
        let projected = s.project(&[0, 2]);
        assert_eq!(projected.column_count(), 2);
        assert_eq!(projected.column_by_ordinal(0).unwrap().name, "id");
        assert_eq!(projected.column_by_ordinal(1).unwrap().name, "score");
    }

    #[test]
    fn add_column() {
        let mut s = Schema::empty();
        s.add_column(Column::new("x", DataType::Boolean, false));
        assert_eq!(s.column_count(), 1);
        assert!(s.has_column("x"));
    }

    #[test]
    fn equality() {
        let a = sample_schema();
        let b = sample_schema();
        assert_eq!(a, b);
    }
}
