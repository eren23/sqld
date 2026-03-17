use sqld::types::{Column, DataType, Schema};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn users_schema() -> Schema {
    Schema::new(vec![
        Column::new("id", DataType::Integer, false),
        Column::new("name", DataType::Varchar(255), false),
        Column::new("email", DataType::Text, false),
        Column::new("score", DataType::Float, true),
    ])
}

fn orders_schema() -> Schema {
    Schema::new(vec![
        Column::new("order_id", DataType::BigInt, false),
        Column::new("user_id", DataType::Integer, false),
        Column::new("total", DataType::Decimal(10, 2), false),
        Column::new("created_at", DataType::Timestamp, false),
    ])
}

// ===========================================================================
// Construction & basic accessors
// ===========================================================================

#[test]
fn column_count() {
    assert_eq!(users_schema().column_count(), 4);
    assert_eq!(Schema::empty().column_count(), 0);
}

#[test]
fn columns_returns_all() {
    let s = users_schema();
    let cols = s.columns();
    assert_eq!(cols.len(), 4);
    assert_eq!(cols[0].name, "id");
    assert_eq!(cols[3].name, "score");
}

// ===========================================================================
// Lookup by name
// ===========================================================================

#[test]
fn lookup_by_name_existing() {
    let s = users_schema();
    let (idx, col) = s.column_by_name("name").unwrap();
    assert_eq!(idx, 1);
    assert_eq!(col.data_type, DataType::Varchar(255));
    assert!(!col.nullable);
}

#[test]
fn lookup_by_name_missing() {
    let s = users_schema();
    assert!(s.column_by_name("nonexistent").is_none());
}

#[test]
fn has_column() {
    let s = users_schema();
    assert!(s.has_column("id"));
    assert!(s.has_column("score"));
    assert!(!s.has_column("missing"));
}

// ===========================================================================
// Lookup by ordinal
// ===========================================================================

#[test]
fn lookup_by_ordinal_valid() {
    let s = users_schema();
    let col = s.column_by_ordinal(0).unwrap();
    assert_eq!(col.name, "id");
    assert_eq!(col.data_type, DataType::Integer);

    let col = s.column_by_ordinal(3).unwrap();
    assert_eq!(col.name, "score");
    assert!(col.nullable);
}

#[test]
fn lookup_by_ordinal_out_of_bounds() {
    let s = users_schema();
    assert!(s.column_by_ordinal(4).is_none());
    assert!(s.column_by_ordinal(100).is_none());
}

// ===========================================================================
// add_column
// ===========================================================================

#[test]
fn add_column_to_empty() {
    let mut s = Schema::empty();
    s.add_column(Column::new("x", DataType::Boolean, false));
    assert_eq!(s.column_count(), 1);
    assert!(s.has_column("x"));
    let (idx, _) = s.column_by_name("x").unwrap();
    assert_eq!(idx, 0);
}

#[test]
fn add_column_to_existing() {
    let mut s = users_schema();
    s.add_column(Column::new("active", DataType::Boolean, false));
    assert_eq!(s.column_count(), 5);
    assert!(s.has_column("active"));
    let (idx, _) = s.column_by_name("active").unwrap();
    assert_eq!(idx, 4);
}

// ===========================================================================
// Merge (for joins)
// ===========================================================================

#[test]
fn merge_no_conflicts() {
    let left = users_schema();
    let right = orders_schema();
    let merged = left.merge(&right);
    assert_eq!(merged.column_count(), 8);
    assert!(merged.has_column("id"));
    assert!(merged.has_column("order_id"));
    assert!(merged.has_column("total"));
}

#[test]
fn merge_with_name_conflict() {
    let left = Schema::new(vec![
        Column::new("id", DataType::Integer, false),
        Column::new("name", DataType::Text, false),
    ]);
    let right = Schema::new(vec![
        Column::new("id", DataType::Integer, false),
        Column::new("value", DataType::Float, true),
    ]);
    let merged = left.merge(&right);
    assert_eq!(merged.column_count(), 4);
    assert!(merged.has_column("id"));
    assert!(merged.has_column("name"));
    assert!(merged.has_column("_right_id"));
    assert!(merged.has_column("value"));
}

#[test]
fn merge_multiple_conflicts() {
    let left = Schema::new(vec![
        Column::new("a", DataType::Integer, false),
        Column::new("b", DataType::Integer, false),
    ]);
    let right = Schema::new(vec![
        Column::new("a", DataType::BigInt, false),
        Column::new("b", DataType::BigInt, false),
    ]);
    let merged = left.merge(&right);
    assert_eq!(merged.column_count(), 4);
    assert!(merged.has_column("a"));
    assert!(merged.has_column("b"));
    assert!(merged.has_column("_right_a"));
    assert!(merged.has_column("_right_b"));
}

#[test]
fn merge_preserves_column_order() {
    let left = Schema::new(vec![Column::new("a", DataType::Integer, false)]);
    let right = Schema::new(vec![Column::new("b", DataType::Integer, false)]);
    let merged = left.merge(&right);
    assert_eq!(merged.column_by_ordinal(0).unwrap().name, "a");
    assert_eq!(merged.column_by_ordinal(1).unwrap().name, "b");
}

#[test]
fn merge_with_empty() {
    let s = users_schema();
    let empty = Schema::empty();

    let m1 = s.merge(&empty);
    assert_eq!(m1.column_count(), 4);

    let m2 = empty.merge(&s);
    assert_eq!(m2.column_count(), 4);
}

// ===========================================================================
// Project
// ===========================================================================

#[test]
fn project_subset() {
    let s = users_schema();
    let projected = s.project(&[0, 2]);
    assert_eq!(projected.column_count(), 2);
    assert_eq!(projected.column_by_ordinal(0).unwrap().name, "id");
    assert_eq!(projected.column_by_ordinal(1).unwrap().name, "email");
}

#[test]
fn project_single_column() {
    let s = users_schema();
    let projected = s.project(&[1]);
    assert_eq!(projected.column_count(), 1);
    assert_eq!(projected.column_by_ordinal(0).unwrap().name, "name");
}

#[test]
fn project_all_columns() {
    let s = users_schema();
    let projected = s.project(&[0, 1, 2, 3]);
    assert_eq!(projected, s);
}

#[test]
fn project_empty() {
    let s = users_schema();
    let projected = s.project(&[]);
    assert_eq!(projected.column_count(), 0);
}

#[test]
fn project_out_of_bounds_skipped() {
    let s = users_schema();
    let projected = s.project(&[0, 99]);
    assert_eq!(projected.column_count(), 1);
    assert_eq!(projected.column_by_ordinal(0).unwrap().name, "id");
}

// ===========================================================================
// Equality
// ===========================================================================

#[test]
fn schema_equality() {
    let a = users_schema();
    let b = users_schema();
    assert_eq!(a, b);
}

#[test]
fn schema_inequality_different_columns() {
    let a = users_schema();
    let b = orders_schema();
    assert_ne!(a, b);
}

#[test]
fn schema_inequality_different_nullable() {
    let a = Schema::new(vec![Column::new("x", DataType::Integer, false)]);
    let b = Schema::new(vec![Column::new("x", DataType::Integer, true)]);
    assert_ne!(a, b);
}

#[test]
fn schema_inequality_different_types() {
    let a = Schema::new(vec![Column::new("x", DataType::Integer, false)]);
    let b = Schema::new(vec![Column::new("x", DataType::BigInt, false)]);
    assert_ne!(a, b);
}

#[test]
fn schema_inequality_different_names() {
    let a = Schema::new(vec![Column::new("x", DataType::Integer, false)]);
    let b = Schema::new(vec![Column::new("y", DataType::Integer, false)]);
    assert_ne!(a, b);
}

// ===========================================================================
// Column construction
// ===========================================================================

#[test]
fn column_new() {
    let c = Column::new("test", DataType::Varchar(100), true);
    assert_eq!(c.name, "test");
    assert_eq!(c.data_type, DataType::Varchar(100));
    assert!(c.nullable);
}
