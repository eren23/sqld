use sqld::sql::ast::*;
use sqld::sql::parser::parse;
use sqld::types::DataType;

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

fn first_stmt(sql: &str) -> Statement {
    let r = parse(sql);
    assert!(r.errors.is_empty(), "parse errors: {:?}", r.errors);
    r.statements.into_iter().next().unwrap()
}

// ===========================================================================
// CREATE TABLE
// ===========================================================================

#[test]
fn test_create_table_simple() {
    let stmt = first_stmt("CREATE TABLE t (id INTEGER, name TEXT)");
    match stmt {
        Statement::CreateTable(ct) => {
            assert!(!ct.if_not_exists);
            assert_eq!(ct.name, "t");
            assert_eq!(ct.columns.len(), 2);
            assert_eq!(ct.columns[0].name, "id");
            assert_eq!(ct.columns[0].data_type, DataType::Integer);
            assert_eq!(ct.columns[1].name, "name");
            assert_eq!(ct.columns[1].data_type, DataType::Text);
            assert!(ct.constraints.is_empty());
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }
}

#[test]
fn test_create_table_if_not_exists() {
    let stmt = first_stmt("CREATE TABLE IF NOT EXISTS t (id INTEGER)");
    match stmt {
        Statement::CreateTable(ct) => {
            assert!(ct.if_not_exists);
            assert_eq!(ct.name, "t");
            assert_eq!(ct.columns.len(), 1);
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }
}

#[test]
fn test_create_table_not_null() {
    let stmt = first_stmt("CREATE TABLE t (id INTEGER NOT NULL)");
    match stmt {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.columns.len(), 1);
            assert!(ct.columns[0].constraints.contains(&ColumnConstraint::NotNull));
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }
}

#[test]
fn test_create_table_default() {
    let stmt = first_stmt("CREATE TABLE t (id INTEGER DEFAULT 0)");
    match stmt {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.columns.len(), 1);
            assert_eq!(
                ct.columns[0].constraints,
                vec![ColumnConstraint::Default(Expr::Integer(0))]
            );
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }
}

#[test]
fn test_create_table_primary_key_column() {
    let stmt = first_stmt("CREATE TABLE t (id INTEGER PRIMARY KEY)");
    match stmt {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.columns.len(), 1);
            assert!(ct.columns[0].constraints.contains(&ColumnConstraint::PrimaryKey));
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }
}

#[test]
fn test_create_table_unique_column() {
    let stmt = first_stmt("CREATE TABLE t (email TEXT UNIQUE)");
    match stmt {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.columns.len(), 1);
            assert!(ct.columns[0].constraints.contains(&ColumnConstraint::Unique));
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }
}

#[test]
fn test_create_table_check_column() {
    let stmt = first_stmt("CREATE TABLE t (age INTEGER CHECK (age > 0))");
    match stmt {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.columns.len(), 1);
            let constraints = &ct.columns[0].constraints;
            assert_eq!(constraints.len(), 1);
            match &constraints[0] {
                ColumnConstraint::Check(expr) => {
                    // age > 0
                    match expr {
                        Expr::BinaryOp { left, op, right } => {
                            assert_eq!(**left, Expr::Identifier("age".into()));
                            assert_eq!(*op, BinaryOp::Gt);
                            assert_eq!(**right, Expr::Integer(0));
                        }
                        other => panic!("expected BinaryOp, got {:?}", other),
                    }
                }
                other => panic!("expected Check constraint, got {:?}", other),
            }
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }
}

#[test]
fn test_create_table_references() {
    let stmt = first_stmt(
        "CREATE TABLE t (user_id INTEGER REFERENCES users(id) ON DELETE CASCADE ON UPDATE SET NULL)",
    );
    match stmt {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.columns.len(), 1);
            let constraints = &ct.columns[0].constraints;
            let found = constraints.iter().any(|c| matches!(
                c,
                ColumnConstraint::References {
                    table,
                    column,
                    on_delete,
                    on_update,
                } if table == "users"
                    && *column == Some("id".into())
                    && *on_delete == Some(ReferentialAction::Cascade)
                    && *on_update == Some(ReferentialAction::SetNull)
            ));
            assert!(found, "expected References constraint, got {:?}", constraints);
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }
}

#[test]
fn test_create_table_table_level_primary_key() {
    let stmt = first_stmt("CREATE TABLE t (a INTEGER, b INTEGER, PRIMARY KEY (a, b))");
    match stmt {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.columns.len(), 2);
            assert_eq!(ct.constraints.len(), 1);
            match &ct.constraints[0] {
                TableConstraint::PrimaryKey { name, columns } => {
                    assert!(name.is_none());
                    assert_eq!(columns, &["a".to_string(), "b".to_string()]);
                }
                other => panic!("expected PrimaryKey table constraint, got {:?}", other),
            }
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }
}

#[test]
fn test_create_table_named_constraint() {
    let stmt = first_stmt("CREATE TABLE t (a INTEGER, CONSTRAINT pk_t PRIMARY KEY (a))");
    match stmt {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.constraints.len(), 1);
            match &ct.constraints[0] {
                TableConstraint::PrimaryKey { name, columns } => {
                    assert_eq!(*name, Some("pk_t".to_string()));
                    assert_eq!(columns, &["a".to_string()]);
                }
                other => panic!("expected named PrimaryKey constraint, got {:?}", other),
            }
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }
}

#[test]
fn test_create_table_foreign_key_table_constraint() {
    let stmt = first_stmt(
        "CREATE TABLE t (uid INTEGER, FOREIGN KEY (uid) REFERENCES users(id))",
    );
    match stmt {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.columns.len(), 1);
            assert_eq!(ct.constraints.len(), 1);
            match &ct.constraints[0] {
                TableConstraint::ForeignKey {
                    name,
                    columns,
                    ref_table,
                    ref_columns,
                    on_delete,
                    on_update,
                } => {
                    assert!(name.is_none());
                    assert_eq!(columns, &["uid".to_string()]);
                    assert_eq!(ref_table, "users");
                    assert_eq!(ref_columns, &["id".to_string()]);
                    assert!(on_delete.is_none());
                    assert!(on_update.is_none());
                }
                other => panic!("expected ForeignKey table constraint, got {:?}", other),
            }
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }
}

#[test]
fn test_create_table_varchar_and_decimal() {
    let stmt = first_stmt("CREATE TABLE t (name VARCHAR(100), price DECIMAL(10, 2))");
    match stmt {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.columns.len(), 2);
            assert_eq!(ct.columns[0].name, "name");
            assert_eq!(ct.columns[0].data_type, DataType::Varchar(100));
            assert_eq!(ct.columns[1].name, "price");
            assert_eq!(ct.columns[1].data_type, DataType::Decimal(10, 2));
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }
}

#[test]
fn test_create_table_all_data_types() {
    let stmt = first_stmt(
        "CREATE TABLE t (\
            a INT, \
            b BIGINT, \
            c FLOAT, \
            d BOOLEAN, \
            e TEXT, \
            f TIMESTAMP, \
            g DATE, \
            h BLOB\
        )",
    );
    match stmt {
        Statement::CreateTable(ct) => {
            assert_eq!(ct.columns.len(), 8);
            assert_eq!(ct.columns[0].data_type, DataType::Integer);
            assert_eq!(ct.columns[1].data_type, DataType::BigInt);
            assert_eq!(ct.columns[2].data_type, DataType::Float);
            assert_eq!(ct.columns[3].data_type, DataType::Boolean);
            assert_eq!(ct.columns[4].data_type, DataType::Text);
            assert_eq!(ct.columns[5].data_type, DataType::Timestamp);
            assert_eq!(ct.columns[6].data_type, DataType::Date);
            assert_eq!(ct.columns[7].data_type, DataType::Blob);
        }
        other => panic!("expected CreateTable, got {:?}", other),
    }
}

// ===========================================================================
// DROP TABLE
// ===========================================================================

#[test]
fn test_drop_table_simple() {
    let stmt = first_stmt("DROP TABLE t");
    match stmt {
        Statement::DropTable(dt) => {
            assert!(!dt.if_exists);
            assert_eq!(dt.name, "t");
            assert!(!dt.cascade);
        }
        other => panic!("expected DropTable, got {:?}", other),
    }
}

#[test]
fn test_drop_table_if_exists() {
    let stmt = first_stmt("DROP TABLE IF EXISTS t");
    match stmt {
        Statement::DropTable(dt) => {
            assert!(dt.if_exists);
            assert_eq!(dt.name, "t");
            assert!(!dt.cascade);
        }
        other => panic!("expected DropTable, got {:?}", other),
    }
}

#[test]
fn test_drop_table_cascade() {
    let stmt = first_stmt("DROP TABLE t CASCADE");
    match stmt {
        Statement::DropTable(dt) => {
            assert!(!dt.if_exists);
            assert_eq!(dt.name, "t");
            assert!(dt.cascade);
        }
        other => panic!("expected DropTable, got {:?}", other),
    }
}

// ===========================================================================
// ALTER TABLE
// ===========================================================================

#[test]
fn test_alter_table_add_column() {
    let stmt = first_stmt("ALTER TABLE t ADD COLUMN name TEXT");
    match stmt {
        Statement::AlterTable(at) => {
            assert_eq!(at.name, "t");
            match at.action {
                AlterTableAction::AddColumn(col) => {
                    assert_eq!(col.name, "name");
                    assert_eq!(col.data_type, DataType::Text);
                }
                other => panic!("expected AddColumn, got {:?}", other),
            }
        }
        other => panic!("expected AlterTable, got {:?}", other),
    }
}

#[test]
fn test_alter_table_drop_column() {
    let stmt = first_stmt("ALTER TABLE t DROP COLUMN name");
    match stmt {
        Statement::AlterTable(at) => {
            assert_eq!(at.name, "t");
            match at.action {
                AlterTableAction::DropColumn { name } => {
                    assert_eq!(name, "name");
                }
                other => panic!("expected DropColumn, got {:?}", other),
            }
        }
        other => panic!("expected AlterTable, got {:?}", other),
    }
}

#[test]
fn test_alter_table_rename_column() {
    let stmt = first_stmt("ALTER TABLE t RENAME COLUMN old TO new");
    match stmt {
        Statement::AlterTable(at) => {
            assert_eq!(at.name, "t");
            match at.action {
                AlterTableAction::RenameColumn { old_name, new_name } => {
                    assert_eq!(old_name, "old");
                    assert_eq!(new_name, "new");
                }
                other => panic!("expected RenameColumn, got {:?}", other),
            }
        }
        other => panic!("expected AlterTable, got {:?}", other),
    }
}

#[test]
fn test_alter_table_add_constraint() {
    let stmt = first_stmt("ALTER TABLE t ADD CONSTRAINT uq UNIQUE (email)");
    match stmt {
        Statement::AlterTable(at) => {
            assert_eq!(at.name, "t");
            match at.action {
                AlterTableAction::AddConstraint(TableConstraint::Unique { name, columns }) => {
                    assert_eq!(name, Some("uq".to_string()));
                    assert_eq!(columns, vec!["email".to_string()]);
                }
                other => panic!("expected AddConstraint(Unique), got {:?}", other),
            }
        }
        other => panic!("expected AlterTable, got {:?}", other),
    }
}

#[test]
fn test_alter_table_drop_constraint() {
    let stmt = first_stmt("ALTER TABLE t DROP CONSTRAINT uq");
    match stmt {
        Statement::AlterTable(at) => {
            assert_eq!(at.name, "t");
            match at.action {
                AlterTableAction::DropConstraint { name } => {
                    assert_eq!(name, "uq");
                }
                other => panic!("expected DropConstraint, got {:?}", other),
            }
        }
        other => panic!("expected AlterTable, got {:?}", other),
    }
}

// ===========================================================================
// CREATE / DROP INDEX
// ===========================================================================

#[test]
fn test_create_index_simple() {
    let stmt = first_stmt("CREATE INDEX idx ON t (col)");
    match stmt {
        Statement::CreateIndex(ci) => {
            assert!(!ci.unique);
            assert_eq!(ci.name, "idx");
            assert_eq!(ci.table, "t");
            assert_eq!(ci.columns.len(), 1);
            assert_eq!(ci.columns[0].name, "col");
            assert!(ci.columns[0].direction.is_none());
            assert!(ci.using_method.is_none());
        }
        other => panic!("expected CreateIndex, got {:?}", other),
    }
}

#[test]
fn test_create_unique_index() {
    let stmt = first_stmt("CREATE UNIQUE INDEX idx ON t (col)");
    match stmt {
        Statement::CreateIndex(ci) => {
            assert!(ci.unique);
            assert_eq!(ci.name, "idx");
            assert_eq!(ci.table, "t");
        }
        other => panic!("expected CreateIndex, got {:?}", other),
    }
}

#[test]
fn test_create_index_using_hash() {
    let stmt = first_stmt("CREATE INDEX idx ON t USING hash (col)");
    match stmt {
        Statement::CreateIndex(ci) => {
            assert_eq!(ci.using_method, Some(IndexMethod::Hash));
            assert_eq!(ci.columns.len(), 1);
            assert_eq!(ci.columns[0].name, "col");
        }
        other => panic!("expected CreateIndex, got {:?}", other),
    }
}

#[test]
fn test_create_index_multi_columns_with_direction() {
    let stmt = first_stmt("CREATE INDEX idx ON t (a ASC, b DESC)");
    match stmt {
        Statement::CreateIndex(ci) => {
            assert_eq!(ci.columns.len(), 2);
            assert_eq!(ci.columns[0].name, "a");
            assert_eq!(ci.columns[0].direction, Some(OrderDirection::Asc));
            assert_eq!(ci.columns[1].name, "b");
            assert_eq!(ci.columns[1].direction, Some(OrderDirection::Desc));
        }
        other => panic!("expected CreateIndex, got {:?}", other),
    }
}

#[test]
fn test_drop_index_simple() {
    let stmt = first_stmt("DROP INDEX idx");
    match stmt {
        Statement::DropIndex(di) => {
            assert!(!di.if_exists);
            assert_eq!(di.name, "idx");
        }
        other => panic!("expected DropIndex, got {:?}", other),
    }
}

#[test]
fn test_drop_index_if_exists() {
    let stmt = first_stmt("DROP INDEX IF EXISTS idx");
    match stmt {
        Statement::DropIndex(di) => {
            assert!(di.if_exists);
            assert_eq!(di.name, "idx");
        }
        other => panic!("expected DropIndex, got {:?}", other),
    }
}

// ===========================================================================
// CREATE / DROP VIEW
// ===========================================================================

#[test]
fn test_create_view_simple() {
    let stmt = first_stmt("CREATE VIEW v AS SELECT * FROM t");
    match stmt {
        Statement::CreateView(cv) => {
            assert_eq!(cv.name, "v");
            assert!(cv.columns.is_none());
            // The query should be a SELECT * FROM t
            assert!(cv.query.from.is_some());
        }
        other => panic!("expected CreateView, got {:?}", other),
    }
}

#[test]
fn test_create_view_with_columns() {
    let stmt = first_stmt("CREATE VIEW v (a, b) AS SELECT x, y FROM t");
    match stmt {
        Statement::CreateView(cv) => {
            assert_eq!(cv.name, "v");
            assert_eq!(cv.columns, Some(vec!["a".to_string(), "b".to_string()]));
            assert_eq!(cv.query.columns.len(), 2);
        }
        other => panic!("expected CreateView, got {:?}", other),
    }
}

#[test]
fn test_drop_view_simple() {
    let stmt = first_stmt("DROP VIEW v");
    match stmt {
        Statement::DropView(dv) => {
            assert!(!dv.if_exists);
            assert_eq!(dv.name, "v");
        }
        other => panic!("expected DropView, got {:?}", other),
    }
}

#[test]
fn test_drop_view_if_exists() {
    let stmt = first_stmt("DROP VIEW IF EXISTS v");
    match stmt {
        Statement::DropView(dv) => {
            assert!(dv.if_exists);
            assert_eq!(dv.name, "v");
        }
        other => panic!("expected DropView, got {:?}", other),
    }
}

// ===========================================================================
// Transactions
// ===========================================================================

#[test]
fn test_begin() {
    let stmt = first_stmt("BEGIN");
    assert_eq!(stmt, Statement::Begin);
}

#[test]
fn test_commit() {
    let stmt = first_stmt("COMMIT");
    assert_eq!(stmt, Statement::Commit);
}

#[test]
fn test_rollback() {
    let stmt = first_stmt("ROLLBACK");
    assert_eq!(stmt, Statement::Rollback { savepoint: None });
}

#[test]
fn test_rollback_to_savepoint() {
    let stmt = first_stmt("ROLLBACK TO sp1");
    assert_eq!(
        stmt,
        Statement::Rollback {
            savepoint: Some("sp1".to_string()),
        }
    );
}

#[test]
fn test_savepoint() {
    let stmt = first_stmt("SAVEPOINT sp1");
    assert_eq!(
        stmt,
        Statement::Savepoint {
            name: "sp1".to_string(),
        }
    );
}

// ===========================================================================
// Utility statements
// ===========================================================================

#[test]
fn test_explain() {
    let stmt = first_stmt("EXPLAIN SELECT 1");
    match stmt {
        Statement::Explain { analyze, statement } => {
            assert!(!analyze);
            match *statement {
                Statement::Select(_) => {}
                other => panic!("expected Select inside Explain, got {:?}", other),
            }
        }
        other => panic!("expected Explain, got {:?}", other),
    }
}

#[test]
fn test_explain_analyze() {
    let stmt = first_stmt("EXPLAIN ANALYZE SELECT 1");
    match stmt {
        Statement::Explain { analyze, statement } => {
            assert!(analyze);
            match *statement {
                Statement::Select(_) => {}
                other => panic!("expected Select inside Explain Analyze, got {:?}", other),
            }
        }
        other => panic!("expected Explain, got {:?}", other),
    }
}

#[test]
fn test_show_tables() {
    let stmt = first_stmt("SHOW TABLES");
    assert_eq!(stmt, Statement::ShowTables);
}

#[test]
fn test_show_columns() {
    let stmt = first_stmt("SHOW COLUMNS FROM t");
    assert_eq!(
        stmt,
        Statement::ShowColumns {
            table: "t".to_string(),
        }
    );
}

#[test]
fn test_analyze_no_table() {
    let stmt = first_stmt("ANALYZE");
    assert_eq!(stmt, Statement::Analyze { table: None });
}

#[test]
fn test_analyze_with_table() {
    let stmt = first_stmt("ANALYZE t");
    assert_eq!(
        stmt,
        Statement::Analyze {
            table: Some("t".to_string()),
        }
    );
}

#[test]
fn test_vacuum() {
    let stmt = first_stmt("VACUUM");
    assert_eq!(stmt, Statement::Vacuum { table: None });
}

#[test]
fn test_vacuum_with_table() {
    let stmt = first_stmt("VACUUM t");
    assert_eq!(
        stmt,
        Statement::Vacuum {
            table: Some("t".to_string()),
        }
    );
}

#[test]
fn test_copy_from() {
    let stmt = first_stmt("COPY t FROM '/tmp/data.csv'");
    match stmt {
        Statement::Copy(c) => {
            assert_eq!(c.table, "t");
            assert!(c.columns.is_none());
            assert_eq!(c.direction, CopyDirection::From("/tmp/data.csv".to_string()));
        }
        other => panic!("expected Copy, got {:?}", other),
    }
}

#[test]
fn test_copy_to_with_columns() {
    let stmt = first_stmt("COPY t (a, b) TO '/tmp/out.csv'");
    match stmt {
        Statement::Copy(c) => {
            assert_eq!(c.table, "t");
            assert_eq!(
                c.columns,
                Some(vec!["a".to_string(), "b".to_string()])
            );
            assert_eq!(c.direction, CopyDirection::To("/tmp/out.csv".to_string()));
        }
        other => panic!("expected Copy, got {:?}", other),
    }
}
