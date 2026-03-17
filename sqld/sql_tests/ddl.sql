-- ============================================================
-- ddl.sql
-- Tests: CREATE TABLE, CREATE TABLE IF NOT EXISTS, NOT NULL,
--        DEFAULT, DROP TABLE, DROP TABLE IF EXISTS,
--        CREATE INDEX (BTREE, HASH, UNIQUE), DROP INDEX,
--        CREATE VIEW, DROP VIEW.
-- ============================================================

-- Test: CREATE TABLE basic
CREATE TABLE test_ddl (
    id INTEGER,
    name VARCHAR(100),
    active BOOLEAN
);

-- Test: INSERT to verify table was created
INSERT INTO test_ddl VALUES (1, 'test', true);
SELECT * FROM test_ddl;

-- Test: DROP TABLE
DROP TABLE test_ddl;

-- Test: CREATE TABLE IF NOT EXISTS (should succeed, table was dropped)
CREATE TABLE IF NOT EXISTS test_ddl (
    id INTEGER,
    name TEXT
);

-- Test: CREATE TABLE IF NOT EXISTS (should be no-op, table already exists)
CREATE TABLE IF NOT EXISTS test_ddl (
    id INTEGER,
    name TEXT
);

-- Test: DROP TABLE IF EXISTS
DROP TABLE IF EXISTS test_ddl;

-- Test: DROP TABLE IF EXISTS on non-existent table (should be no-op)
DROP TABLE IF EXISTS nonexistent_table;

-- Test: CREATE TABLE with NOT NULL constraint
CREATE TABLE strict_table (
    id INTEGER NOT NULL,
    label TEXT NOT NULL,
    description TEXT
);

INSERT INTO strict_table VALUES (1, 'first', 'a description');
INSERT INTO strict_table VALUES (2, 'second', NULL);
SELECT * FROM strict_table;

-- Test: CREATE TABLE with various data types
CREATE TABLE all_types (
    col_int INTEGER,
    col_bigint BIGINT,
    col_float FLOAT,
    col_bool BOOLEAN,
    col_varchar VARCHAR(255),
    col_text TEXT,
    col_ts TIMESTAMP,
    col_date DATE,
    col_decimal DECIMAL(10, 2),
    col_blob BLOB
);

-- Test: CREATE INDEX (BTREE - default)
CREATE INDEX idx_strict_id ON strict_table (id);

-- Test: CREATE INDEX with HASH type
CREATE INDEX idx_strict_label ON strict_table USING HASH (label);

-- Test: CREATE UNIQUE INDEX
CREATE UNIQUE INDEX idx_strict_id_unique ON strict_table (id);

-- Test: DROP INDEX
DROP INDEX idx_strict_id;
DROP INDEX idx_strict_label;
DROP INDEX idx_strict_id_unique;

-- Test: CREATE VIEW
CREATE VIEW strict_view AS
    SELECT id, label FROM strict_table WHERE id > 0;

-- Verify view works
SELECT * FROM strict_view;

-- Test: DROP VIEW
DROP VIEW strict_view;

-- Cleanup
DROP TABLE strict_table;
DROP TABLE all_types;
