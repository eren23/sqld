-- ============================================================
-- basic_select.sql
-- Tests: Basic SELECT operations, WHERE, ORDER BY, LIMIT,
--        OFFSET, DISTINCT, expressions, and literals.
-- ============================================================

-- Setup
CREATE TABLE test_basic (id INTEGER, name TEXT, value FLOAT);

INSERT INTO test_basic VALUES (1, 'alice', 10.5);
INSERT INTO test_basic VALUES (2, 'bob', 20.0);
INSERT INTO test_basic VALUES (3, 'charlie', 30.5);
INSERT INTO test_basic VALUES (4, 'diana', 20.0);
INSERT INTO test_basic VALUES (5, 'eve', 50.75);

-- Test: SELECT all columns
SELECT * FROM test_basic;

-- Test: SELECT specific columns
SELECT id, name FROM test_basic;

-- Test: SELECT single column
SELECT name FROM test_basic;

-- Test: SELECT with WHERE equality
SELECT * FROM test_basic WHERE id = 3;

-- Test: SELECT with WHERE greater-than
SELECT * FROM test_basic WHERE id > 2;

-- Test: SELECT with WHERE less-than-or-equal
SELECT * FROM test_basic WHERE value <= 20.0;

-- Test: SELECT with WHERE and string comparison
SELECT * FROM test_basic WHERE name = 'bob';

-- Test: SELECT with compound WHERE (AND)
SELECT * FROM test_basic WHERE id > 1 AND value < 30.0;

-- Test: SELECT with compound WHERE (OR)
SELECT * FROM test_basic WHERE name = 'alice' OR name = 'eve';

-- Test: SELECT with ORDER BY ascending (default)
SELECT * FROM test_basic ORDER BY value;

-- Test: SELECT with ORDER BY descending
SELECT * FROM test_basic ORDER BY value DESC;

-- Test: SELECT with ORDER BY multiple columns
SELECT * FROM test_basic ORDER BY value DESC, name ASC;

-- Test: SELECT with LIMIT
SELECT * FROM test_basic ORDER BY id LIMIT 2;

-- Test: SELECT with LIMIT and OFFSET
SELECT * FROM test_basic ORDER BY id LIMIT 2 OFFSET 2;

-- Test: SELECT with OFFSET only (skip first row)
SELECT * FROM test_basic ORDER BY id LIMIT 1 OFFSET 1;

-- Test: SELECT DISTINCT
SELECT DISTINCT value FROM test_basic;

-- Test: SELECT with arithmetic expression and alias
SELECT id, name, value * 2 AS doubled FROM test_basic;

-- Test: SELECT with expression in WHERE
SELECT * FROM test_basic WHERE value * 2 > 40;

-- Test: SELECT literal values (no FROM)
SELECT 42, 'hello', true;

-- Test: SELECT with column alias
SELECT id AS identifier, name AS label FROM test_basic;

-- Cleanup
DROP TABLE test_basic;
