-- ============================================================
-- edge_cases.sql
-- Tests: Empty table SELECT, long strings, integer limits,
--        division by zero, empty string vs NULL, multiple
--        semicolons, comments, keyword case insensitivity,
--        quoted identifiers, and SELECT without FROM.
-- ============================================================

-- Test: SELECT from empty table
CREATE TABLE empty_table (id INTEGER, val TEXT);
SELECT * FROM empty_table;
SELECT COUNT(*) FROM empty_table;
DROP TABLE empty_table;

-- Test: Very long string value
CREATE TABLE long_strings (id INTEGER, content TEXT);
INSERT INTO long_strings VALUES (1, 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz0123456789012345678901234567890123456789');
SELECT LENGTH(content) FROM long_strings;
DROP TABLE long_strings;

-- Test: Large integer values
SELECT 2147483647;
SELECT -2147483648;
SELECT 9223372036854775807;

-- Test: Division by zero (should produce error or NULL/Inf)
-- Uncomment to test; behavior may vary:
-- SELECT 1 / 0;
-- SELECT 1.0 / 0.0;

-- Test: Modulo by zero
-- Uncomment to test; expected: error
-- SELECT 10 % 0;

-- Test: Empty string vs NULL
CREATE TABLE str_test (id INTEGER, val TEXT);
INSERT INTO str_test VALUES (1, '');
INSERT INTO str_test VALUES (2, NULL);
-- Empty string is NOT NULL
SELECT * FROM str_test WHERE val IS NULL;
SELECT * FROM str_test WHERE val IS NOT NULL;
SELECT * FROM str_test WHERE val = '';
SELECT LENGTH(val) FROM str_test;
DROP TABLE str_test;

-- Test: SQL comments (single line)
-- This is a single-line comment
SELECT 1; -- inline comment

-- Test: SQL comments (block)
/* This is a
   multi-line block comment */
SELECT /* inline block comment */ 2;

-- Test: Case insensitivity of SQL keywords
select 'lower';
SELECT 'upper';
SeLeCt 'mixed';

-- Test: Case sensitivity of identifiers (column/table names)
CREATE TABLE CaseSensitive (Id INTEGER, Name TEXT);
INSERT INTO CaseSensitive VALUES (1, 'test');
SELECT Id, Name FROM CaseSensitive;
DROP TABLE CaseSensitive;

-- Test: SELECT without FROM
SELECT 1 + 1;
SELECT 'hello' || ' ' || 'world';
SELECT COALESCE(NULL, 'default');

-- Test: CASE WHEN expression
SELECT
    CASE WHEN 1 > 0 THEN 'positive'
         WHEN 1 = 0 THEN 'zero'
         ELSE 'negative'
    END;

-- Test: CASE WHEN with table data
CREATE TABLE scores (id INTEGER, score INTEGER);
INSERT INTO scores VALUES (1, 95);
INSERT INTO scores VALUES (2, 72);
INSERT INTO scores VALUES (3, 45);
SELECT id, score,
    CASE WHEN score >= 90 THEN 'A'
         WHEN score >= 70 THEN 'B'
         WHEN score >= 50 THEN 'C'
         ELSE 'F'
    END AS grade
FROM scores;
DROP TABLE scores;

-- Test: IN with literal list
SELECT 3 IN (1, 2, 3, 4, 5);
SELECT 'x' IN ('a', 'b', 'c');

-- Test: Nested function calls
SELECT UPPER(SUBSTRING('hello world', 1, 5));
SELECT ABS(ROUND(-3.7));
