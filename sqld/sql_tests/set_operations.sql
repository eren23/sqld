-- ============================================================
-- set_operations.sql
-- Tests: UNION, UNION ALL, INTERSECT, EXCEPT, nested set
--        operations, and ORDER BY with set operations.
-- ============================================================

-- Setup
CREATE TABLE fruits_a (name TEXT, color TEXT);
CREATE TABLE fruits_b (name TEXT, color TEXT);

INSERT INTO fruits_a VALUES ('apple', 'red');
INSERT INTO fruits_a VALUES ('banana', 'yellow');
INSERT INTO fruits_a VALUES ('cherry', 'red');
INSERT INTO fruits_a VALUES ('banana', 'yellow');

INSERT INTO fruits_b VALUES ('banana', 'yellow');
INSERT INTO fruits_b VALUES ('cherry', 'red');
INSERT INTO fruits_b VALUES ('date', 'brown');
INSERT INTO fruits_b VALUES ('elderberry', 'purple');

-- Test: UNION (removes duplicates)
SELECT name, color FROM fruits_a
UNION
SELECT name, color FROM fruits_b;

-- Test: UNION ALL (preserves duplicates)
SELECT name, color FROM fruits_a
UNION ALL
SELECT name, color FROM fruits_b;

-- Test: INTERSECT (common rows)
SELECT name, color FROM fruits_a
INTERSECT
SELECT name, color FROM fruits_b;

-- Test: EXCEPT (in A but not in B)
SELECT name, color FROM fruits_a
EXCEPT
SELECT name, color FROM fruits_b;

-- Test: EXCEPT reversed (in B but not in A)
SELECT name, color FROM fruits_b
EXCEPT
SELECT name, color FROM fruits_a;

-- Test: Set operation with ORDER BY (applied to the result)
SELECT name, color FROM fruits_a
UNION
SELECT name, color FROM fruits_b
ORDER BY name;

-- Test: UNION ALL with LIMIT
SELECT name FROM fruits_a
UNION ALL
SELECT name FROM fruits_b
ORDER BY name
LIMIT 3;

-- Test: Nested set operations
SELECT name, color FROM fruits_a
UNION
SELECT name, color FROM fruits_b
EXCEPT
SELECT name, color FROM fruits_a;

-- Cleanup
DROP TABLE fruits_a;
DROP TABLE fruits_b;
