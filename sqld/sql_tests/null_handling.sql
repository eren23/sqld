-- ============================================================
-- null_handling.sql
-- Tests: IS NULL, IS NOT NULL, NULL in arithmetic and
--        comparisons, NULL in aggregates, COALESCE, NULLIF,
--        NULL in boolean expressions, NULL ordering, and
--        COUNT(*) vs COUNT(nullable_col).
-- ============================================================

-- Setup
CREATE TABLE nullable_data (
    id INTEGER,
    name TEXT,
    score INTEGER,
    grade TEXT
);

INSERT INTO nullable_data VALUES (1, 'Alice', 90, 'A');
INSERT INTO nullable_data VALUES (2, 'Bob', NULL, 'B');
INSERT INTO nullable_data VALUES (3, NULL, 75, NULL);
INSERT INTO nullable_data VALUES (4, 'Diana', NULL, NULL);
INSERT INTO nullable_data VALUES (5, 'Eve', 60, 'C');

-- Test: IS NULL
SELECT * FROM nullable_data WHERE score IS NULL;

-- Test: IS NOT NULL
SELECT * FROM nullable_data WHERE score IS NOT NULL;

-- Test: IS NULL on text column
SELECT * FROM nullable_data WHERE name IS NULL;

-- Test: NULL in arithmetic (should produce NULL)
SELECT id, score, score + 10 AS boosted FROM nullable_data;

-- Test: NULL in comparisons (NULL = NULL should be false/unknown)
SELECT * FROM nullable_data WHERE score = NULL;

-- Test: NULL-safe comparison alternative
SELECT * FROM nullable_data WHERE score IS NULL;

-- Test: NULL in aggregate functions (NULLs excluded from SUM, AVG)
SELECT SUM(score) AS total, AVG(score) AS average FROM nullable_data;

-- Test: MIN and MAX ignore NULLs
SELECT MIN(score), MAX(score) FROM nullable_data;

-- Test: COUNT(*) vs COUNT(column) with NULLs
-- COUNT(*) should be 5, COUNT(score) should be 3
SELECT COUNT(*) AS all_rows, COUNT(score) AS non_null_scores FROM nullable_data;

-- Test: COUNT(name) should be 4 (one NULL name)
SELECT COUNT(name) AS non_null_names FROM nullable_data;

-- Test: COALESCE (return first non-null)
SELECT id, COALESCE(name, 'Unknown') AS display_name FROM nullable_data;

-- Test: COALESCE with multiple arguments
SELECT COALESCE(NULL, NULL, 'fallback');

-- Test: COALESCE with score
SELECT id, COALESCE(score, 0) AS effective_score FROM nullable_data;

-- Test: NULLIF (return NULL if arguments are equal)
SELECT NULLIF(1, 1);
SELECT NULLIF(1, 2);
SELECT NULLIF('hello', 'hello');

-- Test: NULL in boolean expressions
-- NULL AND true = NULL, NULL OR true = true
SELECT NULL AND true;
SELECT NULL OR true;
SELECT NULL AND false;
SELECT NOT NULL;

-- Test: NULL in ORDER BY (observe null positioning)
SELECT id, score FROM nullable_data ORDER BY score ASC;
SELECT id, score FROM nullable_data ORDER BY score DESC;

-- Test: Filtering NULLs with COALESCE in WHERE
SELECT * FROM nullable_data WHERE COALESCE(score, 0) > 50;

-- Cleanup
DROP TABLE nullable_data;
