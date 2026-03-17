-- ============================================================
-- aggregates.sql
-- Tests: COUNT, SUM, AVG, MIN, MAX, GROUP BY, HAVING,
--        DISTINCT inside aggregates, NULL handling in
--        aggregates, and aggregate expressions.
-- ============================================================

-- Setup
CREATE TABLE sales (
    id INTEGER,
    region TEXT,
    product TEXT,
    amount FLOAT,
    qty INTEGER
);

INSERT INTO sales VALUES (1, 'East', 'Widget', 100.0, 5);
INSERT INTO sales VALUES (2, 'East', 'Widget', 150.0, 3);
INSERT INTO sales VALUES (3, 'East', 'Gadget', 200.0, 1);
INSERT INTO sales VALUES (4, 'West', 'Widget', 120.0, 4);
INSERT INTO sales VALUES (5, 'West', 'Gadget', 300.0, 2);
INSERT INTO sales VALUES (6, 'West', 'Gadget', NULL, 0);
INSERT INTO sales VALUES (7, 'North', 'Widget', 80.0, 6);

-- Test: COUNT(*)
SELECT COUNT(*) FROM sales;

-- Test: COUNT(column) -- should exclude NULLs
SELECT COUNT(amount) FROM sales;

-- Test: COUNT(DISTINCT column)
SELECT COUNT(DISTINCT region) FROM sales;

-- Test: SUM
SELECT SUM(amount) FROM sales;

-- Test: AVG (should exclude NULLs from calculation)
SELECT AVG(amount) FROM sales;

-- Test: MIN and MAX
SELECT MIN(amount), MAX(amount) FROM sales;

-- Test: GROUP BY single column
SELECT region, COUNT(*) AS cnt, SUM(amount) AS total
FROM sales
GROUP BY region;

-- Test: GROUP BY multiple columns
SELECT region, product, SUM(amount) AS total
FROM sales
GROUP BY region, product;

-- Test: HAVING clause (filter groups)
SELECT region, SUM(amount) AS total
FROM sales
GROUP BY region
HAVING SUM(amount) > 200;

-- Test: Aggregate with DISTINCT (SUM DISTINCT)
SELECT SUM(DISTINCT qty) FROM sales;

-- Test: MIN and MAX on text column
SELECT MIN(region), MAX(region) FROM sales;

-- Test: Aggregate with expression
SELECT region, SUM(amount * qty) AS revenue
FROM sales
GROUP BY region;

-- Test: COUNT(*) vs COUNT(nullable_col)
-- COUNT(*) = 7, COUNT(amount) = 6
SELECT COUNT(*) AS total_rows, COUNT(amount) AS non_null_amounts FROM sales;

-- Cleanup
DROP TABLE sales;
