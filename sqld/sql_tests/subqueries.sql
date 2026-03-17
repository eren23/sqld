-- ============================================================
-- subqueries.sql
-- Tests: Scalar subquery in SELECT, subquery in WHERE with
--        IN/EXISTS, correlated subquery, derived tables
--        (subquery in FROM), and comparison operators.
-- ============================================================

-- Setup
CREATE TABLE orders (
    id INTEGER,
    customer_id INTEGER,
    total FLOAT
);

CREATE TABLE customers (
    id INTEGER,
    name TEXT,
    city TEXT
);

INSERT INTO customers VALUES (1, 'Alice', 'New York');
INSERT INTO customers VALUES (2, 'Bob', 'Boston');
INSERT INTO customers VALUES (3, 'Charlie', 'New York');
INSERT INTO customers VALUES (4, 'Diana', 'Chicago');

INSERT INTO orders VALUES (100, 1, 250.0);
INSERT INTO orders VALUES (101, 1, 125.0);
INSERT INTO orders VALUES (102, 2, 300.0);
INSERT INTO orders VALUES (103, 3, 50.0);

-- Test: Scalar subquery in SELECT
SELECT name,
       (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) AS order_count
FROM customers c;

-- Test: Subquery in WHERE with IN
SELECT name FROM customers
WHERE id IN (SELECT customer_id FROM orders WHERE total > 100);

-- Test: Subquery in WHERE with NOT IN
SELECT name FROM customers
WHERE id NOT IN (SELECT customer_id FROM orders);

-- Test: Subquery in WHERE with EXISTS
SELECT name FROM customers c
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id);

-- Test: Subquery in WHERE with NOT EXISTS
SELECT name FROM customers c
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id);

-- Test: Subquery with comparison operator (scalar)
SELECT name FROM customers c
WHERE (SELECT SUM(total) FROM orders o WHERE o.customer_id = c.id) > 200;

-- Test: Derived table (subquery in FROM)
SELECT sub.city, sub.customer_count
FROM (
    SELECT city, COUNT(*) AS customer_count
    FROM customers
    GROUP BY city
) sub
WHERE sub.customer_count > 1;

-- Test: Subquery in WHERE with ALL/ANY-style via MAX/MIN
SELECT name FROM customers c
WHERE (SELECT MAX(total) FROM orders o WHERE o.customer_id = c.id) =
      (SELECT MAX(total) FROM orders);

-- Test: Nested subquery
SELECT name FROM customers
WHERE id IN (
    SELECT customer_id FROM orders
    WHERE total > (SELECT AVG(total) FROM orders)
);

-- Cleanup
DROP TABLE orders;
DROP TABLE customers;
