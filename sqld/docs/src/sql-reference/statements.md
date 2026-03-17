# SQL Statements

sqld supports a comprehensive set of SQL statements for data definition, data manipulation, transaction control, set operations, and utility commands. This page documents every `Statement` variant recognized by the parser.

---

## Data Definition Language (DDL)

### CREATE TABLE

Creates a new table with column definitions and optional constraints.

**Syntax:**

```sql
CREATE TABLE [IF NOT EXISTS] table_name (
    column_name data_type [column_constraint ...] [, ...]
    [, table_constraint [, ...]]
);
```

**Column Constraints:**

| Constraint | Description |
|---|---|
| `NOT NULL` | The column cannot contain NULL values. |
| `NULL` | The column explicitly allows NULL values (default). |
| `DEFAULT expr` | Default value when no value is supplied on INSERT. |
| `PRIMARY KEY` | Designates this column as the table's primary key. |
| `UNIQUE` | All values in the column must be distinct. |
| `CHECK (expr)` | Every row must satisfy the boolean expression. |
| `REFERENCES ref_table [(ref_column)]` | Foreign key reference to another table. Supports `ON DELETE` and `ON UPDATE` actions. |

**Referential Actions** (for `ON DELETE` / `ON UPDATE`):

- `CASCADE` -- Propagate the change to referencing rows.
- `RESTRICT` -- Reject the change if referencing rows exist.
- `NO ACTION` -- Similar to RESTRICT (deferred checking).
- `SET NULL` -- Set referencing columns to NULL.
- `SET DEFAULT` -- Set referencing columns to their default values.

**Table Constraints:**

```sql
-- Composite primary key
CONSTRAINT pk_name PRIMARY KEY (col1, col2)

-- Composite unique
CONSTRAINT uq_name UNIQUE (col1, col2)

-- Table-level check
CONSTRAINT ck_name CHECK (expr)

-- Composite foreign key
CONSTRAINT fk_name FOREIGN KEY (col1, col2)
    REFERENCES ref_table (ref_col1, ref_col2)
    [ON DELETE action] [ON UPDATE action]
```

Constraint names are optional. When omitted, sqld assigns an internal name.

**Examples:**

```sql
CREATE TABLE employees (
    id        INTEGER PRIMARY KEY,
    name      VARCHAR(100) NOT NULL,
    email     VARCHAR(255) UNIQUE,
    dept_id   INTEGER REFERENCES departments(id) ON DELETE SET NULL,
    salary    DECIMAL(10,2) DEFAULT 0.00,
    active    BOOLEAN NOT NULL DEFAULT true,
    CHECK (salary >= 0)
);

CREATE TABLE IF NOT EXISTS order_items (
    order_id   INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity   INTEGER NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE RESTRICT
);
```

---

### DROP TABLE

Removes a table and all of its data.

**Syntax:**

```sql
DROP TABLE [IF EXISTS] table_name [CASCADE];
```

- `IF EXISTS` -- Suppresses the error if the table does not exist.
- `CASCADE` -- Also drops objects that depend on the table (e.g., views, foreign key constraints in other tables).

**Examples:**

```sql
DROP TABLE temp_data;
DROP TABLE IF EXISTS old_logs;
DROP TABLE departments CASCADE;
```

---

### ALTER TABLE

Modifies the structure of an existing table.

**Syntax:**

```sql
ALTER TABLE table_name action;
```

**Supported Actions:**

| Action | Syntax |
|---|---|
| Add a column | `ADD COLUMN column_name data_type [constraints...]` |
| Drop a column | `DROP COLUMN column_name` |
| Rename a column | `RENAME COLUMN old_name TO new_name` |
| Add a table constraint | `ADD CONSTRAINT constraint_name constraint_def` |
| Drop a constraint | `DROP CONSTRAINT constraint_name` |

**Examples:**

```sql
-- Add a new column with a default
ALTER TABLE employees ADD COLUMN hire_date DATE DEFAULT '2024-01-01';

-- Remove a column
ALTER TABLE employees DROP COLUMN active;

-- Rename a column
ALTER TABLE employees RENAME COLUMN name TO full_name;

-- Add a unique constraint
ALTER TABLE employees ADD CONSTRAINT uq_email UNIQUE (email);

-- Drop a constraint
ALTER TABLE employees DROP CONSTRAINT uq_email;
```

---

### CREATE INDEX

Creates an index on one or more columns to speed up queries.

**Syntax:**

```sql
CREATE [UNIQUE] INDEX index_name
    ON table_name [USING {BTREE | HASH}]
    (column_name [ASC | DESC] [, ...]);
```

- `UNIQUE` -- Enforces uniqueness on the indexed columns.
- `USING BTREE` -- B-tree index (default). Supports range queries and ordering.
- `USING HASH` -- Hash index. Optimized for equality lookups.
- Each column can specify `ASC` (default) or `DESC` ordering.

**Examples:**

```sql
CREATE INDEX idx_employees_name ON employees (name);

CREATE UNIQUE INDEX idx_employees_email ON employees (email);

CREATE INDEX idx_orders_date_desc ON orders (order_date DESC);

CREATE INDEX idx_lookup ON sessions USING HASH (session_token);

-- Composite index
CREATE INDEX idx_orders_customer_date ON orders (customer_id, order_date DESC);
```

---

### DROP INDEX

Removes an existing index.

**Syntax:**

```sql
DROP INDEX [IF EXISTS] index_name;
```

**Examples:**

```sql
DROP INDEX idx_employees_name;
DROP INDEX IF EXISTS idx_old_lookup;
```

---

### CREATE VIEW

Defines a named query that can be referenced like a table.

**Syntax:**

```sql
CREATE VIEW view_name [(column_name [, ...])] AS select_query;
```

- If column names are specified, they override the column names from the query.

**Examples:**

```sql
CREATE VIEW active_employees AS
    SELECT id, name, dept_id
    FROM employees
    WHERE active = true;

CREATE VIEW dept_salary_summary (department, avg_salary, employee_count) AS
    SELECT d.name, AVG(e.salary), COUNT(*)
    FROM employees e
    JOIN departments d ON e.dept_id = d.id
    GROUP BY d.name;
```

---

### DROP VIEW

Removes an existing view.

**Syntax:**

```sql
DROP VIEW [IF EXISTS] view_name;
```

**Examples:**

```sql
DROP VIEW active_employees;
DROP VIEW IF EXISTS dept_salary_summary;
```

---

## Data Manipulation Language (DML)

### SELECT

Retrieves data from one or more tables.

**Syntax:**

```sql
SELECT [DISTINCT] select_list
    [FROM table_reference [join ...]]
    [WHERE condition]
    [GROUP BY expression [, ...]]
    [HAVING condition]
    [ORDER BY expression [ASC | DESC] [NULLS {FIRST | LAST}] [, ...]]
    [LIMIT count]
    [OFFSET skip];
```

**Select List:**

- `*` -- All columns from all tables.
- `table.*` -- All columns from a specific table.
- `expr [AS alias]` -- An expression with an optional alias.

**FROM Clause:**

- Table name with optional alias: `FROM employees e`
- Subquery with required alias: `FROM (SELECT ...) AS sub`
- Joins (see [Joins](joins.md) for details)

**ORDER BY:**

Each sort expression supports:
- `ASC` (ascending, default) or `DESC` (descending)
- `NULLS FIRST` or `NULLS LAST` to control NULL placement

**Examples:**

```sql
-- Simple query
SELECT * FROM employees;

-- With filtering, ordering, and limit
SELECT name, salary
FROM employees
WHERE dept_id = 3 AND active = true
ORDER BY salary DESC
LIMIT 10;

-- DISTINCT
SELECT DISTINCT dept_id FROM employees;

-- Aliases and expressions
SELECT
    e.name AS employee_name,
    d.name AS department,
    e.salary * 12 AS annual_salary
FROM employees e
JOIN departments d ON e.dept_id = d.id;

-- GROUP BY with HAVING
SELECT dept_id, COUNT(*) AS cnt, AVG(salary) AS avg_sal
FROM employees
GROUP BY dept_id
HAVING COUNT(*) > 5
ORDER BY avg_sal DESC;

-- Subquery in FROM
SELECT sub.dept, sub.total
FROM (
    SELECT dept_id AS dept, SUM(salary) AS total
    FROM employees
    GROUP BY dept_id
) AS sub
WHERE sub.total > 100000;

-- OFFSET for pagination
SELECT * FROM products ORDER BY id LIMIT 20 OFFSET 40;
```

---

### INSERT

Adds new rows to a table.

**Syntax:**

```sql
-- Insert with VALUES
INSERT INTO table_name [(column [, ...])]
    VALUES (expr [, ...]) [, (expr [, ...])]
    [RETURNING select_list];

-- Insert from a SELECT
INSERT INTO table_name [(column [, ...])]
    SELECT ...
    [RETURNING select_list];
```

- When the column list is omitted, values must be supplied for every column in table-definition order.
- Multiple rows can be inserted in a single `VALUES` clause.
- `RETURNING` returns the inserted rows (or a subset of columns).

**Examples:**

```sql
-- Single row
INSERT INTO employees (name, dept_id, salary)
VALUES ('Alice', 1, 75000.00);

-- Multiple rows
INSERT INTO products (name, price)
VALUES ('Widget', 9.99), ('Gadget', 19.99), ('Doohickey', 4.50);

-- Insert from select
INSERT INTO archived_orders (id, customer, total)
SELECT id, customer_id, total
FROM orders
WHERE order_date < '2023-01-01';

-- With RETURNING
INSERT INTO employees (name, dept_id, salary)
VALUES ('Bob', 2, 80000.00)
RETURNING id, name;
```

---

### UPDATE

Modifies existing rows in a table.

**Syntax:**

```sql
UPDATE table_name
    SET column = expr [, column = expr ...]
    [WHERE condition]
    [RETURNING select_list];
```

- Without a `WHERE` clause, all rows in the table are updated.
- `RETURNING` returns the updated rows.

**Examples:**

```sql
-- Update matching rows
UPDATE employees SET salary = salary * 1.10 WHERE dept_id = 3;

-- Update multiple columns
UPDATE products
SET price = price * 0.90, updated_at = now()
WHERE category = 'clearance';

-- With RETURNING
UPDATE employees
SET salary = salary + 5000
WHERE id = 42
RETURNING id, name, salary;
```

---

### DELETE

Removes rows from a table.

**Syntax:**

```sql
DELETE FROM table_name
    [WHERE condition]
    [RETURNING select_list];
```

- Without a `WHERE` clause, all rows in the table are deleted.
- `RETURNING` returns the deleted rows.

**Examples:**

```sql
DELETE FROM sessions WHERE expires_at < now();

-- Delete all rows
DELETE FROM temp_results;

-- With RETURNING
DELETE FROM employees WHERE id = 99 RETURNING *;
```

---

## Transaction Control

### BEGIN

Starts a new transaction.

```sql
BEGIN;
```

### COMMIT

Commits the current transaction, making all changes permanent.

```sql
COMMIT;
```

### SAVEPOINT

Creates a named savepoint within the current transaction.

```sql
SAVEPOINT savepoint_name;
```

### ROLLBACK

Rolls back the current transaction. Optionally rolls back only to a named savepoint.

```sql
-- Rollback entire transaction
ROLLBACK;

-- Rollback to savepoint (keeps the transaction open)
ROLLBACK TO savepoint_name;
```

**Example:**

```sql
BEGIN;

INSERT INTO accounts (name, balance) VALUES ('Alice', 1000);
SAVEPOINT after_alice;

INSERT INTO accounts (name, balance) VALUES ('Bob', -500);
-- Oops, negative balance. Undo only Bob's insert.
ROLLBACK TO after_alice;

INSERT INTO accounts (name, balance) VALUES ('Bob', 500);
COMMIT;
```

---

## Set Operations

Set operations combine the results of two `SELECT` queries. The queries must produce the same number of columns with compatible types.

### UNION / UNION ALL

Returns all rows from both queries. `UNION` removes duplicates; `UNION ALL` preserves them.

```sql
SELECT name FROM employees
UNION
SELECT name FROM contractors;

SELECT name FROM employees
UNION ALL
SELECT name FROM contractors;
```

### INTERSECT / INTERSECT ALL

Returns only rows that appear in both queries.

```sql
SELECT product_id FROM orders_2023
INTERSECT
SELECT product_id FROM orders_2024;
```

### EXCEPT / EXCEPT ALL

Returns rows from the first query that do not appear in the second.

```sql
SELECT id FROM all_users
EXCEPT
SELECT user_id FROM banned_users;
```

**Chaining:** Set operations can be chained. They bind left-to-right.

```sql
SELECT id FROM a
UNION
SELECT id FROM b
EXCEPT
SELECT id FROM c;
```

---

## Utility Statements

### EXPLAIN / EXPLAIN ANALYZE

Displays the query execution plan. `ANALYZE` additionally executes the query and shows actual row counts and timing.

**Syntax:**

```sql
EXPLAIN statement;
EXPLAIN ANALYZE statement;
```

**Examples:**

```sql
EXPLAIN SELECT * FROM employees WHERE dept_id = 3;

EXPLAIN ANALYZE
SELECT e.name, d.name
FROM employees e
JOIN departments d ON e.dept_id = d.id
WHERE e.salary > 50000;
```

---

### SHOW TABLES

Lists all tables in the database.

```sql
SHOW TABLES;
```

---

### SHOW COLUMNS

Displays the column definitions for a table.

```sql
SHOW COLUMNS FROM table_name;
```

**Example:**

```sql
SHOW COLUMNS FROM employees;
```

---

### ANALYZE

Collects statistics for the query planner. When called with a table name, analyzes only that table. Without a table name, analyzes all tables.

```sql
ANALYZE;
ANALYZE employees;
```

---

### VACUUM

Reclaims storage occupied by dead tuples. Optionally targets a specific table.

```sql
VACUUM;
VACUUM orders;
```

---

### COPY

Bulk-loads data from or exports data to a file.

**Syntax:**

```sql
-- Import
COPY table_name [(column [, ...])] FROM 'file_path';

-- Export
COPY table_name [(column [, ...])] TO 'file_path';
```

**Examples:**

```sql
COPY employees FROM '/data/employees.csv';
COPY employees (name, salary) FROM '/data/partial.csv';
COPY orders TO '/backup/orders.csv';
```

---

## Statement Summary

| Category | Statement | Key Clauses |
|---|---|---|
| DDL | `CREATE TABLE` | `IF NOT EXISTS`, column & table constraints |
| DDL | `DROP TABLE` | `IF EXISTS`, `CASCADE` |
| DDL | `ALTER TABLE` | `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, `ADD CONSTRAINT`, `DROP CONSTRAINT` |
| DDL | `CREATE INDEX` | `UNIQUE`, `USING BTREE/HASH` |
| DDL | `DROP INDEX` | `IF EXISTS` |
| DDL | `CREATE VIEW` | Column renaming, AS query |
| DDL | `DROP VIEW` | `IF EXISTS` |
| DML | `SELECT` | `DISTINCT`, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET` |
| DML | `INSERT` | `VALUES`, `SELECT`, `RETURNING` |
| DML | `UPDATE` | `SET`, `WHERE`, `RETURNING` |
| DML | `DELETE` | `WHERE`, `RETURNING` |
| Transaction | `BEGIN` | |
| Transaction | `COMMIT` | |
| Transaction | `ROLLBACK` | `TO savepoint` |
| Transaction | `SAVEPOINT` | named |
| Set Ops | `UNION [ALL]` | |
| Set Ops | `INTERSECT [ALL]` | |
| Set Ops | `EXCEPT [ALL]` | |
| Utility | `EXPLAIN [ANALYZE]` | |
| Utility | `SHOW TABLES` | |
| Utility | `SHOW COLUMNS` | `FROM table` |
| Utility | `ANALYZE` | optional table |
| Utility | `VACUUM` | optional table |
| Utility | `COPY` | `FROM / TO` |
