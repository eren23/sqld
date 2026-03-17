# Joins

sqld supports 7 join types, NATURAL joins, and two join-condition forms (`ON` and `USING`). This page documents each join type with syntax, semantics, and illustrative examples.

---

## Sample Data

All examples on this page use the following two tables:

**employees**

| id | name | dept_id |
|---|---|---|
| 1 | Alice | 10 |
| 2 | Bob | 20 |
| 3 | Charlie | 10 |
| 4 | Diana | NULL |

**departments**

| id | name |
|---|---|
| 10 | Engineering |
| 20 | Marketing |
| 30 | Finance |

---

## Join Types

### INNER JOIN

Returns only rows that have a match in both tables.

**Syntax:**

```sql
SELECT ...
FROM left_table
[INNER] JOIN right_table ON condition;
```

The keyword `INNER` is optional; `JOIN` alone means `INNER JOIN`.

**Example:**

```sql
SELECT e.name, d.name AS department
FROM employees e
JOIN departments d ON e.dept_id = d.id;
```

| name | department |
|---|---|
| Alice | Engineering |
| Bob | Marketing |
| Charlie | Engineering |

Diana is excluded because her `dept_id` is NULL and does not match any department.

---

### LEFT [OUTER] JOIN

Returns all rows from the left table, plus matched rows from the right table. Where no match exists, the right-side columns are filled with NULLs.

**Syntax:**

```sql
SELECT ...
FROM left_table
LEFT [OUTER] JOIN right_table ON condition;
```

The keyword `OUTER` is optional.

**Example:**

```sql
SELECT e.name, d.name AS department
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.id;
```

| name | department |
|---|---|
| Alice | Engineering |
| Bob | Marketing |
| Charlie | Engineering |
| Diana | NULL |

Diana appears with a NULL department because no match was found.

---

### RIGHT [OUTER] JOIN

Returns all rows from the right table, plus matched rows from the left table. Where no match exists, the left-side columns are filled with NULLs.

**Syntax:**

```sql
SELECT ...
FROM left_table
RIGHT [OUTER] JOIN right_table ON condition;
```

**Example:**

```sql
SELECT e.name, d.name AS department
FROM employees e
RIGHT JOIN departments d ON e.dept_id = d.id;
```

| name | department |
|---|---|
| Alice | Engineering |
| Charlie | Engineering |
| Bob | Marketing |
| NULL | Finance |

The Finance department appears with NULL employee fields because no employee belongs to it.

---

### FULL [OUTER] JOIN

Returns all rows from both tables. Where no match exists on either side, the missing columns are filled with NULLs. This is the union of LEFT JOIN and RIGHT JOIN results.

**Syntax:**

```sql
SELECT ...
FROM left_table
FULL [OUTER] JOIN right_table ON condition;
```

**Example:**

```sql
SELECT e.name, d.name AS department
FROM employees e
FULL JOIN departments d ON e.dept_id = d.id;
```

| name | department |
|---|---|
| Alice | Engineering |
| Bob | Marketing |
| Charlie | Engineering |
| Diana | NULL |
| NULL | Finance |

Both Diana (no matching department) and Finance (no matching employee) appear.

---

### CROSS JOIN

Returns the Cartesian product of the two tables -- every combination of rows. No join condition is used.

**Syntax:**

```sql
SELECT ...
FROM left_table
CROSS JOIN right_table;
```

**Example:**

```sql
SELECT e.name, d.name AS department
FROM employees e
CROSS JOIN departments d;
```

Returns 4 x 3 = 12 rows (every employee paired with every department).

| name | department |
|---|---|
| Alice | Engineering |
| Alice | Marketing |
| Alice | Finance |
| Bob | Engineering |
| Bob | Marketing |
| Bob | Finance |
| Charlie | Engineering |
| Charlie | Marketing |
| Charlie | Finance |
| Diana | Engineering |
| Diana | Marketing |
| Diana | Finance |

---

### LEFT SEMI JOIN

Returns rows from the left table that have at least one match in the right table. Only left-side columns are returned. Unlike INNER JOIN, each left row appears at most once regardless of how many right-side matches exist.

**Syntax:**

```sql
SELECT ...
FROM left_table
LEFT SEMI JOIN right_table ON condition;
```

**Example:**

```sql
SELECT e.name
FROM employees e
LEFT SEMI JOIN departments d ON e.dept_id = d.id;
```

| name |
|---|
| Alice |
| Bob |
| Charlie |

Diana is excluded (no matching department). This is semantically equivalent to `WHERE EXISTS (SELECT 1 FROM departments d WHERE d.id = e.dept_id)`.

---

### LEFT ANTI JOIN

Returns rows from the left table that have no match in the right table. Only left-side columns are returned. This is the inverse of LEFT SEMI JOIN.

**Syntax:**

```sql
SELECT ...
FROM left_table
LEFT ANTI JOIN right_table ON condition;
```

**Example:**

```sql
SELECT e.name
FROM employees e
LEFT ANTI JOIN departments d ON e.dept_id = d.id;
```

| name |
|---|
| Diana |

Only Diana is returned because she has no matching department. This is semantically equivalent to `WHERE NOT EXISTS (SELECT 1 FROM departments d WHERE d.id = e.dept_id)`.

---

## NATURAL Joins

A `NATURAL` join automatically matches on all columns that share the same name in both tables. No explicit `ON` or `USING` clause is needed.

**Syntax:**

```sql
SELECT ...
FROM left_table
NATURAL [INNER | LEFT | RIGHT | FULL] JOIN right_table;
```

Any join type can be combined with `NATURAL`.

**Example:**

Suppose both tables have a column named `id`:

```sql
-- Automatically joins ON employees.id = departments.id
-- (This is usually NOT what you want with these example tables!)
SELECT *
FROM employees
NATURAL JOIN departments;
```

For tables that share a foreign-key column name:

```sql
-- If employees had a column named 'dept_id' and departments also had 'dept_id'
SELECT *
FROM employees
NATURAL LEFT JOIN departments;
```

> **Caution:** NATURAL joins are convenient but brittle. If a column is later added to one of the tables with the same name as a column in the other table, the join condition silently changes. Explicit `ON` or `USING` is generally preferred.

---

## Join Conditions

### ON

Specifies an arbitrary boolean expression as the join condition. The expression can reference columns from both tables.

```sql
SELECT *
FROM employees e
JOIN departments d ON e.dept_id = d.id;

-- Complex condition
SELECT *
FROM orders o
JOIN customers c ON o.customer_id = c.id AND o.region = c.region;
```

### USING

Specifies a list of column names that must exist in both tables. The join matches rows where these columns have equal values. Each named column appears only once in the output.

```sql
SELECT *
FROM employees
JOIN departments USING (dept_id);

-- Multiple columns
SELECT *
FROM order_items
JOIN inventory USING (product_id, warehouse_id);
```

### No Condition (CROSS JOIN)

`CROSS JOIN` does not accept `ON` or `USING` -- it always produces the Cartesian product.

---

## Multiple Joins

Joins can be chained. Each join adds another table to the result set. Joins are evaluated left to right.

```sql
SELECT e.name, d.name AS dept, o.name AS office
FROM employees e
JOIN departments d ON e.dept_id = d.id
LEFT JOIN offices o ON d.office_id = o.id
WHERE e.active = true
ORDER BY e.name;
```

---

## Subqueries as Join Targets

Either side of a join can be a subquery (with a required alias).

```sql
SELECT e.name, sub.total_orders
FROM employees e
LEFT JOIN (
    SELECT salesperson_id, COUNT(*) AS total_orders
    FROM orders
    GROUP BY salesperson_id
) AS sub ON e.id = sub.salesperson_id;
```

---

## Executor Implementations

sqld implements three physical join algorithms. The query optimizer selects the appropriate algorithm based on the join condition and the availability of sorted inputs.

| Algorithm | When Used | Strengths |
|---|---|---|
| **Hash Join** | Equi-join conditions (equality on one or more keys). Default for most joins. | Fast for large tables; O(N+M) average. Supports disk spill when the hash table exceeds `work_mem`. |
| **Sort-Merge Join** | When both inputs are already sorted on the join keys, or when the optimizer decides sorting is cheaper than hashing. | Efficient for pre-sorted data and range joins. Produces sorted output. |
| **Nested-Loop Join** | General-purpose fallback. Used for non-equi conditions, CROSS joins, or when one side is very small. | Supports all join types and arbitrary conditions. O(N*M) complexity. |

All three executors support every join type: INNER, LEFT, RIGHT, FULL, CROSS, LEFT SEMI, and LEFT ANTI.

---

## Join Type Summary

| SQL Syntax | Enum Variant | Unmatched Left Rows | Unmatched Right Rows |
|---|---|---|---|
| `[INNER] JOIN` | `Inner` | Excluded | Excluded |
| `LEFT [OUTER] JOIN` | `Left` | Included (NULLs for right) | Excluded |
| `RIGHT [OUTER] JOIN` | `Right` | Excluded | Included (NULLs for left) |
| `FULL [OUTER] JOIN` | `Full` | Included (NULLs for right) | Included (NULLs for left) |
| `CROSS JOIN` | `Cross` | N/A (all combinations) | N/A (all combinations) |
| `LEFT SEMI JOIN` | `LeftSemi` | Excluded | N/A (right cols not returned) |
| `LEFT ANTI JOIN` | `LeftAnti` | Included | N/A (right cols not returned) |
