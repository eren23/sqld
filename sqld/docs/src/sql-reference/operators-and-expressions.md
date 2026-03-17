# Operators and Expressions

This page documents every operator and expression form supported by sqld, including the Pratt-parser-derived operator precedence table.

---

## Arithmetic Operators

| Operator | Description | Example | Result |
|---|---|---|---|
| `+` | Addition | `3 + 4` | `7` |
| `-` | Subtraction | `10 - 3` | `7` |
| `*` | Multiplication | `5 * 6` | `30` |
| `/` | Division (integer division for integer operands) | `7 / 2` | `3` |
| `%` | Modulo (remainder) | `7 % 3` | `1` |
| `^` | Exponentiation | `2 ^ 10` | `1024.0` |
| `\|\|` | String concatenation | `'hello' \|\| ' world'` | `'hello world'` |

**Notes:**
- Division by zero raises an error (never returns infinity or NaN).
- When operands have different numeric types, they are coerced to their common super-type before the operation (see [Data Types: Coercion](data-types.md#type-coercion)).
- The `||` operator coerces both operands to strings and returns `TEXT`.
- Unary `+` and `-` are also supported as prefix operators: `-x`, `+x`.

---

## Comparison Operators

| Operator | Description | Example |
|---|---|---|
| `=` | Equal | `x = 5` |
| `!=` or `<>` | Not equal | `x != 0` or `x <> 0` |
| `<` | Less than | `x < 10` |
| `>` | Greater than | `x > 0` |
| `<=` | Less than or equal | `x <= 100` |
| `>=` | Greater than or equal | `x >= 1` |

All comparison operators return `BOOLEAN`. When comparing values of different types, implicit coercion is applied (e.g., comparing `INTEGER` with `FLOAT` promotes the integer to float). Comparing anything with `NULL` yields `NULL` (three-valued logic).

---

## Logical Operators

| Operator | Description | Example |
|---|---|---|
| `AND` | Logical conjunction | `a > 0 AND b > 0` |
| `OR` | Logical disjunction | `a = 1 OR b = 1` |
| `NOT` | Logical negation (prefix) | `NOT active` |

**Three-valued logic:** `AND` and `OR` follow SQL's three-valued logic rules with `NULL`:

| `a` | `b` | `a AND b` | `a OR b` |
|---|---|---|---|
| TRUE | TRUE | TRUE | TRUE |
| TRUE | FALSE | FALSE | TRUE |
| TRUE | NULL | NULL | TRUE |
| FALSE | FALSE | FALSE | FALSE |
| FALSE | NULL | FALSE | NULL |
| NULL | NULL | NULL | NULL |

---

## Special Comparison Forms

### IS [NOT] NULL

Tests whether a value is or is not `NULL`. Unlike `= NULL`, this always returns `TRUE` or `FALSE`, never `NULL`.

```sql
SELECT * FROM employees WHERE manager_id IS NULL;
SELECT * FROM employees WHERE email IS NOT NULL;
```

### IN (list)

Tests whether a value matches any value in a list of expressions.

```sql
SELECT * FROM products WHERE category IN ('electronics', 'books', 'toys');
SELECT * FROM orders WHERE status IN (1, 2, 3);
```

The negated form `NOT IN` returns true when the value does not match any element.

```sql
SELECT * FROM employees WHERE dept_id NOT IN (5, 10);
```

### IN (subquery)

Tests whether a value matches any row returned by a subquery.

```sql
SELECT * FROM employees
WHERE dept_id IN (SELECT id FROM departments WHERE active = true);

SELECT * FROM products
WHERE id NOT IN (SELECT product_id FROM discontinued);
```

### BETWEEN

Tests whether a value falls within an inclusive range.

```sql
SELECT * FROM orders WHERE total BETWEEN 100 AND 500;
SELECT * FROM events WHERE event_date NOT BETWEEN '2024-01-01' AND '2024-12-31';
```

`BETWEEN` is equivalent to `expr >= low AND expr <= high`.

### LIKE / ILIKE

Pattern matching on strings. `LIKE` is case-sensitive; `ILIKE` is case-insensitive.

- `%` matches zero or more characters.
- `_` matches exactly one character.

```sql
SELECT * FROM users WHERE name LIKE 'A%';         -- starts with 'A'
SELECT * FROM users WHERE email LIKE '%@gmail.com';
SELECT * FROM users WHERE name ILIKE '%smith%';    -- case-insensitive
SELECT * FROM users WHERE code NOT LIKE 'TEMP%';
```

### EXISTS / NOT EXISTS

Tests whether a subquery returns any rows.

```sql
SELECT * FROM departments d
WHERE EXISTS (
    SELECT 1 FROM employees e WHERE e.dept_id = d.id
);

SELECT * FROM products p
WHERE NOT EXISTS (
    SELECT 1 FROM order_items oi WHERE oi.product_id = p.id
);
```

---

## CASE Expressions

CASE provides conditional logic within expressions.

### Searched CASE

Each `WHEN` clause contains an independent boolean condition.

```sql
SELECT name,
    CASE
        WHEN salary > 100000 THEN 'high'
        WHEN salary > 50000  THEN 'medium'
        ELSE 'low'
    END AS salary_band
FROM employees;
```

### Simple CASE

Compares a single operand against each `WHEN` value.

```sql
SELECT order_id,
    CASE status
        WHEN 1 THEN 'pending'
        WHEN 2 THEN 'shipped'
        WHEN 3 THEN 'delivered'
        ELSE 'unknown'
    END AS status_name
FROM orders;
```

The `ELSE` clause is optional. When omitted and no `WHEN` matches, the result is `NULL`.

---

## Conditional Functions

### COALESCE

Returns the first non-NULL argument. Accepts any number of arguments.

```sql
SELECT COALESCE(nickname, first_name, 'Anonymous') AS display_name
FROM users;
```

### NULLIF

Returns `NULL` if the two arguments are equal; otherwise returns the first argument.

```sql
-- Avoid division by zero: returns NULL instead of error
SELECT revenue / NULLIF(units_sold, 0) AS price_per_unit
FROM products;
```

### GREATEST

Returns the largest value among its arguments (NULLs are skipped).

```sql
SELECT GREATEST(a, b, c) AS max_val FROM measurements;
```

### LEAST

Returns the smallest value among its arguments (NULLs are skipped).

```sql
SELECT LEAST(price, max_price) AS effective_price FROM products;
```

---

## Placeholder Expressions

Placeholder parameters use the `$n` syntax (1-based) for prepared statements and parameterized queries.

```sql
SELECT * FROM employees WHERE dept_id = $1 AND salary > $2;
INSERT INTO logs (message, level) VALUES ($1, $2);
```

---

## Subquery Expressions

A parenthesized SELECT can appear anywhere an expression is expected. Scalar subqueries (returning exactly one row and one column) can be used in the select list or in comparisons.

```sql
SELECT name,
    (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) AS order_count
FROM customers c;

SELECT * FROM products
WHERE price > (SELECT AVG(price) FROM products);
```

---

## Operator Precedence

sqld's parser uses a Pratt parser with 14 binding-power levels. Operators with higher precedence bind more tightly. The table below lists all levels from lowest to highest.

| Level | Binding Power | Operators / Forms | Associativity |
|---|---|---|---|
| 1 | 10 | `OR` | Left |
| 2 | 20 | `AND` | Left |
| 3 | 30 | `NOT` (prefix) | Right (prefix) |
| 4 | 40 | `IS [NOT] NULL` | Postfix |
| 5 | 50 | `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=` | Left |
| 6 | 60 | `BETWEEN`, `IN`, `LIKE`, `ILIKE` | Non-assoc |
| 7 | 70 | `\|\|` (concatenation) | Left |
| 8 | 80 | `+`, `-` (binary) | Left |
| 9 | 90 | `*`, `/`, `%` | Left |
| 10 | 100 | `^` (exponentiation) | Right |
| 11 | 110 | `+`, `-` (unary prefix) | Right (prefix) |
| 12 | 120 | `::` (cast) | Left |
| 13 | 130 | `.` (qualified identifier) | Left |
| 14 | 140 | Function call `()` | Postfix |

### Precedence Examples

```sql
-- Multiplication before addition
SELECT 2 + 3 * 4;           -- 14, not 20

-- AND before OR
SELECT * FROM t WHERE a = 1 OR b = 2 AND c = 3;
-- Equivalent to: a = 1 OR (b = 2 AND c = 3)

-- Exponentiation before multiplication
SELECT 2 * 3 ^ 2;           -- 18 (2 * 9), not 36

-- Cast binds tightly
SELECT '42'::INTEGER + 1;   -- 43

-- NOT before AND
SELECT * FROM t WHERE NOT a AND b;
-- Equivalent to: (NOT a) AND b

-- Concatenation between comparison and addition
SELECT 'id:' || id + 1 FROM t;
-- Parsed as: 'id:' || (id + 1)  (addition before concat)
```

---

## Expression Grammar Summary

```
expression   = or_expr
or_expr      = and_expr ( OR and_expr )*
and_expr     = not_expr ( AND not_expr )*
not_expr     = NOT not_expr | is_expr
is_expr      = cmp_expr [ IS [NOT] NULL ]
cmp_expr     = range_expr ( ( = | != | <> | < | > | <= | >= ) range_expr )?
range_expr   = concat_expr [ [NOT] BETWEEN concat_expr AND concat_expr ]
             | concat_expr [ [NOT] IN ( expr_list | subquery ) ]
             | concat_expr [ [NOT] LIKE concat_expr ]
             | concat_expr [ [NOT] ILIKE concat_expr ]
concat_expr  = add_expr ( || add_expr )*
add_expr     = mul_expr ( ( + | - ) mul_expr )*
mul_expr     = exp_expr ( ( * | / | % ) exp_expr )*
exp_expr     = unary_expr ( ^ unary_expr )*
unary_expr   = ( + | - ) unary_expr | cast_expr
cast_expr    = primary ( :: data_type )*
primary      = literal | identifier | table.column | function(args)
             | ( expression ) | ( SELECT ... )
             | CASE ... END | COALESCE(...) | NULLIF(...) | GREATEST(...) | LEAST(...)
             | EXISTS ( SELECT ... ) | $n
```
