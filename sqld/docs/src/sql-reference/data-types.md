# Data Types

sqld supports 10 SQL data types covering integers, floating point, exact decimals, booleans, strings, temporal values, and binary data. This page describes each type, its internal representation, storage characteristics, and the rules that govern implicit coercion and explicit casting.

---

## Type Summary

| SQL Type | Rust Representation | Fixed Size | Category |
|---|---|---|---|
| `INTEGER` | `i32` | 4 bytes | Numeric |
| `BIGINT` | `i64` | 8 bytes | Numeric |
| `FLOAT` | `f64` (IEEE 754) | 8 bytes | Numeric |
| `DECIMAL(p, s)` | `i128` mantissa + `u8` scale | 17 bytes | Numeric |
| `BOOLEAN` | `bool` | 1 byte | Boolean |
| `VARCHAR(n)` | `String` (max `n` bytes) | Variable | String |
| `TEXT` | `String` (unbounded) | Variable | String |
| `DATE` | `i32` (days since epoch) | 4 bytes | Temporal |
| `TIMESTAMP` | `i64` (microseconds since epoch, UTC) | 8 bytes | Temporal |
| `BLOB` | `Vec<u8>` | Variable | Binary |

---

## Numeric Types

### INTEGER

A 32-bit signed integer.

- **Range:** -2,147,483,648 to 2,147,483,647
- **Storage:** 4 bytes (little-endian)

```sql
CREATE TABLE counters (id INTEGER PRIMARY KEY, value INTEGER NOT NULL);
INSERT INTO counters VALUES (1, 42);
```

### BIGINT

A 64-bit signed integer.

- **Range:** -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807
- **Storage:** 8 bytes (little-endian)
- Integer literals in expressions are parsed as BIGINT by the planner.

```sql
CREATE TABLE large_ids (id BIGINT PRIMARY KEY);
INSERT INTO large_ids VALUES (9223372036854775807);
```

### FLOAT

A 64-bit IEEE 754 double-precision floating-point number.

- **Range:** approximately +/-1.8 x 10^308
- **Precision:** approximately 15-17 significant decimal digits
- **Storage:** 8 bytes (little-endian)
- Arithmetic follows IEEE 754 rules. Division by zero returns an error (not infinity).

```sql
SELECT 3.14159 * radius * radius AS area FROM circles;
```

### DECIMAL(p, s)

An exact numeric type with configurable precision and scale. Internally stored as a 128-bit integer mantissa with an associated scale.

- **Precision (p):** Total number of significant digits (up to 38).
- **Scale (s):** Number of digits to the right of the decimal point.
- **Storage:** 17 bytes (16 bytes for the `i128` mantissa + 1 byte for the scale).
- Arithmetic is exact (no floating-point rounding). Addition and subtraction use the larger scale of the two operands. Multiplication produces `scale_a + scale_b`. Division adds 6 extra precision digits.

```sql
CREATE TABLE transactions (
    id     INTEGER PRIMARY KEY,
    amount DECIMAL(10, 2) NOT NULL
);
INSERT INTO transactions VALUES (1, 99.95);
SELECT amount * 1.08 AS with_tax FROM transactions;
-- Result: DECIMAL with scale 2
```

---

## Boolean Type

### BOOLEAN

A logical true/false value.

- **Storage:** 1 byte (0 = false, non-zero = true)
- Accepted literals: `TRUE`, `FALSE`

```sql
CREATE TABLE flags (name VARCHAR(50), enabled BOOLEAN DEFAULT true);
SELECT * FROM flags WHERE enabled = true;
```

---

## String Types

### VARCHAR(n)

A variable-length character string with a maximum byte-length of `n`.

- **Storage:** 4-byte length prefix + string bytes (variable)
- Values exceeding `n` bytes produce an error on insertion.

```sql
CREATE TABLE users (username VARCHAR(64) NOT NULL UNIQUE);
```

### TEXT

An unbounded variable-length character string. Functionally equivalent to `VARCHAR` with no length limit.

- **Storage:** 4-byte length prefix + string bytes (variable)

```sql
CREATE TABLE articles (title VARCHAR(200), body TEXT);
```

---

## Temporal Types

### DATE

A calendar date (year, month, day) stored as the number of days since the Unix epoch (1970-01-01).

- **Range:** Roughly +/-5.8 million years
- **Storage:** 4 bytes (`i32`, little-endian)

```sql
CREATE TABLE events (name TEXT, event_date DATE);
INSERT INTO events VALUES ('Launch', '2024-06-15');
```

### TIMESTAMP

A date and time stored as microseconds since the Unix epoch (1970-01-01 00:00:00 UTC).

- **Precision:** microsecond
- **Storage:** 8 bytes (`i64`, little-endian)
- The `now()` / `current_timestamp` function returns the current wall-clock time as a TIMESTAMP.

```sql
CREATE TABLE audit_log (action TEXT, created_at TIMESTAMP DEFAULT now());
```

---

## Binary Type

### BLOB

An arbitrary sequence of bytes.

- **Storage:** 4-byte length prefix + raw bytes (variable)
- Displayed as `BLOB[n]` where `n` is the byte length.
- BLOBs support equality and ordering (lexicographic byte comparison) but not arithmetic or string operations.

```sql
CREATE TABLE files (name TEXT, content BLOB);
```

---

## Type Coercion

sqld uses an **implicit widening lattice** to automatically coerce values when two different types appear in the same expression (comparisons, arithmetic, `UNION`, etc.). The coercion always goes from a narrower type to a wider type -- never the reverse.

### Numeric Coercion Lattice

```
INTEGER --> BIGINT --> DECIMAL --> FLOAT
```

- `INTEGER` widens to `BIGINT`, `DECIMAL`, or `FLOAT`.
- `BIGINT` widens to `DECIMAL` or `FLOAT`.
- `DECIMAL` widens to `FLOAT`.
- `FLOAT` is the widest numeric type and cannot be implicitly narrowed.

When two `DECIMAL` values with different precision/scale are combined, the result uses the larger precision and the larger scale.

### String Coercion

```
VARCHAR(n) --> TEXT
```

Two `VARCHAR` values with different lengths coerce to `TEXT`. A `VARCHAR` and a `TEXT` coerce to `TEXT`.

### Temporal Coercion

```
DATE --> TIMESTAMP
```

A `DATE` value is coerced to `TIMESTAMP` by multiplying the day count by microseconds-per-day (midnight of that date).

### Incompatible Types

The following combinations cannot be implicitly coerced and produce a type error:

- Numeric and Boolean
- String and Numeric
- BLOB and any other type
- Boolean and String
- Temporal and Numeric

### Coercion in Practice

```sql
-- INTEGER + FLOAT --> FLOAT
SELECT 10 + 3.14;           -- Result: 13.14 (FLOAT)

-- INTEGER compared with BIGINT --> BIGINT
SELECT * FROM t WHERE int_col = bigint_col;

-- DATE compared with TIMESTAMP --> TIMESTAMP
SELECT * FROM events WHERE event_date < now();
```

---

## Explicit Casting (CAST)

sqld supports two syntaxes for explicit type conversion.

### CAST Function Syntax

```sql
CAST(expression AS target_type)
```

### PostgreSQL-Style Cast Syntax

```sql
expression::target_type
```

Both forms are semantically identical. The `::` operator has very high precedence (level 12 of 14 in the Pratt parser), so it binds tightly.

### Supported Casts

| From | To | Notes |
|---|---|---|
| `INTEGER` | `BIGINT` | Lossless widening |
| `INTEGER` | `FLOAT` | May lose precision for large values |
| `INTEGER` | `DECIMAL(p,s)` | Mantissa is scaled by 10^s |
| `INTEGER` | `TEXT` | String representation |
| `BIGINT` | `FLOAT` | May lose precision |
| `BIGINT` | `DECIMAL(p,s)` | Mantissa is scaled by 10^s |
| `FLOAT` | `INTEGER` | Truncates toward zero |
| `FLOAT` | `BIGINT` | Truncates toward zero |
| `TEXT` | `INTEGER` | Parses the string |
| `TEXT` | `BIGINT` | Parses the string |
| `TEXT` | `FLOAT` | Parses the string |
| `TEXT` | `BOOLEAN` | `'true'`/`'false'` |
| `BOOLEAN` | `TEXT` | `'true'`/`'false'` |
| `VARCHAR` | `TEXT` | Always succeeds |
| `DATE` | `TIMESTAMP` | Midnight of that date |

### Examples

```sql
SELECT CAST(42 AS FLOAT);           -- 42.0
SELECT CAST('123' AS INTEGER);      -- 123
SELECT 3.14::INTEGER;               -- 3
SELECT salary::TEXT FROM employees;  -- '75000.00'
SELECT '2024-01-15'::DATE;
```

---

## The `typeof` / `pg_typeof` Function

Returns the type name of a value at runtime.

```sql
SELECT typeof(42);          -- 'INTEGER'
SELECT pg_typeof(3.14);     -- 'FLOAT'
SELECT typeof(NULL);        -- 'NULL'
SELECT typeof(name) FROM employees LIMIT 1;  -- 'TEXT' or 'VARCHAR'
```
