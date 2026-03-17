# Functions

sqld provides 39 built-in scalar functions and 9 aggregate functions. This page documents every function with its signature, description, and examples.

---

## Scalar Functions

### String Functions (17)

#### length / char_length / character_length

Returns the number of characters in a string.

```sql
length(string) -> INTEGER
```

```sql
SELECT length('hello');          -- 5
SELECT char_length('cafe');      -- 4 (not byte count)
```

---

#### upper

Converts a string to uppercase.

```sql
upper(string) -> TEXT
```

```sql
SELECT upper('hello');           -- 'HELLO'
```

---

#### lower

Converts a string to lowercase.

```sql
lower(string) -> TEXT
```

```sql
SELECT lower('HELLO');           -- 'hello'
```

---

#### trim / btrim

Removes leading and trailing characters from a string. With one argument, trims whitespace. With two arguments, trims the characters in the second argument.

```sql
trim(string) -> TEXT
trim(string, characters) -> TEXT
```

```sql
SELECT trim('  hello  ');        -- 'hello'
SELECT trim('xxhelloxx', 'x');   -- 'hello'
SELECT btrim('  hi  ');          -- 'hi'
```

---

#### ltrim

Removes leading characters. Defaults to whitespace.

```sql
ltrim(string) -> TEXT
ltrim(string, characters) -> TEXT
```

```sql
SELECT ltrim('  hello');         -- 'hello'
SELECT ltrim('xxxhello', 'x');   -- 'hello'
```

---

#### rtrim

Removes trailing characters. Defaults to whitespace.

```sql
rtrim(string) -> TEXT
rtrim(string, characters) -> TEXT
```

```sql
SELECT rtrim('hello   ');        -- 'hello'
SELECT rtrim('helloyyy', 'y');   -- 'hello'
```

---

#### substring / substr

Extracts a portion of a string. Positions are 1-based.

```sql
substring(string, start) -> TEXT
substring(string, start, length) -> TEXT
```

```sql
SELECT substring('PostgreSQL', 1, 4);   -- 'Post'
SELECT substring('PostgreSQL', 8);       -- 'SQL'
SELECT substr('abcdef', 3, 2);           -- 'cd'
```

---

#### position / strpos

Returns the 1-based position of a substring within a string. Returns 0 if not found.

```sql
position(substring, string) -> INTEGER
```

```sql
SELECT position('lo', 'hello');          -- 4
SELECT strpos('wor', 'hello world');     -- 7
SELECT position('xyz', 'hello');         -- 0
```

---

#### replace

Replaces all occurrences of a substring with another string.

```sql
replace(string, from, to) -> TEXT
```

```sql
SELECT replace('hello world', 'world', 'sqld');   -- 'hello sqld'
SELECT replace('aabbcc', 'bb', 'XX');              -- 'aaXXcc'
```

---

#### concat

Concatenates any number of arguments into a single string. NULL arguments are skipped (not propagated).

```sql
concat(arg1, arg2, ...) -> TEXT
```

```sql
SELECT concat('Hello', ' ', 'World');    -- 'Hello World'
SELECT concat('id=', 42, ', name=', NULL, 'test');  -- 'id=42, name=test'
```

---

#### left

Returns the first `n` characters. If `n` is negative, returns all but the last `|n|` characters.

```sql
left(string, n) -> TEXT
```

```sql
SELECT left('hello', 3);        -- 'hel'
SELECT left('hello', -2);       -- 'hel'
```

---

#### right

Returns the last `n` characters. If `n` is negative, returns all but the first `|n|` characters.

```sql
right(string, n) -> TEXT
```

```sql
SELECT right('hello', 3);       -- 'llo'
SELECT right('hello', -2);      -- 'llo'
```

---

#### reverse

Reverses the characters in a string.

```sql
reverse(string) -> TEXT
```

```sql
SELECT reverse('hello');         -- 'olleh'
```

---

#### lpad

Pads a string on the left to a given length. The fill string defaults to a space.

```sql
lpad(string, length) -> TEXT
lpad(string, length, fill) -> TEXT
```

```sql
SELECT lpad('42', 5);            -- '   42'
SELECT lpad('42', 5, '0');       -- '00042'
SELECT lpad('hello', 3);         -- 'hel'  (truncated if longer)
```

---

#### rpad

Pads a string on the right to a given length. The fill string defaults to a space.

```sql
rpad(string, length) -> TEXT
rpad(string, length, fill) -> TEXT
```

```sql
SELECT rpad('hi', 5);            -- 'hi   '
SELECT rpad('hi', 5, '.');       -- 'hi...'
```

---

#### repeat

Repeats a string a given number of times.

```sql
repeat(string, count) -> TEXT
```

```sql
SELECT repeat('ab', 3);          -- 'ababab'
SELECT repeat('-', 20);          -- '--------------------'
```

---

#### split_part

Splits a string by a delimiter and returns the field at the given 1-based index. Returns an empty string if the index exceeds the number of parts.

```sql
split_part(string, delimiter, field) -> TEXT
```

```sql
SELECT split_part('a,b,c', ',', 2);      -- 'b'
SELECT split_part('hello world', ' ', 1); -- 'hello'
SELECT split_part('a.b.c', '.', 5);       -- ''
```

---

### Math Functions (13)

#### abs

Returns the absolute value.

```sql
abs(x) -> NUMERIC
```

```sql
SELECT abs(-42);                 -- 42
SELECT abs(-3.14);               -- 3.14
```

---

#### ceil / ceiling

Returns the smallest integer not less than the argument.

```sql
ceil(x) -> FLOAT
```

```sql
SELECT ceil(4.2);                -- 5.0
SELECT ceiling(-4.8);            -- -4.0
```

---

#### floor

Returns the largest integer not greater than the argument.

```sql
floor(x) -> FLOAT
```

```sql
SELECT floor(4.8);               -- 4.0
SELECT floor(-4.2);              -- -5.0
```

---

#### round

Rounds to the nearest integer, or to `d` decimal places if a second argument is given.

```sql
round(x) -> FLOAT
round(x, d) -> FLOAT
```

```sql
SELECT round(4.5);               -- 5.0
SELECT round(3.14159, 2);        -- 3.14
```

---

#### trunc / truncate

Truncates toward zero, optionally to `d` decimal places.

```sql
trunc(x) -> FLOAT
trunc(x, d) -> FLOAT
```

```sql
SELECT trunc(4.9);               -- 4.0
SELECT truncate(3.14159, 3);     -- 3.141
```

---

#### sqrt

Returns the square root.

```sql
sqrt(x) -> FLOAT
```

```sql
SELECT sqrt(144);                -- 12.0
SELECT sqrt(2);                  -- 1.4142135623730951
```

---

#### power / pow

Returns `base` raised to the power of `exp`.

```sql
power(base, exp) -> FLOAT
```

```sql
SELECT power(2, 10);             -- 1024.0
SELECT pow(3, 0.5);              -- 1.7320508075688772
```

---

#### mod

Returns the remainder of `a / b`. Raises an error on division by zero.

```sql
mod(a, b) -> FLOAT
```

```sql
SELECT mod(17, 5);               -- 2.0
SELECT mod(10.5, 3);             -- 1.5
```

---

#### ln

Returns the natural logarithm (base e).

```sql
ln(x) -> FLOAT
```

```sql
SELECT ln(1);                    -- 0.0
SELECT ln(2.718281828);          -- ~1.0
```

---

#### log / log10

With one argument, returns the base-10 logarithm. With two arguments, returns the logarithm of the second argument in the base of the first.

```sql
log(x) -> FLOAT
log(base, x) -> FLOAT
```

```sql
SELECT log(100);                 -- 2.0
SELECT log10(1000);              -- 3.0
SELECT log(2, 8);                -- 3.0
```

---

#### exp

Returns Euler's number (e) raised to the given power.

```sql
exp(x) -> FLOAT
```

```sql
SELECT exp(1);                   -- 2.718281828459045
SELECT exp(0);                   -- 1.0
```

---

#### sign

Returns -1.0, 0.0, or 1.0 depending on the sign of the argument.

```sql
sign(x) -> FLOAT
```

```sql
SELECT sign(-42);                -- -1.0
SELECT sign(0);                  -- 0.0
SELECT sign(100);                -- 1.0
```

---

#### random

Returns a pseudo-random floating-point value in the range [0.0, 1.0). Uses an internal xorshift64 PRNG (not cryptographically secure).

```sql
random() -> FLOAT
```

```sql
SELECT random();                 -- e.g., 0.7291038475...
SELECT floor(random() * 100);   -- random integer 0-99
```

---

### Date/Time Functions (5)

#### now / current_timestamp

Returns the current date and time as a TIMESTAMP (microseconds since epoch, UTC).

```sql
now() -> TIMESTAMP
```

```sql
SELECT now();
SELECT current_timestamp;
INSERT INTO events (created_at) VALUES (now());
```

---

#### extract

Extracts a field from a date or timestamp value. Returns FLOAT.

```sql
extract(field, temporal_value) -> FLOAT
```

Supported fields: `year`, `month`, `day`, `hour`, `minute`, `second`, `epoch`, `dow` / `dayofweek` (0 = Sunday), `doy` / `dayofyear`.

```sql
SELECT extract('year', now());       -- e.g., 2024.0
SELECT extract('month', created_at) FROM events;
SELECT extract('epoch', now());      -- seconds since epoch (with decimals)
SELECT extract('dow', '2024-01-01'::DATE);  -- day of week
```

---

#### date_trunc

Truncates a timestamp to the specified precision.

```sql
date_trunc(field, temporal_value) -> TIMESTAMP
```

Supported fields: `year`, `month`, `day`, `hour`, `minute`, `second`.

```sql
SELECT date_trunc('month', now());   -- first instant of current month
SELECT date_trunc('hour', created_at) FROM events;
SELECT date_trunc('day', '2024-06-15 14:30:00'::TIMESTAMP);
```

---

#### age

Returns the difference between two temporal values as microseconds (BIGINT). With one argument, computes the difference from now.

```sql
age(temporal) -> BIGINT
age(temporal_a, temporal_b) -> BIGINT
```

```sql
SELECT age(created_at) FROM users;         -- microseconds since created_at
SELECT age(end_time, start_time) FROM jobs; -- duration in microseconds
```

---

#### to_char

Formats a date or timestamp as a string using a format pattern.

```sql
to_char(temporal_value, format) -> TEXT
```

Supported format tokens: `YYYY`, `MM`, `DD`, `HH24`, `HH`, `MI`, `SS`.

```sql
SELECT to_char(now(), 'YYYY-MM-DD');            -- '2024-06-15'
SELECT to_char(now(), 'YYYY-MM-DD HH24:MI:SS'); -- '2024-06-15 14:30:00'
SELECT to_char(hire_date, 'MM/DD/YYYY') FROM employees;
```

---

### Type Functions (2)

#### cast

Converts a value to a target type. This function form is rarely used directly; prefer the `CAST(expr AS type)` or `expr::type` syntax instead.

```sql
cast(value, type_name) -> target_type
```

See [Data Types: Explicit Casting](data-types.md#explicit-casting-cast) for full details.

---

#### typeof / pg_typeof

Returns the SQL type name of a value as a string.

```sql
typeof(value) -> TEXT
```

```sql
SELECT typeof(42);                 -- 'INTEGER'
SELECT pg_typeof(3.14);            -- 'FLOAT'
SELECT typeof(NULL);               -- 'NULL'
SELECT typeof(name) FROM users;    -- 'TEXT' or 'VARCHAR'
```

---

### Null-Handling Functions (2)

#### coalesce

Returns the first non-NULL argument. Accepts any number of arguments. If all arguments are NULL, returns NULL.

```sql
coalesce(arg1, arg2, ...) -> any
```

```sql
SELECT coalesce(NULL, NULL, 42);           -- 42
SELECT coalesce(nickname, name, 'N/A');    -- first non-null
```

> **Note:** `COALESCE` is also recognized as a special expression form in the parser (not just a function call), so it bypasses the normal function dispatch and evaluates arguments lazily.

---

#### nullif

Returns NULL if the two arguments are equal; otherwise returns the first argument.

```sql
nullif(a, b) -> any
```

```sql
SELECT nullif(5, 5);                       -- NULL
SELECT nullif(5, 3);                       -- 5
SELECT revenue / nullif(quantity, 0);      -- avoids division by zero
```

> **Note:** Like `COALESCE`, `NULLIF` is parsed as a dedicated expression node.

---

## Aggregate Functions

Aggregate functions compute a single result from a set of input rows. They are used with `GROUP BY` or operate over the entire result set when no grouping is specified.

All aggregate functions (except `COUNT(*)`) skip NULL input values. All aggregates support `DISTINCT` to operate only on unique values.

### Summary Table

| Function | Return Type | Description |
|---|---|---|
| `COUNT(expr)` | `BIGINT` | Number of non-NULL values |
| `COUNT(*)` | `BIGINT` | Number of rows |
| `SUM(expr)` | Widened numeric | Sum of non-NULL values |
| `AVG(expr)` | `FLOAT` | Arithmetic mean of non-NULL values |
| `MIN(expr)` | Same as input | Minimum non-NULL value |
| `MAX(expr)` | Same as input | Maximum non-NULL value |
| `STRING_AGG(expr, delimiter)` | `TEXT` | Concatenation with delimiter |
| `ARRAY_AGG(expr)` | `TEXT` | All values as text (serialized) |
| `BOOL_AND(expr)` / `EVERY(expr)` | `BOOLEAN` | TRUE if all values are true |
| `BOOL_OR(expr)` | `BOOLEAN` | TRUE if any value is true |

---

### COUNT

Counts non-NULL values, or counts all rows when used as `COUNT(*)`.

```sql
SELECT COUNT(*) FROM employees;                    -- total rows
SELECT COUNT(email) FROM employees;                -- rows with non-NULL email
SELECT COUNT(DISTINCT dept_id) FROM employees;     -- unique departments
```

---

### SUM

Computes the sum of numeric values. The return type is widened: `INTEGER` inputs produce `BIGINT`, `BIGINT` stays `BIGINT`, `FLOAT` stays `FLOAT`, `DECIMAL` stays `DECIMAL`.

```sql
SELECT SUM(salary) FROM employees;
SELECT dept_id, SUM(salary) FROM employees GROUP BY dept_id;
SELECT SUM(DISTINCT quantity) FROM order_items;
```

---

### AVG

Computes the arithmetic mean. Always returns `FLOAT`.

```sql
SELECT AVG(salary) FROM employees;
SELECT dept_id, AVG(salary) AS avg_sal FROM employees GROUP BY dept_id;
```

---

### MIN / MAX

Returns the minimum or maximum non-NULL value. The return type matches the input type. Works on any comparable type (numeric, string, temporal).

```sql
SELECT MIN(salary), MAX(salary) FROM employees;
SELECT MIN(hire_date), MAX(hire_date) FROM employees;
SELECT MIN(name) FROM employees;  -- lexicographically smallest
```

---

### STRING_AGG

Concatenates non-NULL string values with a delimiter.

```sql
SELECT STRING_AGG(name, ', ') FROM employees WHERE dept_id = 1;
-- Result: 'Alice, Bob, Charlie'

SELECT dept_id, STRING_AGG(DISTINCT name, '; ')
FROM employees
GROUP BY dept_id;
```

---

### ARRAY_AGG

Collects all non-NULL values into a text-serialized array representation.

```sql
SELECT ARRAY_AGG(name) FROM employees WHERE dept_id = 1;
-- Result: '{Alice,Bob,Charlie}'

SELECT dept_id, ARRAY_AGG(DISTINCT name) FROM employees GROUP BY dept_id;
```

---

### BOOL_AND / EVERY

Returns `TRUE` if all non-NULL input values are true. `EVERY` is an alias for `BOOL_AND`.

```sql
SELECT BOOL_AND(active) FROM employees;
SELECT dept_id, EVERY(active) FROM employees GROUP BY dept_id;
```

---

### BOOL_OR

Returns `TRUE` if any non-NULL input value is true.

```sql
SELECT BOOL_OR(is_admin) FROM users;
SELECT dept_id, BOOL_OR(is_manager) FROM employees GROUP BY dept_id;
```

---

### DISTINCT with Aggregates

Any aggregate function can use `DISTINCT` to consider only unique input values.

```sql
SELECT COUNT(DISTINCT category) FROM products;
SELECT SUM(DISTINCT price) FROM products;
SELECT STRING_AGG(DISTINCT status, ', ') FROM orders;
```

---

## Function Reference (Quick Lookup)

| Category | Functions |
|---|---|
| String (17) | `length`, `char_length`, `upper`, `lower`, `trim`, `btrim`, `ltrim`, `rtrim`, `substring`, `substr`, `position`, `strpos`, `replace`, `concat`, `left`, `right`, `reverse`, `lpad`, `rpad`, `repeat`, `split_part` |
| Math (13) | `abs`, `ceil`, `ceiling`, `floor`, `round`, `trunc`, `truncate`, `sqrt`, `power`, `pow`, `mod`, `ln`, `log`, `log10`, `exp`, `sign`, `random` |
| Date/Time (5) | `now`, `current_timestamp`, `extract`, `date_trunc`, `age`, `to_char` |
| Type (2) | `cast`, `typeof`, `pg_typeof` |
| Null (2) | `coalesce`, `nullif` |
| Aggregates (9) | `count`, `sum`, `avg`, `min`, `max`, `string_agg`, `array_agg`, `bool_and` / `every`, `bool_or` |
