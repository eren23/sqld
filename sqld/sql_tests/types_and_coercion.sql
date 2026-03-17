-- ============================================================
-- types_and_coercion.sql
-- Tests: Integer arithmetic, float arithmetic, boolean ops,
--        string functions (UPPER, LOWER, LENGTH, SUBSTRING,
--        TRIM), CAST, DECIMAL operations, date/timestamp
--        basics, and comparison operators across types.
-- ============================================================

-- Test: Integer arithmetic
SELECT 10 + 3;
SELECT 10 - 3;
SELECT 10 * 3;
SELECT 10 / 3;
SELECT 10 % 3;

-- Test: Float arithmetic
SELECT 10.5 + 2.3;
SELECT 10.5 - 2.3;
SELECT 10.5 * 2.0;
SELECT 10.5 / 2.0;

-- Test: Mixed integer/float arithmetic (coercion to float)
SELECT 10 + 2.5;
SELECT 7 / 2.0;

-- Test: Boolean literals and logic
SELECT true AND false;
SELECT true OR false;
SELECT NOT true;
SELECT true AND true;

-- Test: String functions
SELECT UPPER('hello');
SELECT LOWER('WORLD');
SELECT LENGTH('testing');
SELECT SUBSTRING('abcdef', 2, 3);
SELECT TRIM('  spaces  ');

-- Test: LIKE operator for pattern matching
CREATE TABLE words (word TEXT);
INSERT INTO words VALUES ('apple');
INSERT INTO words VALUES ('application');
INSERT INTO words VALUES ('banana');
INSERT INTO words VALUES ('appetizer');
SELECT word FROM words WHERE word LIKE 'app%';
SELECT word FROM words WHERE word LIKE '%ana%';
DROP TABLE words;

-- Test: CAST between types
SELECT CAST(42 AS TEXT);
SELECT CAST('123' AS INTEGER);
SELECT CAST(3.14 AS INTEGER);
SELECT CAST(100 AS FLOAT);
SELECT CAST(true AS INTEGER);

-- Test: Numeric functions
SELECT ABS(-42);
SELECT ROUND(3.14159, 2);
SELECT CEIL(3.2);
SELECT FLOOR(3.8);
SELECT MOD(17, 5);
SELECT POWER(2, 10);

-- Test: DECIMAL precision
CREATE TABLE money (amount DECIMAL(10, 2));
INSERT INTO money VALUES (19.99);
INSERT INTO money VALUES (1234.50);
SELECT amount, amount * 1.08 AS with_tax FROM money;
DROP TABLE money;

-- Test: BETWEEN operator with different types
SELECT 5 BETWEEN 1 AND 10;
SELECT 'c' BETWEEN 'a' AND 'z';

-- Test: Comparison operators
SELECT 1 = 1, 1 <> 2, 1 < 2, 2 > 1, 1 <= 1, 2 >= 1;
