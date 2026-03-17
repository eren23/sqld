-- ============================================================
-- constraints.sql
-- Tests: NOT NULL constraint violations, UNIQUE constraints
--        (via unique index), primary key simulation, and
--        notes on CHECK and FOREIGN KEY support.
-- ============================================================

-- Setup: table with NOT NULL columns
CREATE TABLE users (
    id INTEGER NOT NULL,
    username VARCHAR(50) NOT NULL,
    email TEXT
);

-- Test: Valid insert (should succeed)
INSERT INTO users VALUES (1, 'alice', 'alice@example.com');
INSERT INTO users VALUES (2, 'bob', NULL);

-- Verify
SELECT * FROM users;

-- Test: NOT NULL violation on id (should fail)
-- Uncomment to test; expected: error
-- INSERT INTO users VALUES (NULL, 'charlie', 'c@example.com');

-- Test: NOT NULL violation on username (should fail)
-- Uncomment to test; expected: error
-- INSERT INTO users VALUES (3, NULL, 'd@example.com');

-- Test: UNIQUE constraint via unique index
CREATE UNIQUE INDEX idx_users_username ON users (username);

-- This should succeed (unique value)
INSERT INTO users VALUES (3, 'charlie', 'charlie@example.com');

-- Test: UNIQUE violation (should fail - 'alice' already exists)
-- Uncomment to test; expected: error
-- INSERT INTO users VALUES (4, 'alice', 'alice2@example.com');

-- Test: Primary key simulation (NOT NULL + UNIQUE INDEX)
CREATE TABLE products (
    id INTEGER NOT NULL,
    name TEXT NOT NULL,
    price FLOAT
);
CREATE UNIQUE INDEX idx_products_pk ON products (id);

INSERT INTO products VALUES (1, 'Widget', 9.99);
INSERT INTO products VALUES (2, 'Gadget', 19.99);

-- Test: PK uniqueness violation (should fail)
-- Uncomment to test; expected: error
-- INSERT INTO products VALUES (1, 'Duplicate', 5.00);

-- Test: Compound unique index
CREATE TABLE enrollment (
    student_id INTEGER NOT NULL,
    course_id INTEGER NOT NULL,
    enrolled_date DATE
);
CREATE UNIQUE INDEX idx_enrollment_pk ON enrollment (student_id, course_id);

INSERT INTO enrollment VALUES (1, 101, '2025-01-15');
INSERT INTO enrollment VALUES (1, 102, '2025-01-16');
INSERT INTO enrollment VALUES (2, 101, '2025-01-17');

-- Test: Compound uniqueness violation (should fail)
-- Uncomment to test; expected: error
-- INSERT INTO enrollment VALUES (1, 101, '2025-02-01');

-- Verify all enrolled
SELECT * FROM enrollment;

-- NOTE: CHECK constraints are not currently supported.
-- NOTE: FOREIGN KEY constraints are not currently supported.
-- These would be tested here once available.

-- Cleanup
DROP INDEX idx_users_username;
DROP INDEX idx_products_pk;
DROP INDEX idx_enrollment_pk;
DROP TABLE enrollment;
DROP TABLE products;
DROP TABLE users;
