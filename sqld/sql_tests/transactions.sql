-- ============================================================
-- transactions.sql
-- Tests: BEGIN/COMMIT, BEGIN/ROLLBACK, SAVEPOINT/ROLLBACK TO,
--        transaction state transitions, error handling in
--        transactions, and autocommit behavior.
-- ============================================================

-- Setup
CREATE TABLE accounts (
    id INTEGER NOT NULL,
    owner TEXT NOT NULL,
    balance FLOAT NOT NULL
);

INSERT INTO accounts VALUES (1, 'Alice', 1000.0);
INSERT INTO accounts VALUES (2, 'Bob', 500.0);

-- Test: Autocommit behavior (implicit commit after each statement)
UPDATE accounts SET balance = 999.0 WHERE id = 1;
SELECT balance FROM accounts WHERE id = 1;
-- Balance should be 999.0 (auto-committed)

-- Reset
UPDATE accounts SET balance = 1000.0 WHERE id = 1;

-- Test: BEGIN and COMMIT (transfer funds)
BEGIN;
UPDATE accounts SET balance = balance - 200.0 WHERE id = 1;
UPDATE accounts SET balance = balance + 200.0 WHERE id = 2;
COMMIT;

-- Verify: Alice=800, Bob=700
SELECT * FROM accounts ORDER BY id;

-- Test: BEGIN and ROLLBACK (undo changes)
BEGIN;
UPDATE accounts SET balance = 0.0 WHERE id = 1;
UPDATE accounts SET balance = 0.0 WHERE id = 2;
-- Oops, roll it back
ROLLBACK;

-- Verify: balances unchanged (Alice=800, Bob=700)
SELECT * FROM accounts ORDER BY id;

-- Test: SAVEPOINT and ROLLBACK TO SAVEPOINT
BEGIN;
UPDATE accounts SET balance = balance - 100.0 WHERE id = 1;
SAVEPOINT sp1;
UPDATE accounts SET balance = balance - 100.0 WHERE id = 1;
-- Undo the second deduction only
ROLLBACK TO sp1;
COMMIT;

-- Verify: Alice lost only 100 (now 700), Bob still 700
SELECT * FROM accounts ORDER BY id;

-- Test: Multiple savepoints
BEGIN;
SAVEPOINT sp_a;
INSERT INTO accounts VALUES (3, 'Charlie', 300.0);
SAVEPOINT sp_b;
INSERT INTO accounts VALUES (4, 'Diana', 400.0);
-- Roll back to sp_b: Diana's insert is undone
ROLLBACK TO sp_b;
COMMIT;

-- Verify: Charlie exists, Diana does not
SELECT * FROM accounts ORDER BY id;

-- Test: ROLLBACK entire transaction discards all savepoint work
BEGIN;
SAVEPOINT sp_x;
INSERT INTO accounts VALUES (5, 'Eve', 500.0);
ROLLBACK;

-- Verify: Eve was not added
SELECT * FROM accounts ORDER BY id;

-- Cleanup
DROP TABLE accounts;
