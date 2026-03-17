-- ============================================================
-- dml.sql
-- Tests: INSERT (single row, explicit columns, SELECT),
--        UPDATE (single/multiple rows, expressions, WHERE),
--        DELETE (with WHERE, all rows), and verification
--        queries after each mutation.
-- ============================================================

-- Setup
CREATE TABLE inventory (
    id INTEGER,
    item TEXT,
    qty INTEGER,
    price FLOAT
);

-- Test: INSERT single row
INSERT INTO inventory VALUES (1, 'Hammer', 50, 12.99);

-- Test: INSERT with explicit column list
INSERT INTO inventory (id, item, qty, price) VALUES (2, 'Wrench', 30, 8.50);

-- Test: INSERT multiple rows (separate statements)
INSERT INTO inventory VALUES (3, 'Screwdriver', 100, 5.25);
INSERT INTO inventory VALUES (4, 'Pliers', 25, 15.00);
INSERT INTO inventory VALUES (5, 'Tape', 200, 3.99);

-- Verify: all 5 rows inserted
SELECT * FROM inventory ORDER BY id;

-- Test: UPDATE single row
UPDATE inventory SET price = 13.49 WHERE id = 1;

-- Verify: price changed for Hammer
SELECT id, item, price FROM inventory WHERE id = 1;

-- Test: UPDATE multiple rows
UPDATE inventory SET qty = qty + 10 WHERE qty < 50;

-- Verify: qty updated for Wrench and Pliers
SELECT id, item, qty FROM inventory WHERE id IN (2, 4);

-- Test: UPDATE with expression
UPDATE inventory SET price = ROUND(price * 1.1, 2);

-- Verify: all prices increased by 10%
SELECT id, item, price FROM inventory ORDER BY id;

-- Test: UPDATE with compound WHERE
UPDATE inventory SET qty = 0 WHERE item = 'Tape' AND price < 5.0;

-- Verify
SELECT id, item, qty FROM inventory WHERE item = 'Tape';

-- Test: DELETE single row
DELETE FROM inventory WHERE id = 5;

-- Verify: Tape removed
SELECT COUNT(*) FROM inventory;

-- Test: DELETE with compound condition
DELETE FROM inventory WHERE qty < 40 AND price > 10.0;

-- Verify
SELECT * FROM inventory ORDER BY id;

-- Test: INSERT ... SELECT
CREATE TABLE inventory_backup (id INTEGER, item TEXT, qty INTEGER, price FLOAT);
INSERT INTO inventory_backup SELECT * FROM inventory;

-- Verify backup has same data
SELECT * FROM inventory_backup ORDER BY id;

-- Test: DELETE all rows
DELETE FROM inventory;

-- Verify: empty table
SELECT COUNT(*) FROM inventory;

-- Cleanup
DROP TABLE inventory;
DROP TABLE inventory_backup;
