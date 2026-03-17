# Getting Started

This guide walks you through building sqld from source, starting the server, connecting with `psql`, and running your first queries.

## Prerequisites

- **Rust toolchain** (1.70 or later) -- install via [rustup](https://rustup.rs/)
- **A PostgreSQL client** -- `psql` is recommended (comes with any PostgreSQL installation)

## Build

Clone the repository and build in release mode:

```bash
cd sqld
cargo build --release
```

The binary is produced at `./target/release/sqld`.

## Run the Server

Start sqld with the included configuration file:

```bash
./target/release/sqld sqld_config.toml
```

By default the server listens on **127.0.0.1:5433**. You can change the host and port in the config file (see [Configuration](configuration.md)).

## Connect

Open a new terminal and connect with `psql`:

```bash
psql -h 127.0.0.1 -p 5433 -U sqld
```

You should see the familiar PostgreSQL prompt. You are now talking to sqld.

## Walkthrough

The examples below mirror the [demo recording](https://github.com/user/sqld/blob/main/demo.tape) shipped with the repository. Follow along to exercise the core SQL features.

### Create Tables

```sql
CREATE TABLE users (
    id      INTEGER,
    name    VARCHAR(50),
    email   VARCHAR(100),
    age     INTEGER
);

CREATE TABLE orders (
    id      INTEGER,
    user_id INTEGER,
    product TEXT,
    amount  FLOAT
);
```

### Insert Data

```sql
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 32);
INSERT INTO users VALUES (2, 'Bob',   'bob@example.com',   28);
INSERT INTO users VALUES (3, 'Carol', 'carol@example.com', 35);

INSERT INTO orders VALUES (1, 1, 'Widget', 29.99);
INSERT INTO orders VALUES (2, 1, 'Gadget', 49.99);
INSERT INTO orders VALUES (3, 2, 'Widget', 29.99);
INSERT INTO orders VALUES (4, 3, 'Gizmo',  99.99);
```

### SELECT with WHERE

Retrieve all rows:

```sql
SELECT * FROM users;
```

Filter with a `WHERE` clause:

```sql
SELECT name, email FROM users WHERE age > 30;
```

### JOIN Queries

Join `users` to `orders` to see who bought what:

```sql
SELECT u.name, o.product, o.amount
FROM users u
JOIN orders o ON u.id = o.user_id;
```

### GROUP BY with HAVING and Aggregates

Find users with more than one order, along with their total spend:

```sql
SELECT u.name,
       COUNT(*)      AS order_count,
       SUM(o.amount) AS total
FROM users u
JOIN orders o ON u.id = o.user_id
GROUP BY u.name
HAVING COUNT(*) > 1;
```

### UPDATE

```sql
UPDATE users SET age = 33 WHERE name = 'Alice';

SELECT name, age FROM users WHERE name = 'Alice';
```

### DELETE

```sql
DELETE FROM orders WHERE product = 'Gizmo';
```

### SHOW TABLES

List all tables in the current database:

```sql
SHOW TABLES;
```

### EXPLAIN

Inspect the query plan without executing the query:

```sql
EXPLAIN SELECT u.name, o.product
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE o.amount > 30;
```

## Running the Tests

sqld ships with **1,529 tests** covering every subsystem. Run the full suite with:

```bash
cargo test
```

Notable test modules include:

| Module | What it covers |
|--------|---------------|
| `sql::parser` | Tokenizer and Pratt parser for all SQL statement types |
| `sql::optimizer` | Cost model, join reordering, predicate pushdown |
| `sql::executor` | Volcano operators: scan, filter, project, join, aggregate, sort |
| `catalog` | Schema management, table and column metadata |
| `storage::page` | Slotted-page layout, tuple serialization |
| `storage::btree` | B+ tree insert, delete, split, merge, range scans |
| `storage::hash_index` | Extensible hash index operations |
| `buffer_pool` | LRU-K eviction, pin/unpin, dirty-page flushing |
| `mvcc` | Snapshot isolation, write-write conflict detection, SSI validation |
| `wal` | Log record serialization, recovery, checkpoint correctness |
| `server::protocol` | PostgreSQL wire protocol message encoding/decoding |
| `config` | TOML parsing, default values, partial overrides |

To run a single test module, for example the parser tests:

```bash
cargo test --lib sql::parser
```
