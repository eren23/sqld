# Limitations & Roadmap

## Current Limitations

### In-Memory Mode Only

The server runs entirely in memory using a `HashMap`-based catalog provider (`MemoryCatalogProvider`). **Data does not survive restarts.** All table data is stored in `HashMap<String, Vec<Tuple>>` structures, and the entire database is lost when the process exits.

### Storage Engine Not Wired

The storage engine (buffer pool, disk manager, heap files, slotted pages, B+ tree index, hash index, TOAST, free space map) is fully implemented and passes all its unit tests, but it is **not connected to the server's query pipeline**. The `CatalogProvider` trait that the executor uses is implemented by `MemoryCatalogProvider` instead of a storage-backed provider.

### Transaction Manager Not Wired

The transaction manager (MVCC, SSI, lock manager with deadlock detection, savepoints) is fully implemented and tested in isolation, but:

- `BEGIN`/`COMMIT`/`ROLLBACK` toggle a `TransactionState` enum (Idle/InBlock/Failed) for the wire protocol but do **not** create real MVCC transactions.
- There is no snapshot isolation -- every statement sees the current state of the in-memory HashMap.
- There is no write-set tracking, no conflict detection, and no rollback of data changes.
- Savepoint commands are parsed but have no effect on data.

### WAL Not Wired

The WAL manager (write-ahead logging, ARIES recovery, fuzzy checkpoints) is implemented and tested, but it is not connected to the buffer pool's page flush path or the transaction manager's commit path. No WAL records are written during normal operation.

### Binder Bypassed

The semantic analyzer/binder from Task 12 is **bypassed** in the current pipeline. SQL goes from the parser directly to the plan builder. This means:

- Column names are not validated against the catalog schema at planning time
- Type checking of expressions is deferred to execution time
- Some semantic errors that should be caught at planning time are instead caught at execution time (or not caught at all)

### DDL Limitations

- `ALTER TABLE` statements parse correctly but are **no-ops** -- they do not modify the in-memory schema.
- `ANALYZE` parses correctly but is a no-op -- there is no statistics collection.
- `VACUUM` parses correctly but is a no-op -- there are no dead tuples to reclaim in the in-memory store.

### Missing Features

- **No CHECK constraints** -- CHECK constraint expressions are parsed and stored in the AST, but they are not enforced on INSERT/UPDATE.
- **No FOREIGN KEY enforcement** -- REFERENCES clauses are parsed but not enforced. There is no cascading delete/update.
- **No authentication** -- The server accepts any connection with any username. The PG startup handshake is completed with `AuthenticationOk` unconditionally.
- **No concurrent query execution** -- Query processing is single-threaded per connection. The server spawns one thread per connection, but there is no parallel query execution within a single query.
- **No system catalog tables** -- There are no `pg_class`, `pg_attribute`, `pg_index`, or other system catalog tables. `SHOW TABLES` and `SHOW COLUMNS` are handled as special cases in the query handler.
- **No subquery execution in expressions** -- The expression evaluator does not support correlated or uncorrelated subqueries (`IN (SELECT ...)`, `EXISTS (SELECT ...)`, scalar subqueries). These are parsed into the AST but rejected at execution time.

## Roadmap / Future Work

### Priority 1: Wire Storage Engine

Replace `MemoryCatalogProvider` with a real storage-backed `CatalogProvider` that:

- Uses the buffer pool for page I/O
- Stores table data in heap files with slotted pages
- Maintains B+ tree and hash indexes
- Uses the free space map for efficient page allocation
- Supports TOAST for large values

This is the single most impactful change, transforming sqld from an in-memory demo into a persistent database.

### Priority 2: Wire Transaction Manager

Integrate the MVCC transaction manager so that:

- `BEGIN` creates a real transaction with a snapshot
- All reads go through MVCC visibility checks
- All writes are tracked in the transaction's write set
- `COMMIT` validates and commits the transaction (with SSI checks for serializable isolation)
- `ROLLBACK` aborts the transaction and undoes changes
- Savepoints capture and restore write-set positions

### Priority 3: Wire WAL

Connect the WAL manager so that:

- All data modifications generate WAL records before modifying pages
- Transaction commit flushes the WAL to disk
- The buffer pool enforces the WAL protocol (page_lsn <= flushed_wal_lsn) before evicting dirty pages
- Crash recovery runs on startup using the ARIES 3-phase algorithm
- Periodic checkpoints reduce recovery time

### Priority 4: Enable Binder

Connect the semantic analyzer into the query pipeline between the parser and plan builder:

- Validate table and column names against the catalog
- Resolve ambiguous column references
- Type-check expressions and insert implicit casts
- Validate constraint references

### Priority 5: Additional Features

- **Authentication** -- Implement password authentication (MD5, SCRAM-SHA-256)
- **System catalog tables** -- Implement `pg_class`, `pg_attribute`, `pg_index`, `pg_type`, etc. so that standard tools can introspect the schema
- **Concurrent query execution** -- Add intra-query parallelism (parallel scan, parallel hash join)
- **Subquery execution** -- Support correlated and uncorrelated subqueries in the expression evaluator
- **CHECK and FOREIGN KEY enforcement** -- Validate constraints on INSERT and UPDATE
- **Statistics collection** -- Implement `ANALYZE` to collect histogram and NDV statistics for the optimizer
- **Online DDL** -- Make `ALTER TABLE` modify the live schema
