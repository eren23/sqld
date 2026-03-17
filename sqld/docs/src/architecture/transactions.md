# Transactions

The transaction subsystem provides MVCC (Multi-Version Concurrency Control), serializable snapshot isolation (SSI), a lock manager with deadlock detection, and savepoints.

> **Note:** The transaction manager is fully implemented and tested in isolation but is **not wired into the server**. The server's `BEGIN`/`COMMIT`/`ROLLBACK` currently toggle a `TransactionState` enum without using real MVCC.

Source files:
- `src/transaction/transaction.rs` -- Transaction struct, isolation levels, status
- `src/transaction/mvcc.rs` -- MVCC snapshot and visibility rules
- `src/transaction/ssi.rs` -- Serializable Snapshot Isolation
- `src/transaction/lock_manager.rs` -- Lock manager with deadlock detection
- `src/transaction/savepoint.rs` -- Savepoint support

## Transaction Struct

Each transaction (`src/transaction/transaction.rs`) maintains:

```rust
pub struct Transaction {
    pub txn_id: u64,
    pub status: TransactionStatus,           // Active, Committed, Aborted
    pub isolation_level: IsolationLevel,     // ReadCommitted, RepeatableRead, Serializable
    pub snapshot: Snapshot,
    pub write_set: Vec<WriteEntry>,          // (table_id, tuple_id) pairs written
    pub read_set: Vec<ReadEntry>,            // (table_id, tuple_id) pairs read
    pub savepoints: Vec<Savepoint>,
    pub start_time: Instant,
    pub commit_time: Option<Instant>,
    pub command_id: u32,                     // Per-statement counter
}
```

### Isolation Levels

| Level | Description |
|-------|-------------|
| `ReadCommitted` | Each statement sees a fresh snapshot of committed data |
| `RepeatableRead` | The entire transaction sees a single snapshot taken at BEGIN |
| `Serializable` | RepeatableRead + SSI anomaly detection |

## MVCC (Multi-Version Concurrency Control)

The MVCC implementation (`src/transaction/mvcc.rs`) follows PostgreSQL's approach using tuple-level versioning with `xmin`/`xmax` headers.

### Snapshot

A snapshot defines the set of transactions visible to a reader:

```rust
pub struct Snapshot {
    pub xmin: u64,           // Lowest active txn_id at snapshot time
    pub xmax: u64,           // Next txn_id to be assigned (exclusive upper bound)
    pub active_txns: HashSet<u64>,  // Txns that were in-progress at snapshot time
}
```

A transaction is "visible in snapshot" if its ID is less than `xmax` AND it is NOT in the `active_txns` set.

### Visibility Rules

The `VisibilityCheck::is_visible()` method implements PostgreSQL-style MVCC visibility:

**Step 1: Check xmin (who created this tuple)**

- If `xmin == our txn_id` -- we created it, visible (unless we also deleted it)
- If `xmin`'s transaction is committed AND visible in our snapshot -- visible
- Otherwise (active or aborted xmin) -- not visible

**Step 2: Check xmax (who deleted this tuple)**

- If `xmax == 0` -- not deleted, visible
- If `xmax == our txn_id` -- we deleted it, NOT visible
- If `xmax`'s transaction aborted -- deletion was rolled back, visible
- If `xmax` committed but NOT in our snapshot -- we do not see the delete yet, visible
- If `xmax` committed AND in our snapshot -- deleted before our snapshot, NOT visible

This is implemented via a `TxnStatusLookup` trait that decouples visibility checking from the transaction manager's storage.

## Serializable Snapshot Isolation (SSI)

The SSI manager (`src/transaction/ssi.rs`) detects serialization anomalies by tracking read-write dependency edges between transactions.

### RW-Dependency Tracking

When transaction T2 writes data that transaction T1 previously read (detected via SIRead locks), an rw-dependency edge `T1 ->rw-> T2` is recorded. The SSI manager maintains:

- `rw_out: HashMap<u64, HashSet<u64>>` -- reader to set of writers
- `rw_in: HashMap<u64, HashSet<u64>>` -- writer to set of readers (reverse index)
- Snapshots for committed transactions (kept until safe to discard)

### Dangerous Structure Detection

On commit, the SSI manager checks for **dangerous structures**: `T1 ->rw-> T2 ->rw-> T3` where T1 and T3 are concurrent (T3 started before T1 committed). The special case `T1 ->rw-> T2 ->rw-> T1` (two-transaction write skew) is also detected.

If a dangerous structure is found, the committing transaction (T2) is aborted with a serialization failure error, and the client can retry.

### SIRead Locks

The `record_write_over_siread()` method integrates with the lock manager: when a transaction writes a row, it checks if any other transaction holds a SIRead lock on that row, and creates the appropriate rw-dependency edge.

## Lock Manager

The lock manager (`src/transaction/lock_manager.rs`) provides row-level and table-level locking with deadlock detection.

### Lock Modes

| Mode | Conflicts With | Description |
|------|---------------|-------------|
| `Shared` | `Exclusive` | Read lock |
| `Exclusive` | `Shared`, `Exclusive` | Write lock |
| `SIRead` | (none) | Advisory serializable read lock (never blocks) |
| `IntentionShared` | `Exclusive` | Table-level read intention |
| `IntentionExclusive` | `Exclusive` | Table-level write intention |

SIRead locks never block other transactions -- they only serve as markers for SSI dependency detection.

### Lock Targets

Locks can be acquired on two granularities:

- `Row { table_id, tuple_id }` -- Individual tuple lock
- `Table(table_id)` -- Entire table lock

### Lock Entry

Each lock target has a `LockEntry` with:

- `holders: Vec<(u64, LockMode)>` -- Transactions currently holding the lock
- `wait_queue: VecDeque<(u64, LockMode)>` -- FIFO queue of transactions waiting

### Lock Acquisition

When a transaction requests a lock:

1. Check if it already holds the lock at the same or stronger mode (Exclusive subsumes Shared).
2. Check compatibility with current holders (same transaction does not conflict with itself for upgrade).
3. If compatible, grant immediately.
4. If not compatible, add to the wait queue.

### Deadlock Detection

The lock manager detects deadlocks by building a waits-for graph and checking for cycles. When a transaction requests a lock and would be blocked, the graph is checked. If a cycle is found, the youngest transaction in the cycle is aborted to break the deadlock.

### Lock Release

On transaction commit or abort, all locks held by that transaction are released. The wait queue is then processed: waiting transactions that can now be granted are promoted to holders.

## Savepoints

Savepoints (`src/transaction/savepoint.rs`) enable partial rollback within a transaction.

```rust
pub struct Savepoint {
    pub name: String,
    pub write_set_position: usize,    // Index into Transaction::write_set
    pub read_set_position: usize,     // Index into Transaction::read_set
}
```

When a savepoint is created, it records the current size of the transaction's write set and read set. Rolling back to a savepoint truncates these sets back to the saved positions, effectively undoing any writes made after the savepoint was created.

Savepoints are stored as a stack in `Transaction::savepoints`, supporting nested savepoints. `RELEASE SAVEPOINT` removes the savepoint (but does not undo any work). `ROLLBACK TO SAVEPOINT` discards work done after the savepoint.
