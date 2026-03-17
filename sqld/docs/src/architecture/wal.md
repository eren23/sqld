# Write-Ahead Log

The WAL (Write-Ahead Log) subsystem provides durability and crash recovery using ARIES-style write-ahead logging with fuzzy checkpointing.

> **Note:** The WAL is fully implemented and tested in isolation but is **not wired into the server**. Connecting it requires integrating the WAL with the buffer pool's page flush path and the transaction manager's commit path.

Source files:
- `src/wal/wal_manager.rs` -- WAL manager (append, flush, read)
- `src/wal/wal_record.rs` -- WAL record types and serialization
- `src/wal/recovery.rs` -- ARIES 3-phase crash recovery
- `src/wal/checkpoint.rs` -- Fuzzy checkpointing

## WAL Manager

The WAL manager (`src/wal/wal_manager.rs`) handles all WAL I/O.

### Architecture

- **WAL file**: `wal.log` in the data directory (append-only)
- **Metadata file**: `wal.meta` stores the last checkpoint LSN
- **In-memory buffer**: 64 KB write buffer for batching appends
- **Flush policy**: Buffer is flushed on transaction commit, when it reaches capacity, or after a 10ms timeout

### LSN (Log Sequence Number)

Each WAL record is assigned a monotonically increasing LSN equal to its byte offset in the WAL file. The LSN serves as a global ordering of all operations.

Key LSN values tracked:

- `current_lsn` -- Next LSN to assign (file size + buffer length)
- `flushed_lsn` -- LSN up to which data has been fsync'd to disk
- `last_checkpoint_lsn` -- LSN of the most recent checkpoint begin

### Per-Transaction Undo Chain

The WAL manager maintains a `txn_prev_lsn` map that tracks the LSN of each transaction's most recent record. Each WAL entry stores a `prev_lsn` field pointing to the previous record from the same transaction, forming a backward-linked chain for undo traversal during recovery.

### API

```rust
impl WalManager {
    fn open(dir: &Path) -> Result<Self>;
    fn append(&self, record: WalRecord) -> Result<u64>;  // Returns assigned LSN
    fn flush(&self) -> Result<()>;                        // Sync buffer to disk
    fn commit(&self, txn_id: u64) -> Result<u64>;         // Append Commit + flush
    fn read_all_entries(&self) -> Result<Vec<WalEntry>>;  // Read entire WAL
    fn last_checkpoint_lsn(&self) -> u64;
    fn set_last_checkpoint_lsn(&self, lsn: u64) -> Result<()>;
    fn truncate_before(&self, lsn: u64) -> Result<()>;
}
```

## WAL Record Types

The `WalRecord` enum (`src/wal/wal_record.rs`) defines 13 record kinds:

### Transaction Lifecycle

| Record | Fields | Purpose |
|--------|--------|---------|
| `Begin` | `txn_id` | Marks transaction start |
| `Commit` | `txn_id` | Marks transaction commit |
| `Abort` | `txn_id` | Marks transaction abort |

### Physiological Tuple Operations

These records carry full undo data so recovery can reverse uncommitted changes without consulting heap pages.

| Record | Fields | Purpose |
|--------|--------|---------|
| `InsertTuple` | `txn_id`, `page_id`, `slot_index`, `data` | Tuple insertion (undo: delete) |
| `DeleteTuple` | `txn_id`, `page_id`, `slot_index`, `data` | Tuple deletion with full tuple for undo (undo: re-insert) |
| `UpdateTuple` | `txn_id`, `page_id`, `slot_index`, `old_data`, `new_data` | Tuple update with both old and new data |

### Index Operations

| Record | Fields | Purpose |
|--------|--------|---------|
| `IndexInsert` | `txn_id`, `index_page_id`, `key`, `tid_page`, `tid_slot` | Index entry insertion |
| `IndexDelete` | `txn_id`, `index_page_id`, `key`, `tid_page`, `tid_slot` | Index entry deletion |

### Page-Level Operations

| Record | Fields | Purpose |
|--------|--------|---------|
| `PageAlloc` | `txn_id`, `page_id` | New page allocation |
| `PageFree` | `txn_id`, `page_id` | Page deallocation |

### Checkpoint

| Record | Fields | Purpose |
|--------|--------|---------|
| `CheckpointBegin` | `active_txns: Vec<u64>` | Start of checkpoint with active transaction list |
| `CheckpointEnd` | `checkpoint_begin_lsn: u64` | End of checkpoint referencing begin LSN |

### Compensation Log Record (CLR)

```rust
Clr {
    txn_id: u64,
    undo_next_lsn: u64,    // Next record to undo (skips compensated record)
    redo: Box<WalRecord>,  // The redo action (CLRs are redo-only, never undone)
}
```

CLRs are written during the undo phase of recovery. The `undo_next_lsn` field allows the undo process to skip over already-compensated records, preventing repeated undo of the same operation during repeated crashes.

### WAL Entry Format

Each WAL entry on disk consists of:

```
+--------+----------+--------+---------+---------+
| lsn    | prev_lsn | length | record  | crc32   |
| 8 bytes| 8 bytes  | 4 bytes| variable| 4 bytes |
+--------+----------+--------+---------+---------+
```

- **Header**: 20 bytes (LSN + prev_lsn + length)
- **Payload**: Serialized `WalRecord` (variable length)
- **Trailer**: CRC-32 checksum for integrity verification

## ARIES Crash Recovery

The recovery manager (`src/wal/recovery.rs`) implements ARIES-style 3-phase crash recovery.

### Phase 1: Analysis

Starting from the last checkpoint, scan the WAL forward to reconstruct:

1. **Dirty Page Table (DPT)**: Maps `page_id -> rec_lsn` -- the LSN of the earliest record that might have dirtied this page. Records that modify pages (`InsertTuple`, `DeleteTuple`, `UpdateTuple`, `IndexInsert`, `IndexDelete`) add entries to the DPT if the page is not already present.

2. **Active Transaction Table (ATT)**: Maps `txn_id -> (last_lsn, status)` -- all transactions that were active at some point. `Begin` records add entries; `Commit` and `Abort` records update the status.

Checkpoint records seed the ATT with the active transactions listed in `CheckpointBegin`.

### Phase 2: Redo

Starting from the minimum `rec_lsn` in the dirty page table, scan the WAL forward. For each record that modifies a page:

1. Check if the page is in the DPT (if not, skip -- the page was already flushed before the checkpoint).
2. Check if the record's LSN is less than or equal to the page's current LSN on disk (if so, skip -- the page already reflects this change).
3. Otherwise, re-apply (redo) the record's operation on the page.

Redo is idempotent: applying it multiple times produces the same result.

### Phase 3: Undo

Walk the undo chains for all transactions that are still Active after analysis. For each active transaction:

1. Start at the transaction's `last_lsn` from the ATT.
2. Read the record at that LSN.
3. If it is a `Clr`, follow its `undo_next_lsn` (skip already-compensated work).
4. Otherwise, undo the operation by applying the inverse action:
   - `InsertTuple` -> delete the tuple
   - `DeleteTuple` -> re-insert the tuple (using saved `data`)
   - `UpdateTuple` -> restore `old_data`
5. Write a CLR record for the undo action.
6. Follow `prev_lsn` to the transaction's previous record.
7. Continue until `prev_lsn == 0` (beginning of transaction).

After undo completes, write an `Abort` record for each undone transaction.

### Recovery State

The recovery manager returns a `RecoveryState` containing the final dirty page table and active transaction table, which the system can use to resume normal operation.

## Checkpoints

The checkpoint manager (`src/wal/checkpoint.rs`) implements fuzzy checkpointing.

### Checkpoint Procedure

1. Record the set of currently active transactions.
2. Write a `CheckpointBegin` record with the active transaction list.
3. Flush the WAL to ensure all records up to this point are durable.
4. Flush all dirty pages from the buffer pool to disk (via the `DirtyPageFlusher` trait).
5. Write a `CheckpointEnd` record referencing the begin LSN.
6. Flush the WAL again.
7. Update the WAL metadata file with the new checkpoint LSN.

The checkpoint is "fuzzy" because normal operations can continue while dirty pages are being flushed -- the system does not need to stop.

### WAL Truncation

After a successful checkpoint, WAL entries before the checkpoint LSN are no longer needed for recovery (assuming no active transactions span that region). The `checkpoint_and_truncate()` method performs a checkpoint and then removes old WAL entries.

### Testing

The `DirtyPageFlusher` trait allows checkpoint logic to be tested in isolation with a `NoOpFlusher` that skips actual page I/O.
