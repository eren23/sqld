# Storage Engine

The storage engine provides disk-oriented data structures for persistent storage: a buffer pool, slotted pages, heap files, B+ tree and hash indexes, TOAST for large values, and a free space map.

> **Note:** The storage engine is fully implemented and tested in isolation but is **not wired into the server**. The server currently uses an in-memory `MemoryCatalogProvider`. Connecting the storage layer is the primary remaining integration work.

Source files:
- `src/storage/buffer_pool.rs` -- Buffer pool with LRU-K eviction
- `src/storage/disk_manager.rs` -- Page-level file I/O
- `src/storage/page.rs` -- Slotted page format
- `src/storage/heap_file.rs` -- Heap file (table storage)
- `src/storage/btree/` -- B+ tree index
- `src/storage/hash_index.rs` -- Extendible hash index
- `src/storage/toast.rs` -- Oversized attribute storage
- `src/storage/free_space_map.rs` -- Free space tracking

## Buffer Pool

The buffer pool manager (`src/storage/buffer_pool.rs`) is the central component of the storage layer. It manages a fixed-size pool of 8 KB page frames backed by a `DiskManager`.

### Design

- **Default configuration**: 32,768 frames (256 MB), LRU-K with K=2
- **Pin/unpin protocol**: Pages are fetched into frames via `fetch_page()` which pins them (preventing eviction). Callers must call `unpin_page()` when done, optionally marking the page dirty.
- **Dirty page tracking**: Each frame has an `AtomicBool` dirty flag. Dirty pages are written back to disk on eviction or explicit flush.
- **WAL protocol enforcement**: A dirty page may only be written to disk when `page_lsn <= flushed_wal_lsn`. This prevents writing a page whose WAL record has not yet been persisted, ensuring crash recovery correctness.

### LRU-K Eviction

The replacement policy is LRU-K (K=2 by default), which provides resistance to sequential scan pollution:

1. Each frame tracks its last K access timestamps in a bounded deque.
2. When evicting, frames with **fewer than K accesses** have "infinite" backward K-distance and are evicted first (LRU among themselves by earliest access time). This means pages touched only once during a table scan are evicted before frequently accessed pages.
3. Among frames with K or more accesses, the one whose K-th-last access is oldest is evicted.

### API

```rust
impl BufferPoolManager {
    fn fetch_page(&self, page_id: PageId) -> Result<Page>;
    fn unpin_page(&self, page_id: PageId, is_dirty: bool) -> Result<()>;
    fn write_page(&self, page_id: PageId, page: Page) -> Result<()>;
    fn flush_page(&self, page_id: PageId) -> Result<()>;
    fn flush_all_pages(&self) -> Result<()>;
    fn new_page(&self, page_type: PageType) -> Result<(PageId, Page)>;
    fn delete_page(&self, page_id: PageId) -> Result<()>;
    fn prefetch_pages(&self, start: PageId, count: usize) -> Result<usize>;
}
```

The `prefetch_pages()` method supports read-ahead for sequential scans, loading up to 32 pages without pinning them.

## Disk Manager

The disk manager (`src/storage/disk_manager.rs`) handles page-level I/O against a single database file (`sqld.db`).

- Pages are identified by `PageId` (u32). Page 0 is reserved and never allocated.
- Each page occupies `PAGE_SIZE` (8,192 bytes) at file offset `page_id * PAGE_SIZE`.
- Thread safety is provided by an internal `Mutex` around the file handle.
- A lock file (`sqld.lock`) is created to signal that the database is in use.
- Deallocated page IDs are maintained in a free list for reuse.

## Slotted Page Format

The page (`src/storage/page.rs`) uses a slotted page layout:

```
+----------+------------------+-------------+------------------+
|  Header  |  Slot Array  ->  | Free Space  |  <- Tuple Data   |
| 24 bytes | 4 bytes / slot   |             |                  |
+----------+------------------+-------------+------------------+
0          24                                              8192
```

### Header Layout (24 bytes, little-endian)

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | `page_id` (u32) |
| 4 | 2 | `page_type` (u16) |
| 6 | 2 | `free_space_offset` (u16) |
| 8 | 2 | `tuple_count` (u16) |
| 10 | 2 | `flags` (u16) |
| 12 | 8 | `lsn` (u64) -- Log Sequence Number |
| 20 | 2 | `checksum` (u16) -- CRC-16/CCITT |
| 22 | 2 | reserved |

### Page Types

- `HeapData` (1) -- Heap file data page
- `BtreeInternal` (2) -- B+ tree internal node
- `BtreeLeaf` (3) -- B+ tree leaf node
- `HashBucket` (4) -- Hash index bucket
- `Overflow` (5) -- Overflow page (for TOAST)
- `FreeSpaceMap` (6) -- Free space map page

### Slot Array

The slot array grows from the header toward higher offsets. Each slot is 4 bytes (2-byte offset + 2-byte length). A slot with offset 0 marks a deleted entry.

### Tuple Data

Tuple data grows from the end of the page toward lower offsets. Free space sits between the slot array and the tuple data.

### Operations

- `insert_tuple(data)` -- Inserts data, reusing deleted slots when possible. Returns the slot index.
- `delete_tuple(slot_index)` -- Zeroes the tuple data and marks the slot as free.
- `fetch_tuple(slot_index)` -- Returns a reference to the raw tuple bytes.
- `update_tuple(slot_index, new_data)` -- Replaces data in-place (must be same length).
- `compact()` -- Moves all live tuples to be contiguous at the end, reclaiming fragmented free space.

### Checksum

Each page carries a CRC-16/CCITT checksum computed over the entire page (with the checksum field zeroed during computation). The checksum is verified on every page read from disk; a mismatch indicates corruption.

## Heap Files

A heap file (`src/storage/heap_file.rs`) stores a table as a sequence of slotted pages. It provides:

- **Insert** -- Tries a target page (suggested by the free space map), falls back to scanning existing pages, then allocates a new page if all are full.
- **Delete** -- Sets the tuple's `xmax` MVCC header field (logical delete).
- **Update** -- Delete + insert (new version gets a new TID).
- **Sequential scan** -- Visits every page in allocation order, returning `(Tid, Tuple)` pairs for all live slots.
- **Random access** -- Fetch a single tuple by `Tid` (page_id + slot_index).
- **Vacuum** -- Removes dead tuples (xmax != 0) and compacts pages.

The `Tid` (Tuple Identifier) is a `(page_id, slot_index)` pair that uniquely identifies a tuple's physical location.

## B+ Tree Index

The B+ tree (`src/storage/btree/`) provides ordered index access for range scans and point lookups.

### Structure

- **Internal nodes** store keys and child page pointers. Keys are stored as raw byte arrays, compared using a pluggable comparator function.
- **Leaf nodes** store keys and TIDs (heap tuple identifiers). Leaf nodes are linked into a doubly-linked list for efficient range scans.
- The tree is parameterized by a `CompareFn` (closure) and supports unique and non-unique indexes.

### Operations

- **Search** -- Traverses from root to leaf using binary search at each level. Returns the TID for an exact key match.
- **Insert** -- Finds the target leaf, inserts the key-TID pair, and splits if the leaf is full. Splits propagate upward; a new root is created if the old root splits.
- **Delete** -- Finds the target leaf and removes the entry. (Merging/redistribution on underflow is implemented.)
- **Range scan** -- Uses the `BTreeIterator` to scan forward or backward from a starting key. Supports both inclusive and exclusive bounds.

### Concurrent Access

The `src/storage/btree/concurrent.rs` module provides latch-coupling (crabbing) for concurrent B+ tree access: a parent node's latch is released only after the child's latch is acquired, preventing lost updates during concurrent modifications.

## Hash Index

The hash index (`src/storage/hash_index.rs`) is an extendible hash index supporting exact-match lookups.

- Uses a directory of `2^d` bucket pointers, where `d` is the global depth.
- Bucket pages store key-TID pairs. Keys are hashed using Rust's `DefaultHasher`.
- When a bucket overflows, it is split by incrementing its local depth. If local depth exceeds global depth, the directory is doubled.
- Supports duplicate keys and concurrent read/write access via `RwLock` per bucket page.

## TOAST (Oversized Attribute Storage)

The TOAST system (`src/storage/toast.rs`) handles values larger than 2,048 bytes by storing them out-of-line.

- Values exceeding `TOAST_THRESHOLD` (2,048 bytes) are split into chunks of `TOAST_CHUNK_SIZE` (2,048 bytes).
- The original value is replaced with a 13-byte `ToastPointer` containing: a magic tag byte (`0xFE`), a toast table ID, a chunk ID, and the total length.
- Chunks are stored in a separate TOAST table backed by a B+ tree index on `(chunk_id, sequence_number)`.
- Retrieval reassembles the chunks in order.

## Free Space Map

The free space map (`src/storage/free_space_map.rs`) tracks approximate free space on each heap page using one byte per page.

- Each byte encodes free space in 32-byte increments: `0` means full (< 32 bytes free), `255` means empty (>= 8,160 bytes free).
- INSERT uses the FSM to quickly find a page with enough room without scanning every heap page.
- The FSM is updated after inserts and deletes to keep it approximately current.

Helper methods:
- `bytes_to_category(free_bytes)` -- Converts actual free bytes to an FSM category byte.
- `needed_to_category(needed_bytes)` -- Converts a needed byte count to the minimum satisfying category.
- `find_page(needed_bytes)` -- Scans the FSM for a page with sufficient free space.
