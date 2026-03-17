use std::path::PathBuf;
use std::sync::Arc;

use sqld::storage::{BufferPoolManager, DiskManager, Page, PageType, PAGE_SIZE};
use sqld::utils::error::{Error, StorageError};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("sqld_test_bp_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn cleanup(dir: &PathBuf) {
    let _ = std::fs::remove_dir_all(dir);
}

/// Create a DiskManager with `num_pages` pre-written heap pages on disk
/// (page ids 1..=num_pages) and a BufferPoolManager of the given size.
fn setup(
    name: &str,
    pool_size: usize,
    k: usize,
    num_pages: usize,
) -> (PathBuf, Arc<DiskManager>, BufferPoolManager) {
    let dir = test_dir(name);
    let dm = Arc::new(DiskManager::new(&dir).unwrap());

    for _ in 0..num_pages {
        let pid = dm.allocate_page().unwrap();
        let page = Page::new(pid, PageType::HeapData);
        dm.write_page(pid, &page).unwrap();
    }

    let bpm = BufferPoolManager::new(pool_size, k, dm.clone());
    (dir, dm, bpm)
}

// ---------------------------------------------------------------------------
// Pin / unpin counting
// ---------------------------------------------------------------------------

#[test]
fn pin_unpin_counting() {
    let (dir, _dm, bpm) = setup("pin_unpin", 4, 2, 3);

    // First fetch → pin_count = 1
    let _p = bpm.fetch_page(1).unwrap();
    assert_eq!(bpm.pin_count(1), Some(1));

    // Second fetch of the same page → pin_count = 2
    let _p = bpm.fetch_page(1).unwrap();
    assert_eq!(bpm.pin_count(1), Some(2));

    // Unpin once → 1
    bpm.unpin_page(1, false).unwrap();
    assert_eq!(bpm.pin_count(1), Some(1));

    // Unpin again → 0
    bpm.unpin_page(1, false).unwrap();
    assert_eq!(bpm.pin_count(1), Some(0));

    // Unpinning beyond 0 is an error.
    assert!(bpm.unpin_page(1, false).is_err());

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Eviction of unpinned pages
// ---------------------------------------------------------------------------

#[test]
fn eviction_of_unpinned_pages() {
    let (dir, _dm, bpm) = setup("evict_unpinned", 3, 2, 5);

    // Fill the pool with pages 1-3, then unpin them all.
    for pid in 1..=3u32 {
        bpm.fetch_page(pid).unwrap();
        bpm.unpin_page(pid, false).unwrap();
    }
    assert_eq!(bpm.size(), 3);

    // Fetching page 4 forces eviction of one unpinned page.
    bpm.fetch_page(4).unwrap();
    bpm.unpin_page(4, false).unwrap();

    assert_eq!(bpm.size(), 3); // one evicted, one added
    assert!(bpm.pin_count(4).is_some());

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Pinned pages survive eviction
// ---------------------------------------------------------------------------

#[test]
fn pinned_pages_survive_eviction() {
    let (dir, _dm, bpm) = setup("pinned_survive", 3, 2, 5);

    // Pages 1 and 2 stay pinned; page 3 is unpinned.
    bpm.fetch_page(1).unwrap();
    bpm.fetch_page(2).unwrap();
    bpm.fetch_page(3).unwrap();
    bpm.unpin_page(3, false).unwrap();

    // Fetch page 4 → page 3 (the only unpinned one) must be evicted.
    bpm.fetch_page(4).unwrap();

    assert!(bpm.pin_count(1).is_some(), "pinned page 1 must survive");
    assert!(bpm.pin_count(2).is_some(), "pinned page 2 must survive");
    assert!(bpm.pin_count(3).is_none(), "unpinned page 3 must be evicted");
    assert!(bpm.pin_count(4).is_some(), "new page 4 must be present");

    // Cleanup pins.
    bpm.unpin_page(1, false).unwrap();
    bpm.unpin_page(2, false).unwrap();
    bpm.unpin_page(4, false).unwrap();

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Dirty page writeback
// ---------------------------------------------------------------------------

#[test]
fn dirty_page_writeback() {
    let (dir, dm, bpm) = setup("dirty_wb", 4, 2, 3);

    // Fetch page 1, modify it, write it back to the frame.
    let mut page = bpm.fetch_page(1).unwrap();
    page.insert_tuple(b"hello dirty").unwrap();
    bpm.write_page(1, page).unwrap();

    assert_eq!(bpm.is_dirty(1), Some(true));

    // Flush to disk (WAL LSN default 0, page LSN 0 → allowed).
    bpm.flush_page(1).unwrap();
    assert_eq!(bpm.is_dirty(1), Some(false));

    // Verify the data landed on disk by reading through the DiskManager.
    let disk_page = dm.read_page(1).unwrap();
    assert_eq!(disk_page.fetch_tuple(0).unwrap(), b"hello dirty");

    bpm.unpin_page(1, false).unwrap();
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Unpin marks dirty
// ---------------------------------------------------------------------------

#[test]
fn unpin_marks_dirty() {
    let (dir, _dm, bpm) = setup("unpin_dirty", 4, 2, 3);

    bpm.fetch_page(1).unwrap();
    assert_eq!(bpm.is_dirty(1), Some(false));

    bpm.unpin_page(1, true).unwrap();
    assert_eq!(bpm.is_dirty(1), Some(true));

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// LRU-K ordering preference
// ---------------------------------------------------------------------------

#[test]
fn lru_k_ordering_preference() {
    // Pool size 3, K = 2. All three pages get 2 accesses each (finite
    // backward K-distance). The page whose K-th-last access is oldest should
    // be evicted first.
    let (dir, _dm, bpm) = setup("lru_k_order", 3, 2, 5);

    // Page 1: two accesses (timestamps 1, 2) → K-th-last = 1
    bpm.fetch_page(1).unwrap();
    bpm.unpin_page(1, false).unwrap();
    bpm.fetch_page(1).unwrap();
    bpm.unpin_page(1, false).unwrap();

    // Page 2: two accesses (timestamps 3, 4) → K-th-last = 3
    bpm.fetch_page(2).unwrap();
    bpm.unpin_page(2, false).unwrap();
    bpm.fetch_page(2).unwrap();
    bpm.unpin_page(2, false).unwrap();

    // Page 3: two accesses (timestamps 5, 6) → K-th-last = 5
    bpm.fetch_page(3).unwrap();
    bpm.unpin_page(3, false).unwrap();
    bpm.fetch_page(3).unwrap();
    bpm.unpin_page(3, false).unwrap();

    // Fetch page 4 → evicts page with oldest K-th-last (page 1, ts=1).
    bpm.fetch_page(4).unwrap();
    bpm.unpin_page(4, false).unwrap();

    assert!(bpm.pin_count(1).is_none(), "page 1 (oldest K-th) evicted");
    assert!(bpm.pin_count(2).is_some(), "page 2 survives");
    assert!(bpm.pin_count(3).is_some(), "page 3 survives");
    assert!(bpm.pin_count(4).is_some(), "page 4 present");

    // Page 4 currently has 1 access (infinite backward K-distance). Give it
    // a second access so all three resident pages have finite K-distance.
    bpm.fetch_page(4).unwrap();
    bpm.unpin_page(4, false).unwrap();

    // Now: page 2 K-th=3, page 3 K-th=5, page 4 K-th=7.
    // Fetch page 5 → evicts page 2 (K-th-last = 3, the oldest).
    bpm.fetch_page(5).unwrap();
    bpm.unpin_page(5, false).unwrap();

    assert!(bpm.pin_count(2).is_none(), "page 2 now evicted");
    assert!(bpm.pin_count(3).is_some(), "page 3 survives");
    assert!(bpm.pin_count(4).is_some(), "page 4 survives");

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Sequential scan resistance
// ---------------------------------------------------------------------------

#[test]
fn sequential_scan_resistance() {
    // Pool size 3, K = 2.
    // "Hot" pages 1 & 2 are accessed twice (finite backward K-distance).
    // "Scan" page 3 is accessed once (infinite backward K-distance).
    // When a 4th page is needed, the scan page should be evicted first.
    let (dir, _dm, bpm) = setup("seq_scan", 3, 2, 5);

    // Hot page 1: two accesses.
    bpm.fetch_page(1).unwrap();
    bpm.unpin_page(1, false).unwrap();
    bpm.fetch_page(1).unwrap();
    bpm.unpin_page(1, false).unwrap();

    // Hot page 2: two accesses.
    bpm.fetch_page(2).unwrap();
    bpm.unpin_page(2, false).unwrap();
    bpm.fetch_page(2).unwrap();
    bpm.unpin_page(2, false).unwrap();

    // Scan page 3: single access.
    bpm.fetch_page(3).unwrap();
    bpm.unpin_page(3, false).unwrap();

    // Fetch page 4 → page 3 (infinite backward K-distance) evicted.
    bpm.fetch_page(4).unwrap();
    bpm.unpin_page(4, false).unwrap();

    assert!(bpm.pin_count(1).is_some(), "hot page 1 survives scan");
    assert!(bpm.pin_count(2).is_some(), "hot page 2 survives scan");
    assert!(bpm.pin_count(3).is_none(), "scan page 3 evicted");
    assert!(bpm.pin_count(4).is_some(), "page 4 present");

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// All pages pinned → BufferPoolExhausted
// ---------------------------------------------------------------------------

#[test]
fn all_pages_pinned_error() {
    let (dir, _dm, bpm) = setup("all_pinned", 3, 2, 5);

    // Fill the pool and keep everything pinned.
    bpm.fetch_page(1).unwrap();
    bpm.fetch_page(2).unwrap();
    bpm.fetch_page(3).unwrap();

    // Attempting to fetch a 4th page must fail.
    let result = bpm.fetch_page(4);
    assert!(result.is_err());
    match result.unwrap_err() {
        Error::Storage(StorageError::BufferPoolExhausted) => {}
        other => panic!("expected BufferPoolExhausted, got: {other:?}"),
    }

    bpm.unpin_page(1, false).unwrap();
    bpm.unpin_page(2, false).unwrap();
    bpm.unpin_page(3, false).unwrap();
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// WAL protocol enforcement
// ---------------------------------------------------------------------------

#[test]
fn wal_protocol_enforcement() {
    let (dir, _dm, bpm) = setup("wal_proto", 4, 2, 3);

    bpm.set_flushed_wal_lsn(100);

    // Fetch page, set its LSN to 200 (beyond flushed WAL LSN).
    let mut page = bpm.fetch_page(1).unwrap();
    page.set_lsn(200);
    bpm.write_page(1, page).unwrap();

    // Flush must fail: page_lsn 200 > flushed_wal_lsn 100.
    assert!(bpm.flush_page(1).is_err());
    assert_eq!(bpm.is_dirty(1), Some(true), "page still dirty after failed flush");

    // Advance flushed WAL LSN.
    bpm.set_flushed_wal_lsn(200);

    // Now flush succeeds.
    bpm.flush_page(1).unwrap();
    assert_eq!(bpm.is_dirty(1), Some(false));

    bpm.unpin_page(1, false).unwrap();
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// WAL protocol enforcement on eviction
// ---------------------------------------------------------------------------

#[test]
fn wal_protocol_enforcement_on_eviction() {
    let (dir, _dm, bpm) = setup("wal_evict", 1, 2, 2);

    // Fetch page 1, set LSN beyond the flushed WAL LSN, make it dirty.
    let mut page = bpm.fetch_page(1).unwrap();
    page.set_lsn(500);
    bpm.write_page(1, page).unwrap();
    bpm.unpin_page(1, true).unwrap();

    // flushed_wal_lsn is 0 — evicting page 1 requires writing it but
    // page_lsn (500) > flushed_wal_lsn (0) → must fail.
    let result = bpm.fetch_page(2);
    assert!(result.is_err(), "eviction should fail due to WAL protocol");

    // Advance the WAL LSN and retry.
    bpm.set_flushed_wal_lsn(500);
    let _p = bpm.fetch_page(2).unwrap();
    bpm.unpin_page(2, false).unwrap();

    // Page 1 was evicted; page 2 is now resident.
    assert!(bpm.pin_count(1).is_none());
    assert!(bpm.pin_count(2).is_some());

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Checksum validation on fetch
// ---------------------------------------------------------------------------

#[test]
fn checksum_validation_on_fetch() {
    let dir = test_dir("checksum_fetch");
    let dm = Arc::new(DiskManager::new(&dir).unwrap());

    let page_id = dm.allocate_page().unwrap();
    let page = Page::new(page_id, PageType::HeapData);
    dm.write_page(page_id, &page).unwrap();

    // Corrupt a byte in the page's data region directly on disk.
    {
        use std::io::{Seek, SeekFrom, Write};
        let db_path = dir.join("sqld.db");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&db_path)
            .unwrap();
        let file_offset = page_id as u64 * PAGE_SIZE as u64 + 100;
        file.seek(SeekFrom::Start(file_offset)).unwrap();
        file.write_all(&[0xFF]).unwrap();
        file.flush().unwrap();
    }

    let bpm = BufferPoolManager::new(4, 2, dm);
    let result = bpm.fetch_page(page_id);
    assert!(result.is_err());
    match result.unwrap_err() {
        Error::Storage(StorageError::CorruptedPage { .. }) => {}
        other => panic!("expected CorruptedPage, got: {other:?}"),
    }

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// new_page creates and pins
// ---------------------------------------------------------------------------

#[test]
fn new_page_creates_and_pins() {
    let (dir, _dm, bpm) = setup("new_page", 4, 2, 0);

    let (page_id, page) = bpm.new_page(PageType::HeapData).unwrap();
    assert_ne!(page_id, 0);
    assert_eq!(page.page_id(), page_id);
    assert_eq!(bpm.pin_count(page_id), Some(1));
    assert_eq!(bpm.is_dirty(page_id), Some(true)); // new pages are dirty

    bpm.unpin_page(page_id, false).unwrap();
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// delete_page removes from pool
// ---------------------------------------------------------------------------

#[test]
fn delete_page_removes_from_pool() {
    let (dir, _dm, bpm) = setup("delete_page", 4, 2, 0);

    let (page_id, _) = bpm.new_page(PageType::HeapData).unwrap();
    bpm.unpin_page(page_id, false).unwrap();

    bpm.delete_page(page_id).unwrap();
    assert!(bpm.pin_count(page_id).is_none());
    assert_eq!(bpm.size(), 0);

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Prefetch pages (read-ahead)
// ---------------------------------------------------------------------------

#[test]
fn prefetch_pages_load_ahead() {
    let (dir, _dm, bpm) = setup("prefetch", 10, 2, 8);

    let loaded = bpm.prefetch_pages(1, 5).unwrap();
    assert_eq!(loaded, 5);

    // Prefetched pages are in the pool but not pinned.
    for pid in 1..=5u32 {
        assert_eq!(bpm.pin_count(pid), Some(0));
    }

    // Fetching a prefetched page is a pool hit, not a disk read.
    let _p = bpm.fetch_page(3).unwrap();
    assert_eq!(bpm.pin_count(3), Some(1));
    bpm.unpin_page(3, false).unwrap();

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// flush_all_pages writes all dirty pages
// ---------------------------------------------------------------------------

#[test]
fn flush_all_pages_writes_dirty() {
    let (dir, dm, bpm) = setup("flush_all", 4, 2, 3);

    // Modify pages 1 and 2, leave page 3 clean.
    let mut p1 = bpm.fetch_page(1).unwrap();
    p1.insert_tuple(b"data1").unwrap();
    bpm.write_page(1, p1).unwrap();

    let mut p2 = bpm.fetch_page(2).unwrap();
    p2.insert_tuple(b"data2").unwrap();
    bpm.write_page(2, p2).unwrap();

    bpm.fetch_page(3).unwrap();

    bpm.flush_all_pages().unwrap();

    assert_eq!(bpm.is_dirty(1), Some(false));
    assert_eq!(bpm.is_dirty(2), Some(false));
    assert_eq!(bpm.is_dirty(3), Some(false));

    // Verify on disk.
    let d1 = dm.read_page(1).unwrap();
    assert_eq!(d1.fetch_tuple(0).unwrap(), b"data1");
    let d2 = dm.read_page(2).unwrap();
    assert_eq!(d2.fetch_tuple(0).unwrap(), b"data2");

    bpm.unpin_page(1, false).unwrap();
    bpm.unpin_page(2, false).unwrap();
    bpm.unpin_page(3, false).unwrap();
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Dirty eviction writes to disk
// ---------------------------------------------------------------------------

#[test]
fn dirty_eviction_writes_to_disk() {
    let (dir, dm, bpm) = setup("dirty_evict", 2, 2, 4);

    // Fill pool with page 1 (dirty) and page 2 (clean).
    let mut p1 = bpm.fetch_page(1).unwrap();
    p1.insert_tuple(b"persisted").unwrap();
    bpm.write_page(1, p1).unwrap();
    bpm.unpin_page(1, false).unwrap();

    bpm.fetch_page(2).unwrap();
    bpm.unpin_page(2, false).unwrap();

    // Fetch page 3 → one of the two is evicted. If page 1 is evicted, its
    // dirty data must have been written back.
    bpm.fetch_page(3).unwrap();
    bpm.unpin_page(3, false).unwrap();

    // Regardless of which page was evicted, page 1's data must be on disk.
    let d1 = dm.read_page(1).unwrap();
    assert_eq!(d1.fetch_tuple(0).unwrap(), b"persisted");

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Configurable pool size
// ---------------------------------------------------------------------------

#[test]
fn configurable_pool_size() {
    let (dir, _dm, bpm) = setup("pool_size", 16, 2, 0);
    assert_eq!(bpm.pool_size(), 16);

    // Default constructor uses 32768.
    let dm2 = Arc::new(DiskManager::new(test_dir("pool_size_default")).unwrap());
    let bpm2 = BufferPoolManager::with_defaults(dm2);
    assert_eq!(bpm2.pool_size(), 32768);

    cleanup(&dir);
    let _ = std::fs::remove_dir_all(test_dir("pool_size_default"));
}

// ---------------------------------------------------------------------------
// Multiple fetch/unpin cycles
// ---------------------------------------------------------------------------

#[test]
fn multiple_fetch_unpin_cycles() {
    let (dir, _dm, bpm) = setup("multi_cycle", 2, 2, 5);

    // Repeatedly cycle through more pages than the pool can hold.
    for round in 0..3 {
        for pid in 1..=5u32 {
            let _p = bpm.fetch_page(pid).unwrap();
            bpm.unpin_page(pid, false).unwrap();
        }
        assert!(bpm.size() <= 2, "round {round}: pool should hold ≤ 2 pages");
    }

    cleanup(&dir);
}
