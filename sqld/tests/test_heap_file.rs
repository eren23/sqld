use std::path::PathBuf;
use std::sync::Arc;

use sqld::storage::buffer_pool::BufferPoolManager;
use sqld::storage::disk_manager::DiskManager;
use sqld::storage::heap_file::HeapFile;
use sqld::types::datum::Datum;
use sqld::types::tuple::{MvccHeader, Tuple};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("sqld_test_heap_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn cleanup(dir: &PathBuf) {
    let _ = std::fs::remove_dir_all(dir);
}

fn make_pool(dir: &PathBuf) -> Arc<BufferPoolManager> {
    let dm = Arc::new(DiskManager::new(dir).unwrap());
    Arc::new(BufferPoolManager::new(64, 2, dm))
}

fn make_tuple(id: i32) -> Tuple {
    Tuple::new(
        MvccHeader::new_insert(1, 0),
        vec![Datum::Integer(id), Datum::Varchar(format!("row_{id}"))],
    )
}

fn make_large_tuple(id: i32, padding_len: usize) -> Tuple {
    Tuple::new(
        MvccHeader::new_insert(1, 0),
        vec![
            Datum::Integer(id),
            Datum::Text("x".repeat(padding_len)),
        ],
    )
}

// ---------------------------------------------------------------------------
// Single insert + fetch
// ---------------------------------------------------------------------------

#[test]
fn single_insert_and_fetch() {
    let dir = test_dir("single_insert");
    let pool = make_pool(&dir);
    let mut heap = HeapFile::new(pool.clone());

    let tuple = make_tuple(42);
    let tid = heap.insert(&tuple, None).unwrap();

    let fetched = heap.fetch(tid).unwrap();
    assert_eq!(fetched.values(), tuple.values());
    assert_eq!(fetched.header.xmin, 1);
    assert_eq!(fetched.header.xmax, 0);

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Bulk insert + fetch
// ---------------------------------------------------------------------------

#[test]
fn bulk_insert_and_fetch() {
    let dir = test_dir("bulk_insert");
    let pool = make_pool(&dir);
    let mut heap = HeapFile::new(pool.clone());

    let count = 100;
    let mut tids = Vec::new();
    for i in 0..count {
        let tuple = make_tuple(i);
        let tid = heap.insert(&tuple, None).unwrap();
        tids.push(tid);
    }

    // Verify all fetches return the correct data.
    for (i, &tid) in tids.iter().enumerate() {
        let fetched = heap.fetch(tid).unwrap();
        assert_eq!(fetched.get(0), Some(&Datum::Integer(i as i32)));
    }

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Sequential scan
// ---------------------------------------------------------------------------

#[test]
fn scan_returns_all_live_tuples() {
    let dir = test_dir("scan");
    let pool = make_pool(&dir);
    let mut heap = HeapFile::new(pool.clone());

    for i in 0..10 {
        heap.insert(&make_tuple(i), None).unwrap();
    }

    let results = heap.scan().unwrap();
    assert_eq!(results.len(), 10);

    // Verify sequential order and content.
    let ids: Vec<i32> = results
        .iter()
        .map(|(_, t)| match t.get(0) {
            Some(Datum::Integer(v)) => *v,
            _ => panic!("expected Integer"),
        })
        .collect();
    assert_eq!(ids, (0..10).collect::<Vec<_>>());

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

#[test]
fn delete_sets_xmax() {
    let dir = test_dir("delete");
    let pool = make_pool(&dir);
    let mut heap = HeapFile::new(pool.clone());

    let tuple = make_tuple(1);
    let tid = heap.insert(&tuple, None).unwrap();

    heap.delete(tid, 99).unwrap();

    let fetched = heap.fetch(tid).unwrap();
    assert_eq!(fetched.header.xmax, 99);
    assert!(fetched.header.is_deleted());

    cleanup(&dir);
}

#[test]
fn scan_includes_deleted_tuples() {
    let dir = test_dir("scan_deleted");
    let pool = make_pool(&dir);
    let mut heap = HeapFile::new(pool.clone());

    let tid0 = heap.insert(&make_tuple(0), None).unwrap();
    let _tid1 = heap.insert(&make_tuple(1), None).unwrap();

    heap.delete(tid0, 10).unwrap();

    // Scan returns all tuples (caller does MVCC filtering).
    let results = heap.scan().unwrap();
    assert_eq!(results.len(), 2);

    // The deleted one has xmax set.
    let (_, deleted) = &results[0];
    assert!(deleted.header.is_deleted());

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Update (delete + insert)
// ---------------------------------------------------------------------------

#[test]
fn update_returns_new_tid() {
    let dir = test_dir("update");
    let pool = make_pool(&dir);
    let mut heap = HeapFile::new(pool.clone());

    let tuple = make_tuple(1);
    let old_tid = heap.insert(&tuple, None).unwrap();

    let new_tuple = Tuple::new(
        MvccHeader::new_insert(2, 0),
        vec![Datum::Integer(1), Datum::Varchar("updated".into())],
    );
    let new_tid = heap.update(old_tid, 2, &new_tuple).unwrap();

    // Old version should be marked deleted.
    let old = heap.fetch(old_tid).unwrap();
    assert!(old.header.is_deleted());

    // New version should be live.
    let new_fetched = heap.fetch(new_tid).unwrap();
    assert!(!new_fetched.header.is_deleted());
    assert_eq!(new_fetched.get(1), Some(&Datum::Varchar("updated".into())));

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Multi-page spanning
// ---------------------------------------------------------------------------

#[test]
fn multi_page_spanning() {
    let dir = test_dir("multi_page");
    let pool = make_pool(&dir);
    let mut heap = HeapFile::new(pool.clone());

    // Insert tuples large enough to span multiple pages.
    // Each tuple ~500 bytes; 8KB page fits ~15 tuples. Insert 50 to force ≥4 pages.
    let count = 50;
    let mut tids = Vec::new();
    for i in 0..count {
        let tuple = make_large_tuple(i, 450);
        let tid = heap.insert(&tuple, None).unwrap();
        tids.push(tid);
    }

    assert!(heap.num_pages() >= 3, "expected multiple pages, got {}", heap.num_pages());

    // Verify all are fetchable.
    for (i, &tid) in tids.iter().enumerate() {
        let fetched = heap.fetch(tid).unwrap();
        assert_eq!(fetched.get(0), Some(&Datum::Integer(i as i32)));
    }

    // Scan should return all.
    let results = heap.scan().unwrap();
    assert_eq!(results.len(), count as usize);

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Free space reuse after vacuum
// ---------------------------------------------------------------------------

#[test]
fn vacuum_reclaims_space() {
    let dir = test_dir("vacuum");
    let pool = make_pool(&dir);
    let mut heap = HeapFile::new(pool.clone());

    // Insert 20 tuples.
    let mut tids = Vec::new();
    for i in 0..20 {
        let tid = heap.insert(&make_tuple(i), None).unwrap();
        tids.push(tid);
    }

    let pages_before = heap.num_pages();

    // Delete half of them.
    for i in (0..20).step_by(2) {
        heap.delete(tids[i], 100).unwrap();
    }

    // Vacuum removes dead tuples.
    let removed = heap.vacuum().unwrap();
    assert_eq!(removed, 10);

    // Scan should now only show 10 live tuples.
    let results = heap.scan().unwrap();
    assert_eq!(results.len(), 10);

    // Insert more tuples — they should reuse freed space without adding pages.
    for i in 100..110 {
        heap.insert(&make_tuple(i), None).unwrap();
    }

    // Should not have needed more pages (space was freed).
    assert_eq!(heap.num_pages(), pages_before);

    cleanup(&dir);
}
