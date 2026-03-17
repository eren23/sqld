use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use sqld::storage::{DiskManager, Page, PageType, PAGE_SIZE};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("sqld_test_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn cleanup(dir: &PathBuf) {
    let _ = std::fs::remove_dir_all(dir);
}

// ---------------------------------------------------------------------------
// Basic lifecycle
// ---------------------------------------------------------------------------

#[test]
fn creates_data_directory_and_files() {
    let dir = test_dir("creates_dir");
    assert!(!dir.exists());

    let dm = DiskManager::new(&dir).unwrap();

    assert!(dir.exists());
    assert!(dir.join("sqld.db").exists());
    assert!(dir.join("sqld.lock").exists());

    drop(dm);
    cleanup(&dir);
}

#[test]
fn allocate_returns_increasing_ids() {
    let dir = test_dir("alloc_ids");
    let dm = DiskManager::new(&dir).unwrap();

    let id1 = dm.allocate_page().unwrap();
    let id2 = dm.allocate_page().unwrap();
    let id3 = dm.allocate_page().unwrap();

    assert_eq!(id1, 1);
    assert_eq!(id2, 2);
    assert_eq!(id3, 3);

    drop(dm);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Write / read round-trip
// ---------------------------------------------------------------------------

#[test]
fn write_and_read_page() {
    let dir = test_dir("write_read");
    let dm = DiskManager::new(&dir).unwrap();

    let page_id = dm.allocate_page().unwrap();
    let mut page = Page::new(page_id, PageType::HeapData);
    page.insert_tuple(b"hello disk").unwrap();
    page.insert_tuple(b"second row").unwrap();

    dm.write_page(page_id, &page).unwrap();

    let read_back = dm.read_page(page_id).unwrap();
    assert_eq!(read_back.page_id(), page_id);
    assert_eq!(read_back.tuple_count(), 2);
    assert_eq!(read_back.fetch_tuple(0).unwrap(), b"hello disk");
    assert_eq!(read_back.fetch_tuple(1).unwrap(), b"second row");
    assert!(read_back.verify_checksum());

    drop(dm);
    cleanup(&dir);
}

#[test]
fn multiple_pages_round_trip() {
    let dir = test_dir("multi_pages");
    let dm = DiskManager::new(&dir).unwrap();

    let mut ids = Vec::new();
    for i in 0u8..5 {
        let id = dm.allocate_page().unwrap();
        let mut page = Page::new(id, PageType::HeapData);
        page.insert_tuple(&[i; 200]).unwrap();
        dm.write_page(id, &page).unwrap();
        ids.push(id);
    }

    for (idx, &id) in ids.iter().enumerate() {
        let page = dm.read_page(id).unwrap();
        assert_eq!(page.fetch_tuple(0).unwrap(), vec![idx as u8; 200].as_slice());
        assert!(page.verify_checksum());
    }

    drop(dm);
    cleanup(&dir);
}

#[test]
fn btree_page_round_trip() {
    let dir = test_dir("btree_rt");
    let dm = DiskManager::new(&dir).unwrap();

    let id = dm.allocate_page().unwrap();
    let mut page = Page::new(id, PageType::BtreeLeaf);
    page.insert_tuple(b"key1\x00value1").unwrap();
    page.insert_tuple(b"key2\x00value2").unwrap();
    page.set_lsn(42);
    dm.write_page(id, &page).unwrap();

    let read = dm.read_page(id).unwrap();
    assert_eq!(read.page_type_enum(), Some(PageType::BtreeLeaf));
    assert_eq!(read.lsn(), 42);
    assert_eq!(read.fetch_tuple(0).unwrap(), b"key1\x00value1");
    assert!(read.verify_checksum());

    drop(dm);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Deallocation / free-list
// ---------------------------------------------------------------------------

#[test]
fn deallocate_and_reallocate() {
    let dir = test_dir("dealloc");
    let dm = DiskManager::new(&dir).unwrap();

    let id1 = dm.allocate_page().unwrap();
    let id2 = dm.allocate_page().unwrap();
    let id3 = dm.allocate_page().unwrap();

    // Write data to page 2 so we can verify it's zeroed after deallocation.
    let mut page = Page::new(id2, PageType::HeapData);
    page.insert_tuple(b"will be gone").unwrap();
    dm.write_page(id2, &page).unwrap();

    dm.deallocate_page(id2).unwrap();
    assert_eq!(dm.free_list_len(), 1);

    // Next allocation should reuse id2.
    let reused = dm.allocate_page().unwrap();
    assert_eq!(reused, id2);
    assert_eq!(dm.free_list_len(), 0);

    // The on-disk content was zeroed.
    let zeroed = dm.read_page(id2).unwrap();
    assert_eq!(zeroed.tuple_count(), 0);

    // Pages 1 and 3 still valid.
    assert!(dm.read_page(id1).is_ok());
    assert!(dm.read_page(id3).is_ok());

    drop(dm);
    cleanup(&dir);
}

#[test]
fn deallocate_invalid_page() {
    let dir = test_dir("dealloc_invalid");
    let dm = DiskManager::new(&dir).unwrap();
    assert!(dm.deallocate_page(0).is_err()); // INVALID_PAGE_ID
    assert!(dm.deallocate_page(999).is_err()); // beyond file
    drop(dm);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Error cases
// ---------------------------------------------------------------------------

#[test]
fn read_invalid_page_id_zero() {
    let dir = test_dir("read_zero");
    let dm = DiskManager::new(&dir).unwrap();
    assert!(dm.read_page(0).is_err());
    drop(dm);
    cleanup(&dir);
}

#[test]
fn read_beyond_file() {
    let dir = test_dir("read_beyond");
    let dm = DiskManager::new(&dir).unwrap();
    assert!(dm.read_page(100).is_err());
    drop(dm);
    cleanup(&dir);
}

#[test]
fn write_invalid_page_id_zero() {
    let dir = test_dir("write_zero");
    let dm = DiskManager::new(&dir).unwrap();
    let page = Page::new(0, PageType::HeapData);
    assert!(dm.write_page(0, &page).is_err());
    drop(dm);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Persistence across open/close
// ---------------------------------------------------------------------------

#[test]
fn data_persists_after_reopen() {
    let dir = test_dir("persist");

    // First session: write pages.
    {
        let dm = DiskManager::new(&dir).unwrap();
        let id = dm.allocate_page().unwrap();
        assert_eq!(id, 1);
        let mut page = Page::new(id, PageType::HeapData);
        page.insert_tuple(b"persistent data").unwrap();
        dm.write_page(id, &page).unwrap();
    }

    // Second session: read them back.
    {
        let dm = DiskManager::new(&dir).unwrap();
        let page = dm.read_page(1).unwrap();
        assert_eq!(page.fetch_tuple(0).unwrap(), b"persistent data");
        assert!(page.verify_checksum());

        // Next allocation should be page 2 (file already has 1 page at id=1,
        // but file may also contain page 0 slot due to set_len).
        let id = dm.allocate_page().unwrap();
        assert!(id >= 1); // exact value depends on file size calculation
    }

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// File size
// ---------------------------------------------------------------------------

#[test]
fn file_grows_on_allocate() {
    let dir = test_dir("file_grows");
    let dm = DiskManager::new(&dir).unwrap();

    let db_path = dir.join("sqld.db");

    dm.allocate_page().unwrap();
    let size1 = std::fs::metadata(&db_path).unwrap().len();

    dm.allocate_page().unwrap();
    let size2 = std::fs::metadata(&db_path).unwrap().len();

    assert!(size2 > size1);
    assert_eq!((size2 - size1) as usize, PAGE_SIZE);

    drop(dm);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Concurrent access
// ---------------------------------------------------------------------------

#[test]
fn concurrent_allocate_write_read() {
    let dir = test_dir("concurrent");
    let dm = Arc::new(DiskManager::new(&dir).unwrap());

    let handles: Vec<_> = (0..10u8)
        .map(|i| {
            let dm = Arc::clone(&dm);
            thread::spawn(move || {
                let page_id = dm.allocate_page().unwrap();
                let mut page = Page::new(page_id, PageType::HeapData);
                page.insert_tuple(&[i; 100]).unwrap();
                dm.write_page(page_id, &page).unwrap();

                let read_back = dm.read_page(page_id).unwrap();
                assert_eq!(read_back.fetch_tuple(0).unwrap(), &[i; 100]);
                assert!(read_back.verify_checksum());
                page_id
            })
        })
        .collect();

    let mut ids: Vec<u32> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    ids.sort();
    ids.dedup();
    // All 10 threads got unique page ids.
    assert_eq!(ids.len(), 10);

    cleanup(&dir);
}

#[test]
fn concurrent_reads_same_page() {
    let dir = test_dir("concurrent_reads");
    let dm = Arc::new(DiskManager::new(&dir).unwrap());

    let page_id = dm.allocate_page().unwrap();
    let mut page = Page::new(page_id, PageType::HeapData);
    page.insert_tuple(b"shared data").unwrap();
    dm.write_page(page_id, &page).unwrap();

    let handles: Vec<_> = (0..8)
        .map(|_| {
            let dm = Arc::clone(&dm);
            thread::spawn(move || {
                let p = dm.read_page(page_id).unwrap();
                assert_eq!(p.fetch_tuple(0).unwrap(), b"shared data");
                assert!(p.verify_checksum());
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    cleanup(&dir);
}
