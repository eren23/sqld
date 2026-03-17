use std::path::PathBuf;
use std::sync::Arc;

use sqld::storage::buffer_pool::BufferPoolManager;
use sqld::storage::disk_manager::DiskManager;
use sqld::storage::toast::{ToastPointer, ToastTable, TOAST_CHUNK_SIZE, TOAST_POINTER_TAG, TOAST_THRESHOLD};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("sqld_test_toast_{name}_{}", std::process::id()));
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

fn make_data(size: usize) -> Vec<u8> {
    (0..size).map(|i| (i % 256) as u8).collect()
}

// ---------------------------------------------------------------------------
// ToastPointer serialization
// ---------------------------------------------------------------------------

#[test]
fn toast_pointer_roundtrip() {
    let ptr = ToastPointer::new(42, 7, 10240);
    let bytes = ptr.serialize();
    assert_eq!(bytes.len(), 13);
    assert_eq!(bytes[0], TOAST_POINTER_TAG);

    let decoded = ToastPointer::deserialize(&bytes).unwrap();
    assert_eq!(decoded, ptr);
}

#[test]
fn toast_pointer_detection() {
    let ptr = ToastPointer::new(1, 1, 100);
    let bytes = ptr.serialize();
    assert!(ToastPointer::is_toast_pointer(&bytes));

    // Regular data should not be detected as a TOAST pointer.
    assert!(!ToastPointer::is_toast_pointer(&[0x00, 0x01, 0x02]));
    assert!(!ToastPointer::is_toast_pointer(&[]));
}

#[test]
fn toast_pointer_deserialize_too_short() {
    assert!(ToastPointer::deserialize(&[TOAST_POINTER_TAG; 5]).is_none());
    assert!(ToastPointer::deserialize(&[]).is_none());
}

// ---------------------------------------------------------------------------
// needs_toast threshold
// ---------------------------------------------------------------------------

#[test]
fn needs_toast_boundary() {
    // Exactly at threshold: does NOT need TOAST.
    assert!(!ToastTable::needs_toast(&make_data(TOAST_THRESHOLD)));
    // One byte over: needs TOAST.
    assert!(ToastTable::needs_toast(&make_data(TOAST_THRESHOLD + 1)));
    // Well under.
    assert!(!ToastTable::needs_toast(&make_data(100)));
}

// ---------------------------------------------------------------------------
// Store and retrieve: exactly at 2KB boundary
// ---------------------------------------------------------------------------

#[test]
fn store_retrieve_at_boundary() {
    let dir = test_dir("boundary");
    let pool = make_pool(&dir);
    let mut toast = ToastTable::new(pool.clone(), 1);

    // 2049 bytes — just over threshold, should produce 2 chunks.
    let data = make_data(TOAST_THRESHOLD + 1);
    let ptr = toast.store(&data).unwrap();
    assert_eq!(ptr.total_length, data.len() as u32);
    assert_eq!(ptr.toast_table_id, 1);

    let retrieved = toast.retrieve(&ptr).unwrap();
    assert_eq!(retrieved, data);

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Store and retrieve: 5KB value
// ---------------------------------------------------------------------------

#[test]
fn store_retrieve_5kb() {
    let dir = test_dir("5kb");
    let pool = make_pool(&dir);
    let mut toast = ToastTable::new(pool.clone(), 1);

    let data = make_data(5 * 1024);
    let ptr = toast.store(&data).unwrap();
    assert_eq!(ptr.total_length, 5120);

    let retrieved = toast.retrieve(&ptr).unwrap();
    assert_eq!(retrieved, data);

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Store and retrieve: 10KB value
// ---------------------------------------------------------------------------

#[test]
fn store_retrieve_10kb() {
    let dir = test_dir("10kb");
    let pool = make_pool(&dir);
    let mut toast = ToastTable::new(pool.clone(), 1);

    let data = make_data(10 * 1024);
    let ptr = toast.store(&data).unwrap();
    assert_eq!(ptr.total_length, 10240);

    let retrieved = toast.retrieve(&ptr).unwrap();
    assert_eq!(retrieved, data);

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Multiple values stored independently
// ---------------------------------------------------------------------------

#[test]
fn multiple_values() {
    let dir = test_dir("multi");
    let pool = make_pool(&dir);
    let mut toast = ToastTable::new(pool.clone(), 1);

    let data1 = make_data(5000);
    let data2 = make_data(8000);
    let data3 = make_data(3000);

    let ptr1 = toast.store(&data1).unwrap();
    let ptr2 = toast.store(&data2).unwrap();
    let ptr3 = toast.store(&data3).unwrap();

    // Each should have a unique chunk_id.
    assert_ne!(ptr1.chunk_id, ptr2.chunk_id);
    assert_ne!(ptr2.chunk_id, ptr3.chunk_id);

    // All should be retrievable.
    assert_eq!(toast.retrieve(&ptr1).unwrap(), data1);
    assert_eq!(toast.retrieve(&ptr2).unwrap(), data2);
    assert_eq!(toast.retrieve(&ptr3).unwrap(), data3);

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Delete TOAST data
// ---------------------------------------------------------------------------

#[test]
fn delete_toast_data() {
    let dir = test_dir("delete");
    let pool = make_pool(&dir);
    let mut toast = ToastTable::new(pool.clone(), 1);

    let data = make_data(5000);
    let ptr = toast.store(&data).unwrap();

    // Should retrieve fine before delete.
    assert_eq!(toast.retrieve(&ptr).unwrap(), data);

    // Delete the chunks.
    toast.delete(&ptr).unwrap();

    // After delete, retrieve should fail (data length mismatch — 0 != 5000).
    let result = toast.retrieve(&ptr);
    assert!(result.is_err());

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Table id mismatch
// ---------------------------------------------------------------------------

#[test]
fn table_id_mismatch() {
    let dir = test_dir("mismatch");
    let pool = make_pool(&dir);
    let toast = ToastTable::new(pool.clone(), 1);

    // Try to retrieve with wrong table id.
    let bad_ptr = ToastPointer::new(99, 1, 100);
    assert!(toast.retrieve(&bad_ptr).is_err());
    assert!(toast.delete(&bad_ptr).is_err());

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Inline storage below threshold
// ---------------------------------------------------------------------------

#[test]
fn inline_storage_below_threshold() {
    // Values at or below 2KB should NOT be toasted.
    let small_data = make_data(2048);
    assert!(!ToastTable::needs_toast(&small_data));

    // Only values over 2KB require TOAST.
    let big_data = make_data(2049);
    assert!(ToastTable::needs_toast(&big_data));
}

// ---------------------------------------------------------------------------
// Chunk count verification
// ---------------------------------------------------------------------------

#[test]
fn chunk_count_for_known_sizes() {
    // 5KB = 5120 bytes, chunk size = 2048 → ceil(5120/2048) = 3 chunks
    let expected_5kb = (5120 + TOAST_CHUNK_SIZE - 1) / TOAST_CHUNK_SIZE;
    assert_eq!(expected_5kb, 3);

    // 10KB = 10240 bytes → ceil(10240/2048) = 5 chunks
    let expected_10kb = (10240 + TOAST_CHUNK_SIZE - 1) / TOAST_CHUNK_SIZE;
    assert_eq!(expected_10kb, 5);
}
