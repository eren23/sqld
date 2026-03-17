use std::sync::Arc;
use std::thread;

use sqld::storage::hash_index::{HashIndex, TID};
use sqld::types::datum::Datum;

// ---------------------------------------------------------------------------
// 1. Basic insert + lookup
// ---------------------------------------------------------------------------

#[test]
fn insert_and_lookup() {
    let index = HashIndex::new();
    let key = Datum::Integer(42);
    let tid = TID::new(1, 0);

    index.insert(&key, tid).unwrap();
    let results = index.lookup(&key).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0], tid);
}

// ---------------------------------------------------------------------------
// 2. 10 000 distinct keys — all retrievable
// ---------------------------------------------------------------------------

#[test]
fn ten_thousand_distinct_keys() {
    let index = HashIndex::new();

    for i in 0..10_000i32 {
        let key = Datum::Integer(i);
        let tid = TID::new(i as u32 + 1, 0);
        index.insert(&key, tid).unwrap();
    }

    for i in 0..10_000i32 {
        let key = Datum::Integer(i);
        let results = index.lookup(&key).unwrap();
        assert_eq!(results.len(), 1, "key {i} should have exactly 1 TID");
        assert_eq!(results[0], TID::new(i as u32 + 1, 0));
    }
}

// ---------------------------------------------------------------------------
// 3. Bucket split with correct redistribution and directory growth
// ---------------------------------------------------------------------------

#[test]
fn bucket_split_and_directory_growth() {
    let index = HashIndex::new();

    // Initial state: depth 0, 1 directory slot, 1 bucket.
    assert_eq!(index.global_depth(), 0);
    assert_eq!(index.directory_size(), 1);
    assert_eq!(index.num_buckets(), 1);

    // Insert enough entries to force multiple splits.
    for i in 0..1_000i32 {
        let key = Datum::Integer(i);
        let tid = TID::new(i as u32 + 1, 0);
        index.insert(&key, tid).unwrap();
    }

    // Directory must have grown.
    assert!(index.global_depth() > 0, "global depth should have increased");
    assert!(
        index.directory_size() > 1,
        "directory should have more than 1 slot"
    );
    assert!(index.num_buckets() > 1, "should have more than 1 bucket");

    // Directory size == 2^global_depth.
    assert_eq!(index.directory_size(), 1 << index.global_depth());

    // Every entry is still retrievable after splits.
    for i in 0..1_000i32 {
        let key = Datum::Integer(i);
        let results = index.lookup(&key).unwrap();
        assert_eq!(results.len(), 1, "key {i} missing after splits");
        assert_eq!(results[0], TID::new(i as u32 + 1, 0));
    }
}

// ---------------------------------------------------------------------------
// 4. Duplicate key handling (store all TIDs)
// ---------------------------------------------------------------------------

#[test]
fn duplicate_key_handling() {
    let index = HashIndex::new();
    let key = Datum::Integer(42);

    let tid1 = TID::new(1, 0);
    let tid2 = TID::new(2, 0);
    let tid3 = TID::new(3, 0);

    index.insert(&key, tid1).unwrap();
    index.insert(&key, tid2).unwrap();
    index.insert(&key, tid3).unwrap();

    let results = index.lookup(&key).unwrap();
    assert_eq!(results.len(), 3);
    assert!(results.contains(&tid1));
    assert!(results.contains(&tid2));
    assert!(results.contains(&tid3));
}

// ---------------------------------------------------------------------------
// 5. Delete
// ---------------------------------------------------------------------------

#[test]
fn delete() {
    let index = HashIndex::new();
    let key = Datum::Integer(99);
    let tid1 = TID::new(1, 0);
    let tid2 = TID::new(2, 0);

    index.insert(&key, tid1).unwrap();
    index.insert(&key, tid2).unwrap();

    // Remove one TID.
    assert!(index.delete(&key, &tid1).unwrap());

    let results = index.lookup(&key).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0], tid2);

    // Deleting the same entry again returns false.
    assert!(!index.delete(&key, &tid1).unwrap());

    // Remove the last TID.
    assert!(index.delete(&key, &tid2).unwrap());
    assert!(index.lookup(&key).unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// 6. Range scan rejection
// ---------------------------------------------------------------------------

#[test]
fn range_scan_rejected() {
    let index = HashIndex::new();
    let start = Datum::Integer(1);
    let end = Datum::Integer(100);

    let result = index.range_scan(&start, &end);
    assert!(result.is_err(), "range scan should be rejected");
}

// ---------------------------------------------------------------------------
// 7. 8-thread concurrent insert + lookup correctness
// ---------------------------------------------------------------------------

#[test]
fn concurrent_8_thread_insert_lookup() {
    let index = Arc::new(HashIndex::new());
    let keys_per_thread: usize = 1_000;
    let num_threads: usize = 8;

    let mut handles = Vec::new();

    for t in 0..num_threads {
        let idx = index.clone();
        handles.push(thread::spawn(move || {
            let start = t * keys_per_thread;

            // Insert phase.
            for i in start..start + keys_per_thread {
                let key = Datum::Integer(i as i32);
                let tid = TID::new(i as u32 + 1, 0);
                idx.insert(&key, tid).unwrap();
            }

            // Lookup phase — each thread verifies its own keys.
            for i in start..start + keys_per_thread {
                let key = Datum::Integer(i as i32);
                let tids = idx.lookup(&key).unwrap();
                assert!(
                    !tids.is_empty(),
                    "thread {t}: key {i} not found during per-thread verification"
                );
                assert!(
                    tids.contains(&TID::new(i as u32 + 1, 0)),
                    "thread {t}: key {i} has wrong TID"
                );
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // Final verification from the main thread: every key is present exactly once.
    let total_keys = num_threads * keys_per_thread;
    for i in 0..total_keys {
        let key = Datum::Integer(i as i32);
        let tids = index.lookup(&key).unwrap();
        assert_eq!(
            tids.len(),
            1,
            "key {i} should have exactly 1 TID, got {}",
            tids.len()
        );
    }
}
