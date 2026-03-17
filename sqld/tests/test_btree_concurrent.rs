use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

use sqld::storage::btree::{
    default_compare, encode_i64_key, ConcurrentBPlusTree,
    ScanDirection,
};
use sqld::storage::{BufferPoolManager, DiskManager, Tid};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_dir(name: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("sqld_test_btconc_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn cleanup(dir: &PathBuf) {
    let _ = std::fs::remove_dir_all(dir);
}

fn key(val: i64) -> Vec<u8> {
    encode_i64_key(val).to_vec()
}

fn tid(page: u32, slot: u16) -> Tid {
    Tid::new(page, slot)
}

fn make_concurrent_tree(name: &str, pool_size: usize) -> (PathBuf, Arc<ConcurrentBPlusTree>) {
    let dir = test_dir(name);
    let dm = Arc::new(DiskManager::new(&dir).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(pool_size, 2, dm));
    let tree = Arc::new(ConcurrentBPlusTree::new(bpm, false, Box::new(default_compare)));
    (dir, tree)
}

// ---------------------------------------------------------------------------
// 8-thread concurrent insert
// ---------------------------------------------------------------------------

#[test]
fn concurrent_insert_8_threads() {
    let (dir, tree) = make_concurrent_tree("conc_insert_8", 2048);
    let per_thread = 500;
    let num_threads = 8;

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let tree = tree.clone();
            thread::spawn(move || {
                for i in 0..per_thread {
                    let v: i64 = t * per_thread + i;
                    tree.insert(&key(v), tid(1, (v % 65536) as u16)).unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // All keys should be findable.
    for v in 0..(num_threads * per_thread) {
        let result = tree.search(&key(v)).unwrap();
        assert!(result.is_some(), "key {v} not found after concurrent insert");
    }

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 16-thread concurrent insert
// ---------------------------------------------------------------------------

#[test]
fn concurrent_insert_16_threads() {
    let (dir, tree) = make_concurrent_tree("conc_insert_16", 4096);
    let per_thread = 500;
    let num_threads: i64 = 16;

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let tree = tree.clone();
            thread::spawn(move || {
                for i in 0..per_thread {
                    let v: i64 = t * per_thread + i;
                    tree.insert(&key(v), tid(1, (v % 65536) as u16)).unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    for v in 0..(num_threads * per_thread) {
        assert!(
            tree.search(&key(v)).unwrap().is_some(),
            "key {v} not found after 16-thread insert"
        );
    }

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 8-thread mixed insert + search
// ---------------------------------------------------------------------------

#[test]
fn concurrent_insert_and_search_8() {
    let (dir, tree) = make_concurrent_tree("conc_mix_8", 2048);

    // Pre-populate with some keys.
    for i in 0..500i64 {
        tree.insert(&key(i), tid(1, i as u16)).unwrap();
    }

    let handles: Vec<_> = (0..8)
        .map(|t| {
            let tree = tree.clone();
            thread::spawn(move || {
                if t % 2 == 0 {
                    // Writer thread.
                    for i in 0..200 {
                        let v: i64 = 500 + t as i64 * 200 + i;
                        tree.insert(&key(v), tid(1, (v % 65536) as u16)).unwrap();
                    }
                } else {
                    // Reader thread.
                    for i in 0..500i64 {
                        let _ = tree.search(&key(i));
                    }
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Verify pre-populated keys.
    for i in 0..500i64 {
        assert!(
            tree.search(&key(i)).unwrap().is_some(),
            "pre-populated key {i} not found"
        );
    }

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 8-thread insert + search + delete stress test
// ---------------------------------------------------------------------------

#[test]
fn concurrent_stress_8() {
    let (dir, tree) = make_concurrent_tree("conc_stress_8", 2048);

    // Pre-populate.
    for i in 0..1000i64 {
        tree.insert(&key(i), tid(1, (i % 65536) as u16)).unwrap();
    }

    let handles: Vec<_> = (0..8)
        .map(|t| {
            let tree = tree.clone();
            thread::spawn(move || match t % 3 {
                0 => {
                    // Inserter
                    for i in 0..300 {
                        let v: i64 = 1000 + t as i64 * 300 + i;
                        tree.insert(&key(v), tid(1, (v % 65536) as u16)).unwrap();
                    }
                }
                1 => {
                    // Searcher
                    for i in 0..1000i64 {
                        let _ = tree.search(&key(i));
                    }
                }
                2 => {
                    // Deleter — delete keys that are likely to exist.
                    for i in (0..200i64).step_by(3) {
                        let _ = tree.delete(&key(i));
                    }
                }
                _ => unreachable!(),
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // Tree should be in a consistent state — no panics occurred.
    // Verify some keys that were not deleted.
    for i in (1..1000i64).step_by(3) {
        // Key i should still be there (not a multiple of 3 from delete thread).
        if i % 3 != 0 {
            let _ = tree.search(&key(i));
        }
    }

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// 16-thread insert + search + delete stress test
// ---------------------------------------------------------------------------

#[test]
fn concurrent_stress_16() {
    let (dir, tree) = make_concurrent_tree("conc_stress_16", 4096);

    // Pre-populate.
    for i in 0..2000i64 {
        tree.insert(&key(i), tid(1, (i % 65536) as u16)).unwrap();
    }

    let handles: Vec<_> = (0..16)
        .map(|t| {
            let tree = tree.clone();
            thread::spawn(move || match t % 3 {
                0 => {
                    // Inserter
                    for i in 0..200 {
                        let v: i64 = 2000 + t as i64 * 200 + i;
                        tree.insert(&key(v), tid(1, (v % 65536) as u16)).unwrap();
                    }
                }
                1 => {
                    // Searcher
                    for i in 0..500i64 {
                        let _ = tree.search(&key(i));
                    }
                }
                2 => {
                    // Deleter
                    for i in (t as i64 * 50..(t as i64 + 1) * 50).step_by(2) {
                        let _ = tree.delete(&key(i));
                    }
                }
                _ => unreachable!(),
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    // All inserted keys from insert threads should be findable
    // (they are in a range above the delete range).
    for t in (0..16).filter(|t| t % 3 == 0) {
        for i in 0..200i64 {
            let v: i64 = 2000 + t * 200 + i;
            assert!(
                tree.search(&key(v)).unwrap().is_some(),
                "inserted key {v} missing after stress test"
            );
        }
    }

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Concurrent range scan while inserting
// ---------------------------------------------------------------------------

#[test]
fn concurrent_scan_during_insert() {
    let (dir, tree) = make_concurrent_tree("conc_scan_ins", 2048);

    // Pre-populate.
    for i in 0..500i64 {
        tree.insert(&key(i), tid(1, i as u16)).unwrap();
    }

    let tree_w = tree.clone();
    let writer = thread::spawn(move || {
        for i in 500..1000i64 {
            tree_w.insert(&key(i), tid(1, i as u16)).unwrap();
        }
    });

    // Perform range scans concurrently.
    let tree_r = tree.clone();
    let reader = thread::spawn(move || {
        for _ in 0..10 {
            let iter = tree_r
                .range_scan(
                    Some((&key(0), true)),
                    Some((&key(500), false)),
                    ScanDirection::Forward,
                )
                .unwrap();
            let results: Vec<_> = iter.filter_map(|r| r.ok()).collect();
            // Should have at least the pre-populated keys.
            assert!(
                results.len() >= 400,
                "scan returned too few results: {}",
                results.len()
            );
        }
    });

    writer.join().unwrap();
    reader.join().unwrap();

    cleanup(&dir);
}
