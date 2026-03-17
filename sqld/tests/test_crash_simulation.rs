use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::PathBuf;

use sqld::wal::{
    MemoryPageStore, RecoveryManager, TxnStatus, WalEntry, WalManager, WalRecord,
};

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

/// Read the raw bytes of the WAL file on disk.
fn read_wal_bytes(dir: &PathBuf) -> Vec<u8> {
    let wal_path = dir.join("wal.log");
    let mut f = std::fs::File::open(&wal_path).unwrap();
    let mut data = Vec::new();
    f.read_to_end(&mut data).unwrap();
    data
}

/// Append raw bytes to the end of the WAL file without going through
/// WalManager (simulates partial/torn writes).
fn append_raw_bytes(dir: &PathBuf, data: &[u8]) {
    let wal_path = dir.join("wal.log");
    let mut f = OpenOptions::new().append(true).open(&wal_path).unwrap();
    f.write_all(data).unwrap();
    f.sync_all().unwrap();
}

// ---------------------------------------------------------------------------
// Test: crash before commit flush
// ---------------------------------------------------------------------------

/// Begin a transaction, insert a tuple, append a Commit record but do NOT
/// flush. Drop the WalManager (simulating a process crash). Reopen and
/// recover. The commit was never flushed to disk, so it should be lost and
/// recovery should undo the insert.
#[test]
fn crash_before_commit_flush() {
    let dir = test_dir("crash_before_commit_flush");

    {
        let wal = WalManager::open(&dir).unwrap();

        // Begin transaction 1
        wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();

        // Insert a tuple (flush it so it is on disk)
        wal.append(WalRecord::InsertTuple {
            txn_id: 1,
            page_id: 10,
            slot_index: 0,
            data: b"hello".to_vec(),
        })
        .unwrap();
        wal.flush().unwrap();

        // Append the commit record but do NOT flush -- it stays in the
        // in-memory buffer only.
        wal.append(WalRecord::Commit { txn_id: 1 }).unwrap();

        // Drop without flushing -- simulates a crash. The commit record
        // is lost because it was only in the buffer.
        drop(wal);
    }

    // Reopen and recover
    let wal = WalManager::open(&dir).unwrap();
    let mut store = MemoryPageStore::new();
    let state = RecoveryManager::recover(&wal, &mut store).unwrap();

    // The transaction should be aborted (undo phase rolled it back because
    // the Commit record was not on disk).
    let txn_state = state.active_txn_table.get(&1).expect("txn 1 should be tracked");
    assert_eq!(
        txn_state.status,
        TxnStatus::Aborted,
        "uncommitted txn should be aborted after recovery"
    );

    // The inserted tuple should have been undone -- slot should be gone.
    assert!(
        store.get_slot(10, 0).is_none(),
        "insert should be undone after crash before commit flush"
    );

    drop(wal);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Test: crash after commit flush
// ---------------------------------------------------------------------------

/// Begin a transaction, insert a tuple, and commit (which flushes). Drop
/// the WalManager and reopen. Recovery should preserve the committed data.
#[test]
fn crash_after_commit_flush() {
    let dir = test_dir("crash_after_commit_flush");

    {
        let wal = WalManager::open(&dir).unwrap();

        wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
        wal.append(WalRecord::InsertTuple {
            txn_id: 1,
            page_id: 10,
            slot_index: 0,
            data: b"committed_data".to_vec(),
        })
        .unwrap();
        // commit() appends Commit and flushes
        wal.commit(1).unwrap();

        // Crash (drop)
        drop(wal);
    }

    // Reopen and recover
    let wal = WalManager::open(&dir).unwrap();
    let mut store = MemoryPageStore::new();
    let state = RecoveryManager::recover(&wal, &mut store).unwrap();

    // Transaction should be committed
    let txn_state = state.active_txn_table.get(&1).expect("txn 1 should be tracked");
    assert_eq!(
        txn_state.status,
        TxnStatus::Committed,
        "committed txn should remain committed after recovery"
    );

    // The inserted data should be present (redo phase re-applied it)
    let slot_data = store.get_slot(10, 0).expect("committed slot should exist");
    assert_eq!(slot_data.as_slice(), b"committed_data");

    drop(wal);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Test: torn write recovery
// ---------------------------------------------------------------------------

/// Write several entries and flush, then manually append partial/truncated
/// bytes to the WAL file to simulate a torn write that occurred right before
/// a crash. Reopen and recover -- all complete entries should be recovered
/// and the torn bytes should be silently ignored.
#[test]
fn torn_write_recovery() {
    let dir = test_dir("torn_write");

    {
        let wal = WalManager::open(&dir).unwrap();

        wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
        wal.append(WalRecord::InsertTuple {
            txn_id: 1,
            page_id: 5,
            slot_index: 0,
            data: b"row_a".to_vec(),
        })
        .unwrap();
        wal.commit(1).unwrap();

        drop(wal);
    }

    // Count how many valid entries we have before the torn write
    let original_entries = {
        let data = read_wal_bytes(&dir);
        WalManager::parse_entries(&data).unwrap()
    };
    let valid_count = original_entries.len();
    assert!(valid_count >= 3, "should have at least Begin, Insert, Commit");

    // Append garbage bytes to simulate a torn write (incomplete entry)
    // This is a partial header followed by random data.
    let garbage: Vec<u8> = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03];
    append_raw_bytes(&dir, &garbage);

    // Reopen and recover -- should gracefully handle the torn tail
    let wal = WalManager::open(&dir).unwrap();
    let mut store = MemoryPageStore::new();
    let state = RecoveryManager::recover(&wal, &mut store).unwrap();

    // The committed transaction's data should still be present
    let txn_state = state.active_txn_table.get(&1).expect("txn 1 should be tracked");
    assert_eq!(txn_state.status, TxnStatus::Committed);

    let slot_data = store.get_slot(5, 0).expect("committed data should survive torn write");
    assert_eq!(slot_data.as_slice(), b"row_a");

    // Verify that parse_entries returns exactly the valid entries (ignoring torn bytes)
    let data = read_wal_bytes(&dir);
    let parsed = WalManager::parse_entries(&data).unwrap();
    assert_eq!(
        parsed.len(),
        valid_count,
        "torn bytes should be ignored; only valid entries returned"
    );

    drop(wal);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Test: crash mid-checkpoint (incomplete checkpoint)
// ---------------------------------------------------------------------------

/// Begin a transaction, insert data, flush. Then manually write a
/// CheckpointBegin record to the WAL (by appending raw serialized bytes)
/// WITHOUT writing the matching CheckpointEnd. Drop and reopen. Recovery
/// should handle the incomplete checkpoint gracefully -- the transaction
/// data should still be recoverable.
#[test]
fn crash_mid_checkpoint() {
    let dir = test_dir("crash_mid_checkpoint");

    {
        let wal = WalManager::open(&dir).unwrap();

        wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
        wal.append(WalRecord::InsertTuple {
            txn_id: 1,
            page_id: 20,
            slot_index: 0,
            data: b"checkpoint_test".to_vec(),
        })
        .unwrap();
        wal.commit(1).unwrap();

        drop(wal);
    }

    // Now manually append a CheckpointBegin entry (with no CheckpointEnd)
    // to simulate a crash that happened in the middle of a checkpoint.
    let file_len = {
        let data = read_wal_bytes(&dir);
        data.len() as u64
    };

    // Build a valid CheckpointBegin WAL entry by hand
    let checkpoint_record = WalRecord::CheckpointBegin {
        active_txns: vec![],
    };
    let checkpoint_entry = WalEntry::new(file_len, 0, checkpoint_record);
    let checkpoint_bytes = checkpoint_entry.serialize();
    append_raw_bytes(&dir, &checkpoint_bytes);

    // No CheckpointEnd written -- this simulates a crash mid-checkpoint.

    // Reopen and recover
    let wal = WalManager::open(&dir).unwrap();
    let mut store = MemoryPageStore::new();
    let state = RecoveryManager::recover(&wal, &mut store).unwrap();

    // The committed transaction should still be visible
    let txn_state = state.active_txn_table.get(&1).expect("txn 1 should be tracked");
    assert_eq!(
        txn_state.status,
        TxnStatus::Committed,
        "committed txn should survive incomplete checkpoint"
    );

    let slot_data = store.get_slot(20, 0).expect("data should survive incomplete checkpoint");
    assert_eq!(slot_data.as_slice(), b"checkpoint_test");

    drop(wal);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Test: double recovery is idempotent
// ---------------------------------------------------------------------------

/// Perform recovery once, then perform recovery again on the same WAL.
/// The result should be identical -- recovery must be idempotent.
#[test]
fn double_recovery_idempotent() {
    let dir = test_dir("double_recovery");

    {
        let wal = WalManager::open(&dir).unwrap();

        // Committed transaction
        wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
        wal.append(WalRecord::InsertTuple {
            txn_id: 1,
            page_id: 30,
            slot_index: 0,
            data: b"persist".to_vec(),
        })
        .unwrap();
        wal.commit(1).unwrap();

        // Uncommitted transaction (flushed but not committed)
        wal.append(WalRecord::Begin { txn_id: 2 }).unwrap();
        wal.append(WalRecord::InsertTuple {
            txn_id: 2,
            page_id: 31,
            slot_index: 0,
            data: b"transient".to_vec(),
        })
        .unwrap();
        wal.flush().unwrap();

        drop(wal);
    }

    // First recovery
    let wal1 = WalManager::open(&dir).unwrap();
    let mut store1 = MemoryPageStore::new();
    let state1 = RecoveryManager::recover(&wal1, &mut store1).unwrap();
    drop(wal1);

    // Second recovery (on the WAL that now also contains CLRs and Abort
    // records written by the first recovery)
    let wal2 = WalManager::open(&dir).unwrap();
    let mut store2 = MemoryPageStore::new();
    let state2 = RecoveryManager::recover(&wal2, &mut store2).unwrap();
    drop(wal2);

    // Both recoveries should agree on txn 1 being committed
    let s1_txn1 = state1.active_txn_table.get(&1).expect("txn 1 in first recovery");
    let s2_txn1 = state2.active_txn_table.get(&1).expect("txn 1 in second recovery");
    assert_eq!(s1_txn1.status, TxnStatus::Committed);
    assert_eq!(s2_txn1.status, TxnStatus::Committed);

    // Committed data should be present in both stores
    let d1 = store1.get_slot(30, 0).expect("slot from first recovery");
    let d2 = store2.get_slot(30, 0).expect("slot from second recovery");
    assert_eq!(d1.as_slice(), b"persist");
    assert_eq!(d2.as_slice(), b"persist");

    // Uncommitted data should be absent in both stores
    assert!(
        store1.get_slot(31, 0).is_none(),
        "uncommitted data should be absent after first recovery"
    );
    assert!(
        store2.get_slot(31, 0).is_none(),
        "uncommitted data should be absent after second recovery"
    );

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Test: crash with mixed committed and uncommitted work
// ---------------------------------------------------------------------------

/// Transaction 1 is committed and flushed. Transaction 2 is in progress
/// (flushed to disk but never committed). Simulate a crash. After recovery,
/// txn 1's data should persist while txn 2's data should be undone.
#[test]
fn crash_mixed_committed_uncommitted() {
    let dir = test_dir("crash_mixed");

    {
        let wal = WalManager::open(&dir).unwrap();

        // Transaction 1: committed
        wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
        wal.append(WalRecord::InsertTuple {
            txn_id: 1,
            page_id: 40,
            slot_index: 0,
            data: b"committed_row".to_vec(),
        })
        .unwrap();
        wal.commit(1).unwrap();

        // Transaction 2: in progress, flushed but NOT committed
        wal.append(WalRecord::Begin { txn_id: 2 }).unwrap();
        wal.append(WalRecord::InsertTuple {
            txn_id: 2,
            page_id: 41,
            slot_index: 0,
            data: b"uncommitted_row".to_vec(),
        })
        .unwrap();
        // Flush ensures the records are on disk, but no Commit record
        wal.flush().unwrap();

        // Crash
        drop(wal);
    }

    // Reopen and recover
    let wal = WalManager::open(&dir).unwrap();
    let mut store = MemoryPageStore::new();
    let state = RecoveryManager::recover(&wal, &mut store).unwrap();

    // Txn 1: committed -- data should be present
    let txn1 = state.active_txn_table.get(&1).expect("txn 1 should be tracked");
    assert_eq!(txn1.status, TxnStatus::Committed);
    let slot1 = store.get_slot(40, 0).expect("committed data should exist");
    assert_eq!(slot1.as_slice(), b"committed_row");

    // Txn 2: should be aborted after undo -- data should be gone
    let txn2 = state.active_txn_table.get(&2).expect("txn 2 should be tracked");
    assert_eq!(
        txn2.status,
        TxnStatus::Aborted,
        "uncommitted txn should be aborted"
    );
    assert!(
        store.get_slot(41, 0).is_none(),
        "uncommitted insert should be undone"
    );

    drop(wal);
    cleanup(&dir);
}
