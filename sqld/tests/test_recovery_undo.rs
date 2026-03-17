//! Tests for the ARIES WAL recovery undo phase.
//!
//! Each test writes WAL records simulating transactions that may or may not
//! have committed before a crash, then runs full three-phase recovery and
//! verifies that uncommitted work is correctly undone while committed work is
//! preserved.

use sqld::wal::{
    MemoryPageStore, RecoveryManager, WalManager, WalRecord, TxnStatus,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("sqld_test_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn cleanup(dir: &std::path::PathBuf) {
    let _ = std::fs::remove_dir_all(dir);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// An uncommitted insert should be undone during recovery so that the page
/// store no longer contains the inserted data, and the transaction is marked
/// Aborted.
#[test]
fn test_uncommitted_insert_is_undone() {
    let dir = test_dir("undo_insert");
    let wal = WalManager::open(&dir).unwrap();
    let mut store = MemoryPageStore::new();

    // Txn 1: begin, insert, but never commit.
    wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
    wal.append(WalRecord::InsertTuple {
        txn_id: 1,
        page_id: 1,
        slot_index: 0,
        data: b"hello".to_vec(),
    })
    .unwrap();
    // No commit -- simulate crash.

    let state = RecoveryManager::recover(&wal, &mut store).unwrap();

    // The insert must have been undone: slot 0 on page 1 should be empty.
    assert!(
        store.get_slot(1, 0).is_none(),
        "uncommitted insert should be undone; slot should be empty"
    );

    // Txn 1 must be marked Aborted.
    let txn1 = state.active_txn_table.get(&1).expect("txn 1 should be in ATT");
    assert_eq!(
        txn1.status,
        TxnStatus::Aborted,
        "uncommitted txn should be Aborted after recovery"
    );

    cleanup(&dir);
}

/// An uncommitted delete should be undone so that the previously committed
/// data is restored.
#[test]
fn test_uncommitted_delete_is_undone() {
    let dir = test_dir("undo_delete");
    let wal = WalManager::open(&dir).unwrap();
    let mut store = MemoryPageStore::new();

    let data = b"important".to_vec();

    // Txn 1: insert and commit.
    wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
    wal.append(WalRecord::InsertTuple {
        txn_id: 1,
        page_id: 1,
        slot_index: 0,
        data: data.clone(),
    })
    .unwrap();
    wal.commit(1).unwrap();

    // Txn 2: delete the same slot but do NOT commit.
    wal.append(WalRecord::Begin { txn_id: 2 }).unwrap();
    wal.append(WalRecord::DeleteTuple {
        txn_id: 2,
        page_id: 1,
        slot_index: 0,
        data: data.clone(), // full tuple carried for undo
    })
    .unwrap();
    // No commit -- simulate crash.

    let state = RecoveryManager::recover(&wal, &mut store).unwrap();

    // The delete must have been undone: the committed data should be back.
    let slot = store.get_slot(1, 0).expect("slot should be restored after undo of delete");
    assert_eq!(
        slot.as_slice(),
        b"important",
        "data should be restored to the committed value"
    );

    // Txn 1 committed, txn 2 aborted.
    assert_eq!(
        state.active_txn_table.get(&1).unwrap().status,
        TxnStatus::Committed
    );
    assert_eq!(
        state.active_txn_table.get(&2).unwrap().status,
        TxnStatus::Aborted
    );

    cleanup(&dir);
}

/// An uncommitted update (and the preceding uncommitted insert in the same
/// txn) should both be undone, leaving the slot empty.
#[test]
fn test_uncommitted_update_is_undone() {
    let dir = test_dir("undo_update");
    let wal = WalManager::open(&dir).unwrap();
    let mut store = MemoryPageStore::new();

    // Txn 1: insert then update, but never commit.
    wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
    wal.append(WalRecord::InsertTuple {
        txn_id: 1,
        page_id: 1,
        slot_index: 0,
        data: b"original".to_vec(),
    })
    .unwrap();
    wal.append(WalRecord::UpdateTuple {
        txn_id: 1,
        page_id: 1,
        slot_index: 0,
        old_data: b"original".to_vec(),
        new_data: b"modified".to_vec(),
    })
    .unwrap();
    // No commit.

    let state = RecoveryManager::recover(&wal, &mut store).unwrap();

    // Both the update and the insert are undone (same uncommitted txn), so
    // the slot should be empty.
    assert!(
        store.get_slot(1, 0).is_none(),
        "slot should be empty after full undo of uncommitted insert + update"
    );

    assert_eq!(
        state.active_txn_table.get(&1).unwrap().status,
        TxnStatus::Aborted
    );

    cleanup(&dir);
}

/// Committed work survives recovery while uncommitted work in a different
/// transaction is undone.
#[test]
fn test_mixed_committed_and_uncommitted() {
    let dir = test_dir("undo_mixed");
    let wal = WalManager::open(&dir).unwrap();
    let mut store = MemoryPageStore::new();

    // Txn 1: insert on page 1 slot 0 and commit.
    wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
    wal.append(WalRecord::InsertTuple {
        txn_id: 1,
        page_id: 1,
        slot_index: 0,
        data: b"committed_data".to_vec(),
    })
    .unwrap();
    wal.commit(1).unwrap();

    // Txn 2: insert on page 2 slot 0 but do NOT commit.
    wal.append(WalRecord::Begin { txn_id: 2 }).unwrap();
    wal.append(WalRecord::InsertTuple {
        txn_id: 2,
        page_id: 2,
        slot_index: 0,
        data: b"uncommitted_data".to_vec(),
    })
    .unwrap();
    // No commit.

    let state = RecoveryManager::recover(&wal, &mut store).unwrap();

    // Txn 1's data must be present.
    let slot1 = store
        .get_slot(1, 0)
        .expect("committed insert should survive recovery");
    assert_eq!(slot1.as_slice(), b"committed_data");

    // Txn 2's data must be gone.
    assert!(
        store.get_slot(2, 0).is_none(),
        "uncommitted insert from txn 2 should be undone"
    );

    assert_eq!(
        state.active_txn_table.get(&1).unwrap().status,
        TxnStatus::Committed
    );
    assert_eq!(
        state.active_txn_table.get(&2).unwrap().status,
        TxnStatus::Aborted
    );

    cleanup(&dir);
}

/// The undo phase must write CLR (Compensation Log Record) entries into the
/// WAL for every data record it reverses.
#[test]
fn test_clrs_are_written_during_undo() {
    let dir = test_dir("undo_clr");
    let wal = WalManager::open(&dir).unwrap();
    let mut store = MemoryPageStore::new();

    // Txn 1: two inserts, no commit.
    wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
    wal.append(WalRecord::InsertTuple {
        txn_id: 1,
        page_id: 1,
        slot_index: 0,
        data: b"a".to_vec(),
    })
    .unwrap();
    wal.append(WalRecord::InsertTuple {
        txn_id: 1,
        page_id: 1,
        slot_index: 1,
        data: b"b".to_vec(),
    })
    .unwrap();

    let _state = RecoveryManager::recover(&wal, &mut store).unwrap();

    // Read back all WAL entries (recovery flushed its CLRs).
    let entries = wal.read_all_entries().unwrap();

    let clr_count = entries
        .iter()
        .filter(|e| matches!(e.record, WalRecord::Clr { .. }))
        .count();

    // We had two undoable records (two inserts), so there should be two CLRs.
    assert_eq!(
        clr_count, 2,
        "expected 2 CLR records in the WAL after undoing 2 inserts, found {clr_count}"
    );

    // Each CLR's inner redo should be a DeleteTuple (the inverse of Insert).
    for entry in &entries {
        if let WalRecord::Clr { redo, txn_id, .. } = &entry.record {
            assert_eq!(*txn_id, 1, "CLR should belong to txn 1");
            assert!(
                matches!(redo.as_ref(), WalRecord::DeleteTuple { .. }),
                "CLR redo for an undone insert should be a DeleteTuple"
            );
        }
    }

    cleanup(&dir);
}

/// The undo phase must append an Abort record for each transaction it undoes.
#[test]
fn test_undo_writes_abort_records() {
    let dir = test_dir("undo_abort_rec");
    let wal = WalManager::open(&dir).unwrap();
    let mut store = MemoryPageStore::new();

    // Two uncommitted transactions.
    wal.append(WalRecord::Begin { txn_id: 10 }).unwrap();
    wal.append(WalRecord::InsertTuple {
        txn_id: 10,
        page_id: 1,
        slot_index: 0,
        data: b"x".to_vec(),
    })
    .unwrap();

    wal.append(WalRecord::Begin { txn_id: 20 }).unwrap();
    wal.append(WalRecord::InsertTuple {
        txn_id: 20,
        page_id: 2,
        slot_index: 0,
        data: b"y".to_vec(),
    })
    .unwrap();
    // Neither txn commits.

    let _state = RecoveryManager::recover(&wal, &mut store).unwrap();

    let entries = wal.read_all_entries().unwrap();

    // Collect all Abort records written to the WAL.
    let abort_txn_ids: Vec<u64> = entries
        .iter()
        .filter_map(|e| {
            if let WalRecord::Abort { txn_id } = &e.record {
                Some(*txn_id)
            } else {
                None
            }
        })
        .collect();

    assert!(
        abort_txn_ids.contains(&10),
        "WAL should contain an Abort record for txn 10"
    );
    assert!(
        abort_txn_ids.contains(&20),
        "WAL should contain an Abort record for txn 20"
    );

    cleanup(&dir);
}
