use std::path::PathBuf;

use sqld::wal::{
    CheckpointManager, MemoryPageStore, NoOpFlusher, RecoveryManager, WalManager, WalRecord,
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

// ---------------------------------------------------------------------------
// Checkpoint writes CheckpointBegin and CheckpointEnd records to WAL
// ---------------------------------------------------------------------------

#[test]
fn checkpoint_writes_begin_and_end_records() {
    let dir = test_dir("cp_begin_end");
    let wal = WalManager::open(&dir).unwrap();
    let flusher = NoOpFlusher;

    // Write a committed transaction so the WAL is non-empty.
    wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
    wal.append(WalRecord::InsertTuple {
        txn_id: 1,
        page_id: 10,
        slot_index: 0,
        data: vec![1, 2, 3],
    })
    .unwrap();
    wal.commit(1).unwrap();

    // Perform a checkpoint.
    let begin_lsn = CheckpointManager::checkpoint(&wal, &flusher).unwrap();

    // Read all entries and look for CheckpointBegin / CheckpointEnd.
    let entries = wal.read_all_entries().unwrap();

    let cp_begin = entries
        .iter()
        .find(|e| matches!(e.record, WalRecord::CheckpointBegin { .. }));
    assert!(cp_begin.is_some(), "CheckpointBegin record not found");
    assert_eq!(cp_begin.unwrap().lsn, begin_lsn);

    let cp_end = entries
        .iter()
        .find(|e| matches!(e.record, WalRecord::CheckpointEnd { .. }));
    assert!(cp_end.is_some(), "CheckpointEnd record not found");

    // CheckpointEnd should reference the begin LSN.
    if let WalRecord::CheckpointEnd {
        checkpoint_begin_lsn,
    } = &cp_end.unwrap().record
    {
        assert_eq!(*checkpoint_begin_lsn, begin_lsn);
    } else {
        panic!("expected CheckpointEnd variant");
    }

    drop(wal);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Checkpoint sets last_checkpoint_lsn correctly
// ---------------------------------------------------------------------------

#[test]
fn checkpoint_sets_last_checkpoint_lsn() {
    let dir = test_dir("cp_sets_lsn");
    let wal = WalManager::open(&dir).unwrap();
    let flusher = NoOpFlusher;

    // Initially no checkpoint.
    assert_eq!(wal.last_checkpoint_lsn(), 0);

    // Write some data and checkpoint.
    wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
    wal.commit(1).unwrap();

    let begin_lsn = CheckpointManager::checkpoint(&wal, &flusher).unwrap();

    assert_eq!(wal.last_checkpoint_lsn(), begin_lsn);

    // Reopen WAL and verify the checkpoint LSN was persisted.
    drop(wal);
    let wal2 = WalManager::open(&dir).unwrap();
    assert_eq!(wal2.last_checkpoint_lsn(), begin_lsn);

    drop(wal2);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// checkpoint_and_truncate removes old records before checkpoint
// ---------------------------------------------------------------------------

#[test]
fn checkpoint_and_truncate_removes_old_records() {
    let dir = test_dir("cp_truncate");
    let wal = WalManager::open(&dir).unwrap();
    let flusher = NoOpFlusher;

    // Write a committed transaction.
    wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
    wal.append(WalRecord::InsertTuple {
        txn_id: 1,
        page_id: 10,
        slot_index: 0,
        data: vec![10, 20, 30],
    })
    .unwrap();
    wal.commit(1).unwrap();

    // Record how many entries exist before checkpoint.
    wal.flush().unwrap();
    let entries_before = wal.read_all_entries().unwrap();
    assert!(
        entries_before.len() >= 3,
        "expected at least Begin + Insert + Commit"
    );

    // Checkpoint and truncate.
    let begin_lsn = CheckpointManager::checkpoint_and_truncate(&wal, &flusher).unwrap();

    // After truncation, only entries from the checkpoint onward should remain.
    let entries_after = wal.read_all_entries().unwrap();

    // Every remaining entry should have an LSN >= begin_lsn.
    for entry in &entries_after {
        assert!(
            entry.lsn >= begin_lsn,
            "found entry with LSN {} which is before checkpoint LSN {}",
            entry.lsn,
            begin_lsn
        );
    }

    // The original pre-checkpoint entries (Begin, Insert, Commit for txn 1) should be gone.
    let old_begin = entries_after
        .iter()
        .find(|e| matches!(e.record, WalRecord::Begin { txn_id: 1 }));
    assert!(
        old_begin.is_none(),
        "old Begin record for txn 1 should have been truncated"
    );

    drop(wal);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Recovery after checkpoint: crash and reopen
// ---------------------------------------------------------------------------

#[test]
fn recovery_after_checkpoint_replays_from_checkpoint() {
    let dir = test_dir("cp_recovery");
    let flusher = NoOpFlusher;

    {
        let wal = WalManager::open(&dir).unwrap();

        // Phase 1: Write and commit txn 1 (pre-checkpoint data).
        wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
        wal.append(WalRecord::InsertTuple {
            txn_id: 1,
            page_id: 10,
            slot_index: 0,
            data: vec![0xAA],
        })
        .unwrap();
        wal.commit(1).unwrap();

        // Checkpoint.
        CheckpointManager::checkpoint(&wal, &flusher).unwrap();

        // Phase 2: Write and commit txn 2 (post-checkpoint data).
        wal.append(WalRecord::Begin { txn_id: 2 }).unwrap();
        wal.append(WalRecord::InsertTuple {
            txn_id: 2,
            page_id: 20,
            slot_index: 1,
            data: vec![0xBB],
        })
        .unwrap();
        wal.commit(2).unwrap();

        // Simulate crash by dropping the WAL manager.
    }

    // Reopen WAL (simulating restart after crash).
    let wal = WalManager::open(&dir).unwrap();
    let mut store = MemoryPageStore::new();

    let state = RecoveryManager::recover(&wal, &mut store).unwrap();

    // Txn 2 (post-checkpoint) should have been redone: page 20, slot 1 -> [0xBB].
    let slot_data = store.get_slot(20, 1);
    assert!(slot_data.is_some(), "page 20 slot 1 should exist after recovery");
    assert_eq!(slot_data.unwrap(), &vec![0xBB]);

    // Txn 2 was committed after checkpoint and must be in the recovery state.
    let txn2_state = state.active_txn_table.get(&2);
    assert!(
        txn2_state.is_some(),
        "txn 2 should appear in recovery state"
    );
    assert_eq!(
        txn2_state.unwrap().status,
        sqld::wal::TxnStatus::Committed
    );

    // Txn 1 was fully committed before the checkpoint. Analysis starts from
    // the checkpoint, so txn 1's records are before the scan window. In a
    // real system the checkpoint flusher would have written those pages to
    // disk already. Recovery does not re-apply them, which is correct.
    // We verify txn 1 is NOT in the recovery active_txn_table.
    assert!(
        !state.active_txn_table.contains_key(&1),
        "txn 1 (pre-checkpoint) should not appear in recovery state"
    );

    // The checkpoint LSN should have survived the crash.
    assert!(
        wal.last_checkpoint_lsn() > 0,
        "checkpoint LSN should be non-zero after reopen"
    );

    drop(wal);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Checkpoint captures active (uncommitted) transactions
// ---------------------------------------------------------------------------

#[test]
fn checkpoint_captures_active_transactions() {
    let dir = test_dir("cp_active_txns");
    let wal = WalManager::open(&dir).unwrap();
    let flusher = NoOpFlusher;

    // Begin txn 1 but do NOT commit it.
    wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
    wal.append(WalRecord::InsertTuple {
        txn_id: 1,
        page_id: 5,
        slot_index: 0,
        data: vec![42],
    })
    .unwrap();

    // Commit txn 2 so it is NOT active at checkpoint time.
    wal.append(WalRecord::Begin { txn_id: 2 }).unwrap();
    wal.commit(2).unwrap();

    // Checkpoint while txn 1 is still active.
    CheckpointManager::checkpoint(&wal, &flusher).unwrap();

    // Read back WAL entries and find CheckpointBegin.
    let entries = wal.read_all_entries().unwrap();
    let cp_begin = entries
        .iter()
        .find(|e| matches!(e.record, WalRecord::CheckpointBegin { .. }))
        .expect("CheckpointBegin record should exist");

    if let WalRecord::CheckpointBegin { active_txns } = &cp_begin.record {
        assert!(
            active_txns.contains(&1),
            "active_txns should contain txn 1, got: {:?}",
            active_txns
        );
        assert!(
            !active_txns.contains(&2),
            "active_txns should NOT contain committed txn 2, got: {:?}",
            active_txns
        );
    } else {
        panic!("expected CheckpointBegin variant");
    }

    drop(wal);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Analysis phase uses checkpoint as starting point
// ---------------------------------------------------------------------------

#[test]
fn analysis_uses_checkpoint_as_starting_point() {
    let dir = test_dir("cp_analysis_start");
    let wal = WalManager::open(&dir).unwrap();
    let flusher = NoOpFlusher;

    // Write and commit txn 1 (pre-checkpoint).
    wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
    wal.append(WalRecord::InsertTuple {
        txn_id: 1,
        page_id: 100,
        slot_index: 0,
        data: vec![1],
    })
    .unwrap();
    wal.commit(1).unwrap();

    // Checkpoint.
    let checkpoint_lsn = CheckpointManager::checkpoint(&wal, &flusher).unwrap();

    // Write and commit txn 2 (post-checkpoint).
    wal.append(WalRecord::Begin { txn_id: 2 }).unwrap();
    wal.append(WalRecord::InsertTuple {
        txn_id: 2,
        page_id: 200,
        slot_index: 0,
        data: vec![2],
    })
    .unwrap();
    wal.commit(2).unwrap();

    // Flush and read all entries.
    wal.flush().unwrap();
    let entries = wal.read_all_entries().unwrap();

    // Run analysis starting from checkpoint LSN.
    let state = RecoveryManager::analysis(&entries, checkpoint_lsn);

    // Txn 2 (post-checkpoint) must appear in the active_txn_table.
    assert!(
        state.active_txn_table.contains_key(&2),
        "txn 2 (post-checkpoint) should be in the active transaction table"
    );
    assert_eq!(
        state.active_txn_table.get(&2).unwrap().status,
        sqld::wal::TxnStatus::Committed
    );

    // Page 200 (from txn 2, post-checkpoint) must appear in the dirty page table.
    assert!(
        state.dirty_page_table.contains_key(&200),
        "page 200 should be in the dirty page table"
    );

    // Txn 1 was committed before the checkpoint. Analysis starts scanning at
    // the checkpoint record, so txn 1's Begin/Insert/Commit are before the
    // scan range. Txn 1 should NOT appear in the active_txn_table.
    assert!(
        !state.active_txn_table.contains_key(&1),
        "txn 1 (pre-checkpoint) should NOT be in the active transaction table \
         because analysis starts from the checkpoint LSN"
    );

    // Page 100 (from txn 1, pre-checkpoint) should NOT be in the dirty page
    // table since those records are before the analysis starting point.
    assert!(
        !state.dirty_page_table.contains_key(&100),
        "page 100 should NOT be in the dirty page table because its record \
         is before the checkpoint"
    );

    drop(wal);
    cleanup(&dir);
}
