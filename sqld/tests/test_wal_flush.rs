use std::path::PathBuf;

use sqld::wal::{WalManager, WalRecord};

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
// Flush semantics
// ---------------------------------------------------------------------------

#[test]
fn flushed_lsn_lags_current_lsn_before_flush() {
    let dir = test_dir("flush_lag");
    let wal = WalManager::open(&dir).unwrap();

    // Before any writes, both are equal (both zero for an empty WAL)
    assert_eq!(wal.current_lsn(), wal.flushed_lsn());

    // Append without flushing
    wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
    wal.append(WalRecord::InsertTuple {
        txn_id: 1,
        page_id: 10,
        slot_index: 0,
        data: vec![0xAA; 32],
    })
    .unwrap();

    assert!(
        wal.flushed_lsn() < wal.current_lsn(),
        "flushed_lsn ({}) should be behind current_lsn ({}) before flush",
        wal.flushed_lsn(),
        wal.current_lsn()
    );

    // After flush, they should be equal
    wal.flush().unwrap();
    assert_eq!(
        wal.flushed_lsn(),
        wal.current_lsn(),
        "flushed_lsn should equal current_lsn after flush"
    );

    drop(wal);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Commit triggers flush
// ---------------------------------------------------------------------------

#[test]
fn commit_triggers_flush() {
    let dir = test_dir("commit_flush");
    let wal = WalManager::open(&dir).unwrap();

    wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
    wal.append(WalRecord::InsertTuple {
        txn_id: 1,
        page_id: 5,
        slot_index: 0,
        data: vec![1, 2, 3],
    })
    .unwrap();

    // Before commit, data is buffered
    let flushed_before = wal.flushed_lsn();
    assert!(flushed_before < wal.current_lsn());

    // Commit flushes everything
    wal.commit(1).unwrap();
    assert_eq!(
        wal.flushed_lsn(),
        wal.current_lsn(),
        "commit should flush all buffered data"
    );
    assert!(
        wal.flushed_lsn() > flushed_before,
        "flushed_lsn should advance after commit"
    );

    drop(wal);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Group commit: one commit flushes records from multiple transactions
// ---------------------------------------------------------------------------

#[test]
fn group_commit_flushes_all_buffered_transactions() {
    let dir = test_dir("group_commit");
    let wal = WalManager::open(&dir).unwrap();

    // Start 3 transactions and append records for each
    for txn_id in 1..=3 {
        wal.append(WalRecord::Begin { txn_id }).unwrap();
        wal.append(WalRecord::InsertTuple {
            txn_id,
            page_id: txn_id as u32 * 10,
            slot_index: 0,
            data: vec![txn_id as u8; 16],
        })
        .unwrap();
    }

    // Nothing flushed yet
    let flushed_before = wal.flushed_lsn();
    let current_before_commit = wal.current_lsn();
    assert!(flushed_before < current_before_commit);

    // Committing txn 1 will flush ALL buffered records (group commit)
    wal.commit(1).unwrap();

    // The commit itself adds a Commit record, so current_lsn advances further
    // But flushed_lsn should now equal the new current_lsn
    assert_eq!(wal.flushed_lsn(), wal.current_lsn());

    // All records (including txn 2 and 3's Begin + InsertTuple) should be on disk
    let entries = wal.read_all_entries().unwrap();
    // 3 Begin + 3 InsertTuple + 1 Commit = 7 entries
    assert_eq!(
        entries.len(),
        7,
        "expected 7 entries (3 begins + 3 inserts + 1 commit), got {}",
        entries.len()
    );

    // Verify records from all 3 txns are present
    let txn_ids_seen: Vec<Option<u64>> = entries.iter().map(|e| e.record.txn_id()).collect();
    assert!(txn_ids_seen.contains(&Some(1)));
    assert!(txn_ids_seen.contains(&Some(2)));
    assert!(txn_ids_seen.contains(&Some(3)));

    drop(wal);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Entries survive flush and read back
// ---------------------------------------------------------------------------

#[test]
fn entries_survive_flush_and_read_back() {
    let dir = test_dir("flush_readback");
    let wal = WalManager::open(&dir).unwrap();

    wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
    wal.append(WalRecord::InsertTuple {
        txn_id: 1,
        page_id: 42,
        slot_index: 7,
        data: vec![0xDE, 0xAD, 0xBE, 0xEF],
    })
    .unwrap();
    wal.append(WalRecord::Commit { txn_id: 1 }).unwrap();
    wal.flush().unwrap();

    let entries = wal.read_all_entries().unwrap();
    assert_eq!(entries.len(), 3);

    // Verify entry contents
    assert_eq!(entries[0].record, WalRecord::Begin { txn_id: 1 });
    assert_eq!(
        entries[1].record,
        WalRecord::InsertTuple {
            txn_id: 1,
            page_id: 42,
            slot_index: 7,
            data: vec![0xDE, 0xAD, 0xBE, 0xEF],
        }
    );
    assert_eq!(entries[2].record, WalRecord::Commit { txn_id: 1 });

    // All entries should have valid CRCs
    for entry in &entries {
        assert!(entry.verify_crc(), "CRC invalid for entry at LSN {}", entry.lsn);
    }

    drop(wal);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// WAL reopen: flushed entries survive close + reopen
// ---------------------------------------------------------------------------

#[test]
fn wal_reopen_preserves_flushed_entries() {
    let dir = test_dir("wal_reopen");

    // Phase 1: write and flush some entries
    {
        let wal = WalManager::open(&dir).unwrap();
        wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
        wal.append(WalRecord::InsertTuple {
            txn_id: 1,
            page_id: 10,
            slot_index: 0,
            data: vec![1, 2, 3, 4],
        })
        .unwrap();
        wal.commit(1).unwrap();
        // WalManager is dropped here, closing the file
    }

    // Phase 2: reopen and verify entries are still there
    {
        let wal = WalManager::open(&dir).unwrap();
        let entries = wal.read_all_entries().unwrap();
        assert_eq!(
            entries.len(),
            3,
            "expected 3 entries after reopen, got {}",
            entries.len()
        );

        assert_eq!(entries[0].record, WalRecord::Begin { txn_id: 1 });
        assert!(matches!(
            &entries[1].record,
            WalRecord::InsertTuple {
                txn_id: 1,
                page_id: 10,
                slot_index: 0,
                data,
            } if data == &vec![1, 2, 3, 4]
        ));
        assert_eq!(entries[2].record, WalRecord::Commit { txn_id: 1 });

        // New appends should get LSNs after the existing data
        let new_lsn = wal
            .append(WalRecord::Begin { txn_id: 2 })
            .unwrap();
        assert!(
            new_lsn > entries.last().unwrap().lsn,
            "new LSN ({new_lsn}) should be after last existing entry LSN ({})",
            entries.last().unwrap().lsn
        );

        drop(wal);
    }

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Buffer-full auto-flush
// ---------------------------------------------------------------------------

#[test]
fn buffer_full_triggers_auto_flush() {
    let dir = test_dir("auto_flush");
    let wal = WalManager::open(&dir).unwrap();

    let initial_flushed = wal.flushed_lsn();

    // The WAL buffer size is 64 KB. We need to exceed it to trigger auto-flush.
    // Each InsertTuple with 1024 bytes of data produces roughly:
    //   WAL_ENTRY_HEADER_SIZE(20) + tag(1) + txn_id(8) + page_id(4) + slot_index(2)
    //   + data_len(4) + data(1024) + CRC(4) = ~1067 bytes
    // So ~62 records should fill a 64 KB buffer.
    let data_payload = vec![0xBB; 1024];

    let mut appended_count = 0;
    let mut flushed_advanced = false;

    for i in 0..100 {
        wal.append(WalRecord::InsertTuple {
            txn_id: 1,
            page_id: i,
            slot_index: 0,
            data: data_payload.clone(),
        })
        .unwrap();
        appended_count += 1;

        if wal.flushed_lsn() > initial_flushed {
            flushed_advanced = true;
            break;
        }
    }

    assert!(
        flushed_advanced,
        "flushed_lsn should advance due to auto-flush after {appended_count} appends"
    );

    // current_lsn should still be >= flushed_lsn (buffer may have new unflushed data
    // from the entry that triggered the flush, but in practice flush happens before
    // the next append updates current_lsn, so they may be equal)
    assert!(wal.current_lsn() >= wal.flushed_lsn());

    drop(wal);
    cleanup(&dir);
}

#[test]
fn auto_flushed_data_is_readable() {
    let dir = test_dir("auto_flush_read");
    let wal = WalManager::open(&dir).unwrap();

    // Append enough data to trigger auto-flush (>64KB buffer)
    let data_payload = vec![0xCC; 1024];
    for i in 0..80 {
        wal.append(WalRecord::InsertTuple {
            txn_id: 1,
            page_id: i,
            slot_index: 0,
            data: data_payload.clone(),
        })
        .unwrap();
    }

    // Flush any remaining buffered data so we can read everything
    wal.flush().unwrap();

    let entries = wal.read_all_entries().unwrap();
    assert_eq!(entries.len(), 80, "all 80 appended entries should be readable");

    // Verify each entry has valid data
    for (i, entry) in entries.iter().enumerate() {
        assert!(entry.verify_crc(), "CRC invalid for entry {i}");
        if let WalRecord::InsertTuple {
            page_id, data, ..
        } = &entry.record
        {
            assert_eq!(*page_id, i as u32);
            assert_eq!(data.len(), 1024);
        } else {
            panic!("expected InsertTuple at index {i}");
        }
    }

    drop(wal);
    cleanup(&dir);
}
