use std::path::PathBuf;

use sqld::wal::{WalEntry, WalManager, WalRecord};

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
// prev_lsn chain within a single transaction
// ---------------------------------------------------------------------------

#[test]
fn prev_lsn_chain_single_txn() {
    let dir = test_dir("prev_lsn_chain");
    let wal = WalManager::open(&dir).unwrap();

    let lsn_begin = wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
    let lsn_ins1 = wal
        .append(WalRecord::InsertTuple {
            txn_id: 1,
            page_id: 10,
            slot_index: 0,
            data: vec![1, 2, 3],
        })
        .unwrap();
    let lsn_ins2 = wal
        .append(WalRecord::InsertTuple {
            txn_id: 1,
            page_id: 10,
            slot_index: 1,
            data: vec![4, 5, 6],
        })
        .unwrap();
    let lsn_commit = wal.commit(1).unwrap();

    // Read back all entries
    let entries = wal.read_all_entries().unwrap();
    assert_eq!(entries.len(), 4, "expected 4 WAL entries");

    // First record of txn 1 (Begin) should have prev_lsn = 0
    let e_begin = entries.iter().find(|e| e.lsn == lsn_begin).unwrap();
    assert_eq!(e_begin.prev_lsn, 0, "Begin should have prev_lsn=0");

    // Second record (InsertTuple) should point back to Begin
    let e_ins1 = entries.iter().find(|e| e.lsn == lsn_ins1).unwrap();
    assert_eq!(
        e_ins1.prev_lsn, lsn_begin,
        "first InsertTuple should chain to Begin"
    );

    // Third record (InsertTuple) should point back to first insert
    let e_ins2 = entries.iter().find(|e| e.lsn == lsn_ins2).unwrap();
    assert_eq!(
        e_ins2.prev_lsn, lsn_ins1,
        "second InsertTuple should chain to first InsertTuple"
    );

    // Commit should point back to second insert
    let e_commit = entries.iter().find(|e| e.lsn == lsn_commit).unwrap();
    assert_eq!(
        e_commit.prev_lsn, lsn_ins2,
        "Commit should chain to second InsertTuple"
    );

    // All CRCs should be valid
    for entry in &entries {
        assert!(entry.verify_crc(), "CRC mismatch at lsn={}", entry.lsn);
    }

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// Interleaved transactions have independent prev_lsn chains
// ---------------------------------------------------------------------------

#[test]
fn interleaved_txn_prev_lsn_chains() {
    let dir = test_dir("interleaved_chains");
    let wal = WalManager::open(&dir).unwrap();

    let lsn_begin1 = wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
    let lsn_begin2 = wal.append(WalRecord::Begin { txn_id: 2 }).unwrap();
    let lsn_ins_t1 = wal
        .append(WalRecord::InsertTuple {
            txn_id: 1,
            page_id: 10,
            slot_index: 0,
            data: vec![0xAA],
        })
        .unwrap();
    let lsn_ins_t2 = wal
        .append(WalRecord::InsertTuple {
            txn_id: 2,
            page_id: 20,
            slot_index: 0,
            data: vec![0xBB],
        })
        .unwrap();
    let lsn_commit1 = wal.commit(1).unwrap();

    wal.flush().unwrap();
    let entries = wal.read_all_entries().unwrap();
    assert_eq!(entries.len(), 5);

    // Txn 1 chain: begin1 -> ins_t1 -> commit1
    let e_begin1 = entries.iter().find(|e| e.lsn == lsn_begin1).unwrap();
    assert_eq!(e_begin1.prev_lsn, 0);

    let e_ins_t1 = entries.iter().find(|e| e.lsn == lsn_ins_t1).unwrap();
    assert_eq!(e_ins_t1.prev_lsn, lsn_begin1);

    let e_commit1 = entries.iter().find(|e| e.lsn == lsn_commit1).unwrap();
    assert_eq!(e_commit1.prev_lsn, lsn_ins_t1);

    // Txn 2 chain: begin2 -> ins_t2  (no commit yet for txn 2)
    let e_begin2 = entries.iter().find(|e| e.lsn == lsn_begin2).unwrap();
    assert_eq!(e_begin2.prev_lsn, 0);

    let e_ins_t2 = entries.iter().find(|e| e.lsn == lsn_ins_t2).unwrap();
    assert_eq!(e_ins_t2.prev_lsn, lsn_begin2);

    // Verify that txn 1 chain does NOT reference any txn 2 LSN
    assert_ne!(e_ins_t1.prev_lsn, lsn_begin2);
    assert_ne!(e_commit1.prev_lsn, lsn_ins_t2);

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// active_txn_ids tracking
// ---------------------------------------------------------------------------

#[test]
fn active_txn_ids_tracking() {
    let dir = test_dir("active_txn_ids");
    let wal = WalManager::open(&dir).unwrap();

    // No active transactions initially
    assert!(
        wal.active_txn_ids().is_empty(),
        "no txns should be active initially"
    );

    // Begin txn 1 and txn 2
    wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
    wal.append(WalRecord::Begin { txn_id: 2 }).unwrap();

    let mut active = wal.active_txn_ids();
    active.sort();
    assert_eq!(active, vec![1, 2], "both txn 1 and 2 should be active");

    // Commit txn 1 — only txn 2 should remain
    wal.commit(1).unwrap();
    let active = wal.active_txn_ids();
    assert_eq!(active, vec![2], "only txn 2 should be active after commit(1)");

    // Abort txn 2 — none should remain
    wal.abort(2).unwrap();
    assert!(
        wal.active_txn_ids().is_empty(),
        "no txns should be active after abort(2)"
    );

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// txn_last_lsn returns the most recent LSN for a transaction
// ---------------------------------------------------------------------------

#[test]
fn txn_last_lsn_tracking() {
    let dir = test_dir("txn_last_lsn");
    let wal = WalManager::open(&dir).unwrap();

    // No record yet — txn_last_lsn should return None
    assert_eq!(wal.txn_last_lsn(1), None);

    let lsn_begin = wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
    assert_eq!(
        wal.txn_last_lsn(1),
        Some(lsn_begin),
        "last LSN should be the Begin record"
    );

    let lsn_ins = wal
        .append(WalRecord::InsertTuple {
            txn_id: 1,
            page_id: 5,
            slot_index: 0,
            data: vec![10, 20],
        })
        .unwrap();
    assert_eq!(
        wal.txn_last_lsn(1),
        Some(lsn_ins),
        "last LSN should advance to the InsertTuple"
    );

    let lsn_del = wal
        .append(WalRecord::DeleteTuple {
            txn_id: 1,
            page_id: 5,
            slot_index: 0,
            data: vec![10, 20],
        })
        .unwrap();
    assert_eq!(
        wal.txn_last_lsn(1),
        Some(lsn_del),
        "last LSN should advance to the DeleteTuple"
    );

    // After commit, the txn tracking is cleaned up
    wal.commit(1).unwrap();
    assert_eq!(
        wal.txn_last_lsn(1),
        None,
        "txn_last_lsn should be None after commit"
    );

    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// parse_entries round-trip: serialize entries, parse them back
// ---------------------------------------------------------------------------

#[test]
fn parse_entries_round_trip() {
    let records = vec![
        WalRecord::Begin { txn_id: 42 },
        WalRecord::InsertTuple {
            txn_id: 42,
            page_id: 7,
            slot_index: 3,
            data: vec![0xDE, 0xAD, 0xBE, 0xEF],
        },
        WalRecord::UpdateTuple {
            txn_id: 42,
            page_id: 7,
            slot_index: 3,
            old_data: vec![0xDE, 0xAD, 0xBE, 0xEF],
            new_data: vec![0xCA, 0xFE],
        },
        WalRecord::Commit { txn_id: 42 },
    ];

    // Build serialized WAL data by hand using WalEntry
    let mut buf = Vec::new();
    let mut prev_lsn: u64 = 0;
    let mut lsn: u64 = 0;
    let mut expected_entries = Vec::new();

    for record in records {
        let entry = WalEntry::new(lsn, prev_lsn, record);
        let serialized = entry.serialize();
        let entry_size = serialized.len() as u64;
        buf.extend_from_slice(&serialized);
        expected_entries.push(entry);
        prev_lsn = lsn;
        lsn += entry_size;
    }

    // Parse back
    let parsed = WalManager::parse_entries(&buf).unwrap();
    assert_eq!(parsed.len(), expected_entries.len());

    for (parsed_entry, expected_entry) in parsed.iter().zip(expected_entries.iter()) {
        assert_eq!(parsed_entry.lsn, expected_entry.lsn);
        assert_eq!(parsed_entry.prev_lsn, expected_entry.prev_lsn);
        assert_eq!(parsed_entry.record, expected_entry.record);
        assert_eq!(parsed_entry.crc32, expected_entry.crc32);
        assert!(parsed_entry.verify_crc());
    }
}

// ---------------------------------------------------------------------------
// Torn write tolerance: truncated data yields only complete entries
// ---------------------------------------------------------------------------

#[test]
fn torn_write_tolerance() {
    // Build a WAL with three entries
    let records = vec![
        WalRecord::Begin { txn_id: 100 },
        WalRecord::InsertTuple {
            txn_id: 100,
            page_id: 1,
            slot_index: 0,
            data: vec![1, 2, 3, 4, 5],
        },
        WalRecord::Commit { txn_id: 100 },
    ];

    let mut buf = Vec::new();
    let mut prev_lsn: u64 = 0;
    let mut lsn: u64 = 0;
    let mut entry_boundaries = Vec::new();

    for record in records {
        let entry = WalEntry::new(lsn, prev_lsn, record);
        let serialized = entry.serialize();
        let entry_size = serialized.len() as u64;
        buf.extend_from_slice(&serialized);
        entry_boundaries.push(buf.len());
        prev_lsn = lsn;
        lsn += entry_size;
    }

    // Sanity check: full buffer parses to 3 entries
    let full_parse = WalManager::parse_entries(&buf).unwrap();
    assert_eq!(full_parse.len(), 3, "full data should yield 3 entries");

    // Case 1: truncate in the middle of the third entry (after the second
    // entry boundary but before the third entry boundary).
    let cut_point = entry_boundaries[1] + 5; // 5 bytes into the third entry
    assert!(cut_point < entry_boundaries[2], "cut should be mid-entry");
    let truncated = &buf[..cut_point];
    let parsed = WalManager::parse_entries(truncated).unwrap();
    assert_eq!(
        parsed.len(),
        2,
        "truncated WAL should yield only the 2 complete entries"
    );
    assert_eq!(parsed[0].record, WalRecord::Begin { txn_id: 100 });
    assert!(matches!(parsed[1].record, WalRecord::InsertTuple { txn_id: 100, .. }));

    // Case 2: truncate inside the header of the second entry (only a partial
    // header remains after the first entry).
    let cut_point = entry_boundaries[0] + 10; // 10 bytes into second entry header
    let truncated = &buf[..cut_point];
    let parsed = WalManager::parse_entries(truncated).unwrap();
    assert_eq!(
        parsed.len(),
        1,
        "partial header should yield only the 1 complete entry"
    );
    assert_eq!(parsed[0].record, WalRecord::Begin { txn_id: 100 });

    // Case 3: truncate to fewer bytes than a header
    let truncated = &buf[..10];
    let parsed = WalManager::parse_entries(truncated).unwrap();
    assert_eq!(
        parsed.len(),
        0,
        "fewer bytes than a header should yield 0 entries"
    );

    // Case 4: empty data
    let parsed = WalManager::parse_entries(&[]).unwrap();
    assert_eq!(parsed.len(), 0, "empty data should yield 0 entries");

    // Case 5: corrupt the CRC of the second entry (flip a byte in the CRC
    // region). parse_entries should stop at the corruption and return only
    // the first entry.
    let mut corrupted = buf.clone();
    let crc_offset = entry_boundaries[1] - 1; // last byte of second entry's CRC
    corrupted[crc_offset] ^= 0xFF;
    let parsed = WalManager::parse_entries(&corrupted).unwrap();
    assert_eq!(
        parsed.len(),
        1,
        "CRC corruption in second entry should stop parsing after first"
    );
}

// ---------------------------------------------------------------------------
// LSN monotonicity and flushed_lsn / current_lsn consistency
// ---------------------------------------------------------------------------

#[test]
fn lsn_monotonicity_and_flush() {
    let dir = test_dir("lsn_monotonicity");
    let wal = WalManager::open(&dir).unwrap();

    let initial_lsn = wal.current_lsn();
    assert_eq!(initial_lsn, 0, "fresh WAL should start at LSN 0");
    assert_eq!(wal.flushed_lsn(), 0, "flushed_lsn should also be 0");

    // Append a record — current_lsn advances, flushed_lsn stays
    let lsn1 = wal.append(WalRecord::Begin { txn_id: 1 }).unwrap();
    assert_eq!(lsn1, 0, "first record should get LSN 0");
    assert!(
        wal.current_lsn() > lsn1,
        "current_lsn should advance past the appended entry"
    );
    assert_eq!(
        wal.flushed_lsn(),
        0,
        "flushed_lsn should not move until flush"
    );

    // Flush — flushed_lsn catches up to current_lsn
    wal.flush().unwrap();
    assert_eq!(
        wal.flushed_lsn(),
        wal.current_lsn(),
        "after flush, flushed_lsn == current_lsn"
    );

    // Append more records; each LSN should be strictly greater than the last
    let lsn2 = wal
        .append(WalRecord::InsertTuple {
            txn_id: 1,
            page_id: 1,
            slot_index: 0,
            data: vec![42],
        })
        .unwrap();
    let lsn3 = wal
        .append(WalRecord::InsertTuple {
            txn_id: 1,
            page_id: 1,
            slot_index: 1,
            data: vec![43],
        })
        .unwrap();
    assert!(lsn2 > lsn1, "LSNs should be monotonically increasing");
    assert!(lsn3 > lsn2, "LSNs should be monotonically increasing");

    cleanup(&dir);
}
