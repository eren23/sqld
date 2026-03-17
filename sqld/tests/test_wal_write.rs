use std::path::PathBuf;

use sqld::wal::{WalEntry, WalManager, WalRecord};
use sqld::wal::wal_record::{WAL_ENTRY_CRC_SIZE, WAL_ENTRY_HEADER_SIZE};

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

/// Build one instance of every WalRecord variant for round-trip testing.
fn all_record_variants() -> Vec<WalRecord> {
    vec![
        WalRecord::Begin { txn_id: 1 },
        WalRecord::Commit { txn_id: 2 },
        WalRecord::Abort { txn_id: 3 },
        WalRecord::InsertTuple {
            txn_id: 10,
            page_id: 42,
            slot_index: 7,
            data: vec![0xDE, 0xAD, 0xBE, 0xEF],
        },
        WalRecord::DeleteTuple {
            txn_id: 11,
            page_id: 43,
            slot_index: 0,
            data: vec![1, 2, 3],
        },
        WalRecord::UpdateTuple {
            txn_id: 12,
            page_id: 44,
            slot_index: 5,
            old_data: vec![10, 20],
            new_data: vec![30, 40, 50],
        },
        WalRecord::IndexInsert {
            txn_id: 20,
            index_page_id: 100,
            key: vec![0xCA, 0xFE],
            tid_page: 200,
            tid_slot: 3,
        },
        WalRecord::IndexDelete {
            txn_id: 21,
            index_page_id: 101,
            key: vec![0xBA, 0xBE],
            tid_page: 201,
            tid_slot: 4,
        },
        WalRecord::PageAlloc {
            txn_id: 30,
            page_id: 500,
        },
        WalRecord::PageFree {
            txn_id: 31,
            page_id: 501,
        },
        WalRecord::CheckpointBegin {
            active_txns: vec![1, 2, 3],
        },
        WalRecord::CheckpointEnd {
            checkpoint_begin_lsn: 12345,
        },
    ]
}

// ---------------------------------------------------------------------------
// WalRecord serialization round-trip
// ---------------------------------------------------------------------------

#[test]
fn record_round_trip_all_variants() {
    for original in all_record_variants() {
        let bytes = original.serialize();
        let (deserialized, consumed) =
            WalRecord::deserialize(&bytes).expect("deserialize failed");
        assert_eq!(
            consumed,
            bytes.len(),
            "consumed bytes mismatch for {:?}",
            original
        );
        assert_eq!(
            deserialized, original,
            "round-trip mismatch for {:?}",
            original
        );
    }
}

#[test]
fn record_round_trip_empty_data() {
    // Edge case: tuple records with zero-length data
    let record = WalRecord::InsertTuple {
        txn_id: 99,
        page_id: 1,
        slot_index: 0,
        data: vec![],
    };
    let bytes = record.serialize();
    let (deser, consumed) = WalRecord::deserialize(&bytes).unwrap();
    assert_eq!(consumed, bytes.len());
    assert_eq!(deser, record);
}

// ---------------------------------------------------------------------------
// CLR with nested redo record
// ---------------------------------------------------------------------------

#[test]
fn clr_with_nested_redo_round_trip() {
    let inner = WalRecord::InsertTuple {
        txn_id: 50,
        page_id: 77,
        slot_index: 2,
        data: vec![0xAA, 0xBB, 0xCC],
    };
    let clr = WalRecord::Clr {
        txn_id: 50,
        undo_next_lsn: 1000,
        redo: Box::new(inner.clone()),
    };

    let bytes = clr.serialize();
    let (deserialized, consumed) = WalRecord::deserialize(&bytes).unwrap();
    assert_eq!(consumed, bytes.len());
    assert_eq!(deserialized, clr);

    // Verify the inner record is intact
    if let WalRecord::Clr { redo, .. } = &deserialized {
        assert_eq!(**redo, inner);
    } else {
        panic!("expected CLR variant");
    }
}

#[test]
fn clr_with_nested_page_alloc_round_trip() {
    let inner = WalRecord::PageAlloc {
        txn_id: 60,
        page_id: 999,
    };
    let clr = WalRecord::Clr {
        txn_id: 60,
        undo_next_lsn: 2000,
        redo: Box::new(inner),
    };
    let bytes = clr.serialize();
    let (deserialized, _) = WalRecord::deserialize(&bytes).unwrap();
    assert_eq!(deserialized, clr);
}

// ---------------------------------------------------------------------------
// WalEntry serialization round-trip
// ---------------------------------------------------------------------------

#[test]
fn entry_round_trip() {
    let record = WalRecord::InsertTuple {
        txn_id: 7,
        page_id: 10,
        slot_index: 3,
        data: vec![1, 2, 3, 4, 5],
    };
    let entry = WalEntry::new(0, 0, record);
    let bytes = entry.serialize();
    let (deserialized, consumed) = WalEntry::deserialize(&bytes).unwrap();
    assert_eq!(consumed, bytes.len());
    assert_eq!(deserialized, entry);
}

#[test]
fn entry_round_trip_all_variants() {
    let mut lsn = 0u64;
    for record in all_record_variants() {
        let entry = WalEntry::new(lsn, 0, record);
        let bytes = entry.serialize();
        let (deserialized, consumed) = WalEntry::deserialize(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(deserialized, entry);
        lsn += consumed as u64;
    }
}

#[test]
fn entry_disk_size_matches_serialized_length() {
    let record = WalRecord::UpdateTuple {
        txn_id: 1,
        page_id: 5,
        slot_index: 2,
        old_data: vec![10; 100],
        new_data: vec![20; 200],
    };
    let entry = WalEntry::new(0, 0, record);
    assert_eq!(entry.disk_size(), entry.serialize().len());
}

#[test]
fn entry_disk_size_includes_header_and_crc() {
    let record = WalRecord::Begin { txn_id: 1 };
    let record_len = record.serialize().len();
    let entry = WalEntry::new(0, 0, record);
    assert_eq!(
        entry.disk_size(),
        WAL_ENTRY_HEADER_SIZE + record_len + WAL_ENTRY_CRC_SIZE
    );
}

// ---------------------------------------------------------------------------
// CRC verification
// ---------------------------------------------------------------------------

#[test]
fn entry_crc_verification_succeeds_for_valid_entry() {
    let record = WalRecord::Commit { txn_id: 42 };
    let entry = WalEntry::new(100, 50, record);
    assert!(entry.verify_crc(), "CRC should be valid for a fresh entry");
}

#[test]
fn entry_crc_verification_fails_when_bytes_corrupted() {
    let record = WalRecord::InsertTuple {
        txn_id: 1,
        page_id: 2,
        slot_index: 3,
        data: vec![0xFF; 16],
    };
    let entry = WalEntry::new(0, 0, record);
    let mut bytes = entry.serialize();

    // Flip a byte in the record payload area (after the 20-byte header)
    let corrupt_index = WAL_ENTRY_HEADER_SIZE + 2;
    bytes[corrupt_index] ^= 0xFF;

    // Deserialization should fail due to CRC mismatch
    let result = WalEntry::deserialize(&bytes);
    assert!(
        result.is_err(),
        "deserialization should fail with corrupted data"
    );
}

#[test]
fn entry_crc_verification_fails_with_tampered_lsn() {
    let record = WalRecord::Begin { txn_id: 99 };
    let entry = WalEntry::new(1000, 0, record);
    let mut bytes = entry.serialize();

    // Tamper with the LSN field (first 8 bytes)
    bytes[0] ^= 0x01;

    let result = WalEntry::deserialize(&bytes);
    assert!(
        result.is_err(),
        "deserialization should fail when LSN is tampered"
    );
}

// ---------------------------------------------------------------------------
// LSN monotonicity via WalManager
// ---------------------------------------------------------------------------

#[test]
fn lsn_monotonicity_across_appends() {
    let dir = test_dir("lsn_monotonicity");
    let wal = WalManager::open(&dir).unwrap();

    let mut prev_lsn = None;
    for i in 0..10 {
        let record = WalRecord::InsertTuple {
            txn_id: 1,
            page_id: i,
            slot_index: 0,
            data: vec![i as u8; 8],
        };
        let lsn = wal.append(record).unwrap();
        if let Some(prev) = prev_lsn {
            assert!(
                lsn > prev,
                "LSN should be strictly increasing: got {lsn} after {prev}"
            );
        }
        prev_lsn = Some(lsn);
    }

    drop(wal);
    cleanup(&dir);
}

#[test]
fn lsn_monotonicity_across_different_record_sizes() {
    let dir = test_dir("lsn_mono_sizes");
    let wal = WalManager::open(&dir).unwrap();

    let records = vec![
        WalRecord::Begin { txn_id: 1 },
        WalRecord::InsertTuple {
            txn_id: 1,
            page_id: 10,
            slot_index: 0,
            data: vec![0xAA; 256],
        },
        WalRecord::UpdateTuple {
            txn_id: 1,
            page_id: 10,
            slot_index: 0,
            old_data: vec![0xAA; 256],
            new_data: vec![0xBB; 512],
        },
        WalRecord::PageAlloc {
            txn_id: 1,
            page_id: 11,
        },
        WalRecord::Commit { txn_id: 1 },
    ];

    let mut lsns = Vec::new();
    for record in records {
        let lsn = wal.append(record).unwrap();
        lsns.push(lsn);
    }

    for window in lsns.windows(2) {
        assert!(
            window[1] > window[0],
            "LSN should increase: {} > {}",
            window[1],
            window[0]
        );
    }

    drop(wal);
    cleanup(&dir);
}

// ---------------------------------------------------------------------------
// txn_id() correctness
// ---------------------------------------------------------------------------

#[test]
fn txn_id_returns_correct_values() {
    assert_eq!(WalRecord::Begin { txn_id: 1 }.txn_id(), Some(1));
    assert_eq!(WalRecord::Commit { txn_id: 2 }.txn_id(), Some(2));
    assert_eq!(WalRecord::Abort { txn_id: 3 }.txn_id(), Some(3));
    assert_eq!(
        WalRecord::InsertTuple {
            txn_id: 10,
            page_id: 1,
            slot_index: 0,
            data: vec![],
        }
        .txn_id(),
        Some(10)
    );
    assert_eq!(
        WalRecord::DeleteTuple {
            txn_id: 11,
            page_id: 1,
            slot_index: 0,
            data: vec![],
        }
        .txn_id(),
        Some(11)
    );
    assert_eq!(
        WalRecord::UpdateTuple {
            txn_id: 12,
            page_id: 1,
            slot_index: 0,
            old_data: vec![],
            new_data: vec![],
        }
        .txn_id(),
        Some(12)
    );
    assert_eq!(
        WalRecord::IndexInsert {
            txn_id: 20,
            index_page_id: 1,
            key: vec![],
            tid_page: 1,
            tid_slot: 0,
        }
        .txn_id(),
        Some(20)
    );
    assert_eq!(
        WalRecord::IndexDelete {
            txn_id: 21,
            index_page_id: 1,
            key: vec![],
            tid_page: 1,
            tid_slot: 0,
        }
        .txn_id(),
        Some(21)
    );
    assert_eq!(
        WalRecord::PageAlloc {
            txn_id: 30,
            page_id: 1,
        }
        .txn_id(),
        Some(30)
    );
    assert_eq!(
        WalRecord::PageFree {
            txn_id: 31,
            page_id: 1,
        }
        .txn_id(),
        Some(31)
    );
    assert_eq!(
        WalRecord::Clr {
            txn_id: 40,
            undo_next_lsn: 0,
            redo: Box::new(WalRecord::Begin { txn_id: 40 }),
        }
        .txn_id(),
        Some(40)
    );

    // Checkpoint records have no txn_id
    assert_eq!(
        WalRecord::CheckpointBegin {
            active_txns: vec![1, 2],
        }
        .txn_id(),
        None
    );
    assert_eq!(
        WalRecord::CheckpointEnd {
            checkpoint_begin_lsn: 0,
        }
        .txn_id(),
        None
    );
}

// ---------------------------------------------------------------------------
// affected_page() correctness
// ---------------------------------------------------------------------------

#[test]
fn affected_page_returns_correct_values() {
    // Tuple operations return their page_id
    assert_eq!(
        WalRecord::InsertTuple {
            txn_id: 1,
            page_id: 42,
            slot_index: 0,
            data: vec![],
        }
        .affected_page(),
        Some(42)
    );
    assert_eq!(
        WalRecord::DeleteTuple {
            txn_id: 1,
            page_id: 43,
            slot_index: 0,
            data: vec![],
        }
        .affected_page(),
        Some(43)
    );
    assert_eq!(
        WalRecord::UpdateTuple {
            txn_id: 1,
            page_id: 44,
            slot_index: 0,
            old_data: vec![],
            new_data: vec![],
        }
        .affected_page(),
        Some(44)
    );

    // Index operations return their index_page_id
    assert_eq!(
        WalRecord::IndexInsert {
            txn_id: 1,
            index_page_id: 100,
            key: vec![],
            tid_page: 200,
            tid_slot: 0,
        }
        .affected_page(),
        Some(100)
    );
    assert_eq!(
        WalRecord::IndexDelete {
            txn_id: 1,
            index_page_id: 101,
            key: vec![],
            tid_page: 201,
            tid_slot: 0,
        }
        .affected_page(),
        Some(101)
    );

    // Page-level operations
    assert_eq!(
        WalRecord::PageAlloc {
            txn_id: 1,
            page_id: 500,
        }
        .affected_page(),
        Some(500)
    );
    assert_eq!(
        WalRecord::PageFree {
            txn_id: 1,
            page_id: 501,
        }
        .affected_page(),
        Some(501)
    );

    // CLR delegates to the inner redo record
    assert_eq!(
        WalRecord::Clr {
            txn_id: 1,
            undo_next_lsn: 0,
            redo: Box::new(WalRecord::InsertTuple {
                txn_id: 1,
                page_id: 77,
                slot_index: 0,
                data: vec![],
            }),
        }
        .affected_page(),
        Some(77)
    );

    // Transaction lifecycle and checkpoint records have no affected page
    assert_eq!(WalRecord::Begin { txn_id: 1 }.affected_page(), None);
    assert_eq!(WalRecord::Commit { txn_id: 1 }.affected_page(), None);
    assert_eq!(WalRecord::Abort { txn_id: 1 }.affected_page(), None);
    assert_eq!(
        WalRecord::CheckpointBegin {
            active_txns: vec![],
        }
        .affected_page(),
        None
    );
    assert_eq!(
        WalRecord::CheckpointEnd {
            checkpoint_begin_lsn: 0,
        }
        .affected_page(),
        None
    );
}
