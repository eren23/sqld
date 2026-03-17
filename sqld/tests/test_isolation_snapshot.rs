use std::collections::HashSet;

use sqld::transaction::mvcc::{TxnStatusLookup, VisibilityCheck};
use sqld::transaction::{IsolationLevel, TransactionManager, TransactionStatus};
use sqld::types::tuple::MvccHeader;

// ===========================================================================
// Mock status lookup
// ===========================================================================

struct MockStatus {
    committed: HashSet<u64>,
    aborted: HashSet<u64>,
    active: HashSet<u64>,
}

impl MockStatus {
    fn new() -> Self {
        Self {
            committed: HashSet::new(),
            aborted: HashSet::new(),
            active: HashSet::new(),
        }
    }

    fn with_committed(mut self, txn_id: u64) -> Self {
        self.committed.insert(txn_id);
        self
    }

    #[allow(dead_code)]
    fn with_active(mut self, txn_id: u64) -> Self {
        self.active.insert(txn_id);
        self
    }
}

impl TxnStatusLookup for MockStatus {
    fn status_of(&self, txn_id: u64) -> Option<TransactionStatus> {
        if self.committed.contains(&txn_id) {
            Some(TransactionStatus::Committed)
        } else if self.aborted.contains(&txn_id) {
            Some(TransactionStatus::Aborted)
        } else if self.active.contains(&txn_id) {
            Some(TransactionStatus::Active)
        } else {
            None
        }
    }
}

/// Adapter: use TransactionManager as a TxnStatusLookup.
struct ManagerStatus<'a> {
    mgr: &'a TransactionManager,
}

impl<'a> TxnStatusLookup for ManagerStatus<'a> {
    fn status_of(&self, txn_id: u64) -> Option<TransactionStatus> {
        if self.mgr.is_committed(txn_id) {
            Some(TransactionStatus::Committed)
        } else if self.mgr.is_active(txn_id) {
            Some(TransactionStatus::Active)
        } else {
            None
        }
    }
}

// ===========================================================================
// 1. Snapshot taken once at begin, not refreshed
// ===========================================================================

#[test]
fn snapshot_taken_once_at_begin() {
    let mgr = TransactionManager::new();

    let t1 = mgr.begin(IsolationLevel::RepeatableRead);
    let original_xmax = t1.snapshot.xmax;
    let original_active = t1.snapshot.active_txns.clone();

    // Start and commit several transactions to advance global state.
    let mut t2 = mgr.begin(IsolationLevel::ReadCommitted);
    let _t3 = mgr.begin(IsolationLevel::ReadCommitted);
    mgr.commit(&mut t2).unwrap();

    // t1's snapshot must remain unchanged.
    assert_eq!(t1.snapshot.xmax, original_xmax);
    assert_eq!(t1.snapshot.active_txns, original_active);
}

#[test]
fn snapshot_xmin_fixed_at_begin() {
    let mgr = TransactionManager::new();

    let t1 = mgr.begin(IsolationLevel::RepeatableRead);
    let original_xmin = t1.snapshot.xmin;

    // More activity.
    let mut t2 = mgr.begin(IsolationLevel::ReadCommitted);
    mgr.commit(&mut t2).unwrap();

    assert_eq!(
        t1.snapshot.xmin, original_xmin,
        "xmin must not change after begin"
    );
}

// ===========================================================================
// 2. Cannot see changes committed after begin
// ===========================================================================

#[test]
fn committed_data_after_snapshot_not_visible() {
    let mgr = TransactionManager::new();

    let reader = mgr.begin(IsolationLevel::RepeatableRead);
    let reader_id = reader.txn_id;

    // Writer begins after reader, inserts a row, and commits.
    let mut writer = mgr.begin(IsolationLevel::ReadCommitted);
    let writer_id = writer.txn_id;
    let row = MvccHeader::new_insert(writer_id, 0);
    mgr.commit(&mut writer).unwrap();

    let status = ManagerStatus { mgr: &mgr };
    assert!(
        !VisibilityCheck::is_visible(&row, reader_id, &reader.snapshot, &status),
        "RepeatableRead reader must NOT see data committed after its snapshot"
    );
}

#[test]
fn concurrent_writer_committed_during_reader_not_visible() {
    let mgr = TransactionManager::new();

    // Writer begins first but is still active.
    let mut writer = mgr.begin(IsolationLevel::ReadCommitted);
    let writer_id = writer.txn_id;
    let row = MvccHeader::new_insert(writer_id, 0);

    // Reader begins -- writer is in the active set.
    let reader = mgr.begin(IsolationLevel::RepeatableRead);
    let reader_id = reader.txn_id;
    assert!(reader.snapshot.active_txns.contains(&writer_id));

    // Writer commits while reader is running.
    mgr.commit(&mut writer).unwrap();

    // Still not visible (was active at snapshot time).
    let status = ManagerStatus { mgr: &mgr };
    assert!(
        !VisibilityCheck::is_visible(&row, reader_id, &reader.snapshot, &status),
        "Txn active at snapshot time must not become visible after it commits"
    );
}

// ===========================================================================
// 3. refresh_snapshot is a no-op
// ===========================================================================

#[test]
fn refresh_snapshot_noop_for_repeatable_read() {
    let mgr = TransactionManager::new();

    let mut t1 = mgr.begin(IsolationLevel::RepeatableRead);
    let snap_before = t1.snapshot.clone();

    let mut t2 = mgr.begin(IsolationLevel::ReadCommitted);
    mgr.commit(&mut t2).unwrap();
    let mut t3 = mgr.begin(IsolationLevel::ReadCommitted);
    mgr.commit(&mut t3).unwrap();

    mgr.refresh_snapshot(&mut t1);

    assert_eq!(t1.snapshot.xmin, snap_before.xmin);
    assert_eq!(t1.snapshot.xmax, snap_before.xmax);
    assert_eq!(t1.snapshot.active_txns, snap_before.active_txns);
}

#[test]
fn refresh_snapshot_noop_for_serializable() {
    let mgr = TransactionManager::new();

    let mut t1 = mgr.begin(IsolationLevel::Serializable);
    let snap_before = t1.snapshot.clone();

    let mut t2 = mgr.begin(IsolationLevel::ReadCommitted);
    mgr.commit(&mut t2).unwrap();

    mgr.refresh_snapshot(&mut t1);

    assert_eq!(t1.snapshot.xmin, snap_before.xmin);
    assert_eq!(t1.snapshot.xmax, snap_before.xmax);
    assert_eq!(t1.snapshot.active_txns, snap_before.active_txns);
}

// ===========================================================================
// 4. Write-write conflict detection (check_write_conflict)
// ===========================================================================

#[test]
fn write_write_conflict_detected_active_holder() {
    let mgr = TransactionManager::new();

    let mut inserter = mgr.begin(IsolationLevel::ReadCommitted);
    let inserter_id = inserter.txn_id;
    mgr.commit(&mut inserter).unwrap();

    // T1 "updates" the row (sets xmax).
    let t1 = mgr.begin(IsolationLevel::RepeatableRead);
    let t1_id = t1.txn_id;
    let row_after_t1 = MvccHeader::new(inserter_id, t1_id, 0);

    // T2 tries to update the same row.
    let t2 = mgr.begin(IsolationLevel::RepeatableRead);
    let t2_id = t2.txn_id;

    let status = ManagerStatus { mgr: &mgr };
    assert!(
        VisibilityCheck::check_write_conflict(&row_after_t1, t2_id, &status),
        "Conflict when T1 (active) holds xmax"
    );
}

#[test]
fn write_write_conflict_detected_committed_holder() {
    let mgr = TransactionManager::new();

    let mut inserter = mgr.begin(IsolationLevel::ReadCommitted);
    let inserter_id = inserter.txn_id;
    mgr.commit(&mut inserter).unwrap();

    let mut t1 = mgr.begin(IsolationLevel::RepeatableRead);
    let t1_id = t1.txn_id;
    let row_after_t1 = MvccHeader::new(inserter_id, t1_id, 0);
    mgr.commit(&mut t1).unwrap();

    let t2 = mgr.begin(IsolationLevel::RepeatableRead);
    let t2_id = t2.txn_id;

    let status = ManagerStatus { mgr: &mgr };
    assert!(
        VisibilityCheck::check_write_conflict(&row_after_t1, t2_id, &status),
        "Conflict when T1 (committed) set xmax"
    );
}

#[test]
fn no_write_conflict_when_no_xmax() {
    let header = MvccHeader::new_insert(5, 0);
    let status = MockStatus::new().with_committed(5);

    assert!(
        !VisibilityCheck::check_write_conflict(&header, 10, &status),
        "No conflict when xmax is zero"
    );
}

// ===========================================================================
// 5. Consistent reads across multiple statements
// ===========================================================================

#[test]
fn repeatable_reads_same_results_after_delete() {
    let mgr = TransactionManager::new();

    let mut w1 = mgr.begin(IsolationLevel::ReadCommitted);
    let w1_id = w1.txn_id;
    let row = MvccHeader::new_insert(w1_id, 0);
    mgr.commit(&mut w1).unwrap();

    let reader = mgr.begin(IsolationLevel::RepeatableRead);
    let reader_id = reader.txn_id;

    let status = ManagerStatus { mgr: &mgr };
    let read_1 = VisibilityCheck::is_visible(&row, reader_id, &reader.snapshot, &status);
    assert!(read_1, "First read should see the committed row");

    // Another txn deletes the row and commits.
    let mut deleter = mgr.begin(IsolationLevel::ReadCommitted);
    let deleter_id = deleter.txn_id;
    let deleted_row = MvccHeader::new(w1_id, deleter_id, 0);
    mgr.commit(&mut deleter).unwrap();

    let status = ManagerStatus { mgr: &mgr };
    let read_2 = VisibilityCheck::is_visible(&deleted_row, reader_id, &reader.snapshot, &status);
    assert!(read_2, "Deletion not visible under RepeatableRead");
    assert_eq!(read_1, read_2, "Repeatable read: both reads identical");
}

#[test]
fn no_phantom_reads_under_snapshot_isolation() {
    let mgr = TransactionManager::new();

    let mut w_a = mgr.begin(IsolationLevel::ReadCommitted);
    let wa_id = w_a.txn_id;
    let row_a = MvccHeader::new_insert(wa_id, 0);
    mgr.commit(&mut w_a).unwrap();

    let reader = mgr.begin(IsolationLevel::RepeatableRead);
    let reader_id = reader.txn_id;

    let status = ManagerStatus { mgr: &mgr };
    let mut scan_1 = Vec::new();
    if VisibilityCheck::is_visible(&row_a, reader_id, &reader.snapshot, &status) {
        scan_1.push("row_a");
    }
    assert_eq!(scan_1, vec!["row_a"]);

    // Another writer inserts row_b after reader started.
    let mut w_b = mgr.begin(IsolationLevel::ReadCommitted);
    let wb_id = w_b.txn_id;
    let row_b = MvccHeader::new_insert(wb_id, 0);
    mgr.commit(&mut w_b).unwrap();

    let status = ManagerStatus { mgr: &mgr };
    let mut scan_2 = Vec::new();
    if VisibilityCheck::is_visible(&row_a, reader_id, &reader.snapshot, &status) {
        scan_2.push("row_a");
    }
    if VisibilityCheck::is_visible(&row_b, reader_id, &reader.snapshot, &status) {
        scan_2.push("row_b");
    }
    assert_eq!(scan_2, vec!["row_a"], "No phantom reads under RepeatableRead");
    assert_eq!(scan_1, scan_2, "Both scans must be identical");
}
