use std::collections::HashSet;

use sqld::transaction::mvcc::{TxnStatusLookup, VisibilityCheck};
use sqld::transaction::{IsolationLevel, Snapshot, TransactionManager, TransactionStatus};
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
// 1. Each statement gets a fresh snapshot (via refresh_snapshot)
// ===========================================================================

#[test]
fn refresh_snapshot_advances_xmax() {
    let tm = TransactionManager::new();

    let mut t1 = tm.begin(IsolationLevel::ReadCommitted);
    let original_xmax = t1.snapshot.xmax;

    // Start another transaction so next_txn_id advances.
    let _t2 = tm.begin(IsolationLevel::ReadCommitted);

    tm.refresh_snapshot(&mut t1);

    assert!(
        t1.snapshot.xmax > original_xmax,
        "xmax should advance after refresh"
    );
}

#[test]
fn refresh_snapshot_removes_committed_from_active_set() {
    let tm = TransactionManager::new();

    let mut t1 = tm.begin(IsolationLevel::ReadCommitted);
    let mut t2 = tm.begin(IsolationLevel::ReadCommitted);
    let t2_id = t2.txn_id;

    tm.commit(&mut t2).unwrap();
    tm.refresh_snapshot(&mut t1);

    assert!(
        !t1.snapshot.active_txns.contains(&t2_id),
        "committed t2 should not be active after refresh"
    );
    assert!(
        t1.snapshot.is_visible(t2_id),
        "committed t2 should be visible in refreshed snapshot"
    );
}

#[test]
fn refresh_snapshot_sees_newly_committed_insert() {
    let mgr = TransactionManager::new();

    let mut t1 = mgr.begin(IsolationLevel::ReadCommitted);
    let t1_id = t1.txn_id;
    let row = MvccHeader::new_insert(t1_id, 0);

    let mut t2 = mgr.begin(IsolationLevel::ReadCommitted);
    let t2_id = t2.txn_id;

    let status = ManagerStatus { mgr: &mgr };
    assert!(
        !VisibilityCheck::is_visible(&row, t2_id, &t2.snapshot, &status),
        "T2 should not see T1's row before T1 commits"
    );

    mgr.commit(&mut t1).unwrap();

    // Still stale snapshot.
    let status = ManagerStatus { mgr: &mgr };
    assert!(
        !VisibilityCheck::is_visible(&row, t2_id, &t2.snapshot, &status),
        "T2 should not see T1's row with stale snapshot"
    );

    mgr.refresh_snapshot(&mut t2);
    let status = ManagerStatus { mgr: &mgr };
    assert!(
        VisibilityCheck::is_visible(&row, t2_id, &t2.snapshot, &status),
        "T2 should see T1's row after refreshing the snapshot"
    );
}

// ===========================================================================
// 2. Can see data committed by other txns between statements
// ===========================================================================

#[test]
fn sees_data_committed_between_statements() {
    let mgr = TransactionManager::new();

    let mut reader = mgr.begin(IsolationLevel::ReadCommitted);
    let reader_id = reader.txn_id;

    // Writer 1 inserts and commits.
    let mut w1 = mgr.begin(IsolationLevel::ReadCommitted);
    let w1_id = w1.txn_id;
    let row_w1 = MvccHeader::new_insert(w1_id, 0);
    mgr.commit(&mut w1).unwrap();

    // Statement 1: refresh, then check visibility.
    mgr.refresh_snapshot(&mut reader);
    let status = ManagerStatus { mgr: &mgr };
    assert!(
        VisibilityCheck::is_visible(&row_w1, reader_id, &reader.snapshot, &status),
        "Statement 1 should see w1's row after refresh"
    );

    // Writer 2 inserts and commits after statement 1.
    let mut w2 = mgr.begin(IsolationLevel::ReadCommitted);
    let w2_id = w2.txn_id;
    let row_w2 = MvccHeader::new_insert(w2_id, 0);
    mgr.commit(&mut w2).unwrap();

    // Before refresh for statement 2, w2 is not visible.
    let status = ManagerStatus { mgr: &mgr };
    assert!(
        !VisibilityCheck::is_visible(&row_w2, reader_id, &reader.snapshot, &status),
        "Before statement 2 refresh, w2 not visible"
    );

    // Statement 2: refresh again.
    mgr.refresh_snapshot(&mut reader);
    let status = ManagerStatus { mgr: &mgr };
    assert!(
        VisibilityCheck::is_visible(&row_w2, reader_id, &reader.snapshot, &status),
        "Statement 2 should see w2's row after refresh"
    );
}

#[test]
fn non_repeatable_read_is_possible() {
    let mgr = TransactionManager::new();

    let mut writer = mgr.begin(IsolationLevel::ReadCommitted);
    let writer_id = writer.txn_id;
    let row = MvccHeader::new_insert(writer_id, 0);
    mgr.commit(&mut writer).unwrap();

    let mut reader = mgr.begin(IsolationLevel::ReadCommitted);
    let reader_id = reader.txn_id;

    let status = ManagerStatus { mgr: &mgr };
    let read_1 = VisibilityCheck::is_visible(&row, reader_id, &reader.snapshot, &status);
    assert!(read_1, "First read should see the row");

    let mut deleter = mgr.begin(IsolationLevel::ReadCommitted);
    let deleter_id = deleter.txn_id;
    let deleted_row = MvccHeader::new(writer_id, deleter_id, 0);
    mgr.commit(&mut deleter).unwrap();

    mgr.refresh_snapshot(&mut reader);
    let status = ManagerStatus { mgr: &mgr };
    let read_2 = VisibilityCheck::is_visible(&deleted_row, reader_id, &reader.snapshot, &status);
    assert!(!read_2, "Second read should NOT see deleted row");
    assert_ne!(read_1, read_2, "Non-repeatable read occurred as expected");
}

// ===========================================================================
// 3. Write-write conflicts detected
// ===========================================================================

#[test]
fn write_write_conflict_active_xmax() {
    let header = MvccHeader::new(5, 7, 0);
    let status = MockStatus::new().with_committed(5).with_active(7);

    assert!(
        VisibilityCheck::check_write_conflict(&header, 10, &status),
        "conflict when another active txn holds xmax"
    );
}

#[test]
fn write_write_conflict_committed_xmax() {
    let header = MvccHeader::new(5, 7, 0);
    let status = MockStatus::new().with_committed(5).with_committed(7);

    assert!(
        VisibilityCheck::check_write_conflict(&header, 10, &status),
        "conflict when another committed txn holds xmax"
    );
}

#[test]
fn no_write_conflict_own_xmax() {
    let header = MvccHeader::new(5, 10, 0);
    let status = MockStatus::new().with_committed(5).with_active(10);

    assert!(
        !VisibilityCheck::check_write_conflict(&header, 10, &status),
        "no conflict when we ourselves set xmax"
    );
}

// ===========================================================================
// 4. No phantom reads within a single statement (snapshot-based)
// ===========================================================================

#[test]
fn single_snapshot_provides_consistent_reads() {
    let snap = Snapshot::new(1, 10, HashSet::new());
    let header = MvccHeader::new_insert(15, 0);
    let status = MockStatus::new().with_committed(15);

    for _ in 0..100 {
        assert!(
            !VisibilityCheck::is_visible(&header, 5, &snap, &status),
            "future txn must never be visible within a single snapshot"
        );
    }
}

#[test]
fn phantom_reads_possible_across_refreshes() {
    let mgr = TransactionManager::new();

    let mut w_a = mgr.begin(IsolationLevel::ReadCommitted);
    let wa_id = w_a.txn_id;
    let row_a = MvccHeader::new_insert(wa_id, 0);
    mgr.commit(&mut w_a).unwrap();

    let mut reader = mgr.begin(IsolationLevel::ReadCommitted);
    let reader_id = reader.txn_id;

    let status = ManagerStatus { mgr: &mgr };
    let mut scan_1 = Vec::new();
    if VisibilityCheck::is_visible(&row_a, reader_id, &reader.snapshot, &status) {
        scan_1.push("row_a");
    }
    assert_eq!(scan_1, vec!["row_a"]);

    let mut w_b = mgr.begin(IsolationLevel::ReadCommitted);
    let wb_id = w_b.txn_id;
    let row_b = MvccHeader::new_insert(wb_id, 0);
    mgr.commit(&mut w_b).unwrap();

    mgr.refresh_snapshot(&mut reader);
    let status = ManagerStatus { mgr: &mgr };
    let mut scan_2 = Vec::new();
    if VisibilityCheck::is_visible(&row_a, reader_id, &reader.snapshot, &status) {
        scan_2.push("row_a");
    }
    if VisibilityCheck::is_visible(&row_b, reader_id, &reader.snapshot, &status) {
        scan_2.push("row_b");
    }
    assert_eq!(scan_2, vec!["row_a", "row_b"]);
    assert!(scan_2.len() > scan_1.len(), "Phantom read: new row appeared");
}

// ===========================================================================
// 5. Multiple refreshes show progressively more committed data
// ===========================================================================

#[test]
fn progressive_refreshes_reveal_new_commits() {
    let tm = TransactionManager::new();

    let mut reader = tm.begin(IsolationLevel::ReadCommitted);

    let mut writer_ids = Vec::new();
    for _ in 0..3 {
        let mut tw = tm.begin(IsolationLevel::ReadCommitted);
        writer_ids.push(tw.txn_id);
        tm.commit(&mut tw).unwrap();
    }

    for &wid in &writer_ids {
        assert!(!reader.snapshot.is_visible(wid));
    }

    tm.refresh_snapshot(&mut reader);
    for &wid in &writer_ids {
        assert!(reader.snapshot.is_visible(wid));
    }

    let mut tw4 = tm.begin(IsolationLevel::ReadCommitted);
    let w4_id = tw4.txn_id;
    tm.commit(&mut tw4).unwrap();

    assert!(!reader.snapshot.is_visible(w4_id));
    tm.refresh_snapshot(&mut reader);
    assert!(reader.snapshot.is_visible(w4_id));
}

#[test]
fn refresh_only_applies_to_read_committed() {
    let tm = TransactionManager::new();

    let mut t_rc = tm.begin(IsolationLevel::ReadCommitted);
    let mut t_rr = tm.begin(IsolationLevel::RepeatableRead);
    let mut t_si = tm.begin(IsolationLevel::Serializable);

    let rc_xmax = t_rc.snapshot.xmax;
    let rr_xmax = t_rr.snapshot.xmax;
    let si_xmax = t_si.snapshot.xmax;

    let mut tw = tm.begin(IsolationLevel::ReadCommitted);
    tm.commit(&mut tw).unwrap();

    tm.refresh_snapshot(&mut t_rc);
    tm.refresh_snapshot(&mut t_rr);
    tm.refresh_snapshot(&mut t_si);

    assert!(t_rc.snapshot.xmax > rc_xmax, "READ COMMITTED xmax should advance");
    assert_eq!(t_rr.snapshot.xmax, rr_xmax, "REPEATABLE READ xmax unchanged");
    assert_eq!(t_si.snapshot.xmax, si_xmax, "SERIALIZABLE xmax unchanged");
}
