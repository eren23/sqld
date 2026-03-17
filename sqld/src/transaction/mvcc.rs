use std::collections::HashSet;

use crate::types::tuple::MvccHeader;

use super::transaction::TransactionStatus;

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Lowest active txn_id at snapshot time.
    pub xmin: u64,
    /// Next txn_id to be assigned (exclusive upper bound).
    pub xmax: u64,
    /// Set of txn_ids that were in-progress at snapshot time.
    pub active_txns: HashSet<u64>,
}

impl Snapshot {
    pub fn new(xmin: u64, xmax: u64, active_txns: HashSet<u64>) -> Self {
        Self {
            xmin,
            xmax,
            active_txns,
        }
    }

    /// A transaction is "visible in snapshot" if it committed before the
    /// snapshot was taken: its id is < xmax AND it is NOT in the active set.
    pub fn is_visible(&self, txn_id: u64) -> bool {
        txn_id < self.xmax && !self.active_txns.contains(&txn_id)
    }
}

// ---------------------------------------------------------------------------
// Transaction status lookup (trait for decoupling)
// ---------------------------------------------------------------------------

pub trait TxnStatusLookup {
    fn status_of(&self, txn_id: u64) -> Option<TransactionStatus>;
}

// ---------------------------------------------------------------------------
// Visibility check
// ---------------------------------------------------------------------------

pub struct VisibilityCheck;

impl VisibilityCheck {
    /// Determine if a tuple is visible to the given transaction.
    ///
    /// Rules (PostgreSQL-style MVCC):
    ///
    /// 1. xmin must be "visible":
    ///    - If xmin == our txn_id → we created it, visible (unless deleted by us).
    ///    - Else xmin must be committed AND in our snapshot.
    ///
    /// 2. xmax must NOT hide the tuple:
    ///    - If xmax == 0 (invalid) → not deleted, visible.
    ///    - If xmax == our txn_id → we deleted it, NOT visible.
    ///    - If xmax's transaction aborted → deletion rolled back, visible.
    ///    - If xmax committed but NOT in our snapshot → we don't see the delete yet, visible.
    ///    - If xmax committed AND in our snapshot → deleted before our snapshot, NOT visible.
    pub fn is_visible(
        header: &MvccHeader,
        txn_id: u64,
        snapshot: &Snapshot,
        status_lookup: &dyn TxnStatusLookup,
    ) -> bool {
        // --- Check xmin (creator) ---
        let xmin_visible = if header.xmin == txn_id {
            // We created this tuple — visible to us.
            true
        } else {
            match status_lookup.status_of(header.xmin) {
                Some(TransactionStatus::Committed) => snapshot.is_visible(header.xmin),
                _ => false, // Active or Aborted xmin → not visible.
            }
        };

        if !xmin_visible {
            return false;
        }

        // --- Check xmax (deleter) ---
        if header.xmax == 0 {
            // Not deleted.
            return true;
        }

        if header.xmax == txn_id {
            // We deleted it — not visible to us.
            return false;
        }

        match status_lookup.status_of(header.xmax) {
            Some(TransactionStatus::Aborted) => {
                // Deletion was rolled back — still visible.
                true
            }
            Some(TransactionStatus::Committed) => {
                // Deleted by a committed txn. Visible only if the deleting
                // txn is NOT in our snapshot (i.e., committed after snapshot).
                !snapshot.is_visible(header.xmax)
            }
            _ => {
                // Deleter is still active (and not us) — tuple is still visible.
                true
            }
        }
    }

    /// Check if a write-write conflict exists: another active or committed
    /// transaction has already written (deleted/updated) this tuple.
    pub fn check_write_conflict(
        header: &MvccHeader,
        txn_id: u64,
        status_lookup: &dyn TxnStatusLookup,
    ) -> bool {
        if header.xmax == 0 || header.xmax == txn_id {
            return false;
        }
        match status_lookup.status_of(header.xmax) {
            Some(TransactionStatus::Committed) => true,
            Some(TransactionStatus::Active) => true,
            _ => false,
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn own_insert_visible() {
        let snap = Snapshot::new(10, 20, HashSet::new());
        let header = MvccHeader::new_insert(10, 0);
        let status = MockStatus::new();
        assert!(VisibilityCheck::is_visible(&header, 10, &snap, &status));
    }

    #[test]
    fn own_delete_not_visible() {
        let snap = Snapshot::new(10, 20, HashSet::new());
        let header = MvccHeader::new(5, 10, 0);
        let mut status = MockStatus::new();
        status.committed.insert(5);
        assert!(!VisibilityCheck::is_visible(&header, 10, &snap, &status));
    }

    #[test]
    fn committed_insert_in_snapshot_visible() {
        let snap = Snapshot::new(1, 20, HashSet::new());
        let header = MvccHeader::new_insert(5, 0);
        let mut status = MockStatus::new();
        status.committed.insert(5);
        assert!(VisibilityCheck::is_visible(&header, 10, &snap, &status));
    }

    #[test]
    fn uncommitted_insert_not_visible() {
        let snap = Snapshot::new(1, 20, HashSet::new());
        let header = MvccHeader::new_insert(5, 0);
        let mut status = MockStatus::new();
        status.active.insert(5);
        assert!(!VisibilityCheck::is_visible(&header, 10, &snap, &status));
    }

    #[test]
    fn aborted_xmax_still_visible() {
        let snap = Snapshot::new(1, 20, HashSet::new());
        let header = MvccHeader::new(5, 7, 0);
        let mut status = MockStatus::new();
        status.committed.insert(5);
        status.aborted.insert(7);
        assert!(VisibilityCheck::is_visible(&header, 10, &snap, &status));
    }

    #[test]
    fn active_xmin_in_snapshot_not_visible() {
        let mut active = HashSet::new();
        active.insert(5u64);
        let snap = Snapshot::new(1, 20, active);
        let header = MvccHeader::new_insert(5, 0);
        let mut status = MockStatus::new();
        status.active.insert(5);
        assert!(!VisibilityCheck::is_visible(&header, 10, &snap, &status));
    }

    #[test]
    fn snapshot_visibility() {
        let mut active = HashSet::new();
        active.insert(3u64);
        let snap = Snapshot::new(1, 10, active);

        assert!(snap.is_visible(1));
        assert!(snap.is_visible(2));
        assert!(!snap.is_visible(3)); // Was active.
        assert!(snap.is_visible(5));
        assert!(snap.is_visible(9));
        assert!(!snap.is_visible(10)); // >= xmax.
        assert!(!snap.is_visible(100));
    }
}
