use std::collections::HashMap;

use crate::storage::page::PageId;
use crate::utils::error::Error;
use crate::wal::wal_manager::WalManager;
use crate::wal::wal_record::{WalEntry, WalRecord};


// ---------------------------------------------------------------------------
// Recovery state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnStatus {
    Active,
    Committed,
    Aborted,
}

#[derive(Debug, Clone)]
pub struct TxnState {
    pub last_lsn: u64,
    pub status: TxnStatus,
}

#[derive(Debug, Clone)]
pub struct RecoveryState {
    /// page_id -> rec_lsn: the LSN of the earliest record that might have
    /// dirtied this page and not yet been flushed to disk.
    pub dirty_page_table: HashMap<PageId, u64>,
    /// txn_id -> state: all transactions that were active at some point
    /// during the analyzed log range.
    pub active_txn_table: HashMap<u64, TxnState>,
}

// ---------------------------------------------------------------------------
// Page store trait (abstraction for testing)
// ---------------------------------------------------------------------------

/// Trait abstracting page-level I/O so recovery can be tested without a real
/// buffer pool.
pub trait PageStore {
    /// Read the current LSN of a page. Returns 0 if the page doesn't exist.
    fn page_lsn(&self, page_id: PageId) -> u64;
    /// Apply a redo action to a page, updating its LSN.
    fn redo_record(&mut self, page_id: PageId, lsn: u64, record: &WalRecord);
    /// Undo a record on a page (reverse the operation).
    fn undo_record(&mut self, page_id: PageId, record: &WalRecord);
}

// ---------------------------------------------------------------------------
// ARIES Recovery Manager
// ---------------------------------------------------------------------------

pub struct RecoveryManager;

impl RecoveryManager {
    /// Run full ARIES 3-phase recovery.
    ///
    /// 1. **Analysis** — scan from last checkpoint, rebuild dirty page table
    ///    and active transaction table.
    /// 2. **Redo** — forward scan from the earliest rec_lsn, re-apply records
    ///    where page_lsn < record_lsn (idempotent).
    /// 3. **Undo** — walk undo chains for uncommitted transactions, writing
    ///    CLRs. CLRs are never undone.
    pub fn recover(
        wal: &WalManager,
        store: &mut dyn PageStore,
    ) -> Result<RecoveryState, Error> {
        // Flush any buffered WAL data so we read everything
        wal.flush()?;

        let all_entries = wal.read_all_entries()?;
        let checkpoint_lsn = wal.last_checkpoint_lsn();

        // Phase 1: Analysis
        let mut state = Self::analysis(&all_entries, checkpoint_lsn);

        // Phase 2: Redo
        Self::redo(&all_entries, &state, store);

        // Phase 3: Undo
        Self::undo(&all_entries, &mut state, wal, store)?;

        Ok(state)
    }

    /// Phase 1: Analysis — scan from the last checkpoint to reconstruct the
    /// dirty page table and active transaction table.
    pub fn analysis(entries: &[WalEntry], checkpoint_lsn: u64) -> RecoveryState {
        let mut state = RecoveryState {
            dirty_page_table: HashMap::new(),
            active_txn_table: HashMap::new(),
        };

        // Find the starting point: the checkpoint begin record, or the start
        // of the log if no checkpoint.
        let start_idx = if checkpoint_lsn == 0 {
            0
        } else {
            entries
                .iter()
                .position(|e| e.lsn == checkpoint_lsn)
                .unwrap_or(0)
        };

        for entry in &entries[start_idx..] {
            let record = &entry.record;

            // Process checkpoint begin: seed active txn table
            if let WalRecord::CheckpointBegin { active_txns } = record {
                for &txn_id in active_txns {
                    state
                        .active_txn_table
                        .entry(txn_id)
                        .or_insert(TxnState {
                            last_lsn: 0,
                            status: TxnStatus::Active,
                        });
                }
            }

            // Track transaction state
            if let Some(txn_id) = record.txn_id() {
                match record {
                    WalRecord::Begin { .. } => {
                        state.active_txn_table.insert(
                            txn_id,
                            TxnState {
                                last_lsn: entry.lsn,
                                status: TxnStatus::Active,
                            },
                        );
                    }
                    WalRecord::Commit { .. } => {
                        if let Some(ts) = state.active_txn_table.get_mut(&txn_id) {
                            ts.status = TxnStatus::Committed;
                            ts.last_lsn = entry.lsn;
                        }
                    }
                    WalRecord::Abort { .. } => {
                        if let Some(ts) = state.active_txn_table.get_mut(&txn_id) {
                            ts.status = TxnStatus::Aborted;
                            ts.last_lsn = entry.lsn;
                        }
                    }
                    _ => {
                        // Data/index/CLR records: update last_lsn
                        if let Some(ts) = state.active_txn_table.get_mut(&txn_id) {
                            ts.last_lsn = entry.lsn;
                        } else {
                            // Transaction started before checkpoint
                            state.active_txn_table.insert(
                                txn_id,
                                TxnState {
                                    last_lsn: entry.lsn,
                                    status: TxnStatus::Active,
                                },
                            );
                        }
                    }
                }
            }

            // Track dirty pages
            if let Some(page_id) = record.affected_page() {
                state
                    .dirty_page_table
                    .entry(page_id)
                    .or_insert(entry.lsn);
            }
        }

        state
    }

    /// Phase 2: Redo — forward scan. Re-apply records where page LSN < record
    /// LSN (the page is behind this log record).
    pub fn redo(
        entries: &[WalEntry],
        state: &RecoveryState,
        store: &mut dyn PageStore,
    ) {
        // Find the minimum rec_lsn in the dirty page table — this is where
        // redo must start.
        let min_rec_lsn = state
            .dirty_page_table
            .values()
            .copied()
            .min()
            .unwrap_or(u64::MAX);

        for entry in entries {
            if entry.lsn < min_rec_lsn {
                continue;
            }

            let record = &entry.record;

            // Determine the affected page, considering CLRs
            let page_id = match record {
                WalRecord::Clr { redo, .. } => redo.affected_page(),
                other => other.affected_page(),
            };

            let Some(page_id) = page_id else {
                continue;
            };

            // Only redo if this page is in the dirty page table and the
            // record's LSN is >= the rec_lsn for this page.
            if let Some(&rec_lsn) = state.dirty_page_table.get(&page_id) {
                if entry.lsn < rec_lsn {
                    continue;
                }
            } else {
                continue; // page not dirty — skip
            }

            // Idempotency check: only redo if page_lsn < record_lsn
            let current_page_lsn = store.page_lsn(page_id);
            if current_page_lsn >= entry.lsn {
                continue; // already applied
            }

            // Apply the redo
            match record {
                WalRecord::Clr { redo, .. } => {
                    store.redo_record(page_id, entry.lsn, redo);
                }
                _ => {
                    store.redo_record(page_id, entry.lsn, record);
                }
            }
        }
    }

    /// Phase 3: Undo — walk undo chains for all uncommitted (Active)
    /// transactions, writing CLRs. CLRs are never undone.
    pub fn undo(
        entries: &[WalEntry],
        state: &mut RecoveryState,
        wal: &WalManager,
        store: &mut dyn PageStore,
    ) -> Result<(), Error> {
        // Build an index from LSN -> entry for fast lookup
        let entry_map: HashMap<u64, &WalEntry> =
            entries.iter().map(|e| (e.lsn, e)).collect();

        // Collect transactions that need undo (Active status)
        let to_undo: Vec<(u64, u64)> = state
            .active_txn_table
            .iter()
            .filter(|(_, ts)| ts.status == TxnStatus::Active)
            .map(|(&txn_id, ts)| (txn_id, ts.last_lsn))
            .collect();

        // Collect txn_ids for the abort phase later
        let undo_txn_ids: Vec<u64> = to_undo.iter().map(|(tid, _)| *tid).collect();

        // Use a max-heap approach: always undo the record with the highest LSN
        // first (to undo in reverse order across all transactions).
        let mut undo_list: Vec<(u64, u64)> = to_undo; // (txn_id, next_lsn_to_undo)

        loop {
            // Find the entry with the highest LSN to undo
            let mut best_idx = None;
            let mut best_lsn = 0u64;
            for (i, &(_, lsn)) in undo_list.iter().enumerate() {
                if lsn > 0 && lsn > best_lsn {
                    best_lsn = lsn;
                    best_idx = Some(i);
                }
            }

            let Some(idx) = best_idx else {
                break; // all done
            };

            let (txn_id, undo_lsn) = undo_list[idx];

            let Some(entry) = entry_map.get(&undo_lsn) else {
                // Entry not found — skip (shouldn't happen in a valid WAL)
                undo_list[idx].1 = 0;
                continue;
            };

            let record = &entry.record;

            // CLRs are never undone — follow their undo_next_lsn
            if let WalRecord::Clr { undo_next_lsn, .. } = record {
                undo_list[idx].1 = *undo_next_lsn;
                continue;
            }

            // Skip non-undoable records (Begin, Commit, Abort, Checkpoint*)
            let needs_undo = matches!(
                record,
                WalRecord::InsertTuple { .. }
                    | WalRecord::DeleteTuple { .. }
                    | WalRecord::UpdateTuple { .. }
                    | WalRecord::IndexInsert { .. }
                    | WalRecord::IndexDelete { .. }
                    | WalRecord::PageAlloc { .. }
                    | WalRecord::PageFree { .. }
            );

            if needs_undo {
                // Build the compensating action (redo of the inverse)
                let compensation = Self::build_compensation(record);

                if let Some(page_id) = record.affected_page() {
                    // Undo on the page
                    store.undo_record(page_id, record);
                }

                // Write a CLR
                let undo_next = entry.prev_lsn;
                if let Some(comp) = compensation {
                    wal.append(WalRecord::Clr {
                        txn_id,
                        undo_next_lsn: undo_next,
                        redo: Box::new(comp),
                    })?;
                }
            }

            // Follow the undo chain
            undo_list[idx].1 = entry.prev_lsn;
        }

        // Mark all undone transactions as aborted
        for txn_id in &undo_txn_ids {
            if let Some(ts) = state.active_txn_table.get_mut(txn_id) {
                ts.status = TxnStatus::Aborted;
            }
            // Write abort record
            wal.append(WalRecord::Abort { txn_id: *txn_id })?;
        }

        wal.flush()?;
        Ok(())
    }

    /// Build the compensation (redo-of-undo) record for a given record.
    fn build_compensation(record: &WalRecord) -> Option<WalRecord> {
        match record {
            WalRecord::InsertTuple {
                txn_id,
                page_id,
                slot_index,
                data,
            } => Some(WalRecord::DeleteTuple {
                txn_id: *txn_id,
                page_id: *page_id,
                slot_index: *slot_index,
                data: data.clone(),
            }),
            WalRecord::DeleteTuple {
                txn_id,
                page_id,
                slot_index,
                data,
            } => Some(WalRecord::InsertTuple {
                txn_id: *txn_id,
                page_id: *page_id,
                slot_index: *slot_index,
                data: data.clone(),
            }),
            WalRecord::UpdateTuple {
                txn_id,
                page_id,
                slot_index,
                old_data,
                new_data,
            } => Some(WalRecord::UpdateTuple {
                txn_id: *txn_id,
                page_id: *page_id,
                slot_index: *slot_index,
                old_data: new_data.clone(),
                new_data: old_data.clone(),
            }),
            WalRecord::IndexInsert {
                txn_id,
                index_page_id,
                key,
                tid_page,
                tid_slot,
            } => Some(WalRecord::IndexDelete {
                txn_id: *txn_id,
                index_page_id: *index_page_id,
                key: key.clone(),
                tid_page: *tid_page,
                tid_slot: *tid_slot,
            }),
            WalRecord::IndexDelete {
                txn_id,
                index_page_id,
                key,
                tid_page,
                tid_slot,
            } => Some(WalRecord::IndexInsert {
                txn_id: *txn_id,
                index_page_id: *index_page_id,
                key: key.clone(),
                tid_page: *tid_page,
                tid_slot: *tid_slot,
            }),
            WalRecord::PageAlloc { txn_id, page_id } => Some(WalRecord::PageFree {
                txn_id: *txn_id,
                page_id: *page_id,
            }),
            WalRecord::PageFree { txn_id, page_id } => Some(WalRecord::PageAlloc {
                txn_id: *txn_id,
                page_id: *page_id,
            }),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// In-memory page store for testing
// ---------------------------------------------------------------------------

/// Simple in-memory page store for unit testing recovery.
pub struct MemoryPageStore {
    /// page_id -> (lsn, data as a map of slot -> bytes)
    pages: HashMap<PageId, MemoryPage>,
}

#[derive(Debug, Clone)]
struct MemoryPage {
    lsn: u64,
    slots: HashMap<u16, Vec<u8>>,
    allocated: bool,
}

impl MemoryPageStore {
    pub fn new() -> Self {
        MemoryPageStore {
            pages: HashMap::new(),
        }
    }

    pub fn get_slot(&self, page_id: PageId, slot: u16) -> Option<&Vec<u8>> {
        self.pages
            .get(&page_id)
            .and_then(|p| p.slots.get(&slot))
    }

    pub fn is_allocated(&self, page_id: PageId) -> bool {
        self.pages
            .get(&page_id)
            .map_or(false, |p| p.allocated)
    }

    fn ensure_page(&mut self, page_id: PageId) -> &mut MemoryPage {
        self.pages.entry(page_id).or_insert_with(|| MemoryPage {
            lsn: 0,
            slots: HashMap::new(),
            allocated: true,
        })
    }
}

impl PageStore for MemoryPageStore {
    fn page_lsn(&self, page_id: PageId) -> u64 {
        self.pages.get(&page_id).map_or(0, |p| p.lsn)
    }

    fn redo_record(&mut self, page_id: PageId, lsn: u64, record: &WalRecord) {
        let page = self.ensure_page(page_id);
        match record {
            WalRecord::InsertTuple {
                slot_index, data, ..
            } => {
                page.slots.insert(*slot_index, data.clone());
            }
            WalRecord::DeleteTuple { slot_index, .. } => {
                page.slots.remove(slot_index);
            }
            WalRecord::UpdateTuple {
                slot_index,
                new_data,
                ..
            } => {
                page.slots.insert(*slot_index, new_data.clone());
            }
            WalRecord::IndexInsert {
                key,
                tid_page,
                tid_slot,
                ..
            } => {
                // Store as slot 0 with key+tid concatenated
                let mut val = key.clone();
                val.extend_from_slice(&tid_page.to_le_bytes());
                val.extend_from_slice(&tid_slot.to_le_bytes());
                page.slots.insert(0, val);
            }
            WalRecord::IndexDelete { .. } => {
                page.slots.remove(&0);
            }
            WalRecord::PageAlloc { .. } => {
                page.allocated = true;
            }
            WalRecord::PageFree { .. } => {
                page.allocated = false;
                page.slots.clear();
            }
            _ => {}
        }
        page.lsn = lsn;
    }

    fn undo_record(&mut self, page_id: PageId, record: &WalRecord) {
        let page = self.ensure_page(page_id);
        match record {
            WalRecord::InsertTuple { slot_index, .. } => {
                page.slots.remove(slot_index);
            }
            WalRecord::DeleteTuple {
                slot_index, data, ..
            } => {
                page.slots.insert(*slot_index, data.clone());
            }
            WalRecord::UpdateTuple {
                slot_index,
                old_data,
                ..
            } => {
                page.slots.insert(*slot_index, old_data.clone());
            }
            WalRecord::IndexInsert { .. } => {
                page.slots.remove(&0);
            }
            WalRecord::IndexDelete {
                key,
                tid_page,
                tid_slot,
                ..
            } => {
                let mut val = key.clone();
                val.extend_from_slice(&tid_page.to_le_bytes());
                val.extend_from_slice(&tid_slot.to_le_bytes());
                page.slots.insert(0, val);
            }
            WalRecord::PageAlloc { .. } => {
                page.allocated = false;
            }
            WalRecord::PageFree { .. } => {
                page.allocated = true;
            }
            _ => {}
        }
    }
}
