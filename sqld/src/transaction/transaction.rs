use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::utils::error::Error;

use super::lock_manager::LockManager;
use super::mvcc::Snapshot;
use super::savepoint::Savepoint;

// ---------------------------------------------------------------------------
// Isolation levels
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

// ---------------------------------------------------------------------------
// Transaction status
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    Active,
    Committed,
    Aborted,
}

// ---------------------------------------------------------------------------
// Write-set entry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WriteEntry {
    pub table_id: u64,
    pub tuple_id: u64,
}

// ---------------------------------------------------------------------------
// Read-set entry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReadEntry {
    pub table_id: u64,
    pub tuple_id: u64,
}

// ---------------------------------------------------------------------------
// Transaction
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct Transaction {
    pub txn_id: u64,
    pub status: TransactionStatus,
    pub isolation_level: IsolationLevel,
    pub snapshot: Snapshot,
    pub write_set: Vec<WriteEntry>,
    pub read_set: Vec<ReadEntry>,
    pub savepoints: Vec<Savepoint>,
    pub start_time: Instant,
    pub commit_time: Option<Instant>,
    /// Command counter incremented per statement.
    pub command_id: u32,
}

impl Transaction {
    pub fn new(txn_id: u64, isolation_level: IsolationLevel, snapshot: Snapshot) -> Self {
        Self {
            txn_id,
            status: TransactionStatus::Active,
            isolation_level,
            snapshot,
            write_set: Vec::new(),
            read_set: Vec::new(),
            savepoints: Vec::new(),
            start_time: Instant::now(),
            commit_time: None,
            command_id: 0,
        }
    }

    pub fn is_active(&self) -> bool {
        self.status == TransactionStatus::Active
    }

    pub fn next_command_id(&mut self) -> u32 {
        let cid = self.command_id;
        self.command_id += 1;
        cid
    }

    pub fn add_write(&mut self, table_id: u64, tuple_id: u64) {
        self.write_set.push(WriteEntry { table_id, tuple_id });
    }

    pub fn add_read(&mut self, table_id: u64, tuple_id: u64) {
        self.read_set.push(ReadEntry { table_id, tuple_id });
    }

    pub fn write_count(&self) -> usize {
        self.write_set.len()
    }

    // -- Savepoint operations -----------------------------------------------

    pub fn create_savepoint(&mut self, name: String) {
        let sp = Savepoint::new(name, self.write_set.len(), self.read_set.len());
        self.savepoints.push(sp);
    }

    pub fn rollback_to_savepoint(&mut self, name: &str) -> Result<(), Error> {
        let idx = self
            .savepoints
            .iter()
            .rposition(|sp| sp.name == name)
            .ok_or_else(|| {
                Error::Internal(format!("savepoint '{name}' does not exist"))
            })?;

        let sp = &self.savepoints[idx];
        let write_pos = sp.write_set_position;
        let read_pos = sp.read_set_position;

        // Truncate write/read sets back to savepoint position.
        self.write_set.truncate(write_pos);
        self.read_set.truncate(read_pos);

        // Remove savepoints created after this one (but keep this one).
        self.savepoints.truncate(idx + 1);

        Ok(())
    }

    pub fn release_savepoint(&mut self, name: &str) -> Result<(), Error> {
        let idx = self
            .savepoints
            .iter()
            .rposition(|sp| sp.name == name)
            .ok_or_else(|| {
                Error::Internal(format!("savepoint '{name}' does not exist"))
            })?;

        // Remove this savepoint and any nested ones created after it.
        self.savepoints.truncate(idx);

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Transaction Manager
// ---------------------------------------------------------------------------

pub struct TransactionManager {
    next_txn_id: AtomicU64,
    /// Committed transaction IDs and their commit timestamps (monotonic).
    committed: Mutex<HashSet<u64>>,
    /// Currently active transaction IDs.
    active: Mutex<HashSet<u64>>,
    pub lock_manager: Arc<LockManager>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            next_txn_id: AtomicU64::new(1),
            committed: Mutex::new(HashSet::new()),
            active: Mutex::new(HashSet::new()),
            lock_manager: Arc::new(LockManager::new()),
        }
    }

    pub fn begin(&self, isolation_level: IsolationLevel) -> Transaction {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);

        // Build snapshot.
        let snapshot = self.create_snapshot(txn_id);

        // Register as active.
        self.active.lock().unwrap().insert(txn_id);

        Transaction::new(txn_id, isolation_level, snapshot)
    }

    pub fn commit(&self, txn: &mut Transaction) -> Result<(), Error> {
        if txn.status != TransactionStatus::Active {
            return Err(Error::Internal(format!(
                "transaction {} is not active",
                txn.txn_id
            )));
        }

        txn.status = TransactionStatus::Committed;
        txn.commit_time = Some(Instant::now());

        {
            let mut active = self.active.lock().unwrap();
            active.remove(&txn.txn_id);
        }
        {
            let mut committed = self.committed.lock().unwrap();
            committed.insert(txn.txn_id);
        }

        // Release all locks held by this transaction.
        self.lock_manager.release_all(txn.txn_id);

        Ok(())
    }

    pub fn abort(&self, txn: &mut Transaction) {
        txn.status = TransactionStatus::Aborted;

        {
            let mut active = self.active.lock().unwrap();
            active.remove(&txn.txn_id);
        }

        // Release all locks held by this transaction.
        self.lock_manager.release_all(txn.txn_id);
    }

    pub fn is_committed(&self, txn_id: u64) -> bool {
        self.committed.lock().unwrap().contains(&txn_id)
    }

    pub fn is_active(&self, txn_id: u64) -> bool {
        self.active.lock().unwrap().contains(&txn_id)
    }

    /// Create a snapshot of the current database state.
    pub fn create_snapshot(&self, txn_id: u64) -> Snapshot {
        let active = self.active.lock().unwrap();
        let active_txns: HashSet<u64> = active.clone();

        // xmin: lowest active txn id (or our own id if none active).
        let xmin = active_txns.iter().copied().min().unwrap_or(txn_id);
        // xmax: next txn id to be assigned.
        let xmax = self.next_txn_id.load(Ordering::SeqCst);

        Snapshot {
            xmin,
            xmax,
            active_txns,
        }
    }

    /// For READ COMMITTED: refresh snapshot to see newly committed data.
    pub fn refresh_snapshot(&self, txn: &mut Transaction) {
        if txn.isolation_level == IsolationLevel::ReadCommitted {
            txn.snapshot = self.create_snapshot(txn.txn_id);
        }
    }

    /// Pick deadlock victim: transaction with fewest writes.
    pub fn pick_deadlock_victim(&self, candidates: &[u64]) -> Option<u64> {
        // This is called externally with transaction write counts.
        // For now, return smallest txn_id as heuristic (fewest writes resolved externally).
        candidates.iter().copied().min()
    }

    pub fn active_transaction_ids(&self) -> Vec<u64> {
        let active = self.active.lock().unwrap();
        active.iter().copied().collect()
    }
}
