use crate::utils::error::Error;
use crate::wal::wal_manager::WalManager;
use crate::wal::wal_record::WalRecord;

// ---------------------------------------------------------------------------
// Checkpoint Manager
// ---------------------------------------------------------------------------

/// Performs fuzzy checkpointing:
///
/// 1. Write `CheckpointBegin` with the set of active transactions.
/// 2. (Caller flushes dirty pages to disk.)
/// 3. Write `CheckpointEnd` referencing the begin LSN.
/// 4. Update the stored checkpoint LSN.
/// 5. Optionally truncate the WAL before the checkpoint LSN.
pub struct CheckpointManager;

/// Trait for flushing dirty pages during checkpoint. Abstracts away the
/// buffer pool so that tests can provide a no-op implementation.
pub trait DirtyPageFlusher {
    /// Flush all dirty pages to disk. The WAL must already be flushed up to
    /// at least `wal_flushed_lsn` before this is called.
    fn flush_dirty_pages(&self) -> Result<(), Error>;
}

impl CheckpointManager {
    /// Run a full checkpoint cycle.
    ///
    /// Returns the LSN of the CheckpointBegin record.
    pub fn checkpoint(
        wal: &WalManager,
        flusher: &dyn DirtyPageFlusher,
    ) -> Result<u64, Error> {
        // Step 1: Record active transactions
        let active_txns = wal.active_txn_ids();

        // Step 2: Write CheckpointBegin
        let begin_lsn = wal.append(WalRecord::CheckpointBegin {
            active_txns,
        })?;

        // Step 3: Flush WAL so all records up to now are durable
        wal.flush()?;

        // Step 4: Flush dirty pages to disk
        flusher.flush_dirty_pages()?;

        // Step 5: Write CheckpointEnd
        wal.append(WalRecord::CheckpointEnd {
            checkpoint_begin_lsn: begin_lsn,
        })?;
        wal.flush()?;

        // Step 6: Update metadata with checkpoint LSN
        wal.set_last_checkpoint_lsn(begin_lsn)?;

        Ok(begin_lsn)
    }

    /// Checkpoint and then truncate WAL entries before the checkpoint LSN.
    pub fn checkpoint_and_truncate(
        wal: &WalManager,
        flusher: &dyn DirtyPageFlusher,
    ) -> Result<u64, Error> {
        let begin_lsn = Self::checkpoint(wal, flusher)?;

        // Truncate WAL entries before the checkpoint
        wal.truncate_before(begin_lsn)?;

        Ok(begin_lsn)
    }
}

// ---------------------------------------------------------------------------
// No-op flusher for testing
// ---------------------------------------------------------------------------

/// A flusher that does nothing (for testing checkpoint logic in isolation).
pub struct NoOpFlusher;

impl DirtyPageFlusher for NoOpFlusher {
    fn flush_dirty_pages(&self) -> Result<(), Error> {
        Ok(())
    }
}
