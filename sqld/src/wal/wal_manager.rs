use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Instant;

use crate::utils::error::Error;
use crate::wal::wal_record::{WalEntry, WalRecord, WAL_ENTRY_HEADER_SIZE};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// In-memory WAL buffer size: 64 KB.
const WAL_BUFFER_SIZE: usize = 64 * 1024;

/// Flush timeout: buffer is flushed if older than this.
const FLUSH_TIMEOUT_MS: u128 = 10;

/// WAL file name.
const WAL_FILE_NAME: &str = "wal.log";
/// Metadata file storing the last checkpoint LSN.
const WAL_META_FILE_NAME: &str = "wal.meta";

// ---------------------------------------------------------------------------
// WalManager
// ---------------------------------------------------------------------------

pub struct WalManager {
    inner: Mutex<WalManagerInner>,
}

struct WalManagerInner {
    file: File,
    dir: PathBuf,
    /// In-memory write buffer.
    buffer: Vec<u8>,
    /// Next LSN to assign (= current file size + buffer length).
    current_lsn: u64,
    /// LSN up to which data has been fsync'd to disk.
    flushed_lsn: u64,
    /// Per-transaction: the LSN of the last record written by that txn.
    txn_prev_lsn: HashMap<u64, u64>,
    /// Timestamp of the first unflushed write (for timeout-based flush).
    first_unflushed_time: Option<Instant>,
    /// Last checkpoint begin LSN (loaded from metadata on startup).
    last_checkpoint_lsn: u64,
}

impl WalManager {
    /// Open (or create) a WAL in the given directory.
    pub fn open(dir: &Path) -> Result<Self, Error> {
        std::fs::create_dir_all(dir)?;
        let wal_path = dir.join(WAL_FILE_NAME);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&wal_path)?;

        let file_len = file.metadata()?.len();

        let last_checkpoint_lsn = Self::read_meta(dir).unwrap_or(0);

        Ok(WalManager {
            inner: Mutex::new(WalManagerInner {
                file,
                dir: dir.to_path_buf(),
                buffer: Vec::with_capacity(WAL_BUFFER_SIZE),
                current_lsn: file_len,
                flushed_lsn: file_len,
                txn_prev_lsn: HashMap::new(),
                first_unflushed_time: None,
                last_checkpoint_lsn,
            }),
        })
    }

    /// Append a record to the WAL. Returns the assigned LSN.
    ///
    /// The record is buffered; call [`flush`] or [`commit`] to persist.
    pub fn append(&self, record: WalRecord) -> Result<u64, Error> {
        let mut inner = self.inner.lock().unwrap();

        let txn_id = record.txn_id();
        let prev_lsn = txn_id
            .and_then(|tid| inner.txn_prev_lsn.get(&tid).copied())
            .unwrap_or(0);

        let lsn = inner.current_lsn;
        let entry = WalEntry::new(lsn, prev_lsn, record);
        let entry_bytes = entry.serialize();

        // Track per-txn undo chain
        if let Some(tid) = txn_id {
            inner.txn_prev_lsn.insert(tid, lsn);
        }

        // Append to buffer
        inner.buffer.extend_from_slice(&entry_bytes);
        inner.current_lsn += entry_bytes.len() as u64;

        if inner.first_unflushed_time.is_none() {
            inner.first_unflushed_time = Some(Instant::now());
        }

        // Auto-flush if buffer is full
        if inner.buffer.len() >= WAL_BUFFER_SIZE {
            Self::flush_inner(&mut inner)?;
        }

        Ok(lsn)
    }

    /// Flush the in-memory buffer to disk and fsync.
    pub fn flush(&self) -> Result<u64, Error> {
        let mut inner = self.inner.lock().unwrap();
        Self::flush_inner(&mut inner)
    }

    /// Append a Commit record and flush (group commit — the single fsync
    /// covers all buffered records from multiple transactions).
    pub fn commit(&self, txn_id: u64) -> Result<u64, Error> {
        let commit_lsn = self.append(WalRecord::Commit { txn_id })?;
        self.flush()?;
        // Clean up txn undo chain tracking
        let mut inner = self.inner.lock().unwrap();
        inner.txn_prev_lsn.remove(&txn_id);
        Ok(commit_lsn)
    }

    /// Append an Abort record and flush.
    pub fn abort(&self, txn_id: u64) -> Result<u64, Error> {
        let abort_lsn = self.append(WalRecord::Abort { txn_id })?;
        self.flush()?;
        let mut inner = self.inner.lock().unwrap();
        inner.txn_prev_lsn.remove(&txn_id);
        Ok(abort_lsn)
    }

    /// Check if the buffer should be flushed due to timeout.
    pub fn maybe_flush_timeout(&self) -> Result<bool, Error> {
        let mut inner = self.inner.lock().unwrap();
        if let Some(t) = inner.first_unflushed_time {
            if t.elapsed().as_millis() >= FLUSH_TIMEOUT_MS {
                Self::flush_inner(&mut inner)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The current (next-to-assign) LSN.
    pub fn current_lsn(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner.current_lsn
    }

    /// The LSN up to which data has been durably flushed.
    pub fn flushed_lsn(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner.flushed_lsn
    }

    /// Last checkpoint begin LSN.
    pub fn last_checkpoint_lsn(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner.last_checkpoint_lsn
    }

    /// Update the stored checkpoint LSN (called by checkpoint logic).
    pub fn set_last_checkpoint_lsn(&self, lsn: u64) -> Result<(), Error> {
        let mut inner = self.inner.lock().unwrap();
        inner.last_checkpoint_lsn = lsn;
        Self::write_meta(&inner.dir, lsn)
    }

    /// Read all WAL entries from the file (for recovery). This reads the
    /// on-disk data only (buffer must be flushed first for completeness).
    pub fn read_all_entries(&self) -> Result<Vec<WalEntry>, Error> {
        let inner = self.inner.lock().unwrap();
        Self::read_entries_from_file(&inner.dir)
    }

    /// Read WAL entries starting from a given LSN.
    pub fn read_entries_from(&self, start_lsn: u64) -> Result<Vec<WalEntry>, Error> {
        let entries = self.read_all_entries()?;
        Ok(entries.into_iter().filter(|e| e.lsn >= start_lsn).collect())
    }

    /// Read entries from the WAL file (static helper).
    pub fn read_entries_from_file(dir: &Path) -> Result<Vec<WalEntry>, Error> {
        let wal_path = dir.join(WAL_FILE_NAME);
        if !wal_path.exists() {
            return Ok(Vec::new());
        }
        let mut file = File::open(&wal_path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;
        Self::parse_entries(&data)
    }

    /// Parse a byte buffer into WAL entries, stopping at the first
    /// corruption/truncation (torn write tolerance).
    pub fn parse_entries(data: &[u8]) -> Result<Vec<WalEntry>, Error> {
        let mut entries = Vec::new();
        let mut offset = 0;
        while offset < data.len() {
            // Need at least a header to proceed
            if data.len() - offset < WAL_ENTRY_HEADER_SIZE {
                break; // torn write — partial header
            }
            match WalEntry::deserialize(&data[offset..]) {
                Ok((entry, consumed)) => {
                    entries.push(entry);
                    offset += consumed;
                }
                Err(_) => {
                    // Corrupted or torn entry — stop scanning
                    break;
                }
            }
        }
        Ok(entries)
    }

    /// Truncate the WAL file to a given byte offset. Used after checkpoint
    /// to reclaim space.
    pub fn truncate_before(&self, lsn: u64) -> Result<(), Error> {
        let mut inner = self.inner.lock().unwrap();

        // Flush any buffered data first
        Self::flush_inner(&mut inner)?;

        let wal_path = inner.dir.join(WAL_FILE_NAME);

        // Read all data
        let mut data = Vec::new();
        {
            let mut f = File::open(&wal_path)?;
            f.read_to_end(&mut data)?;
        }

        // Find the offset of the first entry with lsn >= truncation point
        let mut keep_offset = 0;
        let mut off = 0;
        while off < data.len() {
            if data.len() - off < WAL_ENTRY_HEADER_SIZE {
                break;
            }
            let entry_lsn = u64::from_le_bytes(data[off..off + 8].try_into().unwrap());
            if entry_lsn >= lsn {
                keep_offset = off;
                break;
            }
            match WalEntry::deserialize(&data[off..]) {
                Ok((_, consumed)) => {
                    off += consumed;
                    keep_offset = off;
                }
                Err(_) => break,
            }
        }

        // Write out remaining data
        let remaining = data[keep_offset..].to_vec();
        {
            let mut f = File::create(&wal_path)?;
            f.write_all(&remaining)?;
            f.sync_all()?;
        }

        // Reopen in append mode
        inner.file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&wal_path)?;

        let new_len = inner.file.metadata()?.len();
        // Adjust LSNs: entries keep their original LSNs as stored in the
        // records, but the file is now shorter. current_lsn and flushed_lsn
        // stay based on absolute WAL position.
        inner.flushed_lsn = inner.current_lsn;

        // If the buffer was empty (we flushed above), this is fine.
        // The current_lsn stays the same since LSNs are absolute.
        let _ = new_len; // suppress unused warning

        Ok(())
    }

    /// Get the set of active (uncommitted) transaction IDs being tracked.
    pub fn active_txn_ids(&self) -> Vec<u64> {
        let inner = self.inner.lock().unwrap();
        inner.txn_prev_lsn.keys().copied().collect()
    }

    /// Get the prev_lsn for a transaction (the last record's LSN).
    pub fn txn_last_lsn(&self, txn_id: u64) -> Option<u64> {
        let inner = self.inner.lock().unwrap();
        inner.txn_prev_lsn.get(&txn_id).copied()
    }

    /// Directory where this WAL resides.
    pub fn dir(&self) -> PathBuf {
        let inner = self.inner.lock().unwrap();
        inner.dir.clone()
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn flush_inner(inner: &mut WalManagerInner) -> Result<u64, Error> {
        if inner.buffer.is_empty() {
            return Ok(inner.flushed_lsn);
        }
        inner.file.write_all(&inner.buffer)?;
        inner.file.sync_all()?;
        inner.buffer.clear();
        inner.flushed_lsn = inner.current_lsn;
        inner.first_unflushed_time = None;
        Ok(inner.flushed_lsn)
    }

    fn read_meta(dir: &Path) -> Option<u64> {
        let path = dir.join(WAL_META_FILE_NAME);
        let mut file = File::open(path).ok()?;
        let mut buf = [0u8; 8];
        file.read_exact(&mut buf).ok()?;
        Some(u64::from_le_bytes(buf))
    }

    fn write_meta(dir: &Path, checkpoint_lsn: u64) -> Result<(), Error> {
        let path = dir.join(WAL_META_FILE_NAME);
        let mut file = File::create(path)?;
        file.write_all(&checkpoint_lsn.to_le_bytes())?;
        file.sync_all()?;
        Ok(())
    }
}
