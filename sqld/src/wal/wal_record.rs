use crate::storage::page::PageId;

// ---------------------------------------------------------------------------
// WAL Record Types
// ---------------------------------------------------------------------------

/// A single WAL log record.
///
/// Physiological records (InsertTuple, DeleteTuple, UpdateTuple) carry full
/// undo data so that recovery can reverse uncommitted changes without
/// consulting heap pages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalRecord {
    // -- Transaction lifecycle --
    Begin { txn_id: u64 },
    Commit { txn_id: u64 },
    Abort { txn_id: u64 },

    // -- Physiological tuple operations --
    InsertTuple {
        txn_id: u64,
        page_id: PageId,
        slot_index: u16,
        data: Vec<u8>,
    },
    DeleteTuple {
        txn_id: u64,
        page_id: PageId,
        slot_index: u16,
        data: Vec<u8>, // full tuple for undo (re-insert)
    },
    UpdateTuple {
        txn_id: u64,
        page_id: PageId,
        slot_index: u16,
        old_data: Vec<u8>,
        new_data: Vec<u8>,
    },

    // -- Index operations --
    IndexInsert {
        txn_id: u64,
        index_page_id: PageId,
        key: Vec<u8>,
        tid_page: PageId,
        tid_slot: u16,
    },
    IndexDelete {
        txn_id: u64,
        index_page_id: PageId,
        key: Vec<u8>,
        tid_page: PageId,
        tid_slot: u16,
    },

    // -- Page-level operations --
    PageAlloc { txn_id: u64, page_id: PageId },
    PageFree { txn_id: u64, page_id: PageId },

    // -- Checkpoint --
    CheckpointBegin { active_txns: Vec<u64> },
    CheckpointEnd { checkpoint_begin_lsn: u64 },

    // -- Compensation log record (CLR) --
    /// Written during undo. `undo_next_lsn` points to the *next* record to
    /// undo (skipping the record that was just compensated). CLRs are
    /// redo-only: they are never themselves undone.
    Clr {
        txn_id: u64,
        undo_next_lsn: u64,
        redo: Box<WalRecord>,
    },
}

// Tag bytes for serialization
const TAG_BEGIN: u8 = 0;
const TAG_COMMIT: u8 = 1;
const TAG_ABORT: u8 = 2;
const TAG_INSERT_TUPLE: u8 = 3;
const TAG_DELETE_TUPLE: u8 = 4;
const TAG_UPDATE_TUPLE: u8 = 5;
const TAG_INDEX_INSERT: u8 = 6;
const TAG_INDEX_DELETE: u8 = 7;
const TAG_PAGE_ALLOC: u8 = 8;
const TAG_PAGE_FREE: u8 = 9;
const TAG_CHECKPOINT_BEGIN: u8 = 10;
const TAG_CHECKPOINT_END: u8 = 11;
const TAG_CLR: u8 = 12;

impl WalRecord {
    /// Return the transaction id associated with this record, if any.
    pub fn txn_id(&self) -> Option<u64> {
        match self {
            WalRecord::Begin { txn_id }
            | WalRecord::Commit { txn_id }
            | WalRecord::Abort { txn_id }
            | WalRecord::InsertTuple { txn_id, .. }
            | WalRecord::DeleteTuple { txn_id, .. }
            | WalRecord::UpdateTuple { txn_id, .. }
            | WalRecord::IndexInsert { txn_id, .. }
            | WalRecord::IndexDelete { txn_id, .. }
            | WalRecord::PageAlloc { txn_id, .. }
            | WalRecord::PageFree { txn_id, .. }
            | WalRecord::Clr { txn_id, .. } => Some(*txn_id),
            WalRecord::CheckpointBegin { .. } | WalRecord::CheckpointEnd { .. } => None,
        }
    }

    /// The page affected by this record, if it is a physiological record.
    pub fn affected_page(&self) -> Option<PageId> {
        match self {
            WalRecord::InsertTuple { page_id, .. }
            | WalRecord::DeleteTuple { page_id, .. }
            | WalRecord::UpdateTuple { page_id, .. } => Some(*page_id),
            WalRecord::IndexInsert {
                index_page_id, ..
            }
            | WalRecord::IndexDelete {
                index_page_id, ..
            } => Some(*index_page_id),
            WalRecord::PageAlloc { page_id, .. }
            | WalRecord::PageFree { page_id, .. } => Some(*page_id),
            WalRecord::Clr { redo, .. } => redo.affected_page(),
            _ => None,
        }
    }

    /// Serialize the record to bytes.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.serialize_into(&mut buf);
        buf
    }

    fn serialize_into(&self, buf: &mut Vec<u8>) {
        match self {
            WalRecord::Begin { txn_id } => {
                buf.push(TAG_BEGIN);
                buf.extend_from_slice(&txn_id.to_le_bytes());
            }
            WalRecord::Commit { txn_id } => {
                buf.push(TAG_COMMIT);
                buf.extend_from_slice(&txn_id.to_le_bytes());
            }
            WalRecord::Abort { txn_id } => {
                buf.push(TAG_ABORT);
                buf.extend_from_slice(&txn_id.to_le_bytes());
            }
            WalRecord::InsertTuple {
                txn_id,
                page_id,
                slot_index,
                data,
            } => {
                buf.push(TAG_INSERT_TUPLE);
                buf.extend_from_slice(&txn_id.to_le_bytes());
                buf.extend_from_slice(&page_id.to_le_bytes());
                buf.extend_from_slice(&slot_index.to_le_bytes());
                buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
                buf.extend_from_slice(data);
            }
            WalRecord::DeleteTuple {
                txn_id,
                page_id,
                slot_index,
                data,
            } => {
                buf.push(TAG_DELETE_TUPLE);
                buf.extend_from_slice(&txn_id.to_le_bytes());
                buf.extend_from_slice(&page_id.to_le_bytes());
                buf.extend_from_slice(&slot_index.to_le_bytes());
                buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
                buf.extend_from_slice(data);
            }
            WalRecord::UpdateTuple {
                txn_id,
                page_id,
                slot_index,
                old_data,
                new_data,
            } => {
                buf.push(TAG_UPDATE_TUPLE);
                buf.extend_from_slice(&txn_id.to_le_bytes());
                buf.extend_from_slice(&page_id.to_le_bytes());
                buf.extend_from_slice(&slot_index.to_le_bytes());
                buf.extend_from_slice(&(old_data.len() as u32).to_le_bytes());
                buf.extend_from_slice(old_data);
                buf.extend_from_slice(&(new_data.len() as u32).to_le_bytes());
                buf.extend_from_slice(new_data);
            }
            WalRecord::IndexInsert {
                txn_id,
                index_page_id,
                key,
                tid_page,
                tid_slot,
            } => {
                buf.push(TAG_INDEX_INSERT);
                buf.extend_from_slice(&txn_id.to_le_bytes());
                buf.extend_from_slice(&index_page_id.to_le_bytes());
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
                buf.extend_from_slice(&tid_page.to_le_bytes());
                buf.extend_from_slice(&tid_slot.to_le_bytes());
            }
            WalRecord::IndexDelete {
                txn_id,
                index_page_id,
                key,
                tid_page,
                tid_slot,
            } => {
                buf.push(TAG_INDEX_DELETE);
                buf.extend_from_slice(&txn_id.to_le_bytes());
                buf.extend_from_slice(&index_page_id.to_le_bytes());
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
                buf.extend_from_slice(&tid_page.to_le_bytes());
                buf.extend_from_slice(&tid_slot.to_le_bytes());
            }
            WalRecord::PageAlloc { txn_id, page_id } => {
                buf.push(TAG_PAGE_ALLOC);
                buf.extend_from_slice(&txn_id.to_le_bytes());
                buf.extend_from_slice(&page_id.to_le_bytes());
            }
            WalRecord::PageFree { txn_id, page_id } => {
                buf.push(TAG_PAGE_FREE);
                buf.extend_from_slice(&txn_id.to_le_bytes());
                buf.extend_from_slice(&page_id.to_le_bytes());
            }
            WalRecord::CheckpointBegin { active_txns } => {
                buf.push(TAG_CHECKPOINT_BEGIN);
                buf.extend_from_slice(&(active_txns.len() as u32).to_le_bytes());
                for txn in active_txns {
                    buf.extend_from_slice(&txn.to_le_bytes());
                }
            }
            WalRecord::CheckpointEnd {
                checkpoint_begin_lsn,
            } => {
                buf.push(TAG_CHECKPOINT_END);
                buf.extend_from_slice(&checkpoint_begin_lsn.to_le_bytes());
            }
            WalRecord::Clr {
                txn_id,
                undo_next_lsn,
                redo,
            } => {
                buf.push(TAG_CLR);
                buf.extend_from_slice(&txn_id.to_le_bytes());
                buf.extend_from_slice(&undo_next_lsn.to_le_bytes());
                // Serialize the inner redo record with a length prefix
                let redo_bytes = redo.serialize();
                buf.extend_from_slice(&(redo_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(&redo_bytes);
            }
        }
    }

    /// Deserialize a record from bytes. Returns the record and number of bytes
    /// consumed.
    pub fn deserialize(data: &[u8]) -> Result<(Self, usize), String> {
        if data.is_empty() {
            return Err("empty record data".into());
        }
        let tag = data[0];
        let rest = &data[1..];
        match tag {
            TAG_BEGIN => {
                let txn_id = read_u64(rest, 0)?;
                Ok((WalRecord::Begin { txn_id }, 1 + 8))
            }
            TAG_COMMIT => {
                let txn_id = read_u64(rest, 0)?;
                Ok((WalRecord::Commit { txn_id }, 1 + 8))
            }
            TAG_ABORT => {
                let txn_id = read_u64(rest, 0)?;
                Ok((WalRecord::Abort { txn_id }, 1 + 8))
            }
            TAG_INSERT_TUPLE => {
                let txn_id = read_u64(rest, 0)?;
                let page_id = read_u32(rest, 8)?;
                let slot_index = read_u16(rest, 12)?;
                let data_len = read_u32(rest, 14)? as usize;
                let data = read_bytes(rest, 18, data_len)?;
                Ok((
                    WalRecord::InsertTuple {
                        txn_id,
                        page_id,
                        slot_index,
                        data,
                    },
                    1 + 18 + data_len,
                ))
            }
            TAG_DELETE_TUPLE => {
                let txn_id = read_u64(rest, 0)?;
                let page_id = read_u32(rest, 8)?;
                let slot_index = read_u16(rest, 12)?;
                let data_len = read_u32(rest, 14)? as usize;
                let data = read_bytes(rest, 18, data_len)?;
                Ok((
                    WalRecord::DeleteTuple {
                        txn_id,
                        page_id,
                        slot_index,
                        data,
                    },
                    1 + 18 + data_len,
                ))
            }
            TAG_UPDATE_TUPLE => {
                let txn_id = read_u64(rest, 0)?;
                let page_id = read_u32(rest, 8)?;
                let slot_index = read_u16(rest, 12)?;
                let old_len = read_u32(rest, 14)? as usize;
                let old_data = read_bytes(rest, 18, old_len)?;
                let new_off = 18 + old_len;
                let new_len = read_u32(rest, new_off)? as usize;
                let new_data = read_bytes(rest, new_off + 4, new_len)?;
                Ok((
                    WalRecord::UpdateTuple {
                        txn_id,
                        page_id,
                        slot_index,
                        old_data,
                        new_data,
                    },
                    1 + new_off + 4 + new_len,
                ))
            }
            TAG_INDEX_INSERT => {
                let txn_id = read_u64(rest, 0)?;
                let index_page_id = read_u32(rest, 8)?;
                let key_len = read_u32(rest, 12)? as usize;
                let key = read_bytes(rest, 16, key_len)?;
                let off = 16 + key_len;
                let tid_page = read_u32(rest, off)?;
                let tid_slot = read_u16(rest, off + 4)?;
                Ok((
                    WalRecord::IndexInsert {
                        txn_id,
                        index_page_id,
                        key,
                        tid_page,
                        tid_slot,
                    },
                    1 + off + 6,
                ))
            }
            TAG_INDEX_DELETE => {
                let txn_id = read_u64(rest, 0)?;
                let index_page_id = read_u32(rest, 8)?;
                let key_len = read_u32(rest, 12)? as usize;
                let key = read_bytes(rest, 16, key_len)?;
                let off = 16 + key_len;
                let tid_page = read_u32(rest, off)?;
                let tid_slot = read_u16(rest, off + 4)?;
                Ok((
                    WalRecord::IndexDelete {
                        txn_id,
                        index_page_id,
                        key,
                        tid_page,
                        tid_slot,
                    },
                    1 + off + 6,
                ))
            }
            TAG_PAGE_ALLOC => {
                let txn_id = read_u64(rest, 0)?;
                let page_id = read_u32(rest, 8)?;
                Ok((WalRecord::PageAlloc { txn_id, page_id }, 1 + 12))
            }
            TAG_PAGE_FREE => {
                let txn_id = read_u64(rest, 0)?;
                let page_id = read_u32(rest, 8)?;
                Ok((WalRecord::PageFree { txn_id, page_id }, 1 + 12))
            }
            TAG_CHECKPOINT_BEGIN => {
                let count = read_u32(rest, 0)? as usize;
                let mut active_txns = Vec::with_capacity(count);
                for i in 0..count {
                    active_txns.push(read_u64(rest, 4 + i * 8)?);
                }
                Ok((
                    WalRecord::CheckpointBegin { active_txns },
                    1 + 4 + count * 8,
                ))
            }
            TAG_CHECKPOINT_END => {
                let checkpoint_begin_lsn = read_u64(rest, 0)?;
                Ok((
                    WalRecord::CheckpointEnd {
                        checkpoint_begin_lsn,
                    },
                    1 + 8,
                ))
            }
            TAG_CLR => {
                let txn_id = read_u64(rest, 0)?;
                let undo_next_lsn = read_u64(rest, 8)?;
                let redo_len = read_u32(rest, 16)? as usize;
                let redo_bytes = read_bytes(rest, 20, redo_len)?;
                let (redo, _) = WalRecord::deserialize(&redo_bytes)?;
                Ok((
                    WalRecord::Clr {
                        txn_id,
                        undo_next_lsn,
                        redo: Box::new(redo),
                    },
                    1 + 20 + redo_len,
                ))
            }
            _ => Err(format!("unknown record tag: {tag}")),
        }
    }
}

// ---------------------------------------------------------------------------
// WAL Entry (record + envelope)
// ---------------------------------------------------------------------------

/// WAL entry header size: lsn(8) + prev_lsn(8) + record_len(4) = 20 bytes.
/// Followed by record bytes and then crc32(4).
pub const WAL_ENTRY_HEADER_SIZE: usize = 20;
pub const WAL_ENTRY_CRC_SIZE: usize = 4;

/// A complete WAL entry: envelope (LSN, prev_lsn, CRC) + record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalEntry {
    /// Log sequence number — byte offset in the WAL file.
    pub lsn: u64,
    /// Previous LSN for this transaction (undo chain). 0 if this is the
    /// first record for the transaction.
    pub prev_lsn: u64,
    /// The log record payload.
    pub record: WalRecord,
    /// CRC-32 over lsn + prev_lsn + record_len + record_bytes.
    pub crc32: u32,
}

impl WalEntry {
    /// Create a new entry, computing its CRC.
    pub fn new(lsn: u64, prev_lsn: u64, record: WalRecord) -> Self {
        let record_bytes = record.serialize();
        let crc = Self::compute_crc(lsn, prev_lsn, &record_bytes);
        WalEntry {
            lsn,
            prev_lsn,
            record,
            crc32: crc,
        }
    }

    /// Verify the CRC of this entry.
    pub fn verify_crc(&self) -> bool {
        let record_bytes = self.record.serialize();
        let expected = Self::compute_crc(self.lsn, self.prev_lsn, &record_bytes);
        self.crc32 == expected
    }

    /// Total size of this entry on disk.
    pub fn disk_size(&self) -> usize {
        WAL_ENTRY_HEADER_SIZE + self.record.serialize().len() + WAL_ENTRY_CRC_SIZE
    }

    /// Serialize the full entry to bytes (header + record + crc).
    pub fn serialize(&self) -> Vec<u8> {
        let record_bytes = self.record.serialize();
        let mut buf = Vec::with_capacity(WAL_ENTRY_HEADER_SIZE + record_bytes.len() + WAL_ENTRY_CRC_SIZE);
        buf.extend_from_slice(&self.lsn.to_le_bytes());
        buf.extend_from_slice(&self.prev_lsn.to_le_bytes());
        buf.extend_from_slice(&(record_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&record_bytes);
        buf.extend_from_slice(&self.crc32.to_le_bytes());
        buf
    }

    /// Deserialize one entry from a byte slice. Returns the entry and bytes
    /// consumed, or an error if the data is truncated or corrupted.
    pub fn deserialize(data: &[u8]) -> Result<(Self, usize), String> {
        if data.len() < WAL_ENTRY_HEADER_SIZE {
            return Err("truncated WAL entry header".into());
        }
        let lsn = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let prev_lsn = u64::from_le_bytes(data[8..16].try_into().unwrap());
        let record_len = u32::from_le_bytes(data[16..20].try_into().unwrap()) as usize;

        let total = WAL_ENTRY_HEADER_SIZE + record_len + WAL_ENTRY_CRC_SIZE;
        if data.len() < total {
            return Err("truncated WAL entry body".into());
        }

        let record_bytes = &data[WAL_ENTRY_HEADER_SIZE..WAL_ENTRY_HEADER_SIZE + record_len];
        let crc_offset = WAL_ENTRY_HEADER_SIZE + record_len;
        let crc32 = u32::from_le_bytes(data[crc_offset..crc_offset + 4].try_into().unwrap());

        let (record, consumed) = WalRecord::deserialize(record_bytes)?;
        if consumed != record_len {
            return Err(format!(
                "record deserialization consumed {consumed} bytes, expected {record_len}"
            ));
        }

        let expected_crc = Self::compute_crc(lsn, prev_lsn, record_bytes);
        if crc32 != expected_crc {
            return Err(format!(
                "CRC mismatch: stored={crc32:#010x}, computed={expected_crc:#010x}"
            ));
        }

        Ok((
            WalEntry {
                lsn,
                prev_lsn,
                record,
                crc32,
            },
            total,
        ))
    }

    fn compute_crc(lsn: u64, prev_lsn: u64, record_bytes: &[u8]) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&lsn.to_le_bytes());
        hasher.update(&prev_lsn.to_le_bytes());
        hasher.update(&(record_bytes.len() as u32).to_le_bytes());
        hasher.update(record_bytes);
        hasher.finalize()
    }
}

// ---------------------------------------------------------------------------
// Binary read helpers
// ---------------------------------------------------------------------------

fn read_u64(data: &[u8], off: usize) -> Result<u64, String> {
    data.get(off..off + 8)
        .and_then(|s| s.try_into().ok())
        .map(u64::from_le_bytes)
        .ok_or_else(|| format!("truncated at offset {off} reading u64"))
}

fn read_u32(data: &[u8], off: usize) -> Result<u32, String> {
    data.get(off..off + 4)
        .and_then(|s| s.try_into().ok())
        .map(u32::from_le_bytes)
        .ok_or_else(|| format!("truncated at offset {off} reading u32"))
}

fn read_u16(data: &[u8], off: usize) -> Result<u16, String> {
    data.get(off..off + 2)
        .and_then(|s| s.try_into().ok())
        .map(u16::from_le_bytes)
        .ok_or_else(|| format!("truncated at offset {off} reading u16"))
}

fn read_bytes(data: &[u8], off: usize, len: usize) -> Result<Vec<u8>, String> {
    data.get(off..off + len)
        .map(|s| s.to_vec())
        .ok_or_else(|| format!("truncated at offset {off} reading {len} bytes"))
}
