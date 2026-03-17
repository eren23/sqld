use std::sync::Arc;

use crate::storage::btree::{BPlusTree, ScanDirection};
use crate::storage::buffer_pool::BufferPoolManager;
use crate::storage::heap_file::Tid;
use crate::storage::page::{PageId, PageType};
use crate::utils::error::Error;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Values larger than this threshold are stored out-of-line via TOAST.
pub const TOAST_THRESHOLD: usize = 2048;

/// Each TOAST chunk stores up to this many bytes of payload.
pub const TOAST_CHUNK_SIZE: usize = 2048;

/// Magic tag byte written at the start of a TOAST pointer to distinguish it
/// from inline data.
pub const TOAST_POINTER_TAG: u8 = 0xFE;

/// Serialized size of a TOAST pointer: tag(1) + toast_table_id(4) +
/// chunk_id(4) + total_length(4) = 13 bytes.
pub const TOAST_POINTER_SIZE: usize = 13;

// ---------------------------------------------------------------------------
// ToastPointer
// ---------------------------------------------------------------------------

/// A reference to out-of-line data stored in a TOAST table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ToastPointer {
    pub toast_table_id: u32,
    pub chunk_id: u32,
    pub total_length: u32,
}

impl ToastPointer {
    pub fn new(toast_table_id: u32, chunk_id: u32, total_length: u32) -> Self {
        Self {
            toast_table_id,
            chunk_id,
            total_length,
        }
    }

    /// Serialize the pointer into a byte vector (13 bytes).
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(TOAST_POINTER_SIZE);
        buf.push(TOAST_POINTER_TAG);
        buf.extend_from_slice(&self.toast_table_id.to_le_bytes());
        buf.extend_from_slice(&self.chunk_id.to_le_bytes());
        buf.extend_from_slice(&self.total_length.to_le_bytes());
        buf
    }

    /// Deserialize a pointer from bytes. Returns `None` if the tag doesn't
    /// match or the buffer is too short.
    pub fn deserialize(buf: &[u8]) -> Option<Self> {
        if buf.len() < TOAST_POINTER_SIZE {
            return None;
        }
        if buf[0] != TOAST_POINTER_TAG {
            return None;
        }
        let toast_table_id = u32::from_le_bytes(buf[1..5].try_into().unwrap());
        let chunk_id = u32::from_le_bytes(buf[5..9].try_into().unwrap());
        let total_length = u32::from_le_bytes(buf[9..13].try_into().unwrap());
        Some(Self {
            toast_table_id,
            chunk_id,
            total_length,
        })
    }

    /// Returns `true` if the given buffer starts with the TOAST pointer tag.
    pub fn is_toast_pointer(buf: &[u8]) -> bool {
        buf.first() == Some(&TOAST_POINTER_TAG)
    }
}

// ---------------------------------------------------------------------------
// ToastChunkKey
// ---------------------------------------------------------------------------

/// Key for locating a specific chunk within the TOAST table.
/// Chunks are keyed by (chunk_id, sequence_no) and serialized in big-endian
/// so that byte-lexicographic comparison in the B+ tree gives correct
/// numeric ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ToastChunkKey {
    pub chunk_id: u32,
    pub sequence_no: u32,
}

impl ToastChunkKey {
    pub fn new(chunk_id: u32, sequence_no: u32) -> Self {
        Self {
            chunk_id,
            sequence_no,
        }
    }

    /// Serialize as 8 big-endian bytes for byte-ordered B+ tree indexing.
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8);
        buf.extend_from_slice(&self.chunk_id.to_be_bytes());
        buf.extend_from_slice(&self.sequence_no.to_be_bytes());
        buf
    }

    pub fn deserialize(buf: &[u8]) -> Option<Self> {
        if buf.len() < 8 {
            return None;
        }
        let chunk_id = u32::from_be_bytes(buf[0..4].try_into().unwrap());
        let sequence_no = u32::from_be_bytes(buf[4..8].try_into().unwrap());
        Some(Self {
            chunk_id,
            sequence_no,
        })
    }
}

// ---------------------------------------------------------------------------
// ToastTable
// ---------------------------------------------------------------------------

/// A TOAST overflow table that stores large values split into fixed-size
/// chunks across overflow pages.
///
/// Chunk data is stored in slotted overflow pages, and a B+ tree index
/// keyed by `(chunk_id, sequence_no)` maps each chunk to its overflow page
/// location (page_id, slot_index). This provides O(log n) lookup instead
/// of linear scan.
pub struct ToastTable {
    pool: Arc<BufferPoolManager>,
    /// Unique identifier for this toast table.
    table_id: u32,
    /// Overflow pages belonging to this toast table (data storage).
    page_ids: Vec<PageId>,
    /// Monotonically increasing chunk id allocator.
    next_chunk_id: u32,
    /// B+ tree index: (chunk_id, sequence_no) → Tid in overflow pages.
    index: BPlusTree,
}

impl ToastTable {
    /// Create a new, empty toast table.
    pub fn new(pool: Arc<BufferPoolManager>, table_id: u32) -> Self {
        let index = BPlusTree::new(
            pool.clone(),
            true, // unique keys — each (chunk_id, seq) is unique
            Box::new(|a: &[u8], b: &[u8]| a.cmp(b)),
        );
        Self {
            pool,
            table_id,
            page_ids: Vec::new(),
            next_chunk_id: 1,
            index,
        }
    }

    pub fn table_id(&self) -> u32 {
        self.table_id
    }

    // -----------------------------------------------------------------------
    // Store
    // -----------------------------------------------------------------------

    /// Store a large value, splitting it into chunks. Returns a
    /// [`ToastPointer`] that can be embedded in the heap tuple.
    pub fn store(&mut self, data: &[u8]) -> Result<ToastPointer, Error> {
        let chunk_id = self.next_chunk_id;
        self.next_chunk_id += 1;

        let total_length = data.len() as u32;
        let mut offset = 0usize;
        let mut seq: u32 = 0;

        while offset < data.len() {
            let end = (offset + TOAST_CHUNK_SIZE).min(data.len());
            let chunk_data = &data[offset..end];

            // Store chunk payload in an overflow page.
            let tid = self.store_chunk_data(chunk_data)?;

            // Index in B+ tree: (chunk_id, seq) → Tid.
            let key = ToastChunkKey::new(chunk_id, seq);
            self.index.insert(&key.serialize(), tid)?;

            offset = end;
            seq += 1;
        }

        Ok(ToastPointer::new(self.table_id, chunk_id, total_length))
    }

    /// Store chunk payload in an overflow page. Returns the Tid where the
    /// chunk was placed.
    fn store_chunk_data(&mut self, payload: &[u8]) -> Result<Tid, Error> {
        // Try existing overflow pages.
        for &pid in &self.page_ids {
            let mut page = self.pool.fetch_page(pid)?;
            match page.insert_tuple(payload) {
                Ok(slot) => {
                    self.pool.write_page(pid, page)?;
                    self.pool.unpin_page(pid, true)?;
                    return Ok(Tid::new(pid, slot));
                }
                Err(_) => {
                    self.pool.unpin_page(pid, false)?;
                }
            }
        }

        // Allocate a new overflow page.
        let (page_id, mut page) = self.pool.new_page(PageType::Overflow)?;
        self.page_ids.push(page_id);
        let slot = page.insert_tuple(payload)?;
        self.pool.write_page(page_id, page)?;
        self.pool.unpin_page(page_id, true)?;
        Ok(Tid::new(page_id, slot))
    }

    // -----------------------------------------------------------------------
    // Retrieve
    // -----------------------------------------------------------------------

    /// Retrieve a large value given its TOAST pointer. Uses a B+ tree range
    /// scan over `(chunk_id, 0)..=(chunk_id, MAX)` to locate all chunks.
    pub fn retrieve(&self, ptr: &ToastPointer) -> Result<Vec<u8>, Error> {
        if ptr.toast_table_id != self.table_id {
            return Err(Error::Internal(format!(
                "TOAST table id mismatch: expected {}, got {}",
                self.table_id, ptr.toast_table_id
            )));
        }

        let start_key = ToastChunkKey::new(ptr.chunk_id, 0).serialize();
        let end_key = ToastChunkKey::new(ptr.chunk_id, u32::MAX).serialize();

        let iter = self.index.range_scan(
            Some((&start_key, true)),
            Some((&end_key, true)),
            ScanDirection::Forward,
        )?;

        // Collect chunks — the B+ tree yields them in key order, so they
        // are already sorted by sequence_no.
        let mut result = Vec::with_capacity(ptr.total_length as usize);

        for entry in iter {
            let (_key_bytes, tid) = entry?;

            // Fetch chunk data from the overflow page.
            let page = self.pool.fetch_page(tid.page_id)?;
            let raw = page.fetch_tuple(tid.slot_index)?.to_vec();
            self.pool.unpin_page(tid.page_id, false)?;

            result.extend_from_slice(&raw);
        }

        if result.len() != ptr.total_length as usize {
            return Err(Error::Internal(format!(
                "TOAST data length mismatch: expected {}, got {}",
                ptr.total_length,
                result.len()
            )));
        }

        Ok(result)
    }

    // -----------------------------------------------------------------------
    // Delete
    // -----------------------------------------------------------------------

    /// Delete all chunks for a given TOAST pointer.
    pub fn delete(&self, ptr: &ToastPointer) -> Result<(), Error> {
        if ptr.toast_table_id != self.table_id {
            return Err(Error::Internal(format!(
                "TOAST table id mismatch: expected {}, got {}",
                self.table_id, ptr.toast_table_id
            )));
        }

        let start_key = ToastChunkKey::new(ptr.chunk_id, 0).serialize();
        let end_key = ToastChunkKey::new(ptr.chunk_id, u32::MAX).serialize();

        // Collect all entries for this chunk_id via range scan.
        let iter = self.index.range_scan(
            Some((&start_key, true)),
            Some((&end_key, true)),
            ScanDirection::Forward,
        )?;

        let mut entries: Vec<(Vec<u8>, Tid)> = Vec::new();
        for entry in iter {
            let (key_bytes, tid) = entry?;
            entries.push((key_bytes, tid));
        }

        // Delete from B+ tree and overflow pages.
        for (key, tid) in entries {
            self.index.delete(&key)?;

            let mut page = self.pool.fetch_page(tid.page_id)?;
            let _ = page.delete_tuple(tid.slot_index);
            self.pool.write_page(tid.page_id, page)?;
            self.pool.unpin_page(tid.page_id, true)?;
        }

        Ok(())
    }

    /// Returns true if the given data exceeds the TOAST threshold.
    pub fn needs_toast(data: &[u8]) -> bool {
        data.len() > TOAST_THRESHOLD
    }
}
