use std::fmt;

use crate::utils::error::{Error, StorageError};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const PAGE_SIZE: usize = 8192;
pub const PAGE_HEADER_SIZE: usize = 24;
pub const SLOT_SIZE: usize = 4;

pub type PageId = u32;
pub const INVALID_PAGE_ID: PageId = 0;

// Header layout (24 bytes, little-endian):
//   [0..4]   page_id: u32
//   [4..6]   page_type: u16
//   [6..8]   free_space_offset: u16
//   [8..10]  tuple_count: u16
//   [10..12] flags: u16
//   [12..20] lsn: u64
//   [20..22] checksum: u16
//   [22..24] reserved: u16

// ---------------------------------------------------------------------------
// PageType
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum PageType {
    HeapData = 1,
    BtreeInternal = 2,
    BtreeLeaf = 3,
    HashBucket = 4,
    Overflow = 5,
    FreeSpaceMap = 6,
}

impl PageType {
    pub fn from_u16(v: u16) -> Option<Self> {
        match v {
            1 => Some(Self::HeapData),
            2 => Some(Self::BtreeInternal),
            3 => Some(Self::BtreeLeaf),
            4 => Some(Self::HashBucket),
            5 => Some(Self::Overflow),
            6 => Some(Self::FreeSpaceMap),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// PageHeader
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct PageHeader {
    pub page_id: PageId,
    pub page_type: u16,
    pub free_space_offset: u16,
    pub tuple_count: u16,
    pub flags: u16,
    pub lsn: u64,
    pub checksum: u16,
}

// ---------------------------------------------------------------------------
// Slot (internal)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct Slot {
    offset: u16,
    length: u16,
}

impl Slot {
    fn is_free(self) -> bool {
        self.offset == 0
    }
}

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

/// A slotted page with a 24-byte header, a slot array growing toward higher
/// offsets, and tuple data growing from the end of the page toward lower
/// offsets. Free space sits between the two regions.
///
/// ```text
/// ┌──────────┬──────────────────┬─────────────┬──────────────────┐
/// │  Header  │  Slot Array  →   │ Free Space  │  ← Tuple Data   │
/// │ 24 bytes │ 4 bytes / slot   │             │                  │
/// └──────────┴──────────────────┴─────────────┴──────────────────┘
/// 0          24                                              8192
/// ```
pub struct Page {
    data: Vec<u8>,
}

impl Clone for Page {
    fn clone(&self) -> Self {
        Page {
            data: self.data.clone(),
        }
    }
}

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let h = self.header();
        f.debug_struct("Page")
            .field("page_id", &h.page_id)
            .field("page_type", &h.page_type)
            .field("free_space_offset", &h.free_space_offset)
            .field("tuple_count", &h.tuple_count)
            .field("flags", &h.flags)
            .field("lsn", &h.lsn)
            .field("checksum", &h.checksum)
            .finish()
    }
}

impl Page {
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// Create a fresh, empty page.
    pub fn new(page_id: PageId, page_type: PageType) -> Self {
        let mut data = vec![0u8; PAGE_SIZE];

        // Write header fields
        data[0..4].copy_from_slice(&page_id.to_le_bytes());
        data[4..6].copy_from_slice(&(page_type as u16).to_le_bytes());
        data[6..8].copy_from_slice(&(PAGE_SIZE as u16).to_le_bytes()); // free_space_offset
        // tuple_count, flags, lsn, checksum, reserved all start at 0

        let mut page = Page { data };
        page.update_checksum();
        page
    }

    /// Construct a page from raw bytes (no checksum verification).
    pub fn from_bytes(data: &[u8]) -> Result<Self, Error> {
        if data.len() != PAGE_SIZE {
            return Err(Error::Internal(format!(
                "invalid page data length: expected {PAGE_SIZE}, got {}",
                data.len()
            )));
        }
        Ok(Page {
            data: data.to_vec(),
        })
    }

    /// Raw page bytes for serialization / disk I/O.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Mutable access to raw page bytes for in-place modifications (e.g. B+
    /// tree node operations). Caller must call [`recompute_checksum`] when
    /// finished modifying the data.
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// Recompute and store the CRC-16 checksum. Call this after modifying the
    /// page through [`as_bytes_mut`].
    pub fn recompute_checksum(&mut self) {
        self.update_checksum();
    }

    // -----------------------------------------------------------------------
    // Header accessors
    // -----------------------------------------------------------------------

    pub fn header(&self) -> PageHeader {
        PageHeader {
            page_id: self.page_id(),
            page_type: self.page_type_raw(),
            free_space_offset: self.free_space_offset(),
            tuple_count: self.tuple_count(),
            flags: self.flags(),
            lsn: self.lsn(),
            checksum: self.checksum(),
        }
    }

    pub fn page_id(&self) -> PageId {
        u32::from_le_bytes(self.data[0..4].try_into().unwrap())
    }

    pub fn page_type_raw(&self) -> u16 {
        u16::from_le_bytes(self.data[4..6].try_into().unwrap())
    }

    pub fn page_type_enum(&self) -> Option<PageType> {
        PageType::from_u16(self.page_type_raw())
    }

    pub fn free_space_offset(&self) -> u16 {
        u16::from_le_bytes(self.data[6..8].try_into().unwrap())
    }

    pub fn tuple_count(&self) -> u16 {
        u16::from_le_bytes(self.data[8..10].try_into().unwrap())
    }

    pub fn flags(&self) -> u16 {
        u16::from_le_bytes(self.data[10..12].try_into().unwrap())
    }

    pub fn set_flags(&mut self, v: u16) {
        self.data[10..12].copy_from_slice(&v.to_le_bytes());
        self.update_checksum();
    }

    pub fn lsn(&self) -> u64 {
        u64::from_le_bytes(self.data[12..20].try_into().unwrap())
    }

    pub fn set_lsn(&mut self, v: u64) {
        self.data[12..20].copy_from_slice(&v.to_le_bytes());
        self.update_checksum();
    }

    pub fn checksum(&self) -> u16 {
        u16::from_le_bytes(self.data[20..22].try_into().unwrap())
    }

    // -----------------------------------------------------------------------
    // Free space
    // -----------------------------------------------------------------------

    /// Usable free space in the page (bytes available for one tuple + its slot
    /// when a new slot must be appended, or just data when reusing a free slot).
    pub fn free_space(&self) -> usize {
        let slot_array_end = PAGE_HEADER_SIZE + (self.tuple_count() as usize) * SLOT_SIZE;
        let fso = self.free_space_offset() as usize;
        fso.saturating_sub(slot_array_end)
    }

    // -----------------------------------------------------------------------
    // Tuple operations
    // -----------------------------------------------------------------------

    /// Insert `tuple_data` into the page. Returns the slot index on success.
    pub fn insert_tuple(&mut self, tuple_data: &[u8]) -> Result<u16, Error> {
        let data_len = tuple_data.len();
        if data_len == 0 {
            return Err(Error::Internal("cannot insert zero-length tuple".into()));
        }
        if data_len > PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_SIZE {
            return Err(StorageError::PageFull.into());
        }

        // Look for a free (deleted) slot we can reuse.
        let tuple_count = self.tuple_count();
        let mut reuse_slot: Option<u16> = None;
        for i in 0..tuple_count {
            if self.read_slot(i).is_free() {
                reuse_slot = Some(i);
                break;
            }
        }

        let space_needed = if reuse_slot.is_some() {
            data_len
        } else {
            data_len + SLOT_SIZE
        };

        if self.free_space() < space_needed {
            return Err(StorageError::PageFull.into());
        }

        // Write tuple data at the bottom of the free region.
        let fso = self.free_space_offset() as usize;
        let new_offset = fso - data_len;
        self.data[new_offset..fso].copy_from_slice(tuple_data);
        self.set_free_space_offset_raw(new_offset as u16);

        // Write the slot entry.
        let slot_idx = reuse_slot.unwrap_or(tuple_count);
        self.write_slot(
            slot_idx,
            Slot {
                offset: new_offset as u16,
                length: data_len as u16,
            },
        );

        if reuse_slot.is_none() {
            self.set_tuple_count_raw(tuple_count + 1);
        }

        self.update_checksum();
        Ok(slot_idx)
    }

    /// Mark the tuple at `slot_index` as deleted.
    pub fn delete_tuple(&mut self, slot_index: u16) -> Result<(), Error> {
        if slot_index >= self.tuple_count() {
            return Err(Error::Internal(format!(
                "slot index {slot_index} out of range (tuple_count={})",
                self.tuple_count()
            )));
        }
        let slot = self.read_slot(slot_index);
        if slot.is_free() {
            return Err(Error::Internal(format!(
                "slot {slot_index} is already deleted"
            )));
        }

        // Zero out the tuple data region.
        let start = slot.offset as usize;
        let end = start + slot.length as usize;
        self.data[start..end].fill(0);

        // Mark slot as free.
        self.write_slot(slot_index, Slot { offset: 0, length: 0 });

        self.update_checksum();
        Ok(())
    }

    /// Fetch the raw tuple data at `slot_index`.
    pub fn fetch_tuple(&self, slot_index: u16) -> Result<&[u8], Error> {
        if slot_index >= self.tuple_count() {
            return Err(Error::Internal(format!(
                "slot index {slot_index} out of range (tuple_count={})",
                self.tuple_count()
            )));
        }
        let slot = self.read_slot(slot_index);
        if slot.is_free() {
            return Err(Error::Internal(format!(
                "slot {slot_index} is deleted"
            )));
        }
        let start = slot.offset as usize;
        let end = start + slot.length as usize;
        Ok(&self.data[start..end])
    }

    /// Replace the raw data at `slot_index` in-place. The new data must be
    /// exactly the same length as the existing tuple.
    pub fn update_tuple(&mut self, slot_index: u16, new_data: &[u8]) -> Result<(), Error> {
        if slot_index >= self.tuple_count() {
            return Err(Error::Internal(format!(
                "slot index {slot_index} out of range (tuple_count={})",
                self.tuple_count()
            )));
        }
        let slot = self.read_slot(slot_index);
        if slot.is_free() {
            return Err(Error::Internal(format!(
                "slot {slot_index} is deleted"
            )));
        }
        if new_data.len() != slot.length as usize {
            return Err(Error::Internal(format!(
                "update_tuple: length mismatch (expected {}, got {})",
                slot.length,
                new_data.len()
            )));
        }
        let start = slot.offset as usize;
        self.data[start..start + new_data.len()].copy_from_slice(new_data);
        self.update_checksum();
        Ok(())
    }

    /// Compact the page by moving all live tuples to be contiguous at the end
    /// of the page, reclaiming fragmented free space.
    pub fn compact(&mut self) {
        let tuple_count = self.tuple_count();
        if tuple_count == 0 {
            return;
        }

        // Collect live tuples (slot_index, data).
        let mut live: Vec<(u16, Vec<u8>)> = Vec::new();
        for i in 0..tuple_count {
            let slot = self.read_slot(i);
            if !slot.is_free() {
                let start = slot.offset as usize;
                let end = start + slot.length as usize;
                live.push((i, self.data[start..end].to_vec()));
            }
        }

        // Clear the entire data region (after the slot array).
        let slot_array_end = PAGE_HEADER_SIZE + (tuple_count as usize) * SLOT_SIZE;
        self.data[slot_array_end..PAGE_SIZE].fill(0);

        // Re-pack tuples from the end of the page.
        let mut write_pos = PAGE_SIZE;
        for (slot_idx, tuple_data) in &live {
            write_pos -= tuple_data.len();
            self.data[write_pos..write_pos + tuple_data.len()].copy_from_slice(tuple_data);
            self.write_slot(
                *slot_idx,
                Slot {
                    offset: write_pos as u16,
                    length: tuple_data.len() as u16,
                },
            );
        }

        self.set_free_space_offset_raw(write_pos as u16);
        self.update_checksum();
    }

    // -----------------------------------------------------------------------
    // Checksum
    // -----------------------------------------------------------------------

    /// Compute the CRC-16/CCITT checksum over the page, treating the checksum
    /// field itself as zero.
    fn compute_checksum(&self) -> u16 {
        let mut crc: u16 = 0xFFFF;
        // Hash bytes [0..20] (before checksum field).
        for &b in &self.data[0..20] {
            crc = crc16_step(crc, b);
        }
        // Skip the checksum field [20..22] by feeding zeros.
        crc = crc16_step(crc, 0);
        crc = crc16_step(crc, 0);
        // Hash bytes [22..PAGE_SIZE].
        for &b in &self.data[22..PAGE_SIZE] {
            crc = crc16_step(crc, b);
        }
        crc
    }

    fn update_checksum(&mut self) {
        let crc = self.compute_checksum();
        self.data[20..22].copy_from_slice(&crc.to_le_bytes());
    }

    /// Verify the page's CRC-16 checksum.
    pub fn verify_checksum(&self) -> bool {
        self.checksum() == self.compute_checksum()
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn set_free_space_offset_raw(&mut self, v: u16) {
        self.data[6..8].copy_from_slice(&v.to_le_bytes());
    }

    fn set_tuple_count_raw(&mut self, v: u16) {
        self.data[8..10].copy_from_slice(&v.to_le_bytes());
    }

    fn slot_byte_offset(index: u16) -> usize {
        PAGE_HEADER_SIZE + (index as usize) * SLOT_SIZE
    }

    fn read_slot(&self, index: u16) -> Slot {
        let base = Self::slot_byte_offset(index);
        Slot {
            offset: u16::from_le_bytes(self.data[base..base + 2].try_into().unwrap()),
            length: u16::from_le_bytes(self.data[base + 2..base + 4].try_into().unwrap()),
        }
    }

    fn write_slot(&mut self, index: u16, slot: Slot) {
        let base = Self::slot_byte_offset(index);
        self.data[base..base + 2].copy_from_slice(&slot.offset.to_le_bytes());
        self.data[base + 2..base + 4].copy_from_slice(&slot.length.to_le_bytes());
    }
}

// ---------------------------------------------------------------------------
// CRC-16/CCITT helper
// ---------------------------------------------------------------------------

#[inline]
fn crc16_step(crc: u16, byte: u8) -> u16 {
    let mut c = crc ^ ((byte as u16) << 8);
    for _ in 0..8 {
        if c & 0x8000 != 0 {
            c = (c << 1) ^ 0x1021;
        } else {
            c <<= 1;
        }
    }
    c
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_page_header() {
        let page = Page::new(42, PageType::HeapData);
        let h = page.header();
        assert_eq!(h.page_id, 42);
        assert_eq!(h.page_type, PageType::HeapData as u16);
        assert_eq!(h.free_space_offset, PAGE_SIZE as u16);
        assert_eq!(h.tuple_count, 0);
        assert_eq!(h.flags, 0);
        assert_eq!(h.lsn, 0);
        assert!(page.verify_checksum());
    }

    #[test]
    fn page_type_round_trip() {
        for pt in [
            PageType::HeapData,
            PageType::BtreeInternal,
            PageType::BtreeLeaf,
            PageType::HashBucket,
            PageType::Overflow,
            PageType::FreeSpaceMap,
        ] {
            let page = Page::new(1, pt);
            assert_eq!(page.page_type_enum(), Some(pt));
        }
    }

    #[test]
    fn insert_and_fetch() {
        let mut page = Page::new(1, PageType::HeapData);
        let data = b"hello world";
        let slot = page.insert_tuple(data).unwrap();
        assert_eq!(slot, 0);
        assert_eq!(page.fetch_tuple(0).unwrap(), data);
        assert!(page.verify_checksum());
    }

    #[test]
    fn free_space_decreases() {
        let mut page = Page::new(1, PageType::HeapData);
        let initial = page.free_space();
        page.insert_tuple(&[0xAB; 100]).unwrap();
        // Used 100 bytes of data + 4 bytes for the new slot.
        assert_eq!(page.free_space(), initial - 100 - SLOT_SIZE);
    }

    #[test]
    fn delete_and_reuse() {
        let mut page = Page::new(1, PageType::HeapData);
        let s0 = page.insert_tuple(b"aaa").unwrap();
        let s1 = page.insert_tuple(b"bbb").unwrap();
        let _s2 = page.insert_tuple(b"ccc").unwrap();

        page.delete_tuple(s1).unwrap();
        assert!(page.fetch_tuple(s1).is_err());

        // Next insert should reuse slot 1.
        let s_new = page.insert_tuple(b"ddd").unwrap();
        assert_eq!(s_new, s1);
        assert_eq!(page.fetch_tuple(s_new).unwrap(), b"ddd");

        // Slot 0 and 2 unchanged.
        assert_eq!(page.fetch_tuple(s0).unwrap(), b"aaa");
        assert_eq!(page.fetch_tuple(2).unwrap(), b"ccc");
    }

    #[test]
    fn compact_reclaims_space() {
        let mut page = Page::new(1, PageType::HeapData);
        page.insert_tuple(&[1; 200]).unwrap();
        page.insert_tuple(&[2; 200]).unwrap();
        page.insert_tuple(&[3; 200]).unwrap();

        let before_delete = page.free_space();
        page.delete_tuple(0).unwrap();
        page.delete_tuple(2).unwrap();

        // Free space didn't grow (fragmented).
        assert_eq!(page.free_space(), before_delete);

        page.compact();

        // After compaction, we reclaimed 400 bytes of tuple data.
        assert_eq!(page.free_space(), before_delete + 400);

        // Remaining tuple still accessible.
        assert_eq!(page.fetch_tuple(1).unwrap(), &[2; 200]);
        assert!(page.verify_checksum());
    }

    #[test]
    fn page_full() {
        let mut page = Page::new(1, PageType::HeapData);
        // Fill the page with large tuples until it's full.
        let big = [0xCC; 2000];
        let mut count = 0;
        loop {
            match page.insert_tuple(&big) {
                Ok(_) => count += 1,
                Err(_) => break,
            }
        }
        assert!(count >= 3); // 3 * 2000 + 3 * 4 = 6012, fits in ~8168 usable
        assert!(page.verify_checksum());
    }

    #[test]
    fn zero_length_tuple_rejected() {
        let mut page = Page::new(1, PageType::HeapData);
        assert!(page.insert_tuple(&[]).is_err());
    }

    #[test]
    fn checksum_detects_corruption() {
        let mut page = Page::new(1, PageType::HeapData);
        page.insert_tuple(b"important data").unwrap();
        assert!(page.verify_checksum());

        // Corrupt a byte in the tuple data area.
        let last = page.data.len() - 1;
        page.data[last] ^= 0xFF;
        assert!(!page.verify_checksum());
    }

    #[test]
    fn from_bytes_round_trip() {
        let mut page = Page::new(7, PageType::BtreeLeaf);
        page.insert_tuple(b"leaf node data").unwrap();
        page.set_lsn(12345);

        let bytes = page.as_bytes().to_vec();
        let restored = Page::from_bytes(&bytes).unwrap();
        assert_eq!(restored.page_id(), 7);
        assert_eq!(restored.page_type_enum(), Some(PageType::BtreeLeaf));
        assert_eq!(restored.lsn(), 12345);
        assert_eq!(restored.fetch_tuple(0).unwrap(), b"leaf node data");
        assert!(restored.verify_checksum());
    }

    #[test]
    fn set_flags_and_lsn() {
        let mut page = Page::new(1, PageType::HeapData);
        page.set_flags(0xBEEF);
        page.set_lsn(0xDEAD_CAFE_0000);
        assert_eq!(page.flags(), 0xBEEF);
        assert_eq!(page.lsn(), 0xDEAD_CAFE_0000);
        assert!(page.verify_checksum());
    }
}
