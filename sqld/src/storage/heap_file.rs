use std::sync::Arc;

use crate::storage::buffer_pool::BufferPoolManager;
use crate::storage::page::{PageId, PageType};
use crate::types::tuple::Tuple;
use crate::utils::error::Error;

// ---------------------------------------------------------------------------
// TID (Tuple Identifier)
// ---------------------------------------------------------------------------

/// A tuple identifier consisting of (page_id, slot_index).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Tid {
    pub page_id: PageId,
    pub slot_index: u16,
}

impl Tid {
    pub fn new(page_id: PageId, slot_index: u16) -> Self {
        Self { page_id, slot_index }
    }
}

// ---------------------------------------------------------------------------
// HeapFile
// ---------------------------------------------------------------------------

/// A table stored as a sequence of heap pages.
///
/// Supports insert, delete (sets xmax), update (delete + insert), sequential
/// scan, and random-access fetch by TID. Works with an external
/// [`FreeSpaceMap`](super::free_space_map::FreeSpaceMap) to locate pages with
/// room for new tuples.
pub struct HeapFile {
    /// Buffer pool for page I/O.
    pool: Arc<BufferPoolManager>,
    /// Ordered list of page ids belonging to this heap.
    page_ids: Vec<PageId>,
}

impl HeapFile {
    /// Create a new, empty heap file.
    pub fn new(pool: Arc<BufferPoolManager>) -> Self {
        Self {
            pool,
            page_ids: Vec::new(),
        }
    }

    /// Create a heap file with pre-existing pages (e.g. recovered from catalog).
    pub fn with_pages(pool: Arc<BufferPoolManager>, page_ids: Vec<PageId>) -> Self {
        Self { pool, page_ids }
    }

    /// Page ids belonging to this heap (in allocation order).
    pub fn page_ids(&self) -> &[PageId] {
        &self.page_ids
    }

    /// Number of pages in this heap.
    pub fn num_pages(&self) -> usize {
        self.page_ids.len()
    }

    // -----------------------------------------------------------------------
    // Insert
    // -----------------------------------------------------------------------

    /// Insert a tuple into the heap. Tries `target_page` first (as suggested
    /// by the free space map). If that page is full or `None`, appends a new
    /// page. Returns the TID of the inserted tuple.
    pub fn insert(&mut self, tuple: &Tuple, target_page: Option<PageId>) -> Result<Tid, Error> {
        let data = tuple.serialize();

        // Try the suggested page first.
        if let Some(pid) = target_page {
            if self.page_ids.contains(&pid) {
                match self.try_insert_into_page(pid, &data) {
                    Ok(tid) => return Ok(tid),
                    Err(_) => {} // page full, fall through
                }
            }
        }

        // Try existing pages (linear scan — the FSM should prevent this in
        // the common case).
        for &pid in &self.page_ids {
            match self.try_insert_into_page(pid, &data) {
                Ok(tid) => return Ok(tid),
                Err(_) => continue,
            }
        }

        // All pages full — allocate a new one.
        let (page_id, page) = self.pool.new_page(PageType::HeapData)?;
        self.page_ids.push(page_id);

        let mut page = page;
        let slot = page.insert_tuple(&data)?;
        self.pool.write_page(page_id, page)?;
        self.pool.unpin_page(page_id, true)?;

        Ok(Tid::new(page_id, slot))
    }

    fn try_insert_into_page(&self, page_id: PageId, data: &[u8]) -> Result<Tid, Error> {
        let mut page = self.pool.fetch_page(page_id)?;
        match page.insert_tuple(data) {
            Ok(slot) => {
                self.pool.write_page(page_id, page)?;
                self.pool.unpin_page(page_id, true)?;
                Ok(Tid::new(page_id, slot))
            }
            Err(e) => {
                self.pool.unpin_page(page_id, false)?;
                Err(e)
            }
        }
    }

    // -----------------------------------------------------------------------
    // Delete
    // -----------------------------------------------------------------------

    /// Mark a tuple as deleted by setting its `xmax` field.
    pub fn delete(&self, tid: Tid, xmax: u64) -> Result<(), Error> {
        let mut page = self.pool.fetch_page(tid.page_id)?;
        let raw = page.fetch_tuple(tid.slot_index)?.to_vec();

        let mut tuple = Tuple::deserialize(&raw)?;
        if tuple.header.is_deleted() {
            self.pool.unpin_page(tid.page_id, false)?;
            return Err(Error::Internal(format!(
                "tuple at {:?} already deleted",
                tid
            )));
        }

        tuple.header.xmax = xmax;
        let new_data = tuple.serialize();

        // Overwrite in place: delete old slot, re-insert into same page.
        page.delete_tuple(tid.slot_index)?;
        let new_slot = page.insert_tuple(&new_data)?;
        debug_assert_eq!(new_slot, tid.slot_index, "slot reuse should give same index");

        self.pool.write_page(tid.page_id, page)?;
        self.pool.unpin_page(tid.page_id, true)?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Update (delete + insert)
    // -----------------------------------------------------------------------

    /// Update a tuple: marks the old version deleted and inserts the new
    /// version. Returns the TID of the new tuple.
    pub fn update(
        &mut self,
        old_tid: Tid,
        xmax: u64,
        new_tuple: &Tuple,
    ) -> Result<Tid, Error> {
        self.delete(old_tid, xmax)?;
        self.insert(new_tuple, Some(old_tid.page_id))
    }

    // -----------------------------------------------------------------------
    // Fetch (random access)
    // -----------------------------------------------------------------------

    /// Fetch a single tuple by TID.
    pub fn fetch(&self, tid: Tid) -> Result<Tuple, Error> {
        let page = self.pool.fetch_page(tid.page_id)?;
        let raw = page.fetch_tuple(tid.slot_index)?;
        let tuple = Tuple::deserialize(raw)?;
        self.pool.unpin_page(tid.page_id, false)?;
        Ok(tuple)
    }

    // -----------------------------------------------------------------------
    // Sequential scan
    // -----------------------------------------------------------------------

    /// Return all tuples in the heap via sequential scan.
    ///
    /// Visits every page in allocation order and returns `(Tid, Tuple)` pairs
    /// for every live slot. The caller can apply MVCC visibility filtering on
    /// the returned tuples.
    pub fn scan(&self) -> Result<Vec<(Tid, Tuple)>, Error> {
        let mut results = Vec::new();

        for &page_id in &self.page_ids {
            let page = self.pool.fetch_page(page_id)?;
            let tuple_count = page.tuple_count();

            for slot in 0..tuple_count {
                match page.fetch_tuple(slot) {
                    Ok(raw) => {
                        let tuple = Tuple::deserialize(raw)?;
                        results.push((Tid::new(page_id, slot), tuple));
                    }
                    Err(_) => {
                        // Deleted slot — skip.
                    }
                }
            }

            self.pool.unpin_page(page_id, false)?;
        }

        Ok(results)
    }

    // -----------------------------------------------------------------------
    // Vacuum
    // -----------------------------------------------------------------------

    /// Remove dead tuples (xmax != 0) from all pages and compact.
    /// Returns the number of tuples removed.
    pub fn vacuum(&mut self) -> Result<usize, Error> {
        let mut removed = 0;

        for &page_id in &self.page_ids {
            let mut page = self.pool.fetch_page(page_id)?;
            let tuple_count = page.tuple_count();
            let mut dirty = false;

            for slot in 0..tuple_count {
                if let Ok(raw) = page.fetch_tuple(slot) {
                    let tuple = Tuple::deserialize(raw)?;
                    if tuple.header.is_deleted() {
                        page.delete_tuple(slot)?;
                        removed += 1;
                        dirty = true;
                    }
                }
            }

            if dirty {
                page.compact();
                self.pool.write_page(page_id, page)?;
            }
            self.pool.unpin_page(page_id, dirty)?;
        }

        Ok(removed)
    }

    /// Get the free space on a given page (in bytes).
    pub fn page_free_space(&self, page_id: PageId) -> Result<usize, Error> {
        let page = self.pool.fetch_page(page_id)?;
        let free = page.free_space();
        self.pool.unpin_page(page_id, false)?;
        Ok(free)
    }
}
