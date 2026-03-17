//! Core B+ tree index backed by the buffer pool.

use std::sync::{Arc, Mutex};

use crate::storage::buffer_pool::BufferPoolManager;
use crate::storage::heap_file::Tid;
use crate::storage::page::{PageId, PageType, INVALID_PAGE_ID};
use crate::utils::error::{Error, StorageError};

use super::iterator::{BTreeIterator, ScanDirection};
use super::node;
use super::CompareFn;

// ---------------------------------------------------------------------------
// BPlusTree
// ---------------------------------------------------------------------------

pub struct BPlusTree {
    root_page_id: Mutex<PageId>,
    buffer_pool: Arc<BufferPoolManager>,
    compare: Arc<dyn Fn(&[u8], &[u8]) -> std::cmp::Ordering + Send + Sync>,
    is_unique: bool,
}

impl BPlusTree {
    /// Create a new (empty) B+ tree.
    pub fn new(
        buffer_pool: Arc<BufferPoolManager>,
        is_unique: bool,
        compare: Box<CompareFn>,
    ) -> Self {
        Self {
            root_page_id: Mutex::new(INVALID_PAGE_ID),
            buffer_pool,
            compare: Arc::from(compare),
            is_unique,
        }
    }

    pub fn root_page_id(&self) -> PageId {
        *self.root_page_id.lock().unwrap()
    }

    pub fn buffer_pool(&self) -> &Arc<BufferPoolManager> {
        &self.buffer_pool
    }

    pub fn comparator(&self) -> &Arc<dyn Fn(&[u8], &[u8]) -> std::cmp::Ordering + Send + Sync> {
        &self.compare
    }

    pub fn is_unique(&self) -> bool {
        self.is_unique
    }

    // -------------------------------------------------------------------
    // Point lookup
    // -------------------------------------------------------------------

    pub fn search(&self, key: &[u8]) -> Result<Option<Tid>, Error> {
        let root = self.root_page_id();
        if root == INVALID_PAGE_ID {
            return Ok(None);
        }

        let mut current_id = root;
        loop {
            let page = self.buffer_pool.fetch_page(current_id)?;
            let data = page.as_bytes();
            let level = node::get_level(data);

            if level == 0 {
                let result = node::search_leaf(data, key, &*self.compare);
                self.buffer_pool.unpin_page(current_id, false)?;
                return Ok(result);
            }

            let child_id = node::find_child(data, key, &*self.compare);
            self.buffer_pool.unpin_page(current_id, false)?;
            current_id = child_id;
        }
    }

    // -------------------------------------------------------------------
    // Insert
    // -------------------------------------------------------------------

    pub fn insert(&self, key: &[u8], tid: Tid) -> Result<(), Error> {
        let root = self.root_page_id();

        if root == INVALID_PAGE_ID {
            return self.insert_into_empty_tree(key, tid);
        }

        // Phase 1: find path to leaf.
        let mut path: Vec<PageId> = Vec::new();
        let mut current_id = root;
        loop {
            let page = self.buffer_pool.fetch_page(current_id)?;
            let level = node::get_level(page.as_bytes());
            self.buffer_pool.unpin_page(current_id, false)?;

            if level == 0 {
                break;
            }
            let child_id = node::find_child(page.as_bytes(), key, &*self.compare);
            path.push(current_id);
            current_id = child_id;
        }

        // Phase 2: insert into leaf.
        let mut page = self.buffer_pool.fetch_page(current_id)?;

        // Unique check.
        if self.is_unique {
            if node::search_leaf(page.as_bytes(), key, &*self.compare).is_some() {
                self.buffer_pool.unpin_page(current_id, false)?;
                return Err(StorageError::DuplicateKey.into());
            }
        }

        if node::insert_leaf_entry(page.as_bytes_mut(), key, tid, &*self.compare) {
            page.recompute_checksum();
            self.buffer_pool.write_page(current_id, page)?;
            self.buffer_pool.unpin_page(current_id, true)?;
            return Ok(());
        }

        // Phase 3: split leaf.
        let (new_leaf_id, mut new_leaf) = self.buffer_pool.new_page(PageType::BtreeLeaf)?;
        node::init_leaf(new_leaf.as_bytes_mut());

        let separator = node::split_leaf(
            page.as_bytes_mut(),
            new_leaf.as_bytes_mut(),
            key,
            tid,
            &*self.compare,
        );

        // Update sibling pointers.
        let old_next = node::get_leaf_next(page.as_bytes());
        node::set_leaf_next(page.as_bytes_mut(), new_leaf_id);
        node::set_leaf_prev(new_leaf.as_bytes_mut(), current_id);
        node::set_leaf_next(new_leaf.as_bytes_mut(), old_next);

        if old_next != INVALID_PAGE_ID {
            let mut old_next_page = self.buffer_pool.fetch_page(old_next)?;
            node::set_leaf_prev(old_next_page.as_bytes_mut(), new_leaf_id);
            old_next_page.recompute_checksum();
            self.buffer_pool.write_page(old_next, old_next_page)?;
            self.buffer_pool.unpin_page(old_next, true)?;
        }

        page.recompute_checksum();
        new_leaf.recompute_checksum();
        self.buffer_pool.write_page(current_id, page)?;
        self.buffer_pool.write_page(new_leaf_id, new_leaf)?;
        self.buffer_pool.unpin_page(current_id, true)?;
        self.buffer_pool.unpin_page(new_leaf_id, true)?;

        // Phase 4: propagate split upward.
        self.propagate_split(path, separator, new_leaf_id)
    }

    fn insert_into_empty_tree(&self, key: &[u8], tid: Tid) -> Result<(), Error> {
        let (new_root_id, mut new_root) = self.buffer_pool.new_page(PageType::BtreeLeaf)?;
        node::init_leaf(new_root.as_bytes_mut());
        node::insert_leaf_entry(new_root.as_bytes_mut(), key, tid, &*self.compare);
        new_root.recompute_checksum();
        self.buffer_pool.write_page(new_root_id, new_root)?;
        self.buffer_pool.unpin_page(new_root_id, true)?;
        *self.root_page_id.lock().unwrap() = new_root_id;
        Ok(())
    }

    fn propagate_split(
        &self,
        mut path: Vec<PageId>,
        mut split_key: Vec<u8>,
        mut split_child: PageId,
    ) -> Result<(), Error> {
        while let Some(parent_id) = path.pop() {
            let mut parent = self.buffer_pool.fetch_page(parent_id)?;

            if node::insert_internal_entry(
                parent.as_bytes_mut(),
                &split_key,
                split_child,
                &*self.compare,
            ) {
                parent.recompute_checksum();
                self.buffer_pool.write_page(parent_id, parent)?;
                self.buffer_pool.unpin_page(parent_id, true)?;
                return Ok(());
            }

            // Split internal node.
            let (new_internal_id, mut new_internal) =
                self.buffer_pool.new_page(PageType::BtreeInternal)?;
            let level = node::get_level(parent.as_bytes());
            node::init_internal(new_internal.as_bytes_mut(), level, INVALID_PAGE_ID);

            let push_up = node::split_internal(
                parent.as_bytes_mut(),
                new_internal.as_bytes_mut(),
                &split_key,
                split_child,
                &*self.compare,
            );

            parent.recompute_checksum();
            new_internal.recompute_checksum();
            self.buffer_pool.write_page(parent_id, parent)?;
            self.buffer_pool.write_page(new_internal_id, new_internal)?;
            self.buffer_pool.unpin_page(parent_id, true)?;
            self.buffer_pool.unpin_page(new_internal_id, true)?;

            split_key = push_up;
            split_child = new_internal_id;
        }

        // Split reached root — create a new root.
        self.create_new_root(split_key, split_child)
    }

    fn create_new_root(&self, key: Vec<u8>, right_child: PageId) -> Result<(), Error> {
        let old_root = self.root_page_id();
        let old_root_page = self.buffer_pool.fetch_page(old_root)?;
        let old_level = node::get_level(old_root_page.as_bytes());
        self.buffer_pool.unpin_page(old_root, false)?;

        let (new_root_id, mut new_root) = self.buffer_pool.new_page(PageType::BtreeInternal)?;
        node::init_internal(new_root.as_bytes_mut(), old_level + 1, old_root);
        node::insert_internal_entry(
            new_root.as_bytes_mut(),
            &key,
            right_child,
            &*self.compare,
        );
        new_root.recompute_checksum();
        self.buffer_pool.write_page(new_root_id, new_root)?;
        self.buffer_pool.unpin_page(new_root_id, true)?;
        *self.root_page_id.lock().unwrap() = new_root_id;
        Ok(())
    }

    // -------------------------------------------------------------------
    // Delete (lazy marking)
    // -------------------------------------------------------------------

    pub fn delete(&self, key: &[u8]) -> Result<bool, Error> {
        let root = self.root_page_id();
        if root == INVALID_PAGE_ID {
            return Ok(false);
        }

        let mut current_id = root;
        loop {
            let page = self.buffer_pool.fetch_page(current_id)?;
            let level = node::get_level(page.as_bytes());

            if level == 0 {
                let mut page = page;
                let deleted =
                    node::mark_deleted_leaf(page.as_bytes_mut(), key, &*self.compare);
                if deleted {
                    page.recompute_checksum();
                    self.buffer_pool.write_page(current_id, page)?;
                    self.buffer_pool.unpin_page(current_id, true)?;
                } else {
                    self.buffer_pool.unpin_page(current_id, false)?;
                }
                return Ok(deleted);
            }

            let child_id = node::find_child(page.as_bytes(), key, &*self.compare);
            self.buffer_pool.unpin_page(current_id, false)?;
            current_id = child_id;
        }
    }

    // -------------------------------------------------------------------
    // Height
    // -------------------------------------------------------------------

    pub fn height(&self) -> Result<usize, Error> {
        let root = self.root_page_id();
        if root == INVALID_PAGE_ID {
            return Ok(0);
        }
        let page = self.buffer_pool.fetch_page(root)?;
        let h = node::height_from_root(page.as_bytes());
        self.buffer_pool.unpin_page(root, false)?;
        Ok(h)
    }

    // -------------------------------------------------------------------
    // VACUUM (merge under-full leaves)
    // -------------------------------------------------------------------

    /// Scan all leaves and merge pairs that are < 40% full. Returns the number
    /// of merges performed.
    pub fn vacuum(&self) -> Result<usize, Error> {
        let root = self.root_page_id();
        if root == INVALID_PAGE_ID {
            return Ok(0);
        }

        // Find leftmost leaf.
        let mut current_id = root;
        loop {
            let page = self.buffer_pool.fetch_page(current_id)?;
            let level = node::get_level(page.as_bytes());
            if level == 0 {
                self.buffer_pool.unpin_page(current_id, false)?;
                break;
            }
            let child = node::get_first_child(page.as_bytes());
            self.buffer_pool.unpin_page(current_id, false)?;
            current_id = child;
        }

        // Walk the leaf chain and compact under-full nodes.
        let mut merges = 0usize;
        loop {
            let mut page = self.buffer_pool.fetch_page(current_id)?;
            // Compact deleted entries.
            node::compact_leaf(page.as_bytes_mut());
            page.recompute_checksum();
            self.buffer_pool.write_page(current_id, page.clone())?;

            let fill = node::leaf_fill_factor(page.as_bytes());
            let next_id = node::get_leaf_next(page.as_bytes());
            self.buffer_pool.unpin_page(current_id, true)?;

            if fill < 0.40 && next_id != INVALID_PAGE_ID {
                // Try to merge with next sibling.
                if self.try_merge_leaves(current_id, next_id)? {
                    merges += 1;
                    continue; // re-check the same node (now has more entries).
                }
            }

            if next_id == INVALID_PAGE_ID {
                break;
            }
            current_id = next_id;
        }

        Ok(merges)
    }

    /// Try merging `left_id` and `right_id` into `left_id`. Returns true on
    /// success.
    fn try_merge_leaves(&self, left_id: PageId, right_id: PageId) -> Result<bool, Error> {
        let mut left = self.buffer_pool.fetch_page(left_id)?;
        let right = self.buffer_pool.fetch_page(right_id)?;

        // Collect both sets of entries.
        let mut entries: Vec<(Vec<u8>, Tid)> = Vec::new();
        let ln = node::get_key_count(left.as_bytes()) as usize;
        for i in 0..ln {
            if let Some(e) = node::read_leaf_entry(left.as_bytes(), i) {
                entries.push(e);
            }
        }
        let rn = node::get_key_count(right.as_bytes()) as usize;
        for i in 0..rn {
            if let Some(e) = node::read_leaf_entry(right.as_bytes(), i) {
                entries.push(e);
            }
        }

        // Check if combined entries fit on one page.
        let total_key_bytes: usize = entries.iter().map(|(k, _)| k.len()).sum();
        let total_needed = entries.len() * (super::node::LEAF_CELL_OVERHEAD + 2)
            + total_key_bytes
            + super::node::LEAF_CELL_PTRS_OFF; // approximate header
        if total_needed > PAGE_SIZE {
            self.buffer_pool.unpin_page(left_id, false)?;
            self.buffer_pool.unpin_page(right_id, false)?;
            return Ok(false);
        }

        // Rebuild left with all entries.
        node::init_leaf(left.as_bytes_mut());
        for (key, tid) in &entries {
            node::insert_leaf_entry(left.as_bytes_mut(), key, *tid, &*self.compare);
        }

        // Update sibling pointers.
        let right_next = node::get_leaf_next(right.as_bytes());
        node::set_leaf_next(left.as_bytes_mut(), right_next);

        left.recompute_checksum();
        self.buffer_pool.write_page(left_id, left)?;
        self.buffer_pool.unpin_page(left_id, true)?;
        self.buffer_pool.unpin_page(right_id, false)?;

        if right_next != INVALID_PAGE_ID {
            let mut next_page = self.buffer_pool.fetch_page(right_next)?;
            node::set_leaf_prev(next_page.as_bytes_mut(), left_id);
            next_page.recompute_checksum();
            self.buffer_pool.write_page(right_next, next_page)?;
            self.buffer_pool.unpin_page(right_next, true)?;
        }

        // NOTE: We don't remove the separator from the parent here for
        // simplicity; the orphaned internal entry will be cleaned up on a
        // future tree rebuild. In a full implementation the parent entry would
        // be removed and possibly trigger further merges.

        Ok(true)
    }

    // -------------------------------------------------------------------
    // Range scan
    // -------------------------------------------------------------------

    pub fn range_scan(
        &self,
        start_bound: Option<(&[u8], bool)>, // (key, inclusive)
        end_bound: Option<(&[u8], bool)>,   // (key, inclusive)
        direction: ScanDirection,
    ) -> Result<BTreeIterator, Error> {
        let end_owned = end_bound.map(|(k, inc)| (k.to_vec(), inc));
        let start_owned = start_bound.map(|(k, inc)| (k.to_vec(), inc));

        match direction {
            ScanDirection::Forward => {
                let (leaf_id, idx) = self.find_start_forward(start_owned.as_ref())?;
                Ok(BTreeIterator::new_forward(
                    self.buffer_pool.clone(),
                    self.compare.clone(),
                    leaf_id,
                    idx,
                    end_owned,
                ))
            }
            ScanDirection::Backward => {
                let (leaf_id, idx) = self.find_start_backward(end_owned.as_ref())?;
                Ok(BTreeIterator::new_backward(
                    self.buffer_pool.clone(),
                    self.compare.clone(),
                    leaf_id,
                    idx,
                    start_owned,
                ))
            }
        }
    }

    /// Find the leaf and index for forward scan start.
    fn find_start_forward(
        &self,
        start: Option<&(Vec<u8>, bool)>,
    ) -> Result<(PageId, usize), Error> {
        let root = self.root_page_id();
        if root == INVALID_PAGE_ID {
            return Ok((INVALID_PAGE_ID, 0));
        }

        match start {
            None => {
                // Start from leftmost leaf, index 0.
                let leaf_id = self.find_leftmost_leaf()?;
                Ok((leaf_id, 0))
            }
            Some((key, inclusive)) => {
                let leaf_id = self.find_leaf_for_key(key)?;
                let page = self.buffer_pool.fetch_page(leaf_id)?;
                let n = node::get_key_count(page.as_bytes()) as usize;
                for i in 0..n {
                    if let Some(entry) = node::read_leaf_entry(page.as_bytes(), i) {
                        let ord = (self.compare)(&entry.0, key);
                        if *inclusive {
                            if ord != std::cmp::Ordering::Less {
                                self.buffer_pool.unpin_page(leaf_id, false)?;
                                return Ok((leaf_id, i));
                            }
                        } else if ord == std::cmp::Ordering::Greater {
                            self.buffer_pool.unpin_page(leaf_id, false)?;
                            return Ok((leaf_id, i));
                        }
                    }
                }
                self.buffer_pool.unpin_page(leaf_id, false)?;
                // All entries in this leaf are <= key. Move to next leaf.
                let page2 = self.buffer_pool.fetch_page(leaf_id)?;
                let next = node::get_leaf_next(page2.as_bytes());
                self.buffer_pool.unpin_page(leaf_id, false)?;
                if next == INVALID_PAGE_ID {
                    Ok((INVALID_PAGE_ID, 0))
                } else {
                    Ok((next, 0))
                }
            }
        }
    }

    /// Find the leaf and index for backward scan start.
    fn find_start_backward(
        &self,
        end: Option<&(Vec<u8>, bool)>,
    ) -> Result<(PageId, usize), Error> {
        let root = self.root_page_id();
        if root == INVALID_PAGE_ID {
            return Ok((INVALID_PAGE_ID, 0));
        }

        match end {
            None => {
                // Start from rightmost leaf, last entry.
                let leaf_id = self.find_rightmost_leaf()?;
                let page = self.buffer_pool.fetch_page(leaf_id)?;
                let n = node::get_key_count(page.as_bytes()) as usize;
                self.buffer_pool.unpin_page(leaf_id, false)?;
                let idx = if n > 0 { n - 1 } else { 0 };
                Ok((leaf_id, idx))
            }
            Some((key, inclusive)) => {
                let leaf_id = self.find_leaf_for_key(key)?;
                let page = self.buffer_pool.fetch_page(leaf_id)?;
                let n = node::get_key_count(page.as_bytes()) as usize;
                // Find last entry <= key (inclusive) or < key (exclusive).
                let mut found_idx: Option<usize> = None;
                for i in (0..n).rev() {
                    if let Some(entry) = node::read_leaf_entry(page.as_bytes(), i) {
                        let ord = (self.compare)(&entry.0, key);
                        if *inclusive {
                            if ord != std::cmp::Ordering::Greater {
                                found_idx = Some(i);
                                break;
                            }
                        } else if ord == std::cmp::Ordering::Less {
                            found_idx = Some(i);
                            break;
                        }
                    }
                }
                self.buffer_pool.unpin_page(leaf_id, false)?;
                match found_idx {
                    Some(idx) => Ok((leaf_id, idx)),
                    None => {
                        // All entries in this leaf are > key, go to prev leaf.
                        let page2 = self.buffer_pool.fetch_page(leaf_id)?;
                        let prev = node::get_leaf_prev(page2.as_bytes());
                        self.buffer_pool.unpin_page(leaf_id, false)?;
                        if prev == INVALID_PAGE_ID {
                            Ok((INVALID_PAGE_ID, 0))
                        } else {
                            let pp = self.buffer_pool.fetch_page(prev)?;
                            let pn = node::get_key_count(pp.as_bytes()) as usize;
                            self.buffer_pool.unpin_page(prev, false)?;
                            Ok((prev, if pn > 0 { pn - 1 } else { 0 }))
                        }
                    }
                }
            }
        }
    }

    fn find_leaf_for_key(&self, key: &[u8]) -> Result<PageId, Error> {
        let root = self.root_page_id();
        let mut current_id = root;
        loop {
            let page = self.buffer_pool.fetch_page(current_id)?;
            let level = node::get_level(page.as_bytes());
            if level == 0 {
                self.buffer_pool.unpin_page(current_id, false)?;
                return Ok(current_id);
            }
            let child = node::find_child(page.as_bytes(), key, &*self.compare);
            self.buffer_pool.unpin_page(current_id, false)?;
            current_id = child;
        }
    }

    fn find_leftmost_leaf(&self) -> Result<PageId, Error> {
        let root = self.root_page_id();
        let mut current_id = root;
        loop {
            let page = self.buffer_pool.fetch_page(current_id)?;
            let level = node::get_level(page.as_bytes());
            if level == 0 {
                self.buffer_pool.unpin_page(current_id, false)?;
                return Ok(current_id);
            }
            let child = node::get_first_child(page.as_bytes());
            self.buffer_pool.unpin_page(current_id, false)?;
            current_id = child;
        }
    }

    fn find_rightmost_leaf(&self) -> Result<PageId, Error> {
        let root = self.root_page_id();
        let mut current_id = root;
        loop {
            let page = self.buffer_pool.fetch_page(current_id)?;
            let data = page.as_bytes();
            let level = node::get_level(data);
            if level == 0 {
                self.buffer_pool.unpin_page(current_id, false)?;
                return Ok(current_id);
            }
            let n = node::get_key_count(data) as usize;
            let child = if n == 0 {
                node::get_first_child(data)
            } else {
                // Last child pointer is in the last cell entry.
                let off = (get_raw_cell_ptr_pub(data, n - 1, false) & OFFSET_MASK) as usize;
                let key_len = u16::from_le_bytes(data[off..off+2].try_into().unwrap()) as usize;
                u32::from_le_bytes(data[off+2+key_len..off+2+key_len+4].try_into().unwrap())
            };
            self.buffer_pool.unpin_page(current_id, false)?;
            current_id = child;
        }
    }
}

// Thin shim so btree.rs can read a cell pointer without making node internals
// fully public.
const OFFSET_MASK: u16 = 0x7FFF;

fn get_raw_cell_ptr_pub(data: &[u8], index: usize, is_leaf: bool) -> u16 {
    let base = if is_leaf { 38 } else { 34 };
    let off = base + index * 2;
    u16::from_le_bytes(data[off..off + 2].try_into().unwrap())
}

use crate::storage::page::PAGE_SIZE;
