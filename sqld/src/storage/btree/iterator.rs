//! Range scan iterator over B+ tree leaf entries.

use std::sync::Arc;

use crate::storage::buffer_pool::BufferPoolManager;
use crate::storage::heap_file::Tid;
use crate::storage::page::{PageId, INVALID_PAGE_ID};
use crate::utils::error::Error;

use super::node;

// ---------------------------------------------------------------------------
// ScanDirection
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanDirection {
    Forward,
    Backward,
}

// ---------------------------------------------------------------------------
// BTreeIterator
// ---------------------------------------------------------------------------

pub struct BTreeIterator {
    buffer_pool: Arc<BufferPoolManager>,
    compare: Arc<dyn Fn(&[u8], &[u8]) -> std::cmp::Ordering + Send + Sync>,
    current_leaf_id: PageId,
    current_index: usize,
    direction: ScanDirection,
    /// For forward: end bound (key, inclusive).
    /// For backward: start bound (key, inclusive).
    bound: Option<(Vec<u8>, bool)>,
    exhausted: bool,
}

impl BTreeIterator {
    pub fn new_forward(
        buffer_pool: Arc<BufferPoolManager>,
        compare: Arc<dyn Fn(&[u8], &[u8]) -> std::cmp::Ordering + Send + Sync>,
        leaf_id: PageId,
        start_index: usize,
        end_bound: Option<(Vec<u8>, bool)>,
    ) -> Self {
        Self {
            buffer_pool,
            compare,
            current_leaf_id: leaf_id,
            current_index: start_index,
            direction: ScanDirection::Forward,
            bound: end_bound,
            exhausted: leaf_id == INVALID_PAGE_ID,
        }
    }

    pub fn new_backward(
        buffer_pool: Arc<BufferPoolManager>,
        compare: Arc<dyn Fn(&[u8], &[u8]) -> std::cmp::Ordering + Send + Sync>,
        leaf_id: PageId,
        start_index: usize,
        start_bound: Option<(Vec<u8>, bool)>,
    ) -> Self {
        Self {
            buffer_pool,
            compare,
            current_leaf_id: leaf_id,
            current_index: start_index,
            direction: ScanDirection::Backward,
            bound: start_bound,
            exhausted: leaf_id == INVALID_PAGE_ID,
        }
    }

    fn next_forward(&mut self) -> Option<Result<(Vec<u8>, Tid), Error>> {
        loop {
            if self.current_leaf_id == INVALID_PAGE_ID {
                self.exhausted = true;
                return None;
            }

            let page = match self.buffer_pool.fetch_page(self.current_leaf_id) {
                Ok(p) => p,
                Err(e) => return Some(Err(e)),
            };

            let data = page.as_bytes();
            let n = node::get_key_count(data) as usize;

            // Advance through entries.
            while self.current_index < n {
                let idx = self.current_index;
                self.current_index += 1;

                if let Some(entry) = node::read_leaf_entry(data, idx) {
                    // Check end bound.
                    if let Some((ref bound_key, inclusive)) = self.bound {
                        let ord = (self.compare)(&entry.0, bound_key);
                        if ord == std::cmp::Ordering::Greater
                            || (!inclusive && ord == std::cmp::Ordering::Equal)
                        {
                            let _ = self.buffer_pool.unpin_page(self.current_leaf_id, false);
                            self.exhausted = true;
                            return None;
                        }
                    }
                    let _ = self.buffer_pool.unpin_page(self.current_leaf_id, false);
                    return Some(Ok(entry));
                }
            }

            // Move to next leaf.
            let next = node::get_leaf_next(data);
            let _ = self.buffer_pool.unpin_page(self.current_leaf_id, false);
            self.current_leaf_id = next;
            self.current_index = 0;
        }
    }

    fn next_backward(&mut self) -> Option<Result<(Vec<u8>, Tid), Error>> {
        loop {
            if self.current_leaf_id == INVALID_PAGE_ID {
                self.exhausted = true;
                return None;
            }

            let page = match self.buffer_pool.fetch_page(self.current_leaf_id) {
                Ok(p) => p,
                Err(e) => return Some(Err(e)),
            };

            let data = page.as_bytes();
            let n = node::get_key_count(data) as usize;

            // Clamp index to valid range.
            if self.current_index >= n && n > 0 {
                self.current_index = n - 1;
            }

            // Walk backward through entries.
            loop {
                if n == 0 {
                    break;
                }

                let idx = self.current_index;

                if let Some(entry) = node::read_leaf_entry(data, idx) {
                    // Check start bound.
                    if let Some((ref bound_key, inclusive)) = self.bound {
                        let ord = (self.compare)(&entry.0, bound_key);
                        if ord == std::cmp::Ordering::Less
                            || (!inclusive && ord == std::cmp::Ordering::Equal)
                        {
                            let _ = self.buffer_pool.unpin_page(self.current_leaf_id, false);
                            self.exhausted = true;
                            return None;
                        }
                    }

                    if idx == 0 {
                        // Next call will move to prev leaf.
                        let prev = node::get_leaf_prev(data);
                        let _ = self.buffer_pool.unpin_page(self.current_leaf_id, false);
                        self.current_leaf_id = prev;
                        if prev != INVALID_PAGE_ID {
                            // Will set index to last entry on next call.
                            self.current_index = usize::MAX;
                        }
                        return Some(Ok(entry));
                    }
                    self.current_index = idx - 1;
                    let _ = self.buffer_pool.unpin_page(self.current_leaf_id, false);
                    return Some(Ok(entry));
                }

                // This entry was deleted; skip it.
                if idx == 0 {
                    break;
                }
                self.current_index = idx - 1;
            }

            // Move to prev leaf.
            let prev = node::get_leaf_prev(data);
            let _ = self.buffer_pool.unpin_page(self.current_leaf_id, false);
            self.current_leaf_id = prev;
            if prev != INVALID_PAGE_ID {
                self.current_index = usize::MAX; // will be clamped on next iteration
            }
        }
    }
}

impl Iterator for BTreeIterator {
    type Item = Result<(Vec<u8>, Tid), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }
        match self.direction {
            ScanDirection::Forward => self.next_forward(),
            ScanDirection::Backward => self.next_backward(),
        }
    }
}
