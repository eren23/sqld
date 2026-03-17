//! Thread-safe B+ tree with latch crabbing (latch coupling).
//!
//! Read operations use per-page read latches with top-down crabbing: acquire
//! the child latch, then release the parent. Write operations acquire a
//! tree-level exclusive lock to serialize structural modifications, then
//! delegate to the inner [`BPlusTree`].

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::storage::buffer_pool::BufferPoolManager;
use crate::storage::heap_file::Tid;
use crate::storage::page::{PageId, INVALID_PAGE_ID};
use crate::utils::error::Error;

use super::btree::BPlusTree;
use super::iterator::{BTreeIterator, ScanDirection};
use super::node;
use super::CompareFn;

// ---------------------------------------------------------------------------
// RwLatch — lightweight reader-writer spinlock
// ---------------------------------------------------------------------------

pub struct RwLatch {
    /// -1 = write-locked, 0 = free, >0 = reader count.
    state: AtomicI32,
}

impl RwLatch {
    pub fn new() -> Self {
        Self {
            state: AtomicI32::new(0),
        }
    }

    pub fn read_lock(&self) {
        loop {
            let s = self.state.load(Ordering::Acquire);
            if s >= 0 {
                if self
                    .state
                    .compare_exchange_weak(s, s + 1, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    return;
                }
            }
            std::hint::spin_loop();
        }
    }

    pub fn read_unlock(&self) {
        self.state.fetch_sub(1, Ordering::Release);
    }

    pub fn write_lock(&self) {
        loop {
            if self
                .state
                .compare_exchange_weak(0, -1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
            std::hint::spin_loop();
        }
    }

    pub fn write_unlock(&self) {
        self.state.store(0, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// LatchGuard — RAII guard holding an Arc<RwLatch>
// ---------------------------------------------------------------------------

struct LatchGuard {
    latch: Arc<RwLatch>,
    is_write: bool,
}

impl Drop for LatchGuard {
    fn drop(&mut self) {
        if self.is_write {
            self.latch.write_unlock();
        } else {
            self.latch.read_unlock();
        }
    }
}

// ---------------------------------------------------------------------------
// LatchManager — per-page latch table
// ---------------------------------------------------------------------------

struct LatchManager {
    latches: Mutex<HashMap<PageId, Arc<RwLatch>>>,
}

impl LatchManager {
    fn new() -> Self {
        Self {
            latches: Mutex::new(HashMap::new()),
        }
    }

    fn get_latch(&self, page_id: PageId) -> Arc<RwLatch> {
        let mut map = self.latches.lock().unwrap();
        map.entry(page_id)
            .or_insert_with(|| Arc::new(RwLatch::new()))
            .clone()
    }

    fn read_lock(&self, page_id: PageId) -> LatchGuard {
        let latch = self.get_latch(page_id);
        latch.read_lock();
        LatchGuard {
            latch,
            is_write: false,
        }
    }

    #[allow(dead_code)]
    fn write_lock(&self, page_id: PageId) -> LatchGuard {
        let latch = self.get_latch(page_id);
        latch.write_lock();
        LatchGuard {
            latch,
            is_write: true,
        }
    }
}

// ---------------------------------------------------------------------------
// ConcurrentBPlusTree
// ---------------------------------------------------------------------------

/// Thread-safe B+ tree.
///
/// Searches use per-page read latches with top-down latch crabbing: the parent
/// latch is released as soon as the child latch is acquired. Inserts and
/// deletes acquire a tree-wide exclusive lock, ensuring structural
/// modifications (splits, merges) are serialized.
pub struct ConcurrentBPlusTree {
    tree: BPlusTree,
    /// Tree-wide lock: shared for reads, exclusive for writes.
    tree_lock: RwLock<()>,
    /// Per-page latch table for fine-grained read crabbing.
    latch_manager: LatchManager,
}

impl ConcurrentBPlusTree {
    pub fn new(
        buffer_pool: Arc<BufferPoolManager>,
        is_unique: bool,
        compare: Box<CompareFn>,
    ) -> Self {
        Self {
            tree: BPlusTree::new(buffer_pool, is_unique, compare),
            tree_lock: RwLock::new(()),
            latch_manager: LatchManager::new(),
        }
    }

    pub fn inner(&self) -> &BPlusTree {
        &self.tree
    }

    // -------------------------------------------------------------------
    // Search — read path with per-page latch crabbing
    // -------------------------------------------------------------------

    pub fn search(&self, key: &[u8]) -> Result<Option<Tid>, Error> {
        let _shared = self.tree_lock.read().unwrap();

        let root = self.tree.root_page_id();
        if root == INVALID_PAGE_ID {
            return Ok(None);
        }

        let mut current_id = root;
        let mut guard = self.latch_manager.read_lock(current_id);

        loop {
            let page = self.tree.buffer_pool().fetch_page(current_id)?;
            let data = page.as_bytes();
            let level = node::get_level(data);

            if level == 0 {
                let result = node::search_leaf(data, key, &**self.tree.comparator());
                self.tree.buffer_pool().unpin_page(current_id, false)?;
                drop(guard);
                return Ok(result);
            }

            let child_id = node::find_child(data, key, &**self.tree.comparator());
            self.tree.buffer_pool().unpin_page(current_id, false)?;

            // Latch crabbing: acquire child read latch, release parent.
            let child_guard = self.latch_manager.read_lock(child_id);
            drop(guard);
            guard = child_guard;
            current_id = child_id;
        }
    }

    // -------------------------------------------------------------------
    // Insert — exclusive tree lock, delegates to inner tree
    // -------------------------------------------------------------------

    pub fn insert(&self, key: &[u8], tid: Tid) -> Result<(), Error> {
        let _exclusive = self.tree_lock.write().unwrap();
        self.tree.insert(key, tid)
    }

    // -------------------------------------------------------------------
    // Delete — exclusive tree lock
    // -------------------------------------------------------------------

    pub fn delete(&self, key: &[u8]) -> Result<bool, Error> {
        let _exclusive = self.tree_lock.write().unwrap();
        self.tree.delete(key)
    }

    // -------------------------------------------------------------------
    // Range scan — shared tree lock, delegates to inner tree
    // -------------------------------------------------------------------

    pub fn range_scan(
        &self,
        start_bound: Option<(&[u8], bool)>,
        end_bound: Option<(&[u8], bool)>,
        direction: ScanDirection,
    ) -> Result<BTreeIterator, Error> {
        let _shared = self.tree_lock.read().unwrap();
        self.tree.range_scan(start_bound, end_bound, direction)
    }

    pub fn height(&self) -> Result<usize, Error> {
        let _shared = self.tree_lock.read().unwrap();
        self.tree.height()
    }

    pub fn vacuum(&self) -> Result<usize, Error> {
        let _exclusive = self.tree_lock.write().unwrap();
        self.tree.vacuum()
    }
}
