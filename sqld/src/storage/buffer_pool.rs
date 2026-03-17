use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::storage::disk_manager::DiskManager;
use crate::storage::page::{Page, PageId, PageType, INVALID_PAGE_ID};
use crate::utils::error::{Error, StorageError, WalError};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default number of frames: 256 MB / 8 KB = 32768.
pub const DEFAULT_POOL_SIZE: usize = 32768;
/// Default K parameter for LRU-K.
pub const DEFAULT_LRU_K: usize = 2;
/// Maximum number of pages to pre-fetch in one read-ahead hint.
pub const PREFETCH_SIZE: usize = 32;

pub type FrameId = usize;

// ---------------------------------------------------------------------------
// Frame
// ---------------------------------------------------------------------------

/// A single buffer frame holding one page.
struct Frame {
    page: Option<Page>,
    page_id: PageId,
    pin_count: u32,
    is_dirty: AtomicBool,
}

impl Frame {
    fn new() -> Self {
        Frame {
            page: None,
            page_id: INVALID_PAGE_ID,
            pin_count: 0,
            is_dirty: AtomicBool::new(false),
        }
    }

    fn reset(&mut self) {
        self.page = None;
        self.page_id = INVALID_PAGE_ID;
        self.pin_count = 0;
        self.is_dirty.store(false, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// LRU-K Replacer
// ---------------------------------------------------------------------------

/// LRU-K replacement policy.
///
/// Tracks the last K access timestamps per frame. When evicting, selects the
/// frame with the largest backward K-distance:
///
/// 1. Frames with fewer than K accesses have infinite backward K-distance and
///    are evicted first (LRU among themselves by earliest access).
/// 2. Among frames with ≥ K accesses, the one whose K-th-last access is
///    oldest is evicted.
///
/// This provides sequential scan resistance: pages touched only once during a
/// scan are evicted before frequently accessed pages.
struct LruKReplacer {
    k: usize,
    current_timestamp: u64,
    /// Last K access timestamps per frame (front = oldest).
    history: HashMap<FrameId, VecDeque<u64>>,
    /// Whether each tracked frame is eligible for eviction.
    evictable: HashMap<FrameId, bool>,
}

impl LruKReplacer {
    fn new(k: usize) -> Self {
        LruKReplacer {
            k,
            current_timestamp: 0,
            history: HashMap::new(),
            evictable: HashMap::new(),
        }
    }

    /// Record an access for the given frame.
    fn record_access(&mut self, frame_id: FrameId) {
        self.current_timestamp += 1;
        let entry = self.history.entry(frame_id).or_default();
        entry.push_back(self.current_timestamp);
        if entry.len() > self.k {
            entry.pop_front();
        }
    }

    /// Mark a frame as evictable or not.
    fn set_evictable(&mut self, frame_id: FrameId, evictable: bool) {
        self.evictable.insert(frame_id, evictable);
    }

    /// Remove a frame from the replacer entirely (e.g. on eviction or delete).
    fn remove(&mut self, frame_id: FrameId) {
        self.history.remove(&frame_id);
        self.evictable.remove(&frame_id);
    }

    /// Select and remove a victim frame. Returns `None` if no evictable frame
    /// exists (all frames are pinned).
    fn evict(&mut self) -> Option<FrameId> {
        // Two candidate pools: infinite backward K-distance (< K accesses)
        // and finite backward K-distance (≥ K accesses).
        let mut best_inf: Option<(FrameId, u64)> = None;
        let mut best_finite: Option<(FrameId, u64)> = None;

        for (&frame_id, &is_evictable) in &self.evictable {
            if !is_evictable {
                continue;
            }

            let history = match self.history.get(&frame_id) {
                Some(h) if !h.is_empty() => h,
                _ => continue,
            };

            if history.len() < self.k {
                // Infinite backward K-distance — use earliest access as
                // tiebreaker (evict the one accessed earliest = true LRU).
                let earliest = history[0];
                if best_inf.map_or(true, |(_, ts)| earliest < ts) {
                    best_inf = Some((frame_id, earliest));
                }
            } else {
                // Finite backward K-distance — K-th-last access is front of
                // the deque. Evict the one with the smallest (oldest) value.
                let kth_last = history[0];
                if best_finite.map_or(true, |(_, ts)| kth_last < ts) {
                    best_finite = Some((frame_id, kth_last));
                }
            }
        }

        // Prefer evicting frames with infinite backward K-distance first.
        let victim = best_inf.or(best_finite).map(|(id, _)| id);
        if let Some(id) = victim {
            self.remove(id);
        }
        victim
    }

    /// Number of currently evictable frames.
    fn size(&self) -> usize {
        self.evictable.values().filter(|&&v| v).count()
    }
}

// ---------------------------------------------------------------------------
// BufferPoolManager
// ---------------------------------------------------------------------------

struct BufferPoolInner {
    frames: Vec<Frame>,
    page_table: HashMap<PageId, FrameId>,
    free_list: VecDeque<FrameId>,
    replacer: LruKReplacer,
}

/// Fixed-size buffer pool of 8 KB page frames backed by a [`DiskManager`].
///
/// Pages are fetched into frames via [`fetch_page`](Self::fetch_page) which
/// pins them (preventing eviction). Callers must [`unpin_page`](Self::unpin_page)
/// when done. Dirty pages are tracked with [`AtomicBool`] per frame and are
/// written back on eviction or explicit flush.
///
/// The WAL protocol is enforced: a dirty page may only be written to disk when
/// its `page_lsn ≤ flushed_wal_lsn`.
pub struct BufferPoolManager {
    pool_size: usize,
    inner: Mutex<BufferPoolInner>,
    disk_manager: Arc<DiskManager>,
    flushed_wal_lsn: AtomicU64,
}

impl BufferPoolManager {
    /// Create a buffer pool with `pool_size` frames and LRU-K parameter `k`.
    pub fn new(pool_size: usize, k: usize, disk_manager: Arc<DiskManager>) -> Self {
        let mut frames = Vec::with_capacity(pool_size);
        let mut free_list = VecDeque::with_capacity(pool_size);

        for i in 0..pool_size {
            frames.push(Frame::new());
            free_list.push_back(i);
        }

        BufferPoolManager {
            pool_size,
            inner: Mutex::new(BufferPoolInner {
                frames,
                page_table: HashMap::new(),
                free_list,
                replacer: LruKReplacer::new(k),
            }),
            disk_manager,
            flushed_wal_lsn: AtomicU64::new(0),
        }
    }

    /// Create with default settings (32 768 frames / 256 MB, K = 2).
    pub fn with_defaults(disk_manager: Arc<DiskManager>) -> Self {
        Self::new(DEFAULT_POOL_SIZE, DEFAULT_LRU_K, disk_manager)
    }

    // -------------------------------------------------------------------
    // WAL LSN
    // -------------------------------------------------------------------

    /// Set the flushed WAL LSN. Dirty pages with `page_lsn > flushed_wal_lsn`
    /// cannot be flushed to disk.
    pub fn set_flushed_wal_lsn(&self, lsn: u64) {
        self.flushed_wal_lsn.store(lsn, Ordering::SeqCst);
    }

    pub fn flushed_wal_lsn(&self) -> u64 {
        self.flushed_wal_lsn.load(Ordering::SeqCst)
    }

    // -------------------------------------------------------------------
    // Core API
    // -------------------------------------------------------------------

    /// Fetch a page into the buffer pool and pin it. Returns a clone of the
    /// page data.
    ///
    /// If the page is already resident, its pin count is incremented. Otherwise
    /// the page is read from disk (with checksum validation) into a free or
    /// evicted frame.
    pub fn fetch_page(&self, page_id: PageId) -> Result<Page, Error> {
        if page_id == INVALID_PAGE_ID {
            return Err(StorageError::InvalidPageId(page_id as u64).into());
        }

        let mut inner = self.inner.lock().unwrap();

        // Fast path: page already resident.
        if let Some(&frame_id) = inner.page_table.get(&page_id) {
            inner.frames[frame_id].pin_count += 1;
            inner.replacer.record_access(frame_id);
            inner.replacer.set_evictable(frame_id, false);
            return Ok(inner.frames[frame_id].page.clone().unwrap());
        }

        // Slow path: need a frame.
        let frame_id = self.get_free_frame(&mut *inner)?;

        let page = self.disk_manager.read_page(page_id)?;

        if !page.verify_checksum() {
            inner.free_list.push_back(frame_id);
            return Err(StorageError::CorruptedPage {
                page_id: page_id as u64,
                reason: "checksum mismatch".into(),
            }
            .into());
        }

        inner.frames[frame_id].page = Some(page.clone());
        inner.frames[frame_id].page_id = page_id;
        inner.frames[frame_id].pin_count = 1;
        inner.frames[frame_id].is_dirty.store(false, Ordering::Relaxed);

        inner.page_table.insert(page_id, frame_id);
        inner.replacer.record_access(frame_id);
        inner.replacer.set_evictable(frame_id, false);

        Ok(page)
    }

    /// Decrement the pin count of a page. When the count reaches zero the page
    /// becomes eligible for eviction. Optionally marks the page as dirty.
    pub fn unpin_page(&self, page_id: PageId, is_dirty: bool) -> Result<(), Error> {
        let mut inner = self.inner.lock().unwrap();

        let &frame_id = inner
            .page_table
            .get(&page_id)
            .ok_or_else(|| Error::Internal(format!("page {page_id} not in buffer pool")))?;

        if inner.frames[frame_id].pin_count == 0 {
            return Err(Error::Internal(format!(
                "page {page_id} pin_count already 0"
            )));
        }

        inner.frames[frame_id].pin_count -= 1;
        if is_dirty {
            inner.frames[frame_id]
                .is_dirty
                .store(true, Ordering::Relaxed);
        }
        if inner.frames[frame_id].pin_count == 0 {
            inner.replacer.set_evictable(frame_id, true);
        }

        Ok(())
    }

    /// Replace the page content in a frame with a modified copy and mark the
    /// frame dirty.
    pub fn write_page(&self, page_id: PageId, page: Page) -> Result<(), Error> {
        let mut inner = self.inner.lock().unwrap();

        let &frame_id = inner
            .page_table
            .get(&page_id)
            .ok_or_else(|| Error::Internal(format!("page {page_id} not in buffer pool")))?;

        inner.frames[frame_id].page = Some(page);
        inner.frames[frame_id]
            .is_dirty
            .store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Flush a specific dirty page to disk.
    ///
    /// Enforces the WAL protocol: returns an error if the page's LSN exceeds
    /// the flushed WAL LSN.
    pub fn flush_page(&self, page_id: PageId) -> Result<(), Error> {
        let inner = self.inner.lock().unwrap();

        let &frame_id = inner
            .page_table
            .get(&page_id)
            .ok_or_else(|| Error::Internal(format!("page {page_id} not in buffer pool")))?;

        let frame = &inner.frames[frame_id];
        if !frame.is_dirty.load(Ordering::Relaxed) {
            return Ok(());
        }

        if let Some(ref page) = frame.page {
            let page_lsn = page.lsn();
            let wal_lsn = self.flushed_wal_lsn.load(Ordering::SeqCst);
            if page_lsn > wal_lsn {
                return Err(WalError::LogCorrupted(format!(
                    "WAL protocol violation: page_lsn ({page_lsn}) > flushed_wal_lsn ({wal_lsn})"
                ))
                .into());
            }

            self.disk_manager.write_page(page_id, page)?;
            frame.is_dirty.store(false, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Flush all dirty pages whose LSN permits writing.
    pub fn flush_all_pages(&self) -> Result<(), Error> {
        let inner = self.inner.lock().unwrap();
        let wal_lsn = self.flushed_wal_lsn.load(Ordering::SeqCst);

        for (&page_id, &frame_id) in &inner.page_table {
            let frame = &inner.frames[frame_id];
            if !frame.is_dirty.load(Ordering::Relaxed) {
                continue;
            }
            if let Some(ref page) = frame.page {
                if page.lsn() > wal_lsn {
                    continue; // WAL not yet flushed far enough
                }
                self.disk_manager.write_page(page_id, page)?;
                frame.is_dirty.store(false, Ordering::Relaxed);
            }
        }

        Ok(())
    }

    /// Allocate a new page, install it in the pool, and pin it.
    pub fn new_page(&self, page_type: PageType) -> Result<(PageId, Page), Error> {
        let mut inner = self.inner.lock().unwrap();

        let frame_id = self.get_free_frame(&mut *inner)?;
        let page_id = self.disk_manager.allocate_page()?;
        let page = Page::new(page_id, page_type);

        inner.frames[frame_id].page = Some(page.clone());
        inner.frames[frame_id].page_id = page_id;
        inner.frames[frame_id].pin_count = 1;
        inner.frames[frame_id]
            .is_dirty
            .store(true, Ordering::Relaxed);

        inner.page_table.insert(page_id, frame_id);
        inner.replacer.record_access(frame_id);
        inner.replacer.set_evictable(frame_id, false);

        Ok((page_id, page))
    }

    /// Remove a page from the pool and deallocate it on disk. The page must
    /// not be pinned.
    pub fn delete_page(&self, page_id: PageId) -> Result<(), Error> {
        let mut inner = self.inner.lock().unwrap();

        if let Some(&frame_id) = inner.page_table.get(&page_id) {
            if inner.frames[frame_id].pin_count > 0 {
                return Err(Error::Internal(format!(
                    "cannot delete page {page_id}: still pinned (pin_count={})",
                    inner.frames[frame_id].pin_count
                )));
            }

            inner.page_table.remove(&page_id);
            inner.replacer.remove(frame_id);
            inner.frames[frame_id].reset();
            inner.free_list.push_back(frame_id);
        }

        self.disk_manager.deallocate_page(page_id)?;
        Ok(())
    }

    /// Hint that pages `[start_page_id .. start_page_id + count)` will be
    /// accessed soon (sequential scan read-ahead). Pages are loaded without
    /// pinning; count is capped at [`PREFETCH_SIZE`].
    pub fn prefetch_pages(&self, start_page_id: PageId, count: usize) -> Result<usize, Error> {
        let count = count.min(PREFETCH_SIZE);
        let mut inner = self.inner.lock().unwrap();
        let mut loaded = 0;

        for i in 0..count {
            let page_id = start_page_id + i as u32;
            if page_id == INVALID_PAGE_ID {
                continue;
            }
            if inner.page_table.contains_key(&page_id) {
                continue;
            }

            // Try the free list; fall back to eviction; stop if pool is full.
            let frame_id = if let Some(id) = inner.free_list.pop_front() {
                id
            } else {
                match self.evict_frame(&mut *inner) {
                    Ok(id) => id,
                    Err(_) => break,
                }
            };

            match self.disk_manager.read_page(page_id) {
                Ok(page) => {
                    if !page.verify_checksum() {
                        inner.free_list.push_back(frame_id);
                        continue;
                    }

                    inner.frames[frame_id].page = Some(page);
                    inner.frames[frame_id].page_id = page_id;
                    inner.frames[frame_id].pin_count = 0;
                    inner.frames[frame_id]
                        .is_dirty
                        .store(false, Ordering::Relaxed);

                    inner.page_table.insert(page_id, frame_id);
                    inner.replacer.record_access(frame_id);
                    inner.replacer.set_evictable(frame_id, true);
                    loaded += 1;
                }
                Err(_) => {
                    inner.free_list.push_back(frame_id);
                    break; // likely past end of file
                }
            }
        }

        Ok(loaded)
    }

    // -------------------------------------------------------------------
    // Introspection
    // -------------------------------------------------------------------

    /// Pin count for a page, or `None` if the page is not in the pool.
    pub fn pin_count(&self, page_id: PageId) -> Option<u32> {
        let inner = self.inner.lock().unwrap();
        inner
            .page_table
            .get(&page_id)
            .map(|&fid| inner.frames[fid].pin_count)
    }

    /// Whether a page is dirty, or `None` if the page is not in the pool.
    pub fn is_dirty(&self, page_id: PageId) -> Option<bool> {
        let inner = self.inner.lock().unwrap();
        inner
            .page_table
            .get(&page_id)
            .map(|&fid| inner.frames[fid].is_dirty.load(Ordering::Relaxed))
    }

    /// Total number of frames in the pool (capacity).
    pub fn pool_size(&self) -> usize {
        self.pool_size
    }

    /// Number of pages currently resident in the pool.
    pub fn size(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.page_table.len()
    }

    /// Number of evictable (unpinned) frames.
    pub fn evictable_count(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.replacer.size()
    }

    // -------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------

    /// Obtain a free frame from the free list, or by evicting a victim.
    fn get_free_frame(&self, inner: &mut BufferPoolInner) -> Result<FrameId, Error> {
        if let Some(frame_id) = inner.free_list.pop_front() {
            return Ok(frame_id);
        }
        self.evict_frame(inner)
    }

    /// Evict a victim frame, writing its dirty page to disk if necessary.
    ///
    /// Enforces the WAL protocol: a dirty page may only be written when
    /// `page_lsn ≤ flushed_wal_lsn`.
    fn evict_frame(&self, inner: &mut BufferPoolInner) -> Result<FrameId, Error> {
        let frame_id = inner
            .replacer
            .evict()
            .ok_or(StorageError::BufferPoolExhausted)?;

        let old_page_id = inner.frames[frame_id].page_id;
        let is_dirty = inner.frames[frame_id].is_dirty.load(Ordering::Relaxed);

        if is_dirty {
            if let Some(ref page) = inner.frames[frame_id].page {
                let page_lsn = page.lsn();
                let wal_lsn = self.flushed_wal_lsn.load(Ordering::SeqCst);
                if page_lsn > wal_lsn {
                    // Restore the frame in the replacer so it isn't lost.
                    inner.replacer.record_access(frame_id);
                    inner.replacer.set_evictable(frame_id, true);
                    return Err(WalError::LogCorrupted(format!(
                        "WAL protocol violation: page_lsn ({page_lsn}) > flushed_wal_lsn ({wal_lsn})"
                    ))
                    .into());
                }
                self.disk_manager.write_page(old_page_id, page)?;
            }
        }

        inner.page_table.remove(&old_page_id);
        inner.frames[frame_id].reset();

        Ok(frame_id)
    }
}
