use crate::storage::page::PageId;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Granularity of free space tracking: each FSM byte represents this many
/// bytes of actual free space on a heap page.
const FSM_GRANULARITY: usize = 32;

/// Maximum FSM category value (255 = completely empty page).
const FSM_MAX_CATEGORY: u8 = 255;

// ---------------------------------------------------------------------------
// FreeSpaceMap
// ---------------------------------------------------------------------------

/// Tracks approximate free space for each heap page using one byte per page.
///
/// The byte value encodes free space in [`FSM_GRANULARITY`]-byte (32-byte)
/// increments:
///   - `0`   → page is full (< 32 bytes free)
///   - `255` → page is empty (≥ 8160 bytes free)
///
/// Used by INSERT to quickly find a page with enough room without scanning
/// every heap page.
pub struct FreeSpaceMap {
    /// One entry per heap page, indexed by position in the heap's page list.
    entries: Vec<u8>,
}

impl FreeSpaceMap {
    /// Create a new, empty free space map.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Create a free space map pre-sized for `num_pages` pages, all marked
    /// empty (255).
    pub fn with_capacity(num_pages: usize) -> Self {
        Self {
            entries: vec![FSM_MAX_CATEGORY; num_pages],
        }
    }

    /// Number of pages tracked.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    // -----------------------------------------------------------------------
    // Conversion helpers
    // -----------------------------------------------------------------------

    /// Convert actual free bytes to an FSM category byte.
    pub fn bytes_to_category(free_bytes: usize) -> u8 {
        let cat = free_bytes / FSM_GRANULARITY;
        if cat > FSM_MAX_CATEGORY as usize {
            FSM_MAX_CATEGORY
        } else {
            cat as u8
        }
    }

    /// Convert a needed byte count to the minimum FSM category that satisfies
    /// it.
    pub fn needed_to_category(needed_bytes: usize) -> u8 {
        let cat = (needed_bytes + FSM_GRANULARITY - 1) / FSM_GRANULARITY;
        if cat > FSM_MAX_CATEGORY as usize {
            FSM_MAX_CATEGORY
        } else {
            cat as u8
        }
    }

    // -----------------------------------------------------------------------
    // Update
    // -----------------------------------------------------------------------

    /// Record the free space (in bytes) for the page at `index` in the heap's
    /// page list.
    pub fn update(&mut self, index: usize, free_bytes: usize) {
        let cat = Self::bytes_to_category(free_bytes);
        if index >= self.entries.len() {
            self.entries.resize(index + 1, FSM_MAX_CATEGORY);
        }
        self.entries[index] = cat;
    }

    /// Record the free space using a page id and a mapping function that
    /// converts page ids to heap-list indices.
    pub fn update_page(&mut self, page_ids: &[PageId], page_id: PageId, free_bytes: usize) {
        if let Some(idx) = page_ids.iter().position(|&p| p == page_id) {
            self.update(idx, free_bytes);
        }
    }

    // -----------------------------------------------------------------------
    // Search
    // -----------------------------------------------------------------------

    /// Find a page with at least `needed_bytes` of free space. Returns the
    /// page id using the provided page-id list, or `None` if no page has
    /// enough room.
    pub fn find_page(&self, page_ids: &[PageId], needed_bytes: usize) -> Option<PageId> {
        let min_cat = Self::needed_to_category(needed_bytes);
        for (i, &cat) in self.entries.iter().enumerate() {
            if cat >= min_cat {
                if let Some(&pid) = page_ids.get(i) {
                    return Some(pid);
                }
            }
        }
        None
    }

    /// Get the raw category byte for a given index.
    pub fn get_category(&self, index: usize) -> Option<u8> {
        self.entries.get(index).copied()
    }

    /// Get the approximate free bytes for a given index.
    pub fn get_free_bytes(&self, index: usize) -> Option<usize> {
        self.entries.get(index).map(|&cat| cat as usize * FSM_GRANULARITY)
    }
}

impl Default for FreeSpaceMap {
    fn default() -> Self {
        Self::new()
    }
}
