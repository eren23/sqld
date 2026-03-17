use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, RwLock};

use crate::storage::page::{Page, PageId, PageType};
use crate::types::datum::Datum;
use crate::utils::error::Error;

// ---------------------------------------------------------------------------
// TID — Tuple Identifier (page + slot)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TID {
    pub page_id: PageId,
    pub slot_id: u16,
}

impl TID {
    pub fn new(page_id: PageId, slot_id: u16) -> Self {
        TID { page_id, slot_id }
    }

    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.page_id.to_le_bytes());
        buf.extend_from_slice(&self.slot_id.to_le_bytes());
    }

    fn deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, Error> {
        if *offset + 6 > buf.len() {
            return Err(Error::Internal(
                "TID deserialize: unexpected end of buffer".into(),
            ));
        }
        let page_id = u32::from_le_bytes(buf[*offset..*offset + 4].try_into().unwrap());
        *offset += 4;
        let slot_id = u16::from_le_bytes(buf[*offset..*offset + 2].try_into().unwrap());
        *offset += 2;
        Ok(TID { page_id, slot_id })
    }
}

// ---------------------------------------------------------------------------
// Entry serialization: [Datum bytes][PageId: 4B][SlotId: 2B]
// ---------------------------------------------------------------------------

fn serialize_entry(key: &Datum, tid: &TID) -> Vec<u8> {
    let mut buf = Vec::new();
    key.serialize(&mut buf);
    tid.serialize(&mut buf);
    buf
}

fn deserialize_entry(data: &[u8]) -> Result<(Datum, TID), Error> {
    let mut offset = 0;
    let key = Datum::deserialize(data, &mut offset)?;
    let tid = TID::deserialize(data, &mut offset)?;
    Ok((key, tid))
}

// ---------------------------------------------------------------------------
// Hashing helpers
// ---------------------------------------------------------------------------

fn hash_key(key: &Datum) -> u64 {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

/// Extract the bucket index from a hash value given the current directory depth.
fn bucket_index(hash: u64, depth: u32) -> usize {
    if depth == 0 {
        0
    } else {
        (hash & ((1u64 << depth) - 1)) as usize
    }
}

// ---------------------------------------------------------------------------
// Internal state
// ---------------------------------------------------------------------------

const MAX_SPLIT_ATTEMPTS: usize = 64;

struct IndexState {
    global_depth: u32,
    bucket_ptrs: Vec<PageId>,
    /// Bucket pages keyed by PageId, each behind a page-level latch.
    pages: HashMap<PageId, Arc<RwLock<Page>>>,
}

// ---------------------------------------------------------------------------
// HashIndex — extendible hash index
// ---------------------------------------------------------------------------

/// An extendible hash index with a directory of 2^d pointers and bucket pages
/// that store key–TID pairs. Supports exact-match lookups, duplicate keys,
/// and concurrent access via page-level RwLock latches.
pub struct HashIndex {
    state: RwLock<IndexState>,
    next_page_id: AtomicU32,
}

impl HashIndex {
    /// Create a new, empty extendible hash index (global depth 0, one bucket).
    pub fn new() -> Self {
        let first_page_id: PageId = 1;
        let page = Page::new(first_page_id, PageType::HashBucket);
        // Local depth = 0 stored in page flags (default 0).

        let mut pages = HashMap::new();
        pages.insert(first_page_id, Arc::new(RwLock::new(page)));

        HashIndex {
            state: RwLock::new(IndexState {
                global_depth: 0,
                bucket_ptrs: vec![first_page_id],
                pages,
            }),
            next_page_id: AtomicU32::new(2),
        }
    }

    fn alloc_page_id(&self) -> PageId {
        self.next_page_id.fetch_add(1, Ordering::Relaxed)
    }

    // -------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------

    /// Insert a key–TID pair. Splits buckets and doubles the directory as
    /// needed when a bucket overflows.
    pub fn insert(&self, key: &Datum, tid: TID) -> Result<(), Error> {
        let entry_data = serialize_entry(key, &tid);
        let hash = hash_key(key);

        // Fast path: read-lock directory, write-lock bucket page.
        {
            let state = self.state.read().unwrap();
            let idx = bucket_index(hash, state.global_depth);
            let page_id = state.bucket_ptrs[idx];
            let page_lock = state.pages.get(&page_id).unwrap().clone();
            let mut page = page_lock.write().unwrap();
            if page.insert_tuple(&entry_data).is_ok() {
                return Ok(());
            }
            // Page full — fall through to split path.
        }

        // Slow path: write-lock directory, split, retry.
        for _ in 0..MAX_SPLIT_ATTEMPTS {
            let mut state = self.state.write().unwrap();
            let idx = bucket_index(hash, state.global_depth);
            let page_id = state.bucket_ptrs[idx];

            // Re-check: another thread may have already split this bucket.
            {
                let page_lock = state.pages.get(&page_id).unwrap().clone();
                let mut page = page_lock.write().unwrap();
                if page.insert_tuple(&entry_data).is_ok() {
                    return Ok(());
                }
            }

            self.split_bucket(&mut state, page_id)?;

            // Try insert after split.
            let new_idx = bucket_index(hash, state.global_depth);
            let new_page_id = state.bucket_ptrs[new_idx];
            {
                let page_lock = state.pages.get(&new_page_id).unwrap().clone();
                let mut page = page_lock.write().unwrap();
                if page.insert_tuple(&entry_data).is_ok() {
                    return Ok(());
                }
            }
            // Still full (all entries share the same hash prefix) — split again.
        }

        Err(Error::Internal(
            "hash index: exceeded maximum split attempts".into(),
        ))
    }

    /// Look up all TIDs for an exact-match on `key`. Returns an empty vec
    /// if the key is absent.
    pub fn lookup(&self, key: &Datum) -> Result<Vec<TID>, Error> {
        let hash = hash_key(key);
        let state = self.state.read().unwrap();
        let idx = bucket_index(hash, state.global_depth);
        let page_id = state.bucket_ptrs[idx];
        let page_lock = state.pages.get(&page_id).unwrap().clone();
        let page = page_lock.read().unwrap();

        let mut results = Vec::new();
        let tuple_count = page.tuple_count();
        for i in 0..tuple_count {
            if let Ok(data) = page.fetch_tuple(i) {
                let (entry_key, tid) = deserialize_entry(data)?;
                if entry_key == *key {
                    results.push(tid);
                }
            }
        }

        Ok(results)
    }

    /// Delete a specific key–TID pair. Returns `true` if the entry was found
    /// and removed, `false` otherwise.
    pub fn delete(&self, key: &Datum, tid: &TID) -> Result<bool, Error> {
        let hash = hash_key(key);
        let state = self.state.read().unwrap();
        let idx = bucket_index(hash, state.global_depth);
        let page_id = state.bucket_ptrs[idx];
        let page_lock = state.pages.get(&page_id).unwrap().clone();
        let mut page = page_lock.write().unwrap();

        let tuple_count = page.tuple_count();
        for i in 0..tuple_count {
            if let Ok(data) = page.fetch_tuple(i) {
                let (entry_key, entry_tid) = deserialize_entry(data)?;
                if entry_key == *key && entry_tid == *tid {
                    page.delete_tuple(i)?;
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    /// Range scans are not supported on hash indexes.
    pub fn range_scan(&self, _start: &Datum, _end: &Datum) -> Result<Vec<TID>, Error> {
        Err(Error::Internal(
            "range scan is not supported on hash index".into(),
        ))
    }

    // -------------------------------------------------------------------
    // Introspection
    // -------------------------------------------------------------------

    pub fn global_depth(&self) -> u32 {
        self.state.read().unwrap().global_depth
    }

    pub fn directory_size(&self) -> usize {
        self.state.read().unwrap().bucket_ptrs.len()
    }

    pub fn num_buckets(&self) -> usize {
        self.state.read().unwrap().pages.len()
    }

    // -------------------------------------------------------------------
    // Bucket splitting
    // -------------------------------------------------------------------

    /// Split an overflowing bucket: increment local depth, create a sibling
    /// page, redistribute entries by the distinguishing bit, and update the
    /// directory (doubling it when `local_depth` exceeds `global_depth`).
    fn split_bucket(&self, state: &mut IndexState, page_id: PageId) -> Result<(), Error> {
        let page_lock = state.pages.get(&page_id).unwrap().clone();
        let mut old_page = page_lock.write().unwrap();

        let local_depth = old_page.flags() as u32;
        let new_local_depth = local_depth + 1;

        // Double the directory if the new local depth exceeds global depth.
        if new_local_depth > state.global_depth {
            let old_size = state.bucket_ptrs.len();
            let mut new_ptrs = Vec::with_capacity(old_size * 2);
            for i in 0..old_size * 2 {
                new_ptrs.push(state.bucket_ptrs[i % old_size]);
            }
            state.bucket_ptrs = new_ptrs;
            state.global_depth += 1;
        }

        // Collect all live entries from the bucket.
        let mut entries: Vec<(Vec<u8>, u64)> = Vec::new();
        let tuple_count = old_page.tuple_count();
        for i in 0..tuple_count {
            if let Ok(data) = old_page.fetch_tuple(i) {
                let data_vec = data.to_vec();
                let (key, _) = deserialize_entry(&data_vec)?;
                let h = hash_key(&key);
                entries.push((data_vec, h));
            }
        }

        // Create the sibling bucket.
        let new_page_id = self.alloc_page_id();
        let mut new_page = Page::new(new_page_id, PageType::HashBucket);
        new_page.set_flags(new_local_depth as u16);

        // Rebuild the original bucket (fresh page, no fragmentation).
        let old_page_id = old_page.page_id();
        let mut rebuilt = Page::new(old_page_id, PageType::HashBucket);
        rebuilt.set_flags(new_local_depth as u16);

        // Redistribute entries by the split bit.
        let split_bit = 1u64 << local_depth;
        for (data, h) in &entries {
            if h & split_bit != 0 {
                new_page.insert_tuple(data)?;
            } else {
                rebuilt.insert_tuple(data)?;
            }
        }

        // Replace old page in-place (through the latch).
        *old_page = rebuilt;
        drop(old_page);

        // Register the sibling page.
        state
            .pages
            .insert(new_page_id, Arc::new(RwLock::new(new_page)));

        // Update directory pointers: entries that previously pointed to the
        // old bucket and whose index has the split bit set now point to the
        // sibling.
        for i in 0..state.bucket_ptrs.len() {
            if state.bucket_ptrs[i] == old_page_id && (i as u64 & split_bit) != 0 {
                state.bucket_ptrs[i] = new_page_id;
            }
        }

        Ok(())
    }
}
