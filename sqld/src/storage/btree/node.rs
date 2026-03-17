//! B+ tree node layout and manipulation on raw page bytes.
//!
//! Two node types share a common 4-byte prefix after the 24-byte page header:
//!
//! ```text
//! Offset 24-25: key_count  (u16)  -- total entries (incl. lazy-deleted in leaves)
//! Offset 26-27: level      (u16)  -- 0 = leaf, >=1 = internal
//! ```
//!
//! # Internal node (PageType::BtreeInternal)
//!
//! ```text
//! 28-31: first_child (u32)  -- leftmost child pointer (ptr_0)
//! 32-33: cell_end    (u16)  -- byte offset of lowest cell data
//! 34+  : cell_ptrs[0..key_count], each u16
//! ```
//!
//! Each cell: `[key_len: u16][key: key_len bytes][child_ptr: u32]`
//!
//! # Leaf node (PageType::BtreeLeaf)
//!
//! ```text
//! 28-31: prev_leaf  (u32)
//! 32-35: next_leaf  (u32)
//! 36-37: cell_end   (u16)
//! 38+  : cell_ptrs[0..key_count], each u16 (bit 15 = deleted flag)
//! ```
//!
//! Each cell: `[key_len: u16][key: key_len bytes][page_id: u32][slot_id: u16]`

use std::cmp::Ordering;

use crate::storage::heap_file::Tid;
use crate::storage::page::{PageId, INVALID_PAGE_ID, PAGE_HEADER_SIZE, PAGE_SIZE};

use super::CompareFn;

// ---------------------------------------------------------------------------
// Layout constants
// ---------------------------------------------------------------------------

const KEY_COUNT_OFF: usize = PAGE_HEADER_SIZE;       // 24
const LEVEL_OFF: usize = PAGE_HEADER_SIZE + 2;       // 26

// Internal-specific
const INT_FIRST_CHILD_OFF: usize = PAGE_HEADER_SIZE + 4;  // 28
const INT_CELL_END_OFF: usize = PAGE_HEADER_SIZE + 8;     // 32
const INT_CELL_PTRS_OFF: usize = PAGE_HEADER_SIZE + 10;   // 34

// Leaf-specific
const LEAF_PREV_OFF: usize = PAGE_HEADER_SIZE + 4;        // 28
const LEAF_NEXT_OFF: usize = PAGE_HEADER_SIZE + 8;        // 32
const LEAF_CELL_END_OFF: usize = PAGE_HEADER_SIZE + 12;   // 36
pub(crate) const LEAF_CELL_PTRS_OFF: usize = PAGE_HEADER_SIZE + 14;  // 38

const CELL_PTR_SIZE: usize = 2;
const DELETED_FLAG: u16 = 0x8000;
const OFFSET_MASK: u16 = 0x7FFF;

// Cell overhead (excluding key bytes)
const INT_CELL_OVERHEAD: usize = 6;   // key_len(2) + child_ptr(4)
pub(crate) const LEAF_CELL_OVERHEAD: usize = 8;  // key_len(2) + page_id(4) + slot_id(2)

// ---------------------------------------------------------------------------
// Primitive read/write
// ---------------------------------------------------------------------------

#[inline]
fn read_u16(data: &[u8], off: usize) -> u16 {
    u16::from_le_bytes(data[off..off + 2].try_into().unwrap())
}

#[inline]
fn write_u16(data: &mut [u8], off: usize, v: u16) {
    data[off..off + 2].copy_from_slice(&v.to_le_bytes());
}

#[inline]
fn read_u32(data: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(data[off..off + 4].try_into().unwrap())
}

#[inline]
fn write_u32(data: &mut [u8], off: usize, v: u32) {
    data[off..off + 4].copy_from_slice(&v.to_le_bytes());
}

// ---------------------------------------------------------------------------
// Node header accessors
// ---------------------------------------------------------------------------

pub fn get_key_count(data: &[u8]) -> u16 {
    read_u16(data, KEY_COUNT_OFF)
}

pub fn set_key_count(data: &mut [u8], v: u16) {
    write_u16(data, KEY_COUNT_OFF, v);
}

pub fn get_level(data: &[u8]) -> u16 {
    read_u16(data, LEVEL_OFF)
}

pub fn set_level(data: &mut [u8], v: u16) {
    write_u16(data, LEVEL_OFF, v);
}

pub fn is_leaf(data: &[u8]) -> bool {
    get_level(data) == 0
}

// -- Internal-specific --

pub fn get_first_child(data: &[u8]) -> PageId {
    read_u32(data, INT_FIRST_CHILD_OFF)
}

pub fn set_first_child(data: &mut [u8], v: PageId) {
    write_u32(data, INT_FIRST_CHILD_OFF, v);
}

fn get_int_cell_end(data: &[u8]) -> u16 {
    read_u16(data, INT_CELL_END_OFF)
}

fn set_int_cell_end(data: &mut [u8], v: u16) {
    write_u16(data, INT_CELL_END_OFF, v);
}

// -- Leaf-specific --

pub fn get_leaf_prev(data: &[u8]) -> PageId {
    read_u32(data, LEAF_PREV_OFF)
}

pub fn set_leaf_prev(data: &mut [u8], v: PageId) {
    write_u32(data, LEAF_PREV_OFF, v);
}

pub fn get_leaf_next(data: &[u8]) -> PageId {
    read_u32(data, LEAF_NEXT_OFF)
}

pub fn set_leaf_next(data: &mut [u8], v: PageId) {
    write_u32(data, LEAF_NEXT_OFF, v);
}

fn get_leaf_cell_end(data: &[u8]) -> u16 {
    read_u16(data, LEAF_CELL_END_OFF)
}

fn set_leaf_cell_end(data: &mut [u8], v: u16) {
    write_u16(data, LEAF_CELL_END_OFF, v);
}

// ---------------------------------------------------------------------------
// Cell pointer helpers
// ---------------------------------------------------------------------------

fn cell_ptr_offset(index: usize, is_leaf: bool) -> usize {
    let base = if is_leaf { LEAF_CELL_PTRS_OFF } else { INT_CELL_PTRS_OFF };
    base + index * CELL_PTR_SIZE
}

fn get_raw_cell_ptr(data: &[u8], index: usize, is_leaf: bool) -> u16 {
    read_u16(data, cell_ptr_offset(index, is_leaf))
}

fn set_raw_cell_ptr(data: &mut [u8], index: usize, is_leaf: bool, v: u16) {
    write_u16(data, cell_ptr_offset(index, is_leaf), v);
}

fn cell_data_offset(data: &[u8], index: usize, is_leaf: bool) -> usize {
    (get_raw_cell_ptr(data, index, is_leaf) & OFFSET_MASK) as usize
}

fn is_deleted(data: &[u8], index: usize) -> bool {
    get_raw_cell_ptr(data, index, true) & DELETED_FLAG != 0
}

// ---------------------------------------------------------------------------
// Cell read helpers
// ---------------------------------------------------------------------------

fn read_cell_key(data: &[u8], off: usize) -> (&[u8], usize) {
    let key_len = read_u16(data, off) as usize;
    let key_start = off + 2;
    (&data[key_start..key_start + key_len], 2 + key_len)
}

fn read_int_cell_child(data: &[u8], off: usize) -> PageId {
    let key_len = read_u16(data, off) as usize;
    read_u32(data, off + 2 + key_len)
}

fn read_leaf_cell_tid(data: &[u8], off: usize) -> Tid {
    let key_len = read_u16(data, off) as usize;
    let tid_off = off + 2 + key_len;
    Tid::new(read_u32(data, tid_off), read_u16(data, tid_off + 4))
}

// ---------------------------------------------------------------------------
// Free-space
// ---------------------------------------------------------------------------

fn cell_end_val(data: &[u8], is_leaf: bool) -> u16 {
    if is_leaf { get_leaf_cell_end(data) } else { get_int_cell_end(data) }
}

fn set_cell_end_val(data: &mut [u8], is_leaf: bool, v: u16) {
    if is_leaf { set_leaf_cell_end(data, v) } else { set_int_cell_end(data, v) }
}

fn ptr_array_end(data: &[u8], is_leaf: bool) -> usize {
    let base = if is_leaf { LEAF_CELL_PTRS_OFF } else { INT_CELL_PTRS_OFF };
    base + (get_key_count(data) as usize) * CELL_PTR_SIZE
}

fn free_space(data: &[u8], is_leaf: bool) -> usize {
    let ce = cell_end_val(data, is_leaf) as usize;
    let pae = ptr_array_end(data, is_leaf);
    ce.saturating_sub(pae)
}

pub fn has_space(data: &[u8], key_len: usize, is_leaf: bool) -> bool {
    let cell_overhead = if is_leaf { LEAF_CELL_OVERHEAD } else { INT_CELL_OVERHEAD };
    let needed = cell_overhead + key_len + CELL_PTR_SIZE;
    free_space(data, is_leaf) >= needed
}

// ---------------------------------------------------------------------------
// Initialization
// ---------------------------------------------------------------------------

pub fn init_leaf(data: &mut [u8]) {
    set_key_count(data, 0);
    set_level(data, 0);
    set_leaf_prev(data, INVALID_PAGE_ID);
    set_leaf_next(data, INVALID_PAGE_ID);
    set_leaf_cell_end(data, PAGE_SIZE as u16);
}

pub fn init_internal(data: &mut [u8], level: u16, first_child: PageId) {
    set_key_count(data, 0);
    set_level(data, level);
    set_first_child(data, first_child);
    set_int_cell_end(data, PAGE_SIZE as u16);
}

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

/// Binary search in an internal node. Returns the child page id to descend to.
pub fn find_child(data: &[u8], key: &[u8], cmp: &CompareFn) -> PageId {
    let n = get_key_count(data) as usize;
    if n == 0 {
        return get_first_child(data);
    }
    // Find first i where cell_key[i] > key.
    let mut lo: usize = 0;
    let mut hi: usize = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let off = cell_data_offset(data, mid, false);
        let (cell_key, _) = read_cell_key(data, off);
        if cmp(cell_key, key) != Ordering::Greater {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo == 0 {
        get_first_child(data)
    } else {
        let off = cell_data_offset(data, lo - 1, false);
        read_int_cell_child(data, off)
    }
}

/// Search a leaf for an exact key match. Returns the TID if found. Skips
/// lazy-deleted entries.
pub fn search_leaf(data: &[u8], key: &[u8], cmp: &CompareFn) -> Option<Tid> {
    let n = get_key_count(data) as usize;
    for i in 0..n {
        if is_deleted(data, i) {
            continue;
        }
        let off = cell_data_offset(data, i, true);
        let (cell_key, _) = read_cell_key(data, off);
        match cmp(cell_key, key) {
            Ordering::Equal => return Some(read_leaf_cell_tid(data, off)),
            Ordering::Greater => return None,
            Ordering::Less => {}
        }
    }
    None
}

fn find_leaf_insert_pos(data: &[u8], key: &[u8], cmp: &CompareFn) -> usize {
    let n = get_key_count(data) as usize;
    let mut lo: usize = 0;
    let mut hi: usize = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let off = cell_data_offset(data, mid, true);
        let (cell_key, _) = read_cell_key(data, off);
        if cmp(cell_key, key) == Ordering::Less {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

fn find_internal_insert_pos(data: &[u8], key: &[u8], cmp: &CompareFn) -> usize {
    let n = get_key_count(data) as usize;
    let mut lo: usize = 0;
    let mut hi: usize = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let off = cell_data_offset(data, mid, false);
        let (cell_key, _) = read_cell_key(data, off);
        if cmp(cell_key, key) == Ordering::Less {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

// ---------------------------------------------------------------------------
// Leaf entry read (for iterator and tests)
// ---------------------------------------------------------------------------

pub fn read_leaf_entry(data: &[u8], index: usize) -> Option<(Vec<u8>, Tid)> {
    if is_deleted(data, index) {
        return None;
    }
    let off = cell_data_offset(data, index, true);
    let (key, _) = read_cell_key(data, off);
    let tid = read_leaf_cell_tid(data, off);
    Some((key.to_vec(), tid))
}

// ---------------------------------------------------------------------------
// Insert helpers
// ---------------------------------------------------------------------------

fn write_cell(data: &mut [u8], is_leaf: bool, key: &[u8], payload: &[u8]) -> u16 {
    let csize = 2 + key.len() + payload.len();
    let ce = cell_end_val(data, is_leaf) as usize;
    let new_ce = ce - csize;

    write_u16(data, new_ce, key.len() as u16);
    data[new_ce + 2..new_ce + 2 + key.len()].copy_from_slice(key);
    data[new_ce + 2 + key.len()..new_ce + 2 + key.len() + payload.len()]
        .copy_from_slice(payload);

    set_cell_end_val(data, is_leaf, new_ce as u16);
    new_ce as u16
}

fn insert_cell_ptr(data: &mut [u8], is_leaf: bool, index: usize, ptr_val: u16) {
    let n = get_key_count(data) as usize;
    // Shift pointers [index..n) right by one.
    for i in (index..n).rev() {
        let v = get_raw_cell_ptr(data, i, is_leaf);
        set_raw_cell_ptr(data, i + 1, is_leaf, v);
    }
    set_raw_cell_ptr(data, index, is_leaf, ptr_val);
    set_key_count(data, (n + 1) as u16);
}

pub fn insert_leaf_entry(data: &mut [u8], key: &[u8], tid: Tid, cmp: &CompareFn) -> bool {
    if !has_space(data, key.len(), true) {
        return false;
    }
    let pos = find_leaf_insert_pos(data, key, cmp);
    let mut payload = [0u8; 6];
    payload[0..4].copy_from_slice(&tid.page_id.to_le_bytes());
    payload[4..6].copy_from_slice(&tid.slot_index.to_le_bytes());
    let cell_off = write_cell(data, true, key, &payload);
    insert_cell_ptr(data, true, pos, cell_off);
    true
}

pub fn insert_internal_entry(
    data: &mut [u8],
    key: &[u8],
    child: PageId,
    cmp: &CompareFn,
) -> bool {
    if !has_space(data, key.len(), false) {
        return false;
    }
    let pos = find_internal_insert_pos(data, key, cmp);
    let payload = child.to_le_bytes();
    let cell_off = write_cell(data, false, key, &payload);
    insert_cell_ptr(data, false, pos, cell_off);
    true
}

// ---------------------------------------------------------------------------
// Delete (lazy marking)
// ---------------------------------------------------------------------------

pub fn mark_deleted_leaf(data: &mut [u8], key: &[u8], cmp: &CompareFn) -> bool {
    let n = get_key_count(data) as usize;
    for i in 0..n {
        if is_deleted(data, i) {
            continue;
        }
        let off = cell_data_offset(data, i, true);
        let (cell_key, _) = read_cell_key(data, off);
        match cmp(cell_key, key) {
            Ordering::Equal => {
                let raw = get_raw_cell_ptr(data, i, true);
                set_raw_cell_ptr(data, i, true, raw | DELETED_FLAG);
                return true;
            }
            Ordering::Greater => return false,
            Ordering::Less => {}
        }
    }
    false
}

pub fn live_count_leaf(data: &[u8]) -> usize {
    let n = get_key_count(data) as usize;
    (0..n).filter(|&i| !is_deleted(data, i)).count()
}

pub fn leaf_capacity(avg_key_len: usize) -> usize {
    let usable = PAGE_SIZE - LEAF_CELL_PTRS_OFF;
    let per_entry = CELL_PTR_SIZE + LEAF_CELL_OVERHEAD + avg_key_len;
    if per_entry == 0 { return 0; }
    usable / per_entry
}

pub fn leaf_fill_factor(data: &[u8]) -> f64 {
    let n = get_key_count(data) as usize;
    if n == 0 {
        return 0.0;
    }
    let mut total_key_bytes: usize = 0;
    let mut count = 0usize;
    for i in 0..n {
        if !is_deleted(data, i) {
            let off = cell_data_offset(data, i, true);
            total_key_bytes += read_u16(data, off) as usize;
            count += 1;
        }
    }
    if count == 0 {
        return 0.0;
    }
    let avg_key = total_key_bytes / count;
    let cap = leaf_capacity(avg_key);
    if cap == 0 {
        return 1.0;
    }
    count as f64 / cap as f64
}

// ---------------------------------------------------------------------------
// Splitting
// ---------------------------------------------------------------------------

fn collect_leaf_entries(data: &[u8]) -> Vec<(Vec<u8>, Tid)> {
    let n = get_key_count(data) as usize;
    let mut entries = Vec::with_capacity(n);
    for i in 0..n {
        if is_deleted(data, i) {
            continue;
        }
        let off = cell_data_offset(data, i, true);
        let (key, _) = read_cell_key(data, off);
        let tid = read_leaf_cell_tid(data, off);
        entries.push((key.to_vec(), tid));
    }
    entries
}

fn collect_internal_entries(data: &[u8]) -> Vec<(Vec<u8>, PageId)> {
    let n = get_key_count(data) as usize;
    let mut entries = Vec::with_capacity(n);
    for i in 0..n {
        let off = cell_data_offset(data, i, false);
        let (key, _) = read_cell_key(data, off);
        let child = read_int_cell_child(data, off);
        entries.push((key.to_vec(), child));
    }
    entries
}

fn rebuild_leaf(data: &mut [u8], entries: &[(Vec<u8>, Tid)]) {
    let prev = get_leaf_prev(data);
    let next = get_leaf_next(data);
    // Clear cell area.
    data[LEAF_CELL_PTRS_OFF..PAGE_SIZE].fill(0);
    set_key_count(data, 0);
    set_leaf_cell_end(data, PAGE_SIZE as u16);
    set_leaf_prev(data, prev);
    set_leaf_next(data, next);
    for (key, tid) in entries {
        let mut payload = [0u8; 6];
        payload[0..4].copy_from_slice(&tid.page_id.to_le_bytes());
        payload[4..6].copy_from_slice(&tid.slot_index.to_le_bytes());
        let cell_off = write_cell(data, true, key, &payload);
        let n = get_key_count(data) as usize;
        set_raw_cell_ptr(data, n, true, cell_off);
        set_key_count(data, (n + 1) as u16);
    }
}

fn rebuild_internal(data: &mut [u8], first_child: PageId, entries: &[(Vec<u8>, PageId)]) {
    let level = get_level(data);
    data[INT_CELL_PTRS_OFF..PAGE_SIZE].fill(0);
    set_key_count(data, 0);
    set_level(data, level);
    set_first_child(data, first_child);
    set_int_cell_end(data, PAGE_SIZE as u16);
    for (key, child) in entries {
        let payload = child.to_le_bytes();
        let cell_off = write_cell(data, false, key, &payload);
        let n = get_key_count(data) as usize;
        set_raw_cell_ptr(data, n, false, cell_off);
        set_key_count(data, (n + 1) as u16);
    }
}

/// Split a full leaf including a new (key, tid). Writes left half into `left`,
/// right half into `right`. Returns the separator key (first key of right).
pub fn split_leaf(
    left: &mut [u8],
    right: &mut [u8],
    new_key: &[u8],
    new_tid: Tid,
    cmp: &CompareFn,
) -> Vec<u8> {
    let mut entries = collect_leaf_entries(left);
    let pos = entries.partition_point(|(k, _)| cmp(k, new_key) == Ordering::Less);
    entries.insert(pos, (new_key.to_vec(), new_tid));

    let mid = entries.len() / 2;
    let separator = entries[mid].0.clone();

    rebuild_leaf(left, &entries[..mid]);
    rebuild_leaf(right, &entries[mid..]);

    separator
}

/// Split a full internal node including a new (key, child). Writes left half
/// into `left`, right half into `right`. Returns the push-up key.
pub fn split_internal(
    left: &mut [u8],
    right: &mut [u8],
    new_key: &[u8],
    new_child: PageId,
    cmp: &CompareFn,
) -> Vec<u8> {
    let first_child = get_first_child(left);
    let mut entries = collect_internal_entries(left);
    let pos = entries.partition_point(|(k, _)| cmp(k, new_key) == Ordering::Less);
    entries.insert(pos, (new_key.to_vec(), new_child));

    let mid = entries.len() / 2;
    let push_up = entries[mid].0.clone();
    let right_first_child = entries[mid].1;

    let right_level = get_level(left);

    rebuild_internal(left, first_child, &entries[..mid]);
    set_level(right, right_level);
    rebuild_internal(right, right_first_child, &entries[mid + 1..]);
    set_level(right, right_level);

    push_up
}

/// Compact a leaf node: remove deleted entries and reclaim space.
pub fn compact_leaf(data: &mut [u8]) {
    let entries = collect_leaf_entries(data);
    rebuild_leaf(data, &entries);
}

// ---------------------------------------------------------------------------
// Safety checks for latch crabbing
// ---------------------------------------------------------------------------

pub fn is_safe_for_insert(data: &[u8], key_len: usize) -> bool {
    has_space(data, key_len, is_leaf(data))
}

pub fn is_safe_for_delete(data: &[u8]) -> bool {
    if !is_leaf(data) {
        return true;
    }
    live_count_leaf(data) > 2
}

pub fn height_from_root(data: &[u8]) -> usize {
    get_level(data) as usize + 1
}
