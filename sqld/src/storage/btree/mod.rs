pub mod btree;
pub mod concurrent;
pub mod iterator;
pub mod node;

pub use btree::BPlusTree;
pub use concurrent::ConcurrentBPlusTree;
pub use iterator::{BTreeIterator, ScanDirection};

use std::cmp::Ordering;

// ---------------------------------------------------------------------------
// Key comparison
// ---------------------------------------------------------------------------

/// A key comparator function: compares two byte-slice keys and returns their
/// ordering. The B+ tree is generic over this comparator.
pub type CompareFn = dyn Fn(&[u8], &[u8]) -> Ordering + Send + Sync;

/// Default ascending byte-lexicographic comparison.
pub fn default_compare(a: &[u8], b: &[u8]) -> Ordering {
    a.cmp(b)
}

/// Descending comparison (reverses the default byte order).
pub fn reverse_compare(a: &[u8], b: &[u8]) -> Ordering {
    b.cmp(a)
}

// ---------------------------------------------------------------------------
// Key encoding helpers (for integer keys in tests and general use)
// ---------------------------------------------------------------------------

/// Encode an i64 as 8 big-endian bytes that sort in natural numeric order.
/// Uses sign-bit flip so negative values sort before positive values.
pub fn encode_i64_key(val: i64) -> [u8; 8] {
    let unsigned = (val as u64) ^ (1u64 << 63);
    unsigned.to_be_bytes()
}

/// Decode an i64 from the encoding produced by [`encode_i64_key`].
pub fn decode_i64_key(bytes: &[u8]) -> i64 {
    let unsigned = u64::from_be_bytes(bytes[..8].try_into().unwrap());
    (unsigned ^ (1u64 << 63)) as i64
}

/// Build a composite key by concatenating multiple fixed-width encoded columns.
pub fn encode_composite_key(columns: &[&[u8]]) -> Vec<u8> {
    let mut buf = Vec::new();
    for col in columns {
        buf.extend_from_slice(col);
    }
    buf
}

/// Compare composite keys column-by-column. `col_sizes` gives the byte width
/// of each column. If a key is shorter than the full composite width, it acts
/// as a prefix (equal to any key sharing that prefix).
pub fn composite_compare(a: &[u8], b: &[u8], col_sizes: &[usize]) -> Ordering {
    let mut off_a = 0;
    let mut off_b = 0;
    for &size in col_sizes {
        if off_a + size > a.len() || off_b + size > b.len() {
            break;
        }
        let cmp = a[off_a..off_a + size].cmp(&b[off_b..off_b + size]);
        if cmp != Ordering::Equal {
            return cmp;
        }
        off_a += size;
        off_b += size;
    }
    Ordering::Equal
}
