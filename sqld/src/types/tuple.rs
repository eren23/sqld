use crate::utils::error::Error;

use super::datum::Datum;

// ---------------------------------------------------------------------------
// MVCC Header
// ---------------------------------------------------------------------------

/// Multi-Version Concurrency Control header attached to every row.
///
/// - `xmin`: transaction ID that created this tuple version.
/// - `xmax`: transaction ID that deleted/updated this tuple (0 = live).
/// - `cid`:  command ID within the creating transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MvccHeader {
    pub xmin: u64,
    pub xmax: u64,
    pub cid: u32,
}

/// Byte size of the serialized MVCC header (8 + 8 + 4).
const MVCC_HEADER_SIZE: usize = 20;

impl MvccHeader {
    pub fn new(xmin: u64, xmax: u64, cid: u32) -> Self {
        Self { xmin, xmax, cid }
    }

    /// A new, live tuple created by `xmin`.
    pub fn new_insert(xmin: u64, cid: u32) -> Self {
        Self { xmin, xmax: 0, cid }
    }

    pub fn is_deleted(&self) -> bool {
        self.xmax != 0
    }

    pub fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.xmin.to_le_bytes());
        buf.extend_from_slice(&self.xmax.to_le_bytes());
        buf.extend_from_slice(&self.cid.to_le_bytes());
    }

    pub fn deserialize(buf: &[u8], offset: &mut usize) -> Result<Self, Error> {
        if *offset + MVCC_HEADER_SIZE > buf.len() {
            return Err(Error::Serialization(
                "buffer too short for MVCC header".into(),
            ));
        }
        let xmin = u64::from_le_bytes(buf[*offset..*offset + 8].try_into().unwrap());
        *offset += 8;
        let xmax = u64::from_le_bytes(buf[*offset..*offset + 8].try_into().unwrap());
        *offset += 8;
        let cid = u32::from_le_bytes(buf[*offset..*offset + 4].try_into().unwrap());
        *offset += 4;
        Ok(Self { xmin, xmax, cid })
    }
}

// ---------------------------------------------------------------------------
// Tuple
// ---------------------------------------------------------------------------

/// A row of data consisting of an MVCC header and a vector of [`Datum`]
/// values.
///
/// ## Serialization layout
///
/// ```text
/// [MvccHeader: 20 bytes]
/// [column_count: u16]
/// [null_bitmap: ceil(column_count / 8) bytes]
/// [datum_0] [datum_1] ... (only for non-null columns)
/// ```
///
/// Each datum is serialized in `[tag][payload]` format (see
/// [`Datum::serialize`]).
#[derive(Debug, Clone, PartialEq)]
pub struct Tuple {
    pub header: MvccHeader,
    data: Vec<Datum>,
}

impl Tuple {
    pub fn new(header: MvccHeader, data: Vec<Datum>) -> Self {
        Self { header, data }
    }

    pub fn get(&self, index: usize) -> Option<&Datum> {
        self.data.get(index)
    }

    pub fn column_count(&self) -> usize {
        self.data.len()
    }

    pub fn values(&self) -> &[Datum] {
        &self.data
    }

    pub fn into_values(self) -> Vec<Datum> {
        self.data
    }

    // -----------------------------------------------------------------------
    // Null bitmap helpers
    // -----------------------------------------------------------------------

    fn build_null_bitmap(data: &[Datum]) -> Vec<u8> {
        let nbytes = (data.len() + 7) / 8;
        let mut bitmap = vec![0u8; nbytes];
        for (i, d) in data.iter().enumerate() {
            if d.is_null() {
                bitmap[i / 8] |= 1 << (i % 8);
            }
        }
        bitmap
    }

    fn is_null_in_bitmap(bitmap: &[u8], index: usize) -> bool {
        bitmap[index / 8] & (1 << (index % 8)) != 0
    }

    // -----------------------------------------------------------------------
    // Serialization
    // -----------------------------------------------------------------------

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64);

        // MVCC header
        self.header.serialize(&mut buf);

        // Column count
        let count = self.data.len() as u16;
        buf.extend_from_slice(&count.to_le_bytes());

        // Null bitmap
        let bitmap = Self::build_null_bitmap(&self.data);
        buf.extend_from_slice(&bitmap);

        // Non-null datum payloads
        for d in &self.data {
            if !d.is_null() {
                d.serialize(&mut buf);
            }
        }

        buf
    }

    pub fn deserialize(buf: &[u8]) -> Result<Self, Error> {
        let mut offset = 0;

        // MVCC header
        let header = MvccHeader::deserialize(buf, &mut offset)?;

        // Column count
        if offset + 2 > buf.len() {
            return Err(Error::Serialization(
                "buffer too short for column count".into(),
            ));
        }
        let count =
            u16::from_le_bytes(buf[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;

        // Null bitmap
        let bitmap_len = (count + 7) / 8;
        if offset + bitmap_len > buf.len() {
            return Err(Error::Serialization(
                "buffer too short for null bitmap".into(),
            ));
        }
        let bitmap = buf[offset..offset + bitmap_len].to_vec();
        offset += bitmap_len;

        // Datum payloads
        let mut data = Vec::with_capacity(count);
        for i in 0..count {
            if Self::is_null_in_bitmap(&bitmap, i) {
                data.push(Datum::Null);
            } else {
                let d = Datum::deserialize(buf, &mut offset)?;
                data.push(d);
            }
        }

        Ok(Tuple { header, data })
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_tuple() -> Tuple {
        Tuple::new(
            MvccHeader::new_insert(1, 0),
            vec![
                Datum::Integer(42),
                Datum::Varchar("hello".into()),
                Datum::Null,
                Datum::Float(3.14),
                Datum::Boolean(true),
            ],
        )
    }

    #[test]
    fn roundtrip_serialization() {
        let original = sample_tuple();
        let bytes = original.serialize();
        let decoded = Tuple::deserialize(&bytes).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn roundtrip_all_types() {
        let tuple = Tuple::new(
            MvccHeader::new(100, 200, 5),
            vec![
                Datum::Integer(-1),
                Datum::BigInt(i64::MIN),
                Datum::Float(2.718),
                Datum::Boolean(false),
                Datum::Varchar("test".into()),
                Datum::Text("long text".into()),
                Datum::Timestamp(1_700_000_000_000_000),
                Datum::Date(19700),
                Datum::Decimal {
                    mantissa: 99999,
                    scale: 3,
                },
                Datum::Blob(vec![1, 2, 3, 4]),
                Datum::Null,
            ],
        );
        let bytes = tuple.serialize();
        let decoded = Tuple::deserialize(&bytes).unwrap();
        assert_eq!(tuple, decoded);
    }

    #[test]
    fn empty_tuple() {
        let tuple = Tuple::new(MvccHeader::new_insert(0, 0), vec![]);
        let bytes = tuple.serialize();
        let decoded = Tuple::deserialize(&bytes).unwrap();
        assert_eq!(tuple, decoded);
        assert_eq!(decoded.column_count(), 0);
    }

    #[test]
    fn all_nulls() {
        let tuple = Tuple::new(
            MvccHeader::new_insert(5, 1),
            vec![Datum::Null, Datum::Null, Datum::Null],
        );
        let bytes = tuple.serialize();
        let decoded = Tuple::deserialize(&bytes).unwrap();
        assert_eq!(tuple, decoded);
    }

    #[test]
    fn mvcc_header() {
        let h = MvccHeader::new(10, 20, 3);
        assert!(h.is_deleted());

        let live = MvccHeader::new_insert(10, 0);
        assert!(!live.is_deleted());

        let mut buf = Vec::new();
        h.serialize(&mut buf);
        let mut offset = 0;
        let decoded = MvccHeader::deserialize(&buf, &mut offset).unwrap();
        assert_eq!(h, decoded);
    }

    #[test]
    fn get_column() {
        let t = sample_tuple();
        assert_eq!(t.get(0), Some(&Datum::Integer(42)));
        assert_eq!(t.get(2), Some(&Datum::Null));
        assert_eq!(t.get(99), None);
    }

    #[test]
    fn null_bitmap_many_columns() {
        // 10 columns — exercises the second byte of the bitmap.
        let data: Vec<Datum> = (0..10)
            .map(|i| {
                if i % 3 == 0 {
                    Datum::Null
                } else {
                    Datum::Integer(i)
                }
            })
            .collect();
        let tuple = Tuple::new(MvccHeader::new_insert(1, 0), data);
        let bytes = tuple.serialize();
        let decoded = Tuple::deserialize(&bytes).unwrap();
        assert_eq!(tuple, decoded);
    }
}
