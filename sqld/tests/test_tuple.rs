use sqld::types::datum::Datum;
use sqld::types::tuple::{MvccHeader, Tuple};

// ===========================================================================
// MvccHeader
// ===========================================================================

#[test]
fn mvcc_new_insert_is_live() {
    let h = MvccHeader::new_insert(1, 0);
    assert_eq!(h.xmin, 1);
    assert_eq!(h.xmax, 0);
    assert_eq!(h.cid, 0);
    assert!(!h.is_deleted());
}

#[test]
fn mvcc_deleted_when_xmax_nonzero() {
    let h = MvccHeader::new(1, 2, 0);
    assert!(h.is_deleted());
}

#[test]
fn mvcc_header_roundtrip() {
    let h = MvccHeader::new(u64::MAX, u64::MAX - 1, u32::MAX);
    let mut buf = Vec::new();
    h.serialize(&mut buf);
    assert_eq!(buf.len(), 20);
    let mut offset = 0;
    let decoded = MvccHeader::deserialize(&buf, &mut offset).unwrap();
    assert_eq!(h, decoded);
    assert_eq!(offset, 20);
}

#[test]
fn mvcc_header_deserialize_truncated() {
    let buf = vec![0u8; 10]; // too short
    let mut offset = 0;
    assert!(MvccHeader::deserialize(&buf, &mut offset).is_err());
}

// ===========================================================================
// Tuple round-trip serialization
// ===========================================================================

#[test]
fn roundtrip_basic_tuple() {
    let tuple = Tuple::new(
        MvccHeader::new_insert(1, 0),
        vec![
            Datum::Integer(42),
            Datum::Varchar("hello".into()),
            Datum::Boolean(true),
        ],
    );
    let bytes = tuple.serialize();
    let decoded = Tuple::deserialize(&bytes).unwrap();
    assert_eq!(tuple, decoded);
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
            Datum::Decimal { mantissa: 99999, scale: 3 },
            Datum::Blob(vec![1, 2, 3, 4]),
            Datum::Null,
        ],
    );
    let bytes = tuple.serialize();
    let decoded = Tuple::deserialize(&bytes).unwrap();
    assert_eq!(tuple, decoded);
}

#[test]
fn roundtrip_empty_tuple() {
    let tuple = Tuple::new(MvccHeader::new_insert(0, 0), vec![]);
    let bytes = tuple.serialize();
    let decoded = Tuple::deserialize(&bytes).unwrap();
    assert_eq!(tuple, decoded);
    assert_eq!(decoded.column_count(), 0);
}

#[test]
fn roundtrip_all_nulls() {
    let tuple = Tuple::new(
        MvccHeader::new_insert(5, 1),
        vec![Datum::Null, Datum::Null, Datum::Null],
    );
    let bytes = tuple.serialize();
    let decoded = Tuple::deserialize(&bytes).unwrap();
    assert_eq!(tuple, decoded);
}

#[test]
fn roundtrip_single_value() {
    let tuple = Tuple::new(
        MvccHeader::new_insert(1, 0),
        vec![Datum::BigInt(999)],
    );
    let bytes = tuple.serialize();
    let decoded = Tuple::deserialize(&bytes).unwrap();
    assert_eq!(tuple, decoded);
}

#[test]
fn roundtrip_single_null() {
    let tuple = Tuple::new(MvccHeader::new_insert(1, 0), vec![Datum::Null]);
    let bytes = tuple.serialize();
    let decoded = Tuple::deserialize(&bytes).unwrap();
    assert_eq!(tuple, decoded);
}

// ===========================================================================
// Null bitmap edge cases
// ===========================================================================

#[test]
fn null_bitmap_many_columns() {
    // 10 columns — exercises the second byte of the bitmap
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

#[test]
fn null_bitmap_exactly_8_columns() {
    // Exactly 1 bitmap byte
    let data: Vec<Datum> = (0..8)
        .map(|i| {
            if i % 2 == 0 {
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

#[test]
fn null_bitmap_16_columns() {
    // Exactly 2 bitmap bytes
    let data: Vec<Datum> = (0..16)
        .map(|i| {
            if i == 0 || i == 7 || i == 8 || i == 15 {
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

// ===========================================================================
// Accessor methods
// ===========================================================================

#[test]
fn get_valid_index() {
    let tuple = Tuple::new(
        MvccHeader::new_insert(1, 0),
        vec![Datum::Integer(10), Datum::Integer(20)],
    );
    assert_eq!(tuple.get(0), Some(&Datum::Integer(10)));
    assert_eq!(tuple.get(1), Some(&Datum::Integer(20)));
}

#[test]
fn get_out_of_bounds() {
    let tuple = Tuple::new(
        MvccHeader::new_insert(1, 0),
        vec![Datum::Integer(10)],
    );
    assert_eq!(tuple.get(1), None);
    assert_eq!(tuple.get(100), None);
}

#[test]
fn column_count() {
    let tuple = Tuple::new(
        MvccHeader::new_insert(1, 0),
        vec![Datum::Integer(1), Datum::Integer(2), Datum::Integer(3)],
    );
    assert_eq!(tuple.column_count(), 3);
}

#[test]
fn values_returns_all_data() {
    let data = vec![Datum::Integer(1), Datum::Null, Datum::Boolean(true)];
    let tuple = Tuple::new(MvccHeader::new_insert(1, 0), data.clone());
    assert_eq!(tuple.values(), &data[..]);
}

#[test]
fn into_values_consumes() {
    let data = vec![Datum::Integer(1), Datum::Integer(2)];
    let tuple = Tuple::new(MvccHeader::new_insert(1, 0), data.clone());
    let extracted = tuple.into_values();
    assert_eq!(extracted, data);
}

// ===========================================================================
// MVCC header preserved through serialization
// ===========================================================================

#[test]
fn mvcc_header_preserved() {
    let header = MvccHeader::new(42, 99, 7);
    let tuple = Tuple::new(header.clone(), vec![Datum::Integer(1)]);
    let bytes = tuple.serialize();
    let decoded = Tuple::deserialize(&bytes).unwrap();
    assert_eq!(decoded.header, header);
    assert_eq!(decoded.header.xmin, 42);
    assert_eq!(decoded.header.xmax, 99);
    assert_eq!(decoded.header.cid, 7);
}

// ===========================================================================
// Deserialization error cases
// ===========================================================================

#[test]
fn deserialize_too_short_for_header() {
    let buf = vec![0u8; 5];
    assert!(Tuple::deserialize(&buf).is_err());
}

#[test]
fn deserialize_too_short_for_column_count() {
    // 20 bytes for header, but no column count
    let buf = vec![0u8; 20];
    assert!(Tuple::deserialize(&buf).is_err());
}

// ===========================================================================
// Variable-length fields
// ===========================================================================

#[test]
fn roundtrip_large_varchar() {
    let long_string = "x".repeat(10_000);
    let tuple = Tuple::new(
        MvccHeader::new_insert(1, 0),
        vec![Datum::Varchar(long_string.clone())],
    );
    let bytes = tuple.serialize();
    let decoded = Tuple::deserialize(&bytes).unwrap();
    assert_eq!(decoded.get(0), Some(&Datum::Varchar(long_string)));
}

#[test]
fn roundtrip_large_blob() {
    let blob_data: Vec<u8> = (0..=255).cycle().take(5000).collect();
    let tuple = Tuple::new(
        MvccHeader::new_insert(1, 0),
        vec![Datum::Blob(blob_data.clone())],
    );
    let bytes = tuple.serialize();
    let decoded = Tuple::deserialize(&bytes).unwrap();
    assert_eq!(decoded.get(0), Some(&Datum::Blob(blob_data)));
}

#[test]
fn roundtrip_mixed_fixed_and_variable() {
    let tuple = Tuple::new(
        MvccHeader::new_insert(1, 0),
        vec![
            Datum::Integer(1),
            Datum::Varchar("short".into()),
            Datum::BigInt(2),
            Datum::Text("medium length text data".into()),
            Datum::Float(3.0),
            Datum::Blob(vec![0xFF; 100]),
            Datum::Null,
            Datum::Date(1000),
        ],
    );
    let bytes = tuple.serialize();
    let decoded = Tuple::deserialize(&bytes).unwrap();
    assert_eq!(tuple, decoded);
}
