use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use sqld::types::{Datum, DataType};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn hash_datum(d: &Datum) -> u64 {
    let mut h = DefaultHasher::new();
    d.hash(&mut h);
    h.finish()
}

// ===========================================================================
// Equality
// ===========================================================================

#[test]
fn eq_same_type_same_value() {
    assert_eq!(Datum::Integer(42), Datum::Integer(42));
    assert_eq!(Datum::BigInt(100), Datum::BigInt(100));
    assert_eq!(Datum::Float(1.5), Datum::Float(1.5));
    assert_eq!(Datum::Boolean(true), Datum::Boolean(true));
    assert_eq!(Datum::Varchar("abc".into()), Datum::Varchar("abc".into()));
    assert_eq!(Datum::Text("abc".into()), Datum::Text("abc".into()));
    assert_eq!(Datum::Timestamp(1000), Datum::Timestamp(1000));
    assert_eq!(Datum::Date(100), Datum::Date(100));
    assert_eq!(Datum::Blob(vec![1, 2]), Datum::Blob(vec![1, 2]));
    assert_eq!(Datum::Null, Datum::Null);
}

#[test]
fn eq_different_values() {
    assert_ne!(Datum::Integer(1), Datum::Integer(2));
    assert_ne!(Datum::Varchar("a".into()), Datum::Varchar("b".into()));
}

#[test]
fn eq_different_types_not_equal() {
    assert_ne!(Datum::Integer(1), Datum::BigInt(1));
    assert_ne!(Datum::Varchar("hi".into()), Datum::Text("hi".into()));
    assert_ne!(Datum::Integer(0), Datum::Null);
}

#[test]
fn decimal_eq_different_scale() {
    // 1.20 (mantissa=120, scale=2) == 1.2 (mantissa=12, scale=1)
    let a = Datum::Decimal { mantissa: 120, scale: 2 };
    let b = Datum::Decimal { mantissa: 12, scale: 1 };
    assert_eq!(a, b);

    // 5.000 == 5.0
    let c = Datum::Decimal { mantissa: 5000, scale: 3 };
    let d = Datum::Decimal { mantissa: 50, scale: 1 };
    assert_eq!(c, d);
}

// ===========================================================================
// Hashing
// ===========================================================================

#[test]
fn hash_equal_values_same_hash() {
    assert_eq!(hash_datum(&Datum::Integer(42)), hash_datum(&Datum::Integer(42)));
    assert_eq!(hash_datum(&Datum::Null), hash_datum(&Datum::Null));
}

#[test]
fn hash_different_values_likely_different() {
    assert_ne!(hash_datum(&Datum::Integer(1)), hash_datum(&Datum::Integer(2)));
}

#[test]
fn hash_decimal_normalized() {
    // 1.20 and 1.2 must hash the same since they're equal
    let a = Datum::Decimal { mantissa: 120, scale: 2 };
    let b = Datum::Decimal { mantissa: 12, scale: 1 };
    assert_eq!(hash_datum(&a), hash_datum(&b));
}

#[test]
fn datum_usable_as_hashmap_key() {
    let mut map = HashMap::new();
    map.insert(Datum::Integer(1), "one");
    map.insert(Datum::Varchar("hello".into()), "greeting");
    map.insert(Datum::Null, "nothing");
    assert_eq!(map.get(&Datum::Integer(1)), Some(&"one"));
    assert_eq!(map.get(&Datum::Varchar("hello".into())), Some(&"greeting"));
    assert_eq!(map.get(&Datum::Null), Some(&"nothing"));
}

// ===========================================================================
// Comparison (sql_cmp with coercion)
// ===========================================================================

#[test]
fn sql_cmp_same_type() {
    let a = Datum::Integer(10);
    let b = Datum::Integer(20);
    assert_eq!(a.sql_cmp(&b).unwrap(), Some(Ordering::Less));
    assert_eq!(b.sql_cmp(&a).unwrap(), Some(Ordering::Greater));
    assert_eq!(a.sql_cmp(&a).unwrap(), Some(Ordering::Equal));
}

#[test]
fn sql_cmp_cross_type_int_bigint() {
    let a = Datum::Integer(10);
    let b = Datum::BigInt(20);
    assert_eq!(a.sql_cmp(&b).unwrap(), Some(Ordering::Less));
}

#[test]
fn sql_cmp_cross_type_int_float() {
    let a = Datum::Integer(5);
    let b = Datum::Float(5.0);
    assert_eq!(a.sql_cmp(&b).unwrap(), Some(Ordering::Equal));
}

#[test]
fn sql_cmp_cross_type_date_timestamp() {
    let a = Datum::Date(1); // day 1 -> 86_400_000_000 micros
    let b = Datum::Timestamp(86_400_000_000);
    assert_eq!(a.sql_cmp(&b).unwrap(), Some(Ordering::Equal));
}

#[test]
fn sql_cmp_null_propagation() {
    let a = Datum::Integer(1);
    assert_eq!(a.sql_cmp(&Datum::Null).unwrap(), None);
    assert_eq!(Datum::Null.sql_cmp(&a).unwrap(), None);
    assert_eq!(Datum::Null.sql_cmp(&Datum::Null).unwrap(), None);
}

#[test]
fn sql_cmp_strings() {
    let a = Datum::Varchar("apple".into());
    let b = Datum::Varchar("banana".into());
    assert_eq!(a.sql_cmp(&b).unwrap(), Some(Ordering::Less));
}

#[test]
fn sql_cmp_incompatible_is_error() {
    let a = Datum::Integer(1);
    let b = Datum::Boolean(true);
    assert!(a.sql_cmp(&b).is_err());
}

// ===========================================================================
// Coercion
// ===========================================================================

#[test]
fn coerce_integer_to_bigint() {
    let d = Datum::Integer(42);
    let c = d.coerce_to(&DataType::BigInt).unwrap();
    assert_eq!(c, Datum::BigInt(42));
}

#[test]
fn coerce_integer_to_decimal() {
    let d = Datum::Integer(7);
    let c = d.coerce_to(&DataType::Decimal(10, 2)).unwrap();
    assert_eq!(c, Datum::Decimal { mantissa: 700, scale: 2 });
}

#[test]
fn coerce_integer_to_float() {
    let d = Datum::Integer(42);
    let c = d.coerce_to(&DataType::Float).unwrap();
    assert_eq!(c, Datum::Float(42.0));
}

#[test]
fn coerce_bigint_to_float() {
    let d = Datum::BigInt(100);
    let c = d.coerce_to(&DataType::Float).unwrap();
    assert_eq!(c, Datum::Float(100.0));
}

#[test]
fn coerce_varchar_to_text() {
    let d = Datum::Varchar("hello".into());
    let c = d.coerce_to(&DataType::Text).unwrap();
    assert_eq!(c, Datum::Text("hello".into()));
}

#[test]
fn coerce_date_to_timestamp() {
    let d = Datum::Date(1);
    let c = d.coerce_to(&DataType::Timestamp).unwrap();
    assert_eq!(c, Datum::Timestamp(86_400_000_000));
}

#[test]
fn coerce_null_to_anything() {
    assert_eq!(Datum::Null.coerce_to(&DataType::Integer).unwrap(), Datum::Null);
    assert_eq!(Datum::Null.coerce_to(&DataType::Text).unwrap(), Datum::Null);
}

#[test]
fn coerce_same_type_returns_clone() {
    let d = Datum::Integer(42);
    let c = d.coerce_to(&DataType::Integer).unwrap();
    assert_eq!(c, d);
}

#[test]
fn coerce_invalid_fails() {
    assert!(Datum::Boolean(true).coerce_to(&DataType::Integer).is_err());
    assert!(Datum::Integer(1).coerce_to(&DataType::Boolean).is_err());
    assert!(Datum::Float(1.0).coerce_to(&DataType::Integer).is_err()); // no narrowing
    assert!(Datum::Text("hi".into()).coerce_to(&DataType::Integer).is_err());
}

// ===========================================================================
// Arithmetic
// ===========================================================================

#[test]
fn add_integers() {
    let a = Datum::Integer(10);
    let b = Datum::Integer(3);
    assert_eq!(a.add(&b).unwrap(), Datum::Integer(13));
}

#[test]
fn sub_integers() {
    let a = Datum::Integer(10);
    let b = Datum::Integer(3);
    assert_eq!(a.sub(&b).unwrap(), Datum::Integer(7));
}

#[test]
fn mul_integers() {
    let a = Datum::Integer(10);
    let b = Datum::Integer(3);
    assert_eq!(a.mul(&b).unwrap(), Datum::Integer(30));
}

#[test]
fn div_integers() {
    let a = Datum::Integer(10);
    let b = Datum::Integer(3);
    assert_eq!(a.div(&b).unwrap(), Datum::Integer(3));
}

#[test]
fn arithmetic_cross_type_promotes() {
    // int + float -> float
    let a = Datum::Integer(10);
    let b = Datum::Float(2.5);
    match a.add(&b).unwrap() {
        Datum::Float(v) => assert!((v - 12.5).abs() < f64::EPSILON),
        other => panic!("expected Float, got {other:?}"),
    }
}

#[test]
fn arithmetic_bigint() {
    let a = Datum::BigInt(i64::MAX - 1);
    let b = Datum::BigInt(1);
    assert_eq!(a.add(&b).unwrap(), Datum::BigInt(i64::MAX));
}

#[test]
fn arithmetic_null_propagation() {
    let a = Datum::Integer(10);
    assert!(a.add(&Datum::Null).unwrap().is_null());
    assert!(Datum::Null.sub(&a).unwrap().is_null());
    assert!(Datum::Null.mul(&Datum::Null).unwrap().is_null());
}

#[test]
fn division_by_zero() {
    assert!(Datum::Integer(1).div(&Datum::Integer(0)).is_err());
    assert!(Datum::Float(1.0).div(&Datum::Float(0.0)).is_err());
    assert!(Datum::BigInt(1).div(&Datum::BigInt(0)).is_err());
}

#[test]
fn negate() {
    assert_eq!(Datum::Integer(5).neg().unwrap(), Datum::Integer(-5));
    assert_eq!(Datum::BigInt(100).neg().unwrap(), Datum::BigInt(-100));
    assert_eq!(Datum::Float(1.5).neg().unwrap(), Datum::Float(-1.5));
    assert!(Datum::Null.neg().unwrap().is_null());
    assert!(Datum::Boolean(true).neg().is_err());
}

#[test]
fn integer_overflow() {
    let a = Datum::Integer(i32::MAX);
    let b = Datum::Integer(1);
    assert!(a.add(&b).is_err());
}

// ===========================================================================
// Serialization round-trip
// ===========================================================================

#[test]
fn serialize_roundtrip_all_variants() {
    let datums = vec![
        Datum::Null,
        Datum::Integer(-42),
        Datum::Integer(i32::MAX),
        Datum::BigInt(i64::MAX),
        Datum::BigInt(i64::MIN),
        Datum::Float(3.14),
        Datum::Float(0.0),
        Datum::Float(f64::NEG_INFINITY),
        Datum::Boolean(true),
        Datum::Boolean(false),
        Datum::Varchar("hello".into()),
        Datum::Varchar(String::new()),
        Datum::Text("world of text".into()),
        Datum::Timestamp(1_000_000),
        Datum::Timestamp(0),
        Datum::Date(19000),
        Datum::Date(-100),
        Datum::Decimal { mantissa: 12345, scale: 2 },
        Datum::Decimal { mantissa: 0, scale: 0 },
        Datum::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
        Datum::Blob(vec![]),
    ];

    let mut buf = Vec::new();
    for d in &datums {
        d.serialize(&mut buf);
    }

    let mut offset = 0;
    for original in &datums {
        let decoded = Datum::deserialize(&buf, &mut offset).unwrap();
        assert_eq!(&decoded, original, "round-trip mismatch for {original:?}");
    }
    assert_eq!(offset, buf.len(), "trailing bytes after deserialization");
}

#[test]
fn deserialize_truncated_buffer_is_error() {
    let d = Datum::Integer(42);
    let mut buf = Vec::new();
    d.serialize(&mut buf);
    // Truncate to just the tag byte
    assert!(Datum::deserialize(&buf[..1], &mut 0).is_err());
}

#[test]
fn deserialize_empty_buffer_is_error() {
    let empty: &[u8] = &[];
    assert!(Datum::deserialize(empty, &mut 0).is_err());
}

// ===========================================================================
// PartialOrd (strict same-type)
// ===========================================================================

#[test]
fn partial_ord_same_type() {
    assert!(Datum::Integer(1) < Datum::Integer(2));
    assert!(Datum::BigInt(100) > Datum::BigInt(50));
    assert!(Datum::Text("a".into()) < Datum::Text("b".into()));
    assert!(Datum::Date(10) < Datum::Date(20));
}

#[test]
fn partial_ord_cross_type_is_none() {
    assert_eq!(Datum::Integer(1).partial_cmp(&Datum::BigInt(1)), None);
    assert_eq!(Datum::Varchar("a".into()).partial_cmp(&Datum::Text("a".into())), None);
}

// ===========================================================================
// Display
// ===========================================================================

#[test]
fn display_formatting() {
    assert_eq!(format!("{}", Datum::Null), "NULL");
    assert_eq!(format!("{}", Datum::Integer(42)), "42");
    assert_eq!(format!("{}", Datum::Boolean(true)), "true");
    assert_eq!(format!("{}", Datum::Varchar("hi".into())), "hi");
}

// ===========================================================================
// Introspection
// ===========================================================================

#[test]
fn data_type_introspection() {
    assert_eq!(Datum::Integer(0).data_type(), Some(DataType::Integer));
    assert_eq!(Datum::BigInt(0).data_type(), Some(DataType::BigInt));
    assert_eq!(Datum::Float(0.0).data_type(), Some(DataType::Float));
    assert_eq!(Datum::Boolean(true).data_type(), Some(DataType::Boolean));
    assert_eq!(Datum::Timestamp(0).data_type(), Some(DataType::Timestamp));
    assert_eq!(Datum::Date(0).data_type(), Some(DataType::Date));
    assert_eq!(Datum::Blob(vec![]).data_type(), Some(DataType::Blob));
    assert_eq!(Datum::Null.data_type(), None);
}

#[test]
fn is_null() {
    assert!(Datum::Null.is_null());
    assert!(!Datum::Integer(0).is_null());
}

#[test]
fn type_name() {
    assert_eq!(Datum::Integer(0).type_name(), "INTEGER");
    assert_eq!(Datum::Null.type_name(), "NULL");
    assert_eq!(Datum::Blob(vec![]).type_name(), "BLOB");
}
