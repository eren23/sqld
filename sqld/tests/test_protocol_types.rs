use std::io::Cursor;

use sqld::protocol::messages::*;
use sqld::types::*;

// ===========================================================================
// Tests — datum_to_text
// ===========================================================================

#[test]
fn test_datum_to_text_integer() {
    assert_eq!(datum_to_text(&Datum::Integer(42)), Some(b"42".to_vec()));
    assert_eq!(datum_to_text(&Datum::Integer(-1)), Some(b"-1".to_vec()));
    assert_eq!(datum_to_text(&Datum::Integer(0)), Some(b"0".to_vec()));
}

#[test]
fn test_datum_to_text_bigint() {
    assert_eq!(
        datum_to_text(&Datum::BigInt(9999999999)),
        Some(b"9999999999".to_vec())
    );
    assert_eq!(
        datum_to_text(&Datum::BigInt(-9999999999)),
        Some(b"-9999999999".to_vec())
    );
}

#[test]
fn test_datum_to_text_float() {
    let result = datum_to_text(&Datum::Float(3.14)).unwrap();
    let text = String::from_utf8(result).unwrap();
    let parsed: f64 = text.parse().unwrap();
    assert!((parsed - 3.14).abs() < f64::EPSILON);

    // NaN
    assert_eq!(datum_to_text(&Datum::Float(f64::NAN)), Some(b"NaN".to_vec()));

    // Infinity
    assert_eq!(
        datum_to_text(&Datum::Float(f64::INFINITY)),
        Some(b"Infinity".to_vec())
    );
    assert_eq!(
        datum_to_text(&Datum::Float(f64::NEG_INFINITY)),
        Some(b"-Infinity".to_vec())
    );
}

#[test]
fn test_datum_to_text_boolean() {
    assert_eq!(datum_to_text(&Datum::Boolean(true)), Some(b"t".to_vec()));
    assert_eq!(datum_to_text(&Datum::Boolean(false)), Some(b"f".to_vec()));
}

#[test]
fn test_datum_to_text_varchar() {
    assert_eq!(
        datum_to_text(&Datum::Varchar("hello".to_string())),
        Some(b"hello".to_vec())
    );
}

#[test]
fn test_datum_to_text_text() {
    assert_eq!(
        datum_to_text(&Datum::Text("world".to_string())),
        Some(b"world".to_vec())
    );
}

#[test]
fn test_datum_to_text_timestamp() {
    // 1_000_000 microseconds = 1 second since epoch
    let result = datum_to_text(&Datum::Timestamp(1_000_000)).unwrap();
    let text = String::from_utf8(result).unwrap();
    assert!(text.contains("1"), "timestamp text should contain the seconds");
}

#[test]
fn test_datum_to_text_date() {
    // Day 0 = Unix epoch
    assert_eq!(datum_to_text(&Datum::Date(0)), Some(b"0".to_vec()));
    assert_eq!(datum_to_text(&Datum::Date(19000)), Some(b"19000".to_vec()));
}

#[test]
fn test_datum_to_text_decimal() {
    let d = Datum::Decimal {
        mantissa: 12345,
        scale: 2,
    };
    assert_eq!(datum_to_text(&d), Some(b"123.45".to_vec()));

    let d0 = Datum::Decimal {
        mantissa: 42,
        scale: 0,
    };
    assert_eq!(datum_to_text(&d0), Some(b"42".to_vec()));
}

#[test]
fn test_datum_to_text_blob() {
    let d = Datum::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]);
    assert_eq!(datum_to_text(&d), Some(b"\\xdeadbeef".to_vec()));
}

#[test]
fn test_datum_to_text_null() {
    assert!(datum_to_text(&Datum::Null).is_none());
}

// ===========================================================================
// Tests — datum_to_binary
// ===========================================================================

#[test]
fn test_datum_to_binary_integer() {
    let result = datum_to_binary(&Datum::Integer(42)).unwrap();
    assert_eq!(result.len(), 4);
    assert_eq!(result, 42i32.to_be_bytes().to_vec());
}

#[test]
fn test_datum_to_binary_bigint() {
    let result = datum_to_binary(&Datum::BigInt(9999999999)).unwrap();
    assert_eq!(result.len(), 8);
    assert_eq!(result, 9999999999i64.to_be_bytes().to_vec());
}

#[test]
fn test_datum_to_binary_float() {
    let result = datum_to_binary(&Datum::Float(3.14)).unwrap();
    assert_eq!(result.len(), 8);
    assert_eq!(result, 3.14f64.to_be_bytes().to_vec());
}

#[test]
fn test_datum_to_binary_boolean() {
    assert_eq!(datum_to_binary(&Datum::Boolean(true)).unwrap(), vec![1u8]);
    assert_eq!(datum_to_binary(&Datum::Boolean(false)).unwrap(), vec![0u8]);
}

#[test]
fn test_datum_to_binary_timestamp() {
    // PG epoch offset: 946_684_800_000_000 microseconds
    let unix_micros: i64 = 1_000_000_000_000_000; // some future timestamp
    let expected_pg_micros = unix_micros - 946_684_800_000_000i64;

    let result = datum_to_binary(&Datum::Timestamp(unix_micros)).unwrap();
    assert_eq!(result.len(), 8);
    assert_eq!(result, expected_pg_micros.to_be_bytes().to_vec());
}

#[test]
fn test_datum_to_binary_date() {
    // PG epoch offset: day 10957
    let unix_days: i32 = 20000;
    let expected_pg_days = unix_days - 10957;

    let result = datum_to_binary(&Datum::Date(unix_days)).unwrap();
    assert_eq!(result.len(), 4);
    assert_eq!(result, expected_pg_days.to_be_bytes().to_vec());
}

// ===========================================================================
// Tests — text_to_datum roundtrip
// ===========================================================================

#[test]
fn test_text_to_datum_roundtrip() {
    let cases: Vec<(Datum, DataType)> = vec![
        (Datum::Integer(42), DataType::Integer),
        (Datum::BigInt(-1000), DataType::BigInt),
        (Datum::Float(3.14), DataType::Float),
        (Datum::Boolean(true), DataType::Boolean),
        (Datum::Varchar("hello".into()), DataType::Varchar(255)),
        (Datum::Text("world".into()), DataType::Text),
    ];

    for (datum, dt) in &cases {
        let text = datum_to_text(datum).unwrap();
        let roundtrip = text_to_datum(&text, dt).unwrap();
        assert_eq!(
            format!("{datum}"),
            format!("{roundtrip}"),
            "roundtrip failed for {dt}"
        );
    }
}

#[test]
fn test_text_to_datum_boolean_variants() {
    let true_inputs = ["true", "t", "1", "yes", "on"];
    let false_inputs = ["false", "f", "0", "no", "off"];

    for input in &true_inputs {
        let result = text_to_datum(input.as_bytes(), &DataType::Boolean).unwrap();
        assert_eq!(result, Datum::Boolean(true), "input '{input}' should be true");
    }

    for input in &false_inputs {
        let result = text_to_datum(input.as_bytes(), &DataType::Boolean).unwrap();
        assert_eq!(result, Datum::Boolean(false), "input '{input}' should be false");
    }

    // Invalid boolean input
    let result = text_to_datum(b"maybe", &DataType::Boolean);
    assert!(result.is_err(), "expected error for invalid boolean");
}

// ===========================================================================
// Tests — pg_type_mapping
// ===========================================================================

#[test]
fn test_pg_type_mapping() {
    assert_eq!(pg_type_info(&DataType::Integer), (23, 4));
    assert_eq!(pg_type_info(&DataType::BigInt), (20, 8));
    assert_eq!(pg_type_info(&DataType::Float), (701, 8));
    assert_eq!(pg_type_info(&DataType::Boolean), (16, 1));
    assert_eq!(pg_type_info(&DataType::Varchar(255)), (1043, -1));
    assert_eq!(pg_type_info(&DataType::Text), (25, -1));
    assert_eq!(pg_type_info(&DataType::Timestamp), (1114, 8));
    assert_eq!(pg_type_info(&DataType::Date), (1082, 4));
    assert_eq!(pg_type_info(&DataType::Decimal(38, 6)), (1700, -1));
    assert_eq!(pg_type_info(&DataType::Blob), (17, -1));

    // Verify reverse mapping
    assert_eq!(pg_oid_to_datatype(23), Some(DataType::Integer));
    assert_eq!(pg_oid_to_datatype(20), Some(DataType::BigInt));
    assert_eq!(pg_oid_to_datatype(701), Some(DataType::Float));
    assert_eq!(pg_oid_to_datatype(16), Some(DataType::Boolean));
    assert_eq!(pg_oid_to_datatype(1043), Some(DataType::Varchar(255)));
    assert_eq!(pg_oid_to_datatype(25), Some(DataType::Text));
    assert_eq!(pg_oid_to_datatype(1114), Some(DataType::Timestamp));
    assert_eq!(pg_oid_to_datatype(1082), Some(DataType::Date));
    assert_eq!(pg_oid_to_datatype(1700), Some(DataType::Decimal(38, 6)));
    assert_eq!(pg_oid_to_datatype(17), Some(DataType::Blob));
}

#[test]
fn test_pg_oid_to_datatype_unknown() {
    assert_eq!(pg_oid_to_datatype(0), None);
    assert_eq!(pg_oid_to_datatype(99999), None);
    assert_eq!(pg_oid_to_datatype(-1), None);
}

// ===========================================================================
// Tests — message encoding roundtrip
// ===========================================================================

#[test]
fn test_message_encoding_roundtrip() {
    // ReadyForQuery
    let msg = BackendMessage::ReadyForQuery {
        state: TransactionState::Idle,
    };
    let mut buf = Vec::new();
    msg.encode(&mut buf).unwrap();
    assert_eq!(buf[0], b'Z');
    assert_eq!(&buf[1..5], &5i32.to_be_bytes());
    assert_eq!(buf[5], b'I');

    // ReadyForQuery - InBlock
    let mut buf = Vec::new();
    BackendMessage::ReadyForQuery {
        state: TransactionState::InBlock,
    }
    .encode(&mut buf)
    .unwrap();
    assert_eq!(buf[5], b'T');

    // ReadyForQuery - Failed
    let mut buf = Vec::new();
    BackendMessage::ReadyForQuery {
        state: TransactionState::Failed,
    }
    .encode(&mut buf)
    .unwrap();
    assert_eq!(buf[5], b'E');

    // RowDescription
    let fields = vec![
        FieldDescription::new("id", &DataType::Integer),
        FieldDescription::new("name", &DataType::Text),
    ];
    let mut buf = Vec::new();
    BackendMessage::RowDescription { fields }.encode(&mut buf).unwrap();
    assert_eq!(buf[0], b'T');

    // DataRow
    let mut buf = Vec::new();
    BackendMessage::DataRow {
        values: vec![Some(b"42".to_vec()), Some(b"hello".to_vec()), None],
    }
    .encode(&mut buf)
    .unwrap();
    assert_eq!(buf[0], b'D');

    // CommandComplete
    let mut buf = Vec::new();
    BackendMessage::CommandComplete {
        tag: "SELECT 5".to_string(),
    }
    .encode(&mut buf)
    .unwrap();
    assert_eq!(buf[0], b'C');
    let tag_bytes = b"SELECT 5\0";
    assert!(buf.windows(tag_bytes.len()).any(|w| w == tag_bytes));

    // ErrorResponse
    let fields = ErrorFields::new(Severity::Error, "42601", "syntax error");
    let mut buf = Vec::new();
    BackendMessage::ErrorResponse(fields).encode(&mut buf).unwrap();
    assert_eq!(buf[0], b'E');

    // ParseComplete
    let mut buf = Vec::new();
    BackendMessage::ParseComplete.encode(&mut buf).unwrap();
    assert_eq!(buf[0], b'1');
    assert_eq!(&buf[1..5], &4i32.to_be_bytes());

    // BindComplete
    let mut buf = Vec::new();
    BackendMessage::BindComplete.encode(&mut buf).unwrap();
    assert_eq!(buf[0], b'2');
    assert_eq!(&buf[1..5], &4i32.to_be_bytes());
}

// ===========================================================================
// Tests — frontend message decode
// ===========================================================================

/// Helper: encode a frontend message with type byte, length, and payload.
fn encode_frontend(type_byte: u8, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(type_byte);
    let len = (4 + payload.len()) as i32;
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(payload);
    buf
}

/// Helper: write a null-terminated C string into a buffer.
fn write_cstring(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(s.as_bytes());
    buf.push(0);
}

#[test]
fn test_frontend_message_decode_query() {
    let mut payload = Vec::new();
    write_cstring(&mut payload, "SELECT 1");
    let buf = encode_frontend(b'Q', &payload);

    let mut cursor = Cursor::new(buf);
    let msg = read_frontend_message(&mut cursor).unwrap();
    match msg {
        FrontendMessage::Query { sql } => assert_eq!(sql, "SELECT 1"),
        other => panic!("expected Query, got {other:?}"),
    }
}

#[test]
fn test_frontend_message_decode_parse() {
    let mut payload = Vec::new();
    write_cstring(&mut payload, "stmt1");
    write_cstring(&mut payload, "SELECT $1");
    payload.extend_from_slice(&1i16.to_be_bytes()); // 1 param type
    payload.extend_from_slice(&23i32.to_be_bytes()); // int4

    let buf = encode_frontend(b'P', &payload);
    let mut cursor = Cursor::new(buf);
    let msg = read_frontend_message(&mut cursor).unwrap();
    match msg {
        FrontendMessage::Parse {
            name,
            query,
            param_types,
        } => {
            assert_eq!(name, "stmt1");
            assert_eq!(query, "SELECT $1");
            assert_eq!(param_types, vec![23]);
        }
        other => panic!("expected Parse, got {other:?}"),
    }
}

#[test]
fn test_frontend_message_decode_bind() {
    let mut payload = Vec::new();
    write_cstring(&mut payload, "portal1");
    write_cstring(&mut payload, "stmt1");
    // 1 parameter format
    payload.extend_from_slice(&1i16.to_be_bytes());
    payload.extend_from_slice(&0i16.to_be_bytes()); // text format
    // 1 parameter value
    payload.extend_from_slice(&1i16.to_be_bytes());
    let val = b"42";
    payload.extend_from_slice(&(val.len() as i32).to_be_bytes());
    payload.extend_from_slice(val);
    // 0 result formats
    payload.extend_from_slice(&0i16.to_be_bytes());

    let buf = encode_frontend(b'B', &payload);
    let mut cursor = Cursor::new(buf);
    let msg = read_frontend_message(&mut cursor).unwrap();
    match msg {
        FrontendMessage::Bind {
            portal,
            statement,
            param_formats,
            param_values,
            result_formats,
        } => {
            assert_eq!(portal, "portal1");
            assert_eq!(statement, "stmt1");
            assert_eq!(param_formats, vec![0]);
            assert_eq!(param_values, vec![Some(b"42".to_vec())]);
            assert!(result_formats.is_empty());
        }
        other => panic!("expected Bind, got {other:?}"),
    }
}

#[test]
fn test_frontend_message_decode_describe() {
    // Describe Statement
    let mut payload = Vec::new();
    payload.push(b'S');
    write_cstring(&mut payload, "my_stmt");
    let buf = encode_frontend(b'D', &payload);
    let mut cursor = Cursor::new(buf);
    let msg = read_frontend_message(&mut cursor).unwrap();
    match msg {
        FrontendMessage::Describe { target, name } => {
            assert_eq!(target, DescribeTarget::Statement);
            assert_eq!(name, "my_stmt");
        }
        other => panic!("expected Describe, got {other:?}"),
    }

    // Describe Portal
    let mut payload = Vec::new();
    payload.push(b'P');
    write_cstring(&mut payload, "my_portal");
    let buf = encode_frontend(b'D', &payload);
    let mut cursor = Cursor::new(buf);
    let msg = read_frontend_message(&mut cursor).unwrap();
    match msg {
        FrontendMessage::Describe { target, name } => {
            assert_eq!(target, DescribeTarget::Portal);
            assert_eq!(name, "my_portal");
        }
        other => panic!("expected Describe, got {other:?}"),
    }
}

#[test]
fn test_frontend_message_decode_execute() {
    let mut payload = Vec::new();
    write_cstring(&mut payload, "p1");
    payload.extend_from_slice(&100i32.to_be_bytes()); // max_rows

    let buf = encode_frontend(b'E', &payload);
    let mut cursor = Cursor::new(buf);
    let msg = read_frontend_message(&mut cursor).unwrap();
    match msg {
        FrontendMessage::Execute { portal, max_rows } => {
            assert_eq!(portal, "p1");
            assert_eq!(max_rows, 100);
        }
        other => panic!("expected Execute, got {other:?}"),
    }
}

#[test]
fn test_frontend_message_decode_sync() {
    let buf = encode_frontend(b'S', &[]);
    let mut cursor = Cursor::new(buf);
    let msg = read_frontend_message(&mut cursor).unwrap();
    assert!(matches!(msg, FrontendMessage::Sync));
}

#[test]
fn test_frontend_message_decode_terminate() {
    let buf = encode_frontend(b'X', &[]);
    let mut cursor = Cursor::new(buf);
    let msg = read_frontend_message(&mut cursor).unwrap();
    assert!(matches!(msg, FrontendMessage::Terminate));
}

// ===========================================================================
// Tests — startup message
// ===========================================================================

#[test]
fn test_startup_message_decode() {
    let mut buf = Vec::new();
    let params = b"user\0test\0database\0mydb\0\0";
    let len = (8 + params.len()) as i32;
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&PROTOCOL_VERSION_3.to_be_bytes());
    buf.extend_from_slice(params);

    let mut cursor = Cursor::new(buf);
    let msg = read_startup_message(&mut cursor).unwrap();
    match msg {
        FrontendMessage::Startup { version, params } => {
            assert_eq!(version, PROTOCOL_VERSION_3);
            assert_eq!(params.get("user").unwrap(), "test");
            assert_eq!(params.get("database").unwrap(), "mydb");
        }
        other => panic!("expected Startup, got {other:?}"),
    }
}

#[test]
fn test_ssl_request_decode() {
    let mut buf = Vec::new();
    let len: i32 = 8;
    buf.extend_from_slice(&len.to_be_bytes());
    let ssl_code: i32 = 80877103;
    buf.extend_from_slice(&ssl_code.to_be_bytes());

    let mut cursor = Cursor::new(buf);
    let msg = read_startup_message(&mut cursor).unwrap();
    assert!(matches!(msg, FrontendMessage::SslRequest));
}
