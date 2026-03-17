use std::collections::HashMap;
use std::io::{self, Read, Write};

use crate::types::{DataType, Datum};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// PostgreSQL protocol version 3.0
pub const PROTOCOL_VERSION_3: i32 = 196608; // 3 << 16

/// SSL request code
pub const SSL_REQUEST_CODE: i32 = 80877103;

/// Cancel request code
pub const CANCEL_REQUEST_CODE: i32 = 80877102;

// ---------------------------------------------------------------------------
// Transaction state for ReadyForQuery
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Idle,       // 'I'
    InBlock,    // 'T'
    Failed,     // 'E'
}

impl TransactionState {
    pub fn as_byte(self) -> u8 {
        match self {
            TransactionState::Idle => b'I',
            TransactionState::InBlock => b'T',
            TransactionState::Failed => b'E',
        }
    }
}

// ---------------------------------------------------------------------------
// Error/Notice severity
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Error,
    Fatal,
    Panic,
    Warning,
    Notice,
    Debug,
    Info,
    Log,
}

impl Severity {
    pub fn as_str(self) -> &'static str {
        match self {
            Severity::Error => "ERROR",
            Severity::Fatal => "FATAL",
            Severity::Panic => "PANIC",
            Severity::Warning => "WARNING",
            Severity::Notice => "NOTICE",
            Severity::Debug => "DEBUG",
            Severity::Info => "INFO",
            Severity::Log => "LOG",
        }
    }
}

// ---------------------------------------------------------------------------
// Error fields
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ErrorFields {
    pub severity: Severity,
    pub code: String,     // SQLSTATE 5-char code
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub position: Option<i32>,
}

impl ErrorFields {
    pub fn new(severity: Severity, code: &str, message: impl Into<String>) -> Self {
        Self {
            severity,
            code: code.to_string(),
            message: message.into(),
            detail: None,
            hint: None,
            position: None,
        }
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    pub fn with_position(mut self, pos: i32) -> Self {
        self.position = Some(pos);
        self
    }

    /// Internal error (XX000)
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(Severity::Error, "XX000", message)
    }

    /// Syntax error (42601)
    pub fn syntax_error(message: impl Into<String>) -> Self {
        Self::new(Severity::Error, "42601", message)
    }

    /// Undefined table (42P01)
    pub fn undefined_table(message: impl Into<String>) -> Self {
        Self::new(Severity::Error, "42P01", message)
    }

    /// Undefined column (42703)
    pub fn undefined_column(message: impl Into<String>) -> Self {
        Self::new(Severity::Error, "42703", message)
    }

    /// Duplicate key (23505)
    pub fn unique_violation(message: impl Into<String>) -> Self {
        Self::new(Severity::Error, "23505", message)
    }

    /// Not-null violation (23502)
    pub fn not_null_violation(message: impl Into<String>) -> Self {
        Self::new(Severity::Error, "23502", message)
    }

    /// Foreign key violation (23503)
    pub fn foreign_key_violation(message: impl Into<String>) -> Self {
        Self::new(Severity::Error, "23503", message)
    }

    /// Check violation (23514)
    pub fn check_violation(message: impl Into<String>) -> Self {
        Self::new(Severity::Error, "23514", message)
    }

    /// Serialization failure (40001)
    pub fn serialization_failure(message: impl Into<String>) -> Self {
        Self::new(Severity::Error, "40001", message)
    }

    /// Deadlock detected (40P01)
    pub fn deadlock_detected(message: impl Into<String>) -> Self {
        Self::new(Severity::Error, "40P01", message)
    }

    /// Invalid transaction state (25000)
    pub fn invalid_transaction_state(message: impl Into<String>) -> Self {
        Self::new(Severity::Error, "25000", message)
    }

    /// Data exception (22000)
    pub fn data_exception(message: impl Into<String>) -> Self {
        Self::new(Severity::Error, "22000", message)
    }

    /// Division by zero (22012)
    pub fn division_by_zero() -> Self {
        Self::new(Severity::Error, "22012", "division by zero")
    }

    /// Feature not supported (0A000)
    pub fn feature_not_supported(message: impl Into<String>) -> Self {
        Self::new(Severity::Error, "0A000", message)
    }
}

// ---------------------------------------------------------------------------
// Column description (for RowDescription)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct FieldDescription {
    pub name: String,
    pub table_oid: i32,
    pub column_attr: i16,
    pub type_oid: i32,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format_code: i16, // 0 = text, 1 = binary
}

impl FieldDescription {
    pub fn new(name: impl Into<String>, data_type: &DataType) -> Self {
        let (type_oid, type_size) = pg_type_info(data_type);
        Self {
            name: name.into(),
            table_oid: 0,
            column_attr: 0,
            type_oid,
            type_size,
            type_modifier: -1,
            format_code: 0, // text format
        }
    }
}

/// Map our DataType to PostgreSQL type OID and size.
pub fn pg_type_info(dt: &DataType) -> (i32, i16) {
    match dt {
        DataType::Integer => (23, 4),      // int4
        DataType::BigInt => (20, 8),       // int8
        DataType::Float => (701, 8),       // float8
        DataType::Boolean => (16, 1),      // bool
        DataType::Varchar(_) => (1043, -1), // varchar
        DataType::Text => (25, -1),        // text
        DataType::Timestamp => (1114, 8),  // timestamp
        DataType::Date => (1082, 4),       // date
        DataType::Decimal(_, _) => (1700, -1), // numeric
        DataType::Blob => (17, -1),        // bytea
    }
}

/// Map PostgreSQL type OID to our DataType.
pub fn pg_oid_to_datatype(oid: i32) -> Option<DataType> {
    match oid {
        23 => Some(DataType::Integer),
        20 => Some(DataType::BigInt),
        701 => Some(DataType::Float),
        16 => Some(DataType::Boolean),
        1043 => Some(DataType::Varchar(255)),
        25 => Some(DataType::Text),
        1114 => Some(DataType::Timestamp),
        1082 => Some(DataType::Date),
        1700 => Some(DataType::Decimal(38, 6)),
        17 => Some(DataType::Blob),
        0 => None, // unspecified
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Frontend messages (client → server)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum FrontendMessage {
    /// Initial startup (no type byte)
    Startup {
        version: i32,
        params: HashMap<String, String>,
    },
    /// SSL negotiation request
    SslRequest,
    /// Cancel request
    CancelRequest {
        process_id: i32,
        secret_key: i32,
    },
    /// Simple query ('Q')
    Query {
        sql: String,
    },
    /// Parse a prepared statement ('P')
    Parse {
        name: String,
        query: String,
        param_types: Vec<i32>,
    },
    /// Bind parameters to a prepared statement ('B')
    Bind {
        portal: String,
        statement: String,
        param_formats: Vec<i16>,
        param_values: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    },
    /// Describe a statement or portal ('D')
    Describe {
        target: DescribeTarget,
        name: String,
    },
    /// Execute a portal ('E')
    Execute {
        portal: String,
        max_rows: i32,
    },
    /// Sync — end of extended query cycle ('S')
    Sync,
    /// Close a statement or portal ('C')
    Close {
        target: DescribeTarget,
        name: String,
    },
    /// Flush ('H')
    Flush,
    /// Terminate connection ('X')
    Terminate,
    /// COPY data ('d')
    CopyData {
        data: Vec<u8>,
    },
    /// COPY done ('c')
    CopyDone,
    /// COPY failed ('f')
    CopyFail {
        message: String,
    },
    /// Password response ('p')
    PasswordMessage {
        password: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribeTarget {
    Statement, // 'S'
    Portal,    // 'P'
}

// ---------------------------------------------------------------------------
// Backend messages (server → client)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum BackendMessage {
    /// Authentication request ('R')
    AuthenticationOk,
    AuthenticationCleartextPassword,

    /// Parameter status ('S')
    ParameterStatus {
        name: String,
        value: String,
    },

    /// Backend key data ('K')
    BackendKeyData {
        process_id: i32,
        secret_key: i32,
    },

    /// Ready for query ('Z')
    ReadyForQuery {
        state: TransactionState,
    },

    /// Row description ('T')
    RowDescription {
        fields: Vec<FieldDescription>,
    },

    /// Data row ('D')
    DataRow {
        values: Vec<Option<Vec<u8>>>,
    },

    /// Command complete ('C')
    CommandComplete {
        tag: String,
    },

    /// Empty query response ('I')
    EmptyQueryResponse,

    /// Error response ('E')
    ErrorResponse(ErrorFields),

    /// Notice response ('N')
    NoticeResponse(ErrorFields),

    /// Parse complete ('1')
    ParseComplete,

    /// Bind complete ('2')
    BindComplete,

    /// Close complete ('3')
    CloseComplete,

    /// No data ('n')
    NoData,

    /// Parameter description ('t')
    ParameterDescription {
        type_oids: Vec<i32>,
    },

    /// Copy-in response ('G')
    CopyInResponse {
        format: i8,
        column_formats: Vec<i16>,
    },

    /// Copy-out response ('H')
    CopyOutResponse {
        format: i8,
        column_formats: Vec<i16>,
    },

    /// Copy data ('d')
    CopyData {
        data: Vec<u8>,
    },

    /// Copy done ('c')
    CopyDone,
}

// ===========================================================================
// Encoding — BackendMessage → bytes
// ===========================================================================

impl BackendMessage {
    /// Encode this message into the provided writer.
    pub fn encode<W: Write>(&self, out: &mut W) -> io::Result<()> {
        match self {
            BackendMessage::AuthenticationOk => {
                out.write_all(&[b'R'])?;
                write_i32(out, 8)?;  // length
                write_i32(out, 0)?;  // auth ok
            }
            BackendMessage::AuthenticationCleartextPassword => {
                out.write_all(&[b'R'])?;
                write_i32(out, 8)?;
                write_i32(out, 3)?;  // cleartext password
            }
            BackendMessage::ParameterStatus { name, value } => {
                out.write_all(&[b'S'])?;
                let len = 4 + name.len() + 1 + value.len() + 1;
                write_i32(out, len as i32)?;
                write_cstring(out, name)?;
                write_cstring(out, value)?;
            }
            BackendMessage::BackendKeyData { process_id, secret_key } => {
                out.write_all(&[b'K'])?;
                write_i32(out, 12)?;
                write_i32(out, *process_id)?;
                write_i32(out, *secret_key)?;
            }
            BackendMessage::ReadyForQuery { state } => {
                out.write_all(&[b'Z'])?;
                write_i32(out, 5)?;
                out.write_all(&[state.as_byte()])?;
            }
            BackendMessage::RowDescription { fields } => {
                out.write_all(&[b'T'])?;
                let mut body = Vec::new();
                write_i16(&mut body, fields.len() as i16)?;
                for f in fields {
                    write_cstring(&mut body, &f.name)?;
                    write_i32(&mut body, f.table_oid)?;
                    write_i16(&mut body, f.column_attr)?;
                    write_i32(&mut body, f.type_oid)?;
                    write_i16(&mut body, f.type_size)?;
                    write_i32(&mut body, f.type_modifier)?;
                    write_i16(&mut body, f.format_code)?;
                }
                write_i32(out, (4 + body.len()) as i32)?;
                out.write_all(&body)?;
            }
            BackendMessage::DataRow { values } => {
                out.write_all(&[b'D'])?;
                let mut body = Vec::new();
                write_i16(&mut body, values.len() as i16)?;
                for v in values {
                    match v {
                        None => write_i32(&mut body, -1)?,
                        Some(data) => {
                            write_i32(&mut body, data.len() as i32)?;
                            body.write_all(data)?;
                        }
                    }
                }
                write_i32(out, (4 + body.len()) as i32)?;
                out.write_all(&body)?;
            }
            BackendMessage::CommandComplete { tag } => {
                out.write_all(&[b'C'])?;
                let len = 4 + tag.len() + 1;
                write_i32(out, len as i32)?;
                write_cstring(out, tag)?;
            }
            BackendMessage::EmptyQueryResponse => {
                out.write_all(&[b'I'])?;
                write_i32(out, 4)?;
            }
            BackendMessage::ErrorResponse(fields) => {
                encode_error_notice(out, b'E', fields)?;
            }
            BackendMessage::NoticeResponse(fields) => {
                encode_error_notice(out, b'N', fields)?;
            }
            BackendMessage::ParseComplete => {
                out.write_all(&[b'1'])?;
                write_i32(out, 4)?;
            }
            BackendMessage::BindComplete => {
                out.write_all(&[b'2'])?;
                write_i32(out, 4)?;
            }
            BackendMessage::CloseComplete => {
                out.write_all(&[b'3'])?;
                write_i32(out, 4)?;
            }
            BackendMessage::NoData => {
                out.write_all(&[b'n'])?;
                write_i32(out, 4)?;
            }
            BackendMessage::ParameterDescription { type_oids } => {
                out.write_all(&[b't'])?;
                let len = 4 + 2 + type_oids.len() * 4;
                write_i32(out, len as i32)?;
                write_i16(out, type_oids.len() as i16)?;
                for oid in type_oids {
                    write_i32(out, *oid)?;
                }
            }
            BackendMessage::CopyInResponse { format, column_formats } => {
                out.write_all(&[b'G'])?;
                let len = 4 + 1 + 2 + column_formats.len() * 2;
                write_i32(out, len as i32)?;
                out.write_all(&[*format as u8])?;
                write_i16(out, column_formats.len() as i16)?;
                for cf in column_formats {
                    write_i16(out, *cf)?;
                }
            }
            BackendMessage::CopyOutResponse { format, column_formats } => {
                out.write_all(&[b'H'])?;
                let len = 4 + 1 + 2 + column_formats.len() * 2;
                write_i32(out, len as i32)?;
                out.write_all(&[*format as u8])?;
                write_i16(out, column_formats.len() as i16)?;
                for cf in column_formats {
                    write_i16(out, *cf)?;
                }
            }
            BackendMessage::CopyData { data } => {
                out.write_all(&[b'd'])?;
                write_i32(out, (4 + data.len()) as i32)?;
                out.write_all(data)?;
            }
            BackendMessage::CopyDone => {
                out.write_all(&[b'c'])?;
                write_i32(out, 4)?;
            }
        }
        Ok(())
    }
}

fn encode_error_notice<W: Write>(out: &mut W, tag: u8, fields: &ErrorFields) -> io::Result<()> {
    out.write_all(&[tag])?;
    let mut body = Vec::new();
    // Severity
    body.push(b'S');
    write_cstring(&mut body, fields.severity.as_str())?;
    // Severity (non-localized)
    body.push(b'V');
    write_cstring(&mut body, fields.severity.as_str())?;
    // SQLSTATE code
    body.push(b'C');
    write_cstring(&mut body, &fields.code)?;
    // Message
    body.push(b'M');
    write_cstring(&mut body, &fields.message)?;
    // Detail
    if let Some(ref detail) = fields.detail {
        body.push(b'D');
        write_cstring(&mut body, detail)?;
    }
    // Hint
    if let Some(ref hint) = fields.hint {
        body.push(b'H');
        write_cstring(&mut body, hint)?;
    }
    // Position
    if let Some(pos) = fields.position {
        body.push(b'P');
        write_cstring(&mut body, &pos.to_string())?;
    }
    // Terminator
    body.push(0);
    write_i32(out, (4 + body.len()) as i32)?;
    out.write_all(&body)?;
    Ok(())
}

// ===========================================================================
// Decoding — bytes → FrontendMessage
// ===========================================================================

/// Read the initial startup-phase message (no type byte).
/// Returns the message or an error.
pub fn read_startup_message<R: Read>(reader: &mut R) -> io::Result<FrontendMessage> {
    let len = read_i32(reader)?;
    if len < 8 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "startup message too short"));
    }
    let code = read_i32(reader)?;

    match code {
        SSL_REQUEST_CODE => Ok(FrontendMessage::SslRequest),
        CANCEL_REQUEST_CODE => {
            let process_id = read_i32(reader)?;
            let secret_key = read_i32(reader)?;
            Ok(FrontendMessage::CancelRequest { process_id, secret_key })
        }
        PROTOCOL_VERSION_3 => {
            let remaining = (len - 8) as usize;
            let mut buf = vec![0u8; remaining];
            reader.read_exact(&mut buf)?;
            let params = parse_startup_params(&buf);
            Ok(FrontendMessage::Startup {
                version: PROTOCOL_VERSION_3,
                params,
            })
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported protocol version: {code}"),
        )),
    }
}

fn parse_startup_params(buf: &[u8]) -> HashMap<String, String> {
    let mut params = HashMap::new();
    let mut i = 0;
    loop {
        if i >= buf.len() || buf[i] == 0 {
            break;
        }
        let key_start = i;
        while i < buf.len() && buf[i] != 0 {
            i += 1;
        }
        let key = String::from_utf8_lossy(&buf[key_start..i]).to_string();
        i += 1; // skip null
        let val_start = i;
        while i < buf.len() && buf[i] != 0 {
            i += 1;
        }
        let val = String::from_utf8_lossy(&buf[val_start..i]).to_string();
        i += 1; // skip null
        params.insert(key, val);
    }
    params
}

/// Read a typed frontend message (after startup phase).
pub fn read_frontend_message<R: Read>(reader: &mut R) -> io::Result<FrontendMessage> {
    let mut type_buf = [0u8; 1];
    reader.read_exact(&mut type_buf)?;
    let msg_type = type_buf[0];
    let len = read_i32(reader)? as usize;
    if len < 4 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "message length too short"));
    }
    let payload_len = len - 4;
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        reader.read_exact(&mut payload)?;
    }

    decode_frontend_message(msg_type, &payload)
}

fn decode_frontend_message(msg_type: u8, payload: &[u8]) -> io::Result<FrontendMessage> {
    match msg_type {
        b'Q' => {
            let sql = read_cstring_from(payload, 0)?;
            Ok(FrontendMessage::Query { sql })
        }
        b'P' => {
            let mut off = 0;
            let name = read_cstring_at(payload, &mut off)?;
            let query = read_cstring_at(payload, &mut off)?;
            let num_params = read_i16_at(payload, &mut off)? as usize;
            let mut param_types = Vec::with_capacity(num_params);
            for _ in 0..num_params {
                param_types.push(read_i32_at(payload, &mut off)?);
            }
            Ok(FrontendMessage::Parse { name, query, param_types })
        }
        b'B' => {
            let mut off = 0;
            let portal = read_cstring_at(payload, &mut off)?;
            let statement = read_cstring_at(payload, &mut off)?;
            let num_param_formats = read_i16_at(payload, &mut off)? as usize;
            let mut param_formats = Vec::with_capacity(num_param_formats);
            for _ in 0..num_param_formats {
                param_formats.push(read_i16_at(payload, &mut off)?);
            }
            let num_params = read_i16_at(payload, &mut off)? as usize;
            let mut param_values = Vec::with_capacity(num_params);
            for _ in 0..num_params {
                let val_len = read_i32_at(payload, &mut off)?;
                if val_len == -1 {
                    param_values.push(None);
                } else {
                    let end = off + val_len as usize;
                    if end > payload.len() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "bind parameter value exceeds payload",
                        ));
                    }
                    param_values.push(Some(payload[off..end].to_vec()));
                    off = end;
                }
            }
            let num_result_formats = read_i16_at(payload, &mut off)? as usize;
            let mut result_formats = Vec::with_capacity(num_result_formats);
            for _ in 0..num_result_formats {
                result_formats.push(read_i16_at(payload, &mut off)?);
            }
            Ok(FrontendMessage::Bind {
                portal,
                statement,
                param_formats,
                param_values,
                result_formats,
            })
        }
        b'D' => {
            if payload.is_empty() {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "describe: missing target"));
            }
            let target = match payload[0] {
                b'S' => DescribeTarget::Statement,
                b'P' => DescribeTarget::Portal,
                _ => return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("describe: unknown target '{}'", payload[0] as char),
                )),
            };
            let name = read_cstring_from(payload, 1)?;
            Ok(FrontendMessage::Describe { target, name })
        }
        b'E' => {
            let mut off = 0;
            let portal = read_cstring_at(payload, &mut off)?;
            let max_rows = read_i32_at(payload, &mut off)?;
            Ok(FrontendMessage::Execute { portal, max_rows })
        }
        b'S' => Ok(FrontendMessage::Sync),
        b'C' => {
            if payload.is_empty() {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "close: missing target"));
            }
            let target = match payload[0] {
                b'S' => DescribeTarget::Statement,
                b'P' => DescribeTarget::Portal,
                _ => return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("close: unknown target '{}'", payload[0] as char),
                )),
            };
            let name = read_cstring_from(payload, 1)?;
            Ok(FrontendMessage::Close { target, name })
        }
        b'H' => Ok(FrontendMessage::Flush),
        b'X' => Ok(FrontendMessage::Terminate),
        b'd' => Ok(FrontendMessage::CopyData { data: payload.to_vec() }),
        b'c' => Ok(FrontendMessage::CopyDone),
        b'f' => {
            let message = read_cstring_from(payload, 0)?;
            Ok(FrontendMessage::CopyFail { message })
        }
        b'p' => {
            let password = read_cstring_from(payload, 0)?;
            Ok(FrontendMessage::PasswordMessage { password })
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unknown frontend message type: '{}'", msg_type as char),
        )),
    }
}

// ===========================================================================
// Type serialization — Datum → text/binary format
// ===========================================================================

/// Serialize a Datum to PostgreSQL text format (returns None for SQL NULL).
pub fn datum_to_text(datum: &Datum) -> Option<Vec<u8>> {
    match datum {
        Datum::Null => None,
        Datum::Integer(v) => Some(v.to_string().into_bytes()),
        Datum::BigInt(v) => Some(v.to_string().into_bytes()),
        Datum::Float(v) => {
            if v.is_nan() {
                Some(b"NaN".to_vec())
            } else if v.is_infinite() {
                Some(if *v > 0.0 { b"Infinity".to_vec() } else { b"-Infinity".to_vec() })
            } else {
                Some(format!("{v}").into_bytes())
            }
        }
        Datum::Boolean(v) => Some(if *v { b"t".to_vec() } else { b"f".to_vec() }),
        Datum::Varchar(s) | Datum::Text(s) => Some(s.as_bytes().to_vec()),
        Datum::Timestamp(v) => {
            // Microseconds since Unix epoch → simplified ISO format
            let secs = v / 1_000_000;
            let micros = (v % 1_000_000).unsigned_abs();
            Some(format!("{secs}.{micros:06}").into_bytes())
        }
        Datum::Date(v) => {
            // Days since Unix epoch → simplified format
            Some(format!("{v}").into_bytes())
        }
        Datum::Decimal { mantissa, scale } => {
            if *scale == 0 {
                Some(mantissa.to_string().into_bytes())
            } else {
                let divisor = 10i128.pow(*scale as u32);
                let int_part = mantissa / divisor;
                let frac_part = (mantissa % divisor).unsigned_abs();
                Some(format!("{int_part}.{frac_part:0>width$}", width = *scale as usize).into_bytes())
            }
        }
        Datum::Blob(data) => {
            // PostgreSQL hex format: \x followed by hex bytes
            let mut out = Vec::with_capacity(2 + data.len() * 2);
            out.extend_from_slice(b"\\x");
            for b in data {
                out.extend_from_slice(format!("{b:02x}").as_bytes());
            }
            Some(out)
        }
    }
}

/// Serialize a Datum to PostgreSQL binary format (returns None for SQL NULL).
pub fn datum_to_binary(datum: &Datum) -> Option<Vec<u8>> {
    match datum {
        Datum::Null => None,
        Datum::Integer(v) => Some(v.to_be_bytes().to_vec()),
        Datum::BigInt(v) => Some(v.to_be_bytes().to_vec()),
        Datum::Float(v) => Some(v.to_be_bytes().to_vec()),
        Datum::Boolean(v) => Some(vec![if *v { 1 } else { 0 }]),
        Datum::Varchar(s) | Datum::Text(s) => Some(s.as_bytes().to_vec()),
        Datum::Timestamp(v) => {
            // PG epoch is 2000-01-01, Unix epoch is 1970-01-01
            // Difference: 10957 days = 946684800 seconds = 946684800000000 microseconds
            let pg_micros = v - 946_684_800_000_000i64;
            Some(pg_micros.to_be_bytes().to_vec())
        }
        Datum::Date(v) => {
            // PG epoch is 2000-01-01 = day 10957
            let pg_days = v - 10957;
            Some(pg_days.to_be_bytes().to_vec())
        }
        Datum::Decimal { mantissa: _mantissa, scale: _scale } => {
            // Simplified: send as text representation in binary
            let text = datum_to_text(datum).unwrap();
            Some(text)
        }
        Datum::Blob(data) => Some(data.clone()),
    }
}

/// Parse a Datum from PostgreSQL text format.
pub fn text_to_datum(data: &[u8], target_type: &DataType) -> Result<Datum, String> {
    let s = std::str::from_utf8(data).map_err(|e| format!("invalid UTF-8: {e}"))?;

    match target_type {
        DataType::Integer => s
            .trim()
            .parse::<i32>()
            .map(Datum::Integer)
            .map_err(|e| format!("invalid integer: {e}")),
        DataType::BigInt => s
            .trim()
            .parse::<i64>()
            .map(Datum::BigInt)
            .map_err(|e| format!("invalid bigint: {e}")),
        DataType::Float => s
            .trim()
            .parse::<f64>()
            .map(Datum::Float)
            .map_err(|e| format!("invalid float: {e}")),
        DataType::Boolean => match s.trim().to_lowercase().as_str() {
            "t" | "true" | "1" | "yes" | "on" => Ok(Datum::Boolean(true)),
            "f" | "false" | "0" | "no" | "off" => Ok(Datum::Boolean(false)),
            _ => Err(format!("invalid boolean: {s}")),
        },
        DataType::Varchar(_) => Ok(Datum::Varchar(s.to_string())),
        DataType::Text => Ok(Datum::Text(s.to_string())),
        DataType::Timestamp => s
            .trim()
            .parse::<i64>()
            .map(Datum::Timestamp)
            .map_err(|e| format!("invalid timestamp: {e}")),
        DataType::Date => s
            .trim()
            .parse::<i32>()
            .map(Datum::Date)
            .map_err(|e| format!("invalid date: {e}")),
        DataType::Decimal(_, scale) => {
            parse_decimal(s.trim(), *scale)
        }
        DataType::Blob => {
            // Accept hex format: \xDEADBEEF
            if let Some(hex) = s.strip_prefix("\\x") {
                let bytes = hex_to_bytes(hex)?;
                Ok(Datum::Blob(bytes))
            } else {
                Ok(Datum::Blob(data.to_vec()))
            }
        }
    }
}

fn parse_decimal(s: &str, default_scale: u8) -> Result<Datum, String> {
    if let Some(dot_pos) = s.find('.') {
        let int_part = &s[..dot_pos];
        let frac_part = &s[dot_pos + 1..];
        let scale = frac_part.len() as u8;
        let combined = format!("{int_part}{frac_part}");
        let mantissa = combined
            .parse::<i128>()
            .map_err(|e| format!("invalid decimal: {e}"))?;
        Ok(Datum::Decimal { mantissa, scale })
    } else {
        let mantissa = s
            .parse::<i128>()
            .map_err(|e| format!("invalid decimal: {e}"))?
            * 10i128.pow(default_scale as u32);
        Ok(Datum::Decimal {
            mantissa,
            scale: default_scale,
        })
    }
}

fn hex_to_bytes(hex: &str) -> Result<Vec<u8>, String> {
    if hex.len() % 2 != 0 {
        return Err("hex string has odd length".to_string());
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for i in (0..hex.len()).step_by(2) {
        let byte = u8::from_str_radix(&hex[i..i + 2], 16)
            .map_err(|e| format!("invalid hex: {e}"))?;
        bytes.push(byte);
    }
    Ok(bytes)
}

// ===========================================================================
// Wire helpers
// ===========================================================================

fn write_i32<W: Write>(w: &mut W, v: i32) -> io::Result<()> {
    w.write_all(&v.to_be_bytes())
}

fn write_i16<W: Write>(w: &mut W, v: i16) -> io::Result<()> {
    w.write_all(&v.to_be_bytes())
}

fn write_cstring<W: Write>(w: &mut W, s: &str) -> io::Result<()> {
    w.write_all(s.as_bytes())?;
    w.write_all(&[0])
}

fn read_i32<R: Read>(r: &mut R) -> io::Result<i32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(i32::from_be_bytes(buf))
}

fn read_i16_at(buf: &[u8], off: &mut usize) -> io::Result<i16> {
    if *off + 2 > buf.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
    }
    let v = i16::from_be_bytes([buf[*off], buf[*off + 1]]);
    *off += 2;
    Ok(v)
}

fn read_i32_at(buf: &[u8], off: &mut usize) -> io::Result<i32> {
    if *off + 4 > buf.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
    }
    let v = i32::from_be_bytes([buf[*off], buf[*off + 1], buf[*off + 2], buf[*off + 3]]);
    *off += 4;
    Ok(v)
}

fn read_cstring_from(buf: &[u8], start: usize) -> io::Result<String> {
    let mut off = start;
    read_cstring_at(buf, &mut off)
}

fn read_cstring_at(buf: &[u8], off: &mut usize) -> io::Result<String> {
    let start = *off;
    while *off < buf.len() && buf[*off] != 0 {
        *off += 1;
    }
    if *off >= buf.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unterminated c-string",
        ));
    }
    let s = String::from_utf8_lossy(&buf[start..*off]).to_string();
    *off += 1; // skip null terminator
    Ok(s)
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn encode_decode_ready_for_query() {
        let msg = BackendMessage::ReadyForQuery {
            state: TransactionState::Idle,
        };
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();
        assert_eq!(buf[0], b'Z');
        assert_eq!(&buf[1..5], &5i32.to_be_bytes());
        assert_eq!(buf[5], b'I');
    }

    #[test]
    fn encode_error_response() {
        let fields = ErrorFields::new(Severity::Error, "42601", "syntax error at position 5")
            .with_position(5);
        let msg = BackendMessage::ErrorResponse(fields);
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();
        assert_eq!(buf[0], b'E');
    }

    #[test]
    fn encode_row_description() {
        let fields = vec![
            FieldDescription::new("id", &DataType::Integer),
            FieldDescription::new("name", &DataType::Text),
        ];
        let msg = BackendMessage::RowDescription { fields };
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();
        assert_eq!(buf[0], b'T');
    }

    #[test]
    fn encode_data_row() {
        let msg = BackendMessage::DataRow {
            values: vec![
                Some(b"42".to_vec()),
                Some(b"hello".to_vec()),
                None,
            ],
        };
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();
        assert_eq!(buf[0], b'D');
    }

    #[test]
    fn datum_text_format_roundtrip() {
        let test_cases: Vec<(Datum, DataType)> = vec![
            (Datum::Integer(42), DataType::Integer),
            (Datum::BigInt(-1000), DataType::BigInt),
            (Datum::Float(3.14), DataType::Float),
            (Datum::Boolean(true), DataType::Boolean),
            (Datum::Varchar("hello".into()), DataType::Varchar(255)),
            (Datum::Text("world".into()), DataType::Text),
        ];

        for (datum, dt) in &test_cases {
            let text = datum_to_text(datum).unwrap();
            let roundtrip = text_to_datum(&text, dt).unwrap();
            // Compare via Display since types may differ slightly
            assert_eq!(format!("{datum}"), format!("{roundtrip}"), "failed for {dt}");
        }
    }

    #[test]
    fn datum_null_serialization() {
        assert!(datum_to_text(&Datum::Null).is_none());
        assert!(datum_to_binary(&Datum::Null).is_none());
    }

    #[test]
    fn read_startup() {
        let mut buf = Vec::new();
        // length(4) + version(4) + "user\0test\0\0"
        let params = b"user\0test\0database\0mydb\0\0";
        let len = 8 + params.len();
        write_i32(&mut buf, len as i32).unwrap();
        write_i32(&mut buf, PROTOCOL_VERSION_3).unwrap();
        buf.extend_from_slice(params);

        let mut cursor = Cursor::new(buf);
        let msg = read_startup_message(&mut cursor).unwrap();
        match msg {
            FrontendMessage::Startup { version, params } => {
                assert_eq!(version, PROTOCOL_VERSION_3);
                assert_eq!(params.get("user").unwrap(), "test");
                assert_eq!(params.get("database").unwrap(), "mydb");
            }
            _ => panic!("expected Startup"),
        }
    }

    #[test]
    fn read_query_message() {
        let sql = "SELECT 1;\0";
        let mut buf = Vec::new();
        buf.push(b'Q');
        write_i32(&mut buf, (4 + sql.len()) as i32).unwrap();
        buf.extend_from_slice(sql.as_bytes());

        let mut cursor = Cursor::new(buf);
        let msg = read_frontend_message(&mut cursor).unwrap();
        match msg {
            FrontendMessage::Query { sql } => {
                assert_eq!(sql, "SELECT 1;");
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn read_parse_message() {
        let mut payload = Vec::new();
        write_cstring(&mut payload, "stmt1").unwrap();       // name
        write_cstring(&mut payload, "SELECT $1").unwrap();   // query
        write_i16(&mut payload, 1).unwrap();                  // 1 param type
        write_i32(&mut payload, 23).unwrap();                 // int4

        let mut buf = Vec::new();
        buf.push(b'P');
        write_i32(&mut buf, (4 + payload.len()) as i32).unwrap();
        buf.extend_from_slice(&payload);

        let mut cursor = Cursor::new(buf);
        let msg = read_frontend_message(&mut cursor).unwrap();
        match msg {
            FrontendMessage::Parse { name, query, param_types } => {
                assert_eq!(name, "stmt1");
                assert_eq!(query, "SELECT $1");
                assert_eq!(param_types, vec![23]);
            }
            _ => panic!("expected Parse"),
        }
    }

    #[test]
    fn read_terminate() {
        let mut buf = Vec::new();
        buf.push(b'X');
        write_i32(&mut buf, 4).unwrap();

        let mut cursor = Cursor::new(buf);
        let msg = read_frontend_message(&mut cursor).unwrap();
        assert!(matches!(msg, FrontendMessage::Terminate));
    }

    #[test]
    fn pg_type_mapping() {
        assert_eq!(pg_type_info(&DataType::Integer), (23, 4));
        assert_eq!(pg_type_info(&DataType::BigInt), (20, 8));
        assert_eq!(pg_type_info(&DataType::Text), (25, -1));
        assert_eq!(pg_type_info(&DataType::Boolean), (16, 1));
    }

    #[test]
    fn transaction_state_bytes() {
        assert_eq!(TransactionState::Idle.as_byte(), b'I');
        assert_eq!(TransactionState::InBlock.as_byte(), b'T');
        assert_eq!(TransactionState::Failed.as_byte(), b'E');
    }

    #[test]
    fn command_complete_encoding() {
        let msg = BackendMessage::CommandComplete {
            tag: "SELECT 5".to_string(),
        };
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();
        assert_eq!(buf[0], b'C');
        // Check the tag appears in the output
        let tag_bytes = b"SELECT 5\0";
        assert!(buf.windows(tag_bytes.len()).any(|w| w == tag_bytes));
    }

    #[test]
    fn copy_in_response_encoding() {
        let msg = BackendMessage::CopyInResponse {
            format: 0,
            column_formats: vec![0, 0, 0],
        };
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();
        assert_eq!(buf[0], b'G');
    }

    #[test]
    fn blob_text_format() {
        let datum = Datum::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        let text = datum_to_text(&datum).unwrap();
        assert_eq!(text, b"\\xdeadbeef");
    }

    #[test]
    fn text_to_datum_boolean() {
        assert_eq!(
            text_to_datum(b"t", &DataType::Boolean).unwrap(),
            Datum::Boolean(true)
        );
        assert_eq!(
            text_to_datum(b"false", &DataType::Boolean).unwrap(),
            Datum::Boolean(false)
        );
    }
}
