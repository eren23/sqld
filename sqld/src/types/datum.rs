use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use crate::types::data_type::DataType;
use crate::utils::error::{Error, TypeError};

// ---------------------------------------------------------------------------
// Datum — the runtime representation of a single SQL value
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum Datum {
    Integer(i32),
    BigInt(i64),
    Float(f64),
    Boolean(bool),
    Varchar(String),
    Text(String),
    Timestamp(i64),  // microseconds since Unix epoch (UTC)
    Date(i32),       // days since Unix epoch
    Decimal {
        mantissa: i128,
        scale: u8,
    },
    Blob(Vec<u8>),
    Null,
}

// ---------------------------------------------------------------------------
// Tag bytes for serialization
// ---------------------------------------------------------------------------

const TAG_NULL: u8 = 0;
const TAG_INTEGER: u8 = 1;
const TAG_BIGINT: u8 = 2;
const TAG_FLOAT: u8 = 3;
const TAG_BOOLEAN: u8 = 4;
const TAG_VARCHAR: u8 = 5;
const TAG_TEXT: u8 = 6;
const TAG_TIMESTAMP: u8 = 7;
const TAG_DATE: u8 = 8;
const TAG_DECIMAL: u8 = 9;
const TAG_BLOB: u8 = 10;

// Microseconds per day for DATE → TIMESTAMP coercion
const MICROS_PER_DAY: i64 = 86_400 * 1_000_000;

impl Datum {
    // -----------------------------------------------------------------------
    // Introspection
    // -----------------------------------------------------------------------

    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Datum::Integer(_) => Some(DataType::Integer),
            Datum::BigInt(_) => Some(DataType::BigInt),
            Datum::Float(_) => Some(DataType::Float),
            Datum::Boolean(_) => Some(DataType::Boolean),
            Datum::Varchar(s) => Some(DataType::Varchar(s.len() as u32)),
            Datum::Text(_) => Some(DataType::Text),
            Datum::Timestamp(_) => Some(DataType::Timestamp),
            Datum::Date(_) => Some(DataType::Date),
            Datum::Decimal { scale, .. } => Some(DataType::Decimal(38, *scale)),
            Datum::Blob(_) => Some(DataType::Blob),
            Datum::Null => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Datum::Null)
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Datum::Integer(_) => "INTEGER",
            Datum::BigInt(_) => "BIGINT",
            Datum::Float(_) => "FLOAT",
            Datum::Boolean(_) => "BOOLEAN",
            Datum::Varchar(_) => "VARCHAR",
            Datum::Text(_) => "TEXT",
            Datum::Timestamp(_) => "TIMESTAMP",
            Datum::Date(_) => "DATE",
            Datum::Decimal { .. } => "DECIMAL",
            Datum::Blob(_) => "BLOB",
            Datum::Null => "NULL",
        }
    }

    // -----------------------------------------------------------------------
    // Coercion
    // -----------------------------------------------------------------------

    /// Coerce `self` to the given target [`DataType`].
    ///
    /// Follows the implicit widening lattice:
    /// `INTEGER → BIGINT → DECIMAL → FLOAT`,
    /// `VARCHAR → TEXT`, `DATE → TIMESTAMP`.
    pub fn coerce_to(&self, target: &DataType) -> Result<Datum, Error> {
        // Null coerces to anything.
        if self.is_null() {
            return Ok(Datum::Null);
        }

        // Already the right type?
        if let Some(ref dt) = self.data_type() {
            if dt == target {
                return Ok(self.clone());
            }
        }

        match (self, target) {
            // Numeric widening
            (Datum::Integer(v), DataType::BigInt) => Ok(Datum::BigInt(*v as i64)),
            (Datum::Integer(v), DataType::Decimal(_, s)) => Ok(Datum::Decimal {
                mantissa: (*v as i128) * 10i128.pow(*s as u32),
                scale: *s,
            }),
            (Datum::Integer(v), DataType::Float) => Ok(Datum::Float(*v as f64)),

            (Datum::BigInt(v), DataType::Decimal(_, s)) => Ok(Datum::Decimal {
                mantissa: (*v as i128) * 10i128.pow(*s as u32),
                scale: *s,
            }),
            (Datum::BigInt(v), DataType::Float) => Ok(Datum::Float(*v as f64)),

            (Datum::Decimal { mantissa, scale }, DataType::Float) => {
                let f = *mantissa as f64 / 10f64.powi(*scale as i32);
                Ok(Datum::Float(f))
            }

            // String widening
            (Datum::Varchar(s), DataType::Text) => Ok(Datum::Text(s.clone())),

            // Temporal widening
            (Datum::Date(d), DataType::Timestamp) => {
                Ok(Datum::Timestamp(*d as i64 * MICROS_PER_DAY))
            }

            _ => Err(TypeError::InvalidCoercion {
                from: self.type_name().to_string(),
                to: target.to_string(),
            }
            .into()),
        }
    }

    /// Coerce a pair of datums to their common super-type (for comparisons
    /// and arithmetic).  Returns the two coerced values.
    pub fn coerce_pair(a: &Datum, b: &Datum) -> Result<(Datum, Datum), Error> {
        if a.is_null() || b.is_null() {
            return Ok((a.clone(), b.clone()));
        }

        let dt_a = a.data_type().unwrap();
        let dt_b = b.data_type().unwrap();

        if dt_a == dt_b {
            return Ok((a.clone(), b.clone()));
        }

        let common = DataType::common_type(dt_a, dt_b).ok_or_else(|| TypeError::TypeMismatch {
            expected: dt_a.to_string(),
            found: dt_b.to_string(),
        })?;

        Ok((a.coerce_to(&common)?, b.coerce_to(&common)?))
    }

    // -----------------------------------------------------------------------
    // SQL-level comparison (with coercion)
    // -----------------------------------------------------------------------

    /// Compare two datums following SQL semantics:
    /// - NULL compared with anything yields `None` (SQL NULL).
    /// - Cross-type operands are coerced to the common super-type first.
    pub fn sql_cmp(&self, other: &Datum) -> Result<Option<Ordering>, Error> {
        if self.is_null() || other.is_null() {
            return Ok(None);
        }

        let (a, b) = Datum::coerce_pair(self, other)?;
        match (&a, &b) {
            (Datum::Integer(x), Datum::Integer(y)) => Ok(Some(x.cmp(y))),
            (Datum::BigInt(x), Datum::BigInt(y)) => Ok(Some(x.cmp(y))),
            (Datum::Float(x), Datum::Float(y)) => Ok(x.partial_cmp(y)),
            (Datum::Boolean(x), Datum::Boolean(y)) => Ok(Some(x.cmp(y))),
            (Datum::Varchar(x), Datum::Varchar(y)) => Ok(Some(x.cmp(y))),
            (Datum::Text(x), Datum::Text(y)) => Ok(Some(x.cmp(y))),
            (Datum::Timestamp(x), Datum::Timestamp(y)) => Ok(Some(x.cmp(y))),
            (Datum::Date(x), Datum::Date(y)) => Ok(Some(x.cmp(y))),
            (Datum::Decimal { .. }, Datum::Decimal { .. }) => {
                let (na, nb) = decimal_normalize(&a, &b);
                Ok(Some(na.cmp(&nb)))
            }
            (Datum::Blob(x), Datum::Blob(y)) => Ok(Some(x.cmp(y))),
            _ => Err(TypeError::InvalidComparison {
                lhs: a.type_name().to_string(),
                rhs: b.type_name().to_string(),
            }
            .into()),
        }
    }

    // -----------------------------------------------------------------------
    // Arithmetic
    // -----------------------------------------------------------------------

    pub fn add(&self, other: &Datum) -> Result<Datum, Error> {
        numeric_binop(self, other, NumericOp::Add)
    }

    pub fn sub(&self, other: &Datum) -> Result<Datum, Error> {
        numeric_binop(self, other, NumericOp::Sub)
    }

    pub fn mul(&self, other: &Datum) -> Result<Datum, Error> {
        numeric_binop(self, other, NumericOp::Mul)
    }

    pub fn div(&self, other: &Datum) -> Result<Datum, Error> {
        numeric_binop(self, other, NumericOp::Div)
    }

    pub fn neg(&self) -> Result<Datum, Error> {
        match self {
            Datum::Integer(v) => v
                .checked_neg()
                .map(Datum::Integer)
                .ok_or_else(|| TypeError::ArithmeticOverflow.into()),
            Datum::BigInt(v) => v
                .checked_neg()
                .map(Datum::BigInt)
                .ok_or_else(|| TypeError::ArithmeticOverflow.into()),
            Datum::Float(v) => Ok(Datum::Float(-v)),
            Datum::Decimal { mantissa, scale } => Ok(Datum::Decimal {
                mantissa: -mantissa,
                scale: *scale,
            }),
            Datum::Null => Ok(Datum::Null),
            _ => Err(TypeError::TypeMismatch {
                expected: "numeric".into(),
                found: self.type_name().into(),
            }
            .into()),
        }
    }

    // -----------------------------------------------------------------------
    // Serialization
    // -----------------------------------------------------------------------

    /// Append the wire-format bytes of this datum to `buf`.
    ///
    /// Format: `[tag:u8][payload...]`
    pub fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            Datum::Null => buf.push(TAG_NULL),
            Datum::Integer(v) => {
                buf.push(TAG_INTEGER);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Datum::BigInt(v) => {
                buf.push(TAG_BIGINT);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Datum::Float(v) => {
                buf.push(TAG_FLOAT);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Datum::Boolean(v) => {
                buf.push(TAG_BOOLEAN);
                buf.push(if *v { 1 } else { 0 });
            }
            Datum::Varchar(s) => {
                buf.push(TAG_VARCHAR);
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            Datum::Text(s) => {
                buf.push(TAG_TEXT);
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            Datum::Timestamp(v) => {
                buf.push(TAG_TIMESTAMP);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Datum::Date(v) => {
                buf.push(TAG_DATE);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Datum::Decimal { mantissa, scale } => {
                buf.push(TAG_DECIMAL);
                buf.extend_from_slice(&mantissa.to_le_bytes());
                buf.push(*scale);
            }
            Datum::Blob(data) => {
                buf.push(TAG_BLOB);
                buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
                buf.extend_from_slice(data);
            }
        }
    }

    /// Deserialize a single datum starting at `offset` within `buf`.
    /// Advances `offset` past the consumed bytes.
    pub fn deserialize(buf: &[u8], offset: &mut usize) -> Result<Datum, Error> {
        let tag = read_u8(buf, offset)?;
        match tag {
            TAG_NULL => Ok(Datum::Null),
            TAG_INTEGER => {
                let v = read_i32(buf, offset)?;
                Ok(Datum::Integer(v))
            }
            TAG_BIGINT => {
                let v = read_i64(buf, offset)?;
                Ok(Datum::BigInt(v))
            }
            TAG_FLOAT => {
                let v = read_f64(buf, offset)?;
                Ok(Datum::Float(v))
            }
            TAG_BOOLEAN => {
                let v = read_u8(buf, offset)?;
                Ok(Datum::Boolean(v != 0))
            }
            TAG_VARCHAR => {
                let s = read_string(buf, offset)?;
                Ok(Datum::Varchar(s))
            }
            TAG_TEXT => {
                let s = read_string(buf, offset)?;
                Ok(Datum::Text(s))
            }
            TAG_TIMESTAMP => {
                let v = read_i64(buf, offset)?;
                Ok(Datum::Timestamp(v))
            }
            TAG_DATE => {
                let v = read_i32(buf, offset)?;
                Ok(Datum::Date(v))
            }
            TAG_DECIMAL => {
                let mantissa = read_i128(buf, offset)?;
                let scale = read_u8(buf, offset)?;
                Ok(Datum::Decimal { mantissa, scale })
            }
            TAG_BLOB => {
                let data = read_bytes(buf, offset)?;
                Ok(Datum::Blob(data))
            }
            _ => Err(Error::Serialization(format!("unknown datum tag: {tag}"))),
        }
    }
}

// ---------------------------------------------------------------------------
// PartialEq / Eq — strict same-type equality (Rust-level, not SQL)
// ---------------------------------------------------------------------------

impl PartialEq for Datum {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Datum::Null, Datum::Null) => true,
            (Datum::Integer(a), Datum::Integer(b)) => a == b,
            (Datum::BigInt(a), Datum::BigInt(b)) => a == b,
            (Datum::Float(a), Datum::Float(b)) => a.to_bits() == b.to_bits(),
            (Datum::Boolean(a), Datum::Boolean(b)) => a == b,
            (Datum::Varchar(a), Datum::Varchar(b)) => a == b,
            (Datum::Text(a), Datum::Text(b)) => a == b,
            (Datum::Timestamp(a), Datum::Timestamp(b)) => a == b,
            (Datum::Date(a), Datum::Date(b)) => a == b,
            (Datum::Decimal {
                mantissa: ma,
                scale: sa,
            }, Datum::Decimal {
                mantissa: mb,
                scale: sb,
            }) => {
                // Normalize to same scale for equality.
                let (na, nb) = decimal_normalize_raw(*ma, *sa, *mb, *sb);
                na == nb
            }
            (Datum::Blob(a), Datum::Blob(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Datum {}

// ---------------------------------------------------------------------------
// Hash — must be consistent with PartialEq
// ---------------------------------------------------------------------------

impl Hash for Datum {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Datum::Null => {}
            Datum::Integer(v) => v.hash(state),
            Datum::BigInt(v) => v.hash(state),
            Datum::Float(v) => {
                // Normalize -0.0 to +0.0 so they hash equally.
                let bits = if *v == 0.0 { 0.0f64.to_bits() } else { v.to_bits() };
                bits.hash(state);
            }
            Datum::Boolean(v) => v.hash(state),
            Datum::Varchar(s) => s.hash(state),
            Datum::Text(s) => s.hash(state),
            Datum::Timestamp(v) => v.hash(state),
            Datum::Date(v) => v.hash(state),
            Datum::Decimal { mantissa, scale } => {
                // Normalize: strip trailing zeros so 1.20 (mantissa=120, scale=2)
                // hashes the same as 1.2 (mantissa=12, scale=1).
                let (m, s) = decimal_strip_trailing_zeros(*mantissa, *scale);
                m.hash(state);
                s.hash(state);
            }
            Datum::Blob(b) => b.hash(state),
        }
    }
}

// ---------------------------------------------------------------------------
// PartialOrd — strict same-type ordering
// ---------------------------------------------------------------------------

impl PartialOrd for Datum {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Datum::Null, Datum::Null) => Some(Ordering::Equal),
            (Datum::Integer(a), Datum::Integer(b)) => Some(a.cmp(b)),
            (Datum::BigInt(a), Datum::BigInt(b)) => Some(a.cmp(b)),
            (Datum::Float(a), Datum::Float(b)) => a.partial_cmp(b),
            (Datum::Boolean(a), Datum::Boolean(b)) => Some(a.cmp(b)),
            (Datum::Varchar(a), Datum::Varchar(b)) => Some(a.cmp(b)),
            (Datum::Text(a), Datum::Text(b)) => Some(a.cmp(b)),
            (Datum::Timestamp(a), Datum::Timestamp(b)) => Some(a.cmp(b)),
            (Datum::Date(a), Datum::Date(b)) => Some(a.cmp(b)),
            (Datum::Decimal { .. }, Datum::Decimal { .. }) => {
                let (na, nb) = decimal_normalize(self, other);
                Some(na.cmp(&nb))
            }
            (Datum::Blob(a), Datum::Blob(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Display
// ---------------------------------------------------------------------------

impl std::fmt::Display for Datum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Datum::Null => write!(f, "NULL"),
            Datum::Integer(v) => write!(f, "{v}"),
            Datum::BigInt(v) => write!(f, "{v}"),
            Datum::Float(v) => write!(f, "{v}"),
            Datum::Boolean(v) => write!(f, "{v}"),
            Datum::Varchar(s) | Datum::Text(s) => write!(f, "{s}"),
            Datum::Timestamp(v) => write!(f, "TIMESTAMP({v})"),
            Datum::Date(v) => write!(f, "DATE({v})"),
            Datum::Decimal { mantissa, scale } => {
                if *scale == 0 {
                    write!(f, "{mantissa}")
                } else {
                    let divisor = 10i128.pow(*scale as u32);
                    let int_part = mantissa / divisor;
                    let frac_part = (mantissa % divisor).unsigned_abs();
                    write!(f, "{int_part}.{frac_part:0>width$}", width = *scale as usize)
                }
            }
            Datum::Blob(b) => write!(f, "BLOB[{}]", b.len()),
        }
    }
}

// ===========================================================================
// Helpers — arithmetic
// ===========================================================================

enum NumericOp {
    Add,
    Sub,
    Mul,
    Div,
}

fn numeric_binop(lhs: &Datum, rhs: &Datum, op: NumericOp) -> Result<Datum, Error> {
    // NULL propagation
    if lhs.is_null() || rhs.is_null() {
        return Ok(Datum::Null);
    }

    let (a, b) = Datum::coerce_pair(lhs, rhs)?;

    match (&a, &b) {
        (Datum::Integer(x), Datum::Integer(y)) => int_op(*x, *y, &op),
        (Datum::BigInt(x), Datum::BigInt(y)) => bigint_op(*x, *y, &op),
        (Datum::Float(x), Datum::Float(y)) => float_op(*x, *y, &op),
        (Datum::Decimal { .. }, Datum::Decimal { .. }) => decimal_op(&a, &b, &op),
        _ => Err(TypeError::TypeMismatch {
            expected: "numeric".into(),
            found: a.type_name().into(),
        }
        .into()),
    }
}

fn int_op(x: i32, y: i32, op: &NumericOp) -> Result<Datum, Error> {
    let result = match op {
        NumericOp::Add => x.checked_add(y),
        NumericOp::Sub => x.checked_sub(y),
        NumericOp::Mul => x.checked_mul(y),
        NumericOp::Div => {
            if y == 0 {
                return Err(TypeError::DivisionByZero.into());
            }
            x.checked_div(y)
        }
    };
    result
        .map(Datum::Integer)
        .ok_or_else(|| TypeError::ArithmeticOverflow.into())
}

fn bigint_op(x: i64, y: i64, op: &NumericOp) -> Result<Datum, Error> {
    let result = match op {
        NumericOp::Add => x.checked_add(y),
        NumericOp::Sub => x.checked_sub(y),
        NumericOp::Mul => x.checked_mul(y),
        NumericOp::Div => {
            if y == 0 {
                return Err(TypeError::DivisionByZero.into());
            }
            x.checked_div(y)
        }
    };
    result
        .map(Datum::BigInt)
        .ok_or_else(|| TypeError::ArithmeticOverflow.into())
}

fn float_op(x: f64, y: f64, op: &NumericOp) -> Result<Datum, Error> {
    let result = match op {
        NumericOp::Add => x + y,
        NumericOp::Sub => x - y,
        NumericOp::Mul => x * y,
        NumericOp::Div => {
            if y == 0.0 {
                return Err(TypeError::DivisionByZero.into());
            }
            x / y
        }
    };
    Ok(Datum::Float(result))
}

fn decimal_op(a: &Datum, b: &Datum, op: &NumericOp) -> Result<Datum, Error> {
    let (ma, sa) = match a {
        Datum::Decimal { mantissa, scale } => (*mantissa, *scale),
        _ => unreachable!(),
    };
    let (mb, sb) = match b {
        Datum::Decimal { mantissa, scale } => (*mantissa, *scale),
        _ => unreachable!(),
    };

    match op {
        NumericOp::Add | NumericOp::Sub => {
            let target_scale = sa.max(sb);
            let na = ma * 10i128.pow((target_scale - sa) as u32);
            let nb = mb * 10i128.pow((target_scale - sb) as u32);
            let result = match op {
                NumericOp::Add => na.checked_add(nb),
                NumericOp::Sub => na.checked_sub(nb),
                _ => unreachable!(),
            };
            result
                .map(|m| Datum::Decimal {
                    mantissa: m,
                    scale: target_scale,
                })
                .ok_or_else(|| TypeError::ArithmeticOverflow.into())
        }
        NumericOp::Mul => {
            ma.checked_mul(mb)
                .map(|m| Datum::Decimal {
                    mantissa: m,
                    scale: sa + sb,
                })
                .ok_or_else(|| TypeError::ArithmeticOverflow.into())
        }
        NumericOp::Div => {
            if mb == 0 {
                return Err(TypeError::DivisionByZero.into());
            }
            // Increase precision of dividend to maintain scale.
            let extra_scale: u8 = 6; // additional precision digits
            let na = ma * 10i128.pow((sb + extra_scale) as u32);
            let result = na / mb;
            Ok(Datum::Decimal {
                mantissa: result,
                scale: sa + extra_scale,
            })
        }
    }
}

// ===========================================================================
// Helpers — decimal normalization
// ===========================================================================

/// Normalize two `Datum::Decimal` values to the same scale, returning
/// their mantissas.
fn decimal_normalize(a: &Datum, b: &Datum) -> (i128, i128) {
    let (ma, sa) = match a {
        Datum::Decimal { mantissa, scale } => (*mantissa, *scale),
        _ => unreachable!(),
    };
    let (mb, sb) = match b {
        Datum::Decimal { mantissa, scale } => (*mantissa, *scale),
        _ => unreachable!(),
    };
    decimal_normalize_raw(ma, sa, mb, sb)
}

fn decimal_normalize_raw(ma: i128, sa: u8, mb: i128, sb: u8) -> (i128, i128) {
    if sa == sb {
        (ma, mb)
    } else if sa > sb {
        (ma, mb * 10i128.pow((sa - sb) as u32))
    } else {
        (ma * 10i128.pow((sb - sa) as u32), mb)
    }
}

/// Strip trailing zeros from a decimal mantissa/scale pair.
fn decimal_strip_trailing_zeros(mut mantissa: i128, mut scale: u8) -> (i128, u8) {
    if mantissa == 0 {
        return (0, 0);
    }
    while scale > 0 && mantissa % 10 == 0 {
        mantissa /= 10;
        scale -= 1;
    }
    (mantissa, scale)
}

// ===========================================================================
// Helpers — deserialization primitives
// ===========================================================================

fn ensure_remaining(buf: &[u8], offset: usize, need: usize) -> Result<(), Error> {
    if offset + need > buf.len() {
        return Err(Error::Serialization(format!(
            "unexpected end of buffer at offset {offset}, need {need} bytes, have {}",
            buf.len() - offset
        )));
    }
    Ok(())
}

fn read_u8(buf: &[u8], offset: &mut usize) -> Result<u8, Error> {
    ensure_remaining(buf, *offset, 1)?;
    let v = buf[*offset];
    *offset += 1;
    Ok(v)
}

fn read_i32(buf: &[u8], offset: &mut usize) -> Result<i32, Error> {
    ensure_remaining(buf, *offset, 4)?;
    let v = i32::from_le_bytes(buf[*offset..*offset + 4].try_into().unwrap());
    *offset += 4;
    Ok(v)
}

fn read_i64(buf: &[u8], offset: &mut usize) -> Result<i64, Error> {
    ensure_remaining(buf, *offset, 8)?;
    let v = i64::from_le_bytes(buf[*offset..*offset + 8].try_into().unwrap());
    *offset += 8;
    Ok(v)
}

fn read_f64(buf: &[u8], offset: &mut usize) -> Result<f64, Error> {
    ensure_remaining(buf, *offset, 8)?;
    let v = f64::from_le_bytes(buf[*offset..*offset + 8].try_into().unwrap());
    *offset += 8;
    Ok(v)
}

fn read_i128(buf: &[u8], offset: &mut usize) -> Result<i128, Error> {
    ensure_remaining(buf, *offset, 16)?;
    let v = i128::from_le_bytes(buf[*offset..*offset + 16].try_into().unwrap());
    *offset += 16;
    Ok(v)
}

fn read_string(buf: &[u8], offset: &mut usize) -> Result<String, Error> {
    let raw = read_bytes(buf, offset)?;
    String::from_utf8(raw).map_err(|e| Error::Serialization(format!("invalid UTF-8: {e}")))
}

fn read_bytes(buf: &[u8], offset: &mut usize) -> Result<Vec<u8>, Error> {
    ensure_remaining(buf, *offset, 4)?;
    let len =
        u32::from_le_bytes(buf[*offset..*offset + 4].try_into().unwrap()) as usize;
    *offset += 4;
    ensure_remaining(buf, *offset, len)?;
    let data = buf[*offset..*offset + len].to_vec();
    *offset += len;
    Ok(data)
}

// ===========================================================================
// Unit tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datum_eq_same_type() {
        assert_eq!(Datum::Integer(42), Datum::Integer(42));
        assert_ne!(Datum::Integer(1), Datum::Integer(2));
        assert_eq!(Datum::Null, Datum::Null);
        assert_eq!(Datum::Boolean(true), Datum::Boolean(true));
        assert_eq!(
            Datum::Varchar("hi".into()),
            Datum::Varchar("hi".into())
        );
    }

    #[test]
    fn datum_eq_different_type() {
        // Strict PartialEq: different types are not equal.
        assert_ne!(Datum::Integer(1), Datum::BigInt(1));
    }

    #[test]
    fn decimal_equality_different_scale() {
        let a = Datum::Decimal { mantissa: 120, scale: 2 }; // 1.20
        let b = Datum::Decimal { mantissa: 12, scale: 1 };  // 1.2
        assert_eq!(a, b);
    }

    #[test]
    fn float_neg_zero() {
        // IEEE: 0.0 == -0.0, but our PartialEq uses to_bits so they differ.
        // That's intentional for correctness with Hash.
        assert_ne!(Datum::Float(0.0), Datum::Float(-0.0));
    }

    #[test]
    fn sql_cmp_cross_type() {
        let a = Datum::Integer(10);
        let b = Datum::BigInt(20);
        assert_eq!(a.sql_cmp(&b).unwrap(), Some(Ordering::Less));

        let c = Datum::Integer(5);
        let d = Datum::Float(5.0);
        assert_eq!(c.sql_cmp(&d).unwrap(), Some(Ordering::Equal));
    }

    #[test]
    fn sql_cmp_null() {
        let a = Datum::Integer(1);
        assert_eq!(a.sql_cmp(&Datum::Null).unwrap(), None);
    }

    #[test]
    fn coerce_integer_to_bigint() {
        let d = Datum::Integer(42);
        let coerced = d.coerce_to(&DataType::BigInt).unwrap();
        assert_eq!(coerced, Datum::BigInt(42));
    }

    #[test]
    fn coerce_date_to_timestamp() {
        let d = Datum::Date(1); // day 1
        let coerced = d.coerce_to(&DataType::Timestamp).unwrap();
        assert_eq!(coerced, Datum::Timestamp(MICROS_PER_DAY));
    }

    #[test]
    fn coerce_invalid() {
        let d = Datum::Boolean(true);
        assert!(d.coerce_to(&DataType::Integer).is_err());
    }

    #[test]
    fn arithmetic_integers() {
        let a = Datum::Integer(10);
        let b = Datum::Integer(3);
        assert_eq!(a.add(&b).unwrap(), Datum::Integer(13));
        assert_eq!(a.sub(&b).unwrap(), Datum::Integer(7));
        assert_eq!(a.mul(&b).unwrap(), Datum::Integer(30));
        assert_eq!(a.div(&b).unwrap(), Datum::Integer(3)); // integer division
    }

    #[test]
    fn arithmetic_cross_type() {
        let a = Datum::Integer(10);
        let b = Datum::Float(2.5);
        match a.add(&b).unwrap() {
            Datum::Float(v) => assert!((v - 12.5).abs() < f64::EPSILON),
            other => panic!("expected Float, got {other:?}"),
        }
    }

    #[test]
    fn arithmetic_null_propagation() {
        let a = Datum::Integer(10);
        assert!(a.add(&Datum::Null).unwrap().is_null());
    }

    #[test]
    fn arithmetic_division_by_zero() {
        let a = Datum::Integer(1);
        let b = Datum::Integer(0);
        assert!(a.div(&b).is_err());
    }

    #[test]
    fn serialize_roundtrip() {
        let datums = vec![
            Datum::Null,
            Datum::Integer(-42),
            Datum::BigInt(i64::MAX),
            Datum::Float(3.14),
            Datum::Boolean(true),
            Datum::Varchar("hello".into()),
            Datum::Text("world of text".into()),
            Datum::Timestamp(1_000_000),
            Datum::Date(19000),
            Datum::Decimal { mantissa: 12345, scale: 2 },
            Datum::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
        ];

        let mut buf = Vec::new();
        for d in &datums {
            d.serialize(&mut buf);
        }

        let mut offset = 0;
        for original in &datums {
            let decoded = Datum::deserialize(&buf, &mut offset).unwrap();
            assert_eq!(&decoded, original);
        }
        assert_eq!(offset, buf.len());
    }

    #[test]
    fn partial_ord_same_type() {
        assert!(Datum::Integer(1) < Datum::Integer(2));
        assert!(Datum::Text("a".into()) < Datum::Text("b".into()));
        assert_eq!(
            Datum::Integer(1).partial_cmp(&Datum::BigInt(1)),
            None // different types
        );
    }

    #[test]
    fn hash_consistency() {
        use std::collections::hash_map::DefaultHasher;
        fn hash_it(d: &Datum) -> u64 {
            let mut h = DefaultHasher::new();
            d.hash(&mut h);
            h.finish()
        }

        let a = Datum::Integer(42);
        let b = Datum::Integer(42);
        assert_eq!(hash_it(&a), hash_it(&b));

        // Different values → (very likely) different hashes
        let c = Datum::Integer(43);
        assert_ne!(hash_it(&a), hash_it(&c));

        // Decimal normalization: 1.20 and 1.2 should hash the same
        let d1 = Datum::Decimal { mantissa: 120, scale: 2 };
        let d2 = Datum::Decimal { mantissa: 12, scale: 1 };
        assert_eq!(hash_it(&d1), hash_it(&d2));
    }
}
