use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use crate::types::{DataType, Datum};
use crate::utils::error::{Result, SqlError, TypeError};

use super::expr_eval::datum_to_f64;

// ---------------------------------------------------------------------------
// Dispatcher
// ---------------------------------------------------------------------------

/// Dispatch a scalar function call by name.
pub fn call_scalar_function(name: &str, args: Vec<Datum>) -> Result<Datum> {
    match name {
        // --- String functions ---
        "length" | "char_length" | "character_length" => fn_length(args),
        "upper" => fn_upper(args),
        "lower" => fn_lower(args),
        "trim" | "btrim" => fn_trim(args),
        "ltrim" => fn_ltrim(args),
        "rtrim" => fn_rtrim(args),
        "substring" | "substr" => fn_substring(args),
        "position" | "strpos" => fn_position(args),
        "replace" => fn_replace(args),
        "concat" => fn_concat(args),
        "left" => fn_left(args),
        "right" => fn_right(args),
        "reverse" => fn_reverse(args),
        "lpad" => fn_lpad(args),
        "rpad" => fn_rpad(args),
        "repeat" => fn_repeat(args),
        "split_part" => fn_split_part(args),

        // --- Math functions ---
        "abs" => fn_abs(args),
        "ceil" | "ceiling" => fn_ceil(args),
        "floor" => fn_floor(args),
        "round" => fn_round(args),
        "trunc" | "truncate" => fn_trunc(args),
        "sqrt" => fn_sqrt(args),
        "power" | "pow" => fn_power(args),
        "mod" => fn_mod(args),
        "ln" => fn_ln(args),
        "log" | "log10" => fn_log(args),
        "exp" => fn_exp(args),
        "sign" => fn_sign(args),
        "random" => fn_random(args),

        // --- Date/Time functions ---
        "now" | "current_timestamp" => fn_now(args),
        "extract" => fn_extract(args),
        "date_trunc" => fn_date_trunc(args),
        "age" => fn_age(args),
        "to_char" => fn_to_char(args),

        // --- Type functions ---
        "cast" => fn_cast(args),
        "typeof" | "pg_typeof" => fn_typeof(args),

        // --- Null functions ---
        "coalesce" => fn_coalesce(args),
        "nullif" => fn_nullif(args),

        _ => Err(SqlError::ExecutionError(format!(
            "unknown function: {name}"
        ))
        .into()),
    }
}

// ===========================================================================
// String functions
// ===========================================================================

fn expect_args(name: &str, args: &[Datum], min: usize, max: usize) -> Result<()> {
    if args.len() < min || args.len() > max {
        return Err(SqlError::ExecutionError(format!(
            "{name}() expects {min}-{max} arguments, got {}",
            args.len()
        ))
        .into());
    }
    Ok(())
}

fn to_string(d: &Datum) -> Option<String> {
    if d.is_null() {
        None
    } else {
        Some(format!("{d}"))
    }
}

fn fn_length(args: Vec<Datum>) -> Result<Datum> {
    expect_args("length", &args, 1, 1)?;
    match to_string(&args[0]) {
        Some(s) => Ok(Datum::Integer(s.chars().count() as i32)),
        None => Ok(Datum::Null),
    }
}

fn fn_upper(args: Vec<Datum>) -> Result<Datum> {
    expect_args("upper", &args, 1, 1)?;
    match to_string(&args[0]) {
        Some(s) => Ok(Datum::Text(s.to_uppercase())),
        None => Ok(Datum::Null),
    }
}

fn fn_lower(args: Vec<Datum>) -> Result<Datum> {
    expect_args("lower", &args, 1, 1)?;
    match to_string(&args[0]) {
        Some(s) => Ok(Datum::Text(s.to_lowercase())),
        None => Ok(Datum::Null),
    }
}

fn fn_trim(args: Vec<Datum>) -> Result<Datum> {
    expect_args("trim", &args, 1, 2)?;
    match to_string(&args[0]) {
        None => Ok(Datum::Null),
        Some(s) => {
            if args.len() == 2 {
                if let Some(chars) = to_string(&args[1]) {
                    let chars: Vec<char> = chars.chars().collect();
                    Ok(Datum::Text(
                        s.trim_matches(|c: char| chars.contains(&c)).to_string(),
                    ))
                } else {
                    Ok(Datum::Null)
                }
            } else {
                Ok(Datum::Text(s.trim().to_string()))
            }
        }
    }
}

fn fn_ltrim(args: Vec<Datum>) -> Result<Datum> {
    expect_args("ltrim", &args, 1, 2)?;
    match to_string(&args[0]) {
        None => Ok(Datum::Null),
        Some(s) => {
            if args.len() == 2 {
                if let Some(chars) = to_string(&args[1]) {
                    let chars: Vec<char> = chars.chars().collect();
                    Ok(Datum::Text(
                        s.trim_start_matches(|c: char| chars.contains(&c))
                            .to_string(),
                    ))
                } else {
                    Ok(Datum::Null)
                }
            } else {
                Ok(Datum::Text(s.trim_start().to_string()))
            }
        }
    }
}

fn fn_rtrim(args: Vec<Datum>) -> Result<Datum> {
    expect_args("rtrim", &args, 1, 2)?;
    match to_string(&args[0]) {
        None => Ok(Datum::Null),
        Some(s) => {
            if args.len() == 2 {
                if let Some(chars) = to_string(&args[1]) {
                    let chars: Vec<char> = chars.chars().collect();
                    Ok(Datum::Text(
                        s.trim_end_matches(|c: char| chars.contains(&c))
                            .to_string(),
                    ))
                } else {
                    Ok(Datum::Null)
                }
            } else {
                Ok(Datum::Text(s.trim_end().to_string()))
            }
        }
    }
}

fn fn_substring(args: Vec<Datum>) -> Result<Datum> {
    expect_args("substring", &args, 2, 3)?;
    let s = match to_string(&args[0]) {
        Some(s) => s,
        None => return Ok(Datum::Null),
    };
    let start = match &args[1] {
        Datum::Integer(v) => *v as i64,
        Datum::BigInt(v) => *v,
        Datum::Null => return Ok(Datum::Null),
        _ => {
            return Err(
                SqlError::ExecutionError("substring: start must be integer".into()).into(),
            )
        }
    };
    // SQL SUBSTRING is 1-based
    let start_idx = if start < 1 { 0usize } else { (start - 1) as usize };

    let chars: Vec<char> = s.chars().collect();

    if args.len() == 3 {
        let len = match &args[2] {
            Datum::Integer(v) => *v as usize,
            Datum::BigInt(v) => *v as usize,
            Datum::Null => return Ok(Datum::Null),
            _ => {
                return Err(
                    SqlError::ExecutionError("substring: length must be integer".into())
                        .into(),
                )
            }
        };
        let end = (start_idx + len).min(chars.len());
        let result: String = chars[start_idx.min(chars.len())..end].iter().collect();
        Ok(Datum::Text(result))
    } else {
        let result: String = chars[start_idx.min(chars.len())..].iter().collect();
        Ok(Datum::Text(result))
    }
}

fn fn_position(args: Vec<Datum>) -> Result<Datum> {
    expect_args("position", &args, 2, 2)?;
    let sub = match to_string(&args[0]) {
        Some(s) => s,
        None => return Ok(Datum::Null),
    };
    let s = match to_string(&args[1]) {
        Some(s) => s,
        None => return Ok(Datum::Null),
    };
    // 1-based position, 0 if not found
    match s.find(&sub) {
        Some(pos) => {
            let char_pos = s[..pos].chars().count() + 1;
            Ok(Datum::Integer(char_pos as i32))
        }
        None => Ok(Datum::Integer(0)),
    }
}

fn fn_replace(args: Vec<Datum>) -> Result<Datum> {
    expect_args("replace", &args, 3, 3)?;
    let s = match to_string(&args[0]) {
        Some(s) => s,
        None => return Ok(Datum::Null),
    };
    let from = match to_string(&args[1]) {
        Some(s) => s,
        None => return Ok(Datum::Null),
    };
    let to = match to_string(&args[2]) {
        Some(s) => s,
        None => return Ok(Datum::Null),
    };
    Ok(Datum::Text(s.replace(&from, &to)))
}

fn fn_concat(args: Vec<Datum>) -> Result<Datum> {
    let mut result = String::new();
    for arg in &args {
        if !arg.is_null() {
            result.push_str(&format!("{arg}"));
        }
    }
    Ok(Datum::Text(result))
}

fn fn_left(args: Vec<Datum>) -> Result<Datum> {
    expect_args("left", &args, 2, 2)?;
    let s = match to_string(&args[0]) {
        Some(s) => s,
        None => return Ok(Datum::Null),
    };
    let n = match &args[1] {
        Datum::Integer(v) => *v as i64,
        Datum::BigInt(v) => *v,
        Datum::Null => return Ok(Datum::Null),
        _ => return Err(SqlError::ExecutionError("left: n must be integer".into()).into()),
    };
    let chars: Vec<char> = s.chars().collect();
    if n < 0 {
        let take = (chars.len() as i64 + n).max(0) as usize;
        Ok(Datum::Text(chars[..take].iter().collect()))
    } else {
        let take = (n as usize).min(chars.len());
        Ok(Datum::Text(chars[..take].iter().collect()))
    }
}

fn fn_right(args: Vec<Datum>) -> Result<Datum> {
    expect_args("right", &args, 2, 2)?;
    let s = match to_string(&args[0]) {
        Some(s) => s,
        None => return Ok(Datum::Null),
    };
    let n = match &args[1] {
        Datum::Integer(v) => *v as i64,
        Datum::BigInt(v) => *v,
        Datum::Null => return Ok(Datum::Null),
        _ => {
            return Err(SqlError::ExecutionError("right: n must be integer".into()).into())
        }
    };
    let chars: Vec<char> = s.chars().collect();
    if n < 0 {
        let skip = (-n).min(chars.len() as i64) as usize;
        Ok(Datum::Text(chars[skip..].iter().collect()))
    } else {
        let take = (n as usize).min(chars.len());
        let start = chars.len() - take;
        Ok(Datum::Text(chars[start..].iter().collect()))
    }
}

fn fn_reverse(args: Vec<Datum>) -> Result<Datum> {
    expect_args("reverse", &args, 1, 1)?;
    match to_string(&args[0]) {
        Some(s) => Ok(Datum::Text(s.chars().rev().collect())),
        None => Ok(Datum::Null),
    }
}

fn fn_lpad(args: Vec<Datum>) -> Result<Datum> {
    expect_args("lpad", &args, 2, 3)?;
    let s = match to_string(&args[0]) {
        Some(s) => s,
        None => return Ok(Datum::Null),
    };
    let len = match &args[1] {
        Datum::Integer(v) => *v as usize,
        Datum::BigInt(v) => *v as usize,
        Datum::Null => return Ok(Datum::Null),
        _ => return Err(SqlError::ExecutionError("lpad: length must be integer".into()).into()),
    };
    let fill = if args.len() == 3 {
        match to_string(&args[2]) {
            Some(s) => s,
            None => return Ok(Datum::Null),
        }
    } else {
        " ".to_string()
    };

    let chars: Vec<char> = s.chars().collect();
    if chars.len() >= len {
        return Ok(Datum::Text(chars[..len].iter().collect()));
    }
    let needed = len - chars.len();
    let fill_chars: Vec<char> = fill.chars().collect();
    if fill_chars.is_empty() {
        return Ok(Datum::Text(s));
    }
    let mut result: Vec<char> = Vec::with_capacity(len);
    for i in 0..needed {
        result.push(fill_chars[i % fill_chars.len()]);
    }
    result.extend_from_slice(&chars);
    Ok(Datum::Text(result.into_iter().collect()))
}

fn fn_rpad(args: Vec<Datum>) -> Result<Datum> {
    expect_args("rpad", &args, 2, 3)?;
    let s = match to_string(&args[0]) {
        Some(s) => s,
        None => return Ok(Datum::Null),
    };
    let len = match &args[1] {
        Datum::Integer(v) => *v as usize,
        Datum::BigInt(v) => *v as usize,
        Datum::Null => return Ok(Datum::Null),
        _ => return Err(SqlError::ExecutionError("rpad: length must be integer".into()).into()),
    };
    let fill = if args.len() == 3 {
        match to_string(&args[2]) {
            Some(s) => s,
            None => return Ok(Datum::Null),
        }
    } else {
        " ".to_string()
    };

    let chars: Vec<char> = s.chars().collect();
    if chars.len() >= len {
        return Ok(Datum::Text(chars[..len].iter().collect()));
    }
    let needed = len - chars.len();
    let fill_chars: Vec<char> = fill.chars().collect();
    if fill_chars.is_empty() {
        return Ok(Datum::Text(s));
    }
    let mut result: Vec<char> = chars;
    for i in 0..needed {
        result.push(fill_chars[i % fill_chars.len()]);
    }
    Ok(Datum::Text(result.into_iter().collect()))
}

fn fn_repeat(args: Vec<Datum>) -> Result<Datum> {
    expect_args("repeat", &args, 2, 2)?;
    let s = match to_string(&args[0]) {
        Some(s) => s,
        None => return Ok(Datum::Null),
    };
    let n = match &args[1] {
        Datum::Integer(v) => *v as usize,
        Datum::BigInt(v) => *v as usize,
        Datum::Null => return Ok(Datum::Null),
        _ => {
            return Err(
                SqlError::ExecutionError("repeat: count must be integer".into()).into(),
            )
        }
    };
    Ok(Datum::Text(s.repeat(n)))
}

fn fn_split_part(args: Vec<Datum>) -> Result<Datum> {
    expect_args("split_part", &args, 3, 3)?;
    let s = match to_string(&args[0]) {
        Some(s) => s,
        None => return Ok(Datum::Null),
    };
    let delim = match to_string(&args[1]) {
        Some(s) => s,
        None => return Ok(Datum::Null),
    };
    let n = match &args[2] {
        Datum::Integer(v) => *v,
        Datum::BigInt(v) => *v as i32,
        Datum::Null => return Ok(Datum::Null),
        _ => {
            return Err(
                SqlError::ExecutionError("split_part: field must be integer".into()).into(),
            )
        }
    };
    if n < 1 {
        return Err(
            SqlError::ExecutionError("split_part: field number must be >= 1".into()).into(),
        );
    }
    let parts: Vec<&str> = s.split(&delim).collect();
    let idx = (n - 1) as usize;
    if idx < parts.len() {
        Ok(Datum::Text(parts[idx].to_string()))
    } else {
        Ok(Datum::Text(String::new()))
    }
}

// ===========================================================================
// Math functions
// ===========================================================================

fn fn_abs(args: Vec<Datum>) -> Result<Datum> {
    expect_args("abs", &args, 1, 1)?;
    if args[0].is_null() {
        return Ok(Datum::Null);
    }
    match &args[0] {
        Datum::Integer(v) => Ok(Datum::Integer(v.abs())),
        Datum::BigInt(v) => Ok(Datum::BigInt(v.abs())),
        Datum::Float(v) => Ok(Datum::Float(v.abs())),
        _ => {
            let f = datum_to_f64(&args[0])?;
            Ok(Datum::Float(f.abs()))
        }
    }
}

fn fn_ceil(args: Vec<Datum>) -> Result<Datum> {
    expect_args("ceil", &args, 1, 1)?;
    if args[0].is_null() {
        return Ok(Datum::Null);
    }
    let f = datum_to_f64(&args[0])?;
    Ok(Datum::Float(f.ceil()))
}

fn fn_floor(args: Vec<Datum>) -> Result<Datum> {
    expect_args("floor", &args, 1, 1)?;
    if args[0].is_null() {
        return Ok(Datum::Null);
    }
    let f = datum_to_f64(&args[0])?;
    Ok(Datum::Float(f.floor()))
}

fn fn_round(args: Vec<Datum>) -> Result<Datum> {
    expect_args("round", &args, 1, 2)?;
    if args[0].is_null() {
        return Ok(Datum::Null);
    }
    let f = datum_to_f64(&args[0])?;
    if args.len() == 2 {
        let places = match &args[1] {
            Datum::Integer(v) => *v,
            Datum::BigInt(v) => *v as i32,
            Datum::Null => return Ok(Datum::Null),
            _ => 0,
        };
        let factor = 10f64.powi(places);
        Ok(Datum::Float((f * factor).round() / factor))
    } else {
        Ok(Datum::Float(f.round()))
    }
}

fn fn_trunc(args: Vec<Datum>) -> Result<Datum> {
    expect_args("trunc", &args, 1, 2)?;
    if args[0].is_null() {
        return Ok(Datum::Null);
    }
    let f = datum_to_f64(&args[0])?;
    if args.len() == 2 {
        let places = match &args[1] {
            Datum::Integer(v) => *v,
            Datum::BigInt(v) => *v as i32,
            Datum::Null => return Ok(Datum::Null),
            _ => 0,
        };
        let factor = 10f64.powi(places);
        Ok(Datum::Float((f * factor).trunc() / factor))
    } else {
        Ok(Datum::Float(f.trunc()))
    }
}

fn fn_sqrt(args: Vec<Datum>) -> Result<Datum> {
    expect_args("sqrt", &args, 1, 1)?;
    if args[0].is_null() {
        return Ok(Datum::Null);
    }
    let f = datum_to_f64(&args[0])?;
    Ok(Datum::Float(f.sqrt()))
}

fn fn_power(args: Vec<Datum>) -> Result<Datum> {
    expect_args("power", &args, 2, 2)?;
    if args[0].is_null() || args[1].is_null() {
        return Ok(Datum::Null);
    }
    let base = datum_to_f64(&args[0])?;
    let exp = datum_to_f64(&args[1])?;
    Ok(Datum::Float(base.powf(exp)))
}

fn fn_mod(args: Vec<Datum>) -> Result<Datum> {
    expect_args("mod", &args, 2, 2)?;
    if args[0].is_null() || args[1].is_null() {
        return Ok(Datum::Null);
    }
    let a = datum_to_f64(&args[0])?;
    let b = datum_to_f64(&args[1])?;
    if b == 0.0 {
        return Err(TypeError::DivisionByZero.into());
    }
    Ok(Datum::Float(a % b))
}

fn fn_ln(args: Vec<Datum>) -> Result<Datum> {
    expect_args("ln", &args, 1, 1)?;
    if args[0].is_null() {
        return Ok(Datum::Null);
    }
    let f = datum_to_f64(&args[0])?;
    Ok(Datum::Float(f.ln()))
}

fn fn_log(args: Vec<Datum>) -> Result<Datum> {
    expect_args("log", &args, 1, 2)?;
    if args[0].is_null() {
        return Ok(Datum::Null);
    }
    if args.len() == 2 {
        if args[1].is_null() {
            return Ok(Datum::Null);
        }
        let base = datum_to_f64(&args[0])?;
        let val = datum_to_f64(&args[1])?;
        Ok(Datum::Float(val.log(base)))
    } else {
        let f = datum_to_f64(&args[0])?;
        Ok(Datum::Float(f.log10()))
    }
}

fn fn_exp(args: Vec<Datum>) -> Result<Datum> {
    expect_args("exp", &args, 1, 1)?;
    if args[0].is_null() {
        return Ok(Datum::Null);
    }
    let f = datum_to_f64(&args[0])?;
    Ok(Datum::Float(f.exp()))
}

fn fn_sign(args: Vec<Datum>) -> Result<Datum> {
    expect_args("sign", &args, 1, 1)?;
    if args[0].is_null() {
        return Ok(Datum::Null);
    }
    let f = datum_to_f64(&args[0])?;
    let s = if f > 0.0 {
        1.0
    } else if f < 0.0 {
        -1.0
    } else {
        0.0
    };
    Ok(Datum::Float(s))
}

/// Simple xorshift64 PRNG (no external dependency).
static RANDOM_STATE: AtomicU64 = AtomicU64::new(0x123456789ABCDEF0);

fn fn_random(_args: Vec<Datum>) -> Result<Datum> {
    let mut state = RANDOM_STATE.load(AtomicOrdering::Relaxed);
    if state == 0 {
        state = 0x123456789ABCDEF0;
    }
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    RANDOM_STATE.store(state, AtomicOrdering::Relaxed);
    // Map to [0.0, 1.0)
    let f = (state >> 11) as f64 / ((1u64 << 53) as f64);
    Ok(Datum::Float(f))
}

// ===========================================================================
// Date/Time functions
// ===========================================================================

const MICROS_PER_SECOND: i64 = 1_000_000;
const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SECOND;
const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;
const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;
const SECONDS_PER_DAY: i64 = 86_400;

fn fn_now(_args: Vec<Datum>) -> Result<Datum> {
    // Return current timestamp as microseconds since epoch
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let micros = now.as_micros() as i64;
    Ok(Datum::Timestamp(micros))
}

fn fn_extract(args: Vec<Datum>) -> Result<Datum> {
    expect_args("extract", &args, 2, 2)?;
    let field = match to_string(&args[0]) {
        Some(s) => s.to_ascii_lowercase(),
        None => return Ok(Datum::Null),
    };
    if args[1].is_null() {
        return Ok(Datum::Null);
    }

    let micros = match &args[1] {
        Datum::Timestamp(ts) => *ts,
        Datum::Date(d) => *d as i64 * MICROS_PER_DAY,
        _ => {
            return Err(
                SqlError::ExecutionError("extract: second arg must be temporal".into())
                    .into(),
            )
        }
    };

    let total_secs = micros / MICROS_PER_SECOND;
    let (year, month, day, hour, min, sec) = timestamp_to_parts(total_secs);

    let result = match field.as_str() {
        "year" => year as f64,
        "month" => month as f64,
        "day" => day as f64,
        "hour" => hour as f64,
        "minute" => min as f64,
        "second" => sec as f64 + (micros % MICROS_PER_SECOND) as f64 / 1_000_000.0,
        "epoch" => micros as f64 / 1_000_000.0,
        "dow" | "dayofweek" => {
            // Days since epoch (1970-01-01 = Thursday = 4)
            let days = if total_secs >= 0 {
                total_secs / SECONDS_PER_DAY
            } else {
                (total_secs - SECONDS_PER_DAY + 1) / SECONDS_PER_DAY
            };
            ((days % 7 + 4) % 7) as f64 // 0=Sunday
        }
        "doy" | "dayofyear" => {
            day_of_year(year, month as u32, day as u32) as f64
        }
        _ => {
            return Err(SqlError::ExecutionError(format!(
                "extract: unknown field '{field}'"
            ))
            .into())
        }
    };
    Ok(Datum::Float(result))
}

fn fn_date_trunc(args: Vec<Datum>) -> Result<Datum> {
    expect_args("date_trunc", &args, 2, 2)?;
    let field = match to_string(&args[0]) {
        Some(s) => s.to_ascii_lowercase(),
        None => return Ok(Datum::Null),
    };
    if args[1].is_null() {
        return Ok(Datum::Null);
    }

    let micros = match &args[1] {
        Datum::Timestamp(ts) => *ts,
        Datum::Date(d) => *d as i64 * MICROS_PER_DAY,
        _ => {
            return Err(
                SqlError::ExecutionError("date_trunc: second arg must be temporal".into())
                    .into(),
            )
        }
    };

    let total_secs = micros / MICROS_PER_SECOND;
    let (year, month, day, hour, min, _sec) = timestamp_to_parts(total_secs);

    let truncated = match field.as_str() {
        "year" => parts_to_timestamp(year, 1, 1, 0, 0, 0),
        "month" => parts_to_timestamp(year, month, 1, 0, 0, 0),
        "day" => parts_to_timestamp(year, month, day, 0, 0, 0),
        "hour" => parts_to_timestamp(year, month, day, hour, 0, 0),
        "minute" => parts_to_timestamp(year, month, day, hour, min, 0),
        "second" => parts_to_timestamp(year, month, day, hour, min, _sec),
        _ => {
            return Err(SqlError::ExecutionError(format!(
                "date_trunc: unknown field '{field}'"
            ))
            .into())
        }
    };
    Ok(Datum::Timestamp(truncated * MICROS_PER_SECOND))
}

fn fn_age(args: Vec<Datum>) -> Result<Datum> {
    expect_args("age", &args, 1, 2)?;
    let ts1 = match &args[0] {
        Datum::Timestamp(t) => *t,
        Datum::Date(d) => *d as i64 * MICROS_PER_DAY,
        Datum::Null => return Ok(Datum::Null),
        _ => {
            return Err(
                SqlError::ExecutionError("age: argument must be temporal".into()).into(),
            )
        }
    };

    let ts2 = if args.len() == 2 {
        match &args[1] {
            Datum::Timestamp(t) => *t,
            Datum::Date(d) => *d as i64 * MICROS_PER_DAY,
            Datum::Null => return Ok(Datum::Null),
            _ => {
                return Err(
                    SqlError::ExecutionError("age: argument must be temporal".into())
                        .into(),
                )
            }
        }
    } else {
        // age(ts) = now - ts
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        now.as_micros() as i64
    };

    // Return difference as microseconds (interval)
    let diff = ts1 - ts2;
    Ok(Datum::BigInt(diff))
}

fn fn_to_char(args: Vec<Datum>) -> Result<Datum> {
    expect_args("to_char", &args, 2, 2)?;
    if args[0].is_null() || args[1].is_null() {
        return Ok(Datum::Null);
    }
    let micros = match &args[0] {
        Datum::Timestamp(t) => *t,
        Datum::Date(d) => *d as i64 * MICROS_PER_DAY,
        _ => return Ok(Datum::Text(format!("{}", args[0]))),
    };
    let total_secs = micros / MICROS_PER_SECOND;
    let (year, month, day, hour, min, sec) = timestamp_to_parts(total_secs);
    let fmt = match to_string(&args[1]) {
        Some(s) => s,
        None => return Ok(Datum::Null),
    };
    // Simple format substitution
    let result = fmt
        .replace("YYYY", &format!("{year:04}"))
        .replace("MM", &format!("{month:02}"))
        .replace("DD", &format!("{day:02}"))
        .replace("HH24", &format!("{hour:02}"))
        .replace("HH", &format!("{hour:02}"))
        .replace("MI", &format!("{min:02}"))
        .replace("SS", &format!("{sec:02}"));
    Ok(Datum::Text(result))
}

// ===========================================================================
// Type functions
// ===========================================================================

fn fn_cast(args: Vec<Datum>) -> Result<Datum> {
    // CAST is normally handled by ExprOp::Cast, but provided here for
    // completeness when called as a function.
    if args.len() != 2 {
        return Err(
            SqlError::ExecutionError("cast() requires 2 arguments".into()).into(),
        );
    }
    // Second arg is the target type name as a string
    let type_name = match to_string(&args[1]) {
        Some(s) => s.to_ascii_uppercase(),
        None => return Ok(args[0].clone()),
    };
    let dt = match type_name.as_str() {
        "INTEGER" | "INT" => DataType::Integer,
        "BIGINT" => DataType::BigInt,
        "FLOAT" | "DOUBLE" | "REAL" => DataType::Float,
        "BOOLEAN" | "BOOL" => DataType::Boolean,
        "TEXT" => DataType::Text,
        _ => DataType::Text,
    };
    super::expr_eval::eval_expr_direct(
        &crate::sql::ast::Expr::Null, // dummy, we manually cast
        &crate::types::Tuple::new(
            crate::types::MvccHeader::new(0, 0, 0),
            vec![],
        ),
        &crate::types::Schema::empty(),
    )?;
    args[0].coerce_to(&dt).or_else(|_| Ok(args[0].clone()))
}

fn fn_typeof(args: Vec<Datum>) -> Result<Datum> {
    expect_args("typeof", &args, 1, 1)?;
    Ok(Datum::Text(args[0].type_name().to_string()))
}

// ===========================================================================
// Null functions
// ===========================================================================

fn fn_coalesce(args: Vec<Datum>) -> Result<Datum> {
    Ok(args
        .into_iter()
        .find(|d| !d.is_null())
        .unwrap_or(Datum::Null))
}

fn fn_nullif(args: Vec<Datum>) -> Result<Datum> {
    expect_args("nullif", &args, 2, 2)?;
    if args[0].sql_cmp(&args[1])? == Some(std::cmp::Ordering::Equal) {
        Ok(Datum::Null)
    } else {
        Ok(args[0].clone())
    }
}

// ===========================================================================
// Date/time helpers
// ===========================================================================

/// Convert Unix timestamp (seconds since epoch) to (year, month, day, hour, min, sec).
fn timestamp_to_parts(total_secs: i64) -> (i32, i32, i32, i32, i32, i32) {
    let secs_in_day = total_secs.rem_euclid(SECONDS_PER_DAY);
    let hour = (secs_in_day / 3600) as i32;
    let min = ((secs_in_day % 3600) / 60) as i32;
    let sec = (secs_in_day % 60) as i32;

    let mut days = total_secs.div_euclid(SECONDS_PER_DAY);
    // Epoch: 1970-01-01
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
    days += 719_468; // shift to 0000-03-01
    let era = if days >= 0 {
        days / 146_097
    } else {
        (days - 146_096) / 146_097
    };
    let doe = (days - era * 146_097) as u32; // day of era [0, 146096]
    let yoe =
        (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // year of era
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // day of year [0, 365]
    let mp = (5 * doy + 2) / 153; // month [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };

    (y as i32, m as i32, d as i32, hour, min, sec)
}

/// Convert date parts to Unix timestamp (seconds since epoch).
fn parts_to_timestamp(year: i32, month: i32, day: i32, hour: i32, min: i32, sec: i32) -> i64 {
    // Inverse of timestamp_to_parts using the same algorithm
    let (y, m) = if month <= 2 {
        (year as i64 - 1, (month + 9) as u32)
    } else {
        (year as i64, (month - 3) as u32)
    };
    let era = if y >= 0 {
        y / 400
    } else {
        (y - 399) / 400
    };
    let yoe = (y - era * 400) as u32;
    let doy = (153 * m + 2) / 5 + day as u32 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe as i64 - 719_468;
    days * SECONDS_PER_DAY + hour as i64 * 3600 + min as i64 * 60 + sec as i64
}

fn day_of_year(year: i32, month: u32, day: u32) -> u32 {
    let is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    let month_days: [u32; 12] = [31, if is_leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let mut doy = 0u32;
    for i in 0..(month as usize - 1).min(11) {
        doy += month_days[i];
    }
    doy + day
}
