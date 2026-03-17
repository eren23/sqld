use sqld::executor::scalar_functions::call_scalar_function;
use sqld::types::Datum;

// ===========================================================================
// Helpers
// ===========================================================================

fn call(name: &str, args: Vec<Datum>) -> Datum {
    call_scalar_function(name, args).expect(&format!("{name}() should not error"))
}

fn assert_text(result: Datum, expected: &str) {
    match result {
        Datum::Text(s) => assert_eq!(s, expected),
        other => panic!("expected Text(\"{expected}\"), got {other:?}"),
    }
}

fn assert_integer(result: Datum, expected: i32) {
    match result {
        Datum::Integer(v) => assert_eq!(v, expected),
        other => panic!("expected Integer({expected}), got {other:?}"),
    }
}

fn assert_float(result: Datum, expected: f64) {
    match result {
        Datum::Float(v) => assert!(
            (v - expected).abs() < 1e-9,
            "expected Float({expected}), got Float({v})"
        ),
        other => panic!("expected Float({expected}), got {other:?}"),
    }
}

fn assert_float_approx(result: Datum, expected: f64, tolerance: f64) {
    match result {
        Datum::Float(v) => assert!(
            (v - expected).abs() < tolerance,
            "expected Float(~{expected}), got Float({v})"
        ),
        other => panic!("expected Float(~{expected}), got {other:?}"),
    }
}

fn assert_null(result: Datum) {
    assert!(result.is_null(), "expected Null, got {result:?}");
}

// ===========================================================================
// String functions
// ===========================================================================

#[test]
fn test_length() {
    assert_integer(
        call("length", vec![Datum::Text("hello".into())]),
        5,
    );
    assert_integer(
        call("length", vec![Datum::Text("".into())]),
        0,
    );
    assert_null(call("length", vec![Datum::Null]));
}

#[test]
fn test_upper() {
    assert_text(
        call("upper", vec![Datum::Text("hello".into())]),
        "HELLO",
    );
    assert_text(
        call("upper", vec![Datum::Text("Hello World".into())]),
        "HELLO WORLD",
    );
    assert_null(call("upper", vec![Datum::Null]));
}

#[test]
fn test_lower() {
    assert_text(
        call("lower", vec![Datum::Text("HELLO".into())]),
        "hello",
    );
    assert_text(
        call("lower", vec![Datum::Text("Hello World".into())]),
        "hello world",
    );
    assert_null(call("lower", vec![Datum::Null]));
}

#[test]
fn test_trim() {
    assert_text(
        call("trim", vec![Datum::Text("  hi  ".into())]),
        "hi",
    );
    assert_text(
        call("trim", vec![Datum::Text("hi".into())]),
        "hi",
    );
    assert_text(
        call("trim", vec![Datum::Text("   ".into())]),
        "",
    );
    assert_null(call("trim", vec![Datum::Null]));
}

#[test]
fn test_ltrim() {
    assert_text(
        call("ltrim", vec![Datum::Text("  hi".into())]),
        "hi",
    );
    assert_text(
        call("ltrim", vec![Datum::Text("  hi  ".into())]),
        "hi  ",
    );
    assert_null(call("ltrim", vec![Datum::Null]));
}

#[test]
fn test_rtrim() {
    assert_text(
        call("rtrim", vec![Datum::Text("hi  ".into())]),
        "hi",
    );
    assert_text(
        call("rtrim", vec![Datum::Text("  hi  ".into())]),
        "  hi",
    );
    assert_null(call("rtrim", vec![Datum::Null]));
}

#[test]
fn test_substring() {
    // SQL SUBSTRING is 1-based: substring("hello", 2, 3) = "ell"
    assert_text(
        call(
            "substring",
            vec![
                Datum::Text("hello".into()),
                Datum::Integer(2),
                Datum::Integer(3),
            ],
        ),
        "ell",
    );
    // Without length: substring("hello", 2) = "ello"
    assert_text(
        call(
            "substring",
            vec![Datum::Text("hello".into()), Datum::Integer(2)],
        ),
        "ello",
    );
    // Start at 1
    assert_text(
        call(
            "substring",
            vec![
                Datum::Text("hello".into()),
                Datum::Integer(1),
                Datum::Integer(5),
            ],
        ),
        "hello",
    );
    assert_null(call("substring", vec![Datum::Null, Datum::Integer(1)]));
    assert_null(
        call("substring", vec![Datum::Text("hello".into()), Datum::Null]),
    );
}

#[test]
fn test_position() {
    // position("lo", "hello") = 4 (1-based)
    assert_integer(
        call(
            "position",
            vec![
                Datum::Text("lo".into()),
                Datum::Text("hello".into()),
            ],
        ),
        4,
    );
    // Not found => 0
    assert_integer(
        call(
            "position",
            vec![
                Datum::Text("xyz".into()),
                Datum::Text("hello".into()),
            ],
        ),
        0,
    );
    assert_null(
        call("position", vec![Datum::Null, Datum::Text("hello".into())]),
    );
    assert_null(
        call("position", vec![Datum::Text("lo".into()), Datum::Null]),
    );
}

#[test]
fn test_replace() {
    assert_text(
        call(
            "replace",
            vec![
                Datum::Text("hello".into()),
                Datum::Text("l".into()),
                Datum::Text("r".into()),
            ],
        ),
        "herro",
    );
    // No match => unchanged
    assert_text(
        call(
            "replace",
            vec![
                Datum::Text("hello".into()),
                Datum::Text("z".into()),
                Datum::Text("r".into()),
            ],
        ),
        "hello",
    );
    assert_null(
        call(
            "replace",
            vec![
                Datum::Null,
                Datum::Text("l".into()),
                Datum::Text("r".into()),
            ],
        ),
    );
}

#[test]
fn test_concat() {
    assert_text(
        call(
            "concat",
            vec![
                Datum::Text("a".into()),
                Datum::Text("b".into()),
                Datum::Text("c".into()),
            ],
        ),
        "abc",
    );
    // concat with NULL skips the NULL
    assert_text(
        call(
            "concat",
            vec![
                Datum::Text("a".into()),
                Datum::Null,
                Datum::Text("c".into()),
            ],
        ),
        "ac",
    );
    // All NULLs => empty string
    assert_text(
        call("concat", vec![Datum::Null, Datum::Null]),
        "",
    );
    // concat with no args
    assert_text(call("concat", vec![]), "");
}

#[test]
fn test_left() {
    assert_text(
        call(
            "left",
            vec![Datum::Text("hello".into()), Datum::Integer(3)],
        ),
        "hel",
    );
    // n > length => whole string
    assert_text(
        call(
            "left",
            vec![Datum::Text("hi".into()), Datum::Integer(10)],
        ),
        "hi",
    );
    // n = 0 => empty
    assert_text(
        call(
            "left",
            vec![Datum::Text("hello".into()), Datum::Integer(0)],
        ),
        "",
    );
    assert_null(
        call("left", vec![Datum::Null, Datum::Integer(3)]),
    );
    assert_null(
        call("left", vec![Datum::Text("hello".into()), Datum::Null]),
    );
}

#[test]
fn test_right() {
    assert_text(
        call(
            "right",
            vec![Datum::Text("hello".into()), Datum::Integer(3)],
        ),
        "llo",
    );
    // n > length => whole string
    assert_text(
        call(
            "right",
            vec![Datum::Text("hi".into()), Datum::Integer(10)],
        ),
        "hi",
    );
    // n = 0 => empty
    assert_text(
        call(
            "right",
            vec![Datum::Text("hello".into()), Datum::Integer(0)],
        ),
        "",
    );
    assert_null(
        call("right", vec![Datum::Null, Datum::Integer(3)]),
    );
    assert_null(
        call("right", vec![Datum::Text("hello".into()), Datum::Null]),
    );
}

#[test]
fn test_reverse() {
    assert_text(
        call("reverse", vec![Datum::Text("hello".into())]),
        "olleh",
    );
    assert_text(
        call("reverse", vec![Datum::Text("".into())]),
        "",
    );
    assert_text(
        call("reverse", vec![Datum::Text("a".into())]),
        "a",
    );
    assert_null(call("reverse", vec![Datum::Null]));
}

#[test]
fn test_lpad() {
    // Default fill is space
    assert_text(
        call(
            "lpad",
            vec![Datum::Text("hi".into()), Datum::Integer(5)],
        ),
        "   hi",
    );
    // Custom fill character
    assert_text(
        call(
            "lpad",
            vec![
                Datum::Text("hi".into()),
                Datum::Integer(5),
                Datum::Text("*".into()),
            ],
        ),
        "***hi",
    );
    // String already long enough => truncate to length
    assert_text(
        call(
            "lpad",
            vec![Datum::Text("hello".into()), Datum::Integer(3)],
        ),
        "hel",
    );
    assert_null(
        call("lpad", vec![Datum::Null, Datum::Integer(5)]),
    );
}

#[test]
fn test_rpad() {
    assert_text(
        call(
            "rpad",
            vec![
                Datum::Text("hi".into()),
                Datum::Integer(5),
                Datum::Text("*".into()),
            ],
        ),
        "hi***",
    );
    // Default fill is space
    assert_text(
        call(
            "rpad",
            vec![Datum::Text("hi".into()), Datum::Integer(5)],
        ),
        "hi   ",
    );
    // String already long enough => truncate to length
    assert_text(
        call(
            "rpad",
            vec![Datum::Text("hello".into()), Datum::Integer(3)],
        ),
        "hel",
    );
    assert_null(
        call("rpad", vec![Datum::Null, Datum::Integer(5)]),
    );
}

#[test]
fn test_repeat() {
    assert_text(
        call(
            "repeat",
            vec![Datum::Text("ab".into()), Datum::Integer(3)],
        ),
        "ababab",
    );
    assert_text(
        call(
            "repeat",
            vec![Datum::Text("x".into()), Datum::Integer(0)],
        ),
        "",
    );
    assert_null(
        call("repeat", vec![Datum::Null, Datum::Integer(3)]),
    );
    assert_null(
        call("repeat", vec![Datum::Text("ab".into()), Datum::Null]),
    );
}

#[test]
fn test_split_part() {
    assert_text(
        call(
            "split_part",
            vec![
                Datum::Text("a.b.c".into()),
                Datum::Text(".".into()),
                Datum::Integer(2),
            ],
        ),
        "b",
    );
    // First part
    assert_text(
        call(
            "split_part",
            vec![
                Datum::Text("a.b.c".into()),
                Datum::Text(".".into()),
                Datum::Integer(1),
            ],
        ),
        "a",
    );
    // Last part
    assert_text(
        call(
            "split_part",
            vec![
                Datum::Text("a.b.c".into()),
                Datum::Text(".".into()),
                Datum::Integer(3),
            ],
        ),
        "c",
    );
    // Out of bounds => empty string
    assert_text(
        call(
            "split_part",
            vec![
                Datum::Text("a.b.c".into()),
                Datum::Text(".".into()),
                Datum::Integer(5),
            ],
        ),
        "",
    );
    assert_null(
        call(
            "split_part",
            vec![
                Datum::Null,
                Datum::Text(".".into()),
                Datum::Integer(1),
            ],
        ),
    );
}

// ===========================================================================
// Math functions
// ===========================================================================

#[test]
fn test_abs() {
    // Integer
    assert_integer(call("abs", vec![Datum::Integer(-42)]), 42);
    assert_integer(call("abs", vec![Datum::Integer(42)]), 42);
    assert_integer(call("abs", vec![Datum::Integer(0)]), 0);

    // Float
    assert_float(call("abs", vec![Datum::Float(3.14)]), 3.14);
    assert_float(call("abs", vec![Datum::Float(-3.14)]), 3.14);

    // BigInt
    match call("abs", vec![Datum::BigInt(-100)]) {
        Datum::BigInt(v) => assert_eq!(v, 100),
        other => panic!("expected BigInt(100), got {other:?}"),
    }

    assert_null(call("abs", vec![Datum::Null]));
}

#[test]
fn test_ceil() {
    assert_float(call("ceil", vec![Datum::Float(3.2)]), 4.0);
    assert_float(call("ceil", vec![Datum::Float(3.0)]), 3.0);
    assert_float(call("ceil", vec![Datum::Float(-3.2)]), -3.0);
    // Integer input coerced to float
    assert_float(call("ceil", vec![Datum::Integer(5)]), 5.0);
    assert_null(call("ceil", vec![Datum::Null]));
}

#[test]
fn test_floor() {
    assert_float(call("floor", vec![Datum::Float(3.8)]), 3.0);
    assert_float(call("floor", vec![Datum::Float(3.0)]), 3.0);
    assert_float(call("floor", vec![Datum::Float(-3.2)]), -4.0);
    assert_float(call("floor", vec![Datum::Integer(5)]), 5.0);
    assert_null(call("floor", vec![Datum::Null]));
}

#[test]
fn test_round() {
    // round(3.456, 2) = 3.46
    assert_float(
        call(
            "round",
            vec![Datum::Float(3.456), Datum::Integer(2)],
        ),
        3.46,
    );
    // round(3.5) = 4.0
    assert_float(call("round", vec![Datum::Float(3.5)]), 4.0);
    // round(3.4) = 3.0
    assert_float(call("round", vec![Datum::Float(3.4)]), 3.0);
    // round with 0 decimal places
    assert_float(
        call(
            "round",
            vec![Datum::Float(3.456), Datum::Integer(0)],
        ),
        3.0,
    );
    assert_null(call("round", vec![Datum::Null]));
    assert_null(
        call("round", vec![Datum::Float(3.456), Datum::Null]),
    );
}

#[test]
fn test_trunc() {
    // trunc(3.456, 2) = 3.45
    assert_float(
        call(
            "trunc",
            vec![Datum::Float(3.456), Datum::Integer(2)],
        ),
        3.45,
    );
    // trunc(3.9) = 3.0
    assert_float(call("trunc", vec![Datum::Float(3.9)]), 3.0);
    // trunc(-3.9) = -3.0
    assert_float(call("trunc", vec![Datum::Float(-3.9)]), -3.0);
    assert_null(call("trunc", vec![Datum::Null]));
}

#[test]
fn test_sqrt() {
    assert_float(call("sqrt", vec![Datum::Float(16.0)]), 4.0);
    assert_float(call("sqrt", vec![Datum::Float(0.0)]), 0.0);
    assert_float(call("sqrt", vec![Datum::Float(2.0)]), std::f64::consts::SQRT_2);
    assert_null(call("sqrt", vec![Datum::Null]));
}

#[test]
fn test_power() {
    assert_float(
        call("power", vec![Datum::Float(2.0), Datum::Float(3.0)]),
        8.0,
    );
    assert_float(
        call("power", vec![Datum::Float(10.0), Datum::Float(0.0)]),
        1.0,
    );
    assert_float(
        call("power", vec![Datum::Float(5.0), Datum::Float(1.0)]),
        5.0,
    );
    assert_null(
        call("power", vec![Datum::Null, Datum::Float(2.0)]),
    );
    assert_null(
        call("power", vec![Datum::Float(2.0), Datum::Null]),
    );
}

#[test]
fn test_mod() {
    // mod uses datum_to_f64 so result is Float
    assert_float(
        call("mod", vec![Datum::Integer(10), Datum::Integer(3)]),
        1.0,
    );
    assert_float(
        call("mod", vec![Datum::Float(10.5), Datum::Float(3.0)]),
        1.5,
    );
    assert_null(
        call("mod", vec![Datum::Null, Datum::Integer(3)]),
    );
    assert_null(
        call("mod", vec![Datum::Integer(10), Datum::Null]),
    );

    // Division by zero should error
    let result = call_scalar_function("mod", vec![Datum::Integer(10), Datum::Integer(0)]);
    assert!(result.is_err(), "mod by zero should produce an error");
}

#[test]
fn test_ln() {
    assert_float_approx(
        call("ln", vec![Datum::Float(std::f64::consts::E)]),
        1.0,
        1e-9,
    );
    assert_float(call("ln", vec![Datum::Float(1.0)]), 0.0);
    assert_null(call("ln", vec![Datum::Null]));
}

#[test]
fn test_log() {
    // log(100.0) = log10(100.0) = 2.0
    assert_float_approx(
        call("log", vec![Datum::Float(100.0)]),
        2.0,
        1e-9,
    );
    assert_float_approx(
        call("log", vec![Datum::Float(1000.0)]),
        3.0,
        1e-9,
    );
    // Two-argument form: log(base, value)
    assert_float_approx(
        call("log", vec![Datum::Float(2.0), Datum::Float(8.0)]),
        3.0,
        1e-9,
    );
    assert_null(call("log", vec![Datum::Null]));
}

#[test]
fn test_exp() {
    // exp(1.0) = e
    assert_float_approx(
        call("exp", vec![Datum::Float(1.0)]),
        std::f64::consts::E,
        1e-9,
    );
    assert_float(call("exp", vec![Datum::Float(0.0)]), 1.0);
    assert_null(call("exp", vec![Datum::Null]));
}

#[test]
fn test_sign() {
    assert_float(call("sign", vec![Datum::Integer(-5)]), -1.0);
    assert_float(call("sign", vec![Datum::Integer(0)]), 0.0);
    assert_float(call("sign", vec![Datum::Integer(5)]), 1.0);
    assert_float(call("sign", vec![Datum::Float(-3.14)]), -1.0);
    assert_float(call("sign", vec![Datum::Float(3.14)]), 1.0);
    assert_null(call("sign", vec![Datum::Null]));
}

#[test]
fn test_random() {
    let result = call("random", vec![]);
    match result {
        Datum::Float(v) => {
            assert!(v >= 0.0, "random() should return >= 0.0, got {v}");
            assert!(v < 1.0, "random() should return < 1.0, got {v}");
        }
        other => panic!("expected Float, got {other:?}"),
    }
    // Call again to verify it produces a result (may differ)
    let result2 = call("random", vec![]);
    match result2 {
        Datum::Float(v) => {
            assert!(v >= 0.0 && v < 1.0);
        }
        other => panic!("expected Float, got {other:?}"),
    }
}

// ===========================================================================
// Type functions
// ===========================================================================

#[test]
fn test_typeof() {
    assert_text(call("typeof", vec![Datum::Integer(42)]), "INTEGER");
    assert_text(call("typeof", vec![Datum::BigInt(100)]), "BIGINT");
    assert_text(call("typeof", vec![Datum::Float(1.5)]), "FLOAT");
    assert_text(call("typeof", vec![Datum::Boolean(true)]), "BOOLEAN");
    assert_text(call("typeof", vec![Datum::Text("hi".into())]), "TEXT");
    assert_text(call("typeof", vec![Datum::Null]), "NULL");
}

// ===========================================================================
// Null functions
// ===========================================================================

#[test]
fn test_coalesce() {
    // coalesce(NULL, NULL, 3) = 3
    assert_integer(
        call(
            "coalesce",
            vec![Datum::Null, Datum::Null, Datum::Integer(3)],
        ),
        3,
    );
    // First non-null value is returned
    assert_integer(
        call(
            "coalesce",
            vec![Datum::Null, Datum::Integer(7), Datum::Integer(3)],
        ),
        7,
    );
    // All NULLs => NULL
    assert_null(call("coalesce", vec![Datum::Null, Datum::Null]));
    // First value is non-null
    assert_integer(
        call("coalesce", vec![Datum::Integer(1), Datum::Null]),
        1,
    );
    // Empty args => NULL
    assert_null(call("coalesce", vec![]));
}

#[test]
fn test_nullif() {
    // nullif(1, 1) = NULL (equal values)
    assert_null(
        call("nullif", vec![Datum::Integer(1), Datum::Integer(1)]),
    );
    // nullif(1, 2) = 1 (different values)
    assert_integer(
        call("nullif", vec![Datum::Integer(1), Datum::Integer(2)]),
        1,
    );
    // Different types that coerce to equal
    assert_null(
        call("nullif", vec![Datum::Integer(5), Datum::BigInt(5)]),
    );
    // Text comparison
    assert_null(
        call(
            "nullif",
            vec![Datum::Text("abc".into()), Datum::Text("abc".into())],
        ),
    );
    assert_text(
        call(
            "nullif",
            vec![Datum::Text("abc".into()), Datum::Text("xyz".into())],
        ),
        "abc",
    );
}

// ===========================================================================
// NULL propagation
// ===========================================================================

#[test]
fn test_null_propagation() {
    // String functions that should return NULL when given NULL input
    assert_null(call("length", vec![Datum::Null]));
    assert_null(call("upper", vec![Datum::Null]));
    assert_null(call("lower", vec![Datum::Null]));
    assert_null(call("trim", vec![Datum::Null]));
    assert_null(call("ltrim", vec![Datum::Null]));
    assert_null(call("rtrim", vec![Datum::Null]));
    assert_null(call("reverse", vec![Datum::Null]));
    assert_null(call("substring", vec![Datum::Null, Datum::Integer(1)]));
    assert_null(
        call(
            "replace",
            vec![
                Datum::Null,
                Datum::Text("a".into()),
                Datum::Text("b".into()),
            ],
        ),
    );
    assert_null(
        call(
            "replace",
            vec![
                Datum::Text("hello".into()),
                Datum::Null,
                Datum::Text("b".into()),
            ],
        ),
    );
    assert_null(
        call(
            "replace",
            vec![
                Datum::Text("hello".into()),
                Datum::Text("a".into()),
                Datum::Null,
            ],
        ),
    );
    assert_null(call("left", vec![Datum::Null, Datum::Integer(3)]));
    assert_null(call("right", vec![Datum::Null, Datum::Integer(3)]));
    assert_null(call("repeat", vec![Datum::Null, Datum::Integer(3)]));
    assert_null(
        call(
            "split_part",
            vec![
                Datum::Null,
                Datum::Text(".".into()),
                Datum::Integer(1),
            ],
        ),
    );
    assert_null(call("lpad", vec![Datum::Null, Datum::Integer(5)]));
    assert_null(call("rpad", vec![Datum::Null, Datum::Integer(5)]));

    // Math functions that should return NULL when given NULL input
    assert_null(call("abs", vec![Datum::Null]));
    assert_null(call("ceil", vec![Datum::Null]));
    assert_null(call("floor", vec![Datum::Null]));
    assert_null(call("round", vec![Datum::Null]));
    assert_null(call("trunc", vec![Datum::Null]));
    assert_null(call("sqrt", vec![Datum::Null]));
    assert_null(call("power", vec![Datum::Null, Datum::Float(2.0)]));
    assert_null(call("power", vec![Datum::Float(2.0), Datum::Null]));
    assert_null(call("mod", vec![Datum::Null, Datum::Integer(3)]));
    assert_null(call("mod", vec![Datum::Integer(10), Datum::Null]));
    assert_null(call("ln", vec![Datum::Null]));
    assert_null(call("log", vec![Datum::Null]));
    assert_null(call("exp", vec![Datum::Null]));
    assert_null(call("sign", vec![Datum::Null]));
}

// ===========================================================================
// Function aliases
// ===========================================================================

#[test]
fn test_function_aliases() {
    // char_length and character_length are aliases for length
    assert_integer(
        call("char_length", vec![Datum::Text("hello".into())]),
        5,
    );
    assert_integer(
        call("character_length", vec![Datum::Text("hello".into())]),
        5,
    );

    // btrim is an alias for trim
    assert_text(
        call("btrim", vec![Datum::Text("  hi  ".into())]),
        "hi",
    );

    // substr is an alias for substring
    assert_text(
        call(
            "substr",
            vec![
                Datum::Text("hello".into()),
                Datum::Integer(2),
                Datum::Integer(3),
            ],
        ),
        "ell",
    );

    // strpos is an alias for position
    assert_integer(
        call(
            "strpos",
            vec![
                Datum::Text("lo".into()),
                Datum::Text("hello".into()),
            ],
        ),
        4,
    );

    // ceiling is an alias for ceil
    assert_float(call("ceiling", vec![Datum::Float(3.2)]), 4.0);

    // truncate is an alias for trunc
    assert_float(call("truncate", vec![Datum::Float(3.9)]), 3.0);

    // pow is an alias for power
    assert_float(
        call("pow", vec![Datum::Float(2.0), Datum::Float(3.0)]),
        8.0,
    );

    // log10 is an alias for log
    assert_float_approx(
        call("log10", vec![Datum::Float(100.0)]),
        2.0,
        1e-9,
    );

    // pg_typeof is an alias for typeof
    assert_text(call("pg_typeof", vec![Datum::Integer(42)]), "INTEGER");
}

// ===========================================================================
// Error cases
// ===========================================================================

#[test]
fn test_unknown_function() {
    let result = call_scalar_function("nonexistent_func", vec![]);
    assert!(result.is_err(), "unknown function should produce an error");
}

#[test]
fn test_wrong_arg_count() {
    // length expects exactly 1 argument
    let result = call_scalar_function("length", vec![]);
    assert!(result.is_err(), "length() with 0 args should error");

    let result = call_scalar_function(
        "length",
        vec![Datum::Text("a".into()), Datum::Text("b".into())],
    );
    assert!(result.is_err(), "length() with 2 args should error");

    // position expects exactly 2 arguments
    let result = call_scalar_function("position", vec![Datum::Text("a".into())]);
    assert!(result.is_err(), "position() with 1 arg should error");
}

// ===========================================================================
// Edge cases
// ===========================================================================

#[test]
fn test_string_functions_with_integers() {
    // When a non-string Datum is passed, to_string formats it via Display
    // length of the string representation of 42 is "42" => 2 chars
    assert_integer(call("length", vec![Datum::Integer(42)]), 2);
    assert_text(call("upper", vec![Datum::Integer(42)]), "42");
}

#[test]
fn test_concat_mixed_types() {
    // concat coerces all args to strings via Display
    assert_text(
        call(
            "concat",
            vec![
                Datum::Text("val=".into()),
                Datum::Integer(42),
                Datum::Text("!".into()),
            ],
        ),
        "val=42!",
    );
}

#[test]
fn test_lpad_rpad_with_multi_char_fill() {
    // Fill pattern cycles: lpad("x", 6, "ab") => "ababax"
    assert_text(
        call(
            "lpad",
            vec![
                Datum::Text("x".into()),
                Datum::Integer(6),
                Datum::Text("ab".into()),
            ],
        ),
        "ababax",
    );
    assert_text(
        call(
            "rpad",
            vec![
                Datum::Text("x".into()),
                Datum::Integer(6),
                Datum::Text("ab".into()),
            ],
        ),
        "xababa",
    );
}

#[test]
fn test_split_part_field_zero_errors() {
    let result = call_scalar_function(
        "split_part",
        vec![
            Datum::Text("a.b.c".into()),
            Datum::Text(".".into()),
            Datum::Integer(0),
        ],
    );
    assert!(result.is_err(), "split_part with field=0 should error");
}

#[test]
fn test_substring_with_bigint() {
    // substring also accepts BigInt for start/length
    assert_text(
        call(
            "substring",
            vec![
                Datum::Text("hello".into()),
                Datum::BigInt(2),
                Datum::BigInt(3),
            ],
        ),
        "ell",
    );
}

// ===========================================================================
// Date/Time functions
// ===========================================================================

#[test]
fn test_now() {
    let result = call("now", vec![]);
    match result {
        Datum::Timestamp(ts) => {
            assert!(ts > 0, "now() should return a positive timestamp, got {ts}");
        }
        other => panic!("expected Timestamp from now(), got {other:?}"),
    }
}

#[test]
fn test_extract() {
    // 2020-01-15 10:30:00 UTC
    // total seconds: 1579081800, in microseconds: 1579081800 * 1_000_000
    let ts = 1_579_084_200_000_000i64;

    assert_float(
        call("extract", vec![Datum::Text("year".into()), Datum::Timestamp(ts)]),
        2020.0,
    );
    assert_float(
        call("extract", vec![Datum::Text("month".into()), Datum::Timestamp(ts)]),
        1.0,
    );
    assert_float(
        call("extract", vec![Datum::Text("day".into()), Datum::Timestamp(ts)]),
        15.0,
    );
    assert_float(
        call("extract", vec![Datum::Text("hour".into()), Datum::Timestamp(ts)]),
        10.0,
    );
    assert_float(
        call("extract", vec![Datum::Text("minute".into()), Datum::Timestamp(ts)]),
        30.0,
    );
    assert_null(call("extract", vec![Datum::Text("year".into()), Datum::Null]));
}

#[test]
fn test_date_trunc() {
    // 2020-01-15 10:30:45 UTC = 1579081845 seconds since epoch
    let ts = 1_579_084_245_000_000i64;

    // Truncate to day => 2020-01-15 00:00:00 UTC = 1579046400 seconds
    let result = call("date_trunc", vec![
        Datum::Text("day".into()),
        Datum::Timestamp(ts),
    ]);
    match result {
        Datum::Timestamp(v) => {
            let expected = 1_579_046_400_000_000i64;
            assert_eq!(v, expected, "date_trunc to day should zero out hours/mins/secs");
        }
        other => panic!("expected Timestamp, got {other:?}"),
    }

    assert_null(call("date_trunc", vec![Datum::Text("day".into()), Datum::Null]));
}

#[test]
fn test_to_char() {
    // 2020-01-15 10:30:00 UTC
    let ts = 1_579_084_200_000_000i64;
    assert_text(
        call("to_char", vec![
            Datum::Timestamp(ts),
            Datum::Text("YYYY-MM-DD HH24:MI:SS".into()),
        ]),
        "2020-01-15 10:30:00",
    );
    assert_null(call("to_char", vec![Datum::Null, Datum::Text("YYYY".into())]));
    assert_null(call("to_char", vec![Datum::Timestamp(ts), Datum::Null]));
}
