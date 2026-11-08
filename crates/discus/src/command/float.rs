//! `INCRBYFLOAT` arithmetic as Redis does it on x86-64 Linux (spec: Redis `t_string.c` and
//! `util.c`).
//!
//! Redis parses both numbers with `strtold` into an x87 80-bit `long double`, adds them, and
//! stores the sum as text with `ld2string(..., LD_STR_HUMAN)`: `%.17Lf`, then the trailing zeros
//! and a trailing point go, and `-0` becomes `0`. The text is the state, so the next increment
//! parses it again. An f64 sum gives other text, for example `0.30000000000000004` for
//! `0.1 + 0.2` where Redis gives `0.3`. So the sum here runs on a software x87 float
//! (`rustc_apfloat`), which rounds like the FPU, and the text is the exact decimal expansion of
//! the sum rounded to 17 places, as glibc `printf` does it.

use bytes::Bytes;
use num_bigint::BigUint;
use rustc_apfloat::ieee::X87DoubleExtended;
use rustc_apfloat::{Float, Round, Status, StatusAnd};

use super::error::CommandError;

/// An x87 `long double`.
pub type LongDouble = X87DoubleExtended;

/// Redis refuses a number text of this length or more (`MAX_LONG_DOUBLE_CHARS`).
const MAX_CHARS: usize = 5 * 1024;
/// Digits after the point in the stored text.
const PLACES: usize = 17;

/// Parse a number as Redis `string2ld` does: `strtold` must take the whole text, there is no
/// leading space, and NaN, an overflow, and an underflow to zero are refused. `inf` parses.
pub fn parse_long_double(b: &[u8]) -> Result<LongDouble, CommandError> {
    let s = std::str::from_utf8(b).map_err(|_| CommandError::NotFloat)?;
    parse(s).ok_or(CommandError::NotFloat)
}

fn parse(s: &str) -> Option<LongDouble> {
    if s.is_empty() || s.len() >= MAX_CHARS {
        return None;
    }
    let (negative, body) = match s.as_bytes()[0] {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    let body = body.to_ascii_lowercase();
    let value = if body == "inf" || body == "infinity" {
        LongDouble::INFINITY
    } else if let Some(hex) = body.strip_prefix("0x") {
        let (mantissa, exp) = split_exp(hex, 'p');
        if !mantissa_ok(mantissa, |c| c.is_ascii_hexdigit()) || !exp.is_none_or(exp_ok) {
            return None;
        }
        // `strtold` takes a hex number without an exponent. The parser here wants one.
        let text = format!("0x{mantissa}p{}", exp.unwrap_or("0"));
        finite(
            LongDouble::from_str_r(&text, Round::NearestTiesToEven).ok()?,
            mantissa,
        )?
    } else {
        let (mantissa, exp) = split_exp(&body, 'e');
        if !mantissa_ok(mantissa, |c| c.is_ascii_digit()) || !exp.is_none_or(exp_ok) {
            return None;
        }
        finite(
            LongDouble::from_str_r(&body, Round::NearestTiesToEven).ok()?,
            mantissa,
        )?
    };
    Some(if negative { -value } else { value })
}

fn split_exp(s: &str, marker: char) -> (&str, Option<&str>) {
    match s.split_once(marker) {
        Some((m, e)) => (m, Some(e)),
        None => (s, None),
    }
}

fn mantissa_ok(m: &str, digit: impl Fn(char) -> bool) -> bool {
    let (int, frac) = m.split_once('.').unwrap_or((m, ""));
    (!int.is_empty() || !frac.is_empty()) && int.chars().all(&digit) && frac.chars().all(&digit)
}

fn exp_ok(e: &str) -> bool {
    let digits = e.strip_prefix(['+', '-']).unwrap_or(e);
    !digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit())
}

/// `strtold` sets `ERANGE` on an overflow and on an underflow, and Redis refuses the result
/// when it is infinite or zero.
fn finite(parsed: StatusAnd<LongDouble>, mantissa: &str) -> Option<LongDouble> {
    if parsed.status.contains(Status::OVERFLOW) {
        return None;
    }
    let nonzero = mantissa.bytes().any(|c| c != b'0' && c != b'.');
    if parsed.value.is_zero() && nonzero {
        return None;
    }
    Some(parsed.value)
}

/// Add `delta` to the number `current` (zero when absent) and give the text that Redis stores.
pub fn incr_by_float(current: Option<&[u8]>, delta: LongDouble) -> Result<Bytes, CommandError> {
    let current = match current {
        Some(b) => parse_long_double(b)?,
        None => LongDouble::ZERO,
    };
    let sum = (current + delta).value;
    if !sum.is_finite() {
        return Err(CommandError::Custom(
            "ERR increment would produce NaN or Infinity".into(),
        ));
    }
    Ok(Bytes::from(human(sum)))
}

/// `ld2string(..., LD_STR_HUMAN)` for a finite value: `%.17Lf` with ties to even, then no
/// trailing zeros and no trailing point, and `-0` as `0`.
fn human(v: LongDouble) -> String {
    let bits = v.to_bits();
    let negative = (bits >> 79) & 1 == 1;
    let biased = ((bits >> 64) & 0x7fff) as i64;
    let significand = bits as u64;
    // The x87 format keeps the integer bit, so a value is `significand * 2^(e - 16383 - 63)`,
    // and a denormal has the exponent of the smallest normal.
    let exp2 = biased.max(1) - 16383 - 63;
    let scaled = BigUint::from(significand) * BigUint::from(10u32).pow(PLACES as u32);
    let units = if exp2 >= 0 {
        scaled << exp2.unsigned_abs()
    } else {
        let shift = exp2.unsigned_abs();
        let quotient: BigUint = &scaled >> shift;
        let rest = scaled - (&quotient << shift);
        let half = BigUint::from(1u32) << (shift - 1);
        if rest > half || (rest == half && quotient.bit(0)) {
            quotient + 1u32
        } else {
            quotient
        }
    };
    let digits = format!("{units:0>width$}", width = PLACES + 1);
    let (int, frac) = digits.split_at(digits.len() - PLACES);
    let frac = frac.trim_end_matches('0');
    let mut out = String::with_capacity(digits.len() + 2);
    if negative && (int != "0" || !frac.is_empty()) {
        out.push('-');
    }
    out.push_str(int);
    if !frac.is_empty() {
        out.push('.');
        out.push_str(frac);
    }
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn incr(current: &str, delta: &str) -> Result<String, String> {
        let delta = parse_long_double(delta.as_bytes()).map_err(|e| e.text())?;
        incr_by_float(Some(current.as_bytes()), delta)
            .map(|b| String::from_utf8(b.to_vec()).unwrap())
            .map_err(|e| e.text())
    }

    /// Each answer is what Redis 8.10 on x86-64 Linux returns for `SET k a` then
    /// `INCRBYFLOAT k b`.
    #[test]
    fn sums_match_redis() {
        let cases = [
            ("0.1", "0.2", "0.3"),
            ("10.50", "0.1", "10.6"),
            ("5.0e3", "2.0e2", "5200"),
            ("0", "1e30", "1000000000000000000024696061952"),
            ("0", "1e-20", "0"),
            ("0", "-1e-20", "0"),
            ("1", "1e-16", "1.0000000000000001"),
            ("12345.678", "0", "12345.67799999999999994"),
            ("0", "0x10", "16"),
            ("0", "0x1.8p1", "3"),
            ("0", "3.0e-5", "0.00003"),
            ("3", "-3", "0"),
            ("-5", "2.5", "-2.5"),
            ("0", "1e19", "10000000000000000000"),
            (
                "0",
                "123456789012345678901234567890",
                "123456789012345678899921813504",
            ),
            ("0", ".5", "0.5"),
            ("0", "+5", "5"),
            ("0", "5.", "5"),
            ("0", "1E5", "100000"),
            ("0", "-0", "0"),
            ("0", "4e-4951", "0"),
        ];
        for (a, b, want) in cases {
            assert_eq!(incr(a, b).as_deref(), Ok(want), "{a} + {b}");
        }
        let big = incr("0", "1e400").unwrap();
        assert!(big.starts_with("10000000000000000000281880683947586514586453433629"));
        assert_eq!(big.len(), 401);
    }

    #[test]
    fn bad_numbers_and_infinite_sums_are_refused_as_redis_does() {
        let not_float = "ERR value is not a valid float";
        let infinite = "ERR increment would produce NaN or Infinity";
        for bad in [
            "1e", "nan", "1e4950", "1e-5000", "0x", "1_0", " 1", "1 ", "", "abc",
        ] {
            assert_eq!(incr("0", bad), Err(not_float.to_string()), "{bad:?}");
        }
        assert_eq!(incr("0", "inf"), Err(infinite.to_string()));
        assert_eq!(incr("0", "INF"), Err(infinite.to_string()));
        assert_eq!(incr("1e4932", "1e4932"), Err(infinite.to_string()));
    }
}
