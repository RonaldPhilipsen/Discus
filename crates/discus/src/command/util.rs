//! Helpers shared by command handlers.

use bytes::Bytes;

use super::error::CommandError;
use crate::node::Node;

/// Parse a canonical signed 64-bit integer, as `string2ll` of Valkey does. Rejects `+5`, spaces,
/// empty input, a leading zero such as `007`, and `-0`.
pub fn parse_i64(b: &[u8]) -> Result<i64, CommandError> {
    let digits = b.strip_prefix(b"-").unwrap_or(b);
    let leading_zero =
        digits.first() == Some(&b'0') && (digits.len() > 1 || digits.len() < b.len());
    if b.is_empty() || b[0] == b'+' || leading_zero {
        return Err(CommandError::NotInteger);
    }
    std::str::from_utf8(b)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or(CommandError::NotInteger)
}

pub fn parse_f64(b: &[u8]) -> Result<f64, CommandError> {
    let f = std::str::from_utf8(b)
        .ok()
        .filter(|s| !s.is_empty() && !s.starts_with(char::is_whitespace))
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or(CommandError::NotFloat)?;
    if f.is_nan() {
        return Err(CommandError::NotFloat);
    }
    Ok(f)
}

/// What C `printf("%.*s", max, b)` prints, as Valkey puts client bytes in an error text: the
/// bytes before the first NUL, at most `max` of them.
pub fn c_prefix(b: &[u8], max: usize) -> &[u8] {
    let end = b.iter().position(|&c| c == 0).unwrap_or(b.len()).min(max);
    &b[..end]
}

/// [`c_prefix`] with the 128-byte limit that Valkey uses for most client bytes in an error text.
pub fn error_text(b: &[u8]) -> String {
    String::from_utf8_lossy(c_prefix(b, ERROR_TEXT_BYTES)).into_owned()
}

/// Valkey prints client bytes in an error text with `%.128s`.
pub const ERROR_TEXT_BYTES: usize = 128;

pub fn upper(b: &Bytes) -> String {
    String::from_utf8_lossy(b).to_ascii_uppercase()
}

pub fn check_key(node: &Node, key: &[u8]) -> Result<(), CommandError> {
    if key.len() > node.config.max_key_bytes {
        return Err(CommandError::KeyTooLarge);
    }
    Ok(())
}

pub fn check_value(node: &Node, value: &[u8]) -> Result<(), CommandError> {
    if value.len() > node.config.max_value_bytes {
        return Err(CommandError::ValueTooLarge);
    }
    Ok(())
}

/// Check the arity of `MSET`, then each key and value in order.
pub fn check_mset(node: &Node, cmd: &[Bytes]) -> Result<(), CommandError> {
    if cmd.len().is_multiple_of(2) {
        return Err(CommandError::WrongArity("MSET".into()));
    }
    cmd[1..].chunks(2).try_for_each(|pair| {
        check_key(node, &pair[0])?;
        check_value(node, &pair[1])
    })
}

/// Make room for a write of about `bytes`. A value larger than the whole budget is refused.
pub fn admit(node: &Node, bytes: usize) -> Result<(), CommandError> {
    match node
        .store
        .reserve(bytes as u64 + discus_store::entry::ENTRY_OVERHEAD)
    {
        discus_store::Admit::Ok => Ok(()),
        discus_store::Admit::TooLarge => Err(CommandError::Custom(
            "OOM value larger than maxmemory".into(),
        )),
    }
}

pub use discus_store::glob_match;

#[cfg(test)]
mod tests {
    use super::*;

    /// The integers that `string2ll` of redis-server 8.10.2 accepts: `SET k 007` then `INCR k`
    /// is an error there.
    #[test]
    fn an_integer_has_no_leading_zero_and_no_minus_zero() {
        for ok in [&b"0"[..], b"7", b"-7", b"10", b"-9223372036854775808"] {
            assert!(parse_i64(ok).is_ok(), "{ok:?}");
        }
        for bad in [&b"007"[..], b"00", b"-0", b"-07", b"+5", b" 5", b"", b"-"] {
            assert_eq!(parse_i64(bad), Err(CommandError::NotInteger), "{bad:?}");
        }
    }
}
