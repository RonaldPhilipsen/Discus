//! Helpers shared by command handlers.

use bytes::Bytes;

use super::error::CommandError;
use crate::node::Node;

/// Parse a canonical signed 64-bit integer. Rejects `+5`, spaces, and empty input.
pub fn parse_i64(b: &[u8]) -> Result<i64, CommandError> {
    if b.is_empty() || b[0] == b'+' {
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
