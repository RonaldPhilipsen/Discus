//! A stored entry: a value with its expiry, access time, and HLC version.

use discus_placement::Version;

use crate::Value;

/// Fixed bookkeeping cost that the memory estimate adds per entry and per collection member.
pub const ENTRY_OVERHEAD: u64 = 64;

#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub value: Value,
    /// Absolute expiry in Unix milliseconds.
    pub expires_at_ms: Option<u64>,
    /// Last access in Unix milliseconds, at roughly one second of resolution on reads.
    pub last_access_ms: u64,
    /// The HLC timestamp and origin of the last write. Conflicts resolve by this version.
    pub version: Version,
}

impl Entry {
    /// A new entry. The store sets the version and the access time when the write lands.
    pub fn new(value: Value, expires_at_ms: Option<u64>) -> Entry {
        Entry {
            value,
            expires_at_ms,
            last_access_ms: 0,
            version: Version::zero(),
        }
    }

    pub fn is_expired(&self, now_ms: u64) -> bool {
        matches!(self.expires_at_ms, Some(t) if t <= now_ms)
    }
}

/// Estimate the memory that an entry costs, including its key.
pub fn estimate_size(key: &[u8], entry: &Entry) -> u64 {
    let payload: u64 = match &entry.value {
        crate::Value::Str(b) => b.len() as u64,
        crate::Value::Hash(h) => h
            .iter()
            .map(|(k, v)| (k.len() + v.len()) as u64 + ENTRY_OVERHEAD)
            .sum(),
        crate::Value::ZSet(z) => z.byte_size(),
        crate::Value::Set(s) => s.iter().map(|m| m.len() as u64 + ENTRY_OVERHEAD).sum(),
    };
    key.len() as u64 + payload + ENTRY_OVERHEAD
}
