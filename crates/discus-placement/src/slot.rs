//! The Redis Cluster slot of a key and the placement range of a slot.
//!
//! The three constants are part of the placement format. A change is a breaking change.

use crc::{Crc, CRC_16_XMODEM};

pub const SLOTS: u16 = 16384;
pub const SLOTS_PER_RANGE: u16 = 8;
pub const RANGES: u16 = SLOTS / SLOTS_PER_RANGE;

static CRC: Crc<u16> = Crc::<u16>::new(&CRC_16_XMODEM);

/// The slot of `key` by the Redis Cluster rules: the hash tag decides it when the key has one.
#[must_use]
pub fn slot(key: &[u8]) -> u16 {
    CRC.checksum(hash_tag(key)) % SLOTS
}

#[must_use]
pub const fn range_of(slot: u16) -> u16 {
    slot / SLOTS_PER_RANGE
}

/// The bytes between the first `{` and the next `}`, or the whole key when that span is empty or
/// missing.
fn hash_tag(key: &[u8]) -> &[u8] {
    let Some(open) = key.iter().position(|&b| b == b'{') else {
        return key;
    };
    let rest = &key[open + 1..];
    match rest.iter().position(|&b| b == b'}') {
        Some(len) if len > 0 => &rest[..len],
        _ => key,
    }
}
