//! Weighted rendezvous (HRW) placement, spec §5.
//!
//! For key `k` and member `n`, `u(k, n)` comes from streaming xxh3-64 with [`PLACEMENT_SEED`] over
//! the key bytes followed by the 16 big-endian bytes of the node ID. The top 52 bits `m` give
//! `u = (m + 0.5) / 2^52`, which is exact in `f64` and strictly inside `(0, 1)`. The score is
//! `weight / -ln(u)`. A change to the hash, the seed, the byte order, or this mapping is a breaking
//! change, because every node must compute the same owners.

use std::cmp::Ordering;

use xxhash_rust::xxh3::{xxh3_64_with_seed, Xxh3};

use crate::{Member, MemberState, NodeId};

/// The seed of the placement hash. It differs from the store shard seed on purpose, so shard
/// choice and owner choice are independent.
pub const PLACEMENT_SEED: u64 = 0x4449_5343_5553_0002;

pub const DEFAULT_RF: usize = 3;

const TWO_POW_52: f64 = 4_503_599_627_370_496.0;

/// The uniform value `u(k, n)` in `(0, 1)`.
pub fn unit(key: &[u8], node: NodeId) -> f64 {
    let id = node.to_be_bytes();
    // The one-shot hash of the joined input equals the streaming hash, and it skips the large
    // state that `Xxh3::with_seed` sets up for every member of every lookup.
    let digest = if key.len() <= STACK_KEY {
        let mut buf = [0u8; STACK_KEY + 16];
        buf[..key.len()].copy_from_slice(key);
        buf[key.len()..key.len() + 16].copy_from_slice(&id);
        xxh3_64_with_seed(&buf[..key.len() + 16], PLACEMENT_SEED)
    } else {
        let mut hasher = Xxh3::with_seed(PLACEMENT_SEED);
        hasher.update(key);
        hasher.update(&id);
        hasher.digest()
    };
    let m = digest >> 12;
    (m as f64 + 0.5) / TWO_POW_52
}

/// Keys up to this length hash from a stack buffer.
const STACK_KEY: usize = 240;

/// The weighted rendezvous score of `member` for `key`. Higher wins.
pub fn score(key: &[u8], member: &Member) -> f64 {
    member.weight.get() / -unit(key, member.id).ln()
}

/// The owner set of `key`: the top `min(rf, N)` `NORMAL` members by descending score. Equal
/// scores order by ascending node ID. The first owner is the primary.
pub fn owners(key: &[u8], members: &[Member], rf: usize) -> Vec<NodeId> {
    let mut scored: Vec<(f64, NodeId)> = members
        .iter()
        .filter(|m| m.state == MemberState::Normal)
        .map(|m| (score(key, m), m.id))
        .collect();
    scored.sort_unstable_by(|a, b| {
        b.0.partial_cmp(&a.0)
            .unwrap_or(Ordering::Equal)
            .then(a.1.cmp(&b.1))
    });
    scored.truncate(rf);
    scored.into_iter().map(|(_, id)| id).collect()
}
