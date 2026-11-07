//! The member view: one record per member, the rumor precedence rules, and the broadcast queue.

use std::collections::{BTreeMap, BTreeSet};

use discus_placement::NodeId;
use discus_wire::{Liveness, MemberInfo};

use crate::{Detector, Millis};

/// What this node knows about one other member.
pub struct Record {
    pub info: MemberInfo,
    pub liveness: Liveness,
    /// When the liveness last changed.
    pub since: Millis,
    /// The distinct nodes that suspect the member at its current incarnation.
    pub accusers: BTreeSet<NodeId>,
    pub detector: Detector,
    /// The last HLC that the member sent, and when it arrived.
    pub hlc: Option<(u64, Millis)>,
}

fn rank(l: Liveness) -> u8 {
    match l {
        Liveness::Alive => 0,
        Liveness::Suspect => 1,
        Liveness::Dead => 2,
    }
}

/// SWIM precedence: a higher incarnation wins. At the same incarnation, `Dead` beats `Suspect`,
/// and `Suspect` beats `Alive`. Only the member itself raises its incarnation, so an `Alive` with
/// a higher incarnation refutes a suspicion or a death.
pub fn supersedes(new: (u64, Liveness), old: (u64, Liveness)) -> bool {
    new.0 > old.0 || (new.0 == old.0 && rank(new.1) > rank(old.1))
}

/// The broadcast queue: one entry per member, sent with the fewest-sent entries first.
#[derive(Debug, Default)]
pub struct Broadcasts {
    sends: BTreeMap<NodeId, u32>,
}

impl Broadcasts {
    /// Queue the current record of `id` again from zero sends.
    pub fn push(&mut self, id: NodeId) {
        self.sends.insert(id, 0);
    }

    /// Up to `max` members for one message. Each entry leaves after `limit` sends.
    pub fn take(&mut self, max: usize, limit: u32) -> Vec<NodeId> {
        let mut entries: Vec<(u32, NodeId)> = self.sends.iter().map(|(id, n)| (*n, *id)).collect();
        entries.sort();
        let picked: Vec<NodeId> = entries.into_iter().take(max).map(|(_, id)| id).collect();
        for id in &picked {
            if let Some(n) = self.sends.get_mut(id) {
                *n += 1;
                if *n >= limit {
                    self.sends.remove(id);
                }
            }
        }
        picked
    }
}

/// The retransmit limit for a cluster of `n` members: `mult * ceil(log2(n + 1))`.
pub fn retransmit_limit(mult: u32, n: usize) -> u32 {
    let n = n as u64 + 1;
    let log = 64 - (n - 1).leading_zeros();
    mult * log.max(1)
}
