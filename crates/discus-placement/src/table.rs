//! The slot table: the owners of every placement range, computed once per epoch.
//!
//! The owners of range `r` are the weighted rendezvous owners of the two big-endian bytes of `r`,
//! so every slot of a range shares one owner list.

use crate::{hrw, Member, NodeId, RANGES};

const WORDS: usize = RANGES as usize / 64;

/// The ordered owners of every range, stored as indices into the member list the table was built
/// from.
#[derive(Debug, Clone)]
pub struct SlotTable {
    members: Vec<Member>,
    rf: usize,
    per_range: usize,
    owners: Vec<u16>,
    /// The IDs of `owners`, so a lookup can borrow an owner list.
    ids: Vec<NodeId>,
}

impl SlotTable {
    pub fn build(members: &[Member], rf: usize) -> SlotTable {
        let mut by_id: Vec<(NodeId, u16)> = members
            .iter()
            .enumerate()
            .map(|(i, m)| (m.id, i as u16))
            .collect();
        by_id.sort_unstable();
        let index_of = |id: NodeId| {
            by_id
                .binary_search_by_key(&id, |&(n, _)| n)
                .map_or(0, |at| by_id[at].1)
        };
        let mut owners = Vec::new();
        let mut per_range = 0;
        for range in 0..RANGES {
            let ids = hrw::owners(&range.to_be_bytes(), members, rf);
            per_range = ids.len();
            owners.extend(ids.into_iter().map(index_of));
        }
        let ids = owners.iter().map(|&i| members[usize::from(i)].id).collect();
        SlotTable {
            members: members.to_vec(),
            rf,
            per_range,
            owners,
            ids,
        }
    }

    pub fn rf(&self) -> usize {
        self.rf
    }

    /// True when every range lists all `NORMAL` members, so the table answers any `rf`.
    pub fn lists_all_normal(&self) -> bool {
        self.per_range < self.rf
    }

    /// The owners of `range`, primary first.
    pub fn owners(&self, range: u16) -> impl Iterator<Item = &Member> {
        let start = usize::from(range) * self.per_range;
        self.owners
            .get(start..start + self.per_range)
            .unwrap_or_default()
            .iter()
            .map(|&i| &self.members[usize::from(i)])
    }

    /// The IDs of the owners of `range`, primary first.
    pub fn owner_ids(&self, range: u16) -> &[NodeId] {
        let start = usize::from(range) * self.per_range;
        self.ids
            .get(start..start + self.per_range)
            .unwrap_or_default()
    }
}

/// A set of ranges as a bitmap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeSet([u64; WORDS]);

impl RangeSet {
    pub(crate) fn empty() -> RangeSet {
        RangeSet([0; WORDS])
    }

    pub(crate) fn insert(&mut self, range: u16) {
        self.0[usize::from(range) / 64] |= 1 << (range % 64);
    }

    pub fn contains(&self, range: u16) -> bool {
        range < RANGES && self.0[usize::from(range) / 64] & (1 << (range % 64)) != 0
    }

    pub fn is_empty(&self) -> bool {
        self.0.iter().all(|&w| w == 0)
    }

    pub fn len(&self) -> usize {
        self.0.iter().map(|w| w.count_ones() as usize).sum()
    }
}
