//! The slot table: the owners of every placement range, computed once per epoch.
//!
//! The owners of range `r` are the weighted rendezvous owners of the two big-endian bytes of `r`,
//! so every slot of a range shares one owner list.

use crate::{hrw, Member, NodeId, RANGES};

const WORDS: usize = RANGES as usize / 64;

/// The ordered owner IDs of every range, `per_range` IDs per range.
#[derive(Debug, Clone)]
pub struct SlotTable {
    rf: usize,
    per_range: usize,
    ids: Vec<NodeId>,
}

impl SlotTable {
    pub fn build(members: &[Member], rf: usize) -> SlotTable {
        let mut ids = Vec::new();
        let mut per_range = 0;
        for range in 0..RANGES {
            let owners = hrw::owners(&range.to_be_bytes(), members, rf);
            per_range = owners.len();
            ids.extend(owners);
        }
        SlotTable { rf, per_range, ids }
    }

    pub fn rf(&self) -> usize {
        self.rf
    }

    /// True when every range lists all `NORMAL` members, so the table answers any `rf`.
    pub fn lists_all_normal(&self) -> bool {
        self.per_range < self.rf
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
