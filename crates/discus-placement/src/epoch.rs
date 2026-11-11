//! Epochs: numbered snapshots of the member list, spec §5.1.

use std::fmt;
use std::sync::OnceLock;

use crate::table::{RangeSet, SlotTable};
use crate::{hrw, range_of, slot, ClusterId, Member, MemberState, NodeId, RANGES};

/// The generation of the first epoch of a cluster.
pub const FIRST_GENERATION: u64 = 1;

/// The identity of an epoch, ordered by generation and then by origin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EpochId {
    pub generation: u64,
    pub origin: NodeId,
}

impl fmt::Display for EpochId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.generation, self.origin)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EpochError {
    /// The epoch belongs to another cluster. Epochs of different clusters are never compared.
    #[error("epoch of cluster {theirs} does not belong to cluster {ours}")]
    ForeignCluster { ours: ClusterId, theirs: ClusterId },
    /// The member list names one node twice.
    #[error("member {0} appears twice")]
    DuplicateMember(NodeId),
    /// The history does not hold this epoch.
    #[error("epoch {0} is not in the history")]
    Unknown(EpochId),
}

/// One epoch: its identity, its cluster, and its member list sorted by node ID.
#[derive(Debug)]
pub struct Epoch {
    id: EpochId,
    cluster_id: ClusterId,
    members: Vec<Member>,
    table: OnceLock<SlotTable>,
}

impl Clone for Epoch {
    fn clone(&self) -> Epoch {
        Epoch {
            id: self.id,
            cluster_id: self.cluster_id,
            members: self.members.clone(),
            table: OnceLock::new(),
        }
    }
}

impl PartialEq for Epoch {
    fn eq(&self, other: &Epoch) -> bool {
        self.id == other.id && self.cluster_id == other.cluster_id && self.members == other.members
    }
}

impl Epoch {
    /// Build an epoch. The members are sorted by ID. A duplicate ID is an error.
    pub fn new(
        id: EpochId,
        cluster_id: ClusterId,
        mut members: Vec<Member>,
    ) -> Result<Epoch, EpochError> {
        members.sort_by_key(|m| m.id);
        if let Some(pair) = members.windows(2).find(|w| w[0].id == w[1].id) {
            return Err(EpochError::DuplicateMember(pair[0].id));
        }
        Ok(Epoch {
            id,
            cluster_id,
            members,
            table: OnceLock::new(),
        })
    }

    /// The first epoch of a new cluster: generation 1, with the bootstrap node as origin.
    pub fn bootstrap(member: Member, cluster_id: ClusterId) -> Epoch {
        Epoch {
            id: EpochId {
                generation: FIRST_GENERATION,
                origin: member.id,
            },
            cluster_id,
            members: vec![member],
            table: OnceLock::new(),
        }
    }

    pub fn id(&self) -> EpochId {
        self.id
    }

    pub fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    pub fn members(&self) -> &[Member] {
        &self.members
    }

    /// The members in state `NORMAL`.
    pub fn normal(&self) -> impl Iterator<Item = &Member> {
        self.members
            .iter()
            .filter(|m| m.state == MemberState::Normal)
    }

    /// True when `id` is a member in state `NORMAL`.
    pub fn is_normal(&self, id: NodeId) -> bool {
        self.member(id)
            .is_some_and(|m| m.state == MemberState::Normal)
    }

    pub fn member(&self, id: NodeId) -> Option<&Member> {
        self.members
            .binary_search_by_key(&id, |m| m.id)
            .ok()
            .map(|i| &self.members[i])
    }

    /// The slot table of this epoch. The first call builds it with `rf`. Later calls return that
    /// table whatever `rf` they pass.
    pub fn table(&self, rf: usize) -> &SlotTable {
        self.table
            .get_or_init(|| SlotTable::build(&self.members, rf))
    }

    /// The owner set of `key` in this epoch. The first owner is the primary.
    pub fn owners(&self, key: &[u8], rf: usize) -> Vec<NodeId> {
        self.range_owners(range_of(slot(key)), rf)
    }

    /// The primary of `key` in this epoch, if any member is `NORMAL`.
    pub fn primary(&self, key: &[u8]) -> Option<NodeId> {
        self.range_owners(range_of(slot(key)), 1).first().copied()
    }

    pub fn slot_owners(&self, slot: u16, rf: usize) -> Vec<NodeId> {
        self.range_owners(range_of(slot), rf)
    }

    /// The owners of `range`, primary first. The owner list for a smaller `rf` is a prefix of the
    /// longer one, so the cached table answers every `rf` it covers. A larger `rf` is computed
    /// directly.
    pub fn range_owners(&self, range: u16, rf: usize) -> Vec<NodeId> {
        let table = self.table(rf);
        if rf <= table.rf() || table.lists_all_normal() {
            table.owners(range).take(rf).map(|m| m.id).collect()
        } else {
            hrw::owners(&range.to_be_bytes(), &self.members, rf)
        }
    }

    /// The ranges whose ordered owner list differs between the two epochs.
    pub fn changed_ranges(&self, other: &Epoch, rf: usize) -> RangeSet {
        let mut changed = RangeSet::empty();
        for range in 0..RANGES {
            if self.range_owners(range, rf) != other.range_owners(range, rf) {
                changed.insert(range);
            }
        }
        changed
    }
}
