//! Epochs: numbered snapshots of the member list, spec §5.1.

use std::cmp::Ordering;
use std::fmt;

use crate::{hrw, ClusterId, Member, NodeId};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EpochError {
    /// The epoch belongs to another cluster. Epochs of different clusters are never compared.
    ForeignCluster { ours: ClusterId, theirs: ClusterId },
    /// The member list names one node twice.
    DuplicateMember(NodeId),
    /// The history does not hold this epoch.
    Unknown(EpochId),
}

impl fmt::Display for EpochError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EpochError::ForeignCluster { ours, theirs } => {
                write!(f, "epoch of cluster {theirs} does not belong to cluster {ours}")
            }
            EpochError::DuplicateMember(id) => write!(f, "member {id} appears twice"),
            EpochError::Unknown(id) => write!(f, "epoch {id} is not in the history"),
        }
    }
}

impl std::error::Error for EpochError {}

/// One epoch: its identity, its cluster, and its member list sorted by node ID.
#[derive(Debug, Clone, PartialEq)]
pub struct Epoch {
    id: EpochId,
    cluster_id: ClusterId,
    members: Vec<Member>,
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
        })
    }

    /// The first epoch of a new cluster: generation 1, with the bootstrap node as origin.
    pub fn bootstrap(member: Member, cluster_id: ClusterId) -> Epoch {
        Epoch {
            id: EpochId {
                generation: 1,
                origin: member.id,
            },
            cluster_id,
            members: vec![member],
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

    pub fn member(&self, id: NodeId) -> Option<&Member> {
        self.members
            .binary_search_by_key(&id, |m| m.id)
            .ok()
            .map(|i| &self.members[i])
    }

    /// The owner set of `key` in this epoch. The first owner is the primary.
    pub fn owners(&self, key: &[u8], rf: usize) -> Vec<NodeId> {
        hrw::owners(key, &self.members, rf)
    }

    /// The primary of `key` in this epoch, if any member is `NORMAL`.
    pub fn primary(&self, key: &[u8]) -> Option<NodeId> {
        hrw::owners(key, &self.members, 1).first().copied()
    }

    /// Order two epochs of the same cluster. Epochs of different clusters are never compared.
    pub fn compare(&self, other: &Epoch) -> Result<Ordering, EpochError> {
        if self.cluster_id != other.cluster_id {
            return Err(EpochError::ForeignCluster {
                ours: self.cluster_id,
                theirs: other.cluster_id,
            });
        }
        Ok(self.id.cmp(&other.id))
    }
}
