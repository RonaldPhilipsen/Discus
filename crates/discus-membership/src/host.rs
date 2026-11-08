//! What the core needs from the node, and what it reports back.

use std::sync::Arc;

use discus_placement::{Adopt, ClusterId, Epoch, EpochError, EpochId, MemberState, NodeId};
use discus_wire::Liveness;

/// The node side of the core. The node keeps the epoch history and the HLC. The simulator has its
/// own implementation.
pub trait Host {
    /// The current epoch.
    fn epoch(&self) -> Arc<Epoch>;
    /// Offer an epoch of the current cluster.
    fn adopt(&mut self, epoch: Epoch) -> Result<Adopt, EpochError>;
    /// The generation for a new epoch: the highest generation seen plus one.
    fn next_generation(&self) -> u64;
    /// Replace the history with `epoch`, after a join or a lost merge.
    fn reset_cluster(&mut self, epoch: Epoch);
    /// The highest HLC time issued or observed, as a raw `NTP64`.
    fn hlc(&self) -> u64;
    /// Raise the HLC to at least `floor`. False when the floor is too far ahead of the wall
    /// clock.
    fn set_hlc_floor(&mut self, floor: u64) -> bool;
    /// The cluster that the lease names, from the last lease read.
    fn lease_cluster(&self) -> Option<ClusterId>;
}

/// What the core reports to the node.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// The local node changed its state.
    StateChanged {
        state: MemberState,
        incarnation: u64,
    },
    /// Another member changed its state or liveness.
    MemberChanged {
        id: NodeId,
        state: MemberState,
        liveness: Liveness,
    },
    /// The node adopted a newer epoch.
    EpochAdopted { old: EpochId, new: EpochId },
    /// The new epoch gives owner slots to these members.
    OwnershipGained {
        old: EpochId,
        new: EpochId,
        gained: Vec<NodeId>,
    },
    /// The node was dropped from the epoch and joins again with a new incarnation.
    Rejoining { incarnation: u64 },
    /// The node lost a merge. It discards its store and joins the winner.
    ClusterLost { old: ClusterId, winner: ClusterId },
    /// The bootstrap rule asks for the cluster lease.
    LeaseWanted,
}
