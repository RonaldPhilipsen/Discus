//! The core's view of the node: the epoch history, the HLC, and the transport cluster ID.

use std::sync::{Arc, Weak};

use discus_membership::{provisional_epoch, Host};
use discus_placement::{Adopt, ClusterId, Epoch, EpochError, NodeId, NTP64};
use discus_transport::Transport;

use crate::Node;

pub struct NodeHost {
    pub node: Weak<Node>,
    pub id: NodeId,
    pub gossip: Transport,
    pub rpc: Transport,
    /// The cluster that the last lease read named. The merge rule uses it.
    pub lease_cluster: Option<ClusterId>,
}

impl Host for NodeHost {
    fn epoch(&self) -> Arc<Epoch> {
        match self.node.upgrade() {
            Some(node) => node.epochs().current().clone(),
            None => Arc::new(provisional_epoch(self.id)),
        }
    }

    fn adopt(&mut self, epoch: Epoch) -> Result<Adopt, EpochError> {
        match self.node.upgrade() {
            Some(node) => {
                // Adoption spawns store work, which belongs on the data runtime.
                let _data = node.data_handle().enter();
                node.adopt_epoch(epoch)
            }
            None => Ok(Adopt::Stale),
        }
    }

    fn next_generation(&self) -> u64 {
        self.node
            .upgrade()
            .map_or(1, |n| n.epochs().next_generation())
    }

    fn reset_cluster(&mut self, epoch: Epoch) {
        // The RPC channel only admits peers of the new cluster from now on. The gossip endpoint
        // reports the cluster in its HELLO, so it changes too.
        self.gossip.set_cluster_id(epoch.cluster_id());
        self.rpc.set_cluster_id(epoch.cluster_id());
        if let Some(node) = self.node.upgrade() {
            let _data = node.data_handle().enter();
            node.reset_cluster(epoch);
        }
    }

    fn hlc(&self) -> u64 {
        self.node.upgrade().map_or(0, |n| n.hlc.current().as_u64())
    }

    fn set_hlc_floor(&mut self, floor: u64) -> bool {
        self.node
            .upgrade()
            .is_some_and(|n| n.hlc.set_floor(NTP64(floor)).is_ok())
    }

    fn lease_cluster(&self) -> Option<ClusterId> {
        self.lease_cluster
    }
}
