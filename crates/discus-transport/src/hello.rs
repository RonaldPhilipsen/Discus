//! `HELLO` checks and negotiation (spec section 13.3).

use std::net::SocketAddr;

use discus_placement::{ClusterId, NodeId};
use discus_wire::{caps, HandshakeCode, Hello, HELLO_FRAME_LIMIT};

use crate::endpoint::{Channel, PeerInfo};

/// Check the peer `HELLO` from `addr` against the local identity, and return what both sides
/// agreed. `expected` is the node that the dialer meant to reach. The gossip channel accepts
/// another cluster: membership needs it to admit a joiner and to find a second cluster (spec
/// section 6.3).
pub(crate) fn check(
    cluster_id: ClusterId,
    capabilities: u64,
    max_frame: usize,
    theirs: &Hello,
    addr: SocketAddr,
    expected: Option<NodeId>,
    channel: Channel,
) -> Result<PeerInfo, (HandshakeCode, String)> {
    if channel == Channel::Rpc && theirs.cluster_id != cluster_id {
        return Err((
            HandshakeCode::ClusterMismatch,
            format!("cluster {} is not {}", theirs.cluster_id, cluster_id),
        ));
    }
    if (theirs.max_frame as usize) < HELLO_FRAME_LIMIT {
        return Err((
            HandshakeCode::FrameTooSmall,
            format!(
                "a maximum frame of {} bytes is below {HELLO_FRAME_LIMIT}",
                theirs.max_frame
            ),
        ));
    }
    if theirs.capabilities & caps::SLOT_PLACEMENT == 0 {
        return Err((
            HandshakeCode::PlacementMismatch,
            "the peer places keys by an older rule".to_string(),
        ));
    }
    if let Some(want) = expected {
        if theirs.node_id != want {
            return Err((
                HandshakeCode::UnexpectedNode,
                format!("expected node {want}, found {}", theirs.node_id),
            ));
        }
    }
    Ok(PeerInfo {
        node_id: theirs.node_id,
        addr,
        capabilities: capabilities & theirs.capabilities & caps::KNOWN,
        max_frame: max_frame.min(theirs.max_frame as usize),
        epoch: theirs.epoch,
        cluster_id: theirs.cluster_id,
        channel,
    })
}
