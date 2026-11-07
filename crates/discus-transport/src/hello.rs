//! `HELLO` checks and negotiation (spec section 13.3).

use discus_placement::{ClusterId, NodeId};
use discus_wire::{caps, HandshakeCode, Hello, HELLO_FRAME_LIMIT};

/// What both sides agreed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Agreed {
    pub capabilities: u64,
    pub max_frame: usize,
}

/// Check the peer `HELLO` against the local identity. `expected` is the node that the dialer
/// meant to reach.
pub(crate) fn check(
    cluster_id: ClusterId,
    capabilities: u64,
    max_frame: usize,
    theirs: &Hello,
    expected: Option<NodeId>,
) -> Result<Agreed, (HandshakeCode, String)> {
    if theirs.cluster_id != cluster_id {
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
    if let Some(want) = expected {
        if theirs.node_id != want {
            return Err((
                HandshakeCode::UnexpectedNode,
                format!("expected node {want}, found {}", theirs.node_id),
            ));
        }
    }
    Ok(Agreed {
        capabilities: capabilities & theirs.capabilities & caps::KNOWN,
        max_frame: max_frame.min(theirs.max_frame as usize),
    })
}
