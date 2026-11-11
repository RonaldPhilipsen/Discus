//! Send one command to another node and read its reply.

use std::sync::Arc;

use discus_placement::NodeId;
use discus_resp::Command;
use discus_transport::{Lane, PeerInfo, TransportError};
use discus_wire::{caps, encode_command, Forward, ForwardMode, Message};

use tracing::Instrument;

use super::{Hop, Reply};
use crate::node::Node;
use crate::telemetry;

/// Forward `cmd` to `target`. The forward carries the current epoch, the full epoch when the
/// peer has `EPOCH_PUSH` and last reported an older one, the hop count, the path, and the trace
/// context of the `forward` span. The entry node observes the applied HLC of the reply.
/// `timeout` replaces the forward timeout of the route. Counter commands wait for a quorum on
/// the leader, so they need longer than a store write.
pub async fn send(
    node: &Arc<Node>,
    target: NodeId,
    cmd: &Command,
    hop: &Hop,
    mode: ForwardMode,
    write: bool,
    timeout: Option<std::time::Duration>,
) -> Result<Reply, TransportError> {
    #[cfg(feature = "testing")]
    node.faults
        .forwards
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let result = send_in_span(node, target, cmd, hop, mode, write, timeout)
        .instrument(telemetry::forward_span())
        .await;
    if let Some(link) = node.link() {
        link.note_forward(target, result.is_ok());
    }
    telemetry::record_forward(
        discus_telemetry::global(),
        cmd,
        &result,
        hop.hops.saturating_add(1),
    );
    result
}

async fn send_in_span(
    node: &Arc<Node>,
    target: NodeId,
    cmd: &Command,
    hop: &Hop,
    mode: ForwardMode,
    write: bool,
    timeout: Option<std::time::Duration>,
) -> Result<Reply, TransportError> {
    let (link, addr, epoch) = node.peer(target).map_err(TransportError::Connect)?;
    // The full epoch rides along when the peer can take it and is behind (spec section 7.2).
    let mut peer_caps = 0;
    let build = |info: &PeerInfo| {
        peer_caps = info.capabilities;
        let known = link
            .peer_epoch(target)
            .unwrap_or(info.epoch)
            .max(info.epoch);
        let push = info.capabilities & caps::EPOCH_PUSH != 0 && epoch.id() > known;
        let mut path = hop.path.clone();
        path.push(node.id);
        Ok(Message::Forward(Forward {
            command: encode_command(cmd),
            epoch: epoch.id(),
            epoch_body: push.then(|| (*epoch).clone()),
            hops: hop.hops.saturating_add(1),
            path,
            mode,
            track: hop.track,
        }))
    };
    let timeout = timeout.unwrap_or(if write {
        link.write_timeout
    } else {
        link.read_timeout
    });
    let trace = telemetry::outbound(&hop.trace);
    let frame = link
        .transport
        .request_with(Lane::Command, target, addr, build, trace, timeout, write)
        .await?;
    match frame.message {
        Message::ForwardReply(reply) => {
            link.saw_peer_epoch(target, reply.epoch);
            if let Some(applied) = reply.applied {
                // A timestamp too far ahead is dropped by the HLC. The reply still counts.
                let _ = node.hlc.observe(applied);
            }
            // A peer without replication cannot know whether its replicas caught up.
            let replicates = peer_caps & caps::REPLICATION != 0;
            Ok(Reply {
                value: reply.value,
                applied: reply.applied,
                observed: reply.observed,
                settled: reply.settled && replicates,
                failed: None,
            })
        }
        other => Err(TransportError::Frame(format!(
            "expected FORWARD_REPLY, got message type {}",
            other.type_code()
        ))),
    }
}
