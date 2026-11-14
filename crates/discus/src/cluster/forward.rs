//! Send one command to another node and read its reply.

use std::sync::Arc;

use discus_placement::{ClusterId, NodeId};
use discus_resp::Command;
use discus_transport::{Lane, PeerInfo, TransportError};
use discus_wire::{encode_command, Forward, ForwardMode, Message};

use tracing::Instrument;

use super::{Hop, Reply};
use crate::node::Node;
use crate::telemetry;

/// Forward `cmd` in database `db` to `target`. The forward carries the current epoch, the full epoch when the
/// peer last reported an older one, the hop count, the path, and the trace
/// context of the `forward` span. The entry node observes the applied HLC of the reply.
/// `timeout` replaces the forward timeout of the route. Counter commands wait for a quorum on
/// the leader, so they need longer than a store write.
#[allow(clippy::too_many_arguments)]
pub async fn send(
    node: &Arc<Node>,
    target: NodeId,
    db: u32,
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
    let result = async {
        let (link, addr, epoch) = node.peer(target).map_err(TransportError::Connect)?;
        // The full epoch rides along when the peer is behind (spec section 7.2).
        let mut cluster = ClusterId::NIL;
        let build = |info: &PeerInfo| {
            cluster = info.cluster_id;
            link.saw_peer_epoch(target, info.cluster_id, info.epoch);
            let known = link.peer_epoch(target).unwrap_or(info.epoch);
            let push = epoch.id() > known;
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
                db,
            }))
        };
        let timeout = timeout.unwrap_or(if write {
            link.write_timeout()
        } else {
            link.read_timeout()
        });
        let trace = telemetry::outbound(&hop.trace);
        let frame = link
            .transport
            .request_with(Lane::Command, target, addr, build, trace, timeout, write)
            .await?;
        match frame.message {
            Message::ForwardReply(reply) => {
                link.saw_peer_epoch(target, cluster, reply.epoch);
                if let Some(applied) = reply.applied {
                    // A timestamp too far ahead is dropped by the HLC. The reply still counts.
                    let _ = node.hlc.observe(applied);
                }
                Ok(Reply {
                    value: reply.value,
                    applied: reply.applied,
                    observed: reply.observed,
                    settled: reply.settled,
                    failed: None,
                })
            }
            other => Err(TransportError::Frame(format!(
                "expected FORWARD_REPLY, got message type {}",
                other.type_code()
            ))),
        }
    }
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
