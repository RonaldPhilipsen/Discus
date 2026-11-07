//! Send one command to another node and read its reply.

use std::sync::Arc;

use discus_placement::NodeId;
use discus_resp::Command;
use discus_transport::TransportError;
use discus_wire::{caps, encode_command, Forward, ForwardMode, Message};

use tracing::Instrument;

use super::{Hop, Reply};
use crate::node::Node;
use crate::telemetry;

/// Forward `cmd` to `target`. The forward carries the current epoch, the full epoch when the
/// peer has `EPOCH_PUSH` and last reported an older one, the hop count, the path, and the trace
/// context of the `forward` span. The entry node observes the applied HLC of the reply.
pub async fn send(
    node: &Arc<Node>,
    target: NodeId,
    cmd: &Command,
    hop: &Hop,
    mode: ForwardMode,
    write: bool,
) -> Result<Reply, TransportError> {
    let result = send_in_span(node, target, cmd, hop, mode, write)
        .instrument(telemetry::forward_span())
        .await;
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
) -> Result<Reply, TransportError> {
    let link = node
        .link()
        .ok_or_else(|| TransportError::Connect("this node has no transport".into()))?;
    let epoch = node.epochs().current().clone();
    let addr = epoch
        .member(target)
        .map(|m| m.addr)
        .ok_or_else(|| TransportError::Connect(format!("node {target} is not a member")))?;
    let info = link.transport.peer(target, addr).await?;
    let known = link
        .peer_epoch(target)
        .unwrap_or(info.epoch)
        .max(info.epoch);
    let push = info.capabilities & caps::EPOCH_PUSH != 0 && epoch.id() > known;
    let mut path = hop.path.clone();
    path.push(node.id);
    let forward = Forward {
        command: encode_command(cmd),
        epoch: epoch.id(),
        epoch_body: push.then(|| (*epoch).clone()),
        hops: hop.hops.saturating_add(1),
        path,
        mode,
    };
    let timeout = if write {
        link.write_timeout
    } else {
        link.read_timeout
    };
    let frame = link
        .transport
        .request(
            target,
            addr,
            Message::Forward(forward),
            telemetry::outbound(&hop.trace),
            timeout,
            write,
        )
        .await?;
    match frame.message {
        Message::ForwardReply(reply) => {
            link.saw_peer_epoch(target, reply.epoch);
            if let Some(applied) = reply.applied {
                // A timestamp too far ahead is dropped by the HLC. The reply still counts.
                let _ = node.hlc.observe(applied);
            }
            Ok(Reply {
                value: reply.value,
                applied: reply.applied,
            })
        }
        other => Err(TransportError::Frame(format!(
            "expected FORWARD_REPLY, got message type {}",
            other.type_code()
        ))),
    }
}
