//! The inbound side: execute commands that other nodes forward.

use std::sync::{Arc, Weak};

use discus_placement::EpochId;
use discus_resp::{Parser, Value};
use discus_transport::{Channel, Inbound, Service};
use discus_wire::{Forward, ForwardMode, ForwardReply, Message, TraceContext};
use futures::future::BoxFuture;

use super::route::Route;
use super::{local, local_acked, run_hop, Hop, Job, Reply};
use crate::command;
use crate::node::Node;
use crate::telemetry;
use tracing::Instrument;

/// The transport service of a node. It holds the node weakly, because the node holds the
/// transport.
pub struct NodeService(pub Weak<Node>);

impl Service for NodeService {
    fn epoch(&self) -> EpochId {
        self.0.upgrade().map(|n| n.epoch_id()).unwrap_or(EpochId {
            generation: 0,
            origin: discus_placement::NodeId::NIL,
        })
    }

    fn call(self: Arc<Self>, inbound: Inbound) -> BoxFuture<'static, Message> {
        let node = self.0.upgrade();
        Box::pin(async move {
            let Some(node) = node else {
                return unsupported(&inbound.message);
            };
            #[cfg(feature = "testing")]
            if let Some(d) = node.faults.delay(inbound.peer.node_id, &inbound.message) {
                tokio::time::sleep(d).await;
            }
            // Forwards skip the contact time: they are the request path, and gossip already
            // refreshes it every period, which is all the rejoin absence needs.
            if !matches!(inbound.message, Message::Forward(_)) {
                if let Some(r) = node.replicator() {
                    r.touch(inbound.peer.node_id);
                }
            }
            match inbound.message {
                Message::Ping { nonce } => Message::Pong { nonce },
                Message::Gossip(g) => match node.membership().cloned() {
                    Some(m) => Message::Gossip(m.handle(inbound.peer.addr, g).await),
                    None => unsupported(&Message::Gossip(g)),
                },
                // The gossip HELLO admits other clusters and joiners, so it never carries data.
                m @ (Message::Replicate(_)
                | Message::HintReplay(_)
                | Message::Handoff(_)
                | Message::Fetch { .. }
                | Message::Digest { .. })
                    if inbound.peer.channel == Channel::Rpc =>
                {
                    let Some(replicator) = node.replicator().cloned() else {
                        return unsupported(&m);
                    };
                    replicator.handle(inbound.peer.node_id, m).await
                }
                m @ (Message::RaftLeader(_)
                | Message::RaftConfig(_)
                | Message::RaftVote(_)
                | Message::RaftAppend(_)
                | Message::RaftSnapshot(_)
                | Message::CounterSync(_)
                | Message::CounterAck(_))
                    if inbound.peer.channel == Channel::Rpc =>
                {
                    let Some(counters) = node.counters().cloned() else {
                        return unsupported(&m);
                    };
                    counters.handle(inbound.peer.node_id, m).await
                }
                m @ Message::Forward(_) if inbound.peer.channel == Channel::Gossip => {
                    unsupported(&m)
                }
                Message::Forward(f) => {
                    let span =
                        telemetry::forwarded_span(discus_telemetry::global(), &inbound.trace);
                    let reply = forwarded(&node, &inbound.peer, f, inbound.trace)
                        .instrument(span.clone())
                        .await;
                    // An error reply marks the command span.
                    span.in_scope(|| telemetry::record_reply(&reply.value));
                    Message::ForwardReply(ForwardReply {
                        value: reply.value,
                        applied: reply.applied,
                        epoch: node.epoch_id(),
                        observed: reply.observed,
                        settled: reply.settled,
                    })
                }
                other => unsupported(&other),
            }
        })
    }
}

fn unsupported(m: &Message) -> Message {
    Message::Unsupported {
        message_type: m.type_code(),
    }
}

/// Parse the raw command of a forward. The limits match the client parser.
fn parse(node: &Node, raw: &[u8]) -> Result<discus_resp::Command, Value> {
    let mut parser = Parser::new(node.config.parser_limits());
    parser.feed(raw);
    match parser.next() {
        Ok(Some(cmd)) => Ok(cmd),
        Ok(None) => Err(Value::error(
            "ERR Protocol error: incomplete forwarded command",
        )),
        Err(e) => Err(Value::error(&e.to_string())),
    }
}

/// Run a forwarded command.
async fn forwarded(
    node: &Arc<Node>,
    sender: &discus_transport::PeerInfo,
    f: Forward,
    trace: TraceContext,
) -> Reply {
    if let Some(link) = node.link() {
        link.saw_peer_epoch(sender.node_id, sender.cluster_id, f.epoch);
    }
    // A receiver with a lower epoch adopts the higher one before it acts (spec section 7.2).
    if f.epoch > node.epoch_id() {
        if let Some(body) = f.epoch_body.filter(|b| b.id() == f.epoch) {
            if let Err(e) = node.adopt_epoch(body) {
                tracing::warn!(error = %e, "refused a pushed epoch");
            }
        }
    }
    if f.path.contains(&node.id) {
        tracing::debug!(path = ?f.path, "forwarding loop");
        return Reply::try_again();
    }
    let cmd = match parse(node, &f.command) {
        Ok(cmd) => cmd,
        Err(value) => return Reply::value(value),
    };
    let spec = command::lookup(&cmd[0]).or_else(|| command::lookup_internal(&cmd[0]));
    telemetry::record_operation(spec, &cmd);
    // The sender authorized the command on its entry node, and cluster mTLS authenticated the
    // sender, so only the lookup and the arity run here.
    let spec = match command::check(spec, &cmd) {
        Ok(spec) => spec,
        Err(e) => return Reply::error(e),
    };
    if spec.route == Route::Connection {
        return local(node, spec, &cmd);
    }
    if f.mode == ForwardMode::Local {
        return local_acked(node, spec, cmd).finish().await;
    }
    // The hop keeps the inbound context. A further forward sends the context of its own span
    // with `otel`, else this context unchanged.
    let hop = Hop {
        hops: f.hops,
        path: f.path,
        trace,
        track: f.track,
        held: false,
    };
    run_hop(
        node,
        Job::plain(cmd, spec),
        hop,
        None,
        telemetry::route_span(),
    )
    .finish()
    .await
}
