//! The inbound side: execute commands that other nodes forward.

use std::sync::{Arc, Weak};

use discus_placement::EpochId;
use discus_resp::{Limits, Parser, Value};
use discus_transport::{Inbound, Service};
use discus_wire::{Forward, ForwardMode, ForwardReply, Message, TraceContext};
use futures::future::BoxFuture;

use super::route::{route_of, Route};
use super::{local, run_hop, Hop, Job, Reply};
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
                return Message::Unsupported {
                    message_type: inbound.message.type_code(),
                };
            };
            match inbound.message {
                Message::Ping { nonce } => Message::Pong { nonce },
                Message::Forward(f) => {
                    let span =
                        telemetry::forwarded_span(discus_telemetry::global(), &inbound.trace);
                    let reply = forwarded(&node, inbound.peer.node_id, f, inbound.trace)
                        .instrument(span)
                        .await;
                    Message::ForwardReply(ForwardReply {
                        value: reply.value,
                        applied: reply.applied,
                        epoch: node.epoch_id(),
                    })
                }
                other => Message::Unsupported {
                    message_type: other.type_code(),
                },
            }
        })
    }
}

/// Parse the raw command of a forward. The limits match the client parser.
fn parse(node: &Node, raw: &[u8]) -> Result<discus_resp::Command, Value> {
    let cfg = &node.config;
    let limits = Limits {
        max_bulk_bytes: cfg
            .max_value_bytes
            .max(cfg.max_key_bytes)
            .saturating_add(1 << 20),
        ..Limits::default()
    };
    let mut parser = Parser::new(limits);
    parser.feed(raw);
    match parser.next() {
        Ok(Some(cmd)) => Ok(cmd),
        Ok(None) => Err(Value::error(
            "ERR Protocol error: incomplete forwarded command",
        )),
        Err(e) => Err(Value::error(&e.to_string())),
    }
}

/// Run a forwarded command inside its command span, and mark the span on an error reply.
async fn forwarded(
    node: &Arc<Node>,
    sender: discus_placement::NodeId,
    f: Forward,
    trace: TraceContext,
) -> Reply {
    let reply = run_forwarded(node, sender, f, trace).await;
    telemetry::record_reply(&reply.value);
    reply
}

async fn run_forwarded(
    node: &Arc<Node>,
    sender: discus_placement::NodeId,
    f: Forward,
    trace: TraceContext,
) -> Reply {
    if let Some(link) = node.link() {
        link.saw_peer_epoch(sender, f.epoch);
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
    telemetry::record_operation(&cmd);
    if f.mode == ForwardMode::Local {
        return local(node, &cmd);
    }
    // The sender authorized the command on its entry node, and cluster mTLS authenticated the
    // sender, so only the lookup and the arity run here.
    let spec = match command::check(&cmd) {
        Ok(spec) => spec,
        Err(e) => return Reply::error(e),
    };
    let route = route_of(spec.name);
    if route == Route::Connection {
        return local(node, &cmd);
    }
    // The hop keeps the inbound context. A further forward sends the context of its own span
    // with `otel`, else this context unchanged.
    let hop = Hop {
        hops: f.hops,
        path: f.path,
        trace,
    };
    let job = Job { cmd, route, rr: 0 };
    run_hop(node, job, hop).finish().await
}
