//! Hold-and-retry for writes whose primary failed (spec section 7.5).
//!
//! When a forwarded write fails or times out, the transport has already reported the peer to
//! the failure detector. The entry node then holds the command. It plans the route again on
//! every epoch change and every [`RETRY`], and sends the command to the new target. After the
//! hold budget it returns `-TRYAGAIN`. A command that was never transmitted is always safe to
//! send again. A transmitted command without a reply is sent again only if it is on the
//! idempotent list.
//!
//! A held command stays in the connection pipeline, so it counts against the pipeline limit,
//! and later commands on its keys wait for it.

use std::sync::Arc;
use std::time::Duration;

use discus_placement::NodeId;
use discus_resp::Command;
use discus_wire::ForwardMode;
use tokio::time::Instant;

use super::{run_hop, Failure, Hop, Job, Reply};
use crate::command::util::upper;
use crate::command::CommandSpec;
use crate::Node;

/// A held command plans its route again at least this often.
const RETRY: Duration = Duration::from_millis(200);

/// True for a command that is safe to apply twice: the exact list of spec section 7.5
/// plus the internal `DISCUS.PUT`.
pub fn idempotent(cmd: &Command) -> bool {
    let name = upper(&cmd[0]);
    match name.as_str() {
        "SET" => !cmd
            .iter()
            .skip(3)
            .any(|a| matches!(upper(a).as_str(), "NX" | "XX")),
        // The options of ZADD come before the first score.
        "ZADD" => !cmd
            .iter()
            .skip(2)
            .map(upper)
            .take_while(|a| matches!(a.as_str(), "NX" | "XX" | "GT" | "LT" | "CH" | "INCR"))
            .any(|a| a == "INCR"),
        "DEL" | "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" | "PERSIST" | "MSET" | "HSET"
        | "HDEL" | "SADD" | "SREM" | "ZREM" | "DISCUS.PUT" => true,
        _ => false,
    }
}

/// True when a write that failed this way may be sent again.
pub fn resendable(cmd: &Command, failure: Failure) -> bool {
    failure.unsent || idempotent(cmd)
}

/// The forward timeout of a delete of a listed key. The owner waits for a secondary before it
/// replies, so a timeout without this allowance would turn a fail-open delete into `-TRYAGAIN`.
fn delete_timeout(node: &Node, spec: &CommandSpec, cmd: &Command, write: bool) -> Option<Duration> {
    if !write || super::acked_delete_keys(node, spec, cmd).is_empty() {
        return None;
    }
    let live_settings = node.live_settings.get();
    node.link()
        .map(|_| live_settings.forward_write_timeout + live_settings.replication_timeout)
}

/// Forward `cmd` to `target`. A failed read gives `None`, so the caller tries the next owner. A
/// failed write waits for a new primary, on the entry node only.
pub async fn send_or_hold(
    node: &Arc<Node>,
    spec: &'static CommandSpec,
    target: NodeId,
    cmd: &Command,
    hop: &Hop,
    write: bool,
) -> Option<Reply> {
    let timeout = delete_timeout(node, spec, cmd, write);
    match super::forward::send(node, target, cmd, hop, ForwardMode::Route, write, timeout).await {
        Ok(reply) => Some(reply),
        Err(_) if !write => None,
        Err(e) if hop.hops == 0 && !hop.held => {
            Some(held(node, spec, cmd.clone(), hop.clone(), Failure::of(&e)).await)
        }
        Err(e) => Some(Failure::of(&e).reply()),
    }
}

/// Hold `cmd` after its first failure and retry it until it gets an answer or the budget ends.
pub async fn held(
    node: &Arc<Node>,
    spec: &'static CommandSpec,
    cmd: Command,
    hop: Hop,
    first: Failure,
) -> Reply {
    if !resendable(&cmd, first) {
        return Reply::try_again();
    }
    let deadline = Instant::now() + node.live_settings.get().hold_budget;
    let mut epochs = node.subscribe_epoch();
    epochs.mark_unchanged();
    let retry = Hop { held: true, ..hop };
    loop {
        let wake = (Instant::now() + RETRY).min(deadline);
        tokio::select! {
            _ = epochs.changed() => {}
            _ = tokio::time::sleep_until(wake) => {}
        }
        if Instant::now() >= deadline {
            return Reply::try_again();
        }
        let job = Job::plain(cmd.clone(), spec);
        let reply = run_hop(node, job, retry.clone(), None, tracing::Span::none())
            .finish()
            .await;
        match reply.failed {
            None => return reply,
            Some(f) if !resendable(&cmd, f) => return Reply::try_again(),
            Some(_) => {}
        }
    }
}
