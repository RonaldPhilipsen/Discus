//! Multi-key and keyspace commands across the cluster (spec section 7.3).

use std::sync::Arc;

use discus_placement::NodeId;
use discus_resp::{Command, Value};
use discus_wire::ForwardMode;
use futures::future::join_all;

use super::{allowed, candidates, forward, local, spent, Dispatch, Hop, Reply};
use crate::command::util::{check_key, check_value};
use crate::command::CommandError;
use crate::node::Node;

/// The keys of one target: the subcommand and the positions of its keys in the request.
struct Group {
    target: NodeId,
    cmd: Command,
    positions: Vec<usize>,
}

/// Split `cmd` by the target of each key, in first-seen order. `width` is 1 for keys and 2 for
/// key-value pairs.
fn group(node: &Node, cmd: &Command, width: usize, write: bool, rr: u64) -> Option<Vec<Group>> {
    let mut groups: Vec<Group> = Vec::new();
    for (pos, item) in cmd[1..].chunks(width).enumerate() {
        let target = *candidates(node, &item[0], write, rr).first()?;
        let g = match groups.iter().position(|g| g.target == target) {
            Some(i) => &mut groups[i],
            None => {
                groups.push(Group {
                    target,
                    cmd: vec![cmd[0].clone()],
                    positions: Vec::new(),
                });
                groups.last_mut()?
            }
        };
        g.cmd.extend(item.iter().cloned());
        g.positions.push(pos);
    }
    Some(groups)
}

/// Run one group: inline when local, forwarded otherwise, or the spent-budget rule.
async fn run_group(node: &Arc<Node>, g: &Group, hop: &Hop, write: bool) -> Option<Reply> {
    if g.target == node.id {
        return Some(local(node, &g.cmd));
    }
    if allowed(node, hop, vec![g.target]).is_empty() {
        let keys: Vec<&[u8]> = g
            .cmd
            .iter()
            .skip(1)
            .step_by(if g.cmd[0].eq_ignore_ascii_case(b"MSET") {
                2
            } else {
                1
            })
            .map(|k| &k[..])
            .collect();
        return Some(spent(node, &g.cmd, &keys, write));
    }
    forward::send(node, g.target, &g.cmd, hop, ForwardMode::Route, write)
        .await
        .ok()
}

/// The newest applied stamp of the parts.
fn newest(replies: &[Reply]) -> Option<discus_placement::Version> {
    replies.iter().filter_map(|r| r.applied).max()
}

/// How the parts of a multi-key command merge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Merge {
    /// `MGET`: place each value back at its position.
    InOrder,
    /// `EXISTS` and `DEL`: add the integers.
    Sum,
    /// `MSET`: `OK` when every part is `OK`.
    AllOk,
}

/// Plan and run a multi-key command. With every key local, the original command runs inline.
/// The grouping by owner runs in the `route` span.
pub fn multi(
    node: &Arc<Node>,
    cmd: Command,
    hop: Hop,
    rr: u64,
    merge: Merge,
    write: bool,
    route: tracing::Span,
) -> Dispatch {
    let width = if merge == Merge::AllOk { 2 } else { 1 };
    if width == 2 {
        if cmd.len().is_multiple_of(2) {
            return Dispatch::Ready(Reply::error(CommandError::WrongArity("MSET".into())));
        }
        for pair in cmd[1..].chunks(2) {
            if let Err(e) = check_key(node, &pair[0]).and_then(|_| check_value(node, &pair[1])) {
                return Dispatch::Ready(Reply::error(e));
            }
        }
    }
    let groups = route.in_scope(|| group(node, &cmd, width, write, rr));
    drop(route);
    let Some(groups) = groups else {
        return Dispatch::Ready(Reply::try_again());
    };
    if groups.iter().all(|g| g.target == node.id) {
        return Dispatch::Ready(local(node, &cmd));
    }
    let node = node.clone();
    let total = (cmd.len() - 1) / width;
    Dispatch::Pending(Box::pin(async move {
        let parts = join_all(groups.iter().map(|g| run_group(&node, g, &hop, write))).await;
        let mut replies = Vec::with_capacity(parts.len());
        for part in parts {
            match part {
                Some(r) if !matches!(r.value, Value::Error(_)) => replies.push(r),
                // A failed part makes the whole command fail. The other parts stay applied.
                _ => return Reply::try_again(),
            }
        }
        let applied = newest(&replies);
        let value = match merge {
            Merge::InOrder => {
                let mut out = vec![Value::Null; total];
                for (g, r) in groups.iter().zip(replies) {
                    let Value::Array(values) = r.value else {
                        return Reply::try_again();
                    };
                    for (pos, v) in g.positions.iter().zip(values) {
                        out[*pos] = v;
                    }
                }
                Value::Array(out)
            }
            Merge::Sum => {
                let mut sum = 0i64;
                for r in replies {
                    let Value::Integer(n) = r.value else {
                        return Reply::try_again();
                    };
                    sum += n;
                }
                Value::Integer(sum)
            }
            Merge::AllOk => Value::ok(),
        };
        Reply { value, applied }
    }))
}

/// Send a `LOCAL` copy of `cmd` to every other member, and run it here too.
async fn everywhere(
    node: &Arc<Node>,
    cmd: &Command,
    write: bool,
) -> (Reply, Vec<Result<Reply, NodeId>>) {
    let here = local(node, cmd);
    let others: Vec<NodeId> = node
        .epochs()
        .current()
        .members()
        .iter()
        .map(|m| m.id)
        .filter(|id| *id != node.id)
        .collect();
    let hop = Hop::default();
    let results = join_all(others.iter().map(|id| {
        let hop = &hop;
        async move {
            forward::send(node, *id, cmd, hop, ForwardMode::Local, write)
                .await
                .map_err(|_| *id)
        }
    }))
    .await;
    (here, results)
}

/// `DBSIZE`: the sum of the members that answer. Plan 4 divides by the replication factor.
pub fn dbsize(node: &Arc<Node>, cmd: Command) -> Dispatch {
    let node = node.clone();
    Dispatch::Pending(Box::pin(async move {
        let (here, others) = everywhere(&node, &cmd, false).await;
        let mut sum = match here.value {
            Value::Integer(n) => n,
            other => return Reply::value(other),
        };
        for r in others.into_iter().flatten() {
            if let Value::Integer(n) = r.value {
                sum += n;
            }
        }
        Reply::value(Value::Integer(sum))
    }))
}

/// `FLUSHDB` and `FLUSHALL`: every member, best-effort.
pub fn flush(node: &Arc<Node>, cmd: Command) -> Dispatch {
    let node = node.clone();
    Dispatch::Pending(Box::pin(async move {
        let (here, others) = everywhere(&node, &cmd, true).await;
        if matches!(here.value, Value::Error(_)) {
            return here;
        }
        for failed in others.into_iter().filter_map(Result::err) {
            tracing::warn!(peer = %failed, "a member missed the flush");
        }
        here
    }))
}
