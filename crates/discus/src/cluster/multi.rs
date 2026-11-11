//! Multi-key and keyspace commands across the cluster (spec section 7.3).

use std::sync::Arc;

use discus_placement::NodeId;
use discus_resp::{Command, Value};
use discus_wire::ForwardMode;
use futures::future::join_all;

use super::route::{keys_of, Keys};
use super::{allowed, candidates, forward, hold, local, spent, Dispatch, Failure, Hop, Job, Reply};
use std::time::Instant;

use crate::command::util::{check_key, check_value};
use crate::command::{CommandError, CommandSpec};
use crate::node::Node;
use bytes::Bytes;

/// The keys of one target: the subcommand and the positions of its keys in the request.
pub(crate) struct Group<T> {
    pub target: T,
    pub cmd: Command,
    pub positions: Vec<usize>,
}

/// Split `cmd` by the target of each key, in first-seen order. `width` is 1 for keys and 2 for
/// key-value pairs. `None` when a key has no target.
pub(crate) fn split<T: PartialEq>(
    cmd: &Command,
    width: usize,
    mut target: impl FnMut(&Bytes) -> Option<T>,
) -> Option<Vec<Group<T>>> {
    let mut groups: Vec<Group<T>> = Vec::new();
    for (pos, item) in cmd[1..].chunks(width).enumerate() {
        let target = target(&item[0])?;
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

/// Run one group: inline when local, forwarded otherwise, or the spent-budget rule. A failed
/// forward of a write part holds on the entry node, like a single-key write (spec section 7.5).
async fn run_group(
    node: &Arc<Node>,
    spec: &'static CommandSpec,
    g: &Group<NodeId>,
    hop: &Hop,
    write: bool,
) -> Option<Reply> {
    if g.target == node.id {
        return Some(local(node, spec, &g.cmd));
    }
    if allowed(node, hop, vec![g.target]).is_empty() {
        let Keys::Some(keys) = keys_of(spec.route, &g.cmd) else {
            return Some(Reply::try_again());
        };
        let keys: Vec<&[u8]> = keys.iter().map(|k| &k[..]).collect();
        return Some(spent(node, spec, &g.cmd, &keys, write));
    }
    hold::send_or_hold(node, spec, g.target, &g.cmd, hop, write).await
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

impl Merge {
    /// Merge the non-error replies of `groups`, one per group in order. `None` when a part has
    /// the wrong shape.
    pub(crate) fn apply<T>(
        self,
        total: usize,
        groups: &[Group<T>],
        parts: impl IntoIterator<Item = Value>,
    ) -> Option<Value> {
        Some(match self {
            Merge::InOrder => {
                let mut out = vec![Value::Null; total];
                for (g, part) in groups.iter().zip(parts) {
                    let Value::Array(values) = part else {
                        return None;
                    };
                    for (pos, v) in g.positions.iter().zip(values) {
                        out[*pos] = v;
                    }
                }
                Value::Array(out)
            }
            Merge::Sum => {
                let mut sum = 0i64;
                for part in parts {
                    let Value::Integer(n) = part else {
                        return None;
                    };
                    sum += n;
                }
                Value::Integer(sum)
            }
            Merge::AllOk => Value::ok(),
        })
    }
}

/// Plan and run a multi-key command. With every key local, the original command runs inline.
/// The grouping by owner runs in the `route` span.
pub fn multi(
    node: &Arc<Node>,
    job: Job,
    hop: Hop,
    merge: Merge,
    write: bool,
    route: tracing::Span,
) -> Dispatch {
    let Job {
        cmd,
        spec,
        rr,
        writes,
        redirect,
        readonly,
    } = job;
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
    let now = Instant::now();
    let need = |key: &[u8]| match &writes {
        Some(w) if !write => w.lock().required(key, now),
        _ => None,
    };
    if super::redirect::applies(redirect, &hop) {
        let keys: Vec<&[u8]> = cmd[1..].chunks(width).map(|item| &item[0][..]).collect();
        let moved =
            route.in_scope(|| super::redirect::multi_moved(node, &keys, write, need, readonly));
        if let Some(moved) = moved {
            return Dispatch::Ready(Reply::error(moved));
        }
    }
    let groups = route.in_scope(|| {
        split(&cmd, width, |key| {
            let owners = node.owners(key);
            candidates(node, key, &owners, write, rr, need(key))
                .first()
                .copied()
        })
    });
    drop(route);
    let Some(groups) = groups else {
        return Dispatch::Ready(Reply::try_again());
    };
    // Each written key enters the write set with the stamp of its own part.
    let note = move |keys: &[Bytes], reply: &Reply| {
        if let (Some(w), Some(v)) = (&writes, reply.applied) {
            if write && !matches!(reply.value, Value::Error(_)) {
                let mut w = w.lock();
                for k in keys.iter().step_by(width) {
                    w.record(k, v, Instant::now());
                }
            }
        }
    };
    if groups.iter().all(|g| g.target == node.id) {
        let reply = local(node, spec, &cmd);
        note(&cmd[1..], &reply);
        return Dispatch::Ready(reply);
    }
    let node = node.clone();
    let total = (cmd.len() - 1) / width;
    Dispatch::Pending(Box::pin(async move {
        let parts = join_all(
            groups
                .iter()
                .map(|g| run_group(&node, spec, g, &hop, write)),
        )
        .await;
        for (g, part) in groups.iter().zip(&parts) {
            if let Some(r) = part {
                note(&g.cmd[1..], r);
            }
        }
        // A held retry of this command learns whether every failed part was never sent.
        let failures: Vec<Failure> = parts.iter().flatten().filter_map(|r| r.failed).collect();
        if !failures.is_empty() {
            let unsent = failures.iter().all(|f| f.unsent);
            return Failure { unsent }.reply();
        }
        let mut replies = Vec::with_capacity(parts.len());
        for part in parts {
            match part {
                Some(r) if !matches!(r.value, Value::Error(_)) => replies.push(r),
                // A failed part makes the whole command fail. The other parts stay applied.
                _ => return Reply::try_again(),
            }
        }
        let applied = newest(&replies);
        let Some(value) = merge.apply(total, &groups, replies.into_iter().map(|r| r.value)) else {
            return Reply::try_again();
        };
        Reply {
            value,
            applied,
            observed: None,
            settled: false,
            failed: None,
        }
    }))
}

/// Send a `LOCAL` copy of `cmd` to every other member, and run it here too.
async fn everywhere(
    node: &Arc<Node>,
    spec: &CommandSpec,
    cmd: &Command,
    write: bool,
) -> (Reply, Vec<Result<Reply, NodeId>>) {
    let here = local(node, spec, cmd);
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
            forward::send(node, *id, cmd, hop, ForwardMode::Local, write, None)
                .await
                .map_err(|_| *id)
        }
    }))
    .await;
    (here, results)
}

/// `DBSIZE`: the sum of the members that answer, divided by the owners per key. Every key lives
/// on up to `RF` members, so this is an estimate (spec section 7.3).
pub fn dbsize(node: &Arc<Node>, spec: &'static CommandSpec, cmd: Command) -> Dispatch {
    let node = node.clone();
    Dispatch::Pending(Box::pin(async move {
        let (here, others) = everywhere(&node, spec, &cmd, false).await;
        let mut sum = match here.value {
            Value::Integer(n) => n,
            other => return Reply::value(other),
        };
        for r in others.into_iter().flatten() {
            if let Value::Integer(n) = r.value {
                sum += n;
            }
        }
        let normal = discus_membership::normal_count(node.epochs().current());
        let copies = node.config.replication_factor.min(normal).max(1) as i64;
        Reply::value(Value::Integer(sum / copies))
    }))
}

/// `FLUSHDB` and `FLUSHALL`: every member, best-effort.
pub fn flush(node: &Arc<Node>, spec: &'static CommandSpec, cmd: Command) -> Dispatch {
    let node = node.clone();
    Dispatch::Pending(Box::pin(async move {
        let (here, others) = everywhere(&node, spec, &cmd, true).await;
        if matches!(here.value, Value::Error(_)) {
            return here;
        }
        for failed in others.into_iter().filter_map(Result::err) {
            tracing::warn!(peer = %failed, "a member missed the flush");
        }
        here
    }))
}
