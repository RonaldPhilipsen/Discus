//! Multi-key and keyspace commands across the cluster (spec section 7.3).

use std::sync::Arc;

use discus_placement::NodeId;
use discus_resp::{Command, Value};
use discus_wire::ForwardMode;
use futures::future::join_all;

use super::route::{key_args, Route};
use super::writeset::SetKey;
use super::{
    allowed, candidates, forward, hold, local, local_acked, spent, Dispatch, Failure, Hop, Job,
    Reply,
};
use std::time::Instant;

use crate::command::util::check_mset;
use crate::command::CommandSpec;
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
    db: u32,
    g: &Group<NodeId>,
    hop: &Hop,
    write: bool,
) -> Option<Reply> {
    if g.target == node.id {
        return Some(local_acked(node, spec, db, g.cmd.clone()).finish().await);
    }
    if allowed(node, hop, vec![g.target]).is_empty() {
        let Some(keys) = key_args(spec.route, &g.cmd) else {
            return Some(Reply::try_again());
        };
        let keys: Vec<&[u8]> = keys.map(|k| &k[..]).collect();
        return Some(
            spent(node, spec, db, g.cmd.clone(), &keys, write)
                .finish()
                .await,
        );
    }
    hold::send_or_hold(node, spec, db, g.target, &g.cmd, hop, write).await
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
    /// The merge of a multi-key route and the number of arguments per key. `None` for any other
    /// route.
    pub(crate) fn of(route: Route) -> Option<(Merge, usize)> {
        match route {
            Route::MultiRead { sum: true } | Route::MultiWrite { pairs: false } => {
                Some((Merge::Sum, 1))
            }
            Route::MultiRead { sum: false } => Some((Merge::InOrder, 1)),
            Route::MultiWrite { pairs: true } => Some((Merge::AllOk, 2)),
            _ => None,
        }
    }

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
    writes: Option<&super::Writes>,
    route: tracing::Span,
) -> Dispatch {
    let Job {
        cmd,
        spec,
        rr,
        redirect,
        readonly,
        db,
    } = job;
    let Some((merge, width)) = Merge::of(spec.route) else {
        drop(route);
        return Dispatch::Ready(local(node, spec, db, &cmd));
    };
    let write = matches!(spec.route, Route::MultiWrite { .. });
    if width == 2 {
        if let Err(e) = check_mset(node, &cmd) {
            return Dispatch::Ready(Reply::error(e));
        }
    }
    let now = Instant::now();
    let need = |key: &[u8]| match writes {
        Some(w) if !write => w.lock().required(SetKey::of(db, key), || now),
        _ => None,
    };
    if super::redirect::applies(redirect, &hop) {
        let keys: Vec<&[u8]> = key_args(spec.route, &cmd)
            .into_iter()
            .flatten()
            .map(|k| &k[..])
            .collect();
        let moved =
            route.in_scope(|| super::redirect::multi_moved(node, db, &keys, write, need, readonly));
        if let Some(moved) = moved {
            return Dispatch::Ready(Reply::error(moved));
        }
    }
    let groups = route.in_scope(|| {
        split(&cmd, width, |key| {
            let owners = node.owners(key);
            candidates(node, db, key, &owners, write, rr, need(key))
                .first()
                .copied()
        })
    });
    drop(route);
    let Some(groups) = groups else {
        return Dispatch::Ready(Reply::try_again());
    };
    // Each written key enters the write set with the stamp of its own part. Only a write notes.
    let writes = writes.filter(|_| write).cloned();
    let note = move |node: &Node, cmd: &Command, reply: &Reply| {
        if let (Some(w), Some(v)) = (&writes, reply.applied) {
            if write && !matches!(reply.value, Value::Error(_)) {
                let mut w = w.lock();
                for k in key_args(spec.route, cmd)
                    .into_iter()
                    .flatten()
                    .filter(|k| !node.is_primary_read_key(k))
                {
                    w.record(SetKey::of(db, k), v, Instant::now());
                }
            }
        }
    };
    if groups.iter().all(|g| g.target == node.id) {
        return local_acked(node, spec, db, cmd.clone())
            .then(node, move |node, reply| note(node, &cmd, reply));
    }
    let node = node.clone();
    let total = (cmd.len() - 1) / width;
    Dispatch::Pending(Box::pin(async move {
        let parts = join_all(
            groups
                .iter()
                .map(|g| run_group(&node, spec, db, g, &hop, write)),
        )
        .await;
        for (g, part) in groups.iter().zip(&parts) {
            if let Some(r) = part {
                note(&node, &g.cmd, r);
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
            applied,
            ..Reply::value(value)
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
    let here = local(node, spec, 0, cmd);
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
            forward::send(node, *id, 0, cmd, hop, ForwardMode::Local, write, None)
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
        Reply::value(Value::Integer(sum / copies(&node)))
    }))
}

/// How many members hold each key.
pub(crate) fn copies(node: &Node) -> i64 {
    let normal = discus_membership::normal_count(node.epochs().current());
    node.config.replication_factor.min(normal).max(1) as i64
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
