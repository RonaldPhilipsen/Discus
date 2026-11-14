//! Routing of counter commands to the group leader (spec section 10).
//!
//! Every command on a counter key comes here before the normal single-key path, so it never
//! reaches `cluster::local` and never enters a replication queue. A command whose keys all fall
//! in one group runs as one entry on its leader. Several groups fan out like a normal multi-key
//! command. A command that mixes counter keys and other keys is refused.

use std::mem;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use discus_resp::{Command, Value};
use discus_wire::ForwardMode;
use futures::future::join_all;

use super::state::{Kind, FLUSH_ONE_DB};
use super::{Counters, Refusal};
use crate::cluster::multi::{split, Merge};
use crate::cluster::route::key_args;
use crate::cluster::{forward, Dispatch, Hop, Job, Reply, MAX_HOPS};
use crate::command::internal::COUNTER_FLUSH;
use crate::command::util::{check_key, check_mset, check_value, parse_i64};
use crate::command::{CommandError, CommandSpec};
use crate::Node;

/// The leader refused before it applied anything. The entry node tries again.
pub const NO_LEADER: &str = "TRYAGAIN the counter group has no leader, try again later";
/// The command may have applied.
pub const IN_DOUBT: &str = "TRYAGAIN the counter command did not finish, it may have applied";

const MIXED: &str = "ERR counter keys and other keys cannot be mixed in one command";
const ONLY_STRINGS: &str = "ERR counter keys accept only string and expiry commands";

/// How long a forward to the leader waits beyond the counter timeout of the leader.
const FORWARD_SLACK: Duration = Duration::from_millis(500);
/// How long the entry node waits between two looks for a leader.
const RETRY: Duration = Duration::from_millis(20);

fn error(text: &str) -> Reply {
    Reply::value(Value::error(text))
}

fn is_error(v: &Value, text: &str) -> bool {
    matches!(v, Value::Error(e) if e[..] == *text.as_bytes())
}

/// Take a command with counter keys. `None` for any other command.
pub fn intercept(node: &Arc<Node>, job: &Job, hop: &Hop) -> Option<Dispatch> {
    let counters = node.counters()?;
    let Some(args) = key_args(job.spec.route, &job.cmd) else {
        return (job.spec.name == COUNTER_FLUSH.name)
            .then(|| group_flush(node, counters, job, hop));
    };
    let (mut keys, mut strong) = (0, 0);
    for key in args {
        keys += 1;
        strong += usize::from(node.is_counter_key(key));
    }
    if strong == 0 {
        return None;
    }
    if strong != keys {
        return Some(Dispatch::Ready(error(MIXED)));
    }
    let Some(kind) = Kind::of(job.spec.name.as_bytes()) else {
        return Some(Dispatch::Ready(error(ONLY_STRINGS)));
    };
    if let Err(e) = check_sizes(node, kind, &job.cmd) {
        return Some(Dispatch::Ready(Reply::error(e)));
    }
    let (node, counters, cmd, hop) = (node.clone(), counters.clone(), job.cmd.clone(), hop.clone());
    let (route, db) = (job.spec.route, job.db);
    Some(Dispatch::Pending(Box::pin(async move {
        match Merge::of(route) {
            Some((merge, width)) => {
                fan_out(&node, &counters, db, kind, cmd, hop, merge, width).await
            }
            None => {
                let g = counters.group_of(&cmd[1]);
                run(&node, &counters, g, db, kind, cmd, hop).await
            }
        }
    })))
}

/// The flushes of a counter group. They name no key.
fn is_group_flush(kind: Kind) -> bool {
    matches!(kind, Kind::FlushAll | Kind::FlushOneDb)
}

/// A flush names no key, so its forward to a group leader names the group:
/// `DISCUS.COUNTERFLUSH <group> <flush>`.
fn group_flush_command(g: u16, cmd: &Command) -> Command {
    let mut wrapped = Vec::with_capacity(cmd.len() + 2);
    wrapped.push(Bytes::from_static(COUNTER_FLUSH.name.as_bytes()));
    wrapped.push(Bytes::from(g.to_string()));
    wrapped.extend(cmd.iter().cloned());
    wrapped
}

/// Run `DISCUS.COUNTERFLUSH <group> <flush>` that an entry node forwarded.
fn group_flush(node: &Arc<Node>, counters: &Arc<Counters>, job: &Job, hop: &Hop) -> Dispatch {
    let groups = counters.settings.groups;
    let g = parse_i64(&job.cmd[1])
        .ok()
        .and_then(|g| u16::try_from(g).ok())
        .filter(|g| *g < groups);
    let flush: Command = job.cmd[2..].to_vec();
    let kind = Kind::of(&flush[0]).filter(|k| is_group_flush(*k));
    let (Some(g), Some(kind)) = (g, kind) else {
        return Dispatch::Ready(error("ERR malformed DISCUS.COUNTERFLUSH"));
    };
    let (node, counters, hop, db) = (node.clone(), counters.clone(), hop.clone(), job.db);
    Dispatch::Pending(Box::pin(async move {
        run(&node, &counters, g, db, kind, flush, hop).await
    }))
}

fn check_sizes(node: &Node, kind: Kind, cmd: &Command) -> Result<(), CommandError> {
    let (keys, values) = match kind {
        Kind::MSet => return check_mset(node, cmd),
        Kind::Set | Kind::Append => (1, 1),
        _ => (1, 0),
    };
    let every_other = |from| cmd[from..].iter().step_by(2);
    every_other(1)
        .take(keys)
        .try_for_each(|k| check_key(node, k))?;
    every_other(2)
        .take(values)
        .try_for_each(|v| check_value(node, v))
}

/// The reply of a fan-out with a failed part, or `None` when every part succeeded. A client
/// retries a command after `NO_LEADER`, so one part that can have applied makes the whole reply
/// `IN_DOUBT`.
fn failed(parts: &[Reply]) -> Option<Reply> {
    if !parts.iter().any(|r| matches!(r.value, Value::Error(_))) {
        return None;
    }
    if parts.iter().any(|r| is_error(&r.value, IN_DOUBT)) {
        return Some(error(IN_DOUBT));
    }
    if parts.iter().all(|r| is_error(&r.value, NO_LEADER)) {
        return Some(error(NO_LEADER));
    }
    Some(Reply::try_again())
}

/// Split by group and merge like a normal multi-key command (spec section 7.3).
#[allow(clippy::too_many_arguments)]
async fn fan_out(
    node: &Arc<Node>,
    counters: &Arc<Counters>,
    db: u32,
    kind: Kind,
    cmd: Command,
    hop: Hop,
    merge: Merge,
    width: usize,
) -> Reply {
    let Some(mut groups) = split(&cmd, width, |key| Some(counters.group_of(key))) else {
        return Reply::try_again();
    };
    if groups.len() == 1 {
        return run(node, counters, groups[0].target, db, kind, cmd, hop).await;
    }
    let parts = join_all(groups.iter_mut().map(|g| {
        run(
            node,
            counters,
            g.target,
            db,
            kind,
            mem::take(&mut g.cmd),
            hop.clone(),
        )
    }))
    .await;
    if let Some(reply) = failed(&parts) {
        return reply;
    }
    let total = (cmd.len() - 1) / width;
    match merge.apply(total, &groups, parts.into_iter().map(|r| r.value)) {
        Some(value) => Reply::value(value),
        None => Reply::try_again(),
    }
}

/// Run one command of group `g` in database `db` on its leader. The entry node waits up to the
/// counter timeout for a leader and tries again after a refusal that applied nothing. A
/// forwarded copy tries once, so the entry node keeps the only retry loop.
pub async fn run(
    node: &Arc<Node>,
    counters: &Arc<Counters>,
    g: u16,
    db: u32,
    kind: Kind,
    cmd: Command,
    hop: Hop,
) -> Reply {
    let entry = hop.hops == 0;
    let read = kind.is_read();
    let deadline = Instant::now() + counters.settings.timeout;
    let group = counters.group(g);
    loop {
        let changed = group.changed.notified();
        match counters.leader(g) {
            Some(leader) if leader == node.id => match counters.execute(g, db, kind, &cmd).await {
                Ok(v) => return Reply::value(v),
                Err(Refusal::InDoubt) => return error(IN_DOUBT),
                Err(Refusal::NotLeader) => {}
            },
            Some(leader) if hop.hops < MAX_HOPS && !hop.path.contains(&leader) => {
                let timeout = Some(counters.settings.timeout + FORWARD_SLACK);
                // An older leader does not know `DISCUS.COUNTERFLUSH` and answers it with an
                // error, so the group form waits for the gate. Without it, the flush goes out as
                // before, and a leader of this build answers it without clearing anything.
                let wrapped;
                let sent = if is_group_flush(kind) && node.databases.multi_db() {
                    wrapped = group_flush_command(g, &cmd);
                    &wrapped
                } else {
                    &cmd
                };
                let sent = forward::send(
                    node,
                    leader,
                    db,
                    sent,
                    &hop,
                    ForwardMode::Route,
                    true,
                    timeout,
                )
                .await;
                match sent {
                    Ok(reply) if is_error(&reply.value, NO_LEADER) => group.distrust(leader),
                    Ok(reply) => return reply,
                    Err(e) if e.is_unsent() || read => group.distrust(leader),
                    // The next command of the group looks for another leader first.
                    Err(_) => {
                        group.distrust(leader);
                        return error(IN_DOUBT);
                    }
                }
            }
            _ => counters.ask_leader(g),
        }
        if !entry || Instant::now() >= deadline {
            return error(NO_LEADER);
        }
        let _ = tokio::time::timeout(RETRY, changed).await;
    }
}

/// `FLUSHALL` and `FLUSHDB` on the entry node: the cache broadcast, then every counter group
/// (spec section 12.1). A group without a leader or a quorum keeps its counters, and the reply
/// says how many groups were cleared. `FLUSHDB` clears the counters of database `db` only when
/// the gate is open. Before that, only database 0 holds keys, so it clears every database, as
/// before numbered databases.
pub fn broadcast(node: &Arc<Node>, spec: &'static CommandSpec, db: u32, cmd: Command) -> Dispatch {
    let node = node.clone();
    Dispatch::Pending(Box::pin(async move {
        let cache = crate::cluster::multi::flush(&node, spec, db, cmd.clone())
            .finish()
            .await;
        let Some(counters) = node.counters().cloned() else {
            return cache;
        };
        if matches!(cache.value, Value::Error(_)) {
            return cache;
        }
        let (kind, name) = if spec.name == "FLUSHDB" && node.databases.multi_db() {
            (Kind::FlushOneDb, FLUSH_ONE_DB)
        } else {
            (Kind::FlushAll, &b"FLUSHALL"[..])
        };
        let flush: Command = vec![Bytes::from_static(name)];
        let groups = counters.settings.groups;
        let parts = join_all(
            (0..groups).map(|g| run(&node, &counters, g, db, kind, flush.clone(), Hop::default())),
        )
        .await;
        let cleared = parts
            .iter()
            .filter(|r| !matches!(r.value, Value::Error(_)))
            .count();
        if cleared == usize::from(groups) {
            return cache;
        }
        error(&format!(
            "TRYAGAIN FLUSHALL cleared the cache and {cleared} of {groups} counter groups"
        ))
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok() -> Reply {
        Reply::value(Value::Integer(1))
    }

    #[test]
    fn a_part_in_doubt_makes_the_whole_reply_in_doubt() {
        let parts = [error(NO_LEADER), ok(), error(IN_DOUBT)];
        assert_eq!(failed(&parts), Some(error(IN_DOUBT)));
    }

    #[test]
    fn only_all_no_leader_parts_stay_no_leader() {
        let parts = [error(NO_LEADER), error(NO_LEADER)];
        assert_eq!(failed(&parts), Some(error(NO_LEADER)));
        let mixed = [error(NO_LEADER), ok()];
        assert_eq!(failed(&mixed), Some(Reply::try_again()));
        assert_eq!(
            failed(&[error("ERR other"), ok()]),
            Some(Reply::try_again())
        );
    }

    #[test]
    fn parts_without_an_error_are_not_failed() {
        assert_eq!(failed(&[ok(), ok()]), None);
    }
}
