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

use super::{state, Counters, Refusal};
use crate::cluster::multi::{split, Merge};
use crate::cluster::route::key_args;
use crate::cluster::{forward, Dispatch, Hop, Job, Reply, MAX_HOPS};
use crate::command::util::{check_key, check_mset, check_value};
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
    let counters = node.counters()?.clone();
    let (mut keys, mut strong) = (0, 0);
    for key in key_args(job.spec.route, &job.cmd)? {
        keys += 1;
        strong += usize::from(node.is_counter_key(key));
    }
    if strong == 0 {
        return None;
    }
    if strong != keys {
        return Some(Dispatch::Ready(error(MIXED)));
    }
    if !state::accepts(job.spec.name) {
        return Some(Dispatch::Ready(error(ONLY_STRINGS)));
    }
    if let Err(e) = check_sizes(node, job.spec.name, &job.cmd) {
        return Some(Dispatch::Ready(Reply::error(e)));
    }
    let (node, cmd, hop, route) = (node.clone(), job.cmd.clone(), hop.clone(), job.spec.route);
    Some(Dispatch::Pending(Box::pin(async move {
        match Merge::of(route) {
            Some((merge, width)) => fan_out(&node, &counters, cmd, hop, merge, width).await,
            None => {
                let g = counters.group_of(&cmd[1]);
                run(&node, &counters, g, cmd, hop).await
            }
        }
    })))
}

fn check_sizes(node: &Node, name: &str, cmd: &Command) -> Result<(), CommandError> {
    let (keys, values) = match name {
        "MSET" => return check_mset(node, cmd),
        "SET" | "APPEND" => (1, 1),
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
async fn fan_out(
    node: &Arc<Node>,
    counters: &Arc<Counters>,
    cmd: Command,
    hop: Hop,
    merge: Merge,
    width: usize,
) -> Reply {
    let Some(mut groups) = split(&cmd, width, |key| Some(counters.group_of(key))) else {
        return Reply::try_again();
    };
    if groups.len() == 1 {
        return run(node, counters, groups[0].target, cmd, hop).await;
    }
    let parts = join_all(
        groups
            .iter_mut()
            .map(|g| run(node, counters, g.target, mem::take(&mut g.cmd), hop.clone())),
    )
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

/// Run one command of group `g` on its leader. The entry node waits up to the counter timeout
/// for a leader and tries again after a refusal that applied nothing. A forwarded copy tries
/// once, so the entry node keeps the only retry loop.
pub async fn run(
    node: &Arc<Node>,
    counters: &Arc<Counters>,
    g: u16,
    cmd: Command,
    hop: Hop,
) -> Reply {
    let entry = hop.hops == 0;
    let read = state::is_read(&cmd);
    let deadline = Instant::now() + counters.settings.timeout;
    let group = counters.group(g);
    loop {
        let changed = group.changed.notified();
        match counters.leader(g) {
            Some(leader) if leader == node.id => match counters.execute(g, &cmd).await {
                Ok(v) => return Reply::value(v),
                Err(Refusal::InDoubt) => return error(IN_DOUBT),
                Err(Refusal::NotLeader) => {}
            },
            Some(leader) if hop.hops < MAX_HOPS && !hop.path.contains(&leader) => {
                let timeout = Some(counters.settings.timeout + FORWARD_SLACK);
                let sent =
                    forward::send(node, leader, &cmd, &hop, ForwardMode::Route, true, timeout)
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
/// says how many groups were cleared.
pub fn broadcast(node: &Arc<Node>, spec: &'static CommandSpec, cmd: Command) -> Dispatch {
    let node = node.clone();
    Dispatch::Pending(Box::pin(async move {
        let cache = crate::cluster::multi::flush(&node, spec, cmd.clone())
            .finish()
            .await;
        let Some(counters) = node.counters().cloned() else {
            return cache;
        };
        if matches!(cache.value, Value::Error(_)) {
            return cache;
        }
        let flush: Command = vec![Bytes::from_static(b"FLUSHALL")];
        let groups = counters.settings.groups;
        let parts =
            join_all((0..groups).map(|g| run(&node, &counters, g, flush.clone(), Hop::default())))
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
