//! Routing of counter commands to the group leader (spec section 10).
//!
//! Every command on a counter key comes here before the normal single-key path, so it never
//! reaches `cluster::local` and never enters a replication queue. A command whose keys all fall
//! in one group runs as one entry on its leader. Several groups fan out like a normal multi-key
//! command. A command that mixes counter keys and other keys is refused.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use discus_resp::{Command, Value};
use discus_wire::ForwardMode;
use futures::future::join_all;

use super::{state, Counters, Refusal};
use crate::cluster::multi::Merge;
use crate::cluster::route::{keys_of, Keys, Route};
use crate::cluster::{forward, Dispatch, Hop, Job, Reply, MAX_HOPS};
use crate::command::util::{check_key, check_value};
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

fn is_no_leader(v: &Value) -> bool {
    matches!(v, Value::Error(e) if e[..] == *NO_LEADER.as_bytes())
}

/// Take a command with counter keys. `None` for any other command.
pub fn intercept(node: &Arc<Node>, job: &Job, hop: &Hop) -> Option<Dispatch> {
    let counters = node.counters()?.clone();
    let Keys::Some(keys) = keys_of(job.spec.route, &job.cmd) else {
        return None;
    };
    let strong = keys.iter().filter(|k| node.is_counter_key(k)).count();
    if strong == 0 {
        return None;
    }
    if strong != keys.len() {
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
        match route {
            Route::MultiRead { sum } => {
                let merge = if sum { Merge::Sum } else { Merge::InOrder };
                fan_out(&node, &counters, cmd, hop, merge, 1).await
            }
            Route::MultiWrite { pairs } => {
                let (merge, width) = if pairs {
                    (Merge::AllOk, 2)
                } else {
                    (Merge::Sum, 1)
                };
                fan_out(&node, &counters, cmd, hop, merge, width).await
            }
            _ => {
                let g = counters.group_of(&cmd[1]);
                run(&node, &counters, g, cmd, hop).await
            }
        }
    })))
}

fn check_sizes(node: &Node, name: &str, cmd: &Command) -> Result<(), CommandError> {
    let (keys, values): (Vec<&Bytes>, Vec<&Bytes>) = match name {
        "MSET" => {
            if cmd.len().is_multiple_of(2) {
                return Err(CommandError::WrongArity("MSET".into()));
            }
            (
                cmd[1..].iter().step_by(2).collect(),
                cmd[2..].iter().step_by(2).collect(),
            )
        }
        "SET" | "APPEND" => (vec![&cmd[1]], vec![&cmd[2]]),
        _ => (vec![&cmd[1]], vec![]),
    };
    keys.into_iter().try_for_each(|k| check_key(node, k))?;
    values.into_iter().try_for_each(|v| check_value(node, v))
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
    if width == 2 && cmd.len().is_multiple_of(2) {
        return Reply::error(CommandError::WrongArity("MSET".into()));
    }
    let mut groups: Vec<(u16, Command, Vec<usize>)> = Vec::new();
    for (pos, item) in cmd[1..].chunks(width).enumerate() {
        let g = counters.group_of(&item[0]);
        let i = match groups.iter().position(|(x, _, _)| *x == g) {
            Some(i) => i,
            None => {
                groups.push((g, vec![cmd[0].clone()], Vec::new()));
                groups.len() - 1
            }
        };
        groups[i].1.extend(item.iter().cloned());
        groups[i].2.push(pos);
    }
    if groups.len() == 1 {
        return run(node, counters, groups[0].0, cmd, hop).await;
    }
    let parts = join_all(
        groups
            .iter()
            .map(|(g, sub, _)| run(node, counters, *g, sub.clone(), hop.clone())),
    )
    .await;
    if parts.iter().any(|r| matches!(r.value, Value::Error(_))) {
        return Reply::try_again();
    }
    let total = (cmd.len() - 1) / width;
    let value = match merge {
        Merge::InOrder => {
            let mut out = vec![Value::Null; total];
            for ((_, _, positions), r) in groups.iter().zip(parts) {
                let Value::Array(values) = r.value else {
                    return Reply::try_again();
                };
                for (pos, v) in positions.iter().zip(values) {
                    out[*pos] = v;
                }
            }
            Value::Array(out)
        }
        Merge::Sum => Value::Integer(
            parts
                .iter()
                .map(|r| match r.value {
                    Value::Integer(n) => n,
                    _ => 0,
                })
                .sum(),
        ),
        Merge::AllOk => Value::ok(),
    };
    Reply::value(value)
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
    let group = counters.group(g).clone();
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
                    forward::send_for(node, leader, &cmd, &hop, ForwardMode::Route, true, timeout)
                        .await;
                match sent {
                    Ok(reply) if is_no_leader(&reply.value) => counters.distrust(g, leader),
                    Ok(reply) => return reply,
                    Err(e) if e.is_unsent() || read => counters.distrust(g, leader),
                    // The next command of the group looks for another leader first.
                    Err(_) => {
                        counters.distrust(g, leader);
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
