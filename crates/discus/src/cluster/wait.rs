//! `WAIT` and `WAITAOF` (spec "The WAIT command").
//!
//! `WAIT` counts in rounds. Each round offers every key of the connection's record, at its
//! recorded version, to the current owners of the key. The reply is the lowest count over the
//! keys. A count is never too high: an owner that does not answer holds none of the round's keys.
//! A key that no current owner holds counts 0, as a write lost with its primary does. It leaves
//! the record only when the node that ran the write answers, with the same boot id and the epoch
//! of the record's mark, that it no longer holds the key. Then that node removed the key by
//! expiry, delete, eviction, or flush, and did not lose it.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use discus_placement::{EpochId, NodeId, Owners, Version};
use discus_resp::{Command, Value};
use discus_store::DbKey;
use discus_wire::ForwardMode;
use futures::future::join_all;
use tokio::time::Instant;

use super::record::Server;
use super::{forward, Dispatch, Hop, Job, Reply, Writes};
use crate::command::internal::DRAINED;
use crate::command::util::parse_i64;
use crate::command::{CommandError, CommandSpec};
use crate::node::Node;

const FIRST_BACKOFF: Duration = Duration::from_millis(1);
const MAX_BACKOFF: Duration = Duration::from_millis(100);

/// Answer `WAIT` or `WAITAOF` on the entry node. `writes` is `None` without secondaries.
pub fn run(node: &Arc<Node>, job: Job, writes: Option<&Writes>) -> Dispatch {
    let dispatch = match job.spec.name {
        "WAITAOF" => waitaof(&job.cmd),
        _ => wait(node, &job.cmd, writes),
    };
    dispatch.unwrap_or_else(|e| Dispatch::Ready(Reply::error(e)))
}

/// The reply of a wait whose client stopped writing. Valkey frees such a client, so nobody reads
/// it, and a count of 0 is never too high.
pub fn closed(spec: &CommandSpec) -> Reply {
    match spec.name {
        "WAITAOF" => aof_zeros(),
        _ => Reply::value(Value::Integer(0)),
    }
}

fn error(text: &str) -> CommandError {
    CommandError::Custom(format!("ERR {text}"))
}

/// The end of a wait of `arg` milliseconds, with the errors of Valkey. `None` waits without an
/// end: a timeout of 0, or one too far away for the clock.
fn deadline(arg: &[u8]) -> Result<Option<Instant>, CommandError> {
    let ms = parse_i64(arg).map_err(|_| error("timeout is not an integer or out of range"))?;
    if ms < 0 {
        return Err(error("timeout is negative"));
    }
    if ms == 0 {
        return Ok(None);
    }
    // Valkey adds the timeout to the Unix time in milliseconds and refuses an overflow.
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64);
    if ms > i64::MAX - now {
        return Err(error("timeout is out of range"));
    }
    Ok(Instant::now().checked_add(Duration::from_millis(ms as u64)))
}

/// The number of secondaries that any key has: `min(--replication-factor, NORMAL members) - 1`.
fn secondaries(node: &Node) -> i64 {
    let normal = discus_membership::normal_count(node.epochs().current());
    node.config.replication_factor.min(normal).saturating_sub(1) as i64
}

fn wait(
    node: &Arc<Node>,
    cmd: &Command,
    writes: Option<&Writes>,
) -> Result<Dispatch, CommandError> {
    let need = parse_i64(&cmd[1])?;
    let deadline = deadline(&cmd[2])?;
    let idle = writes.is_none_or(|w| {
        let w = w.lock();
        w.record.is_empty() && !w.record.overflowed()
    });
    if idle {
        let count = secondaries(node);
        if count >= need {
            return Ok(Dispatch::Ready(Reply::value(Value::Integer(count))));
        }
    }
    let (node, writes) = (node.clone(), writes.cloned());
    Ok(Dispatch::Pending(Box::pin(async move {
        let count = count_until(&node, writes.as_ref(), need, deadline).await;
        Reply::value(Value::Integer(count))
    })))
}

/// Count in rounds until the count reaches `need` or `deadline` passes. A round that the deadline
/// cuts short leaves the count of the round before.
async fn count_until(
    node: &Arc<Node>,
    writes: Option<&Writes>,
    need: i64,
    deadline: Option<Instant>,
) -> i64 {
    let mut count = 0;
    let mut backoff = FIRST_BACKOFF;
    loop {
        let next = round(node, writes);
        count = match deadline {
            None => next.await,
            Some(d) => match tokio::time::timeout_at(d, next).await {
                Ok(n) => n,
                Err(_) => return count,
            },
        };
        if count >= need {
            return count;
        }
        let pause = match deadline {
            None => backoff,
            Some(d) => {
                let now = Instant::now();
                if now >= d {
                    return count;
                }
                backoff.min(d - now)
            }
        };
        tokio::time::sleep(pause).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// One count over the record of `writes`. The record lock is not held across an await: the round
/// copies the entries, and removes the keys that leave after the answers came.
async fn round(node: &Arc<Node>, writes: Option<&Writes>) -> i64 {
    let Some(writes) = writes else {
        return secondaries(node);
    };
    let (entries, overflow) = {
        let w = writes.lock();
        let entries: Vec<(DbKey, Version, Option<Server>)> = w
            .record
            .served()
            .map(|(db, k, v, s)| (DbKey::new(db, k.clone()), v, s))
            .collect();
        (entries, w.record.overflowed())
    };
    if overflow {
        return drained(node, writes).await;
    }
    if entries.is_empty() {
        return secondaries(node);
    }
    let Some(replicator) = node.replicator() else {
        return 0;
    };
    let owners: Vec<Owners> = entries.iter().map(|(k, ..)| node.owners(&k.key)).collect();
    let mut offers: HashMap<NodeId, Vec<(DbKey, Version)>> = HashMap::new();
    for ((k, v, _), owners) in entries.iter().zip(&owners) {
        for o in owners.iter().filter(|o| **o != node.id) {
            offers.entry(*o).or_default().push((k.clone(), *v));
        }
    }
    let answers: HashMap<NodeId, Answer> =
        join_all(offers.into_iter().map(|(peer, offers)| async move {
            let ack = replicator.missing(node, peer, offers).await.ok()?;
            let answer = Answer {
                missing: ack.want.into_iter().collect(),
                epoch: ack.epoch,
                boot: ack.boot,
            };
            Some((peer, answer))
        }))
        .await
        .into_iter()
        .flatten()
        .collect();
    // `None`: the owner did not answer.
    let holds = |owner: NodeId, k: &DbKey, v: Version| -> Option<bool> {
        if owner == node.id {
            return Some(
                node.store
                    .stored_version(k.db, &k.key)
                    .is_some_and(|s| s >= v),
            );
        }
        answers.get(&owner).map(|a| !a.missing.contains(k))
    };
    // Read after the answers, so an epoch change during the round counts.
    let (here, mark) = (node.epoch_id(), writes.lock().record.mark_epoch());
    let answered_by = |owner: NodeId| -> Option<(EpochId, u64)> {
        if owner == node.id {
            return Some((here, node.boot));
        }
        answers.get(&owner).map(|a| (a.epoch, a.boot))
    };
    let most = node.config.replication_factor.saturating_sub(1);
    let mut lowest: Option<i64> = None;
    let mut leave = Vec::new();
    for ((k, v, served), owners) in entries.into_iter().zip(owners) {
        if owners.is_empty() {
            lowest = Some(0);
            continue;
        }
        let held: Vec<Option<bool>> = owners.iter().map(|o| holds(*o, &k, v)).collect();
        let holders = held.iter().filter(|h| **h == Some(true)).count();
        let answered = held.iter().all(Option::is_some);
        if answered && holders == 0 && removed(served, &owners, answered_by, mark, here) {
            leave.push((k, v));
            continue;
        }
        // One holder is subtracted for the primary, also when the primary is the owner that
        // lacks the key, as a new primary does before its handoff ends.
        let count = holders.saturating_sub(1).min(most) as i64;
        lowest = Some(lowest.map_or(count, |l| l.min(count)));
        if answered && holders == owners.len() {
            leave.push((k, v));
        }
    }
    if !leave.is_empty() {
        let mut w = writes.lock();
        for (k, v) in leave {
            w.record.remove_at(k.db, &k.key, v);
        }
    }
    lowest.unwrap_or_else(|| secondaries(node))
}

/// The answer of one owner in a round.
struct Answer {
    missing: HashSet<DbKey>,
    epoch: EpochId,
    boot: u64,
}

/// A key that no current owner holds was removed, not lost, only with proof from the node that
/// ran the write: it is a current owner and answered this round with the boot id of the write,
/// in the epoch of the mark, and this node is still in that epoch too. A restart with the same
/// node id draws a new boot id, and a store that an epoch change discarded comes with a new
/// epoch. `answered_by` gives the epoch and the boot id of an owner's answer.
fn removed(
    served: Option<Server>,
    owners: &[NodeId],
    answered_by: impl Fn(NodeId) -> Option<(EpochId, u64)>,
    mark: EpochId,
    here: EpochId,
) -> bool {
    let Some((n, boot)) = served else {
        return false;
    };
    here == mark && owners.contains(&n) && answered_by(n) == Some((mark, boot))
}

/// The count of an overflowed record: the number of secondaries when this node and every other
/// member delivered every write, else 0. The full count ends the overflow. A member that left
/// since the record was last empty can have taken writes that no other node holds, so the count
/// is then 0.
async fn drained(node: &Arc<Node>, writes: &Writes) -> i64 {
    if writes.lock().record.departed() || !node.replicator().is_some_and(|r| r.drained()) {
        return 0;
    }
    let others: Vec<NodeId> = node
        .epochs()
        .current()
        .members()
        .iter()
        .map(|m| m.id)
        .filter(|id| *id != node.id)
        .collect();
    let cmd = vec![Bytes::from_static(DRAINED.name.as_bytes())];
    let timeout = node.live_settings.get().replication_timeout;
    let hop = Hop::default();
    let answers = join_all(others.iter().map(|id| {
        forward::send(
            node,
            *id,
            0,
            &cmd,
            &hop,
            ForwardMode::Local,
            false,
            Some(timeout),
        )
    }))
    .await;
    let all = answers.iter().all(|r| {
        matches!(
            r,
            Ok(Reply {
                value: Value::Integer(1),
                ..
            })
        )
    });
    let mut w = writes.lock();
    if !all || w.record.departed() {
        return 0;
    }
    w.record.clear();
    drop(w);
    secondaries(node)
}

fn aof_zeros() -> Reply {
    Reply::value(Value::Array(vec![Value::Integer(0), Value::Integer(0)]))
}

/// `WAITAOF numlocal numreplicas timeout` on a node without an append-only file: no secondary
/// ever reports an fsync, so a wait for one or more lasts until the timeout.
fn waitaof(cmd: &Command) -> Result<Dispatch, CommandError> {
    let numlocal = parse_i64(&cmd[1])?;
    if !(0..=1).contains(&numlocal) {
        return Err(error("value is out of range, value must between 0 and 1"));
    }
    let numreplicas = parse_i64(&cmd[2])
        .ok()
        .filter(|n| *n >= 0)
        .ok_or_else(|| error("value is out of range, must be positive"))?;
    let deadline = deadline(&cmd[3])?;
    if numlocal > 0 {
        return Err(error(
            "WAITAOF cannot be used when numlocal is set but appendonly is disabled.",
        ));
    }
    if numreplicas == 0 {
        return Ok(Dispatch::Ready(aof_zeros()));
    }
    Ok(Dispatch::Pending(Box::pin(async move {
        match deadline {
            Some(d) => tokio::time::sleep_until(d).await,
            None => std::future::pending().await,
        }
        aof_zeros()
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(r: Result<Option<Instant>, CommandError>) -> String {
        r.map(|_| String::new()).unwrap_or_else(|e| e.text())
    }

    #[test]
    fn only_the_node_that_ran_the_write_proves_a_removal() {
        let (p, q) = (NodeId::from_u128(1), NodeId::from_u128(2));
        let e = |g| EpochId {
            generation: g,
            origin: p,
        };
        let owners = [p, q];
        let answers = |boot| move |n: NodeId| (n == p).then_some((e(1), boot));
        assert!(removed(Some((p, 7)), &owners, answers(7), e(1), e(1)));
        // The node restarted with the same id and an empty store.
        assert!(!removed(Some((p, 7)), &owners, answers(8), e(1), e(1)));
        assert!(!removed(None, &owners, answers(7), e(1), e(1)));
        assert!(!removed(Some((p, 7)), &[q], answers(7), e(1), e(1)));
        // The answer or this node is in another epoch than the mark.
        assert!(!removed(Some((p, 7)), &owners, answers(7), e(2), e(2)));
        assert!(!removed(Some((p, 7)), &owners, answers(7), e(1), e(2)));
        assert!(!removed(Some((q, 7)), &owners, answers(7), e(1), e(1)));
    }

    #[test]
    fn a_timeout_has_the_errors_of_valkey() {
        assert_eq!(deadline(b"0").unwrap(), None);
        assert!(deadline(b"300").unwrap().is_some());
        assert_eq!(text(deadline(b"-5")), "ERR timeout is negative");
        for bad in [&b"x"[..], b"-0", b"+5", b"1.5", b"99999999999999999999"] {
            assert_eq!(
                text(deadline(bad)),
                "ERR timeout is not an integer or out of range"
            );
        }
        assert_eq!(
            text(deadline(b"9223372036854775807")),
            "ERR timeout is out of range"
        );
    }
}
