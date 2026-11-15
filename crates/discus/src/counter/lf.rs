//! Leader-follower mode (spec section 10.1).
//!
//! The leader applies each write, numbers it, and hands it to a sender task that streams the ops
//! to the follower in order. The writer waits up to the follower timeout for the ack of its op.
//! A timeout marks the follower lagging, and the sender then sends a fresh snapshot. A new
//! follower always starts with a snapshot.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use discus_placement::NodeId;
use discus_resp::{Command, Value};
use discus_wire::{
    encode_command, CounterAck, CounterOp, CounterSync, Message, Regime, SnapshotChunk,
};
use parking_lot::Mutex;
use tokio::sync::{watch, Notify};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::budget::{Budget, Tracked};
use super::state::{parse_command, CounterState, Kind, Limits};
use super::{Counters, Refusal};

/// Ops in one `COUNTER_SYNC`.
const SYNC_OPS: usize = 512;
/// Command bytes in one `COUNTER_SYNC`, so a batch of large values stays far below the frame
/// limit.
const SYNC_BYTES: usize = 4 << 20;
/// The part size of a snapshot for a follower with `COUNTER_CHUNKS`. Without parts, a state
/// above the frame limit can never reach the follower.
const SNAPSHOT_CHUNK: usize = 4 << 20;
/// How long the sender waits for one sync to be acknowledged.
const SYNC_TIMEOUT: Duration = Duration::from_secs(2);
/// The pause after a failed sync.
const SYNC_RETRY: Duration = Duration::from_millis(100);

struct Pending {
    seq: u64,
    stamp_ms: u64,
    db: u32,
    command: Bytes,
}

struct Core {
    state: Tracked,
    /// The largest stamp so far. Stamps never run backwards within one regime.
    last_stamp: u64,
    /// The number of the last op.
    seq: u64,
    /// Ops that the follower has not acknowledged.
    pending: VecDeque<Pending>,
    /// The state left this core (see [`LfLeader::seal`]). `execute` tests it under the same
    /// lock, so no command applies after the copy.
    sealed: bool,
}

/// The leader of one group in leader-follower mode.
pub struct LfLeader {
    pub regime: Regime,
    core: Mutex<Core>,
    /// The end of the lease by the monotonic clock of this node. `None` when no lease fences the
    /// group, as with the provider `none`.
    lease_until: Mutex<Option<Instant>>,
    margin: Duration,
    follower: Mutex<Option<NodeId>>,
    lagging: AtomicBool,
    acked: watch::Sender<u64>,
    wake: Notify,
    stop: CancellationToken,
    /// The newer regime of a follower that refused a sync of this one.
    superseded: Mutex<Option<Regime>>,
    pub since: Instant,
}

impl LfLeader {
    pub fn new(
        regime: Regime,
        state: CounterState,
        budget: &Arc<Budget>,
        lease_until: Option<Instant>,
        margin: Duration,
    ) -> Arc<LfLeader> {
        Arc::new(LfLeader {
            regime,
            core: Mutex::new(Core {
                last_stamp: state.clock_ms(),
                state: Tracked::new(state, budget.clone()),
                seq: 0,
                pending: VecDeque::new(),
                sealed: false,
            }),
            lease_until: Mutex::new(lease_until),
            margin,
            follower: Mutex::new(None),
            lagging: AtomicBool::new(true),
            acked: watch::channel(0).0,
            wake: Notify::new(),
            stop: CancellationToken::new(),
            superseded: Mutex::new(None),
            since: Instant::now(),
        })
    }

    /// The leader serves while its lease is valid by its own clock, minus the margin (spec
    /// section 10.1).
    pub fn serving(&self) -> bool {
        if self.stop.is_cancelled() {
            return false;
        }
        match *self.lease_until.lock() {
            None => true,
            Some(until) => Instant::now() + self.margin < until,
        }
    }

    pub fn fenced(&self) -> bool {
        self.lease_until.lock().is_some()
    }

    pub fn lease_until(&self) -> Option<Instant> {
        *self.lease_until.lock()
    }

    /// The lease ended without a renewal.
    pub fn expired(&self) -> bool {
        self.lease_until()
            .is_some_and(|until| Instant::now() >= until)
    }

    pub fn renewed(&self, until: Instant) {
        let mut lease = self.lease_until.lock();
        if lease.is_some_and(|old| until > old) {
            *lease = Some(until);
        }
    }

    pub fn stop(&self) {
        self.stop.cancel();
    }

    pub fn superseded(&self) -> Option<Regime> {
        *self.superseded.lock()
    }

    pub fn state(&self) -> CounterState {
        self.core.lock().state.get().clone()
    }

    /// Stop the leader and return its final state. The seal and the copy take one lock, so the
    /// state holds every write that this leader acknowledged.
    pub fn seal(&self) -> CounterState {
        self.stop();
        let mut core = self.core.lock();
        core.sealed = true;
        core.state.get().clone()
    }

    pub fn holds_expired(&self, now_ms: u64) -> bool {
        self.core.lock().state.get().expired_keys(now_ms)
    }

    pub fn databases(&self) -> Vec<u32> {
        self.core.lock().state.get().databases()
    }

    pub fn follower(&self) -> Option<NodeId> {
        *self.follower.lock()
    }

    /// Choose the follower. A new follower starts with a snapshot.
    pub fn set_follower(&self, follower: Option<NodeId>) {
        let mut current = self.follower.lock();
        if *current != follower {
            *current = follower;
            self.lagging.store(true, Ordering::Release);
            self.wake.notify_one();
        }
    }

    /// Run one command in database `db`. A read runs at `now_ms` without a stamp. A write takes
    /// the next stamp, and with a follower it waits up to `wait` for the ack (spec section 10.1).
    pub async fn execute(
        &self,
        kind: Kind,
        db: u32,
        cmd: &Command,
        now_ms: u64,
        limits: &Limits,
        wait: Duration,
    ) -> Result<Value, Refusal> {
        if !self.serving() {
            return Err(Refusal::NotLeader);
        }
        if kind.is_read() {
            let core = self.core.lock();
            if core.sealed {
                return Err(Refusal::NotLeader);
            }
            return Ok(core.state.get().read(now_ms, db, kind, cmd));
        }
        let follower = self.follower().is_some();
        let raw = follower.then(|| encode_command(cmd));
        let (reply, seq) = {
            let mut core = self.core.lock();
            if core.sealed {
                return Err(Refusal::NotLeader);
            }
            let stamp = now_ms.max(core.last_stamp);
            core.last_stamp = stamp;
            core.seq += 1;
            let seq = core.seq;
            let reply = core.state.apply(stamp, db, cmd, limits);
            if let Some(command) = raw {
                core.pending.push_back(Pending {
                    seq,
                    stamp_ms: stamp,
                    db,
                    command,
                });
            }
            (reply, seq)
        };
        if follower {
            self.wake.notify_one();
            let mut acked = self.acked.subscribe();
            let ok = tokio::time::timeout(wait, acked.wait_for(|a| *a >= seq))
                .instrument(crate::telemetry::counter_sync_span())
                .await;
            if !matches!(ok, Ok(Ok(_))) {
                self.lagging.store(true, Ordering::Release);
            }
        }
        Ok(reply)
    }

    fn ack(&self, seq: u64) {
        self.acked.send_if_modified(|a| {
            let newer = seq > *a;
            if newer {
                *a = seq;
            }
            newer
        });
        self.core.lock().pending.retain(|p| p.seq > seq);
    }

    /// Send the snapshot `data` of op `seq` in parts of [`SNAPSHOT_CHUNK`] bytes. Returns the
    /// reply to the last part, or the first reply that is not a plain ack.
    async fn send_chunks(
        &self,
        counters: &Counters,
        group: u16,
        follower: NodeId,
        seq: u64,
        data: Bytes,
    ) -> Result<Message, String> {
        let mut offset = 0;
        loop {
            let end = (offset + SNAPSHOT_CHUNK).min(data.len());
            let last = end == data.len();
            let sync = CounterSync {
                group,
                regime: self.regime,
                seq,
                ops: Vec::new(),
                snapshot: Some(data.slice(offset..end)),
                chunk: Some(SnapshotChunk {
                    offset: offset as u64,
                    last,
                }),
            };
            let reply = counters
                .send(follower, Message::CounterSync(sync), SYNC_TIMEOUT)
                .await;
            let acked = matches!(&reply, Ok(Message::CounterAck(a))
                if a.regime == self.regime && !a.need_snapshot);
            if last || !acked || self.stop.is_cancelled() {
                return reply;
            }
            offset = end;
        }
    }

    /// Stream ops and heartbeats to the follower until the leader stops. A heartbeat goes out
    /// every `renew` while the leader serves, so the follower knows when it went silent.
    pub async fn sender(self: Arc<Self>, counters: Arc<Counters>, group: u16, renew: Duration) {
        loop {
            let busy = self.follower().is_some()
                && (!self.core.lock().pending.is_empty() || self.lagging.load(Ordering::Acquire));
            if !busy {
                tokio::select! {
                    _ = self.stop.cancelled() => return,
                    _ = self.wake.notified() => {}
                    _ = tokio::time::sleep(renew) => {}
                }
            }
            if self.stop.is_cancelled() {
                return;
            }
            let Some(follower) = self.follower() else {
                self.core.lock().pending.clear();
                continue;
            };
            let (reply, snapshot, sent_seq) = if self.lagging.swap(false, Ordering::AcqRel) {
                let (seq, data) = {
                    let mut core = self.core.lock();
                    core.pending.clear();
                    (core.seq, Bytes::from(core.state.get().encode()))
                };
                let chunked = data.len() > SNAPSHOT_CHUNK
                    && counters.peer_caps(follower) & discus_wire::caps::COUNTER_CHUNKS != 0;
                let reply = if chunked {
                    self.send_chunks(&counters, group, follower, seq, data)
                        .await
                } else {
                    let sync = CounterSync {
                        group,
                        regime: self.regime,
                        seq,
                        ops: Vec::new(),
                        snapshot: Some(data),
                        chunk: None,
                    };
                    counters
                        .send(follower, Message::CounterSync(sync), SYNC_TIMEOUT)
                        .await
                };
                (reply, true, seq)
            } else {
                let sync = {
                    let core = self.core.lock();
                    // A batch stops at `SYNC_OPS` ops or `SYNC_BYTES` command bytes, and holds at
                    // least one op.
                    let mut bytes = 0;
                    let ops: Vec<CounterOp> = core
                        .pending
                        .iter()
                        .take(SYNC_OPS)
                        .enumerate()
                        .take_while(|(i, p)| {
                            bytes += p.command.len();
                            *i == 0 || bytes <= SYNC_BYTES
                        })
                        .map(|(_, p)| CounterOp {
                            stamp_ms: p.stamp_ms,
                            db: p.db,
                            command: p.command.clone(),
                        })
                        .collect();
                    if ops.is_empty() && !self.serving() {
                        continue;
                    }
                    CounterSync {
                        group,
                        regime: self.regime,
                        seq: core.pending.front().map_or(core.seq + 1, |p| p.seq),
                        ops,
                        snapshot: None,
                        chunk: None,
                    }
                };
                let seq = sync.seq;
                let reply = counters
                    .send(follower, Message::CounterSync(sync), SYNC_TIMEOUT)
                    .await;
                (reply, false, seq)
            };
            match reply {
                Ok(Message::CounterAck(a)) if a.regime == self.regime && !a.need_snapshot => {
                    self.ack(if snapshot { sent_seq } else { a.seq });
                }
                // A retry gets the same refusal forever. The controller leads on in a newer
                // regime (see `Group::lead`).
                Ok(Message::CounterAck(a)) if a.regime > self.regime => {
                    *self.superseded.lock() = Some(a.regime);
                    return;
                }
                _ => {
                    self.lagging.store(true, Ordering::Release);
                    let retry = tokio::time::sleep(SYNC_RETRY);
                    if self.stop.run_until_cancelled(retry).await.is_none() {
                        return;
                    }
                }
            }
        }
    }
}

struct FollowerCore {
    state: Tracked,
    seq: u64,
    synced: bool,
    /// The parts of a snapshot so far: its op number and its bytes.
    partial: Option<(u64, Vec<u8>)>,
}

/// A node that holds the state of a group without leading it: the follower of a leader-follower
/// leader, or a former leader.
pub struct Follower {
    pub regime: Regime,
    pub leader: Option<NodeId>,
    core: Mutex<FollowerCore>,
    contact: Mutex<Instant>,
    pub since: Instant,
}

impl Follower {
    pub fn new(
        regime: Regime,
        leader: Option<NodeId>,
        state: CounterState,
        budget: &Arc<Budget>,
    ) -> Follower {
        Follower {
            regime,
            leader,
            core: Mutex::new(FollowerCore {
                state: Tracked::new(state, budget.clone()),
                seq: 0,
                synced: false,
                partial: None,
            }),
            contact: Mutex::new(Instant::now()),
            since: Instant::now(),
        }
    }

    /// The follower holds a snapshot of its leader and every op after it.
    pub fn synced(&self) -> bool {
        self.core.lock().synced
    }

    /// The time since the last sync from the leader.
    pub fn silence(&self) -> Duration {
        self.contact.lock().elapsed()
    }

    pub fn state(&self) -> CounterState {
        self.core.lock().state.get().clone()
    }

    /// Apply one sync and answer it.
    pub fn on_sync(&self, s: &CounterSync, limits: &Limits) -> CounterAck {
        *self.contact.lock() = Instant::now();
        let mut core = self.core.lock();
        let mut need_snapshot = false;
        let mut whole = None;
        if let (Some(part), Some(chunk)) = (&s.snapshot, s.chunk) {
            let mut buf = match core.partial.take() {
                _ if chunk.offset == 0 => Vec::new(),
                Some((seq, buf)) if seq == s.seq && buf.len() as u64 == chunk.offset => buf,
                // A part is missing. The leader starts the snapshot again.
                _ => {
                    core.synced = false;
                    return CounterAck {
                        group: s.group,
                        regime: self.regime,
                        seq: core.seq,
                        need_snapshot: true,
                    };
                }
            };
            buf.extend_from_slice(part);
            if !chunk.last {
                core.partial = Some((s.seq, buf));
                return CounterAck {
                    group: s.group,
                    regime: self.regime,
                    seq: core.seq,
                    need_snapshot: false,
                };
            }
            whole = Some(buf);
        }
        let snapshot = whole.as_deref().or(s.snapshot.as_deref());
        if let Some(snapshot) = snapshot {
            match CounterState::decode(snapshot) {
                Some(state) => {
                    core.state.replace(state);
                    core.seq = s.seq;
                    core.synced = true;
                }
                None => need_snapshot = true,
            }
        } else if !s.ops.is_empty() {
            let last = s.seq + s.ops.len() as u64 - 1;
            if core.synced && s.seq == core.seq + 1 {
                for op in &s.ops {
                    match parse_command(&op.command) {
                        Some(cmd) => {
                            core.state.apply(op.stamp_ms, op.db, &cmd, limits);
                        }
                        None => {
                            need_snapshot = true;
                            break;
                        }
                    }
                }
                if !need_snapshot {
                    core.seq = last;
                }
            } else if !(core.synced && last <= core.seq) {
                need_snapshot = true;
            }
        } else {
            need_snapshot = !core.synced;
        }
        if need_snapshot {
            core.synced = false;
        }
        CounterAck {
            group: s.group,
            regime: self.regime,
            seq: core.seq,
            need_snapshot,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::counter::cmd;

    const LIMITS: Limits = Limits {
        max_value_bytes: 1 << 20,
    };

    fn part(seq: u64, data: &[u8], offset: usize, end: usize, last: bool) -> CounterSync {
        CounterSync {
            group: 0,
            regime: Regime::default(),
            seq,
            ops: Vec::new(),
            snapshot: Some(Bytes::copy_from_slice(&data[offset..end])),
            chunk: Some(SnapshotChunk {
                offset: offset as u64,
                last,
            }),
        }
    }

    #[test]
    fn a_follower_joins_the_parts_of_a_snapshot() {
        let budget = Arc::new(Budget::new(u64::MAX, None));
        let mut state = CounterState::new();
        for i in 0..50 {
            state.apply(1, 0, &cmd(&["SET", &format!("cnt:{i}"), "value"]), &LIMITS);
        }
        let data = state.encode();
        let f = Follower::new(Regime::default(), None, CounterState::new(), &budget);
        let third = data.len() / 3;
        for (from, to, last) in [(0, third, false), (third, 2 * third, false)] {
            let ack = f.on_sync(&part(9, &data, from, to, last), &LIMITS);
            assert!(!ack.need_snapshot);
            assert!(!f.synced());
        }
        let ack = f.on_sync(&part(9, &data, 2 * third, data.len(), true), &LIMITS);
        assert!(!ack.need_snapshot);
        assert_eq!(ack.seq, 9);
        assert!(f.synced());
        assert_eq!(f.state().len(), 50);

        // A missing part asks for the snapshot again.
        let g = Follower::new(Regime::default(), None, CounterState::new(), &budget);
        assert!(
            !g.on_sync(&part(9, &data, 0, third, false), &LIMITS)
                .need_snapshot
        );
        let ack = g.on_sync(&part(9, &data, 2 * third, data.len(), true), &LIMITS);
        assert!(ack.need_snapshot);
        assert_eq!(g.state().len(), 0);
    }

    /// The up switch copies the state of a leader while writers run. A write that passed the
    /// serving test before the stop must not apply after the copy.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_sealed_state_holds_every_acknowledged_write() {
        let budget = Arc::new(Budget::new(u64::MAX, None));
        let regime = Regime {
            generation: 1,
            origin: NodeId::from_u128(1),
        };
        for _ in 0..20 {
            let l = LfLeader::new(regime, CounterState::new(), &budget, None, Duration::ZERO);
            let writers: Vec<_> = (0..16)
                .map(|_| {
                    let l = l.clone();
                    tokio::spawn(async move {
                        let incr = cmd(&["INCR", "cnt:x"]);
                        let mut ok = 0i64;
                        while l
                            .execute(Kind::Incr, 0, &incr, 1, &LIMITS, Duration::ZERO)
                            .await
                            .is_ok()
                        {
                            ok += 1;
                            tokio::task::yield_now().await;
                        }
                        ok
                    })
                })
                .collect();
            tokio::time::sleep(Duration::from_millis(5)).await;
            let state = l.seal();
            let mut ok = 0;
            for w in writers {
                ok += w.await.unwrap();
            }
            let value: i64 = state.get(0, b"cnt:x").map_or(0, |e| {
                std::str::from_utf8(&e.value).unwrap().parse().unwrap()
            });
            assert!(
                ok <= value,
                "{ok} writes succeeded, but the value is {value}"
            );
        }
    }
}
