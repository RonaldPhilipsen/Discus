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
use discus_wire::{encode_command, CounterAck, CounterSync, Message, Regime};
use parking_lot::Mutex;
use tokio::sync::{watch, Notify};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::budget::{Budget, Tracked};
use super::state::{parse_command, CounterState, Kind, Limits};
use super::{Counters, Refusal};

/// Ops in one `COUNTER_SYNC`.
const SYNC_OPS: usize = 512;
/// How long the sender waits for one sync to be acknowledged.
const SYNC_TIMEOUT: Duration = Duration::from_secs(2);
/// The pause after a failed sync.
const SYNC_RETRY: Duration = Duration::from_millis(100);

struct Core {
    state: Tracked,
    /// The largest stamp so far. Stamps never run backwards within one regime.
    last_stamp: u64,
    /// The number of the last op.
    seq: u64,
    /// Ops that the follower has not acknowledged: number, stamp, and raw command.
    pending: VecDeque<(u64, u64, Bytes)>,
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
            }),
            lease_until: Mutex::new(lease_until),
            margin,
            follower: Mutex::new(None),
            lagging: AtomicBool::new(true),
            acked: watch::channel(0).0,
            wake: Notify::new(),
            stop: CancellationToken::new(),
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

    pub fn state(&self) -> CounterState {
        self.core.lock().state.get().clone()
    }

    pub fn holds_expired(&self, now_ms: u64) -> bool {
        self.core.lock().state.get().expired_keys(now_ms)
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

    /// Run one command. A read runs at `now_ms` without a stamp. A write takes the next stamp,
    /// and with a follower it waits up to `wait` for the ack (spec section 10.1).
    pub async fn execute(
        &self,
        kind: Kind,
        cmd: &Command,
        now_ms: u64,
        limits: &Limits,
        wait: Duration,
    ) -> Result<Value, Refusal> {
        if !self.serving() {
            return Err(Refusal::NotLeader);
        }
        if kind.is_read() {
            return Ok(self.core.lock().state.get().read(now_ms, kind, cmd));
        }
        let follower = self.follower().is_some();
        let raw = follower.then(|| encode_command(cmd));
        let (reply, seq) = {
            let mut core = self.core.lock();
            let stamp = now_ms.max(core.last_stamp);
            core.last_stamp = stamp;
            core.seq += 1;
            let seq = core.seq;
            let reply = core.state.apply(stamp, cmd, limits);
            if let Some(raw) = raw {
                core.pending.push_back((seq, stamp, raw));
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
        self.core.lock().pending.retain(|(s, _, _)| *s > seq);
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
            let sync = if self.lagging.swap(false, Ordering::AcqRel) {
                let mut core = self.core.lock();
                core.pending.clear();
                CounterSync {
                    group,
                    regime: self.regime,
                    seq: core.seq,
                    ops: Vec::new(),
                    snapshot: Some(Bytes::from(core.state.get().encode())),
                }
            } else {
                let core = self.core.lock();
                let ops: Vec<(u64, u64, Bytes)> =
                    core.pending.iter().take(SYNC_OPS).cloned().collect();
                if ops.is_empty() && !self.serving() {
                    continue;
                }
                CounterSync {
                    group,
                    regime: self.regime,
                    seq: ops.first().map_or(core.seq + 1, |o| o.0),
                    ops: ops.into_iter().map(|(_, stamp, c)| (stamp, c)).collect(),
                    snapshot: None,
                }
            };
            let snapshot = sync.snapshot.is_some();
            let sent_seq = sync.seq;
            match counters
                .send(follower, Message::CounterSync(sync), SYNC_TIMEOUT)
                .await
            {
                Ok(Message::CounterAck(a)) if a.regime == self.regime && !a.need_snapshot => {
                    self.ack(if snapshot { sent_seq } else { a.seq });
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
        if let Some(snapshot) = &s.snapshot {
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
                for (stamp, raw) in &s.ops {
                    match parse_command(raw) {
                        Some(cmd) => {
                            core.state.apply(*stamp, &cmd, limits);
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
