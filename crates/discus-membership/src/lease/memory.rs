//! An in-memory lease for tests and the simulator. It follows the rules of the Kubernetes
//! provider: a contender takes the lease only after `duration_ms` without a change of the
//! record, counted from its own first read of that record.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use discus_placement::ClusterId;
use futures::future::BoxFuture;
use parking_lot::Mutex;

use super::{LeaseError, LeaseHolder, LeaseProvider, LeaseState};
use crate::Millis;

type ClockFn = Arc<dyn Fn() -> Millis + Send + Sync>;

struct Record {
    holder: LeaseHolder,
    renewed: Millis,
    /// Raised on every write, like `resourceVersion`.
    version: u64,
}

#[derive(Default)]
struct State {
    record: Option<Record>,
    next_version: u64,
    /// Per contender identity: the record version it last read, and when it first read it.
    observed: HashMap<String, (u64, Millis)>,
}

#[derive(Clone)]
pub struct MemoryLease {
    clock: ClockFn,
    duration_ms: Millis,
    state: Arc<Mutex<State>>,
    available: Arc<AtomicBool>,
}

impl MemoryLease {
    pub fn new(clock: ClockFn, duration_ms: Millis) -> MemoryLease {
        MemoryLease {
            clock,
            duration_ms,
            state: Arc::new(Mutex::new(State::default())),
            available: Arc::new(AtomicBool::new(true)),
        }
    }

    /// A lease on the process monotonic clock.
    pub fn system(duration_ms: Millis) -> MemoryLease {
        let start = Instant::now();
        MemoryLease::new(
            Arc::new(move || start.elapsed().as_millis() as Millis),
            duration_ms,
        )
    }

    /// `false` simulates an API server outage: every call fails.
    pub fn set_available(&self, available: bool) {
        self.available.store(available, Ordering::Relaxed);
    }

    /// The same lease behind its own availability flag, as one node that loses the API server
    /// while the others keep it.
    pub fn gated(&self, available: Arc<AtomicBool>) -> MemoryLease {
        MemoryLease {
            available,
            ..self.clone()
        }
    }

    /// The holder that renewed the lease less than a duration ago on the shared clock.
    pub fn holder(&self) -> Option<LeaseHolder> {
        let now = (self.clock)();
        self.state
            .lock()
            .record
            .as_ref()
            .filter(|r| now.saturating_sub(r.renewed) < self.duration_ms)
            .map(|r| r.holder.clone())
    }

    /// Delete the record, as if the lease object was removed.
    pub fn expire(&self) {
        self.state.lock().record = None;
    }

    fn call(&self, me: &LeaseHolder) -> Result<LeaseState, LeaseError> {
        if !self.available.load(Ordering::Relaxed) {
            return Err(LeaseError("the API server is unavailable".into()));
        }
        let now = (self.clock)();
        let mut state = self.state.lock();
        let state = &mut *state;
        if let Some(r) = &state.record {
            let seen = state
                .observed
                .entry(me.identity.clone())
                .or_insert((r.version, now));
            if seen.0 != r.version {
                *seen = (r.version, now);
            }
            let live = now.saturating_sub(seen.1) < self.duration_ms;
            let h = &r.holder;
            if live && h.identity != me.identity {
                return if h.cluster_id == me.cluster_id && me.cluster_id != ClusterId::NIL {
                    Ok(LeaseState::Held(h.clone()))
                } else {
                    Ok(LeaseState::HeldByOther(h.clone()))
                };
            }
        }
        state.next_version += 1;
        let version = state.next_version;
        state.record = Some(Record {
            holder: me.clone(),
            renewed: now,
            version,
        });
        state.observed.insert(me.identity.clone(), (version, now));
        Ok(LeaseState::Held(me.clone()))
    }
}

impl LeaseProvider for MemoryLease {
    fn acquire_or_renew<'a>(
        &'a self,
        me: &'a LeaseHolder,
    ) -> BoxFuture<'a, Result<LeaseState, LeaseError>> {
        let result = self.call(me);
        Box::pin(async move { result })
    }

    fn name(&self) -> &'static str {
        "memory"
    }
}
