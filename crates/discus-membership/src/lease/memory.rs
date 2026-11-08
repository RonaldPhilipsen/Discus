//! An in-memory lease for tests and the simulator. It follows the rules of the Kubernetes
//! provider: a lease expires `duration_ms` after its last renewal.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use discus_placement::ClusterId;
use futures::future::BoxFuture;
use parking_lot::Mutex;

use super::{LeaseError, LeaseHolder, LeaseProvider, LeaseState};
use crate::Millis;

type ClockFn = Arc<dyn Fn() -> Millis + Send + Sync>;

#[derive(Clone)]
pub struct MemoryLease {
    clock: ClockFn,
    duration_ms: Millis,
    state: Arc<Mutex<Option<(LeaseHolder, Millis)>>>,
    available: Arc<AtomicBool>,
}

impl MemoryLease {
    pub fn new(clock: ClockFn, duration_ms: Millis) -> MemoryLease {
        MemoryLease {
            clock,
            duration_ms,
            state: Arc::new(Mutex::new(None)),
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

    /// The holder of a live lease.
    pub fn holder(&self) -> Option<LeaseHolder> {
        let now = (self.clock)();
        self.state
            .lock()
            .as_ref()
            .filter(|(_, renewed)| now.saturating_sub(*renewed) < self.duration_ms)
            .map(|(h, _)| h.clone())
    }

    pub fn expire(&self) {
        *self.state.lock() = None;
    }

    fn call(&self, me: &LeaseHolder) -> Result<LeaseState, LeaseError> {
        if !self.available.load(Ordering::Relaxed) {
            return Err(LeaseError("the API server is unavailable".into()));
        }
        let now = (self.clock)();
        let mut state = self.state.lock();
        match state.as_ref() {
            Some((h, renewed)) if now.saturating_sub(*renewed) < self.duration_ms => {
                if h.identity == me.identity {
                    *state = Some((me.clone(), now));
                    Ok(LeaseState::Held(me.clone()))
                } else if h.cluster_id == me.cluster_id && me.cluster_id != ClusterId::NIL {
                    Ok(LeaseState::Held(h.clone()))
                } else {
                    Ok(LeaseState::HeldByOther(h.clone()))
                }
            }
            _ => {
                *state = Some((me.clone(), now));
                Ok(LeaseState::Held(me.clone()))
            }
        }
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
