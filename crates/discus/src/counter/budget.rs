//! The counter memory reserve (spec sections 10.4 and 11).
//!
//! Every counter state that a node holds, as leader, follower, or Raft replica, charges its bytes
//! to the one store budget. The store never evicts them. The reserve only gates new writes on the
//! leader: a write that can grow the state returns `-OOM` once the node's counter bytes reach it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use discus_resp::{Command, Value};
use discus_store::Store;

use super::state::{CounterState, Limits};

pub struct Budget {
    used: AtomicU64,
    reserve: u64,
    store: Option<Arc<Store>>,
}

impl std::fmt::Debug for Budget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Budget")
            .field("used", &self.used())
            .field("reserve", &self.reserve)
            .finish()
    }
}

impl Budget {
    pub fn new(reserve: u64, store: Option<Arc<Store>>) -> Budget {
        Budget {
            used: AtomicU64::new(0),
            reserve,
            store,
        }
    }

    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }

    pub fn reserve(&self) -> u64 {
        self.reserve
    }

    pub fn full(&self) -> bool {
        self.used() >= self.reserve
    }

    fn charge(&self, bytes: u64) {
        self.used.fetch_add(bytes, Ordering::Relaxed);
        if let Some(store) = &self.store {
            store.charge(bytes);
        }
    }

    fn release(&self, bytes: u64) {
        self.used.fetch_sub(bytes, Ordering::Relaxed);
        if let Some(store) = &self.store {
            store.release(bytes);
        }
    }
}

/// A counter state whose bytes count in a [`Budget`] for as long as it lives.
#[derive(Debug)]
pub struct Tracked {
    state: CounterState,
    budget: Arc<Budget>,
    charged: u64,
}

impl Tracked {
    pub fn new(state: CounterState, budget: Arc<Budget>) -> Tracked {
        let mut t = Tracked {
            state,
            budget,
            charged: 0,
        };
        t.sync();
        t
    }

    pub fn get(&self) -> &CounterState {
        &self.state
    }

    pub fn apply(&mut self, stamp_ms: u64, cmd: &Command, limits: &Limits) -> Value {
        let reply = self.state.apply(stamp_ms, cmd, limits);
        self.sync();
        reply
    }

    pub fn replace(&mut self, state: CounterState) {
        self.state = state;
        self.sync();
    }

    fn sync(&mut self) {
        let now = self.state.bytes();
        if now > self.charged {
            self.budget.charge(now - self.charged);
        } else {
            self.budget.release(self.charged - now);
        }
        self.charged = now;
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.budget.release(self.charged);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn a_tracked_state_charges_and_releases() {
        let budget = Arc::new(Budget::new(200, None));
        let limits = Limits {
            max_value_bytes: 1 << 20,
        };
        let cmd: Command = ["SET", "k", "v"]
            .iter()
            .map(|s| Bytes::from_static(s.as_bytes()))
            .collect();
        let mut t = Tracked::new(CounterState::new(), budget.clone());
        t.apply(1, &cmd, &limits);
        assert_eq!(budget.used(), t.get().bytes());
        let other = Tracked::new(t.get().clone(), budget.clone());
        assert_eq!(budget.used(), 2 * t.get().bytes());
        drop(other);
        t.replace(CounterState::new());
        assert_eq!(budget.used(), 0);
        assert!(!budget.full());
    }
}
