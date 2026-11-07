//! Same-key ordering for one connection (spec section 7.1).
//!
//! A command waits while an earlier command on any of its keys is in flight. Commands on disjoint
//! keys run concurrently. A keyless data command is a barrier: it waits for every earlier command,
//! and every later command waits for it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Notify;

pub use crate::cluster::route::Keys;

/// Completion of one command.
#[derive(Debug, Default)]
struct Gate {
    done: AtomicBool,
    notify: Notify,
}

impl Gate {
    fn is_done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }

    async fn wait(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_done() {
                return;
            }
            notified.await;
        }
    }
}

/// Marks a command done when dropped.
#[derive(Debug)]
pub struct Done(Arc<Gate>);

impl Drop for Done {
    fn drop(&mut self) {
        self.0.done.store(true, Ordering::Release);
        self.0.notify.notify_waiters();
    }
}

/// The earlier commands that one command waits for.
#[derive(Debug, Clone, Default)]
pub struct WaitFor(Vec<Arc<Gate>>);

impl WaitFor {
    /// True when every earlier command that this one depends on is done.
    pub fn is_ready(&self) -> bool {
        self.0.iter().all(|g| g.is_done())
    }

    pub async fn wait(self) {
        for gate in &self.0 {
            gate.wait().await;
        }
    }
}

/// The ordering state of one connection.
#[derive(Debug, Default)]
pub struct KeyOrder {
    last: HashMap<Bytes, Arc<Gate>>,
    barrier: Option<Arc<Gate>>,
    prune_at: usize,
}

const MIN_PRUNE: usize = 32;

impl KeyOrder {
    pub fn new() -> KeyOrder {
        KeyOrder {
            prune_at: MIN_PRUNE,
            ..KeyOrder::default()
        }
    }

    /// Register the next command. It waits for the returned `WaitFor` before it runs, and drops
    /// `Done` when it finishes.
    pub fn admit(&mut self, keys: &Keys) -> (WaitFor, Done) {
        let gate = Arc::new(Gate::default());
        let mut deps: Vec<Arc<Gate>> = Vec::new();
        if let Some(b) = self.barrier.as_ref().filter(|b| !b.is_done()) {
            deps.push(b.clone());
        }
        match keys {
            Keys::None => {}
            Keys::Some(list) => {
                for key in list {
                    if let Some(prev) = self.last.insert(key.clone(), gate.clone()) {
                        // A key named twice in one command must not wait for itself.
                        let own = Arc::ptr_eq(&prev, &gate);
                        if !own && !prev.is_done() && !deps.iter().any(|d| Arc::ptr_eq(d, &prev)) {
                            deps.push(prev);
                        }
                    }
                }
            }
            Keys::Barrier => {
                deps.extend(self.last.drain().map(|(_, g)| g).filter(|g| !g.is_done()));
                self.barrier = Some(gate.clone());
            }
        }
        if self.last.len() > self.prune_at {
            self.last.retain(|_, g| !g.is_done());
            self.prune_at = (self.last.len() * 2).max(MIN_PRUNE);
        }
        (WaitFor(deps), Done(gate))
    }

    /// Keys tracked now, for tests.
    #[cfg(test)]
    pub fn tracked(&self) -> usize {
        self.last.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::time::Duration;

    fn keys(names: &[&'static str]) -> Keys {
        Keys::Some(
            names
                .iter()
                .map(|n| Bytes::from_static(n.as_bytes()))
                .collect(),
        )
    }

    async fn pending(wait: WaitFor) -> bool {
        tokio::time::timeout(Duration::from_millis(20), wait.wait())
            .await
            .is_err()
    }

    #[tokio::test]
    async fn same_key_waits_for_the_earlier_command() {
        let mut order = KeyOrder::new();
        let (w1, d1) = order.admit(&keys(&["k"]));
        assert!(w1.is_ready());
        let (w2, _d2) = order.admit(&keys(&["k"]));
        assert!(!w2.is_ready());
        let w2b = w2.clone();
        assert!(pending(w2).await);
        drop(d1);
        assert!(w2b.is_ready());
        w2b.wait().await;
    }

    #[tokio::test]
    async fn a_key_named_twice_does_not_wait_for_itself() {
        let mut order = KeyOrder::new();
        let (w, _d) = order.admit(&keys(&["a", "b", "a"]));
        assert!(w.is_ready());
    }

    #[tokio::test]
    async fn disjoint_keys_do_not_wait() {
        let mut order = KeyOrder::new();
        let (_w1, _d1) = order.admit(&keys(&["a"]));
        let (w2, _d2) = order.admit(&keys(&["b"]));
        assert!(w2.is_ready());
        let (w3, _d3) = order.admit(&Keys::None);
        assert!(w3.is_ready());
    }

    #[tokio::test]
    async fn a_multi_key_command_waits_for_each_key() {
        let mut order = KeyOrder::new();
        let (_, da) = order.admit(&keys(&["a"]));
        let (_, db) = order.admit(&keys(&["b"]));
        let (w, _) = order.admit(&keys(&["a", "b"]));
        drop(da);
        assert!(!w.is_ready());
        drop(db);
        assert!(w.is_ready());
    }

    #[tokio::test]
    async fn a_barrier_waits_for_everything_and_blocks_later_commands() {
        let mut order = KeyOrder::new();
        let (_, da) = order.admit(&keys(&["a"]));
        let (wb, db) = order.admit(&Keys::Barrier);
        let (wc, _) = order.admit(&keys(&["c"]));
        assert!(!wb.is_ready());
        assert!(!wc.is_ready());
        drop(da);
        assert!(wb.is_ready());
        assert!(!wc.is_ready());
        drop(db);
        assert!(wc.is_ready());
    }

    #[tokio::test]
    async fn finished_entries_are_pruned() {
        let mut order = KeyOrder::new();
        for i in 0..10_000u32 {
            let k = Keys::Some(vec![Bytes::from(i.to_string())]);
            let (_, done) = order.admit(&k);
            drop(done);
        }
        assert!(order.tracked() <= 64, "{} entries", order.tracked());
    }
}
