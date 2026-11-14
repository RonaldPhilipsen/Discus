//! Same-key ordering for one connection (spec section 7.1).
//!
//! A command waits while an earlier command on any of its keys is in flight. Commands on disjoint
//! keys run concurrently. A keyless data command is a barrier: it waits for every earlier command,
//! and every later command waits for it.
//!
//! A key is tracked by its hash, so ordering copies no key. Two keys with one hash order as one
//! key: that adds a wait and never drops one.

use std::collections::HashMap;
use std::slice;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use discus_resp::Command;
use tokio::sync::Notify;
use xxhash_rust::xxh3::xxh3_64;

use crate::cluster::route::{key_args, Route};

/// What a command orders on: the hashes of its keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Keys {
    None,
    One(u64),
    Many(Vec<u64>),
    /// Waits for every earlier command, and every later command waits for it.
    Barrier,
}

impl Keys {
    /// The ordering keys of `cmd` for its route. The arity is already checked.
    pub fn of(route: Route, cmd: &Command) -> Keys {
        match route {
            Route::Connection => Keys::None,
            Route::Local | Route::Fanout | Route::Broadcast => Keys::Barrier,
            Route::Single { .. } => Keys::One(xxh3_64(&cmd[1])),
            Route::MultiRead { .. } | Route::MultiWrite { .. } | Route::Rename | Route::Copy => {
                Keys::Many(
                    key_args(route, cmd)
                        .into_iter()
                        .flatten()
                        .map(|k| xxh3_64(k))
                        .collect(),
                )
            }
        }
    }
}

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
    last: HashMap<u64, Arc<Gate>>,
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
            Keys::One(_) | Keys::Many(_) => {
                let list = match keys {
                    Keys::One(h) => slice::from_ref(h),
                    Keys::Many(list) => list,
                    _ => &[],
                };
                for &key in list {
                    if let Some(prev) = self.last.insert(key, gate.clone()) {
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

    fn cmd(args: &[&'static str]) -> Command {
        args.iter()
            .map(|a| Bytes::from_static(a.as_bytes()))
            .collect()
    }

    fn keys(names: &[&'static str]) -> Keys {
        Keys::Many(names.iter().map(|n| xxh3_64(n.as_bytes())).collect())
    }

    #[test]
    fn ordering_keys_follow_the_route_shape() {
        let single = Route::Single { write: true };
        assert_eq!(
            Keys::of(single, &cmd(&["SET", "k", "v"])),
            Keys::One(xxh3_64(b"k"))
        );
        let mset = Keys::of(
            Route::MultiWrite { pairs: true },
            &cmd(&["MSET", "a", "1", "b", "2"]),
        );
        assert_eq!(mset, keys(&["a", "b"]));
        let mget = Keys::of(Route::MultiRead { sum: false }, &cmd(&["MGET", "a", "b"]));
        assert_eq!(mget, keys(&["a", "b"]));
        assert_eq!(Keys::of(Route::Connection, &cmd(&["PING"])), Keys::None);
        assert_eq!(Keys::of(Route::Fanout, &cmd(&["DBSIZE"])), Keys::Barrier);
    }

    #[tokio::test]
    async fn a_multi_key_command_waits_for_a_single_key_command() {
        let mut order = KeyOrder::new();
        let set = Keys::of(Route::Single { write: true }, &cmd(&["SET", "b", "v"]));
        let (_, d1) = order.admit(&set);
        let mget = Keys::of(Route::MultiRead { sum: false }, &cmd(&["MGET", "a", "b"]));
        let (w2, _d2) = order.admit(&mget);
        assert!(!w2.is_ready());
        drop(d1);
        assert!(w2.is_ready());
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
            let k = Keys::One(xxh3_64(i.to_string().as_bytes()));
            let (_, done) = order.admit(&k);
            drop(done);
        }
        assert!(order.tracked() <= 64, "{} entries", order.tracked());
    }
}
