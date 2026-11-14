use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::{ArcSwap, Guard};
use parking_lot::Mutex;

use crate::config::Config;

/// The settings that a running node can change without a restart.
#[derive(Clone, Debug, PartialEq)]
pub struct Reloadable {
    pub forward_read_timeout: Duration,
    pub forward_write_timeout: Duration,
    pub hold_budget: Duration,
    pub replication_timeout: Duration,
    pub read_from_replicas: bool,
    pub handoff_rate_bytes: u64,
    pub auth_max_failures_per_connection: u32,
    pub auth_max_failures_per_source: u32,
    pub auth_failure_window: Duration,
    pub tls_reload: Duration,
}

impl Reloadable {
    pub fn from_config(c: &Config) -> Reloadable {
        Reloadable {
            forward_read_timeout: Duration::from_millis(c.forward_read_timeout_ms),
            forward_write_timeout: Duration::from_millis(c.forward_write_timeout_ms),
            hold_budget: Duration::from_millis(c.hold_budget_ms),
            replication_timeout: Duration::from_millis(c.replication_timeout_ms),
            read_from_replicas: c.read_from_replicas,
            handoff_rate_bytes: c.handoff_rate_bytes,
            auth_max_failures_per_connection: c.security.auth_max_failures_per_connection,
            auth_max_failures_per_source: c.security.auth_max_failures_per_source,
            auth_failure_window: Duration::from_secs(c.security.auth_failure_window_secs),
            tls_reload: Duration::from_secs(c.tls_reload_secs),
        }
    }

    /// Names the settings that differ from `other`, as `section.key` file keys in table order.
    pub fn changed(&self, other: &Reloadable) -> Vec<&'static str> {
        let checks = [
            (
                "budget.forward-read-ms",
                self.forward_read_timeout != other.forward_read_timeout,
            ),
            (
                "budget.forward-write-ms",
                self.forward_write_timeout != other.forward_write_timeout,
            ),
            ("budget.hold-ms", self.hold_budget != other.hold_budget),
            (
                "budget.replication-ms",
                self.replication_timeout != other.replication_timeout,
            ),
            (
                "reads.from-replicas",
                self.read_from_replicas != other.read_from_replicas,
            ),
            (
                "handoff.rate-bytes",
                self.handoff_rate_bytes != other.handoff_rate_bytes,
            ),
            (
                "auth.max-failures-per-connection",
                self.auth_max_failures_per_connection != other.auth_max_failures_per_connection,
            ),
            (
                "auth.max-failures-per-source",
                self.auth_max_failures_per_source != other.auth_max_failures_per_source,
            ),
            (
                "auth.failure-window-secs",
                self.auth_failure_window != other.auth_failure_window,
            ),
            ("tls.reload-secs", self.tls_reload != other.tls_reload),
        ];
        checks
            .into_iter()
            .filter_map(|(name, differs)| differs.then_some(name))
            .collect()
    }
}

/// The shared, atomically replaced snapshot of the reloadable settings.
pub struct Live {
    current: ArcSwap<Reloadable>,
    generation: AtomicU64,
    pending: Mutex<Vec<&'static str>>,
    version: AtomicU32,
}

impl Live {
    pub fn new(r: Reloadable) -> Arc<Live> {
        Arc::new(Live {
            current: ArcSwap::from_pointee(r),
            generation: AtomicU64::new(0),
            pending: Mutex::new(Vec::new()),
            version: AtomicU32::new(0),
        })
    }

    // Every keyed read in cluster mode comes here, so a guard avoids the two atomic operations on
    // one shared counter that a cloned Arc costs. Copy a field out before an `.await`: a held
    // guard takes one of a few per-thread slots.
    pub fn get(&self) -> Guard<Arc<Reloadable>> {
        self.current.load()
    }

    /// Stores `next` and returns the keys that changed. The generation grows only on a change.
    pub fn apply(&self, next: Reloadable) -> Vec<&'static str> {
        let previous = self.current.swap(Arc::new(next));
        let changed = previous.changed(&self.current.load());
        if !changed.is_empty() {
            self.generation.fetch_add(1, Ordering::AcqRel);
        }
        changed
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub fn set_pending(&self, names: Vec<&'static str>) {
        *self.pending.lock() = names;
    }

    pub fn pending(&self) -> Vec<&'static str> {
        self.pending.lock().clone()
    }

    pub fn set_version(&self, v: u32) {
        self.version.store(v, Ordering::Release);
    }

    pub fn version(&self) -> Option<u32> {
        match self.version.load(Ordering::Acquire) {
            0 => None,
            v => Some(v),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_reloadable() -> Reloadable {
        Reloadable::from_config(&Config::parse_from(["discus"]).unwrap())
    }

    #[test]
    fn apply_returns_the_changed_keys_and_counts_one_generation() {
        let base = default_reloadable();
        let live = Live::new(base.clone());
        let next = Reloadable {
            hold_budget: Duration::from_millis(1500),
            read_from_replicas: !base.read_from_replicas,
            ..base
        };
        assert_eq!(live.apply(next), ["budget.hold-ms", "reads.from-replicas"]);
        assert_eq!(live.generation(), 1);
        assert_eq!(live.get().hold_budget, Duration::from_millis(1500));
    }

    #[test]
    fn apply_without_a_change_keeps_the_generation() {
        let base = default_reloadable();
        let live = Live::new(base.clone());
        assert!(live.apply(base).is_empty());
        assert_eq!(live.generation(), 0);
    }

    #[test]
    fn a_reader_keeps_its_snapshot() {
        let base = default_reloadable();
        let live = Live::new(base.clone());
        let before = live.get();
        live.apply(Reloadable {
            hold_budget: base.hold_budget + Duration::from_millis(1),
            ..base.clone()
        });
        assert_eq!(before.hold_budget, base.hold_budget);
        assert_ne!(live.get().hold_budget, base.hold_budget);
    }
}
