//! One bounded replication queue per peer (spec section 8.2).
//!
//! The queue holds keys, not entries. The sender reads each entry from the store at send time,
//! so a batch always carries the latest full entry of a key, and a key written many times waits
//! in the queue once.

use std::collections::HashSet;

use foldhash::fast::RandomState;
use hashlink::LinkedHashMap;
use parking_lot::Mutex;
use tokio::sync::Notify;

use bytes::Bytes;
use discus_placement::NodeId;
use discus_store::{AsDbKey, DbKey};

/// The queue of one peer.
pub struct PeerQueue {
    pub peer: NodeId,
    /// The metric label of `peer`, made once.
    pub label: Option<discus_telemetry::attrs::NodeLabel>,
    state: Mutex<QueueState>,
    pub notify: Notify,
    cap: usize,
}

struct QueueState {
    /// Oldest key first. A removed key leaves its node on the free list of the map, so a queue
    /// that keeps its size allocates nothing for a new key.
    /// Both maps hash with a random seed, so clients cannot choose keys that collide.
    keys: LinkedHashMap<DbKey, (), RandomState>,
    /// Keys of the batch on the wire, until the peer acknowledges it.
    inflight: HashSet<DbKey, RandomState>,
    /// The last batch failed. Writes go to hints until a probe succeeds.
    down: bool,
    /// The queue overflowed since the last drain below the low-water mark.
    lagging: bool,
    /// The peer left the epoch. The sender stops.
    closed: bool,
}

impl PeerQueue {
    pub fn new(peer: NodeId, cap: usize) -> PeerQueue {
        PeerQueue {
            peer,
            label: crate::telemetry::node_label(peer),
            state: Mutex::new(QueueState {
                keys: LinkedHashMap::default(),
                inflight: HashSet::default(),
                down: false,
                lagging: false,
                closed: false,
            }),
            notify: Notify::new(),
            cap: cap.max(1),
        }
    }

    /// Queue `key` of database `db`. A key already queued keeps one place. Returns the key to
    /// hint instead: `key` itself while the peer is down, or the oldest key when the queue
    /// overflowed.
    pub fn push(&self, db: u32, key: &Bytes) -> Option<DbKey> {
        let mut s = self.state.lock();
        if s.down {
            return Some(DbKey::new(db, key.clone()));
        }
        // A key already queued moves to the back. Only a new key is cloned: the first clone of a
        // key from the RESP parser allocates.
        if s.keys.to_back(&(db, &key[..]) as &dyn AsDbKey).is_some() {
            return None;
        }
        s.keys.insert(DbKey::new(db, key.clone()), ());
        let out = if s.keys.len() > self.cap {
            s.lagging = true;
            s.keys.pop_front().map(|(old, ())| old)
        } else {
            None
        };
        drop(s);
        self.notify.notify_one();
        out
    }

    /// Take up to `max` of the oldest keys and mark them in flight.
    pub fn take(&self, max: usize) -> Vec<DbKey> {
        let mut s = self.state.lock();
        let mut out = Vec::new();
        while out.len() < max {
            let Some((k, ())) = s.keys.pop_front() else {
                break;
            };
            s.inflight.insert(k.clone());
            out.push(k);
        }
        out
    }

    /// Put back keys taken for a batch that had no room for them. They go first in line, in
    /// their order. A key written again since `take` already has its place.
    pub fn untake(&self, keys: impl DoubleEndedIterator<Item = DbKey>) {
        let mut s = self.state.lock();
        for k in keys.rev() {
            s.inflight.remove(&k);
            if !s.keys.contains_key(&k) {
                s.keys.insert(k.clone(), ());
            }
            s.keys.to_front(&k);
        }
    }

    /// The batch in flight was acknowledged.
    pub fn acked(&self) {
        self.state.lock().inflight.clear();
    }

    /// The batch in flight failed. Its keys and the queued keys come back for hints, and the
    /// peer is down until a probe succeeds.
    pub fn failed(&self) -> Vec<DbKey> {
        let mut s = self.state.lock();
        s.down = true;
        let mut out: Vec<DbKey> = s.inflight.drain().collect();
        while let Some((k, ())) = s.keys.pop_front() {
            out.push(k);
        }
        out
    }

    /// True once after the queue of a lagging peer drains below a quarter of its size.
    pub fn take_drained(&self) -> bool {
        let mut s = self.state.lock();
        if s.lagging && s.keys.len() < self.cap / 4 {
            s.lagging = false;
            // Free the nodes that the backlog left on the free list.
            s.keys.shrink_to_fit();
            return true;
        }
        false
    }

    pub fn clear(&self) {
        let mut s = self.state.lock();
        s.keys.clear();
        s.inflight.clear();
    }

    pub fn set_up(&self) {
        self.state.lock().down = false;
    }

    pub fn is_down(&self) -> bool {
        self.state.lock().down
    }

    pub fn close(&self) {
        self.state.lock().closed = true;
        self.notify.notify_one();
    }

    pub fn is_closed(&self) -> bool {
        self.state.lock().closed
    }

    pub fn len(&self) -> usize {
        self.state.lock().keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The key is queued or in flight.
    pub fn holds(&self, key: &dyn AsDbKey) -> bool {
        let s = self.state.lock();
        s.keys.contains_key(key) || s.inflight.contains(key)
    }

    /// Drop the queued keys for which `keep` is false.
    pub fn retain(&self, mut keep: impl FnMut(&DbKey) -> bool) {
        let mut s = self.state.lock();
        s.keys.retain(|k, _| keep(k));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    fn k(s: &str) -> DbKey {
        DbKey::new(0, b(s))
    }

    #[test]
    fn a_queued_key_keeps_its_first_copy() {
        let q = PeerQueue::new(NodeId::from_u128(1), 8);
        let first = b("a");
        q.push(0, &first);
        q.push(0, &b("a"));
        assert_eq!(q.len(), 1);
        let taken = q.take(10);
        assert_eq!(taken.len(), 1);
        assert_eq!(taken[0].key.as_ptr(), first.as_ptr());
    }

    #[test]
    fn a_key_waits_once() {
        let q = PeerQueue::new(NodeId::from_u128(1), 8);
        assert_eq!(q.push(0, &b("a")), None);
        assert_eq!(q.push(0, &b("a")), None);
        assert_eq!(q.len(), 1);
        assert_eq!(q.take(10), vec![k("a")]);
        assert!(q.holds(&k("a")));
        q.acked();
        assert!(!q.holds(&k("a")));
    }

    #[test]
    fn the_same_key_in_two_databases_waits_twice() {
        let q = PeerQueue::new(NodeId::from_u128(1), 8);
        let other = DbKey::new(1, k("a").key);
        assert_eq!(q.push(0, &b("a")), None);
        assert_eq!(q.push(other.db, &other.key), None);
        assert_eq!(q.len(), 2);
        assert_eq!(q.take(10), vec![k("a"), other.clone()]);
        assert!(q.holds(&other));
        assert!(!q.holds(&DbKey::new(2, k("a").key)));
    }

    #[test]
    fn overflow_drops_the_oldest_and_marks_lagging() {
        let q = PeerQueue::new(NodeId::from_u128(1), 4);
        for i in 0..4 {
            assert_eq!(q.push(0, &b(&i.to_string())), None);
        }
        assert_eq!(q.push(0, &b("x")), Some(k("0")));
        assert!(!q.take_drained());
        q.take(4);
        assert!(q.take_drained());
        assert!(!q.take_drained());
    }

    #[test]
    fn a_failed_batch_returns_every_key_and_marks_the_peer_down() {
        let q = PeerQueue::new(NodeId::from_u128(1), 8);
        q.push(0, &b("a"));
        q.push(0, &b("b"));
        q.take(1);
        let mut back = q.failed();
        back.sort();
        assert_eq!(back, vec![k("a"), k("b")]);
        assert_eq!(q.push(0, &b("c")), Some(k("c")));
        q.set_up();
        assert_eq!(q.push(0, &b("c")), None);
    }

    #[test]
    fn queues_drop_keys_no_longer_owned() {
        let q = PeerQueue::new(NodeId::from_u128(1), 8);
        q.push(0, &b("keep"));
        q.push(0, &b("gone"));
        q.retain(|key| key.key.as_ref() == b"keep");
        assert_eq!(q.len(), 1);
        assert!(q.holds(&k("keep")));
    }
}
