//! One bounded replication queue per peer (spec section 8.2).
//!
//! The queue holds keys, not entries. The sender reads each entry from the store at send time,
//! so a batch always carries the latest full entry of a key, and a key written many times waits
//! in the queue once.

use std::collections::HashSet;

use bytes::Bytes;
use lru::LruCache;
use parking_lot::Mutex;
use tokio::sync::Notify;

use discus_placement::NodeId;

/// The queue of one peer.
pub struct PeerQueue {
    pub peer: NodeId,
    state: Mutex<QueueState>,
    pub notify: Notify,
    cap: usize,
}

struct QueueState {
    keys: LruCache<Bytes, ()>,
    /// Keys of the batch on the wire, until the peer acknowledges it.
    inflight: HashSet<Bytes>,
    /// The last batch failed. Writes go to hints until a probe succeeds.
    down: bool,
    /// The queue overflowed since the last drain below the low-water mark.
    lagging: bool,
    /// The peer left the epoch. The sender stops.
    closed: bool,
}

/// What happened to a key that `push` was given.
#[derive(Debug, PartialEq, Eq)]
pub enum Pushed {
    Queued,
    /// The queue was full, so its oldest key dropped out.
    Overflow(Bytes),
    /// The peer is down: the key goes to a hint instead.
    Down,
}

impl PeerQueue {
    pub fn new(peer: NodeId, cap: usize) -> PeerQueue {
        PeerQueue {
            peer,
            state: Mutex::new(QueueState {
                keys: LruCache::unbounded(),
                inflight: HashSet::new(),
                down: false,
                lagging: false,
                closed: false,
            }),
            notify: Notify::new(),
            cap: cap.max(1),
        }
    }

    /// Queue `key`. A key already queued keeps one place.
    pub fn push(&self, key: &Bytes) -> Pushed {
        let mut s = self.state.lock();
        if s.down {
            return Pushed::Down;
        }
        if s.keys.contains(key) {
            s.keys.promote(key);
            return Pushed::Queued;
        }
        s.keys.push(key.clone(), ());
        let out = if s.keys.len() > self.cap {
            s.lagging = true;
            match s.keys.pop_lru() {
                Some((old, ())) => Pushed::Overflow(old),
                None => Pushed::Queued,
            }
        } else {
            Pushed::Queued
        };
        drop(s);
        self.notify.notify_one();
        out
    }

    /// Take up to `max` of the oldest keys and mark them in flight.
    pub fn take(&self, max: usize) -> Vec<Bytes> {
        let mut s = self.state.lock();
        let mut out = Vec::new();
        while out.len() < max {
            let Some((k, ())) = s.keys.pop_lru() else {
                break;
            };
            s.inflight.insert(k.clone());
            out.push(k);
        }
        out
    }

    /// The batch in flight was acknowledged.
    pub fn acked(&self) {
        self.state.lock().inflight.clear();
    }

    /// The batch in flight failed. Its keys and the queued keys come back for hints, and the
    /// peer is down until a probe succeeds.
    pub fn failed(&self) -> Vec<Bytes> {
        let mut s = self.state.lock();
        s.down = true;
        let mut out: Vec<Bytes> = s.inflight.drain().collect();
        while let Some((k, ())) = s.keys.pop_lru() {
            out.push(k);
        }
        out
    }

    /// True once after the queue of a lagging peer drains below a quarter of its size.
    pub fn take_drained(&self) -> bool {
        let mut s = self.state.lock();
        if s.lagging && s.keys.len() < self.cap / 4 {
            s.lagging = false;
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

    /// The key is queued or in flight.
    pub fn holds(&self, key: &[u8]) -> bool {
        let s = self.state.lock();
        s.keys.contains(key) || s.inflight.contains(key)
    }

    /// Drop the queued keys for which `keep` is false.
    pub fn retain(&self, keep: impl Fn(&Bytes) -> bool) {
        let mut s = self.state.lock();
        let drop: Vec<Bytes> = s
            .keys
            .iter()
            .map(|(k, _)| k)
            .filter(|k| !keep(k))
            .cloned()
            .collect();
        for k in drop {
            s.keys.pop(&k);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn a_key_waits_once() {
        let q = PeerQueue::new(NodeId::from_u128(1), 8);
        assert_eq!(q.push(&k("a")), Pushed::Queued);
        assert_eq!(q.push(&k("a")), Pushed::Queued);
        assert_eq!(q.len(), 1);
        assert_eq!(q.take(10), vec![k("a")]);
        assert!(q.holds(b"a"));
        q.acked();
        assert!(!q.holds(b"a"));
    }

    #[test]
    fn overflow_drops_the_oldest_and_marks_lagging() {
        let q = PeerQueue::new(NodeId::from_u128(1), 4);
        for i in 0..4 {
            assert_eq!(q.push(&k(&i.to_string())), Pushed::Queued);
        }
        assert_eq!(q.push(&k("x")), Pushed::Overflow(k("0")));
        assert!(!q.take_drained());
        q.take(4);
        assert!(q.take_drained());
        assert!(!q.take_drained());
    }

    #[test]
    fn a_failed_batch_returns_every_key_and_marks_the_peer_down() {
        let q = PeerQueue::new(NodeId::from_u128(1), 8);
        q.push(&k("a"));
        q.push(&k("b"));
        q.take(1);
        let mut back = q.failed();
        back.sort();
        assert_eq!(back, vec![k("a"), k("b")]);
        assert_eq!(q.push(&k("c")), Pushed::Down);
        q.set_up();
        assert_eq!(q.push(&k("c")), Pushed::Queued);
    }

    #[test]
    fn queues_drop_keys_no_longer_owned() {
        let q = PeerQueue::new(NodeId::from_u128(1), 8);
        q.push(&k("keep"));
        q.push(&k("gone"));
        q.retain(|key| key.as_ref() == b"keep");
        assert_eq!(q.len(), 1);
        assert!(q.holds(b"keep"));
    }
}
