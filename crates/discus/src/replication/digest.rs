//! Promotion reconciliation (spec section 8.2).
//!
//! After an epoch removes a member, a node that became the primary of some of that member's
//! keys sends each other member one `DIGEST`. The digest has 256 bucket sums over those keys.
//! The other member computes the same sums over its own copies and returns the entries of every
//! bucket that differs. The new primary applies the newer ones. This runs once per epoch change
//! and has one round: what stays different refills through read repair.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use discus_placement::{Epoch, NodeId};
use discus_store::{refill_bucket, Entry, Store};
use discus_wire::{Batch, Message};
use xxhash_rust::xxh3::Xxh3;

/// The entries that one digest reply carries at most.
const REPLY_BYTES: u64 = 16 << 20;
/// Entries per store scan step.
const SCAN_STEP: usize = 1024;
/// Attempts while the other member has not adopted the new epoch yet.
const ATTEMPTS: usize = 5;

pub type Sums = Vec<u64>;

/// The members that were `NORMAL` in `old` and are not `NORMAL` in `new`.
pub fn removed(old: &Epoch, new: &Epoch) -> BTreeSet<NodeId> {
    old.normal()
        .filter(|m| !new.is_normal(m.id))
        .map(|m| m.id)
        .collect()
}

/// The contribution of one entry to its bucket sum.
fn hash(key: &[u8], e: &Entry) -> u64 {
    let mut h = Xxh3::new();
    h.update(key);
    h.update(&e.version.time().0.to_le_bytes());
    h.update(&e.version.origin().to_be_bytes());
    h.update(&[u8::from(e.tombstone)]);
    h.digest()
}

/// Walk the store and give `f` every entry whose key moved from a removed member to `primary`
/// between `old` and `new`, together with the other owners in `new` that share it.
fn walk_moved(
    store: &Store,
    old: &Epoch,
    new: &Epoch,
    rf: usize,
    primary: NodeId,
    mut f: impl FnMut(&Bytes, &Entry, &[NodeId]),
) {
    let gone = removed(old, new);
    let mut cursor = 0;
    loop {
        let moved = |k: &Bytes, _: &Entry| old.primary(k).is_some_and(|p| gone.contains(&p));
        let (next, entries) = store.scan_entries(cursor, SCAN_STEP, moved);
        for (k, e) in &entries {
            let owners = new.owners(k, rf);
            if owners.first() == Some(&primary) {
                f(k, e, &owners[1..]);
            }
        }
        if next == 0 {
            return;
        }
        cursor = next;
    }
}

/// The digests that `me` sends after `old` gave way to `new`: one per other owner that shares a
/// moved key.
pub fn outgoing(
    store: &Store,
    old: &Epoch,
    new: &Epoch,
    rf: usize,
    me: NodeId,
) -> HashMap<NodeId, Sums> {
    let mut out: HashMap<NodeId, Sums> = HashMap::new();
    walk_moved(store, old, new, rf, me, |k, e, others| {
        let b = usize::from(refill_bucket(k));
        let h = hash(k, e);
        for o in others {
            let sums = out.entry(*o).or_insert_with(|| vec![0; 256]);
            sums[b] = sums[b].wrapping_add(h);
        }
    });
    out
}

/// Answer a digest from `primary`: the entries of every bucket whose sum differs, among the
/// moved keys that this node `me` co-owns.
pub fn answer(
    store: &Store,
    old: &Epoch,
    new: &Epoch,
    rf: usize,
    primary: NodeId,
    me: NodeId,
    theirs: &[u64],
) -> Vec<(Bytes, Entry)> {
    let mut ours = vec![0u64; 256];
    walk_moved(store, old, new, rf, primary, |k, e, others| {
        if others.contains(&me) {
            let b = usize::from(refill_bucket(k));
            ours[b] = ours[b].wrapping_add(hash(k, e));
        }
    });
    let differs: Vec<bool> = (0..256)
        .map(|b| ours[b] != theirs.get(b).copied().unwrap_or(0))
        .collect();
    let mut out = Vec::new();
    let mut bytes = 0;
    walk_moved(store, old, new, rf, primary, |k, e, others| {
        if bytes < REPLY_BYTES && others.contains(&me) && differs[usize::from(refill_bucket(k))] {
            bytes += discus_store::estimate_size(k, e);
            out.push((k.clone(), e.clone()));
        }
    });
    out
}

/// Run the reconciliation of this node after `old` gave way to `new`.
pub async fn reconcile(node: Arc<crate::Node>, old: Arc<Epoch>, new: Arc<Epoch>) {
    let Some(replicator) = node.replicator().cloned() else {
        return;
    };
    let rf = node.config.replication_factor;
    let store = node.store.clone();
    let (o, n, me) = (old.clone(), new.clone(), node.id);
    let digests = match tokio::task::spawn_blocking(move || outgoing(&store, &o, &n, rf, me)).await
    {
        Ok(d) => d,
        Err(_) => return,
    };
    for (peer, sums) in digests {
        let message = Message::Digest {
            from: old.id(),
            to: new.id(),
            sums,
        };
        for attempt in 0..ATTEMPTS {
            match replicator.send(&node, peer, message.clone()).await {
                Ok(Message::Entries(Batch { entries, .. })) => {
                    let n = replicator.apply_entries(&node, entries);
                    tracing::info!(%peer, applied = n, "reconciled after a promotion");
                    break;
                }
                // The peer has not adopted the epoch yet.
                Ok(Message::BatchAck(_)) if attempt + 1 < ATTEMPTS => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Ok(_) => break,
                Err(e) => {
                    tracing::debug!(%peer, error = %e, "a digest exchange failed");
                    break;
                }
            }
        }
    }
}
