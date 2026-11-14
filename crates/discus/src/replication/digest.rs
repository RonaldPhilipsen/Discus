//! Promotion reconciliation (spec section 8.2).
//!
//! After an epoch removes a member, a node that became the primary of some of that member's
//! keys sends each other member one `DIGEST` per database. The digest has 256 bucket sums over
//! the keys of that database.
//! The other member computes the same sums over its own copies and returns the entries of every
//! bucket that differs. The new primary applies the newer ones. This runs once per epoch change
//! and has one round: what stays different refills through read repair.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use discus_placement::{Epoch, NodeId};
use discus_store::{refill_bucket, DbKey, Entry, EntryCursor, Store};
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
/// between `old` and `new`, with its database and the other owners in `new` that share it.
/// `only` limits the walk to one database.
fn walk_moved(
    store: &Store,
    old: &Epoch,
    new: &Epoch,
    rf: usize,
    primary: NodeId,
    only: Option<u32>,
    mut f: impl FnMut(u32, &Bytes, &Entry, &[NodeId]),
) {
    let gone = removed(old, new);
    let mut cursor = EntryCursor {
        db: only.unwrap_or(0),
        pos: 0,
    };
    loop {
        let moved = |db: u32, k: &Bytes, _: &Entry| {
            only.is_none_or(|o| o == db) && old.primary(k).is_some_and(|p| gone.contains(&p))
        };
        let (next, entries) = store.scan_all_entries(cursor, SCAN_STEP, moved);
        for (k, e) in &entries {
            let owners = new.owners(&k.key, rf);
            if owners.first() == Some(&primary) {
                f(k.db, &k.key, e, &owners[1..]);
            }
        }
        match next {
            Some(next) if only.is_none_or(|o| o == next.db) => cursor = next,
            _ => return,
        }
    }
}

/// The digests that `me` sends after `old` gave way to `new`: one for each other owner and
/// database that share a moved key.
pub fn outgoing(
    store: &Store,
    old: &Epoch,
    new: &Epoch,
    rf: usize,
    me: NodeId,
) -> HashMap<(NodeId, u32), Sums> {
    let mut out: HashMap<(NodeId, u32), Sums> = HashMap::new();
    walk_moved(store, old, new, rf, me, None, |db, k, e, others| {
        let b = usize::from(refill_bucket(k));
        let h = hash(k, e);
        for o in others {
            let sums = out.entry((*o, db)).or_insert_with(|| vec![0; 256]);
            sums[b] = sums[b].wrapping_add(h);
        }
    });
    out
}

/// Answer a digest of database `db` from `primary`: the entries of every bucket whose sum
/// differs, among the moved keys of `db` that this node `me` co-owns.
#[allow(clippy::too_many_arguments)]
pub fn answer(
    store: &Store,
    old: &Epoch,
    new: &Epoch,
    rf: usize,
    primary: NodeId,
    me: NodeId,
    db: u32,
    theirs: &[u64],
) -> Vec<(DbKey, Entry)> {
    let mut ours = vec![0u64; 256];
    walk_moved(store, old, new, rf, primary, Some(db), |_, k, e, others| {
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
    walk_moved(
        store,
        old,
        new,
        rf,
        primary,
        Some(db),
        |db, k, e, others| {
            if bytes < REPLY_BYTES && others.contains(&me) && differs[usize::from(refill_bucket(k))]
            {
                bytes += discus_store::estimate_size(k, e);
                out.push((DbKey::new(db, k.clone()), e.clone()));
            }
        },
    );
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
    for ((peer, db), sums) in digests {
        let message = Message::Digest {
            from: old.id(),
            to: new.id(),
            db,
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

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use discus_placement::{
        ClusterId, EpochId, ManualClock, Member, MemberState, Version, Weight, NTP64,
    };
    use discus_store::{StoreConfig, Value};

    use super::*;

    const A: NodeId = NodeId::from_u128(1);
    const B: NodeId = NodeId::from_u128(2);
    const C: NodeId = NodeId::from_u128(3);
    const RF: usize = 2;

    fn member(id: NodeId) -> Member {
        let addr = SocketAddr::from(([10, 0, 0, id.as_u128() as u8], 7000));
        Member {
            id,
            addr,
            rpc_addr: addr,
            client_addr: None,
            client_host: None,
            state: MemberState::Normal,
            weight: Weight::default(),
        }
    }

    fn epoch(generation: u64, ids: &[NodeId]) -> Epoch {
        let id = EpochId {
            generation,
            origin: ids[0],
        };
        Epoch::new(
            id,
            ClusterId::from_u128(1),
            ids.iter().map(|i| member(*i)).collect(),
        )
        .unwrap()
    }

    /// `C` leaves: `old` has `A`, `B`, and `C`, and `new` has `A` and `B`.
    fn epochs() -> (Epoch, Epoch) {
        (epoch(1, &[A, B, C]), epoch(2, &[A, B]))
    }

    fn store() -> Store {
        Store::new(StoreConfig::default(), Arc::new(ManualClock::new(1)))
    }

    fn entry(t: u64) -> Entry {
        let mut e = Entry::new(Value::Str("v".into()), None);
        e.version = Version::new(NTP64(t), A);
        e
    }

    /// Keys that moved from `C` to the primary `A`.
    fn moved_keys(old: &Epoch, new: &Epoch) -> Vec<Bytes> {
        let keys: Vec<Bytes> = (0..400)
            .map(|i| Bytes::from(format!("k{i}")))
            .filter(|k| old.primary(k) == Some(C) && new.owners(k, RF).first() == Some(&A))
            .collect();
        assert!(keys.len() > 1);
        keys
    }

    #[test]
    fn the_digest_of_database_zero_is_the_old_digest() {
        let (old, new) = epochs();
        let s = store();
        let mut want = vec![0u64; 256];
        for (i, k) in moved_keys(&old, &new).iter().enumerate() {
            let e = entry(10 + i as u64);
            let b = usize::from(refill_bucket(k));
            want[b] = want[b].wrapping_add(hash(k, &e));
            s.apply(0, k, e);
        }
        let out = outgoing(&s, &old, &new, RF, A);
        assert_eq!(out.keys().collect::<Vec<_>>(), vec![&(B, 0)]);
        assert_eq!(out[&(B, 0)], want);
    }

    #[test]
    fn each_database_gets_its_own_digest() {
        let (old, new) = epochs();
        let key = moved_keys(&old, &new).swap_remove(0);
        let (a, b) = (store(), store());
        for db in [0, 2] {
            a.apply(db, &key, entry(10));
            b.apply(db, &key, entry(20 + u64::from(db)));
        }
        let sums = outgoing(&a, &old, &new, RF, A);
        assert_eq!(sums.len(), 2);
        let got = answer(&b, &old, &new, RF, A, B, 2, &sums[&(B, 2)]);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0, DbKey::new(2, key));
        assert_eq!(got[0].1.version, entry(22).version);
    }
}
