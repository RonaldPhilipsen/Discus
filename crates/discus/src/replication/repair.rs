//! Miss-driven read repair (spec section 8.2).
//!
//! A replica that has no entry for a key asks the primary, then the other owners, and stores
//! what it finds. A tombstone is an entry, so repair never brings back a deleted key, and a key
//! with a local tombstone is not repaired.

use std::sync::Arc;

use bytes::Bytes;
use discus_placement::NodeId;
use discus_transport::Lane;
use discus_wire::{DbKey, Message};

use crate::Node;

/// The nodes to ask before `key` of database `db` is served here, in order. Empty when the local
/// entry (or tombstone) is there, or when this node is the primary of a read and no handoff
/// toward it is incomplete.
///
/// While a handoff toward this node is incomplete, the previous owners come first, for reads
/// and writes. A write then applies to the handed-off value, so an `INCR` on a moved key does
/// not restart from zero (spec section 5.2).
pub fn sources(node: &Node, db: u32, key: &[u8], write: bool, owners: &[NodeId]) -> Vec<NodeId> {
    let Some(replicator) = node.replicator() else {
        return Vec::new();
    };
    // Only a replica read asks the other owners. Every other command asks only the handoff
    // sources, so without an incomplete handoff the answer is empty and the store lookup is
    // skipped.
    let replica_read = !write && owners.first() != Some(&node.id) && owners.contains(&node.id);
    if !replica_read && !replicator.handoff_pending() {
        return Vec::new();
    }
    if node.store.version(db, key).is_some() {
        return Vec::new();
    }
    let mut out = replicator.fallback_sources(node, key);
    if replica_read {
        for &o in owners {
            if o != node.id && !out.contains(&o) {
                out.push(o);
            }
        }
    }
    out
}

/// The keys of `keys` in database `db` to fetch before a multi-key part runs here, each with
/// its [`sources`].
pub fn plan<'a>(
    node: &Node,
    db: u32,
    keys: impl IntoIterator<Item = &'a Bytes>,
    write: bool,
) -> Vec<(Bytes, Vec<NodeId>)> {
    let Some(replicator) = node.replicator() else {
        return Vec::new();
    };
    // One look at the handoff state for the whole part, as for a single key.
    let replicas = !write && node.link().is_some_and(|l| l.read_from_replicas());
    if !replicas && !replicator.handoff_pending() {
        return Vec::new();
    }
    keys.into_iter()
        .filter_map(|k| {
            let found = sources(node, db, k, write, &node.owners(k));
            (!found.is_empty()).then(|| (k.clone(), found))
        })
        .collect()
}

/// [`fetch`] every key of `plan` at once.
pub async fn fetch_all(node: &Arc<Node>, db: u32, plan: Vec<(Bytes, Vec<NodeId>)>) {
    futures::future::join_all(
        plan.into_iter()
            .map(|(key, sources)| async move { fetch(node, db, &key, sources).await }),
    )
    .await;
}

/// Ask `sources` in order for `key` of database `db` and apply the first answer that has it. Each
/// request has the read forward timeout. A failed source is skipped.
pub async fn fetch(node: &Arc<Node>, db: u32, key: &Bytes, sources: Vec<NodeId>) {
    let (Some(link), Some(replicator)) = (node.link(), node.replicator()) else {
        return;
    };
    for source in sources {
        let build = |_: &_| {
            Ok(Message::Fetch {
                keys: vec![DbKey::new(db, key.clone())],
            })
        };
        let reply = node
            .request(source, Lane::Command, build, link.read_timeout(), false)
            .await;
        if let Ok(Message::Entries(batch)) = reply {
            let found = !batch.entries.is_empty();
            replicator.apply_entries(node, batch.entries);
            if found {
                return;
            }
        }
    }
}
