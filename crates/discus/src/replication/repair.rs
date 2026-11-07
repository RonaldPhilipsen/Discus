//! Miss-driven read repair (spec section 8.2).
//!
//! A replica that has no entry for a key asks the primary, then the other owners, and stores
//! what it finds. A tombstone is an entry, so repair never brings back a deleted key, and a key
//! with a local tombstone is not repaired.

use std::sync::Arc;

use bytes::Bytes;
use discus_placement::NodeId;
use discus_wire::{Message, TraceContext};

use crate::Node;

/// The nodes to ask before `key` is served here, in order. Empty when the local entry (or
/// tombstone) is there, or when this node is the primary of a read.
pub fn sources(node: &Node, key: &[u8], write: bool) -> Vec<NodeId> {
    if node.replicator().is_none() || node.store.version(key).is_some() {
        return Vec::new();
    }
    if write {
        return Vec::new();
    }
    let owners = node.owners(key);
    if owners.first() == Some(&node.id) || !owners.contains(&node.id) {
        return Vec::new();
    }
    owners.into_iter().filter(|o| *o != node.id).collect()
}

/// Ask `sources` in order for `key` and apply the first answer that has it. Each request has
/// the read forward timeout. A failed source is skipped.
pub async fn fetch(node: &Arc<Node>, key: &Bytes, sources: Vec<NodeId>) {
    let (Some(link), Some(replicator)) = (node.link(), node.replicator()) else {
        return;
    };
    for source in sources {
        let Some(addr) = node.epochs().current().member(source).map(|m| m.addr) else {
            continue;
        };
        let reply = link
            .transport
            .request(
                source,
                addr,
                Message::Fetch {
                    keys: vec![key.clone()],
                },
                TraceContext::default(),
                link.read_timeout,
                false,
            )
            .await;
        if let Ok(frame) = reply {
            if let Message::Entries(batch) = frame.message {
                let found = !batch.entries.is_empty();
                replicator.apply_entries(node, batch.entries);
                if found {
                    return;
                }
            }
        }
    }
}
