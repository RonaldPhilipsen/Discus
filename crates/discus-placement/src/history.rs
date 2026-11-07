//! The epoch history: the current epoch, at least the last [`MIN_HISTORY`], and every epoch that
//! a pending handoff pins.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::{ClusterId, Epoch, EpochError, EpochId, Member, NodeId};

/// The history keeps at least this many recent epochs that no handoff pins.
pub const MIN_HISTORY: usize = 4;

/// The outcome of offering an epoch to the history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Adopt {
    /// The epoch is higher than the current one and is now current.
    Newer,
    /// The epoch is not higher than the current one. Nothing changed.
    Stale,
}

#[derive(Debug, Clone)]
pub struct EpochHistory {
    cluster_id: ClusterId,
    epochs: BTreeMap<EpochId, Arc<Epoch>>,
    pins: HashMap<EpochId, usize>,
    max_generation: u64,
}

impl EpochHistory {
    pub fn new(bootstrap: Epoch) -> EpochHistory {
        let id = bootstrap.id();
        EpochHistory {
            cluster_id: bootstrap.cluster_id(),
            max_generation: id.generation,
            epochs: BTreeMap::from([(id, Arc::new(bootstrap))]),
            pins: HashMap::new(),
        }
    }

    pub fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    /// The highest epoch. The history is never empty.
    pub fn current(&self) -> &Arc<Epoch> {
        self.epochs
            .values()
            .next_back()
            .expect("the history always holds the current epoch")
    }

    /// The epoch before the current one, if the history still holds it.
    pub fn previous(&self) -> Option<&Arc<Epoch>> {
        self.epochs.values().nth_back(1)
    }

    pub fn get(&self, id: EpochId) -> Option<&Arc<Epoch>> {
        self.epochs.get(&id)
    }

    /// Every held epoch ID in ascending order.
    pub fn ids(&self) -> Vec<EpochId> {
        self.epochs.keys().copied().collect()
    }

    /// Offer an epoch. A higher epoch becomes current. A lower or equal one is ignored. An epoch
    /// of another cluster is an error and changes nothing.
    pub fn adopt(&mut self, epoch: Epoch) -> Result<Adopt, EpochError> {
        if epoch.cluster_id() != self.cluster_id {
            return Err(EpochError::ForeignCluster {
                ours: self.cluster_id,
                theirs: epoch.cluster_id(),
            });
        }
        let id = epoch.id();
        self.max_generation = self.max_generation.max(id.generation);
        if id <= self.current().id() {
            return Ok(Adopt::Stale);
        }
        self.epochs.insert(id, Arc::new(epoch));
        self.prune();
        Ok(Adopt::Newer)
    }

    /// The generation for a new epoch: the highest generation seen plus one.
    pub fn next_generation(&self) -> u64 {
        self.max_generation + 1
    }

    /// A new epoch of this cluster with the next generation and `origin` as its creator.
    pub fn propose(&self, origin: NodeId, members: Vec<Member>) -> Result<Epoch, EpochError> {
        Epoch::new(
            EpochId {
                generation: self.next_generation(),
                origin,
            },
            self.cluster_id,
            members,
        )
    }

    /// Keep `id` until a matching `unpin`. Pins are counted.
    pub fn pin(&mut self, id: EpochId) -> Result<(), EpochError> {
        if !self.epochs.contains_key(&id) {
            return Err(EpochError::Unknown(id));
        }
        *self.pins.entry(id).or_insert(0) += 1;
        Ok(())
    }

    /// Release one pin of `id`. An epoch without pins is pruned like any other.
    pub fn unpin(&mut self, id: EpochId) {
        if let Some(count) = self.pins.get_mut(&id) {
            *count -= 1;
            if *count == 0 {
                self.pins.remove(&id);
            }
        }
        self.prune();
    }

    /// The oldest pinned epoch, where a handoff scan starts.
    pub fn oldest_pinned(&self) -> Option<EpochId> {
        self.pins.keys().min().copied()
    }

    /// Drop the oldest unpinned epochs until at most `MIN_HISTORY` unpinned ones remain. The
    /// current epoch is the newest, so it is never dropped.
    fn prune(&mut self) {
        let unpinned: Vec<EpochId> = self
            .epochs
            .keys()
            .filter(|id| !self.pins.contains_key(id))
            .copied()
            .collect();
        let excess = unpinned.len().saturating_sub(MIN_HISTORY);
        for id in &unpinned[..excess] {
            self.epochs.remove(id);
        }
    }
}
