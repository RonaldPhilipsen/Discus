//! Active expiry: a sampled sweep, never a walk of a whole shard.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::Store;

impl Store {
    /// Sample up to `samples_per_shard` entries in each shard and remove the expired ones, and
    /// remove every expired tombstone. Returns the number of entries removed. Each shard keeps a
    /// cursor, so repeated sweeps cover the whole shard over time.
    ///
    /// The sample skips over the cursor position each call, which is O(cursor) per shard. A later
    /// plan can replace the map with an indexed one if this shows up in profiles.
    pub fn sweep_expired(&self, samples_per_shard: usize) -> usize {
        let now = self.clock.now_ms();
        let mut removed_total = 0;
        for shard in &self.shards {
            let mut guard = shard.inner.write();
            while let Some((deadline, key)) = guard.tombs.first().cloned() {
                if deadline > now {
                    break;
                }
                self.take(&mut guard, &key);
                removed_total += 1;
            }
            let start = shard.cursor.load(Ordering::Relaxed);
            let mut seen = 0;
            let mut expired: Vec<Bytes> = Vec::new();
            for (k, e) in guard.map.iter().skip(start).take(samples_per_shard) {
                seen += 1;
                if e.is_expired(now) {
                    expired.push(k.clone());
                }
            }
            // A short sample means the end of the shard: start over next time.
            let next = if seen < samples_per_shard {
                0
            } else {
                start + seen - expired.len()
            };
            shard.cursor.store(next, Ordering::Relaxed);
            for k in expired {
                if self.take(&mut guard, &k).is_some() {
                    removed_total += 1;
                }
            }
        }
        removed_total
    }
}

/// Run the sweep on a timer until `shutdown` is cancelled.
pub fn spawn_sweeper(
    store: Arc<Store>,
    interval: Duration,
    samples_per_shard: usize,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => { store.sweep_expired(samples_per_shard); }
            }
        }
    })
}
