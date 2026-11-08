//! Active expiry: a sampled sweep, never a walk of a whole shard.

use std::future::Future;
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
    every(interval, shutdown, move || {
        store.sweep_expired(samples_per_shard);
        std::future::ready(())
    })
}

/// Spawn a task that runs `f` every `period` until `stop` is cancelled. Ticks that a slow `f`
/// missed are skipped, so `f` never runs back to back to catch up.
pub fn every<F, Fut>(period: Duration, stop: CancellationToken, mut f: F) -> JoinHandle<()>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send,
{
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(period);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = ticker.tick() => f().await,
            }
        }
    })
}
