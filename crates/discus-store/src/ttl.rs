//! Active expiry: a sampled sweep, never a walk of a whole shard.

use std::future::Future;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::store::SweepCursor;
use crate::Store;

/// The most buckets that one sweep call reads per sample that it asks for.
const BUCKETS_PER_SAMPLE: usize = 16;

impl Store {
    /// Sample up to `samples_per_shard` entries in each shard and remove the expired ones, and
    /// remove every expired tombstone. Both cover every database. Returns the number of entries
    /// removed. Each shard keeps a cursor of a database and a bucket index, so repeated sweeps
    /// cover the whole shard over time, and a call reads only the buckets of its sample.
    ///
    /// A removal moves no other entry, so the cursor stays in place. A map that grows during a
    /// round can move an entry behind the cursor, and the next round reads it. A sparse map stops
    /// a call after `BUCKETS_PER_SAMPLE` buckets per sample, and the next call continues there.
    pub fn sweep_expired(&self, samples_per_shard: usize) -> usize {
        let now = self.clock.now_ms();
        let mut removed_total = 0;
        for shard in &self.shards {
            let mut guard = shard.inner.write();
            for (_, d) in &mut guard.dbs {
                while let Some((deadline, key)) = d.tombs.first().cloned() {
                    if deadline > now {
                        break;
                    }
                    self.take_from(d, &key);
                    removed_total += 1;
                }
            }
            let cursor = guard.sweep;
            let limit = samples_per_shard.saturating_mul(BUCKETS_PER_SAMPLE);
            let (mut seen, mut read) = (0, 0);
            let mut next = None;
            let mut expired: Vec<(u32, Bytes)> = Vec::new();
            'walk: for (db, d) in guard.dbs.iter().filter(|(db, _)| *db >= cursor.db) {
                let from = if *db == cursor.db { cursor.bucket } else { 0 };
                for bucket in from..d.map.num_buckets() {
                    if seen == samples_per_shard || read == limit {
                        next = Some(SweepCursor { db: *db, bucket });
                        break 'walk;
                    }
                    read += 1;
                    if let Some((k, e)) = d.map.bucket(bucket) {
                        seen += 1;
                        if e.is_expired(now) {
                            expired.push((*db, k.clone()));
                        }
                    }
                }
            }
            // The end of the shard: start over next time.
            guard.sweep = next.unwrap_or_default();
            for (db, k) in expired {
                if self.take(&mut guard, db, &k).is_some() {
                    removed_total += 1;
                }
            }
        }
        removed_total
    }
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

/// Give `f` every event of `rx` until `stop` is cancelled or the sender is gone. Events that a
/// lagging receiver lost are skipped.
pub async fn on_events<T: Clone>(
    mut rx: broadcast::Receiver<T>,
    stop: CancellationToken,
    mut f: impl FnMut(T),
) {
    loop {
        match stop.run_until_cancelled(rx.recv()).await {
            None | Some(Err(RecvError::Closed)) => return,
            Some(Err(RecvError::Lagged(_))) => {}
            Some(Ok(event)) => f(event),
        }
    }
}
