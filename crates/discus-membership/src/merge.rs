//! The merge rule for two independently formed clusters (spec section 6.3), and the join rate
//! limit against crash-looping pods (spec section 17).

use std::collections::HashMap;
use std::net::IpAddr;

use discus_placement::ClusterId;

use crate::Millis;

/// The cluster that wins a merge. Each side is `(cluster_id, NORMAL count)`. The cluster that the
/// lease names wins. Else the larger cluster wins, then the higher cluster ID.
pub fn winner(
    mine: (ClusterId, u32),
    theirs: (ClusterId, u32),
    lease: Option<ClusterId>,
) -> ClusterId {
    match lease {
        Some(l) if l == mine.0 => return mine.0,
        Some(l) if l == theirs.0 => return theirs.0,
        _ => {}
    }
    if (mine.1, mine.0) >= (theirs.1, theirs.0) {
        mine.0
    } else {
        theirs.0
    }
}

const BASE_MS: Millis = 1_000;
const MAX_MS: Millis = 60_000;
const RESET_MS: Millis = 300_000;

/// Admits a join from one source IP at most once per backoff. The backoff starts at 1 s and
/// doubles to 60 s for repeated joins from that IP, and resets after 5 min without a join. A
/// crash-looping container keeps its pod IP, so this slows its epoch churn. The limit is per
/// responder, not cluster-wide.
#[derive(Debug, Default)]
pub struct JoinLimiter {
    by_ip: HashMap<IpAddr, (u32, Millis)>,
}

impl JoinLimiter {
    /// `Err` carries the wait in milliseconds.
    pub fn admit(&mut self, ip: IpAddr, now: Millis) -> Result<(), u32> {
        self.by_ip
            .retain(|_, (_, last)| now.saturating_sub(*last) < RESET_MS);
        match self.by_ip.get_mut(&ip) {
            None => {
                self.by_ip.insert(ip, (1, now));
                Ok(())
            }
            Some((count, last)) => {
                let backoff = BASE_MS
                    .saturating_mul(1 << (*count - 1).min(16))
                    .min(MAX_MS);
                let waited = now.saturating_sub(*last);
                if waited >= backoff {
                    *count += 1;
                    *last = now;
                    Ok(())
                } else {
                    Err((backoff - waited) as u32)
                }
            }
        }
    }
}
