//! Failed authentication attempts per source address (spec §14.2).
//!
//! Hand-written because `governor` consumes a cell on every check. Here a check never counts,
//! only a failure does, and a blocked source is refused before its token is checked.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// The most sources the limiter tracks. Beyond it, new sources are not tracked until old windows
/// end, so an attacker cannot evict a blocked source by flooding with new addresses.
const MAX_SOURCES: usize = 65_536;

pub struct FailureLimiter {
    max: u32,
    window: Duration,
    sources: Mutex<HashMap<IpAddr, (Instant, u32)>>,
}

impl FailureLimiter {
    pub fn new(max: u32, window: Duration) -> FailureLimiter {
        FailureLimiter {
            max,
            window,
            sources: Mutex::new(HashMap::new()),
        }
    }

    /// True when `ip` reached the limit in its current window.
    pub fn blocked(&self, ip: IpAddr) -> bool {
        let Ok(map) = self.sources.lock() else {
            return true;
        };
        match map.get(&ip) {
            Some((start, n)) => start.elapsed() < self.window && *n >= self.max,
            None => false,
        }
    }

    /// Count one failed attempt from `ip`.
    pub fn record(&self, ip: IpAddr) {
        let Ok(mut map) = self.sources.lock() else {
            return;
        };
        if map.len() >= MAX_SOURCES && !map.contains_key(&ip) {
            let window = self.window;
            map.retain(|_, (start, _)| start.elapsed() < window);
            if map.len() >= MAX_SOURCES {
                return;
            }
        }
        let entry = map.entry(ip).or_insert((Instant::now(), 0));
        if entry.0.elapsed() >= self.window {
            *entry = (Instant::now(), 0);
        }
        entry.1 = entry.1.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_source_is_blocked_at_the_limit_and_freed_after_the_window() {
        let l = FailureLimiter::new(2, Duration::from_millis(50));
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        l.record(ip);
        assert!(!l.blocked(ip));
        l.record(ip);
        assert!(l.blocked(ip));
        assert!(!l.blocked("10.0.0.2".parse().unwrap()));
        std::thread::sleep(Duration::from_millis(60));
        assert!(!l.blocked(ip));
    }
}
