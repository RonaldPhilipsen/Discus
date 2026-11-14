//! Failed authentication attempts per source (spec §14.2).
//!
//! A source is one IPv4 address or one IPv6 /64 network, because an IPv6 client normally controls
//! every address in its /64.
//!
//! Hand-written because `governor` consumes a cell on every check. Here a check never counts,
//! only a failure does, and a blocked source is refused before its token is checked.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::settings::Live;

/// The most sources the limiter tracks, which bounds its memory. A client with an IPv6 /48 has
/// this many /64 networks, so a full table is within reach of an attacker. Beyond it, new sources
/// share the overflow bucket. No entry is evicted before its window ends, so a flood of new
/// sources cannot free a blocked source.
const MAX_SOURCES: usize = 65_536;

/// Window start and failure count.
type Bucket = (Instant, u32);

struct State {
    sources: HashMap<IpAddr, Bucket>,
    /// The failures of every source that did not fit in the table.
    overflow: Bucket,
}

pub struct FailureLimiter {
    live_settings: Arc<Live>,
    capacity: usize,
    state: Mutex<State>,
}

/// The key of `ip` in the table: the IPv4 address, also for the IPv4-mapped form, or the IPv6 /64.
fn source(ip: IpAddr) -> IpAddr {
    match ip.to_canonical() {
        IpAddr::V6(v6) => IpAddr::V6(Ipv6Addr::from_bits(v6.to_bits() & !u128::from(u64::MAX))),
        v4 => v4,
    }
}

impl FailureLimiter {
    pub fn new(live_settings: Arc<Live>) -> FailureLimiter {
        FailureLimiter::with_capacity(live_settings, MAX_SOURCES)
    }

    fn with_capacity(live_settings: Arc<Live>, capacity: usize) -> FailureLimiter {
        FailureLimiter {
            live_settings,
            capacity,
            state: Mutex::new(State {
                sources: HashMap::new(),
                overflow: (Instant::now(), 0),
            }),
        }
    }

    /// True when the source of `ip` reached the limit in its current window. A source that is not
    /// in the table gets the verdict of the overflow bucket, so a full table fails closed.
    pub fn blocked(&self, ip: IpAddr) -> bool {
        let Ok(state) = self.state.lock() else {
            return true;
        };
        let settings = self.live_settings.get();
        let max = settings.auth_max_failures_per_source.max(1);
        let (start, n) = state.sources.get(&source(ip)).unwrap_or(&state.overflow);
        start.elapsed() < settings.auth_failure_window && *n >= max
    }

    /// Count one failed attempt from the source of `ip`.
    pub fn record(&self, ip: IpAddr) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let window = self.live_settings.get().auth_failure_window;
        let key = source(ip);
        let State { sources, overflow } = &mut *state;
        if sources.len() >= self.capacity && !sources.contains_key(&key) {
            sources.retain(|_, (start, _)| start.elapsed() < window);
        }
        let bucket = if sources.len() < self.capacity || sources.contains_key(&key) {
            sources.entry(key).or_insert((Instant::now(), 0))
        } else {
            overflow
        };
        if bucket.0.elapsed() >= window {
            *bucket = (Instant::now(), 0);
        }
        bucket.1 = bucket.1.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::config::Config;
    use crate::settings::Reloadable;

    fn settings(max: u32, window: Duration) -> Reloadable {
        Reloadable {
            auth_max_failures_per_source: max,
            auth_failure_window: window,
            ..Reloadable::from_config(&Config::parse_from(["discus"]).unwrap())
        }
    }

    #[test]
    fn a_source_is_blocked_at_the_limit_and_freed_after_the_window() {
        let live_settings = Live::new(settings(2, Duration::from_secs(600)));
        let l = FailureLimiter::new(live_settings.clone());
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        l.record(ip);
        assert!(!l.blocked(ip));
        l.record(ip);
        assert!(l.blocked(ip));
        assert!(!l.blocked("10.0.0.2".parse().unwrap()));
        // A zero window has ended for every attempt, without a wait on the clock.
        live_settings.apply(settings(2, Duration::ZERO));
        assert!(!l.blocked(ip));
    }

    #[test]
    fn a_lower_limit_applies_at_once() {
        let window = Duration::from_secs(600);
        let live_settings = Live::new(settings(20, window));
        let l = FailureLimiter::new(live_settings.clone());
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        for _ in 0..5 {
            l.record(ip);
        }
        assert!(!l.blocked(ip));
        live_settings.apply(settings(5, window));
        assert!(l.blocked(ip));
    }

    fn limiter(max: u32, capacity: usize) -> FailureLimiter {
        let live_settings = Live::new(settings(max, Duration::from_secs(600)));
        FailureLimiter::with_capacity(live_settings, capacity)
    }

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn addresses_in_one_ipv6_64_share_a_budget() {
        let l = limiter(2, MAX_SOURCES);
        l.record(ip("2001:db8:0:1::1"));
        l.record(ip("2001:db8:0:1:ffff:ffff:ffff:ffff"));
        assert!(l.blocked(ip("2001:db8:0:1::1")));
        assert!(l.blocked(ip("2001:db8:0:1::2")));
    }

    #[test]
    fn two_ipv6_64s_have_separate_budgets() {
        let l = limiter(2, MAX_SOURCES);
        l.record(ip("2001:db8:0:1::1"));
        l.record(ip("2001:db8:0:1::1"));
        assert!(!l.blocked(ip("2001:db8:0:2::1")));
        l.record(ip("2001:db8:0:2::1"));
        assert!(!l.blocked(ip("2001:db8:0:2::1")));
    }

    #[test]
    fn an_ipv4_mapped_address_shares_with_its_ipv4_form() {
        let l = limiter(2, MAX_SOURCES);
        l.record(ip("::ffff:10.0.0.1"));
        l.record(ip("10.0.0.1"));
        assert!(l.blocked(ip("10.0.0.1")));
        assert!(l.blocked(ip("::ffff:10.0.0.1")));
        assert!(!l.blocked(ip("10.0.0.2")));
        assert!(!l.blocked(ip("::ffff:10.0.0.2")));
    }

    #[test]
    fn a_full_table_fails_closed_for_new_sources() {
        let l = limiter(2, 2);
        l.record(ip("10.0.0.1"));
        l.record(ip("10.0.0.2"));

        l.record(ip("10.0.0.3"));
        assert!(!l.blocked(ip("10.0.0.3")));
        l.record(ip("10.0.0.4"));
        assert!(l.blocked(ip("10.0.0.3")));
        assert!(l.blocked(ip("10.0.0.4")));
        assert!(l.blocked(ip("10.0.0.5")));

        assert!(!l.blocked(ip("10.0.0.1")));
        assert!(!l.blocked(ip("10.0.0.2")));
    }

    #[test]
    fn a_flood_of_new_sources_does_not_free_a_blocked_source() {
        let l = limiter(2, 2);
        l.record(ip("10.0.0.1"));
        l.record(ip("10.0.0.1"));
        l.record(ip("10.0.0.2"));
        for last in 3..=50 {
            l.record(ip(&format!("10.0.1.{last}")));
        }
        assert!(l.blocked(ip("10.0.0.1")));
        assert!(!l.blocked(ip("10.0.0.2")));
    }

    #[test]
    fn a_full_table_takes_new_sources_again_after_the_window() {
        let live_settings = Live::new(settings(3, Duration::from_secs(600)));
        let l = FailureLimiter::with_capacity(live_settings.clone(), 1);
        l.record(ip("10.0.0.1"));
        l.record(ip("10.0.0.2"));
        // A zero window has ended for every attempt, without a wait on the clock.
        live_settings.apply(settings(3, Duration::ZERO));
        l.record(ip("10.0.0.2"));
        live_settings.apply(settings(3, Duration::from_secs(600)));
        l.record(ip("10.0.0.2"));
        l.record(ip("10.0.0.2"));
        assert!(l.blocked(ip("10.0.0.2")));
        // The overflow bucket holds one failure, so 10.0.0.2 has its own entry now.
        assert!(!l.blocked(ip("10.0.0.3")));
    }
}
