//! The index rule of Redis ranges such as `LRANGE`, `LTRIM`, and `ZRANGE`.

use std::ops::RangeInclusive;

/// The positions that `start` and `stop` select in a collection of `len` members. Both bounds
/// are inclusive, and a negative bound counts from the end. `None` selects nothing.
pub fn rank_range(len: usize, start: i64, stop: i64) -> Option<RangeInclusive<usize>> {
    let len = len as i64;
    let start = if start < 0 {
        len.saturating_add(start).max(0)
    } else {
        start
    };
    let stop = if stop < 0 {
        len.saturating_add(stop)
    } else {
        stop.min(len - 1)
    };
    (start <= stop && start < len).then_some(start as usize..=stop as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounds_follow_redis() {
        assert_eq!(rank_range(5, 0, -1), Some(0..=4));
        assert_eq!(rank_range(5, -100, 100), Some(0..=4));
        assert_eq!(rank_range(5, 1, -2), Some(1..=3));
        assert_eq!(rank_range(5, 5, 9), None);
        assert_eq!(rank_range(5, 3, 1), None);
        assert_eq!(rank_range(5, 0, -6), None);
        assert_eq!(rank_range(0, 0, -1), None);
        assert_eq!(rank_range(5, i64::MIN, i64::MAX), Some(0..=4));
    }
}
