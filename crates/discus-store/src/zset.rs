//! A sorted set: members ordered by score, then by member bytes.

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::ops::{Bound, RangeBounds};

use bytes::Bytes;

use crate::entry::ENTRY_OVERHEAD;

/// A score with a total order, so it can key a `BTreeSet`. NaN never enters a `ZSet`.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Score(f64);

impl Eq for Score {}

impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Score {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ZSet {
    scores: HashMap<Bytes, f64>,
    order: BTreeSet<(Score, Bytes)>,
}

impl ZSet {
    pub fn new() -> ZSet {
        ZSet::default()
    }

    /// Insert or update a member. Returns true when the member is new.
    pub fn insert(&mut self, member: Bytes, score: f64) -> bool {
        match self.scores.insert(member.clone(), score) {
            Some(old) => {
                self.order.remove(&(Score(old), member.clone()));
                self.order.insert((Score(score), member));
                false
            }
            None => {
                self.order.insert((Score(score), member));
                true
            }
        }
    }

    pub fn remove(&mut self, member: &[u8]) -> bool {
        match self.scores.remove_entry(member) {
            Some((m, score)) => {
                self.order.remove(&(Score(score), m));
                true
            }
            None => false,
        }
    }

    pub fn score(&self, member: &[u8]) -> Option<f64> {
        self.scores.get(member).copied()
    }

    pub fn len(&self) -> usize {
        self.scores.len()
    }

    pub fn is_empty(&self) -> bool {
        self.scores.is_empty()
    }

    /// Members by rank. `start` and `stop` follow Redis: inclusive, and negative counts from the
    /// end. With `reverse`, rank 0 is the highest score.
    pub fn range(&self, start: i64, stop: i64, reverse: bool) -> Vec<(&Bytes, f64)> {
        let len = self.len() as i64;
        let start = if start < 0 {
            (len + start).max(0)
        } else {
            start
        };
        let stop = if stop < 0 {
            len + stop
        } else {
            stop.min(len - 1)
        };
        if start > stop || start >= len {
            return Vec::new();
        }
        let take = (stop - start + 1) as usize;
        let iter: Box<dyn Iterator<Item = &(Score, Bytes)>> = if reverse {
            Box::new(self.order.iter().rev())
        } else {
            Box::new(self.order.iter())
        };
        iter.skip(start as usize)
            .take(take)
            .map(|(s, m)| (m, s.0))
            .collect()
    }

    /// Members whose score lies between the bounds, lowest first.
    pub fn range_by_score(&self, min: Bound<f64>, max: Bound<f64>) -> Vec<(&Bytes, f64)> {
        self.scored(min, max).collect()
    }

    pub fn count(&self, min: Bound<f64>, max: Bound<f64>) -> usize {
        self.scored(min, max).count()
    }

    fn scored(&self, min: Bound<f64>, max: Bound<f64>) -> impl Iterator<Item = (&Bytes, f64)> {
        let from = match min {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Included(m) | Bound::Excluded(m) => Bound::Included((Score(m), Bytes::new())),
        };
        self.order
            .range((from, Bound::Unbounded))
            .map(|(s, m)| (m, s.0))
            .filter(move |(_, s)| (min, Bound::Unbounded).contains(s))
            .take_while(move |(_, s)| (Bound::Unbounded, max).contains(s))
    }

    /// Approximate bytes that the members cost.
    pub fn byte_size(&self) -> u64 {
        self.scores
            .keys()
            .map(|m| m.len() as u64 + 8 + ENTRY_OVERHEAD)
            .sum()
    }
}
