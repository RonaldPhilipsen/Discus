//! The numbered databases: the count that this node holds, the count that the cluster serves, and
//! the gate that keeps every database above 0 closed until every member supports them.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use parking_lot::Mutex;

use crate::command::CommandError;

pub const OUT_OF_RANGE: &str = "ERR DB index is out of range";
pub const NEEDS_UPGRADE: &str = "ERR multiple databases need every node to support them";

/// `first_drop` remembers at most this many databases. A peer can name any `u32`, so the set has
/// a bound. Past it, a drop is no longer logged.
const DROP_LOG_LIMIT: usize = 1024;

pub struct Databases {
    /// The databases that this node holds: 0 up to this count, exclusive.
    local: AtomicU32,
    /// The databases that clients can select: the smallest count of the members.
    cluster: AtomicU32,
    /// Every member supports numbered databases, and so does this build.
    multi_db: AtomicBool,
    /// The lowest count that a drop applied on this node, `u32::MAX` for none. Inbound data at
    /// or above it is refused: nodes end the dwell of a lower count at different times, so a
    /// peer that still serves the old count can send entries that nobody could read here. A
    /// later raise moves it up to the new count, and a count that reaches `local` clears it.
    floor: AtomicU32,
    /// `--lower-databases`: a joiner keeps its own count instead of the larger cluster count.
    lower: bool,
    dropped: Mutex<HashSet<u32>>,
    /// Set by [`Databases::pin_cluster`]: the cluster count that every view takes, 0 for none.
    #[cfg(feature = "testing")]
    pinned_cluster: AtomicU32,
}

impl Databases {
    pub fn new(local: u32, lower: bool) -> Databases {
        Databases {
            local: AtomicU32::new(local),
            cluster: AtomicU32::new(local),
            multi_db: AtomicBool::new(true),
            floor: AtomicU32::new(u32::MAX),
            lower,
            dropped: Mutex::default(),
            #[cfg(feature = "testing")]
            pinned_cluster: AtomicU32::new(0),
        }
    }

    /// A node with membership: it serves database 0 only, with the gate closed, until the first
    /// membership view. Until then it does not know the cluster count, and a write to a database
    /// that the cluster does not serve would be deleted by the drop that follows.
    pub fn awaiting_view(local: u32, lower: bool) -> Databases {
        let dbs = Databases::new(local, lower);
        dbs.cluster.store(1, Ordering::Relaxed);
        dbs.multi_db.store(false, Ordering::Relaxed);
        dbs
    }

    pub fn local(&self) -> u32 {
        self.local.load(Ordering::Relaxed)
    }

    pub fn cluster(&self) -> u32 {
        self.cluster.load(Ordering::Relaxed)
    }

    /// This node holds database `db` and accepts data for it.
    pub fn holds(&self, db: u32) -> bool {
        db < self.local() && db < self.floor.load(Ordering::Relaxed)
    }

    pub fn multi_db(&self) -> bool {
        self.multi_db.load(Ordering::Relaxed)
    }

    pub fn lower(&self) -> bool {
        self.lower
    }

    /// Keep the larger of the local count and `n`. True if the count grew.
    pub fn raise_local(&self, n: u32) -> bool {
        self.local.fetch_max(n, Ordering::Relaxed) < n
    }

    /// Take the membership view. `Some(cluster)` if the cluster count dropped: the floor then
    /// refuses inbound data at or above it before the caller drops the databases. The count stays
    /// at least 1, so database 0 always serves.
    pub fn set_view(&self, cluster: u32, multi_db: bool) -> Option<u32> {
        #[cfg(feature = "testing")]
        let cluster = match self.pinned_cluster.load(Ordering::Relaxed) {
            0 => cluster,
            pinned => pinned,
        };
        let cluster = cluster.max(1);
        // A reader can see the new gate with the old count for a moment. That is harmless:
        // `prepare` checks the count again on every data command, so a database above the new
        // count is refused once the count store lands.
        self.multi_db.store(multi_db, Ordering::Relaxed);
        let old = self.cluster.swap(cluster, Ordering::Relaxed);
        if cluster < old {
            self.floor.fetch_min(cluster, Ordering::Relaxed);
            return Some(cluster);
        }
        // A raise moves the floor up to the new count only: the databases between the new count
        // and `local` were dropped too, and clients still cannot reach them.
        if cluster > self.floor.load(Ordering::Relaxed) {
            let floor = if cluster >= self.local() {
                u32::MAX
            } else {
                cluster
            };
            self.floor.store(floor, Ordering::Relaxed);
        }
        None
    }

    /// The database of `SELECT <db>`. The range check comes first, as in Valkey, so a client sees
    /// the same error with or without the gate.
    pub fn check_select(&self, db: i64) -> Result<u32, CommandError> {
        if db < 0 || db >= i64::from(self.cluster()) {
            return Err(CommandError::Custom(OUT_OF_RANGE.into()));
        }
        if db > 0 {
            self.check_gate()?;
        }
        Ok(db as u32)
    }

    /// Fix the cluster count at `n` whatever the membership view says, so tests reach counts
    /// that gossip would not report.
    #[cfg(feature = "testing")]
    pub fn pin_cluster(&self, n: u32) {
        self.pinned_cluster.store(n, Ordering::Relaxed);
        self.cluster.store(n.max(1), Ordering::Relaxed);
    }

    pub fn check_gate(&self) -> Result<(), CommandError> {
        if self.multi_db() {
            Ok(())
        } else {
            Err(CommandError::Custom(NEEDS_UPGRADE.into()))
        }
    }

    /// True the first time that an entry of `db` is dropped, so the log names each database once.
    pub fn first_drop(&self, db: u32) -> bool {
        let mut dropped = self.dropped.lock();
        dropped.len() < DROP_LOG_LIMIT && dropped.insert(db)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err(text: &str) -> Result<u32, CommandError> {
        Err(CommandError::Custom(text.into()))
    }

    #[test]
    fn select_parses_like_valkey() {
        let dbs = Databases::new(3, false);
        dbs.set_view(3, true);
        assert_eq!(dbs.check_select(2), Ok(2));
    }

    #[cfg(feature = "testing")]
    #[test]
    fn a_view_cannot_change_a_pinned_count() {
        let dbs = Databases::new(2, false);
        dbs.pin_cluster(4);
        assert_eq!(dbs.set_view(2, false), None);
        assert_eq!(dbs.cluster(), 4);
    }

    #[test]
    fn select_refuses_the_range_edges() {
        let dbs = Databases::new(3, false);
        for db in [-1, 4294967296, 3] {
            assert_eq!(dbs.check_select(db), err(OUT_OF_RANGE), "{db}");
        }
    }

    #[test]
    fn the_cluster_count_stays_at_least_one() {
        let dbs = Databases::new(3, false);
        assert_eq!(dbs.set_view(0, true), Some(1));
        assert_eq!(dbs.cluster(), 1);
        assert_eq!(dbs.check_select(0), Ok(0));
    }

    #[test]
    fn the_gate_refuses_a_database_above_zero() {
        let dbs = Databases::new(3, false);
        dbs.set_view(3, false);
        assert_eq!(dbs.check_select(0), Ok(0));
        assert_eq!(dbs.check_select(1), err(NEEDS_UPGRADE));
    }

    #[test]
    fn the_range_check_comes_before_the_gate() {
        let dbs = Databases::new(3, false);
        dbs.set_view(1, false);
        assert_eq!(dbs.check_select(1), err(OUT_OF_RANGE));
    }

    #[test]
    fn a_lower_view_reports_the_drop() {
        let dbs = Databases::new(3, false);
        assert_eq!(dbs.set_view(3, true), None);
        assert_eq!(dbs.set_view(1, true), Some(1));
        assert_eq!(dbs.set_view(3, true), None);
    }

    #[test]
    fn a_drop_sets_a_floor_that_a_raise_lifts() {
        let dbs = Databases::new(3, false);
        assert!(dbs.holds(2));
        assert_eq!(dbs.set_view(1, true), Some(1));
        assert_eq!(dbs.local(), 3);
        assert!(dbs.holds(0));
        assert!(!dbs.holds(1) && !dbs.holds(2));
        assert_eq!(dbs.set_view(1, true), None);
        assert!(!dbs.holds(1));
        assert_eq!(dbs.set_view(3, true), None);
        assert!(dbs.holds(1) && dbs.holds(2));
        assert!(!dbs.holds(3));
    }

    #[test]
    fn a_partial_raise_moves_the_floor_to_the_new_count() {
        let dbs = Databases::new(3, false);
        assert_eq!(dbs.set_view(1, true), Some(1));
        assert_eq!(dbs.set_view(2, true), None);
        assert!(dbs.holds(1));
        assert!(!dbs.holds(2));
        assert_eq!(dbs.set_view(3, true), None);
        assert!(dbs.holds(2));
    }

    #[test]
    fn a_node_with_membership_serves_one_database_until_the_first_view() {
        let dbs = Databases::awaiting_view(3, false);
        assert_eq!(dbs.local(), 3);
        assert_eq!(dbs.cluster(), 1);
        assert!(!dbs.multi_db());
        assert_eq!(dbs.check_select(2), err(OUT_OF_RANGE));
        assert!(dbs.holds(2));
        assert_eq!(dbs.set_view(3, true), None);
        assert_eq!(dbs.check_select(2), Ok(2));
    }

    #[test]
    fn raise_local_keeps_the_larger() {
        let dbs = Databases::new(2, false);
        assert!(!dbs.raise_local(1));
        assert!(dbs.raise_local(4));
        assert_eq!(dbs.local(), 4);
    }

    #[test]
    fn first_drop_is_true_once() {
        let dbs = Databases::new(1, false);
        assert!(dbs.first_drop(5));
        assert!(!dbs.first_drop(5));
    }
}
