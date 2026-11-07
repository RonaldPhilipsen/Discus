//! The cluster lease (spec section 6.2). It decides which node forms a new cluster, and it names
//! the winner of a merge between two independently formed clusters (spec section 6.3).

mod memory;

use std::net::SocketAddr;

use discus_placement::ClusterId;
use futures::future::BoxFuture;

pub use memory::MemoryLease;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseHolder {
    /// The `NodeId` in hex.
    pub identity: String,
    pub address: SocketAddr,
    pub cluster_id: ClusterId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseState {
    /// This node holds the lease now, or a live member of its own cluster holds it. The holder
    /// is in the value.
    Held(LeaseHolder),
    /// A node of another cluster holds a live lease. A seeking node adds its address as a seed.
    HeldByOther(LeaseHolder),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseError(pub String);

impl std::fmt::Display for LeaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "lease: {}", self.0)
    }
}

impl std::error::Error for LeaseError {}

/// A lease provider. Every NORMAL member calls `acquire_or_renew` at the renewal interval, so a
/// live cluster always holds the lease. A seeking node calls it once its bootstrap deadline has
/// passed without a NORMAL peer.
pub trait LeaseProvider: Send + Sync {
    /// Take the lease when it is free or expired, renew it when `me` holds it, and leave it
    /// alone when a live member of `me.cluster_id` holds it.
    fn acquire_or_renew<'a>(
        &'a self,
        me: &'a LeaseHolder,
    ) -> BoxFuture<'a, Result<LeaseState, LeaseError>>;

    fn name(&self) -> &'static str;
}

/// The provider `none`: every node holds the lease. It cannot stop a split bootstrap, so it is
/// for single-node development only (spec section 6.2).
#[derive(Debug, Default)]
pub struct NoneLease;

impl LeaseProvider for NoneLease {
    fn acquire_or_renew<'a>(
        &'a self,
        me: &'a LeaseHolder,
    ) -> BoxFuture<'a, Result<LeaseState, LeaseError>> {
        Box::pin(async move { Ok(LeaseState::Held(me.clone())) })
    }

    fn name(&self) -> &'static str {
        "none"
    }
}
