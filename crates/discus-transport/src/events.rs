//! Data-path evidence and transport counters.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};

use discus_placement::NodeId;

/// The evidence channel holds this many events. A slow reader loses the oldest.
pub const EVENT_CAPACITY: usize = 1024;

/// One piece of data-path evidence about a peer. The failure detector weighs these more than a
/// missed ping (spec section 6.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerEvent {
    /// The peer, when the local node knows which node it expected at `addr`.
    pub peer: Option<NodeId>,
    pub addr: SocketAddr,
    pub kind: PeerEventKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerEventKind {
    /// The QUIC handshake, the certificate check, or `HELLO` failed.
    HandshakeFailed(String),
    /// An established connection closed or reset without a local close.
    Reset(String),
    /// A request got no reply within its timeout.
    ForwardTimeout { write: bool },
}

/// Counters for the transport. No metric exports them yet.
#[derive(Debug, Default)]
pub struct TransportStats {
    pub frames_rejected: AtomicU64,
    pub unsupported: AtomicU64,
    pub handshake_failures: AtomicU64,
    pub requests_in: AtomicU64,
    /// Requests that arrived on a shared stream. They count in `requests_in` too.
    pub shared_in: AtomicU64,
    /// Replies to shared-stream requests that went out on a unidirectional stream, because
    /// they were too large for the shared stream.
    pub large_replies_out: AtomicU64,
    pub requests_out: AtomicU64,
}

/// A copy of the counters at one moment.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    pub frames_rejected: u64,
    pub unsupported: u64,
    pub handshake_failures: u64,
    pub requests_in: u64,
    pub shared_in: u64,
    pub large_replies_out: u64,
    pub requests_out: u64,
}

impl TransportStats {
    pub fn snapshot(&self) -> StatsSnapshot {
        let get = |c: &AtomicU64| c.load(Ordering::Relaxed);
        StatsSnapshot {
            frames_rejected: get(&self.frames_rejected),
            unsupported: get(&self.unsupported),
            handshake_failures: get(&self.handshake_failures),
            requests_in: get(&self.requests_in),
            shared_in: get(&self.shared_in),
            large_replies_out: get(&self.large_replies_out),
            requests_out: get(&self.requests_out),
        }
    }

    pub(crate) fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
}
