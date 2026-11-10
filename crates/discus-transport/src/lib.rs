//! Node-to-node transport: QUIC through `quinn` and `rustls` with mTLS (spec section 13.1).
//!
//! One UDP port per node. ALPN `discus-rpc` carries forwarded commands, and ALPN `discus-gossip`
//! carries membership. Every connection starts
//! with `HELLO` on its first stream. Then each request takes its own bidirectional stream with
//! one frame each way, or, for a small write on the command lane, the shared stream of the
//! connection. Handshake failures, resets, and forward timeouts go to the evidence channel
//! that the failure detector of plan 3 consumes.

mod endpoint;
mod events;
pub mod frame;
mod hello;
mod provider;
mod shared;
mod tls;

#[cfg(feature = "testing")]
pub mod testing;

pub use endpoint::{
    Channel, Channels, GossipError, Inbound, Lane, PeerInfo, Service, Transport, TransportConfig,
    TransportError,
};
pub use events::{PeerEvent, PeerEventKind, StatsSnapshot, TransportStats, EVENT_CAPACITY};
pub use provider::{crypto_provider, PROVIDER_NAME};
pub use tls::{pem_certs, pem_key, pem_roots, TlsError, TlsMaterial};

/// ALPN for forwarded commands and other request-reply traffic.
pub const ALPN_RPC: &[u8] = b"discus-rpc";
/// ALPN for gossip. Its `HELLO` accepts another cluster.
pub const ALPN_GOSSIP: &[u8] = b"discus-gossip";

/// Application close code after a failed `HELLO`.
pub const CLOSE_HANDSHAKE_FAILED: u32 = 0x10;
/// Stream reset code for a frame that failed the verifier.
pub const RESET_BAD_FRAME: u32 = 0x11;
/// Application close code at shutdown.
pub const CLOSE_SHUTDOWN: u32 = 0x00;
