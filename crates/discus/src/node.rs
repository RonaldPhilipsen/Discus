//! State shared by every connection on a node.

use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::time::Instant;

use std::sync::Arc;

use bytes::Bytes;
use discus_resp::Protocol;
use discus_store::{Store, StoreConfig, SystemClock, HASH_SEED};

use crate::Config;

/// Where the node is in its life.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Starting,
    Serving,
    Leaving,
}

impl Phase {
    pub fn name(self) -> &'static str {
        match self {
            Phase::Starting => "starting",
            Phase::Serving => "serving",
            Phase::Leaving => "leaving",
        }
    }
}

/// The node. Shared as `Arc<Node>`.
pub struct Node {
    pub config: Config,
    pub started_at: Instant,
    pub store: Arc<Store>,
    next_conn_id: AtomicU64,
    connected: AtomicUsize,
    phase: AtomicU8,
}

impl Node {
    pub fn new(config: Config) -> Node {
        let store = Store::new(
            StoreConfig {
                shards: config.shards,
                max_memory: config.max_memory,
                seed: HASH_SEED,
            },
            Arc::new(SystemClock),
        );
        Node {
            config,
            started_at: Instant::now(),
            store: Arc::new(store),
            next_conn_id: AtomicU64::new(1),
            connected: AtomicUsize::new(0),
            phase: AtomicU8::new(Phase::Starting as u8),
        }
    }

    /// Count a new client. The returned guard decrements the count when dropped.
    pub fn track_connection(self: &Arc<Self>) -> ConnectionGuard {
        self.connected.fetch_add(1, Ordering::Relaxed);
        ConnectionGuard(self.clone())
    }

    pub fn connected_clients(&self) -> usize {
        self.connected.load(Ordering::Relaxed)
    }

    /// Connections accepted since start.
    pub fn total_connections(&self) -> u64 {
        self.next_conn_id.load(Ordering::Relaxed) - 1
    }

    pub fn phase(&self) -> Phase {
        match self.phase.load(Ordering::Acquire) {
            0 => Phase::Starting,
            1 => Phase::Serving,
            _ => Phase::Leaving,
        }
    }

    pub fn set_phase(&self, phase: Phase) {
        self.phase.store(phase as u8, Ordering::Release);
    }

    pub fn next_conn_id(&self) -> u64 {
        self.next_conn_id.fetch_add(1, Ordering::Relaxed)
    }
}

/// Per-connection state.
#[derive(Debug)]
pub struct ConnState {
    pub protocol: Protocol,
    pub authenticated: bool,
    pub name: Option<Bytes>,
    pub id: u64,
    /// Set by `QUIT`: close the connection after the reply is flushed.
    pub closing: bool,
}

impl ConnState {
    pub fn new(id: u64, authenticated: bool) -> ConnState {
        ConnState {
            protocol: Protocol::Resp2,
            authenticated,
            name: None,
            id,
            closing: false,
        }
    }
}

/// Keeps the connected-clients count accurate for the life of one connection.
pub struct ConnectionGuard(Arc<Node>);

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.0.connected.fetch_sub(1, Ordering::Relaxed);
    }
}
