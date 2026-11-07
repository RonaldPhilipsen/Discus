//! State shared by every connection on a node.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use std::sync::Arc;

use bytes::Bytes;
use discus_resp::Protocol;
use discus_store::{Store, StoreConfig, SystemClock, HASH_SEED};

use crate::Config;

/// The node. Shared as `Arc<Node>`.
pub struct Node {
    pub config: Config,
    pub started_at: Instant,
    pub store: Arc<Store>,
    next_conn_id: AtomicU64,
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
        }
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
