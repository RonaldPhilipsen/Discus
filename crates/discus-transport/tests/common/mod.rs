#![allow(dead_code, clippy::unwrap_used)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use std::net::SocketAddr;

use discus_placement::{ClusterId, EpochId, NodeId};
use discus_transport::testing::{quic_client, TestCa, TEST_SAN};
use discus_transport::{frame, Inbound, Service, Transport, TransportConfig, ALPN_RPC};
use discus_wire::{caps, decode_frame, encode_frame, Frame, Hello, Limits, Message, TraceContext};
use futures::future::BoxFuture;

/// A service that answers `Ping` with `Pong` after an optional delay, and tracks concurrency.
pub struct TestService {
    pub epoch: EpochId,
    pub delay: Duration,
    pub active: AtomicUsize,
    pub peak: AtomicUsize,
    pub seen: parking_lot::Mutex<Vec<u64>>,
}

impl TestService {
    pub fn new(epoch: EpochId) -> Arc<TestService> {
        TestService::slow(epoch, Duration::ZERO)
    }

    pub fn slow(epoch: EpochId, delay: Duration) -> Arc<TestService> {
        Arc::new(TestService {
            epoch,
            delay,
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            seen: parking_lot::Mutex::new(Vec::new()),
        })
    }
}

impl Service for TestService {
    fn epoch(&self) -> EpochId {
        self.epoch
    }

    fn call(self: Arc<Self>, inbound: Inbound) -> BoxFuture<'static, Message> {
        Box::pin(async move {
            let now = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(now, Ordering::SeqCst);
            self.seen.lock().push(inbound.request_id);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            self.active.fetch_sub(1, Ordering::SeqCst);
            match inbound.message {
                Message::Ping { nonce } => Message::Pong { nonce },
                // Replication stalls, so a test can see that it does not hold up a command.
                Message::Replicate(b) => {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    Message::BatchAck(discus_wire::BatchAck {
                        applied: 0,
                        want: Vec::new(),
                        epoch: b.epoch,
                        idle: false,
                    })
                }
                _ => Message::Unsupported { message_type: 0 },
            }
        })
    }
}

pub fn epoch(generation: u64, origin: NodeId) -> EpochId {
    EpochId { generation, origin }
}

pub fn config(ca: &TestCa, id: NodeId, cluster: ClusterId) -> TransportConfig {
    let listen = "127.0.0.1:0".parse().unwrap();
    TransportConfig::new(listen, ca.material(TEST_SAN), id, cluster)
}

/// Start a transport with the test service on a free port.
pub async fn start(config: TransportConfig) -> (Transport, Arc<TestService>) {
    let service = TestService::new(epoch(1, config.node_id));
    let t = Transport::bind(config, service.clone()).await.unwrap();
    (t, service)
}

/// A raw connection that passed HELLO, for sending hand-made frames.
pub async fn raw(ca: &TestCa, cluster: ClusterId, addr: SocketAddr) -> quinn::Connection {
    let client = quic_client(ca.material(TEST_SAN).client_config(ALPN_RPC));
    let conn = client.connect(addr, "discus").unwrap().await.unwrap();
    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    let id = NodeId::random();
    let hello = Frame {
        request_id: 0,
        trace: TraceContext::default(),
        message: Message::Hello(Hello {
            node_id: id,
            cluster_id: cluster,
            capabilities: caps::KNOWN,
            epoch: epoch(1, id),
            max_frame: 65536,
        }),
    };
    frame::write_frame(&mut send, &encode_frame(&hello))
        .await
        .unwrap();
    send.finish().unwrap();
    let reply = frame::read_frame(&mut recv, 65536).await.unwrap().unwrap();
    assert!(matches!(
        decode_frame(&reply, &Limits::default()).unwrap().message,
        Message::Hello(_)
    ));
    // Keep the client endpoint alive with the connection.
    std::mem::forget(client);
    conn
}
