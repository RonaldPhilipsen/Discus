#![allow(clippy::unwrap_used)]

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::{config, epoch, start};
use discus_placement::{ClusterId, EpochId, MemberState, NodeId, Weight};
use discus_transport::testing::TestCa;
use discus_transport::{Channel, GossipError, Inbound, Service, Transport, TransportError};
use discus_wire::{Capacity, Gossip, GossipKind, HandshakeCode, MemberInfo, Message};
use futures::future::BoxFuture;

/// Answers every gossip message with an ACK, or never answers when `silent`.
struct GossipService {
    epoch: EpochId,
    silent: bool,
}

impl Service for GossipService {
    fn epoch(&self) -> EpochId {
        self.epoch
    }

    fn call(self: Arc<Self>, inbound: Inbound) -> BoxFuture<'static, Message> {
        Box::pin(async move {
            match inbound.message {
                Message::Gossip(g) if !self.silent => {
                    let mut reply = g.clone();
                    reply.kind = GossipKind::Ack;
                    Message::Gossip(reply)
                }
                Message::Gossip(_) => std::future::pending().await,
                Message::Ping { nonce } => Message::Pong { nonce },
                other => Message::Unsupported {
                    message_type: other.type_code(),
                },
            }
        })
    }
}

async fn start_gossip(ca: &TestCa, id: NodeId, cluster: ClusterId, silent: bool) -> Transport {
    let service = Arc::new(GossipService {
        epoch: epoch(5, id),
        silent,
    });
    Transport::bind(config(ca, id, cluster), service)
        .await
        .unwrap()
}

fn ping(from: NodeId) -> Message {
    Message::Gossip(Gossip::new(
        GossipKind::Ping,
        MemberInfo {
            id: from,
            addr: "127.0.0.1:1".parse().unwrap(),
            rpc_addr: "127.0.0.1:1".parse().unwrap(),
            client_addr: None,
            client_host: None,
            state: MemberState::Joining,
            incarnation: 0,
            weight: Weight::default(),
            epoch: epoch(0, from),
            capabilities: 0,
            capacity: Capacity::default(),
            leader_hints: Bytes::new(),
        },
    ))
}

const SECOND: Duration = Duration::from_secs(1);

#[tokio::test]
async fn gossip_crosses_clusters() {
    let ca = TestCa::new();
    let (a_id, b_id) = (NodeId::random(), NodeId::random());
    let (b_cluster, a_cluster) = (ClusterId::mint(), ClusterId::mint());
    let (a, _) = start(config(&ca, a_id, a_cluster)).await;
    let b = start_gossip(&ca, b_id, b_cluster, false).await;
    let (info, reply) = a
        .gossip(b.local_addr(), Some(b_id), ping(a_id), SECOND)
        .await
        .unwrap();
    assert_eq!(info.node_id, b_id);
    assert_eq!(info.cluster_id, b_cluster);
    assert_eq!(info.channel, Channel::Gossip);
    assert_eq!(info.epoch, epoch(5, b_id));
    assert!(matches!(reply, Message::Gossip(g) if g.kind == GossipKind::Ack));
    // The RPC channel stays strict.
    let err = a.peer(b_id, b.local_addr()).await.unwrap_err();
    assert!(
        matches!(
            err,
            TransportError::Handshake {
                code: HandshakeCode::ClusterMismatch,
                ..
            }
        ),
        "{err:?}"
    );
}

#[tokio::test]
async fn a_nil_cluster_joins_over_gossip() {
    let ca = TestCa::new();
    let (a_id, b_id) = (NodeId::random(), NodeId::random());
    let (a, _) = start(config(&ca, a_id, ClusterId::NIL)).await;
    let b = start_gossip(&ca, b_id, ClusterId::mint(), false).await;
    let (info, _) = a
        .gossip(b.local_addr(), None, ping(a_id), SECOND)
        .await
        .unwrap();
    assert_eq!(info.node_id, b_id);
}

#[tokio::test]
async fn a_silent_peer_is_reported_as_silent() {
    let ca = TestCa::new();
    let cluster = ClusterId::mint();
    let (a_id, b_id) = (NodeId::random(), NodeId::random());
    let (a, _) = start(config(&ca, a_id, cluster)).await;
    let b = start_gossip(&ca, b_id, cluster, true).await;
    let started = std::time::Instant::now();
    let err = a
        .gossip(b.local_addr(), None, ping(a_id), Duration::from_millis(200))
        .await
        .unwrap_err();
    match err {
        GossipError::Silent(info) => {
            assert_eq!(info.node_id, b_id);
            assert_eq!(info.epoch.generation, 5);
        }
        other => panic!("expected Silent, got {other:?}"),
    }
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn an_unreachable_address_is_unreachable() {
    let ca = TestCa::new();
    let mut c = config(&ca, NodeId::random(), ClusterId::mint());
    c.handshake_timeout = Duration::from_millis(300);
    let (a, _) = start(c).await;
    let closed: SocketAddr = {
        let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        s.local_addr().unwrap()
    };
    let err = a
        .gossip(closed, None, ping(a.node_id()), SECOND)
        .await
        .unwrap_err();
    assert!(matches!(err, GossipError::Unreachable(_)), "{err:?}");
}

#[tokio::test]
async fn the_wrong_node_at_an_address_is_refused() {
    let ca = TestCa::new();
    let cluster = ClusterId::mint();
    let (a, _) = start(config(&ca, NodeId::random(), cluster)).await;
    let b = start_gossip(&ca, NodeId::random(), cluster, false).await;
    let err = a
        .gossip(
            b.local_addr(),
            Some(NodeId::random()),
            ping(a.node_id()),
            SECOND,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, GossipError::Refused(_)), "{err:?}");
    // A pooled connection to the right node gives the same answer for the wrong ID.
    a.gossip(b.local_addr(), None, ping(a.node_id()), SECOND)
        .await
        .unwrap();
    let err = a
        .gossip(
            b.local_addr(),
            Some(NodeId::random()),
            ping(a.node_id()),
            SECOND,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, GossipError::Refused(_)), "{err:?}");
}

#[tokio::test]
async fn gossip_failures_are_not_evidence() {
    let ca = TestCa::new();
    let cluster = ClusterId::mint();
    let mut c = config(&ca, NodeId::random(), cluster);
    c.handshake_timeout = Duration::from_millis(300);
    let (a, _) = start(c).await;
    let mut events = a.events();
    let silent = start_gossip(&ca, NodeId::random(), cluster, true).await;
    let closed: SocketAddr = {
        let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        s.local_addr().unwrap()
    };
    let me = a.node_id();
    let short = Duration::from_millis(100);
    let _ = a.gossip(silent.local_addr(), None, ping(me), short).await;
    let _ = a.gossip(closed, None, ping(me), short).await;
    let _ = a
        .gossip(silent.local_addr(), Some(NodeId::random()), ping(me), short)
        .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(events.try_recv().is_err());
}

#[tokio::test]
async fn set_cluster_id_admits_rpc() {
    let ca = TestCa::new();
    let cluster = ClusterId::mint();
    let (a_id, b_id) = (NodeId::random(), NodeId::random());
    let (a, _) = start(config(&ca, a_id, ClusterId::NIL)).await;
    let (b, _) = start(config(&ca, b_id, cluster)).await;
    assert!(a.peer(b_id, b.local_addr()).await.is_err());
    a.set_cluster_id(cluster);
    assert_eq!(a.cluster_id(), cluster);
    let info = a.peer(b_id, b.local_addr()).await.unwrap();
    assert_eq!(info.channel, Channel::Rpc);
    assert_eq!(info.cluster_id, cluster);
}
