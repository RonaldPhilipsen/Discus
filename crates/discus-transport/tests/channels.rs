#![allow(clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::{config, epoch};
use discus_placement::{ClusterId, EpochId, MemberState, NodeId, Weight};
use discus_transport::testing::TestCa;
use discus_transport::{Channels, GossipError, Inbound, Service, Transport, TransportError};
use discus_wire::{Capacity, Gossip, GossipKind, MemberInfo, Message, TraceContext};
use futures::future::BoxFuture;

struct AckService(NodeId);

impl Service for AckService {
    fn epoch(&self) -> EpochId {
        epoch(1, self.0)
    }

    fn call(self: Arc<Self>, inbound: Inbound) -> BoxFuture<'static, Message> {
        Box::pin(async move {
            match inbound.message {
                Message::Gossip(mut g) => {
                    g.kind = GossipKind::Ack;
                    Message::Gossip(g)
                }
                Message::Ping { nonce } => Message::Pong { nonce },
                other => Message::Unsupported {
                    message_type: other.type_code(),
                },
            }
        })
    }
}

async fn start_with(ca: &TestCa, cluster: ClusterId, channels: Channels) -> (Transport, NodeId) {
    let id = NodeId::random();
    let mut config = config(ca, id, cluster);
    config.channels = channels;
    let service: Arc<dyn Service> = Arc::new(AckService(id));
    (Transport::bind(config, service).await.unwrap(), id)
}

fn ping(from: NodeId) -> Message {
    Message::Gossip(Gossip::new(
        GossipKind::Ping,
        MemberInfo {
            id: from,
            addr: "127.0.0.1:1".parse().unwrap(),
            rpc_addr: "127.0.0.1:1".parse().unwrap(),
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

const TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
async fn gossip_only_endpoint_refuses_rpc() {
    let ca = TestCa::new();
    let cluster = ClusterId::mint();
    let (a, a_id) = start_with(&ca, cluster, Channels::GossipOnly).await;
    let (b, _) = start_with(&ca, cluster, Channels::Both).await;
    let err = b.peer(a_id, a.local_addr()).await.unwrap_err();
    assert!(matches!(err, TransportError::Connect(_)), "{err:?}");
    for _ in 0..1000 {
        if a.stats().snapshot().handshake_failures == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(a.stats().snapshot().handshake_failures, 1);
}

#[tokio::test]
async fn rpc_only_endpoint_refuses_gossip() {
    let ca = TestCa::new();
    let cluster = ClusterId::mint();
    let (a, _) = start_with(&ca, cluster, Channels::RpcOnly).await;
    let (b, b_id) = start_with(&ca, cluster, Channels::Both).await;
    let err = b
        .gossip(a.local_addr(), None, ping(b_id), TIMEOUT)
        .await
        .unwrap_err();
    assert!(
        matches!(err, GossipError::Unreachable(_) | GossipError::Refused(_)),
        "{err:?}"
    );
}

#[tokio::test]
async fn a_transport_does_not_dial_a_channel_it_does_not_serve() {
    let ca = TestCa::new();
    let cluster = ClusterId::mint();
    let (target, target_id) = start_with(&ca, cluster, Channels::Both).await;
    let (rpc_only, rpc_id) = start_with(&ca, cluster, Channels::RpcOnly).await;
    let (gossip_only, _) = start_with(&ca, cluster, Channels::GossipOnly).await;
    let err = rpc_only
        .gossip(target.local_addr(), None, ping(rpc_id), TIMEOUT)
        .await
        .unwrap_err();
    assert!(matches!(err, GossipError::Unreachable(_)), "{err:?}");
    let mut events = gossip_only.events();
    gossip_only
        .peer(target_id, target.local_addr())
        .await
        .unwrap_err();
    gossip_only
        .request(
            target_id,
            target.local_addr(),
            Message::Ping { nonce: 1 },
            TraceContext::default(),
            TIMEOUT,
            false,
        )
        .await
        .unwrap_err();
    assert_eq!(gossip_only.stats().snapshot().handshake_failures, 0);
    assert!(events.try_recv().is_err());
    assert_eq!(target.stats().snapshot().handshake_failures, 0);
}

#[tokio::test]
async fn split_endpoints_serve_their_channel() {
    let ca = TestCa::new();
    let cluster = ClusterId::mint();
    let (a_gossip, a_id) = start_with(&ca, cluster, Channels::GossipOnly).await;
    let (a_rpc, _) = start_with(&ca, cluster, Channels::RpcOnly).await;
    let (b_gossip, b_id) = start_with(&ca, cluster, Channels::GossipOnly).await;
    let (b_rpc, b_rpc_id) = start_with(&ca, cluster, Channels::RpcOnly).await;
    let (info, reply) = a_gossip
        .gossip(b_gossip.local_addr(), Some(b_id), ping(a_id), TIMEOUT)
        .await
        .unwrap();
    assert_eq!(info.node_id, b_id);
    assert!(matches!(reply, Message::Gossip(g) if g.kind == GossipKind::Ack));
    let info = a_rpc.peer(b_rpc_id, b_rpc.local_addr()).await.unwrap();
    assert_eq!(info.node_id, b_rpc_id);
}
