#![allow(clippy::unwrap_used)]

mod common;

use std::time::Duration;

use common::{config, start};
use discus_placement::{ClusterId, NodeId};
use discus_transport::testing::{quic_client, TestCa, TEST_SAN};
use discus_transport::{frame, PeerEventKind, TransportError, ALPN_RPC};
use discus_wire::{caps, decode_frame, encode_frame, Frame, HandshakeCode, Limits, Message};

#[tokio::test]
async fn mtls_handshake_and_hello_succeed() {
    let ca = TestCa::new();
    let cluster = ClusterId::mint();
    let (a_id, b_id) = (NodeId::random(), NodeId::random());
    let (a, _) = start(config(&ca, a_id, cluster)).await;
    let mut cb = config(&ca, b_id, cluster);
    cb.max_frame = 1 << 20;
    let (b, _) = start(cb).await;
    let info = a.peer(b_id, b.local_addr()).await.unwrap();
    assert_eq!(info.node_id, b_id);
    assert_eq!(info.epoch, common::epoch(1, b_id));
    assert_eq!(info.capabilities, caps::KNOWN);
    assert_eq!(info.max_frame, 1 << 20);
    // The second call reuses the connection.
    assert_eq!(a.peer(b_id, b.local_addr()).await.unwrap(), info);
}

async fn expect_handshake_failure(
    a: &discus_transport::Transport,
    b_id: NodeId,
    addr: std::net::SocketAddr,
) {
    let mut events = a.events();
    let err = a.peer(b_id, addr).await.unwrap_err();
    assert!(
        matches!(
            err,
            TransportError::Connect(_) | TransportError::Handshake { .. }
        ),
        "{err:?}"
    );
    assert!(a.stats().snapshot().handshake_failures >= 1);
    let event = tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(event.kind, PeerEventKind::HandshakeFailed(_)),
        "{event:?}"
    );
    assert_eq!(event.peer, Some(b_id));
}

#[tokio::test]
async fn a_wrong_san_is_rejected() {
    let ca = TestCa::new();
    let cluster = ClusterId::mint();
    let (a_id, b_id) = (NodeId::random(), NodeId::random());
    let (a, _) = start(config(&ca, a_id, cluster)).await;
    let mut cb = config(&ca, b_id, cluster);
    // b presents a certificate for another SAN, and expects the test SAN from peers.
    cb.tls = ca.material_for("spiffe://other/ns/x/sa/y", TEST_SAN);
    let (b, _) = start(cb).await;
    expect_handshake_failure(&a, b_id, b.local_addr()).await;
}

#[tokio::test]
async fn an_empty_san_accepts_any_certificate_from_the_ca() {
    let ca = TestCa::new();
    let cluster = ClusterId::mint();
    let (a_id, b_id) = (NodeId::random(), NodeId::random());
    let mut ca_config = config(&ca, a_id, cluster);
    ca_config.tls = ca.material_for("spiffe://one/ns/x/sa/y", "");
    let mut cb = config(&ca, b_id, cluster);
    cb.tls = ca.material_for("spiffe://two/ns/x/sa/y", "");
    let (a, _) = start(ca_config).await;
    let (b, _) = start(cb).await;
    assert_eq!(a.peer(b_id, b.local_addr()).await.unwrap().node_id, b_id);
}

#[tokio::test]
async fn an_empty_san_still_rejects_a_wrong_ca() {
    let (ca, other) = (TestCa::new(), TestCa::new());
    let cluster = ClusterId::mint();
    let (a_id, b_id) = (NodeId::random(), NodeId::random());
    let mut ca_config = config(&ca, a_id, cluster);
    ca_config.tls = ca.material_for(TEST_SAN, "");
    let mut cb = config(&other, b_id, cluster);
    cb.tls = other.material_for(TEST_SAN, "");
    let (a, _) = start(ca_config).await;
    let (b, _) = start(cb).await;
    expect_handshake_failure(&a, b_id, b.local_addr()).await;
}

#[tokio::test]
async fn a_wrong_ca_is_rejected() {
    let (ca, other) = (TestCa::new(), TestCa::new());
    let cluster = ClusterId::mint();
    let (a_id, b_id) = (NodeId::random(), NodeId::random());
    let (a, _) = start(config(&ca, a_id, cluster)).await;
    let (b, _) = start(config(&other, b_id, cluster)).await;
    expect_handshake_failure(&a, b_id, b.local_addr()).await;
    // The server side counts its failure too.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(b.stats().snapshot().handshake_failures >= 1);
}

#[tokio::test]
async fn a_peer_without_a_client_cert_is_rejected() {
    let ca = TestCa::new();
    let (b, _) = start(config(&ca, NodeId::random(), ClusterId::mint())).await;
    let tls = discus_transport::testing::client_without_cert(&ca.material(TEST_SAN), ALPN_RPC);
    let client = quic_client(tls);
    let result = async {
        let conn = client.connect(b.local_addr(), "discus")?.await?;
        let (mut send, mut recv) = conn.open_bi().await?;
        frame::write_frame(&mut send, b"hello").await?;
        frame::read_frame(&mut recv, 1024).await?;
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn an_alpn_mismatch_is_rejected() {
    let ca = TestCa::new();
    let (b, _) = start(config(&ca, NodeId::random(), ClusterId::mint())).await;
    let client = quic_client(ca.material(TEST_SAN).client_config(b"h3"));
    let result = client.connect(b.local_addr(), "discus").unwrap().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn a_foreign_cluster_fails_hello() {
    let ca = TestCa::new();
    let (a_id, b_id) = (NodeId::random(), NodeId::random());
    let (a, _) = start(config(&ca, a_id, ClusterId::mint())).await;
    let (b, _) = start(config(&ca, b_id, ClusterId::mint())).await;
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
async fn an_unexpected_node_fails_hello() {
    let ca = TestCa::new();
    let cluster = ClusterId::mint();
    let (a, _) = start(config(&ca, NodeId::random(), cluster)).await;
    let (b, _) = start(config(&ca, NodeId::random(), cluster)).await;
    let err = a.peer(NodeId::random(), b.local_addr()).await.unwrap_err();
    assert!(
        matches!(
            err,
            TransportError::Handshake {
                code: HandshakeCode::UnexpectedNode,
                ..
            }
        ),
        "{err:?}"
    );
}

#[tokio::test]
async fn a_tiny_frame_offer_fails_hello() {
    let ca = TestCa::new();
    let cluster = ClusterId::mint();
    let b_id = NodeId::random();
    let (a, _) = start(config(&ca, NodeId::random(), cluster)).await;
    let mut cb = config(&ca, b_id, cluster);
    cb.max_frame = 1024;
    let (b, _) = start(cb).await;
    let err = a.peer(b_id, b.local_addr()).await.unwrap_err();
    assert!(
        matches!(
            err,
            TransportError::Handshake {
                code: HandshakeCode::FrameTooSmall,
                ..
            }
        ),
        "{err:?}"
    );
}

#[tokio::test]
async fn a_peer_without_slot_placement_is_refused() {
    let ca = TestCa::new();
    let cluster = ClusterId::mint();
    let b_id = NodeId::random();
    let (a, _) = start(config(&ca, NodeId::random(), cluster)).await;
    let mut cb = config(&ca, b_id, cluster);
    cb.capabilities = caps::KNOWN & !caps::SLOT_PLACEMENT;
    let (b, _) = start(cb).await;
    let err = a.peer(b_id, b.local_addr()).await.unwrap_err();
    assert!(
        matches!(
            err,
            TransportError::Handshake {
                code: HandshakeCode::PlacementMismatch,
                ..
            }
        ),
        "{err:?}"
    );
}

#[tokio::test]
async fn disjoint_capabilities_agree_on_none() {
    let ca = TestCa::new();
    let cluster = ClusterId::mint();
    let b_id = NodeId::random();
    let mut ca_cfg = config(&ca, NodeId::random(), cluster);
    ca_cfg.capabilities = caps::SLOT_PLACEMENT | 1;
    let mut cb = config(&ca, b_id, cluster);
    cb.capabilities = caps::SLOT_PLACEMENT | 1 << 7;
    let (a, _) = start(ca_cfg).await;
    let (b, _) = start(cb).await;
    assert_eq!(
        a.peer(b_id, b.local_addr()).await.unwrap().capabilities,
        caps::SLOT_PLACEMENT
    );

    let c_id = NodeId::random();
    let mut cc = config(&ca, NodeId::random(), cluster);
    cc.capabilities = caps::KNOWN | 1 << 63;
    let mut cd = config(&ca, c_id, cluster);
    cd.capabilities = caps::KNOWN | 1 << 63;
    let (c, _) = start(cc).await;
    let (d, _) = start(cd).await;
    assert_eq!(
        c.peer(c_id, d.local_addr()).await.unwrap().capabilities,
        caps::KNOWN
    );
}

#[tokio::test]
async fn a_first_stream_without_hello_fails() {
    let ca = TestCa::new();
    let (b, _) = start(config(&ca, NodeId::random(), ClusterId::mint())).await;
    let client = quic_client(ca.material(TEST_SAN).client_config(ALPN_RPC));
    let conn = client
        .connect(b.local_addr(), "discus")
        .unwrap()
        .await
        .unwrap();
    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    let ping = Frame {
        request_id: 1,
        trace: Default::default(),
        message: Message::Ping { nonce: 1 },
    };
    frame::write_frame(&mut send, &encode_frame(&ping))
        .await
        .unwrap();
    let reply = frame::read_frame(&mut recv, 65536).await.unwrap().unwrap();
    let reply = decode_frame(&reply, &Limits::default()).unwrap();
    match reply.message {
        Message::HandshakeFailed(f) => assert_eq!(f.code, HandshakeCode::NotHello),
        other => panic!("expected HANDSHAKE_FAILED, got {other:?}"),
    }
    let closed = tokio::time::timeout(Duration::from_secs(2), conn.closed()).await;
    assert!(closed.is_ok(), "the connection stays open");
}
