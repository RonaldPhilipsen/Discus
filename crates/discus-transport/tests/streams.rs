#![allow(clippy::unwrap_used)]

mod common;

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::{config, epoch, start, TestService};
use discus_placement::{ClusterId, NodeId};
use discus_transport::testing::{quic_client, TestCa, TEST_SAN};
use discus_transport::{frame, PeerEventKind, Transport, TransportError, ALPN_RPC};
use discus_wire::{
    caps, decode_frame, encode_envelope, encode_frame, Frame, Hello, Limits, Message, TraceContext,
};

fn trace() -> TraceContext {
    TraceContext {
        traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into()),
        tracestate: None,
    }
}

/// A raw connection that passed HELLO, for sending hand-made frames.
async fn raw(ca: &TestCa, cluster: ClusterId, addr: SocketAddr) -> quinn::Connection {
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

/// Send raw bytes as one frame on a new stream. Returns the reply frame, or `None` if the stream
/// ended or was reset without one.
async fn send_raw(conn: &quinn::Connection, bytes: &[u8]) -> Option<Frame> {
    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    frame::write_frame(&mut send, bytes).await.ok()?;
    let _ = send.finish();
    match frame::read_frame(&mut recv, 1 << 20).await {
        Ok(Some(b)) => Some(decode_frame(&b, &Limits::default()).unwrap()),
        _ => None,
    }
}

fn ping(nonce: u64) -> Vec<u8> {
    encode_frame(&Frame {
        request_id: 77,
        trace: TraceContext::default(),
        message: Message::Ping { nonce },
    })
}

async fn pair(ca: &TestCa) -> (Transport, Transport, NodeId, ClusterId) {
    let cluster = ClusterId::mint();
    let b_id = NodeId::random();
    let (a, _) = start(config(ca, NodeId::random(), cluster)).await;
    let (b, _) = start(config(ca, b_id, cluster)).await;
    (a, b, b_id, cluster)
}

#[tokio::test]
async fn request_and_reply_carry_id_and_trace() {
    let ca = TestCa::new();
    let cluster = ClusterId::mint();
    let b_id = NodeId::random();
    let (a, _) = start(config(&ca, NodeId::random(), cluster)).await;
    let (b, service) = start(config(&ca, b_id, cluster)).await;
    let reply = a
        .request(
            b_id,
            b.local_addr(),
            Message::Ping { nonce: 9 },
            trace(),
            Duration::from_secs(1),
            false,
        )
        .await
        .unwrap();
    assert_eq!(reply.message, Message::Pong { nonce: 9 });
    assert_eq!(reply.trace, trace());
    assert_eq!(service.seen.lock().as_slice(), &[reply.request_id]);
    assert_eq!(a.stats().snapshot().requests_out, 1);
    assert_eq!(b.stats().snapshot().requests_in, 1);
}

#[tokio::test]
async fn a_malformed_frame_is_dropped_and_the_connection_survives() {
    let ca = TestCa::new();
    let (_a, b, _, cluster) = pair(&ca).await;
    let conn = raw(&ca, cluster, b.local_addr()).await;
    let garbage: Vec<u8> = (0..64u8)
        .map(|i| i.wrapping_mul(37).wrapping_add(11))
        .collect();
    assert!(send_raw(&conn, &garbage).await.is_none());
    assert_eq!(b.stats().snapshot().frames_rejected, 1);
    // A valid envelope with a payload that fails the verifier is dropped too.
    let bad = encode_envelope(
        discus_wire::fb::MessageType::PING.0,
        3,
        &TraceContext::default(),
        &[9, 9],
    );
    assert!(send_raw(&conn, &bad).await.is_none());
    assert_eq!(b.stats().snapshot().frames_rejected, 2);
    let reply = send_raw(&conn, &ping(4)).await.unwrap();
    assert_eq!(reply.message, Message::Pong { nonce: 4 });
}

#[tokio::test]
async fn an_unknown_message_type_gets_unsupported() {
    let ca = TestCa::new();
    let (_a, b, _, cluster) = pair(&ca).await;
    let conn = raw(&ca, cluster, b.local_addr()).await;
    let reply = send_raw(&conn, &encode_envelope(999, 5, &trace(), &[]))
        .await
        .unwrap();
    assert_eq!(reply.request_id, 5);
    assert_eq!(reply.trace, trace());
    assert_eq!(reply.message, Message::Unsupported { message_type: 999 });
    assert_eq!(b.stats().snapshot().unsupported, 1);
    assert!(conn.close_reason().is_none());
    let reply = send_raw(&conn, &ping(6)).await.unwrap();
    assert_eq!(reply.message, Message::Pong { nonce: 6 });
}

#[tokio::test]
async fn an_oversized_frame_is_rejected() {
    let ca = TestCa::new();
    let cluster = ClusterId::mint();
    let mut cb = config(&ca, NodeId::random(), cluster);
    cb.max_frame = 65536;
    let (b, _) = start(cb).await;
    let conn = raw(&ca, cluster, b.local_addr()).await;
    let big = vec![0u8; 100_000];
    assert!(send_raw(&conn, &big).await.is_none());
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(b.stats().snapshot().frames_rejected, 1);
    let reply = send_raw(&conn, &ping(1)).await.unwrap();
    assert_eq!(reply.message, Message::Pong { nonce: 1 });
}

#[tokio::test]
async fn many_concurrent_requests_respect_the_stream_limit() {
    let ca = TestCa::new();
    let cluster = ClusterId::mint();
    let b_id = NodeId::random();
    let (a, _) = start(config(&ca, NodeId::random(), cluster)).await;
    let mut cb = config(&ca, b_id, cluster);
    cb.max_streams = 16;
    let service = TestService::slow(epoch(1, b_id), Duration::from_millis(5));
    let b = Transport::bind(cb, service.clone()).await.unwrap();
    let addr = b.local_addr();
    let tasks: Vec<_> = (0..200u64)
        .map(|i| {
            let a = a.clone();
            tokio::spawn(async move {
                a.request(
                    b_id,
                    addr,
                    Message::Ping { nonce: i },
                    TraceContext::default(),
                    Duration::from_secs(10),
                    false,
                )
                .await
            })
        })
        .collect();
    for (i, t) in tasks.into_iter().enumerate() {
        let reply = t.await.unwrap().unwrap();
        assert_eq!(reply.message, Message::Pong { nonce: i as u64 });
    }
    let peak = service.peak.load(Ordering::SeqCst);
    assert!(peak <= 16, "peak concurrency {peak}");
}

#[tokio::test]
async fn a_slow_reply_times_out_and_reports_evidence() {
    let ca = TestCa::new();
    let cluster = ClusterId::mint();
    let b_id = NodeId::random();
    let (a, _) = start(config(&ca, NodeId::random(), cluster)).await;
    let service = TestService::slow(epoch(1, b_id), Duration::from_millis(500));
    let b = Transport::bind(config(&ca, b_id, cluster), service)
        .await
        .unwrap();
    a.peer(b_id, b.local_addr()).await.unwrap();
    let mut events = a.events();
    let err = a
        .request(
            b_id,
            b.local_addr(),
            Message::Ping { nonce: 1 },
            TraceContext::default(),
            Duration::from_millis(50),
            true,
        )
        .await
        .unwrap_err();
    assert_eq!(err, TransportError::Timeout);
    let event = tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await
        .expect("a timeout event")
        .unwrap();
    assert_eq!(event.peer, Some(b_id));
    assert_eq!(event.kind, PeerEventKind::ForwardTimeout { write: true });
}

#[tokio::test]
async fn a_closed_peer_reports_a_reset_and_reconnects() {
    let ca = TestCa::new();
    let cluster = ClusterId::mint();
    let b_id = NodeId::random();
    let (a, _) = start(config(&ca, NodeId::random(), cluster)).await;
    let mut cb = config(&ca, b_id, cluster);
    cb.handshake_timeout = Duration::from_millis(300);
    let (b, _) = start(cb.clone()).await;
    let addr = b.local_addr();
    let one = Duration::from_secs(1);
    a.request(
        b_id,
        addr,
        Message::Ping { nonce: 1 },
        TraceContext::default(),
        one,
        false,
    )
    .await
    .unwrap();
    let mut events = a.events();
    b.close();
    drop(b);
    let event = tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(
            event.kind,
            PeerEventKind::Reset(_) | PeerEventKind::HandshakeFailed(_)
        ),
        "{event:?}"
    );
    assert!(a
        .request(
            b_id,
            addr,
            Message::Ping { nonce: 2 },
            TraceContext::default(),
            one,
            false
        )
        .await
        .is_err());

    // A new transport on the same address and node ID serves again.
    cb.listen = addr;
    let mut again = None;
    for _ in 0..50 {
        if let Ok(socket) = std::net::UdpSocket::bind(addr) {
            again = Some(
                Transport::bind_socket(cb.clone(), socket, TestService::new(epoch(1, b_id)))
                    .unwrap(),
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _b2 = again.expect("the port frees up");
    let reply = a
        .request(
            b_id,
            addr,
            Message::Ping { nonce: 3 },
            TraceContext::default(),
            one,
            false,
        )
        .await
        .unwrap();
    assert_eq!(reply.message, Message::Pong { nonce: 3 });
}
