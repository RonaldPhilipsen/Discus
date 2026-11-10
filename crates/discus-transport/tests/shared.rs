//! The shared stream of a command-lane connection.

#![allow(clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::{config, epoch, raw, TestService};
use discus_placement::{ClusterId, EpochId, NodeId};
use discus_transport::testing::TestCa;
use discus_transport::{
    frame, Inbound, PeerEventKind, Service, Transport, TransportError, RESET_BAD_FRAME,
};
use discus_wire::{caps, decode_frame, encode_frame, Frame, Limits, Message, TraceContext};
use futures::future::BoxFuture;

/// Answers `Ping` with `Pong` after `nonce` milliseconds, so replies can come back out of order.
struct Delay(EpochId);

impl Service for Delay {
    fn epoch(&self) -> EpochId {
        self.0
    }

    fn call(self: Arc<Self>, inbound: Inbound) -> BoxFuture<'static, Message> {
        Box::pin(async move {
            match inbound.message {
                Message::Ping { nonce } => {
                    tokio::time::sleep(Duration::from_millis(nonce)).await;
                    Message::Pong { nonce }
                }
                _ => Message::Unsupported { message_type: 0 },
            }
        })
    }
}

struct Pair {
    a: Transport,
    b: Transport,
    b_id: NodeId,
    cluster: ClusterId,
}

async fn pair(b_caps: u64) -> (TestCa, Pair) {
    let ca = TestCa::new();
    let cluster = ClusterId::mint();
    let a_id = NodeId::random();
    let b_id = NodeId::random();
    let a = Transport::bind(config(&ca, a_id, cluster), Arc::new(Delay(epoch(1, a_id))))
        .await
        .unwrap();
    let mut cb = config(&ca, b_id, cluster);
    cb.capabilities = b_caps;
    let b = Transport::bind(cb, Arc::new(Delay(epoch(1, b_id))))
        .await
        .unwrap();
    (
        ca,
        Pair {
            a,
            b,
            b_id,
            cluster,
        },
    )
}

impl Pair {
    async fn send(
        &self,
        message: Message,
        timeout: Duration,
        write: bool,
    ) -> Result<Frame, TransportError> {
        self.a
            .request(
                self.b_id,
                self.b.local_addr(),
                message,
                TraceContext::default(),
                timeout,
                write,
            )
            .await
    }

    async fn ping(
        &self,
        nonce: u64,
        timeout: Duration,
        write: bool,
    ) -> Result<u64, TransportError> {
        match self
            .send(Message::Ping { nonce }, timeout, write)
            .await?
            .message
        {
            Message::Pong { nonce } => Ok(nonce),
            other => panic!("expected PONG, got {other:?}"),
        }
    }
}

const LONG: Duration = Duration::from_secs(10);

#[tokio::test]
async fn concurrent_writes_share_the_stream_and_get_their_own_replies() {
    let (_ca, p) = pair(caps::KNOWN).await;
    let p = Arc::new(p);
    // Later requests finish first, so the replies come back in reverse order.
    let tasks: Vec<_> = (0..100u64)
        .map(|i| {
            let p = p.clone();
            tokio::spawn(async move { p.ping(100 - i, LONG, true).await })
        })
        .collect();
    for (i, t) in tasks.into_iter().enumerate() {
        assert_eq!(t.await.unwrap().unwrap(), 100 - i as u64);
    }
    let stats = p.b.stats().snapshot();
    assert_eq!(stats.requests_in, 100);
    assert_eq!(stats.shared_in, 100);
}

#[tokio::test]
async fn reads_and_large_writes_take_their_own_streams() {
    let (_ca, p) = pair(caps::KNOWN).await;
    assert_eq!(p.ping(0, LONG, false).await.unwrap(), 0);
    let big = Message::Fetch {
        keys: vec![Bytes::from(vec![7u8; 100_000])],
    };
    // The test service does not know FETCH. The reply still shows which stream it took.
    let err = p.send(big, LONG, true).await.unwrap_err();
    assert!(matches!(err, TransportError::PeerUnsupported(_)), "{err:?}");
    let stats = p.b.stats().snapshot();
    assert_eq!(stats.requests_in, 2);
    assert_eq!(stats.shared_in, 0);
}

#[tokio::test]
async fn a_peer_without_the_bit_gets_a_stream_per_write() {
    let (_ca, p) = pair(caps::KNOWN & !caps::SHARED_STREAM).await;
    for nonce in 0..5 {
        assert_eq!(p.ping(nonce, LONG, true).await.unwrap(), nonce);
    }
    let stats = p.b.stats().snapshot();
    assert_eq!(stats.requests_in, 5);
    assert_eq!(stats.shared_in, 0);
}

#[tokio::test]
async fn a_timed_out_write_leaves_the_stream_usable() {
    let (_ca, p) = pair(caps::KNOWN).await;
    p.a.peer(p.b_id, p.b.local_addr()).await.unwrap();
    let mut events = p.a.events();
    let err = p
        .ping(1_000, Duration::from_millis(50), true)
        .await
        .unwrap_err();
    assert_eq!(err, TransportError::Timeout);
    let event = tokio::time::timeout(LONG, events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.kind, PeerEventKind::ForwardTimeout { write: true });
    assert_eq!(p.ping(0, LONG, true).await.unwrap(), 0);
    // The late reply finds no waiter and changes nothing.
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert_eq!(p.ping(2, LONG, true).await.unwrap(), 2);
    assert_eq!(p.b.stats().snapshot().shared_in, 3);
}

#[tokio::test]
async fn a_lost_connection_fails_waiting_writes_at_once() {
    let (_ca, p) = pair(caps::KNOWN).await;
    p.a.peer(p.b_id, p.b.local_addr()).await.unwrap();
    let p = Arc::new(p);
    let pending = {
        let p = p.clone();
        tokio::spawn(async move { p.ping(5_000, LONG, true).await })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    p.b.close();
    let err = tokio::time::timeout(Duration::from_secs(3), pending)
        .await
        .expect("the write fails before its timeout")
        .unwrap()
        .unwrap_err();
    assert!(matches!(err, TransportError::Reset(_)), "{err:?}");
}

#[tokio::test]
async fn shared_writes_respect_the_stream_limit() {
    let ca = TestCa::new();
    let cluster = ClusterId::mint();
    let b_id = NodeId::random();
    let a_id = NodeId::random();
    let a = Transport::bind(config(&ca, a_id, cluster), TestService::new(epoch(1, a_id)))
        .await
        .unwrap();
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
                    LONG,
                    true,
                )
                .await
            })
        })
        .collect();
    for (i, t) in tasks.into_iter().enumerate() {
        let reply = t.await.unwrap().unwrap();
        assert_eq!(reply.message, Message::Pong { nonce: i as u64 });
    }
    assert_eq!(b.stats().snapshot().shared_in, 200);
    let peak = service.peak.load(std::sync::atomic::Ordering::SeqCst);
    assert!(peak <= 16, "peak concurrency {peak}");
}

fn ping_frame(request_id: u64, nonce: u64) -> Vec<u8> {
    encode_frame(&Frame {
        request_id,
        trace: TraceContext::default(),
        message: Message::Ping { nonce },
    })
}

#[tokio::test]
async fn the_acceptor_answers_every_frame_on_a_shared_stream() {
    let (ca, p) = pair(caps::KNOWN).await;
    let conn = raw(&ca, p.cluster, p.b.local_addr()).await;
    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    frame::write_frame(&mut send, &[]).await.unwrap();
    frame::write_frame(&mut send, &ping_frame(10, 300))
        .await
        .unwrap();
    frame::write_frame(&mut send, &ping_frame(11, 0))
        .await
        .unwrap();
    send.finish().unwrap();
    let mut replies = Vec::new();
    let mut framed = tokio_util::codec::FramedRead::new(
        &mut recv,
        tokio_util::codec::LengthDelimitedCodec::new(),
    );
    while let Some(bytes) = futures::StreamExt::next(&mut framed).await {
        let reply = decode_frame(&bytes.unwrap(), &Limits::default()).unwrap();
        replies.push((reply.request_id, reply.message));
    }
    // The faster request answers first, and the stream ends after the last reply.
    assert_eq!(
        replies,
        vec![
            (11, Message::Pong { nonce: 0 }),
            (10, Message::Pong { nonce: 300 })
        ]
    );
}

#[tokio::test]
async fn a_bad_frame_resets_the_shared_stream_but_not_the_connection() {
    let (ca, p) = pair(caps::KNOWN).await;
    let conn = raw(&ca, p.cluster, p.b.local_addr()).await;
    let (mut send, mut recv) = conn.open_bi().await.unwrap();
    frame::write_frame(&mut send, &[]).await.unwrap();
    frame::write_frame(&mut send, &[1, 2, 3, 4, 5])
        .await
        .unwrap();
    let err = frame::read_frame(&mut recv, 1 << 20).await.unwrap_err();
    assert!(
        err.to_string()
            .contains(&format!("{}", quinn::VarInt::from_u32(RESET_BAD_FRAME))),
        "{err}"
    );
    assert_eq!(p.b.stats().snapshot().frames_rejected, 1);
    assert!(conn.close_reason().is_none());
    assert_eq!(p.ping(3, LONG, true).await.unwrap(), 3);
}
