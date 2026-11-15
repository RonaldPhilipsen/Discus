//! The connection pool: dials, failed dials, and the deadline of a request.

#![allow(clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{config, start};
use discus_placement::{ClusterId, NodeId};
use discus_transport::testing::TestCa;
use discus_wire::{Message, TraceContext};

#[tokio::test]
async fn requests_to_a_silent_address_fail_within_their_deadline() {
    let ca = TestCa::new();
    let mut c = config(&ca, NodeId::random(), ClusterId::mint());
    c.handshake_timeout = Duration::from_secs(1);
    let (a, _) = start(c).await;
    // A bound socket that nobody reads: it drops every packet without an ICMP error.
    let silent = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let addr = silent.local_addr().unwrap();
    let node = NodeId::random();
    let a = Arc::new(a);
    let started = Instant::now();
    let tasks: Vec<_> = (0..20u64)
        .map(|nonce| {
            let a = a.clone();
            tokio::spawn(async move {
                a.request(
                    node,
                    addr,
                    Message::Ping { nonce },
                    TraceContext::default(),
                    Duration::from_millis(300),
                    true,
                )
                .await
            })
        })
        .collect();
    for t in tasks {
        let err = t.await.unwrap().unwrap_err();
        assert!(err.is_unsent(), "{err:?}");
    }
    // One dial at a time under the slot lock took about 20 handshake timeouts.
    let elapsed = started.elapsed();
    assert!(elapsed < Duration::from_secs(8), "took {elapsed:?}");
    // A request just after the failed dial gets its error at once, without a new dial.
    tokio::time::sleep(Duration::from_millis(900)).await;
    let failures = a.stats().snapshot().handshake_failures;
    assert!(failures <= 2, "{failures} handshake failures");
    let started = Instant::now();
    let err = a
        .request(
            node,
            addr,
            Message::Ping { nonce: 99 },
            TraceContext::default(),
            Duration::from_secs(10),
            true,
        )
        .await
        .unwrap_err();
    assert!(err.is_unsent(), "{err:?}");
    let elapsed = started.elapsed();
    assert!(elapsed < Duration::from_secs(3), "took {elapsed:?}");
    drop(silent);
}

#[tokio::test]
async fn a_dial_cut_short_by_the_deadline_still_completes() {
    let ca = TestCa::new();
    let cluster = ClusterId::mint();
    let b_id = NodeId::random();
    let (a, _) = start(config(&ca, NodeId::random(), cluster)).await;
    let (b, _) = start(config(&ca, b_id, cluster)).await;
    let err = a
        .request(
            b_id,
            b.local_addr(),
            Message::Ping { nonce: 1 },
            TraceContext::default(),
            Duration::ZERO,
            true,
        )
        .await
        .unwrap_err();
    assert!(err.is_unsent(), "{err:?}");
    assert_eq!(a.stats().snapshot().requests_out, 0);
    // The dial goes on without its caller, and the next request takes its connection.
    let reply = a
        .request(
            b_id,
            b.local_addr(),
            Message::Ping { nonce: 2 },
            TraceContext::default(),
            Duration::from_secs(10),
            true,
        )
        .await
        .unwrap();
    assert_eq!(reply.message, Message::Pong { nonce: 2 });
    assert_eq!(a.stats().snapshot().handshake_failures, 0);
}
