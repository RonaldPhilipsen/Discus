#![allow(clippy::unwrap_used)]

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::gossip::{socket, wait_agreed, wait_until, GossipNode, Net, Opts};
use discus::lag::LagProbe;
use discus_membership::{LeaseProvider, NoneLease};
use redis::AsyncCommands;
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lag_probe_reports_a_blocked_runtime() {
    let stop = CancellationToken::new();
    let probe = LagProbe::new();
    probe.spawn(stop.clone());
    tokio::time::sleep(Duration::from_millis(100)).await;

    let blocked = Arc::new(AtomicBool::new(true));
    let poller = {
        let (probe, blocked) = (probe.clone(), blocked.clone());
        std::thread::spawn(move || {
            let mut seen = false;
            while blocked.load(Ordering::Acquire) {
                seen |= probe.lag_ms() >= 400;
                std::thread::sleep(Duration::from_millis(5));
            }
            seen
        })
    };
    let blockers: Vec<_> = (0..2)
        .map(|_| tokio::spawn(async { std::thread::sleep(Duration::from_secs(2)) }))
        .collect();
    for b in blockers {
        b.await.unwrap();
    }
    blocked.store(false, Ordering::Release);
    assert!(poller.join().unwrap(), "a blocked runtime must show as lag");

    let give_up = Instant::now() + Duration::from_secs(30);
    while probe.lag_ms() >= 400 {
        assert!(Instant::now() < give_up, "lag did not recover");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    stop.cancel();
}

const FORM: Duration = Duration::from_secs(20);

/// A key whose primary in `n`'s current epoch is another node.
fn key_elsewhere(n: &GossipNode, prefix: &str) -> String {
    (0..)
        .map(|i| format!("{prefix}{i}"))
        .find(|k| n.primary(k.as_bytes()) != n.id)
        .unwrap()
}

#[tokio::test]
async fn members_carry_both_addresses_and_forwards_use_rpc() {
    let net = Net::new();
    let nodes = net.start_agreed(3, &[]).await;
    let epoch = nodes[0].node.epochs().current().clone();
    for n in &nodes {
        let m = epoch.member(n.id).unwrap();
        assert_eq!(m.addr, n.cluster_addr);
        assert_eq!(m.rpc_addr, n.rpc_addr);
        assert_ne!(m.addr, m.rpc_addr);
    }

    let key = key_elsewhere(&nodes[0], "fwd");
    let mut c = nodes[0].redis().await;
    let _: () = c.set(&key, "v").await.unwrap();
    let rpc = nodes[0].node.link().unwrap().transport.stats().snapshot();
    let membership = nodes[0].node.membership().unwrap();
    let gossip = membership.gossip_transport().stats().snapshot();
    assert!(rpc.requests_out > 0, "{rpc:?}");
    assert_eq!(gossip.requests_out, 0, "{gossip:?}");
}

#[tokio::test]
async fn forward_timeouts_reach_membership() {
    let net = Net::new();
    let nodes = net
        .start_agreed(3, &["--forward-read-timeout-ms", "300"])
        .await;
    let key = key_elsewhere(&nodes[0], "slow");
    let owner = nodes[0].primary(key.as_bytes());
    let owner = nodes.iter().find(|n| n.id == owner).unwrap();
    owner
        .node
        .faults
        .stall_forwards
        .store(true, Ordering::Relaxed);
    let membership = nodes[0].node.membership().unwrap().clone();
    let before = membership.evidence_received();

    let mut c = nodes[0].redis().await;
    let reply: redis::RedisResult<Option<String>> = c.get(&key).await;
    assert!(reply.is_err(), "a stalled forward must fail: {reply:?}");
    assert!(
        wait_until(Duration::from_secs(30), || membership.evidence_received()
            > before)
        .await,
        "the forward timeout never reached membership"
    );
}

#[tokio::test]
async fn cluster_reset_updates_both_transports() {
    let net = Net::new();
    let none = || Opts {
        lease: Some(Arc::new(NoneLease) as Arc<dyn LeaseProvider>),
        ..Opts::default()
    };
    let lone = net.start_on(socket().0, &[], &[], none()).await;
    let (s, b_addr) = socket();
    let b = net.start_on(s, &[], &[], none()).await;
    assert!(wait_until(FORM, || lone.is_normal() && b.is_normal()).await);
    let c = net.start_on(socket().0, &[b_addr], &[], none()).await;
    wait_agreed(&[&b, &c], FORM).await;
    assert_ne!(lone.node.cluster_id(), b.node.cluster_id());

    lone.node.membership().unwrap().add_seed(b_addr);
    wait_agreed(&[&lone, &b, &c], FORM).await;
    let want = b.node.cluster_id();
    assert_eq!(lone.node.cluster_id(), want);
    let rpc = &lone.node.link().unwrap().transport;
    let gossip = lone.node.membership().unwrap().gossip_transport();
    assert_eq!(rpc.cluster_id(), want);
    assert_eq!(gossip.cluster_id(), want);
}
