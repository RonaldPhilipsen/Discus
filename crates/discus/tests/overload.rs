#![allow(clippy::unwrap_used)]

mod common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
    let rpc = || nodes[0].node.link().unwrap().transport.stats().snapshot();
    let before = rpc();
    let _: () = c.set(&key, "v").await.unwrap();
    let after = rpc();
    assert!(after.requests_out > before.requests_out, "{after:?}");
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

/// `GET /healthz` over a blocking socket. True on a 200 answer.
fn healthz_ok(admin: std::net::SocketAddr) -> bool {
    use std::io::{Read, Write};
    let Ok(mut s) = std::net::TcpStream::connect(admin) else {
        return false;
    };
    let _ = s.set_read_timeout(Some(Duration::from_secs(30)));
    if s.write_all(b"GET /healthz HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .is_err()
    {
        return false;
    }
    let mut reply = String::new();
    let _ = s.read_to_string(&mut reply);
    reply.starts_with("HTTP/1.1 200")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_lane_answers_while_data_workers_are_blocked() {
    const CAP: Duration = Duration::from_secs(60);
    let net = Net::new();
    let nodes = net.start_agreed(2, &[]).await;
    let admin = nodes[0].admin;
    let membership = nodes[0].node.membership().unwrap().clone();
    let before = membership.gossip_handled();

    // Only the observer's success, or a blocker at the cap, ends the block.
    let blocked = Arc::new(AtomicBool::new(true));
    let started = Arc::new(AtomicUsize::new(0));
    // The test body and its sockets may depend on the blocked workers, so a plain thread with
    // blocking IO observes the node.
    let (tx, rx) = tokio::sync::oneshot::channel();
    {
        let (blocked, started) = (blocked.clone(), started.clone());
        std::thread::spawn(move || {
            while started.load(Ordering::Acquire) < 2 {
                std::thread::sleep(Duration::from_millis(5));
            }
            let mut answered = false;
            let mut gossiped = false;
            while blocked.load(Ordering::Acquire) {
                answered |= healthz_ok(admin);
                gossiped |= membership.gossip_handled() > before;
                if answered && gossiped {
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            // A blocker that hit the cap cleared the flag first, so the swap reports it.
            let in_time = blocked.swap(false, Ordering::AcqRel);
            let _ = tx.send((answered && in_time, gossiped && in_time));
        });
    }
    let blockers: Vec<_> = (0..2)
        .map(|_| {
            let (blocked, started) = (blocked.clone(), started.clone());
            tokio::spawn(async move {
                started.fetch_add(1, Ordering::AcqRel);
                let give_up = Instant::now() + CAP;
                while blocked.load(Ordering::Acquire) {
                    if Instant::now() >= give_up {
                        blocked.store(false, Ordering::Release);
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
            })
        })
        .collect();
    for b in blockers {
        b.await.unwrap();
    }
    let (answered, gossiped) = rx.await.unwrap();
    assert!(
        answered,
        "/healthz must answer while the data workers are blocked"
    );
    assert!(
        gossiped,
        "gossip must be handled while the data workers are blocked"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn epoch_work_runs_off_the_control_thread() {
    let net = Net::new();
    let nodes = net.start_agreed(2, &[]).await;
    let joiner = net.start(&[nodes[0].cluster_addr], &[]).await;
    wait_agreed(&[&nodes[0], &nodes[1], &joiner], FORM).await;
    let threads = nodes[0].node.faults.epoch_work_threads.lock().clone();
    assert!(!threads.is_empty());
    assert!(threads.iter().all(|t| t != "discus-control"), "{threads:?}");
}

#[tokio::test]
async fn departure_is_gossiped_on_shutdown() {
    let net = Net::new();
    // Suspicion cannot declare the node dead within the wait, so only its goodbye removes it.
    let slow = [
        "--suspicion-min-ms",
        "600000",
        "--suspicion-max-ms",
        "600000",
        "--phi-dead",
        "16",
    ];
    let nodes = net.start_agreed(3, &slow).await;
    nodes[2].shutdown.cancel();
    let rest = [&nodes[0], &nodes[1]];
    wait_agreed(&rest, Duration::from_secs(120)).await;
    assert!(nodes[0]
        .node
        .epochs()
        .current()
        .member(nodes[2].id)
        .is_none());
}

#[tokio::test]
async fn late_epoch_work_keeps_the_queue_of_a_peer_that_joined_later() {
    let c = common::cluster::start_cluster(2, &["--replication-factor", "2"]).await;
    let (a, p) = (&c.nodes[0], &c.nodes[1]);
    assert!(wait_until(FORM, || a.node.phase() == discus::Phase::Serving).await);
    a.node.faults.epoch_work_threads.lock().clear();
    // P never answers A, so the queued key stays in P's queue or in flight.
    p.node.faults.blocked.lock().insert(a.id);
    let all = a.node.epochs().current().members().to_vec();
    let without_p: Vec<_> = all.iter().filter(|m| m.id != p.id).cloned().collect();
    let e1 = a.node.epochs().propose(a.id, without_p).unwrap();
    a.node.adopt_epoch(e1).unwrap();
    let e2 = a.node.epochs().propose(a.id, all).unwrap();
    a.node.adopt_epoch(e2).unwrap();
    // The test runtime has one thread and nothing above yielded, so the work of both adoptions
    // is still queued when the write reaches P's queue.
    assert!(a.node.faults.epoch_work_threads.lock().is_empty());
    let key = bytes::Bytes::from_static(b"late");
    let replicator = a.node.replicator().unwrap().clone();
    replicator.written(&a.node, &key);
    assert!(!replicator.settled(&key));
    assert!(
        wait_until(FORM, || a.node.faults.epoch_work_threads.lock().len() == 2).await,
        "the epoch work did not run"
    );
    assert!(
        !replicator.settled(&key),
        "the work of the first adoption dropped the queue of P"
    );
}
