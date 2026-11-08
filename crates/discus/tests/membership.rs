#![allow(clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::gossip::{agreed, socket, wait_until, GossipNode, Net, Opts};
use discus::membership::Event;
use discus_membership::{LeaseProvider, NoneLease};
use discus_placement::mock::MockClock;
use discus_placement::{Clock, ClusterId, EpochId, NodeId};
use discus_transport::testing::TEST_SAN;
use discus_transport::{Inbound, Service, Transport, TransportConfig};
use discus_wire::Message;
use futures::future::BoxFuture;
use redis::AsyncCommands;

const FORM: Duration = Duration::from_secs(10);

fn refs(nodes: &[GossipNode]) -> Vec<&GossipNode> {
    nodes.iter().collect()
}

#[tokio::test]
async fn three_nodes_form_one_cluster() {
    let net = Net::new();
    let nodes = net.start_all(3, &[]).await;
    assert!(wait_until(FORM, || agreed(&refs(&nodes))).await);
    let holder = net.lease.holder().unwrap();
    assert_eq!(holder.cluster_id, nodes[0].node.cluster_id());
    for n in &nodes {
        assert_eq!(common::status(n.admin, "/readyz").await, 200);
    }
    // Startup also waits for the handoff of each joiner, which follows the epoch shortly.
    for n in &nodes {
        let mut startup = 503;
        for _ in 0..250 {
            startup = common::status(n.admin, "/startupz").await;
            if startup == 200 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(startup, 200);
    }
    let mut a = nodes[0].redis().await;
    let mut b = nodes[2].redis().await;
    for i in 0..50 {
        let _: () = a.set(format!("k{i}"), i).await.unwrap();
    }
    for i in 0..50 {
        let v: i64 = b.get(format!("k{i}")).await.unwrap();
        assert_eq!(v, i);
    }
}

#[tokio::test]
async fn five_nodes_join_one_by_one() {
    let net = Net::new();
    let first = net.start(&[], &[]).await;
    assert!(wait_until(FORM, || first.is_normal()).await);
    let mut nodes = vec![first];
    for _ in 0..4 {
        let seed = nodes[0].cluster_addr;
        nodes.push(net.start(&[seed], &[]).await);
    }
    assert!(wait_until(FORM, || agreed(&refs(&nodes))).await);
}

#[tokio::test]
async fn a_dead_node_leaves_the_epoch() {
    let net = Net::new();
    let nodes = net.start_all(4, &[]).await;
    assert!(wait_until(FORM, || agreed(&refs(&nodes))).await);
    let dead = &nodes[3];
    let key = (0..)
        .map(|i| format!("gone:{i}"))
        .find(|k| nodes[0].node.owners(k.as_bytes()).first() == Some(&dead.id))
        .unwrap();
    dead.kill();
    let rest = refs(&nodes[..3]);
    assert!(wait_until(Duration::from_secs(5), || agreed(&rest)).await);
    let mut c = nodes[0].redis().await;
    let v: Option<String> = c.get(&key).await.unwrap();
    assert_eq!(v, None);
}

#[tokio::test]
async fn a_join_sets_the_hlc_floor() {
    const SLOT: usize = 20;
    let clock = MockClock::<SLOT>;
    let now = discus_placement::SystemClock.now_ms();
    clock.set(now + 800);
    let net = Net::new();
    let opts = Opts {
        clocks: Some((clock.hlc_clock(), Arc::new(clock))),
        ..Opts::default()
    };
    let (s, addr) = socket();
    let ahead = net.start_on(s, &[], &[], opts).await;
    assert!(wait_until(FORM, || ahead.is_normal()).await);
    let floor = ahead.node.hlc.current();
    let joiner = net.start(&[addr], &[]).await;
    assert!(wait_until(FORM, || joiner.is_normal()).await);
    assert!(
        joiner.node.hlc.last() >= floor,
        "{:?} < {floor:?}",
        joiner.node.hlc.last()
    );
}

#[tokio::test]
async fn the_bootstrap_node_starts_from_the_wall_clock() {
    const SLOT: usize = 21;
    let clock = MockClock::<SLOT>;
    clock.set(discus_placement::SystemClock.now_ms());
    let net = Net::new();
    let opts = Opts {
        clocks: Some((clock.hlc_clock(), Arc::new(clock))),
        ..Opts::default()
    };
    let node = net.start_on(socket().0, &[], &[], opts).await;
    assert!(wait_until(FORM, || node.is_normal()).await);
    let mut c = node.redis().await;
    let _: () = c.set("k", "v").await.unwrap();
    let stamp = node.node.store.read(b"k", |e| e.unwrap().version.time());
    let wall = (clock.hlc_clock())();
    let diff = stamp.to_duration().abs_diff(wall.to_duration());
    assert!(diff < Duration::from_millis(5), "{diff:?}");
}

/// A peer that completes HELLO as a member of a formed cluster and never answers gossip.
struct Silent;

impl Service for Silent {
    fn epoch(&self) -> EpochId {
        EpochId {
            generation: 5,
            origin: NodeId::from_u128(1),
        }
    }

    fn call(self: Arc<Self>, inbound: Inbound) -> BoxFuture<'static, Message> {
        Box::pin(async move {
            match inbound.message {
                Message::Ping { nonce } => Message::Pong { nonce },
                _ => std::future::pending().await,
            }
        })
    }
}

#[tokio::test]
async fn no_writes_while_a_normal_peer_is_silent() {
    let net = Net::new();
    let tc = TransportConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        net.ca.material(TEST_SAN),
        NodeId::random(),
        ClusterId::mint(),
    );
    let silent = Transport::bind(tc, Arc::new(Silent)).await.unwrap();
    let node = net.start(&[silent.local_addr()], &[]).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!node.is_normal());
    assert_eq!(common::status(node.admin, "/readyz").await, 503);
    assert_eq!(common::status(node.admin, "/startupz").await, 503);
    let reply = common::roundtrip(node.addr, b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n").await;
    assert!(reply.starts_with("-TRYAGAIN"), "{reply}");
    assert!(net.lease.holder().is_none());
}

#[tokio::test]
async fn a_self_seed_is_ignored() {
    let net = Net::new();
    let (s, addr) = socket();
    let node = net.start_on(s, &[addr], &[], Opts::default()).await;
    assert!(wait_until(FORM, || node.is_normal()).await);
    assert_eq!(node.node.epochs().current().members().len(), 1);
}

#[tokio::test]
async fn two_clusters_merge_into_one() {
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
    assert!(wait_until(FORM, || agreed(&[&b, &c])).await);
    assert_ne!(lone.node.cluster_id(), b.node.cluster_id());
    let mut conn = lone.redis().await;
    let _: () = conn.set("lost", "1").await.unwrap();
    lone.node.membership().unwrap().add_seed(b_addr);
    assert!(wait_until(Duration::from_secs(20), || agreed(&[&lone, &b, &c])).await);
    assert_eq!(lone.node.cluster_id(), b.node.cluster_id());
    assert!(!lone.node.store.read(b"lost", |e| e.is_some()));
}

#[tokio::test]
async fn ownership_gained_reaches_subscribers() {
    let net = Net::new();
    let rf = ["--replication-factor", "3"];
    let nodes = net.start_all(3, &rf).await;
    assert!(wait_until(FORM, || agreed(&refs(&nodes))).await);
    let mut events = nodes[0].node.membership().unwrap().subscribe();
    let joiner = net.start(&[nodes[0].cluster_addr], &rf).await;
    let gained = tokio::time::timeout(FORM, async {
        loop {
            if let Ok(Event::OwnershipGained(g)) = events.recv().await {
                if g.gained.contains(&joiner.id) {
                    return g;
                }
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(gained.pin.id(), gained.old);
    assert!(nodes[0].node.epochs().get(gained.old).is_some());
}

#[tokio::test]
async fn a_join_leaves_serving_nodes_ready() {
    let net = Net::new();
    let nodes = net.start_all(3, &[]).await;
    assert!(wait_until(FORM, || agreed(&refs(&nodes))).await);
    let joiner = net.start(&[nodes[1].cluster_addr], &[]).await;
    let mut settled = None;
    let deadline = tokio::time::Instant::now() + FORM;
    while tokio::time::Instant::now() < deadline {
        for n in &nodes {
            assert_eq!(common::status(n.admin, "/readyz").await, 200);
        }
        if joiner.is_normal() && settled.is_none() {
            settled = Some(tokio::time::Instant::now());
        }
        if settled.is_some_and(|t| t.elapsed() > Duration::from_secs(1)) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the joiner did not settle");
}

#[tokio::test]
async fn static_mode_keeps_the_static_epoch() {
    let c = common::cluster::start_cluster(2, &[]).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    for n in &c.nodes {
        assert!(n.node.membership().is_none());
        assert_eq!(n.node.epochs().current().id().generation, 1);
        assert!(n.node.is_normal());
    }
}
