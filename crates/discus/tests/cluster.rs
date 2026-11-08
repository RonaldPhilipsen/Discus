#![allow(clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{start_cluster, start_cluster_with, Cluster};
use discus_placement::{Epoch, EpochId, Member, MemberState, NodeId};
use discus_resp::Value;
use discus_transport::testing::TEST_SAN;
use discus_transport::{Inbound, Service, Transport, TransportConfig};
use discus_wire::{encode_command, Forward, ForwardMode, Message, TraceContext};
use futures::future::BoxFuture;
use redis::AsyncCommands;

/// An epoch of `generation` over the same members, where only `normal` stays NORMAL.
fn epoch_with_only(c: &Cluster, generation: u64, normal: NodeId) -> Epoch {
    let members: Vec<Member> = c.nodes[0]
        .node
        .epochs()
        .current()
        .members()
        .iter()
        .map(|m| Member {
            state: if m.id == normal {
                MemberState::Normal
            } else {
                MemberState::Leaving
            },
            ..m.clone()
        })
        .collect();
    let id = EpochId {
        generation,
        origin: normal,
    };
    Epoch::new(id, c.cluster_id, members).unwrap()
}

fn generation(c: &Cluster, i: usize) -> u64 {
    c.nodes[i].node.epochs().current().id().generation
}

#[tokio::test]
async fn set_on_one_node_get_on_the_other() {
    let c = start_cluster(2, &[]).await;
    let mut a = c.nodes[0].redis().await;
    let mut b = c.nodes[1].redis().await;
    for i in 0..200 {
        let key = format!("k:{i}");
        let _: () = a.set(&key, format!("v{i}")).await.unwrap();
        let got: String = b.get(&key).await.unwrap();
        assert_eq!(got, format!("v{i}"));
        let primary = c.nodes[0].primary(key.as_bytes());
        for n in &c.nodes {
            assert_eq!(n.holds(key.as_bytes()), n.id == primary, "{key}");
        }
    }
}

#[tokio::test]
async fn a_forwarded_write_returns_the_applied_hlc() {
    let c = start_cluster(2, &[]).await;
    let key = c.key_on(1, "hlc");
    let mut a = c.nodes[0].redis().await;
    let _: () = a.set(&key, "v").await.unwrap();
    let version = c.nodes[1]
        .node
        .store
        .read(key.as_bytes(), |e| e.unwrap().version);
    assert_eq!(version.origin(), c.nodes[1].id);
    assert!(c.nodes[0].node.hlc.last() >= version.time());
}

#[tokio::test]
async fn errors_cross_the_hop_unchanged() {
    let c = start_cluster(2, &[]).await;
    let key = c.key_on(1, "err");
    let mut a = c.nodes[0].redis().await;
    let _: () = a.set(&key, "abc").await.unwrap();
    let err = a.incr::<_, _, i64>(&key, 1).await.unwrap_err();
    assert_eq!(err.code(), Some("ERR"));
    assert!(err
        .to_string()
        .contains("value is not an integer or out of range"));
    let err = a.hset::<_, _, _, i64>(&key, "f", "v").await.unwrap_err();
    assert_eq!(err.code(), Some("WRONGTYPE"));
}

#[tokio::test]
async fn a_higher_epoch_is_adopted_before_acting() {
    let c = start_cluster(2, &[]).await;
    let key = c.key_on(1, "adopt");
    let members = c.nodes[0].node.epochs().current().members().to_vec();
    let next = c.nodes[0]
        .node
        .epochs()
        .propose(c.nodes[0].id, members)
        .unwrap();
    c.nodes[0].node.adopt_epoch(next).unwrap();
    let mut a = c.nodes[0].redis().await;
    let _: () = a.set(&key, "v").await.unwrap();
    assert_eq!(generation(&c, 1), 2);
}

#[tokio::test]
async fn without_epoch_push_the_receiver_keeps_its_epoch() {
    let c = start_cluster_with(2, |i| {
        if i == 0 {
            vec!["--cluster-capabilities".into(), "0".into()]
        } else {
            vec![]
        }
    })
    .await;
    let key = c.key_on(1, "nopush");
    let members = c.nodes[0].node.epochs().current().members().to_vec();
    let next = c.nodes[0]
        .node
        .epochs()
        .propose(c.nodes[0].id, members)
        .unwrap();
    c.nodes[0].node.adopt_epoch(next).unwrap();
    let mut a = c.nodes[0].redis().await;
    let _: () = a.set(&key, "v").await.unwrap();
    assert_eq!(generation(&c, 1), 1);
}

#[tokio::test]
async fn a_receiver_routes_again_within_the_hop_budget() {
    let c = start_cluster(3, &[]).await;
    let key = c.key_on(1, "reroute");
    let only_c = epoch_with_only(&c, 2, c.nodes[2].id);
    c.nodes[1].node.adopt_epoch(only_c).unwrap();
    let mut a = c.nodes[0].redis().await;
    let _: () = a.set(&key, "v").await.unwrap();
    assert!(c.nodes[2].holds(key.as_bytes()));
    assert!(!c.nodes[1].holds(key.as_bytes()));
    assert_eq!(generation(&c, 2), 2);
    let got: String = a.get(&key).await.unwrap();
    assert_eq!(got, "v");
}

#[tokio::test]
async fn a_spent_budget_falls_back_to_the_previous_owner() {
    let c = start_cluster(3, &[]).await;
    let key = c.key_on(1, "spent");
    c.nodes[1]
        .node
        .adopt_epoch(epoch_with_only(&c, 2, c.nodes[2].id))
        .unwrap();
    // C owned the key in its previous epoch (2), and A owns it in its current one (3).
    c.nodes[2]
        .node
        .adopt_epoch(epoch_with_only(&c, 2, c.nodes[2].id))
        .unwrap();
    c.nodes[2]
        .node
        .adopt_epoch(epoch_with_only(&c, 3, c.nodes[0].id))
        .unwrap();
    let mut a = c.nodes[0].redis().await;
    let _: () = a.set(&key, "v").await.unwrap();
    assert!(c.nodes[2].holds(key.as_bytes()));
}

#[tokio::test]
async fn a_spent_budget_without_a_previous_owner_tries_again() {
    let c = start_cluster(3, &[]).await;
    let key = c.key_on(1, "tryagain");
    c.nodes[1]
        .node
        .adopt_epoch(epoch_with_only(&c, 2, c.nodes[2].id))
        .unwrap();
    // C skips epoch 2: its previous epoch is the static one, where B is the primary.
    c.nodes[2]
        .node
        .adopt_epoch(epoch_with_only(&c, 3, c.nodes[0].id))
        .unwrap();
    let mut a = c.nodes[0].redis().await;
    let err = a.set::<_, _, ()>(&key, "v").await.unwrap_err();
    assert_eq!(err.code(), Some("TRYAGAIN"));
    assert!(err
        .to_string()
        .contains("the cluster is changing, try again later"));
    assert!(c.nodes.iter().all(|n| !n.holds(key.as_bytes())));
}

struct Idle(EpochId);

impl Service for Idle {
    fn epoch(&self) -> EpochId {
        self.0
    }

    fn call(self: Arc<Self>, _: Inbound) -> BoxFuture<'static, Message> {
        Box::pin(async { Message::Unsupported { message_type: 0 } })
    }
}

#[tokio::test]
async fn a_loop_is_detected() {
    let c = start_cluster(2, &[]).await;
    let b = &c.nodes[1];
    let me = NodeId::random();
    let epoch = c.nodes[0].node.epochs().current().id();
    let config = TransportConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        {
            let (cert, key) = c.ca.issue(TEST_SAN);
            discus_transport::TlsMaterial::from_pem(
                c.ca.ca_pem().as_bytes(),
                cert.as_bytes(),
                key.as_bytes(),
                TEST_SAN,
            )
            .unwrap()
        },
        me,
        c.cluster_id,
    );
    let t = Transport::bind(config, Arc::new(Idle(epoch)))
        .await
        .unwrap();
    let key = c.key_on(1, "loop");
    let forward = Forward {
        command: encode_command(&["GET".into(), key.into()]),
        epoch,
        epoch_body: None,
        hops: 1,
        path: vec![me, b.id],
        mode: ForwardMode::Route,
        track: false,
    };
    let reply = t
        .request(
            b.id,
            b.cluster_addr,
            Message::Forward(forward),
            TraceContext::default(),
            Duration::from_secs(2),
            false,
        )
        .await
        .unwrap();
    let Message::ForwardReply(r) = reply.message else {
        panic!("not a reply: {reply:?}");
    };
    let Value::Error(text) = r.value else {
        panic!("not an error: {:?}", r.value);
    };
    assert!(text.starts_with(b"TRYAGAIN"), "{text:?}");
}

#[tokio::test]
async fn local_keys_never_leave_the_node() {
    // The counter tier talks in the background. A long heartbeat keeps it quiet once every
    // follower holds its snapshot.
    let c = start_cluster(2, &["--counter-lease-renew-ms", "600000"]).await;
    let settled = |n: &common::cluster::ClusterNode| {
        n.node
            .counters()
            .unwrap()
            .status()
            .iter()
            .all(|g| g.synced && matches!(g.role, "leader" | "follower"))
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !c.nodes.iter().all(settled) {
        assert!(
            std::time::Instant::now() < deadline,
            "the counter groups did not settle"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let key = c.key_on(0, "local");
    let mut a = c.nodes[0].redis().await;
    let before = c.nodes[0].node.link().unwrap().transport.stats().snapshot();
    let _: () = a.set(&key, "v").await.unwrap();
    let _: String = a.get(&key).await.unwrap();
    let after = c.nodes[0].node.link().unwrap().transport.stats().snapshot();
    assert_eq!(after.requests_out, before.requests_out);
    assert!(c.nodes[0].holds(key.as_bytes()));
}
