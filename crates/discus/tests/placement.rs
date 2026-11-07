#![allow(clippy::unwrap_used)]

mod common;

use discus::security::Security;
use discus::{Config, Node};
use discus_placement::{ClusterId, EpochId, MemberState};

fn node(args: &[&str]) -> Node {
    let mut argv = vec!["discus"];
    argv.extend_from_slice(args);
    let config = Config::parse_from(argv).unwrap();
    let security = Security::from_config(&config).unwrap();
    Node::new(config, std::sync::Arc::new(security))
}

#[test]
fn node_bootstraps_epoch_one_from_itself() {
    let n = node(&[]);
    let epochs = n.epochs();
    let current = epochs.current();
    assert_eq!(
        current.id(),
        EpochId {
            generation: 1,
            origin: n.id
        }
    );
    assert_eq!(current.members().len(), 1);
    let me = &current.members()[0];
    assert_eq!(me.id, n.id);
    assert_eq!(me.state, MemberState::Normal);
    assert_eq!(me.weight.get(), 1.0);
    assert_eq!(current.cluster_id(), n.cluster_id());
    assert_ne!(n.cluster_id(), ClusterId::NIL);
}

#[test]
fn a_single_node_owns_every_key() {
    let n = node(&[]);
    for i in 0..100 {
        assert_eq!(n.owners(format!("key:{i}").as_bytes()), vec![n.id]);
    }
}

#[test]
fn the_weight_flag_reaches_the_member() {
    let n = node(&["--weight", "2.5", "--replication-factor", "2"]);
    assert_eq!(n.epochs().current().members()[0].weight.get(), 2.5);
    assert_eq!(n.config.replication_factor, 2);
}

#[test]
fn two_nodes_mint_different_identities() {
    let (a, b) = (node(&[]), node(&[]));
    assert_ne!(a.id, b.id);
    assert_ne!(a.cluster_id(), b.cluster_id());
}

#[test]
fn bad_placement_settings_are_refused() {
    for args in [
        ["discus", "--replication-factor", "0"],
        ["discus", "--weight", "0"],
        ["discus", "--weight", "-1"],
        ["discus", "--weight", "inf"],
    ] {
        assert!(Config::parse_from(args).is_err(), "{args:?}");
    }
    let c = Config::parse_from(["discus"]).unwrap();
    assert_eq!(c.replication_factor, 3);
    assert_eq!(c.weight, 1.0);
}

#[tokio::test]
async fn info_reports_identity_and_epoch() {
    let addr = common::start_node(&[]).await;
    let client = redis::Client::open(format!("redis://{addr}")).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();
    let info: String = redis::cmd("INFO")
        .arg("discus")
        .query_async(&mut con)
        .await
        .unwrap();
    let field = |name: &str| {
        info.lines()
            .find_map(|l| l.strip_prefix(&format!("{name}:")))
            .unwrap_or_else(|| panic!("missing {name} in {info}"))
            .trim()
            .to_string()
    };
    let id = field("discus_node_id");
    assert_eq!(id.len(), 32);
    assert!(id.bytes().all(|b| b.is_ascii_hexdigit()));
    assert_eq!(field("discus_epoch"), format!("1:{id}"));
    assert_eq!(field("discus_cluster_id").len(), 32);
    assert_ne!(field("discus_cluster_id"), id);
    assert_eq!(field("discus_members"), "1");
    assert_eq!(field("discus_replication_factor"), "3");
    assert_eq!(field("discus_hlc_pinned"), "0");
    assert_eq!(field("discus_hlc_refusals"), "0");
}

#[tokio::test]
async fn static_peers_form_the_same_epoch_on_every_node() {
    let c = common::cluster::start_cluster(3, &[]).await;
    let lowest = c.nodes.iter().map(|n| n.id).min().unwrap();
    let first = c.nodes[0].node.epochs().current().clone();
    assert_eq!(
        first.id(),
        EpochId {
            generation: 1,
            origin: lowest
        }
    );
    assert_eq!(first.members().len(), 3);
    for n in &c.nodes {
        let m = first.member(n.id).unwrap();
        assert_eq!(m.state, MemberState::Normal);
        assert_eq!(m.addr, n.cluster_addr);
        assert_eq!(**n.node.epochs().current(), *first);
    }
}

fn cluster_args(extra: &[&str]) -> Vec<String> {
    let me = discus_placement::NodeId::random();
    let peer = discus_placement::NodeId::random();
    let mut argv: Vec<String> = vec![
        "discus".into(),
        "--listen".into(),
        "127.0.0.1:0".into(),
        "--admin-listen".into(),
        "127.0.0.1:0".into(),
        "--cluster-listen".into(),
        "127.0.0.1:0".into(),
        "--node-id".into(),
        me.to_string(),
        "--cluster-id".into(),
        ClusterId::mint().to_string(),
        "--peer".into(),
        format!("{peer}@127.0.0.1:1"),
    ];
    argv.extend(extra.iter().map(|s| s.to_string()));
    argv
}

#[tokio::test]
async fn peers_without_certificates_are_refused() {
    let config = Config::parse_from(cluster_args(&[])).unwrap();
    let err = discus::Server::bind(config).await.err().unwrap();
    assert!(err.to_string().contains("--cluster-ca"), "{err}");

    let config = Config::parse_from(cluster_args(&[
        "--cluster-ca",
        "/nonexistent/ca.pem",
        "--cluster-cert",
        "/nonexistent/cert.pem",
        "--cluster-key",
        "/nonexistent/key.pem",
        "--weight",
        "2",
    ]))
    .unwrap();
    let err = discus::Server::bind(config).await.err().unwrap();
    assert!(err.to_string().contains("--weight"), "{err}");
}

#[test]
fn bad_peer_syntax_is_refused() {
    let id = discus_placement::NodeId::random();
    for bad in [
        "nothex@127.0.0.1:1".to_string(),
        id.to_string(),
        format!("{id}@nowhere"),
    ] {
        assert!(
            Config::parse_from(["discus", "--peer", bad.as_str()]).is_err(),
            "{bad}"
        );
    }
}
