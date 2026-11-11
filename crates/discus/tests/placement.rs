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

#[tokio::test]
async fn members_advertise_their_redirect_port() {
    // A static peer learns the client address of another member only from the list, so the
    // helper reserves the ports first.
    let (c, ports) = common::cluster::start_redirect_cluster(3, |_| vec![]).await;
    let first = c.nodes[0].node.epochs().current().clone();
    for n in &c.nodes {
        assert_eq!(**n.node.epochs().current(), *first);
        let epoch = n.node.epochs().current().clone();
        for (i, other) in c.nodes.iter().enumerate() {
            let m = epoch.member(other.id).unwrap();
            assert_eq!(m.client_addr, Some(ports[i]));
            assert_eq!(m.client_addr.unwrap().ip(), other.cluster_addr.ip());
        }
        assert_eq!(
            n.node.client_addr(),
            epoch.member(n.id).unwrap().client_addr
        );
    }
}

#[tokio::test]
async fn gossip_members_advertise_their_redirect_port() {
    let net = common::gossip::Net::new();
    let nodes = net
        .start_agreed(
            3,
            &[
                "--redirect-listen",
                "127.0.0.1:0",
                "--client-hostname",
                "{ip}.peers",
            ],
        )
        .await;
    for n in &nodes {
        let epoch = n.node.epochs().current().clone();
        for other in &nodes {
            let m = epoch.member(other.id).unwrap();
            let want = other.node.client_addr().unwrap();
            assert_ne!(want.port(), 0);
            assert_eq!(m.client_addr, Some(want));
            assert_eq!(m.client_host.as_deref(), Some("127-0-0-1.peers"));
        }
    }
}

#[tokio::test]
async fn an_empty_client_hostname_leaves_the_member_without_a_host() {
    let ports: Vec<std::net::SocketAddr> = ["127.0.0.1:6380", "127.0.0.1:6381"]
        .map(|a| a.parse().unwrap())
        .to_vec();
    let c = common::cluster::start_cluster_clients(
        2,
        |i| Some(ports[i]),
        |i| {
            vec![
                "--client-advertise".into(),
                ports[i].to_string(),
                "--client-hostname".into(),
                "".into(),
            ]
        },
    )
    .await;
    let first = c.nodes[0].node.epochs().current().clone();
    for n in &c.nodes {
        assert_eq!(**n.node.epochs().current(), *first);
        assert_eq!(n.node.client_host(), None);
    }
    for (i, n) in c.nodes.iter().enumerate() {
        let m = first.member(n.id).unwrap();
        assert_eq!(m.client_addr, Some(ports[i]));
        assert_eq!(m.client_host, None);
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
        "--membership",
        "static",
    ]))
    .unwrap();
    let err = discus::Server::bind(config).await.err().unwrap();
    assert!(err.to_string().contains("--weight"), "{err}");
}

#[test]
fn a_bare_address_is_a_seed() {
    let config = Config::parse_from(["discus", "--peer", "127.0.0.1:7379"]).unwrap();
    assert_eq!(config.peers[0].id, None);
    assert_eq!(config.peers[0].addr, "127.0.0.1:7379".parse().unwrap());
}

#[test]
fn static_peers_need_their_ids() {
    let mut argv = cluster_args(&["--membership", "static", "--peer", "127.0.0.1:2"]);
    argv.extend(
        [
            "--cluster-ca",
            "a",
            "--cluster-cert",
            "b",
            "--cluster-key",
            "c",
        ]
        .map(String::from),
    );
    let err = Config::parse_from(argv).unwrap().validate().unwrap_err();
    assert!(err.contains("<node-id>@<address>"), "{err}");
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
