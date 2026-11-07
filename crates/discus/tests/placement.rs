#![allow(clippy::unwrap_used)]

mod common;

use discus::{Config, Node};
use discus_placement::{ClusterId, EpochId, MemberState};

fn node(args: &[&str]) -> Node {
    let mut argv = vec!["discus"];
    argv.extend_from_slice(args);
    Node::new(Config::parse_from(argv).unwrap())
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
