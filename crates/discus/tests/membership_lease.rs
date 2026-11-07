#![allow(clippy::unwrap_used)]

mod common;

use std::time::Duration;

use common::gossip::{agreed, wait_until, Net};

#[tokio::test]
async fn an_api_server_outage_keeps_the_cluster_ready() {
    let net = Net::new();
    let nodes = net.start_all(2, &[]).await;
    assert!(wait_until(Duration::from_secs(10), || agreed(&[&nodes[0], &nodes[1]])).await);
    net.lease.set_available(false);
    let lonely = net.start(&[], &[]).await;
    for _ in 0..15 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        for n in &nodes {
            assert_eq!(common::status(n.admin, "/readyz").await, 200);
        }
        assert_eq!(common::status(lonely.admin, "/readyz").await, 503);
    }
    assert!(!lonely.is_normal());
}

#[tokio::test]
async fn a_cold_restart_forms_a_new_cluster() {
    let net = Net::with_lease_ms(1_000);
    let old = net.start_all(2, &[]).await;
    assert!(wait_until(Duration::from_secs(10), || agreed(&[&old[0], &old[1]])).await);
    let old_cluster = old[0].node.cluster_id();
    for n in &old {
        n.kill();
    }
    let fresh = net.start_all(2, &[]).await;
    assert!(wait_until(Duration::from_secs(10), || agreed(&[&fresh[0], &fresh[1]])).await);
    assert_ne!(fresh[0].node.cluster_id(), old_cluster);
    assert_eq!(
        net.lease.holder().unwrap().cluster_id,
        fresh[0].node.cluster_id()
    );
}
