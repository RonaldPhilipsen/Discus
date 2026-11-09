#![allow(clippy::unwrap_used)]

mod common;

use std::time::Duration;

use common::gossip::Net;

#[tokio::test]
async fn an_api_server_outage_keeps_the_cluster_ready() {
    let net = Net::new();
    let nodes = net.start_agreed(2, &[]).await;
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
    let old = net.start_agreed(2, &[]).await;
    let old_cluster = old[0].node.cluster_id();
    for n in &old {
        n.kill();
    }
    let fresh = net.start_agreed(2, &[]).await;
    assert_ne!(fresh[0].node.cluster_id(), old_cluster);
    assert_eq!(
        net.lease.holder().unwrap().cluster_id,
        fresh[0].node.cluster_id()
    );
}
