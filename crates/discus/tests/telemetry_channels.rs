//! The transport metrics carry a `channel` attribute: the gossip and RPC endpoints of a gossip
//! node report apart. They report through the global metrics handle, so this test has a process
//! of its own.

#![cfg(feature = "otel")]
#![allow(clippy::unwrap_used)]

mod common;

use std::time::Duration;

use common::gossip::{fill, wait_until, Net};
use discus_telemetry::testing::TraceHarness;

fn requests(h: &TraceHarness, name: &str, channel: &str) -> f64 {
    h.points()
        .iter()
        .filter(|p| p.name == name)
        .filter(|p| p.attrs.iter().any(|(k, v)| k == "channel" && v == channel))
        .map(|p| p.value)
        .sum()
}

#[tokio::test]
async fn gossip_and_rpc_traffic_report_on_their_own_channels() {
    let h = TraceHarness::new(0.0);
    assert!(discus_telemetry::install_global(h.telemetry.metrics()));
    let net = Net::new();
    let nodes = net.start_agreed(2, &[]).await;
    // Both nodes live in this process and share the global handle, so the last node to register
    // reports. Writing through each node makes every node forward some keys.
    for (i, n) in nodes.iter().enumerate() {
        fill(n, &format!("ch{i}-"), 100).await;
    }

    let give_up = Duration::from_secs(60);
    assert!(
        wait_until(give_up, || requests(
            &h,
            "discus.transport.requests.out",
            "rpc"
        ) > 0.0)
        .await,
        "no rpc requests were counted"
    );
    assert!(
        wait_until(give_up, || requests(
            &h,
            "discus.transport.requests.in",
            "gossip"
        ) > 0.0)
        .await,
        "no gossip requests were counted"
    );
    let points = h.points();
    assert!(points
        .iter()
        .filter(|p| p.name.starts_with("discus.transport."))
        .all(|p| p
            .attrs
            .iter()
            .any(|(k, v)| k == "channel" && (v == "gossip" || v == "rpc"))));
}
