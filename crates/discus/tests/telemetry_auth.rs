//! The auth attempt metric counts every result of the security authenticate path (spec §15.4).
//! It reports through the global metrics handle, so this test has a process of its own.

#![cfg(feature = "otel")]

use std::sync::Arc;

use bytes::Bytes;
use discus::node::{ConnState, Node};
use discus::security::{ConnAuth, Security};
use discus_telemetry::testing::TraceHarness;

fn cmd(parts: &[&str]) -> Vec<Bytes> {
    parts
        .iter()
        .map(|p| Bytes::copy_from_slice(p.as_bytes()))
        .collect()
}

#[test]
fn every_auth_attempt_is_counted() {
    let h = TraceHarness::new(0.0);
    assert!(discus_telemetry::install_global(h.telemetry.metrics()));
    let argv = ["discus", "--listen", "127.0.0.1:0", "--shared-token", "t"];
    let config = discus::Config::parse_from(argv).expect("valid test arguments");
    let security = Security::from_config(&config).expect("valid security configuration");
    let node = Node::new(config, Arc::new(security));
    let mut conn = ConnState::new(1, ConnAuth::default());
    for c in [
        &["AUTH", "wrong"][..],
        &["AUTH", "t"],
        &["HELLO", "3", "AUTH", "default", "nope"],
        &["HELLO", "3", "AUTH", "default", "t"],
        &["GET", "k"],
    ] {
        discus::telemetry::dispatch_with(discus_telemetry::global(), &node, &mut conn, &cmd(c));
    }
    let points = h.points();
    let count = |status: &str| -> f64 {
        points
            .iter()
            .filter(|p| p.name == "discus.auth.attempts")
            .filter(|p| p.attrs.iter().any(|(k, v)| k == "status" && v == status))
            .map(|p| p.value)
            .sum()
    };
    assert_eq!(count("failure"), 2.0);
    assert_eq!(count("success"), 2.0);
}
