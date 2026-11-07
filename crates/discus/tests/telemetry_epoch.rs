//! The `discus.ring.epoch` gauge reports the real epoch generation and follows
//! `Node::adopt_epoch` (spec §15.4). It reports through the global metrics handle, so this test
//! has a process of its own.

#![cfg(feature = "otel")]
#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use discus::node::Node;
use discus::security::Security;
use discus_placement::{Epoch, EpochId};
use discus_telemetry::testing::TraceHarness;

fn gauge(h: &TraceHarness) -> f64 {
    h.points()
        .iter()
        .find(|p| p.name == "discus.ring.epoch")
        .map(|p| p.value)
        .unwrap()
}

#[test]
fn the_epoch_gauge_follows_the_adopted_epoch() {
    let h = TraceHarness::new(0.0);
    assert!(discus_telemetry::install_global(h.telemetry.metrics()));
    let config = discus::Config::parse_from(["discus", "--listen", "127.0.0.1:0"]).unwrap();
    let security = Security::from_config(&config).unwrap();
    let node = Node::new(config, Arc::new(security));
    assert_eq!(gauge(&h), 1.0);

    let members = node.epochs().current().members().to_vec();
    let newer = |generation| {
        let id = EpochId {
            generation,
            origin: node.id,
        };
        Epoch::new(id, node.cluster_id(), members.clone()).unwrap()
    };
    node.adopt_epoch(newer(3)).unwrap();
    assert_eq!(gauge(&h), 3.0);
    // A stale epoch changes nothing.
    node.adopt_epoch(newer(2)).unwrap();
    assert_eq!(gauge(&h), 3.0);
}
