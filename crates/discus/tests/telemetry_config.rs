//! The settings file metrics export from the start, before any reload. They report through the
//! global metrics handle, so this test has a process of its own.

#![cfg(feature = "otel")]

use std::sync::Arc;

mod common;

use discus::node::Node;
use discus::security::Security;
use discus::settings::{Live, Reloadable};
use discus_telemetry::testing::TraceHarness;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn the_pending_gauge_exports_before_the_first_reload() {
    let h = TraceHarness::new(0.0);
    assert!(discus_telemetry::install_global(h.telemetry.metrics()));
    let dir = tempfile::tempdir().expect("temporary directory");
    let path = dir.path().join("discus.toml");
    std::fs::write(&path, "version = 1\n").expect("write the settings file");
    let path = path.to_str().expect("UTF-8 path");
    let argv = ["discus", "--config", path, "--config-reload-secs", "0"];
    let config = common::config(&argv);
    let live_settings = Live::new(Reloadable::from_config(&config));
    let security = Security::from_config(&config, live_settings.clone())
        .expect("valid security configuration");
    let node = Arc::new(Node::new(config, Arc::new(security), live_settings));
    let stop = CancellationToken::new();
    discus::settings::reload::spawn(&node, stop.clone());
    let pending = h
        .points()
        .into_iter()
        .find(|p| p.name == "discus.config.pending")
        .expect("discus.config.pending is exported");
    assert_eq!(pending.value, 0.0);
    stop.cancel();
}
