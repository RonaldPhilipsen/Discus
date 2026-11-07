//! One trace covers a command across a forward (spec §15.4). The entry node injects the context
//! of its `forward` span into the frame, and the owner parents its command span on it. The test
//! installs the global subscriber and metrics handle, so it has a process of its own.

#![cfg(feature = "otel")]
#![allow(clippy::unwrap_used)]

mod common;

use std::time::Duration;

use common::cluster::start_cluster;
use discus_telemetry::testing::TraceHarness;
use redis::AsyncCommands;
use tracing_subscriber::layer::SubscriberExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_trace_id_covers_the_forward() {
    let h = TraceHarness::new(1.0);
    assert!(discus_telemetry::install_global(h.telemetry.metrics()));
    let subscriber = tracing_subscriber::registry().with(h.telemetry.layer());
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let c = start_cluster(2, &[]).await;
    let key = c.key_on(1, "traced");
    let mut entry = c.nodes[0].redis().await;
    let _: () = entry.set(&key, "v").await.unwrap();
    assert!(c.nodes[1].holds(key.as_bytes()));

    // The client also sends setup commands, so pick the `SET` spans. The owner marks its
    // command span with `discus.forwarded`.
    macro_rules! forwarded {
        ($s:expr) => {
            $s.attributes
                .iter()
                .any(|kv| kv.key.as_str() == "discus.forwarded")
        };
    }
    macro_rules! set {
        ($s:expr) => {
            $s.attributes
                .iter()
                .any(|kv| kv.key.as_str() == "db.operation.name" && kv.value.as_str() == "SET")
        };
    }

    // The spans end on other tasks. Wait until the three spans of the `SET` are exported.
    let mut spans = Vec::new();
    for _ in 0..100 {
        h.telemetry.force_flush();
        spans = h.spans.get_finished_spans().unwrap();
        let entry = spans
            .iter()
            .any(|s| s.name == "command" && set!(s) && !forwarded!(s));
        let remote = spans
            .iter()
            .any(|s| s.name == "command" && set!(s) && forwarded!(s));
        if entry && remote && spans.iter().any(|s| s.name == "forward") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let entry_cmd = spans
        .iter()
        .find(|s| s.name == "command" && set!(s) && !forwarded!(s))
        .expect("the entry command span");
    let remote_cmd = spans
        .iter()
        .find(|s| s.name == "command" && set!(s) && forwarded!(s))
        .expect("the forwarded command span");
    let forward = spans
        .iter()
        .find(|s| s.name == "forward")
        .expect("the forward span");

    let trace_id = entry_cmd.span_context.trace_id();
    assert_eq!(forward.span_context.trace_id(), trace_id);
    assert_eq!(remote_cmd.span_context.trace_id(), trace_id);
    assert_eq!(forward.parent_span_id, entry_cmd.span_context.span_id());
    assert_eq!(remote_cmd.parent_span_id, forward.span_context.span_id());
    assert!(remote_cmd.parent_span_is_remote);
    let op = remote_cmd
        .attributes
        .iter()
        .find(|kv| kv.key.as_str() == "db.operation.name")
        .unwrap();
    assert_eq!(op.value.as_str(), "SET");
    for n in &c.nodes {
        n.stop();
    }
}
