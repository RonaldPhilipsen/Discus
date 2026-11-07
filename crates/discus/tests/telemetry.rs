//! Telemetry wiring: flags, the startup check, command spans, and command metrics (spec §15.4).

use bytes::Bytes;
use std::sync::Arc;

use discus::node::{ConnState, Node};
use discus::security::{ConnAuth, Security};

/// A node with the security state of `config`.
fn node(config: discus::Config) -> Node {
    let security = Security::from_config(&config).expect("valid security configuration");
    Node::new(config, Arc::new(security))
}

/// Parse `discus --listen 127.0.0.1:0` followed by `extra`.
fn args(extra: &[&str]) -> discus::Config {
    let mut argv = vec!["discus", "--listen", "127.0.0.1:0"];
    argv.extend_from_slice(extra);
    discus::Config::parse_from(argv).expect("valid test arguments")
}

fn cmd(parts: &[&str]) -> Vec<Bytes> {
    parts
        .iter()
        .map(|p| Bytes::copy_from_slice(p.as_bytes()))
        .collect()
}

#[test]
fn telemetry_flags_have_the_spec_defaults() {
    let t = args(&[]).telemetry;
    assert!(!t.otel);
    assert_eq!(t.otel_endpoint, "http://otel-collector:4317");
    assert_eq!(t.otel_sampling_ratio, 0.01);
    assert_eq!(t.otel_export_interval_secs, 10);
    assert_eq!(t.otel_span_queue, 2048);
    assert_eq!(t.log_rate_limit, 100);
}

#[test]
fn enabling_telemetry_without_the_feature_names_it() {
    let err = args(&["--otel"]).telemetry.validate(false).unwrap_err();
    assert!(err.contains("\"otel\" feature"), "{err}");
    assert!(args(&[]).telemetry.validate(false).is_ok());
    assert!(args(&["--otel"]).telemetry.validate(true).is_ok());
}

#[test]
fn bad_telemetry_flags_are_rejected() {
    for extra in [
        &["--otel-sampling-ratio", "1.5"][..],
        &["--otel-sampling-ratio=-0.1"],
        &["--otel-sampling-ratio", "NaN"],
        &["--otel-cert-file", "c.pem"],
        &["--otel-key-file", "k.pem"],
        &["--otel-ca-file", "ca.pem"],
        &["--otel-export-interval-secs", "0"],
        &["--otel-span-queue", "0"],
    ] {
        assert!(args(extra).telemetry.validate(true).is_err(), "{extra:?}");
    }
    let https = [
        "--otel-endpoint",
        "https://collector:4317",
        "--otel-ca-file",
        "ca.pem",
        "--otel-cert-file",
        "c.pem",
        "--otel-key-file",
        "k.pem",
    ];
    assert!(args(&https).telemetry.validate(true).is_ok());
}

#[test]
fn the_placeholder_node_id_is_a_stable_node_label() {
    let id = discus::telemetry::node_id();
    assert_eq!(id.len(), 32);
    assert_eq!(id, discus::telemetry::node_id());
    assert!(discus_telemetry::attrs::NodeLabel::new(&id).is_ok());
    assert_eq!(discus::telemetry::ring_epoch(), 0);
}

#[test]
fn dispatch_replies_like_the_plain_dispatcher() {
    let node = node(args(&[]));
    let mut conn = ConnState::new(1, ConnAuth::default());
    let m = discus_telemetry::Metrics::noop();
    let set = discus::telemetry::dispatch_with(&m, &node, &mut conn, &cmd(&["SET", "k", "v"]));
    assert_eq!(set, discus_resp::Value::ok());
    let get = discus::telemetry::dispatch_with(&m, &node, &mut conn, &cmd(&["GET", "k"]));
    assert_eq!(get, discus_resp::Value::Bulk(Bytes::from_static(b"v")));
}

#[cfg(not(feature = "otel"))]
#[test]
fn init_refuses_to_start_without_the_feature() {
    let err = discus::telemetry::init(&args(&["--otel"])).err().unwrap();
    assert!(err.contains("\"otel\" feature"), "{err}");
}

#[cfg(feature = "otel")]
mod otel {
    use super::*;
    use discus_telemetry::attrs::{check, ALLOWED_KEYS};
    use discus_telemetry::testing::{Point, TraceHarness};
    use tracing_subscriber::layer::SubscriberExt;

    /// `tracing` caches callsite interest per process. A test that reaches the command span
    /// without a subscriber, while another test holds a scoped one, can disable the span for
    /// both. These tests take turns.
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn serial() -> std::sync::MutexGuard<'static, ()> {
        SERIAL.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[test]
    fn a_command_produces_a_span_with_route_and_store_children() {
        let _serial = serial();
        let h = TraceHarness::new(1.0);
        let subscriber = tracing_subscriber::registry().with(h.telemetry.layer());
        let node = node(args(&[]));
        let mut conn = ConnState::new(1, ConnAuth::default());
        let m = h.telemetry.metrics();
        tracing::subscriber::with_default(subscriber, || {
            discus::telemetry::dispatch_with(
                &m,
                &node,
                &mut conn,
                &cmd(&["SET", "secret-key", "v"]),
            );
        });
        h.telemetry.force_flush();
        let spans = h.spans.get_finished_spans().unwrap();
        let root = spans.iter().find(|s| s.name == "command").unwrap();
        for child in ["route", "store"] {
            let c = spans.iter().find(|s| s.name == child).unwrap();
            assert_eq!(c.parent_span_id, root.span_context.span_id(), "{child}");
            assert_eq!(c.span_context.trace_id(), root.span_context.trace_id());
        }
        let op = root
            .attributes
            .iter()
            .find(|kv| kv.key.as_str() == "db.operation.name")
            .unwrap();
        assert_eq!(op.value.as_str(), "SET");
        for s in &spans {
            for kv in &s.attributes {
                assert!(!kv.value.as_str().contains("secret-key"), "{}", s.name);
            }
        }
    }

    fn sum(points: &[Point], name: &str, attr: (&str, &str)) -> f64 {
        points
            .iter()
            .filter(|p| p.name == name)
            .filter(|p| p.attrs.iter().any(|(k, v)| k == attr.0 && v == attr.1))
            .map(|p| p.value)
            .sum()
    }

    #[test]
    fn metrics_record_commands_auth_and_only_bounded_attributes() {
        let _serial = serial();
        let h = TraceHarness::new(0.0);
        let m = h.telemetry.metrics();
        let node = node(args(&["--shared-token", "t"]));
        let mut conn = ConnState::new(1, ConnAuth::default());
        for c in [
            &["AUTH", "wrong"][..],
            &["AUTH", "t"],
            &["HELLO", "3", "AUTH", "default", "nope"],
            &["GET", "k"],
            &["BOGUS123", "a"],
            &["MULTI"],
        ] {
            discus::telemetry::dispatch_with(&m, &node, &mut conn, &cmd(c));
        }
        let points = h.points();
        assert_eq!(
            sum(&points, "discus.auth.attempts", ("status", "failure")),
            2.0
        );
        assert_eq!(
            sum(&points, "discus.auth.attempts", ("status", "success")),
            1.0
        );
        assert_eq!(sum(&points, "discus.commands", ("command", "GET")), 1.0);
        assert_eq!(sum(&points, "discus.commands", ("command", "unknown")), 1.0);
        assert_eq!(
            sum(&points, "discus.commands", ("command", "unsupported")),
            1.0
        );
        assert_eq!(sum(&points, "discus.commands", ("status", "error")), 4.0);
        for p in &points {
            for (k, v) in &p.attrs {
                assert!(ALLOWED_KEYS.contains(&k.as_str()), "{}: {k}", p.name);
                assert!(!v.contains("BOGUS"), "{}", p.name);
            }
            check(p.attrs.iter().map(|(k, v)| (k.as_str(), v.as_str()))).unwrap();
        }
    }

    #[test]
    fn store_counters_reach_the_metrics() {
        let _serial = serial();
        let h = TraceHarness::new(0.0);
        let m = h.telemetry.metrics();
        let node = Arc::new(node(args(&[])));
        discus::telemetry::observe_store_on(&m, &node);
        let mut conn = ConnState::new(1, ConnAuth::default());
        for c in [&["SET", "k", "v"][..], &["GET", "k"], &["GET", "absent"]] {
            discus::telemetry::dispatch_with(&m, &node, &mut conn, &cmd(c));
        }
        let points = h.points();
        let value = |n: &str| points.iter().find(|p| p.name == n).unwrap().value;
        assert_eq!(value("discus.keyspace.hits"), 1.0);
        assert_eq!(value("discus.keyspace.misses"), 1.0);
        assert_eq!(value("discus.keys"), 1.0);
        assert_eq!(value("discus.memory.limit"), (1u64 << 30) as f64);
    }
}
