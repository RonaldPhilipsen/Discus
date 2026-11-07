use std::io::Write;
use std::sync::{Arc, Mutex};

use discus_telemetry::logging::{json_layer, suppressed};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::EnvFilter;

/// A `MakeWriter` that collects every line in memory.
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
    type Writer = Capture;
    fn make_writer(&'a self) -> Capture {
        self.clone()
    }
}

impl Capture {
    fn lines(&self) -> Vec<String> {
        let bytes = self.0.lock().unwrap_or_else(|e| e.into_inner()).clone();
        String::from_utf8_lossy(&bytes)
            .lines()
            .map(str::to_owned)
            .collect()
    }

    fn count(&self, needle: &str) -> usize {
        self.lines().iter().filter(|l| l.contains(needle)).count()
    }
}

#[test]
fn one_noisy_call_site_does_not_silence_another() {
    let out = Capture::default();
    let (layer, _slot) = json_layer(out.clone(), EnvFilter::new("info"), 5);
    let subscriber = tracing_subscriber::registry().with(layer);
    let before = suppressed();
    tracing::subscriber::with_default(subscriber, || {
        for _ in 0..50 {
            tracing::info!("noisy");
        }
        tracing::info!("quiet");
    });
    assert_eq!(out.count("noisy"), 5);
    assert_eq!(out.count("quiet"), 1);
    assert!(suppressed() - before >= 45);
}

#[test]
fn zero_means_unlimited() {
    let out = Capture::default();
    let (layer, _slot) = json_layer(out.clone(), EnvFilter::new("info"), 0);
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        for _ in 0..200 {
            tracing::info!("flood");
        }
    });
    assert_eq!(out.count("flood"), 200);
}

#[test]
fn the_level_filter_still_applies() {
    let out = Capture::default();
    let (layer, _slot) = json_layer(out.clone(), EnvFilter::new("warn"), 5);
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        tracing::info!("hidden");
        tracing::warn!("shown");
    });
    assert_eq!(out.count("hidden"), 0);
    assert_eq!(out.count("shown"), 1);
}

#[test]
fn lines_are_json_objects_without_trace_ids_outside_a_trace() {
    let out = Capture::default();
    let (layer, _slot) = json_layer(out.clone(), EnvFilter::new("info"), 0);
    let subscriber = tracing_subscriber::registry().with(layer);
    tracing::subscriber::with_default(subscriber, || {
        let _span = tracing::info_span!("plain").entered();
        tracing::info!(answer = 42, "plain line");
    });
    let lines = out.lines();
    let v: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
    assert_eq!(v["fields"]["message"], "plain line");
    assert_eq!(v["fields"]["answer"], 42);
    assert!(v.get("trace_id").is_none());
}

#[cfg(all(feature = "otel", feature = "testing"))]
#[test]
fn log_lines_inside_a_span_carry_the_trace_id() {
    use discus_telemetry::testing::TraceHarness;

    let h = TraceHarness::new(1.0);
    let out = Capture::default();
    let (layer, slot) = json_layer(out.clone(), EnvFilter::new("info"), 0);
    let subscriber = tracing_subscriber::registry()
        .with(layer)
        .with(h.telemetry.layer());
    let dispatch = tracing::Dispatch::new(subscriber);
    assert!(slot.set(dispatch.downgrade()).is_ok());
    tracing::dispatcher::with_default(&dispatch, || {
        let span = tracing::info_span!("traced");
        span.in_scope(|| tracing::info!("inside"));
        tracing::info!("outside");
    });
    h.telemetry.force_flush();
    let lines = out.lines();
    let inside: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
    let trace_id = inside["trace_id"].as_str().unwrap();
    let span_id = inside["span_id"].as_str().unwrap();
    assert_eq!(trace_id.len(), 32);
    assert_eq!(span_id.len(), 16);
    assert_eq!(inside["fields"]["message"], "inside");
    let outside: serde_json::Value = serde_json::from_str(&lines[1]).unwrap();
    assert!(outside.get("trace_id").is_none());

    let spans = h.spans.get_finished_spans().unwrap();
    let traced = spans.iter().find(|s| s.name == "traced").unwrap();
    assert_eq!(traced.span_context.trace_id().to_string(), trace_id);
    assert_eq!(traced.span_context.span_id().to_string(), span_id);
}
