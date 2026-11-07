#![cfg(all(feature = "otel", feature = "testing"))]

use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use discus_telemetry::metrics::Counters;
use discus_telemetry::pipeline::{ResourceInfo, Telemetry, TelemetryConfig};
use discus_telemetry::processor::BoundedSpanProcessor;
use discus_telemetry::testing::TraceHarness;
use opentelemetry::trace::{
    Span as _, SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState, Tracer as _,
    TracerProvider as _,
};
use opentelemetry::{Context, Key};
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
use opentelemetry_sdk::metrics::Temporality;
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};

/// An exporter that blocks every export until `release` is called.
#[derive(Clone, Debug, Default)]
struct BlockingExporter {
    gate: Arc<(Mutex<bool>, Condvar)>,
    exported: Arc<AtomicUsize>,
}

impl BlockingExporter {
    fn release(&self) {
        let (open, cv) = &*self.gate;
        *open.lock().unwrap_or_else(|e| e.into_inner()) = true;
        cv.notify_all();
    }
}

impl SpanExporter for BlockingExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        let (open, cv) = &*self.gate;
        let mut guard = open
            .lock()
            .map_err(|_| OTelSdkError::InternalFailure("lock".into()))?;
        while !*guard {
            guard = cv
                .wait(guard)
                .map_err(|_| OTelSdkError::InternalFailure("lock".into()))?;
        }
        self.exported.fetch_add(batch.len(), Ordering::SeqCst);
        Ok(())
    }
}

/// Opens the gate when dropped, so a failed assertion does not leave the export thread blocked.
struct ReleaseOnDrop(BlockingExporter);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[test]
fn full_queue_drops_and_counts_without_blocking() {
    let exporter = BlockingExporter::default();
    let _release = ReleaseOnDrop(exporter.clone());
    let counters = Arc::new(Counters::default());
    let processor = BoundedSpanProcessor::new(exporter.clone(), 4, counters.clone());
    let provider = SdkTracerProvider::builder()
        .with_span_processor(processor)
        .build();
    let tracer = provider.tracer("test");
    let start = Instant::now();
    for _ in 0..100 {
        tracer.start("op").end();
    }
    assert!(
        start.elapsed() < Duration::from_millis(100),
        "{:?}",
        start.elapsed()
    );
    // At most the queue (4) and one batch (4) are accepted while the exporter is blocked.
    let dropped = counters.spans_dropped.load(Relaxed);
    assert!(dropped >= 92, "{dropped}");
    exporter.release();
    provider.force_flush().unwrap();
    assert_eq!(
        exporter.exported.load(Ordering::SeqCst) as u64 + dropped,
        100
    );
    provider.shutdown().unwrap();
}

#[test]
fn spans_are_exported_after_a_flush() {
    let exporter = BlockingExporter::default();
    exporter.release();
    let counters = Arc::new(Counters::default());
    let processor = BoundedSpanProcessor::new(exporter.clone(), 2048, counters.clone());
    let provider = SdkTracerProvider::builder()
        .with_span_processor(processor)
        .build();
    let tracer = provider.tracer("test");
    for _ in 0..10 {
        tracer.start("op").end();
    }
    provider.force_flush().unwrap();
    assert_eq!(exporter.exported.load(Ordering::SeqCst), 10);
    assert_eq!(counters.spans_dropped.load(Relaxed), 0);
}

#[test]
fn ratio_zero_drops_root_spans_but_keeps_a_sampled_remote_parent() {
    let h = TraceHarness::new(0.0);
    let tracer = h.telemetry.tracer();
    tracer.start("root").end();
    let remote = SpanContext::new(
        TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap(),
        SpanId::from_hex("00f067aa0ba902b7").unwrap(),
        TraceFlags::SAMPLED,
        true,
        TraceState::default(),
    );
    let cx = Context::new().with_remote_span_context(remote);
    tracer.start_with_context("child-of-remote", &cx).end();
    h.telemetry.force_flush();
    let names: Vec<String> = h
        .spans
        .get_finished_spans()
        .unwrap()
        .into_iter()
        .map(|s| s.name.into_owned())
        .collect();
    assert_eq!(names, ["child-of-remote"]);
}

#[test]
fn ratio_one_samples_root_spans() {
    let h = TraceHarness::new(1.0);
    h.telemetry.tracer().start("root").end();
    h.telemetry.force_flush();
    assert_eq!(h.spans.get_finished_spans().unwrap().len(), 1);
}

#[test]
fn resource_carries_the_service_and_k8s_attributes() {
    let cfg = TelemetryConfig {
        resource: ResourceInfo {
            node_id: "ab".into(),
            ring_epoch: 3,
            k8s: vec![("k8s.pod.name", "discus-0".into())],
        },
        ..TelemetryConfig::default()
    };
    let h = TraceHarness::with_config(&cfg);
    h.telemetry.metrics().epoch(3);
    h.telemetry.force_flush();
    let exported = h.metrics.get_finished_metrics().unwrap();
    let r = exported.last().unwrap().resource();
    let get = |k: &'static str| r.get(&Key::from_static_str(k)).unwrap().to_string();
    assert_eq!(get("service.name"), "discus");
    assert_eq!(get("service.instance.id"), "ab");
    assert_eq!(get("discus.ring.epoch"), "3");
    assert_eq!(get("k8s.pod.name"), "discus-0");
}

struct FailingMetricExporter;

impl PushMetricExporter for FailingMetricExporter {
    async fn export(&self, _: &ResourceMetrics) -> OTelSdkResult {
        Err(OTelSdkError::InternalFailure("collector down".into()))
    }
    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }
    fn shutdown_with_timeout(&self, _: Duration) -> OTelSdkResult {
        Ok(())
    }
    fn temporality(&self) -> Temporality {
        Temporality::Cumulative
    }
}

#[test]
fn a_hung_exporter_does_not_block_shutdown_forever() {
    let exporter = BlockingExporter::default();
    let _release = ReleaseOnDrop(exporter.clone());
    let processor = BoundedSpanProcessor::new(exporter, 4, Arc::new(Counters::default()));
    let provider = SdkTracerProvider::builder()
        .with_span_processor(processor)
        .build();
    provider.tracer("test").start("op").end();
    let start = Instant::now();
    assert!(provider.shutdown().is_err());
    assert!(start.elapsed() < Duration::from_secs(10));
}

#[test]
fn failed_metric_export_is_counted() {
    let t = Telemetry::with_exporters(
        &TelemetryConfig::default(),
        BlockingExporter::default(),
        FailingMetricExporter,
    );
    t.metrics().epoch(1);
    t.force_flush();
    assert!(t.counters().metric_export_failures.load(Relaxed) >= 1);
}

#[test]
fn defaults_match_the_spec() {
    let cfg = TelemetryConfig::default();
    assert_eq!(cfg.endpoint, "http://otel-collector:4317");
    assert_eq!(cfg.sampling_ratio, 0.01);
    assert_eq!(cfg.export_interval, Duration::from_secs(10));
    assert_eq!(cfg.span_queue, 2048);
}

#[tokio::test]
async fn otlp_builds_without_a_collector() {
    // Export is lazy, so building against an unreachable endpoint works.
    let t = Telemetry::otlp(&TelemetryConfig::default()).unwrap();
    t.shutdown();
}

#[tokio::test]
async fn otlp_reports_a_missing_ca_file() {
    let cfg = TelemetryConfig {
        endpoint: "https://otel-collector:4317".into(),
        ca_file: Some("/nonexistent/ca.pem".into()),
        ..TelemetryConfig::default()
    };
    let err = Telemetry::otlp(&cfg).err().unwrap();
    assert!(err.to_string().contains("/nonexistent/ca.pem"), "{err}");
}
