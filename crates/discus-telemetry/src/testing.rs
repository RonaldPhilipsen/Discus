//! Test helpers over the SDK in-memory exporters. Only built with `otel` and `testing`.

use std::sync::Arc;
use std::time::Duration;

use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};

use crate::metrics::{Counters, Metrics};

/// One exported data point, flattened. A histogram reports its count as `value`.
#[derive(Clone, Debug, PartialEq)]
pub struct Point {
    pub name: String,
    pub attrs: Vec<(String, String)>,
    pub value: f64,
    pub bounds: Vec<f64>,
}

/// A `Metrics` handle that exports into memory.
pub struct MetricHarness {
    pub metrics: Metrics,
    pub counters: Arc<Counters>,
    pub exporter: InMemoryMetricExporter,
    pub provider: SdkMeterProvider,
}

impl MetricHarness {
    pub fn new() -> MetricHarness {
        let exporter = InMemoryMetricExporter::default();
        // A long interval: the tests export through `force_flush` only.
        let reader = PeriodicReader::builder(exporter.clone())
            .with_interval(Duration::from_secs(3600))
            .build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let counters = Arc::new(Counters::default());
        let metrics = Metrics::new(&provider.meter("discus"), counters.clone());
        MetricHarness {
            metrics,
            counters,
            exporter,
            provider,
        }
    }

    /// Flush and flatten the last export.
    pub fn points(&self) -> Vec<Point> {
        // A flush error leaves the previous export in place, which the caller then reads.
        let _ = self.provider.force_flush();
        points(&self.exporter)
    }
}

impl Default for MetricHarness {
    fn default() -> Self {
        MetricHarness::new()
    }
}

/// Flatten the last export of `exporter`.
pub fn points(exporter: &InMemoryMetricExporter) -> Vec<Point> {
    let finished = exporter.get_finished_metrics().unwrap_or_default();
    finished.last().map(flatten).unwrap_or_default()
}

fn attrs<'a>(kvs: impl Iterator<Item = &'a KeyValue>) -> Vec<(String, String)> {
    kvs.map(|kv| (kv.key.to_string(), kv.value.to_string()))
        .collect()
}

trait AsF64: Copy {
    fn as_f64(self) -> f64;
}
impl AsF64 for f64 {
    fn as_f64(self) -> f64 {
        self
    }
}
impl AsF64 for u64 {
    fn as_f64(self) -> f64 {
        self as f64
    }
}
impl AsF64 for i64 {
    fn as_f64(self) -> f64 {
        self as f64
    }
}

fn push<T: AsF64>(out: &mut Vec<Point>, name: &str, data: &MetricData<T>) {
    let point = |attrs, value, bounds| Point {
        name: name.to_owned(),
        attrs,
        value,
        bounds,
    };
    match data {
        MetricData::Gauge(g) => out.extend(
            g.data_points()
                .map(|p| point(attrs(p.attributes()), p.value().as_f64(), vec![])),
        ),
        MetricData::Sum(s) => out.extend(
            s.data_points()
                .map(|p| point(attrs(p.attributes()), p.value().as_f64(), vec![])),
        ),
        MetricData::Histogram(h) => out.extend(h.data_points().map(|p| {
            point(
                attrs(p.attributes()),
                p.count() as f64,
                p.bounds().collect(),
            )
        })),
        MetricData::ExponentialHistogram(h) => out.extend(
            h.data_points()
                .map(|p| point(attrs(p.attributes()), p.count() as f64, vec![])),
        ),
    }
}

fn flatten(rm: &ResourceMetrics) -> Vec<Point> {
    let mut out = Vec::new();
    for scope in rm.scope_metrics() {
        for metric in scope.metrics() {
            match metric.data() {
                AggregatedMetrics::F64(d) => push(&mut out, metric.name(), d),
                AggregatedMetrics::U64(d) => push(&mut out, metric.name(), d),
                AggregatedMetrics::I64(d) => push(&mut out, metric.name(), d),
            }
        }
    }
    out
}
