//! The export pipeline: OTLP/gRPC exporters, TLS, sampling, and the resource (spec §15.4).
//!
//! The configuration types exist in both feature modes, so the node can describe its settings
//! without `otel`. [`Telemetry`] exists only with `otel`.

use std::path::PathBuf;
use std::time::Duration;

/// The default OTLP/gRPC collector endpoint.
pub const DEFAULT_ENDPOINT: &str = "http://otel-collector:4317";
/// The default head sampling ratio for root spans.
pub const DEFAULT_SAMPLING_RATIO: f64 = 0.01;
pub const DEFAULT_EXPORT_INTERVAL: Duration = Duration::from_secs(10);
pub const DEFAULT_SPAN_QUEUE: usize = 2048;

/// The Downward API variables and the resource attribute that each one sets.
pub const K8S_ENV: [(&str, &str); 4] = [
    ("KUBERNETES_NAMESPACE", "k8s.namespace.name"),
    ("POD_NAME", "k8s.pod.name"),
    ("POD_UID", "k8s.pod.uid"),
    ("NODE_NAME", "k8s.node.name"),
];

/// What the pipeline exports to, and how.
#[derive(Clone, Debug)]
pub struct TelemetryConfig {
    pub endpoint: String,
    /// A PEM CA bundle that replaces the trust roots of the build.
    pub ca_file: Option<PathBuf>,
    /// A PEM client certificate and key for mTLS.
    pub client_identity: Option<(PathBuf, PathBuf)>,
    pub sampling_ratio: f64,
    pub export_interval: Duration,
    pub span_queue: usize,
    pub resource: ResourceInfo,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        TelemetryConfig {
            endpoint: DEFAULT_ENDPOINT.into(),
            ca_file: None,
            client_identity: None,
            sampling_ratio: DEFAULT_SAMPLING_RATIO,
            export_interval: DEFAULT_EXPORT_INTERVAL,
            span_queue: DEFAULT_SPAN_QUEUE,
            resource: ResourceInfo::default(),
        }
    }
}

/// The resource attributes that identify this node.
#[derive(Clone, Debug, Default)]
pub struct ResourceInfo {
    pub node_id: String,
    /// The epoch generation at startup. The live value is the `discus.ring.epoch` gauge.
    pub ring_epoch: u64,
    pub k8s: Vec<(&'static str, String)>,
}

/// The Kubernetes resource attributes from the Downward API variables. Empty ones are skipped.
pub fn k8s_from_env() -> Vec<(&'static str, String)> {
    K8S_ENV
        .iter()
        .filter_map(|(var, attr)| {
            std::env::var(var)
                .ok()
                .filter(|v| !v.is_empty())
                .map(|v| (*attr, v))
        })
        .collect()
}

/// Why the pipeline could not start.
#[derive(Debug, thiserror::Error)]
pub enum TelemetryError {
    #[error("cannot build the OTLP exporter: {0}")]
    Exporter(String),
    #[error("cannot read {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
}

#[cfg(feature = "otel")]
pub use otel::{CountingMetricExporter, Telemetry};

#[cfg(feature = "otel")]
mod otel {
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::tonic_types::transport::{Certificate, ClientTlsConfig, Identity};
    use opentelemetry_otlp::{WithExportConfig, WithTonicConfig};
    use opentelemetry_sdk::error::OTelSdkResult;
    use opentelemetry_sdk::metrics::data::ResourceMetrics;
    use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
    use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
    use opentelemetry_sdk::trace::{Sampler, SdkTracer, SdkTracerProvider, SpanExporter};
    use opentelemetry_sdk::Resource;

    use super::{ResourceInfo, TelemetryConfig, TelemetryError};
    use crate::metrics::{Counters, Metrics};
    use crate::processor::BoundedSpanProcessor;

    /// A running pipeline: a tracer provider, a meter provider, and the metrics handle.
    pub struct Telemetry {
        tracer_provider: SdkTracerProvider,
        meter_provider: SdkMeterProvider,
        tracer: SdkTracer,
        metrics: Metrics,
        counters: Arc<Counters>,
    }

    impl Telemetry {
        /// Build the OTLP/gRPC pipeline. Call it inside a Tokio runtime. The connection is lazy,
        /// so an unreachable collector does not fail the build.
        pub fn otlp(cfg: &TelemetryConfig) -> Result<Telemetry, TelemetryError> {
            let tls = tls_config(cfg)?;
            let mut spans = opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(cfg.endpoint.clone());
            let mut metrics = opentelemetry_otlp::MetricExporter::builder()
                .with_tonic()
                .with_endpoint(cfg.endpoint.clone());
            if let Some(tls) = tls {
                spans = spans.with_tls_config(tls.clone());
                metrics = metrics.with_tls_config(tls);
            }
            let err =
                |e: opentelemetry_otlp::ExporterBuildError| TelemetryError::Exporter(e.to_string());
            let spans = spans.build().map_err(err)?;
            let metrics = metrics.build().map_err(err)?;
            Ok(Telemetry::with_exporters(cfg, spans, metrics))
        }

        /// Build the pipeline over any exporters. The tests use the in-memory ones.
        pub fn with_exporters<S, M>(cfg: &TelemetryConfig, spans: S, metrics: M) -> Telemetry
        where
            S: SpanExporter + 'static,
            M: PushMetricExporter,
        {
            let counters = Arc::new(Counters::default());
            let resource = resource(&cfg.resource);
            let ratio = if cfg.sampling_ratio.is_nan() {
                0.0
            } else {
                cfg.sampling_ratio.clamp(0.0, 1.0)
            };
            let tracer_provider = SdkTracerProvider::builder()
                .with_span_processor(BoundedSpanProcessor::new(
                    spans,
                    cfg.span_queue,
                    counters.clone(),
                ))
                .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                    ratio,
                ))))
                .with_resource(resource.clone())
                .build();
            let reader = PeriodicReader::builder(CountingMetricExporter {
                inner: metrics,
                counters: counters.clone(),
            })
            .with_interval(cfg.export_interval)
            .build();
            let meter_provider = SdkMeterProvider::builder()
                .with_reader(reader)
                .with_resource(resource)
                .build();
            let metrics = Metrics::new(&meter_provider.meter("discus"), counters.clone());
            let tracer = tracer_provider.tracer("discus");
            Telemetry {
                tracer_provider,
                meter_provider,
                tracer,
                metrics,
                counters,
            }
        }

        pub fn metrics(&self) -> Metrics {
            self.metrics.clone()
        }

        pub fn tracer(&self) -> SdkTracer {
            self.tracer.clone()
        }

        /// The `tracing` layer that turns spans into OpenTelemetry spans.
        pub fn layer<S>(&self) -> tracing_opentelemetry::OpenTelemetryLayer<S, SdkTracer>
        where
            S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
        {
            tracing_opentelemetry::layer().with_tracer(self.tracer())
        }

        pub fn counters(&self) -> &Arc<Counters> {
            &self.counters
        }

        /// Export everything that is pending. Errors are counted, not returned.
        pub fn force_flush(&self) {
            let _ = self.tracer_provider.force_flush();
            let _ = self.meter_provider.force_flush();
        }

        /// Flush and stop both providers.
        pub fn shutdown(&self) {
            let _ = self.tracer_provider.shutdown();
            let _ = self.meter_provider.shutdown();
        }
    }

    fn resource(info: &ResourceInfo) -> Resource {
        let mut attrs = vec![
            KeyValue::new("service.instance.id", info.node_id.clone()),
            KeyValue::new("discus.ring.epoch", info.ring_epoch as i64),
        ];
        attrs.extend(info.k8s.iter().map(|(k, v)| KeyValue::new(*k, v.clone())));
        Resource::builder()
            .with_service_name("discus")
            .with_attributes(attrs)
            .build()
    }

    fn read(path: &std::path::Path) -> Result<Vec<u8>, TelemetryError> {
        std::fs::read(path).map_err(|source| TelemetryError::Io {
            path: path.to_owned(),
            source,
        })
    }

    /// The TLS settings for an `https` endpoint, else `None`.
    fn tls_config(cfg: &TelemetryConfig) -> Result<Option<ClientTlsConfig>, TelemetryError> {
        if !cfg.endpoint.starts_with("https://") {
            return Ok(None);
        }
        let mut tls = ClientTlsConfig::new();
        match &cfg.ca_file {
            Some(ca) => tls = tls.ca_certificate(Certificate::from_pem(read(ca)?)),
            None => {
                #[cfg(feature = "roots-system")]
                {
                    tls = tls.with_native_roots();
                }
                #[cfg(feature = "roots-webpki")]
                {
                    tls = tls.with_webpki_roots();
                }
            }
        }
        if let Some((cert, key)) = &cfg.client_identity {
            tls = tls.identity(Identity::from_pem(read(cert)?, read(key)?));
        }
        // tonic falls back to the provider of tls-aws-lc or tls-ring. Under tls-openssl it uses
        // the process default provider. The node installs that provider before it starts
        // telemetry.
        Ok(Some(tls))
    }

    /// Wraps a metric exporter and counts its failed exports.
    pub struct CountingMetricExporter<M> {
        pub inner: M,
        pub counters: Arc<Counters>,
    }

    impl<M: PushMetricExporter> PushMetricExporter for CountingMetricExporter<M> {
        async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
            let result = self.inner.export(metrics).await;
            if result.is_err() {
                self.counters
                    .metric_export_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
            result
        }

        fn force_flush(&self) -> OTelSdkResult {
            self.inner.force_flush()
        }

        fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
            self.inner.shutdown_with_timeout(timeout)
        }

        fn temporality(&self) -> Temporality {
            self.inner.temporality()
        }
    }
}
