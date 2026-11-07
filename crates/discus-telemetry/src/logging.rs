//! Structured stdout logs: JSON lines, a rate limit per call site, and trace IDs (spec §15.4).

use std::fmt;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use governor::{DefaultKeyedRateLimiter, Quota, RateLimiter};
use tracing::callsite::Identifier;
use tracing::dispatcher::WeakDispatch;
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::fmt::format::{self, FormatEvent, FormatFields, Writer};
use tracing_subscriber::fmt::FmtContext;
use tracing_subscriber::layer::{Context, Filter, Layer};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::EnvFilter;

static SUPPRESSED: AtomicU64 = AtomicU64::new(0);

/// Log lines dropped by the rate limit since start.
pub fn suppressed() -> u64 {
    SUPPRESSED.load(Ordering::Relaxed)
}

/// A per-layer filter that lets at most `per_second` events through per call site and second.
/// Spans always pass. A dropped event adds one to [`suppressed`].
pub struct RateLimit {
    limiter: Option<DefaultKeyedRateLimiter<Identifier>>,
}

impl RateLimit {
    /// `per_second` 0 means no limit.
    pub fn new(per_second: u32) -> RateLimit {
        RateLimit {
            limiter: NonZeroU32::new(per_second).map(|n| RateLimiter::keyed(Quota::per_second(n))),
        }
    }
}

impl<S> Filter<S> for RateLimit {
    fn enabled(&self, _meta: &Metadata<'_>, _cx: &Context<'_, S>) -> bool {
        true
    }

    fn event_enabled(&self, event: &Event<'_>, _cx: &Context<'_, S>) -> bool {
        let Some(limiter) = &self.limiter else {
            return true;
        };
        let allowed = limiter.check_key(&event.metadata().callsite()).is_ok();
        if !allowed {
            SUPPRESSED.fetch_add(1, Ordering::Relaxed);
        }
        allowed
    }
}

/// The slot for the dispatch that the trace ID lookup uses. Fill it after the `Dispatch` is
/// built. Inside an event, `tracing` hides the current dispatch, so the formatter needs its own
/// handle.
pub type DispatchSlot = Arc<OnceLock<WeakDispatch>>;

/// An event formatter that adds `trace_id` and `span_id` to the JSON line of `F` when the event
/// is inside a span with a valid OpenTelemetry context. Without `otel`, it writes `F` unchanged.
pub struct TraceIds<F> {
    inner: F,
    #[cfg_attr(not(feature = "otel"), allow(dead_code))]
    dispatch: DispatchSlot,
}

impl<F> TraceIds<F> {
    pub fn new(inner: F, dispatch: DispatchSlot) -> TraceIds<F> {
        TraceIds { inner, dispatch }
    }

    #[cfg(feature = "otel")]
    fn ids<S, N>(&self, ctx: &FmtContext<'_, S, N>) -> Option<(String, String)>
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
        N: for<'a> FormatFields<'a> + 'static,
    {
        use opentelemetry::trace::TraceContextExt;
        let id = ctx.lookup_current()?.id();
        let dispatch = self.dispatch.get()?.upgrade()?;
        let cx = tracing_opentelemetry::get_otel_context(&id, &dispatch)?;
        let span = cx.span();
        let sc = span.span_context();
        sc.is_valid()
            .then(|| (sc.trace_id().to_string(), sc.span_id().to_string()))
    }

    #[cfg(not(feature = "otel"))]
    fn ids<S, N>(&self, _ctx: &FmtContext<'_, S, N>) -> Option<(String, String)>
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
        N: for<'a> FormatFields<'a> + 'static,
    {
        None
    }
}

impl<S, N, F> FormatEvent<S, N> for TraceIds<F>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
    F: FormatEvent<S, N>,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let Some((trace_id, span_id)) = self.ids(ctx) else {
            return self.inner.format_event(ctx, writer, event);
        };
        let mut line = String::new();
        self.inner
            .format_event(ctx, Writer::new(&mut line), event)?;
        // The JSON formatter writes one object and a newline. Add the IDs before its last `}`.
        if let Some(end) = line.rfind('}') {
            line.insert_str(
                end,
                &format!(",\"trace_id\":\"{trace_id}\",\"span_id\":\"{span_id}\""),
            );
        }
        writer.write_str(&line)
    }
}

/// The stdout log layer: JSON lines through `filter` and a [`RateLimit`] of `rate_limit` lines
/// per second per call site, with trace IDs. Fill the returned slot with the dispatch.
pub fn json_layer<S, W>(
    writer: W,
    filter: EnvFilter,
    rate_limit: u32,
) -> (impl Layer<S> + Send + Sync + 'static, DispatchSlot)
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    W: for<'w> tracing_subscriber::fmt::MakeWriter<'w> + Send + Sync + 'static,
{
    use tracing_subscriber::filter::FilterExt;

    let slot: DispatchSlot = Arc::new(OnceLock::new());
    let layer = tracing_subscriber::fmt::layer()
        .json()
        .with_writer(writer)
        .event_format(TraceIds::new(format().json(), slot.clone()))
        .with_filter(filter.and(RateLimit::new(rate_limit)));
    (layer, slot)
}

fn format() -> format::Format {
    format::Format::default()
}
