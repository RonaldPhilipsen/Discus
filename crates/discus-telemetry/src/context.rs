//! W3C Trace Context and the carrier that travels in the node-to-node envelope (spec §13.2).
//!
//! The transport copies the two strings of [`TraceContext`] into the `Envelope`. On receive it
//! calls [`set_parent`] on the span that handles the request. On each forward it calls
//! [`outbound`] to get the context for the next hop. Without the `otel` feature, [`outbound`]
//! returns the inbound context unchanged, so traces stay connected across the node.

pub const TRACEPARENT: &str = "traceparent";
pub const TRACESTATE: &str = "tracestate";

/// `00-<32 hex>-<16 hex>-<2 hex>`, lower case, with non-zero IDs. W3C Trace Context refuses
/// upper case.
fn valid_traceparent(s: &str) -> bool {
    let b = s.as_bytes();
    let hex =
        |r: std::ops::Range<usize>| b[r].iter().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f'));
    let zero = |r: std::ops::Range<usize>| b[r].iter().all(|&c| c == b'0');
    b.len() == 55
        && b.starts_with(b"00-")
        && b[35] == b'-'
        && b[52] == b'-'
        && hex(3..35)
        && hex(36..52)
        && hex(53..55)
        && !zero(3..35)
        && !zero(36..52)
}

/// The trace context carrier. `None` means "no context". The fields have the shape of the wire
/// carrier, so the transport moves them across.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TraceContext {
    pub traceparent: Option<String>,
    pub tracestate: Option<String>,
}

impl TraceContext {
    /// A carrier from received header values. An invalid `traceparent` drops the whole
    /// context, `tracestate` included, as W3C Trace Context requires. An empty `tracestate`
    /// counts as none.
    pub fn new(traceparent: Option<String>, tracestate: Option<String>) -> TraceContext {
        match traceparent {
            Some(tp) if valid_traceparent(&tp) => TraceContext {
                traceparent: Some(tp),
                tracestate: tracestate.filter(|s| !s.is_empty()),
            },
            _ => TraceContext::default(),
        }
    }

    /// True when the carrier holds no `traceparent`.
    pub fn is_empty(&self) -> bool {
        self.traceparent.is_none()
    }
}

/// Make the inbound context the parent of `span`. Without `otel`, this does nothing.
#[cfg(not(feature = "otel"))]
pub fn set_parent(_span: &tracing::Span, _inbound: &TraceContext) {}

/// The context to send to the next hop. Without `otel`, this is the inbound context unchanged.
#[cfg(not(feature = "otel"))]
pub fn outbound(inbound: &TraceContext) -> TraceContext {
    inbound.clone()
}

#[cfg(feature = "otel")]
pub use otel::{outbound, set_parent};

#[cfg(feature = "otel")]
mod otel {
    use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
    use opentelemetry::trace::TraceContextExt;
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    use super::{TraceContext, TRACEPARENT, TRACESTATE};

    impl Extractor for TraceContext {
        fn get(&self, key: &str) -> Option<&str> {
            match key {
                TRACEPARENT => self.traceparent.as_deref(),
                TRACESTATE => self.tracestate.as_deref(),
                _ => None,
            }
        }

        fn keys(&self) -> Vec<&str> {
            vec![TRACEPARENT, TRACESTATE]
        }
    }

    impl Injector for TraceContext {
        fn set(&mut self, key: &str, value: String) {
            // The propagator sets an empty `tracestate` when the span has none.
            let value = (!value.is_empty()).then_some(value);
            match key {
                TRACEPARENT => self.traceparent = value,
                TRACESTATE => self.tracestate = value,
                _ => {}
            }
        }
    }

    /// Make the inbound context the parent of `span`.
    pub fn set_parent(span: &tracing::Span, inbound: &TraceContext) {
        if inbound.is_empty() {
            return;
        }
        let cx = TraceContextPropagator::new().extract(inbound);
        // A span that a filter disabled has no context. Then there is nothing to link.
        let _ = span.set_parent(cx);
    }

    /// The context of the current span, else the inbound context unchanged.
    pub fn outbound(inbound: &TraceContext) -> TraceContext {
        let cx = tracing::Span::current().context();
        if !cx.span().span_context().is_valid() {
            return inbound.clone();
        }
        let mut out = TraceContext::default();
        TraceContextPropagator::new().inject_context(&cx, &mut out);
        out
    }
}
