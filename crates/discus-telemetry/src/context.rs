//! W3C Trace Context and the carrier that travels in the node-to-node envelope (spec §13.2).
//!
//! The transport copies the two strings of [`TraceContext`] into the `Envelope`. On receive it
//! calls [`set_parent`] on the span that handles the request. On each forward it calls
//! [`outbound`] to get the context for the next hop. Without the `otel` feature, [`outbound`]
//! returns the inbound context unchanged, so traces stay connected across the node.

use std::fmt;

/// The `traceparent` header name.
pub const TRACEPARENT: &str = "traceparent";
/// The `tracestate` header name.
pub const TRACESTATE: &str = "tracestate";

/// A parsed `traceparent` value, version `00`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TraceParent {
    pub trace_id: [u8; 16],
    pub parent_id: [u8; 8],
    pub flags: u8,
}

impl TraceParent {
    /// Parse `00-<32 hex>-<16 hex>-<2 hex>`, lower case, with non-zero IDs.
    pub fn parse(s: &str) -> Option<TraceParent> {
        let b = s.as_bytes();
        if b.len() != 55 || b[2] != b'-' || b[35] != b'-' || b[52] != b'-' {
            return None;
        }
        if &b[..2] != b"00" {
            return None;
        }
        let mut trace_id = [0u8; 16];
        let mut parent_id = [0u8; 8];
        let mut flags = [0u8; 1];
        hex_into(&b[3..35], &mut trace_id)?;
        hex_into(&b[36..52], &mut parent_id)?;
        hex_into(&b[53..55], &mut flags)?;
        if trace_id == [0; 16] || parent_id == [0; 8] {
            return None;
        }
        Some(TraceParent {
            trace_id,
            parent_id,
            flags: flags[0],
        })
    }

    /// True when the `sampled` flag is set.
    pub fn is_sampled(&self) -> bool {
        self.flags & 1 == 1
    }
}

impl fmt::Display for TraceParent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("00-")?;
        write_hex(f, &self.trace_id)?;
        f.write_str("-")?;
        write_hex(f, &self.parent_id)?;
        write!(f, "-{:02x}", self.flags)
    }
}

fn write_hex(f: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for byte in bytes {
        write!(f, "{byte:02x}")?;
    }
    Ok(())
}

fn hex_into(src: &[u8], dst: &mut [u8]) -> Option<()> {
    fn nibble(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            _ => None,
        }
    }
    for (i, out) in dst.iter_mut().enumerate() {
        *out = nibble(src[2 * i])? << 4 | nibble(src[2 * i + 1])?;
    }
    Some(())
}

/// The trace context carrier. Empty strings mean "no context".
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TraceContext {
    pub traceparent: String,
    pub tracestate: String,
}

impl TraceContext {
    /// True when the carrier holds no valid `traceparent`.
    pub fn is_empty(&self) -> bool {
        self.traceparent.is_empty()
    }

    /// Build a carrier from header pairs. Unknown names are ignored. An invalid `traceparent`
    /// drops the whole context, `tracestate` included, as W3C Trace Context requires.
    pub fn from_pairs<'a>(pairs: impl IntoIterator<Item = (&'a str, &'a str)>) -> TraceContext {
        let mut cx = TraceContext::default();
        for (name, value) in pairs {
            if name.eq_ignore_ascii_case(TRACEPARENT) {
                cx.traceparent = value.to_owned();
            } else if name.eq_ignore_ascii_case(TRACESTATE) {
                cx.tracestate = value.to_owned();
            }
        }
        if TraceParent::parse(&cx.traceparent).is_none() {
            return TraceContext::default();
        }
        cx
    }

    /// The header pairs to send. An empty carrier gives no pairs, and an empty `tracestate` is
    /// left out.
    pub fn to_pairs(&self) -> Vec<(&'static str, String)> {
        let mut pairs = Vec::new();
        if !self.is_empty() {
            pairs.push((TRACEPARENT, self.traceparent.clone()));
            if !self.tracestate.is_empty() {
                pairs.push((TRACESTATE, self.tracestate.clone()));
            }
        }
        pairs
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
            let value = match key {
                TRACEPARENT => &self.traceparent,
                TRACESTATE => &self.tracestate,
                _ => return None,
            };
            (!value.is_empty()).then_some(value.as_str())
        }

        fn keys(&self) -> Vec<&str> {
            vec![TRACEPARENT, TRACESTATE]
        }
    }

    impl Injector for TraceContext {
        fn set(&mut self, key: &str, value: String) {
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
