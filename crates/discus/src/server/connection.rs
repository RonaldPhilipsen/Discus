//! One client connection.
//!
//! `accept` runs the transport setup of the security mode first: TLS, the PROXY header, or
//! nothing. Then `serve` runs the command loop over the resulting stream.
//!
//! Commands run concurrently but reply in request order (spec section 7.1). The loop keeps a
//! `FuturesOrdered` of reply slots. A command that waits for nothing and needs no other node
//! runs inline. When nothing is pending, its reply goes straight into the output buffer, so the
//! single-node path allocates no future.
//!
//! Each command passes the entry checks in request order: lookup, arity, authentication, and
//! authorization (`cluster::prepare`). Only then does it route. A command that another node
//! forwards arrives over cluster mTLS and is not authorized again. Each command also carries its
//! telemetry span and metrics from the entry checks to the reply.
//!
//! Backpressure: the loop reads only while fewer than `max_pipeline` slots are pending and the
//! parser needs more bytes. It flushes when the buffer reaches 1 MiB and when no slot is pending.
//! A client that does not read its replies stalls the flush, which stalls the next read, so memory
//! stays bounded.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use discus_resp::{encode, Command, Limits, Parser, Protocol, Value};
use discus_telemetry::Metrics;
use futures::future::BoxFuture;
use futures::stream::{FuturesOrdered, StreamExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::order::{KeyOrder, Keys, WaitFor};
use crate::cluster::{self, Dispatch, Prepared};
use crate::node::{ConnState, Node};
use crate::security::ConnAuth;
use crate::telemetry::CommandTrace;

/// Flush when this many reply bytes are pending.
const FLUSH_BYTES: usize = 1 << 20;

/// One reply in request order.
struct Slot {
    protocol: Protocol,
    value: Value,
    /// Close the connection after this reply.
    closes: bool,
}

impl Slot {
    fn reply(protocol: Protocol, value: Value) -> Slot {
        Slot {
            protocol,
            value,
            closes: false,
        }
    }
}

/// Write `out` to the client, giving up when the node force-closes connections. A client that
/// never reads would otherwise hold the write open forever. Returns false to end the connection.
async fn flush<S: AsyncWrite + Unpin>(
    stream: &mut S,
    out: &mut BytesMut,
    hard_stop: &CancellationToken,
) -> bool {
    if out.is_empty() {
        return true;
    }
    let sent = tokio::select! {
        _ = hard_stop.cancelled() => false,
        written = stream.write_all(out) => written.is_ok(),
    };
    out.clear();
    sent
}

struct Pipeline {
    node: Arc<Node>,
    metrics: &'static Metrics,
    conn: ConnState,
    order: KeyOrder,
    pending: FuturesOrdered<BoxFuture<'static, Slot>>,
    out: BytesMut,
    /// A closing reply is queued: parse nothing more.
    closing: bool,
    /// The closing reply is in `out`: flush and end.
    finished: bool,
}

impl Pipeline {
    fn encode(&mut self, slot: Slot) {
        encode(&slot.value, slot.protocol, &mut self.out);
        self.finished |= slot.closes;
    }

    /// Queue a reply that is known now.
    fn ready(&mut self, slot: Slot) {
        self.closing |= slot.closes;
        if self.pending.is_empty() {
            self.encode(slot);
        } else {
            self.pending.push_back(Box::pin(async move { slot }));
        }
    }

    /// The async part of `AUTH` and `HELLO ... AUTH` (a JWKS refresh). Earlier commands keep
    /// running meanwhile, and their replies go into the output buffer in order.
    async fn prefetch(&mut self, cmd: &Command) {
        let node = self.node.clone();
        let fetch = node.security.prefetch(&self.conn, cmd);
        tokio::pin!(fetch);
        loop {
            tokio::select! {
                () = &mut fetch => return,
                Some(slot) = self.pending.next(), if !self.pending.is_empty() => {
                    encode(&slot.value, slot.protocol, &mut self.out);
                    self.finished |= slot.closes;
                }
            }
        }
    }

    /// Prepare one parsed command and queue its reply slot.
    fn submit(&mut self, cmd: Command) {
        let spec = crate::command::lookup(&cmd[0]);
        let trace = CommandTrace::begin(self.metrics, spec, &cmd);
        let span = trace.span().clone();
        let _entered = span.enter();
        // The route span covers the entry checks and the owner choice. A command that waits for
        // an earlier one on the same key ends it and opens another when it runs.
        let route = trace.route_span();
        let prepared = route.in_scope(|| cluster::prepare(&self.node, &mut self.conn, spec, cmd));
        let job = match prepared {
            Prepared::Done(value) => {
                trace.finish(&value);
                let slot = Slot {
                    protocol: self.conn.protocol,
                    value,
                    closes: self.conn.closing,
                };
                return self.ready(slot);
            }
            Prepared::Data(job) => job,
        };
        let protocol = self.conn.protocol;
        // With nothing in flight, the command waits for nothing. It registers its keys only when
        // it does not finish at once, so the single-node path skips the key order.
        let keys = Keys::of(job.spec.route, &job.cmd);
        let (wait, done, keys) = if self.pending.is_empty() {
            (WaitFor::default(), None, Some(keys))
        } else {
            let (wait, done) = self.order.admit(&keys);
            (wait, Some(done), None)
        };
        if wait.is_ready() {
            match cluster::run(&self.node, job, self.conn.writes.as_ref(), route) {
                Dispatch::Ready(reply) => {
                    drop(done);
                    trace.finish(&reply.value);
                    self.ready(Slot::reply(protocol, reply.value));
                }
                Dispatch::Pending(future) => {
                    let done = done.or_else(|| keys.map(|k| self.order.admit(&k).1));
                    self.pending.push_back(Box::pin(
                        async move {
                            let reply = future.await;
                            drop(done);
                            trace.finish(&reply.value);
                            Slot::reply(protocol, reply.value)
                        }
                        .instrument(span.clone()),
                    ))
                }
            }
            return;
        }
        drop(route);
        let node = self.node.clone();
        let writes = self.conn.writes.clone();
        self.pending.push_back(Box::pin(
            async move {
                wait.wait().await;
                let reply = cluster::run(&node, job, writes.as_ref(), trace.route_span())
                    .finish()
                    .await;
                drop(done);
                trace.finish(&reply.value);
                Slot::reply(protocol, reply.value)
            }
            .instrument(span.clone()),
        ));
    }
}

/// Run the transport setup of a new connection (TLS or the PROXY header), then serve it.
/// `redirect` marks a connection of the redirect listener.
pub async fn accept(
    tcp: TcpStream,
    peer: SocketAddr,
    node: Arc<Node>,
    hard_stop: CancellationToken,
    redirect: bool,
) {
    let local_addr = tcp.local_addr().ok();
    let accepted = tokio::select! {
        _ = hard_stop.cancelled() => return,
        accepted = node.security.accept(tcp, peer) => accepted,
    };
    match accepted {
        Ok((stream, auth)) => serve(stream, auth, node, hard_stop, redirect, local_addr).await,
        Err(e) => tracing::debug!(%peer, error = %e, "client connection setup failed"),
    }
}

/// Serve one connection until it closes.
pub async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    auth: ConnAuth,
    node: Arc<Node>,
    hard_stop: CancellationToken,
    redirect: bool,
    local_addr: Option<SocketAddr>,
) {
    let _guard = node.track_connection();
    let cfg = &node.config;
    // The parser limit is looser than the value limit so a moderately oversize value gets a clean
    // `value too large` error. A hugely oversize one still closes the connection.
    let limits = Limits {
        max_bulk_bytes: cfg
            .max_value_bytes
            .max(cfg.max_key_bytes)
            .saturating_add(1 << 20),
        ..Limits::default()
    };
    let max_pipeline = cfg.max_pipeline.max(1);
    let max_request = cfg.max_request_bytes;
    let mut parser = Parser::new(limits);
    let mut conn = ConnState::new(node.next_conn_id(), auth);
    conn.redirect = redirect;
    conn.local_addr = local_addr;
    if node.link().is_some() {
        conn.writes = Some(Arc::new(parking_lot::Mutex::new(node.new_write_set())));
    }
    let mut p = Pipeline {
        node: node.clone(),
        metrics: discus_telemetry::global(),
        conn,
        order: KeyOrder::new(),
        pending: FuturesOrdered::new(),
        out: BytesMut::new(),
        closing: false,
        finished: false,
    };

    loop {
        // Parse what is buffered, up to the pipeline limit.
        let mut starved = false;
        while !p.closing && p.pending.len() < max_pipeline {
            match parser.next() {
                Ok(Some(cmd)) => {
                    if node.security.wants_prefetch(&cmd) {
                        p.prefetch(&cmd).await;
                    }
                    p.submit(cmd);
                }
                Ok(None) => {
                    if parser.buffered() > max_request {
                        p.ready(Slot {
                            protocol: p.conn.protocol,
                            value: Value::error("ERR Protocol error: request too large"),
                            closes: true,
                        });
                    } else {
                        starved = true;
                    }
                    break;
                }
                Err(e) => {
                    p.ready(Slot {
                        protocol: p.conn.protocol,
                        value: Value::error(&e.to_string()),
                        closes: true,
                    });
                    break;
                }
            }
            if p.out.len() >= FLUSH_BYTES && !flush(&mut stream, &mut p.out, &hard_stop).await {
                return;
            }
        }
        if p.finished {
            flush(&mut stream, &mut p.out, &hard_stop).await;
            return;
        }
        if p.pending.is_empty() {
            if !flush(&mut stream, &mut p.out, &hard_stop).await || p.closing {
                return;
            }
            tokio::select! {
                _ = hard_stop.cancelled() => return,
                read = stream.read_buf(parser.buffer()) => match read {
                    Ok(0) | Err(_) => return,
                    Ok(_) => {}
                },
            };
            continue;
        }
        let can_read = starved && !p.closing && p.pending.len() < max_pipeline;
        tokio::select! {
            _ = hard_stop.cancelled() => return,
            Some(slot) = p.pending.next() => {
                p.encode(slot);
                // Drain every reply that is already done before deciding to flush.
                while let Some(Some(slot)) = futures::FutureExt::now_or_never(p.pending.next()) {
                    p.encode(slot);
                }
                if (p.pending.is_empty() || p.out.len() >= FLUSH_BYTES || p.finished)
                    && !flush(&mut stream, &mut p.out, &hard_stop).await
                {
                    return;
                }
                if p.finished {
                    return;
                }
            }
            read = stream.read_buf(parser.buffer()), if can_read => match read {
                // The client stopped writing: finish the pending replies, then end.
                Ok(0) => p.closing = true,
                Err(_) => return,
                Ok(_) => {}
            },
        }
    }
}
