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
//! parser needs more bytes, and while the replies that wait ready in the slots stay under a byte
//! limit. It flushes when the buffer reaches 1 MiB and when no slot is pending.
//! A client that does not read its replies stalls the flush, which stalls the next read, so memory
//! stays bounded.

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::BytesMut;
use discus_resp::{encode, Command, Limits, Parser, Protocol, Value, MAX_IDLE_BUFFER};
use discus_telemetry::Metrics;
use futures::future::{AbortHandle, Abortable, BoxFuture};
use futures::stream::{FuturesOrdered, StreamExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::Instant;
use tracing::Instrument;

use super::order::{KeyOrder, Keys, WaitFor};
use crate::cluster::{self, Dispatch, Prepared};
use crate::node::{ConnState, Node};
use crate::security::ConnAuth;
use crate::telemetry::CommandTrace;

/// Flush when this many reply bytes are pending.
const FLUSH_BYTES: usize = 1 << 20;

/// The largest bulk argument and the largest buffered request of a connection that has not met
/// the authentication policy yet. `HELLO` and `AUTH` with the largest token (16 KiB) fit.
const PREAUTH_REQUEST_BYTES: usize = 64 * 1024;

/// The most arguments in one command of such a connection. The longest command that it needs,
/// `HELLO 3 AUTH <user> <token> SETNAME <name>`, has 7.
const PREAUTH_ARRAY_LEN: usize = 64;

/// The most reply bytes, by estimate, that wait in the pending slots of one connection behind a
/// reply that is not ready. The loop parses no more commands above it.
const MAX_QUEUED_BYTES: usize = 4 * FLUSH_BYTES;

/// One reply in request order.
struct Slot {
    protocol: Protocol,
    value: Value,
    /// Close the connection after this reply.
    closes: bool,
    /// The bytes that this slot adds to [`Output::queued`].
    queued: usize,
}

impl Slot {
    fn reply(protocol: Protocol, value: Value) -> Slot {
        Slot {
            protocol,
            value,
            closes: false,
            queued: 0,
        }
    }
}

/// About the wire size of `value`. It only needs to be close enough to bound memory.
fn wire_estimate(value: &Value) -> usize {
    const FRAMING: usize = 16;
    FRAMING
        + match value {
            Value::Simple(b) | Value::Error(b) | Value::Bulk(b) => b.len(),
            Value::Array(items) => items.iter().map(wire_estimate).sum(),
            Value::Map(pairs) => pairs
                .iter()
                .map(|(k, v)| wire_estimate(k) + wire_estimate(v))
                .sum(),
            Value::Integer(_) | Value::Null | Value::Boolean(_) | Value::Double(_) => 0,
        }
}

/// The encoded replies that wait for a flush.
#[derive(Default)]
struct Output {
    buf: BytesMut,
    /// The estimated bytes of the replies that wait, ready, in the pending slots.
    queued: usize,
    /// The closing reply is in `buf`: flush and end.
    finished: bool,
}

impl Output {
    fn encode(&mut self, slot: Slot) {
        encode(&slot.value, slot.protocol, &mut self.buf);
        self.queued -= slot.queued;
        self.finished |= slot.closes;
    }
}

/// Encode every pending reply that is done, in order. The output is flushed each time it reaches
/// `FLUSH_BYTES`, so many large replies that are ready at once never sit in it together.
/// Returns false to end the connection.
async fn drain_ready<S: AsyncWrite + Unpin>(
    pending: &mut FuturesOrdered<BoxFuture<'static, Slot>>,
    out: &mut Output,
    stream: &mut S,
) -> bool {
    while let Some(Some(slot)) = futures::FutureExt::now_or_never(pending.next()) {
        out.encode(slot);
        if out.buf.len() >= FLUSH_BYTES && !flush(stream, &mut out.buf).await {
            return false;
        }
    }
    true
}

/// Write `out` to the client. Returns false to end the connection. A client that never reads
/// holds the write open until the node force-closes connections with `HardStop`.
async fn flush<S: AsyncWrite + Unpin>(stream: &mut S, out: &mut BytesMut) -> bool {
    if out.is_empty() {
        return true;
    }
    // A TLS stream can return from `write_all` with the last records still in its buffer when
    // the socket is full. Only `flush` sends them, so without it a client can wait forever for
    // the end of a large reply.
    let sent = stream.write_all(out).await.is_ok() && stream.flush().await.is_ok();
    out.clear();
    // One large reply must not keep its buffer for the life of the connection.
    if out.capacity() > MAX_IDLE_BUFFER {
        *out = BytesMut::new();
    }
    sent
}

/// Ends every connection at once when the drain runs out of time. A connection task checks one
/// atomic flag at each poll. A `CancellationToken` takes a mutex at each poll instead, and a
/// connection polls at each command.
pub struct HardStop {
    /// The abort handle of each running connection. `None` after `stop`.
    conns: parking_lot::Mutex<Option<HashMap<u64, AbortHandle>>>,
    next: std::sync::atomic::AtomicU64,
}

impl HardStop {
    pub fn new() -> Arc<HardStop> {
        Arc::new(HardStop {
            conns: parking_lot::Mutex::new(Some(HashMap::new())),
            next: Default::default(),
        })
    }

    /// Run `conn` until it ends or `stop` aborts it.
    pub fn run(self: &Arc<Self>, conn: impl Future<Output = ()>) -> impl Future<Output = ()> {
        let (handle, registration) = AbortHandle::new_pair();
        let id = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        match self.conns.lock().as_mut() {
            Some(conns) => {
                conns.insert(id, handle);
            }
            None => handle.abort(),
        }
        let registered = Registered {
            stop: self.clone(),
            id,
        };
        async move {
            let _registered = registered;
            let _ = Abortable::new(conn, registration).await;
        }
    }

    /// Abort every running connection and every later one.
    pub fn stop(&self) {
        let conns = self.conns.lock().take();
        for handle in conns.into_iter().flat_map(HashMap::into_values) {
            handle.abort();
        }
    }
}

/// Removes the abort handle of a connection when it ends.
struct Registered {
    stop: Arc<HardStop>,
    id: u64,
}

impl Drop for Registered {
    fn drop(&mut self) {
        if let Some(conns) = self.stop.conns.lock().as_mut() {
            conns.remove(&self.id);
        }
    }
}

struct Pipeline {
    node: Arc<Node>,
    metrics: &'static Metrics,
    conn: ConnState,
    order: KeyOrder,
    pending: FuturesOrdered<BoxFuture<'static, Slot>>,
    out: Output,
    /// A closing reply is queued: parse nothing more.
    closing: bool,
}

impl Pipeline {
    /// Queue a reply that is known now.
    fn ready(&mut self, mut slot: Slot) {
        self.closing |= slot.closes;
        if self.pending.is_empty() {
            self.out.encode(slot);
        } else {
            slot.queued = wire_estimate(&slot.value);
            self.out.queued += slot.queued;
            self.pending.push_back(Box::pin(async move { slot }));
        }
    }

    /// The async part of `AUTH` and `HELLO ... AUTH` (a JWKS refresh), and the introspection of an
    /// opaque grant that is due. Earlier commands keep running meanwhile, and their replies go
    /// into the output buffer in order. The command that waits is not submitted yet, so it sees
    /// the grant as the refresh left it.
    async fn prefetch(&mut self, cmd: &Command) {
        let node = self.node.clone();
        let fetch = node.security.prefetch(&mut self.conn, cmd);
        tokio::pin!(fetch);
        loop {
            tokio::select! {
                () = &mut fetch => return,
                Some(slot) = self.pending.next(), if !self.pending.is_empty() => {
                    self.out.encode(slot);
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
                    queued: 0,
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
pub async fn accept(tcp: TcpStream, peer: SocketAddr, node: Arc<Node>, redirect: bool) {
    let accepted = Instant::now();
    let local_addr = tcp.local_addr().ok();
    match node.security.accept(tcp, peer).await {
        Ok((stream, auth)) => serve(stream, auth, node, redirect, local_addr, accepted).await,
        Err(e) => tracing::debug!(%peer, error = %e, "client connection setup failed"),
    }
}

/// Serve one connection until it closes. A connection that does not meet the authentication
/// policy within `--auth-timeout-secs` of `accepted` closes.
pub async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    auth: ConnAuth,
    node: Arc<Node>,
    redirect: bool,
    local_addr: Option<SocketAddr>,
    accepted: Instant,
) {
    let timeout = node.live_settings.get().auth_timeout;
    let authenticated = AtomicBool::new(node.security.is_authenticated(&auth));
    let serving = serve_until_closed(
        &mut stream,
        auth,
        node,
        redirect,
        local_addr,
        &authenticated,
    );
    if timeout.is_zero() || authenticated.load(Ordering::Relaxed) {
        serving.await;
    } else {
        let deadline = tokio::time::sleep_until(accepted + timeout);
        tokio::pin!(deadline);
        // Once the policy is met, the deadline costs one atomic load at each wake of the task.
        let expired = std::future::poll_fn(|cx| {
            if authenticated.load(Ordering::Relaxed) {
                std::task::Poll::Pending
            } else {
                deadline.as_mut().poll(cx)
            }
        });
        tokio::select! {
            biased;
            () = serving => {}
            () = expired => tracing::debug!("client connection closed: no authentication in time"),
        }
    }
    // A TLS client sees a clean end only after close_notify. A client that never reads cannot
    // hold the task past the limit.
    let _ = tokio::time::timeout(CLOSE_TIMEOUT, stream.shutdown()).await;
}

/// How long the node tries to close a connection cleanly.
const CLOSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

async fn serve_until_closed<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: &mut S,
    auth: ConnAuth,
    node: Arc<Node>,
    redirect: bool,
    local_addr: Option<SocketAddr>,
    authenticated: &AtomicBool,
) {
    let _guard = node.track_connection();
    let cfg = &node.config;
    let max_pipeline = cfg.max_pipeline.max(1);
    let limits = cfg.parser_limits();
    // Until the connection meets the policy, the node buffers only what a credential needs.
    let mut preauth = !authenticated.load(Ordering::Relaxed);
    let (mut max_request, mut parser) = if preauth {
        let small = Limits {
            max_bulk_bytes: limits.max_bulk_bytes.min(PREAUTH_REQUEST_BYTES),
            max_array_len: limits.max_array_len.min(PREAUTH_ARRAY_LEN),
        };
        let max_request = cfg.max_request_bytes.min(PREAUTH_REQUEST_BYTES);
        (max_request, Parser::new(small))
    } else {
        (cfg.max_request_bytes, Parser::new(limits))
    };
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
        out: Output::default(),
        closing: false,
    };

    loop {
        // Parse what is buffered, up to the pipeline limit.
        let mut starved = false;
        while !p.closing && p.pending.len() < max_pipeline && p.out.queued < MAX_QUEUED_BYTES {
            match parser.next() {
                Ok(Some(cmd)) => {
                    if node.security.wants_prefetch(&p.conn, &cmd) {
                        p.prefetch(&cmd).await;
                    }
                    p.submit(cmd);
                    // `AUTH` changes the connection inside `submit`, even when its reply waits
                    // behind earlier ones. So the normal limits already hold for a large command
                    // that follows it in the same pipeline.
                    if preauth && node.security.is_authenticated(&p.conn.auth) {
                        preauth = false;
                        max_request = cfg.max_request_bytes;
                        parser.set_limits(limits);
                        authenticated.store(true, Ordering::Relaxed);
                    }
                }
                Ok(None) => {
                    if parser.buffered() > max_request {
                        p.ready(Slot {
                            protocol: p.conn.protocol,
                            value: Value::error("ERR Protocol error: request too large"),
                            closes: true,
                            queued: 0,
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
                        queued: 0,
                    });
                    break;
                }
            }
            if p.out.buf.len() >= FLUSH_BYTES && !flush(&mut stream, &mut p.out.buf).await {
                return;
            }
        }
        if p.out.finished {
            flush(&mut stream, &mut p.out.buf).await;
            return;
        }
        if p.pending.is_empty() {
            if !flush(&mut stream, &mut p.out.buf).await || p.closing {
                return;
            }
            match stream.read_buf(parser.buffer()).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
            continue;
        }
        let can_read = starved && !p.closing && p.pending.len() < max_pipeline;
        tokio::select! {
            Some(slot) = p.pending.next() => {
                p.out.encode(slot);
                // Drain every reply that is already done before deciding to flush.
                if !drain_ready(&mut p.pending, &mut p.out, &mut stream).await {
                    return;
                }
                if (p.pending.is_empty() || p.out.buf.len() >= FLUSH_BYTES || p.out.finished)
                    && !flush(&mut stream, &mut p.out.buf).await
                {
                    return;
                }
                if p.out.finished {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    /// A writer that, like a TLS stream with a full socket, keeps written bytes until a flush.
    #[derive(Default)]
    struct HoldsUntilFlush {
        held: Vec<u8>,
        sent: Vec<u8>,
    }

    impl AsyncWrite for HoldsUntilFlush {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.held.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            let held = std::mem::take(&mut self.held);
            self.sent.extend_from_slice(&held);
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            self.poll_flush(cx)
        }
    }

    #[tokio::test]
    async fn a_flush_sends_every_reply_byte() {
        let mut stream = HoldsUntilFlush::default();
        let mut out = BytesMut::from(&b"+OK\r\n"[..]);
        assert!(flush(&mut stream, &mut out).await);
        assert_eq!(stream.sent, b"+OK\r\n");
        assert!(stream.held.is_empty());
    }

    /// A large reply must not leave its buffer with the connection after the flush.
    #[tokio::test]
    async fn a_flush_drops_a_large_buffer() {
        let mut stream = HoldsUntilFlush::default();
        let mut out = BytesMut::with_capacity(8 << 20);
        out.extend_from_slice(&[b'x'; 4 << 20]);
        assert!(flush(&mut stream, &mut out).await);
        assert!(out.capacity() <= MAX_IDLE_BUFFER, "{}", out.capacity());
        let mut small = BytesMut::with_capacity(4096);
        small.extend_from_slice(b"+OK\r\n");
        assert!(flush(&mut stream, &mut small).await);
        assert_eq!(small.capacity(), 4096);
    }

    /// A writer that records the size of each flush.
    #[derive(Default)]
    struct Flushes {
        held: usize,
        sizes: Vec<usize>,
    }

    impl AsyncWrite for Flushes {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            self.held += buf.len();
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            let held = std::mem::take(&mut self.held);
            if held > 0 {
                self.sizes.push(held);
            }
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            self.poll_flush(cx)
        }
    }

    /// Many large replies that are ready at once go out in pieces of about `FLUSH_BYTES`.
    #[tokio::test]
    async fn a_drain_of_large_replies_flushes_as_it_goes() {
        let value = bytes::Bytes::from(vec![b'v'; 300 * 1024]);
        let mut pending: FuturesOrdered<BoxFuture<'static, Slot>> = FuturesOrdered::new();
        let mut out = Output::default();
        for _ in 0..20 {
            let mut slot = Slot::reply(Protocol::Resp2, Value::Bulk(value.clone()));
            slot.queued = wire_estimate(&slot.value);
            out.queued += slot.queued;
            pending.push_back(Box::pin(async move { slot }));
        }
        let mut stream = Flushes::default();
        assert!(drain_ready(&mut pending, &mut out, &mut stream).await);
        assert!(flush(&mut stream, &mut out.buf).await);
        assert_eq!(out.queued, 0);
        assert_eq!(stream.sizes.iter().sum::<usize>(), 20 * (value.len() + 11));
        assert!(
            stream
                .sizes
                .iter()
                .all(|&n| n < FLUSH_BYTES + value.len() + 16),
            "{:?}",
            stream.sizes
        );
    }
}
