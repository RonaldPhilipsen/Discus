//! One client connection.
//!
//! Commands run concurrently but reply in request order (spec section 7.1). The loop keeps a
//! `FuturesOrdered` of reply slots. A command that waits for nothing and needs no other node
//! runs inline. When nothing is pending, its reply goes straight into the output buffer, so the
//! single-node path allocates no future.
//!
//! Backpressure: the loop reads only while fewer than `max_pipeline` slots are pending and the
//! parser needs more bytes. It flushes when the buffer reaches 1 MiB and when no slot is pending.
//! A client that does not read its replies stalls the flush, which stalls the next read, so memory
//! stays bounded.

use std::sync::Arc;

use bytes::BytesMut;
use discus_resp::{encode, Command, Limits, Parser, Protocol, Value};
use futures::future::BoxFuture;
use futures::stream::{FuturesOrdered, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

use super::order::KeyOrder;
use crate::cluster::{self, Dispatch, Prepared};
use crate::node::{ConnState, Node};

/// Flush when this many reply bytes are pending.
const FLUSH_BYTES: usize = 1 << 20;
const READ_CHUNK: usize = 64 * 1024;

/// One reply in request order.
struct Slot {
    protocol: Protocol,
    value: Value,
    /// Close the connection after this reply.
    closes: bool,
}

/// Write `out` to the client, giving up when the node force-closes connections. A client that
/// never reads would otherwise hold the write open forever. Returns false to end the connection.
async fn flush(stream: &mut TcpStream, out: &mut BytesMut, hard_stop: &CancellationToken) -> bool {
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

    /// Prepare one parsed command and queue its reply slot.
    fn submit(&mut self, cmd: Command) {
        let job = match cluster::prepare(&self.node, &mut self.conn, cmd) {
            Prepared::Done(value) => {
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
        let (wait, done) = self.order.admit(&job.keys());
        if wait.is_ready() {
            match cluster::run(&self.node, job) {
                Dispatch::Ready(reply) => {
                    drop(done);
                    self.ready(Slot {
                        protocol,
                        value: reply.value,
                        closes: false,
                    });
                }
                Dispatch::Pending(future) => self.pending.push_back(Box::pin(async move {
                    let reply = future.await;
                    drop(done);
                    Slot {
                        protocol,
                        value: reply.value,
                        closes: false,
                    }
                })),
            }
            return;
        }
        let node = self.node.clone();
        self.pending.push_back(Box::pin(async move {
            wait.wait().await;
            let reply = match cluster::run(&node, job) {
                Dispatch::Ready(reply) => reply,
                Dispatch::Pending(future) => future.await,
            };
            drop(done);
            Slot {
                protocol,
                value: reply.value,
                closes: false,
            }
        }));
    }
}

pub async fn serve(mut stream: TcpStream, node: Arc<Node>, hard_stop: CancellationToken) {
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
    let conn = ConnState::new(node.next_conn_id(), cfg.shared_token.is_none());
    let mut p = Pipeline {
        node: node.clone(),
        conn,
        order: KeyOrder::new(),
        pending: FuturesOrdered::new(),
        out: BytesMut::new(),
        closing: false,
        finished: false,
    };
    let mut chunk = vec![0u8; READ_CHUNK];

    loop {
        // Parse what is buffered, up to the pipeline limit.
        let mut starved = false;
        while !p.closing && p.pending.len() < max_pipeline {
            match parser.next() {
                Ok(Some(cmd)) => p.submit(cmd),
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
            let n = tokio::select! {
                _ = hard_stop.cancelled() => return,
                read = stream.read(&mut chunk) => match read {
                    Ok(0) | Err(_) => return,
                    Ok(n) => n,
                },
            };
            parser.feed(&chunk[..n]);
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
            read = stream.read(&mut chunk), if can_read => match read {
                // The client stopped writing: finish the pending replies, then end.
                Ok(0) => p.closing = true,
                Err(_) => return,
                Ok(n) => parser.feed(&chunk[..n]),
            },
        }
    }
}
