//! One client connection.

use std::sync::Arc;

use bytes::BytesMut;
use discus_resp::{encode, Limits, Parser, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

use crate::command::dispatch;
use crate::node::{ConnState, Node};
use crate::security::ConnAuth;

/// Flush when this many reply bytes are pending.
const FLUSH_BYTES: usize = 1 << 20;
const READ_CHUNK: usize = 64 * 1024;

/// Serve one connection until it closes.
///
/// Backpressure: the loop reads only after it flushed every reply for the previous read, and it
/// flushes at least every `max_pipeline` commands. A client that does not read its replies stalls
/// the flush, which stalls the next read, so memory stays bounded.
/// Write `out` to the client, giving up when the node force-closes connections. A client that
/// never reads would otherwise hold the write open forever. Returns false to end the connection.
async fn flush(stream: &mut TcpStream, out: &mut BytesMut, hard_stop: &CancellationToken) -> bool {
    let sent = tokio::select! {
        _ = hard_stop.cancelled() => false,
        written = stream.write_all(out) => written.is_ok(),
    };
    out.clear();
    sent
}

pub async fn serve(
    mut stream: TcpStream,
    auth: ConnAuth,
    node: Arc<Node>,
    hard_stop: CancellationToken,
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
    let mut parser = Parser::new(limits);
    let mut conn = ConnState::new(node.next_conn_id(), auth);
    let mut out = BytesMut::new();
    let mut chunk = vec![0u8; READ_CHUNK];
    let mut in_flight = 0usize;

    loop {
        let n = tokio::select! {
            _ = hard_stop.cancelled() => return,
            read = stream.read(&mut chunk) => match read {
                Ok(0) | Err(_) => return,
                Ok(n) => n,
            },
        };
        parser.feed(&chunk[..n]);
        loop {
            match parser.next() {
                Ok(Some(cmd)) => {
                    let reply = dispatch(&node, &mut conn, &cmd);
                    encode(&reply, conn.protocol, &mut out);
                    in_flight += 1;
                    if conn.closing {
                        flush(&mut stream, &mut out, &hard_stop).await;
                        return;
                    }
                    if in_flight >= cfg.max_pipeline.max(1) || out.len() >= FLUSH_BYTES {
                        if !flush(&mut stream, &mut out, &hard_stop).await {
                            return;
                        }
                        in_flight = 0;
                    }
                }
                Ok(None) => {
                    if parser.buffered() > cfg.max_request_bytes {
                        let e = Value::error("ERR Protocol error: request too large");
                        encode(&e, conn.protocol, &mut out);
                        flush(&mut stream, &mut out, &hard_stop).await;
                        return;
                    }
                    break;
                }
                Err(e) => {
                    encode(&Value::error(&e.to_string()), conn.protocol, &mut out);
                    flush(&mut stream, &mut out, &hard_stop).await;
                    return;
                }
            }
        }
        if !out.is_empty() && !flush(&mut stream, &mut out, &hard_stop).await {
            return;
        }
        in_flight = 0;
    }
}
