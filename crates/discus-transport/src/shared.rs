//! The shared stream: one long-lived stream of a command-lane connection that carries many
//! requests and their replies (spec section 13.1).
//!
//! The dialer opens it with an empty frame, so the acceptor can tell it from a stream with one
//! request. Each request and reply is a normal frame. Replies come back in any order and carry
//! the request ID. A writer task sends all frames that wait in one write, so a busy connection
//! pays for fewer stream writes, packets, and task wakeups than with a stream per request.
//!
//! Reads use the stream too. The reply to a read can be large, and on the
//! shared stream it would delay the replies behind it past the short read timeout. So the
//! acceptor sends a reply over [`SHARED_REPLY_BYTES`] on a new unidirectional stream instead.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use discus_wire::{decode_frame, DecodeError, Frame, Limits};
use futures::StreamExt;
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::frame::{codec, read_frame};

/// The bytes that one write takes. Frames that wait past it go out in the next write.
const WRITE_BYTES: usize = 256 * 1024;

/// The largest reply that the acceptor sends on the shared stream. A larger one takes a
/// unidirectional stream.
pub(crate) const SHARED_REPLY_BYTES: usize = 64 * 1024;

/// Runs when a reply frame fails the verifier, with the reason.
pub(crate) type Rejected = Arc<dyn Fn(&str) + Send + Sync>;

/// What the writer task sends.
pub(crate) enum Out {
    Frame(Vec<u8>),
    /// Reset the stream after the frames before it.
    Reset(quinn::VarInt),
}

/// Write each frame with its length prefix, and finish the stream when every sender is gone or
/// `stop` fires.
pub(crate) async fn pump(
    mut send: quinn::SendStream,
    mut rx: mpsc::UnboundedReceiver<Out>,
    stop: CancellationToken,
) -> Result<(), String> {
    let mut buf = BytesMut::new();
    loop {
        let next = tokio::select! {
            _ = stop.cancelled() => None,
            out = rx.recv() => out,
        };
        let Some(mut out) = next else {
            let _ = send.finish();
            return Ok(());
        };
        let mut reset = None;
        loop {
            match out {
                Out::Frame(frame) => {
                    let len = u32::try_from(frame.len())
                        .map_err(|_| "a frame is larger than 4 GiB".to_string())?;
                    buf.put_u32(len);
                    buf.extend_from_slice(&frame);
                }
                Out::Reset(code) => {
                    reset = Some(code);
                    break;
                }
            }
            if buf.len() >= WRITE_BYTES {
                break;
            }
            match rx.try_recv() {
                Ok(o) => out = o,
                Err(_) => break,
            }
        }
        if !buf.is_empty() {
            send.write_all(&buf).await.map_err(|e| e.to_string())?;
            buf.clear();
        }
        if let Some(code) = reset {
            let _ = send.reset(code);
            return Ok(());
        }
    }
}

/// A reply: the decoded frame, or why the stream failed before the reply came.
pub(crate) type Reply = Result<Result<Frame, DecodeError>, String>;

/// The requests that wait for a reply, by request ID. `None` once the stream failed.
type Waiting = Mutex<Option<HashMap<u64, oneshot::Sender<Reply>>>>;

/// The dialer side of a shared stream. Dropping it finishes the stream.
pub(crate) struct Shared {
    out: mpsc::UnboundedSender<Out>,
    waiting: Arc<Waiting>,
}

impl Shared {
    /// Open the stream on `conn` and start its writer and reader tasks. `rejected` runs when a
    /// reply frame fails the verifier. Such a frame on the shared stream cannot be matched to a
    /// request, so the stream ends and its requests fail. A task also takes the large replies that
    /// come on unidirectional streams.
    pub(crate) async fn open(
        conn: &quinn::Connection,
        max_frame: usize,
        rejected: Rejected,
    ) -> Option<Arc<Shared>> {
        let (send, recv) = conn.open_bi().await.ok()?;
        let (out, rx) = mpsc::unbounded_channel();
        out.send(Out::Frame(Vec::new())).ok()?;
        let waiting: Arc<Waiting> = Arc::new(Mutex::new(Some(HashMap::new())));
        let stop = CancellationToken::new();
        tokio::spawn({
            let waiting = waiting.clone();
            let stop = stop.clone();
            async move {
                let reason = match pump(send, rx, stop.clone()).await {
                    Ok(()) => "the shared stream ended".to_string(),
                    Err(e) => e,
                };
                fail_all(&waiting, &stop, reason);
            }
        });
        tokio::spawn(read_large_replies(
            conn.clone(),
            max_frame,
            waiting.clone(),
            stop.clone(),
            rejected.clone(),
        ));
        tokio::spawn(read_replies(
            recv,
            max_frame,
            waiting.clone(),
            stop,
            rejected,
        ));
        Some(Arc::new(Shared { out, waiting }))
    }

    /// Send `frame` with `request_id` and wait for its reply. Returns the frame when the stream
    /// already failed: then it was not sent, and the caller can send it on a stream of its own.
    pub(crate) async fn exchange(&self, request_id: u64, frame: Vec<u8>) -> Result<Reply, Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        match self.waiting.lock().as_mut() {
            Some(w) => w.insert(request_id, tx),
            None => return Err(frame),
        };
        // A timeout drops this future. The guard then forgets the request, so a late reply
        // finds no waiter.
        let _guard = Forget {
            waiting: &self.waiting,
            request_id,
        };
        if let Err(mpsc::error::SendError(Out::Frame(frame))) = self.out.send(Out::Frame(frame)) {
            return Err(frame);
        }
        Ok(rx
            .await
            .unwrap_or_else(|_| Err("the shared stream ended".to_string())))
    }
}

struct Forget<'a> {
    waiting: &'a Waiting,
    request_id: u64,
}

impl Drop for Forget<'_> {
    fn drop(&mut self) {
        if let Some(w) = self.waiting.lock().as_mut() {
            w.remove(&self.request_id);
        }
    }
}

/// Fail every waiting request, refuse new ones, and stop the writer.
fn fail_all(waiting: &Waiting, stop: &CancellationToken, reason: String) {
    stop.cancel();
    let Some(w) = waiting.lock().take() else {
        return;
    };
    for (_, tx) in w {
        let _ = tx.send(Err(reason.clone()));
    }
}

async fn read_replies(
    recv: quinn::RecvStream,
    max_frame: usize,
    waiting: Arc<Waiting>,
    stop: CancellationToken,
    rejected: Rejected,
) {
    let limits = Limits { max_frame };
    let mut frames = tokio_util::codec::FramedRead::new(recv, codec(max_frame));
    let reason = loop {
        let bytes = tokio::select! {
            _ = stop.cancelled() => break "the shared stream ended".to_string(),
            next = frames.next() => match next {
                None => break "the peer ended the shared stream".to_string(),
                Some(Err(e)) => break e.to_string(),
                Some(Ok(bytes)) => bytes,
            },
        };
        let (request_id, reply) = match decode_frame(&bytes, &limits) {
            Ok(frame) => (frame.request_id, Ok(frame)),
            Err(e @ DecodeError::UnknownType { request_id, .. }) => (request_id, Err(e)),
            Err(e @ DecodeError::Invalid(_)) => {
                let reason = e.to_string();
                rejected(&reason);
                break reason;
            }
        };
        deliver(&waiting, request_id, reply);
    };
    fail_all(&waiting, &stop, reason);
}

fn deliver(waiting: &Waiting, request_id: u64, reply: Result<Frame, DecodeError>) {
    let tx = waiting.lock().as_mut().and_then(|w| w.remove(&request_id));
    if let Some(tx) = tx {
        let _ = tx.send(Ok(reply));
    }
}

/// Take each unidirectional stream of `conn` until the shared stream ends. Each carries one
/// reply frame. A frame that fails the verifier cannot be matched, so its request times out, but
/// the shared stream goes on.
async fn read_large_replies(
    conn: quinn::Connection,
    max_frame: usize,
    waiting: Arc<Waiting>,
    stop: CancellationToken,
    rejected: Rejected,
) {
    let limits = Limits { max_frame };
    loop {
        let mut recv = tokio::select! {
            _ = stop.cancelled() => return,
            recv = conn.accept_uni() => match recv {
                Ok(recv) => recv,
                Err(_) => return,
            },
        };
        let waiting = waiting.clone();
        let rejected = rejected.clone();
        tokio::spawn(async move {
            let bytes = match read_frame(&mut recv, max_frame).await {
                Ok(Some(bytes)) => bytes,
                Ok(None) => return rejected("a large reply stream ended without a frame"),
                Err(e) => return rejected(&e.to_string()),
            };
            match decode_frame(&bytes, &limits) {
                Ok(frame) => deliver(&waiting, frame.request_id, Ok(frame)),
                Err(e @ DecodeError::UnknownType { request_id, .. }) => {
                    deliver(&waiting, request_id, Err(e))
                }
                Err(e @ DecodeError::Invalid(_)) => rejected(&e.to_string()),
            }
        });
    }
}
