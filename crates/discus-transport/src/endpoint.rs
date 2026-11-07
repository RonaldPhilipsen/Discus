//! The QUIC endpoint: accept loop, `HELLO`, the outbound connection pool, and request streams.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use discus_placement::{ClusterId, EpochId, NodeId};
use discus_wire::{
    caps, decode_frame, encode_frame, DecodeError, Frame, HandshakeCode, HandshakeFailed, Hello,
    Limits, Message, TraceContext, DEFAULT_MAX_FRAME, HELLO_FRAME_LIMIT,
};
use futures::future::BoxFuture;
use parking_lot::Mutex;
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::events::{PeerEvent, PeerEventKind, TransportStats, EVENT_CAPACITY};
use crate::frame::{read_frame, write_frame};
use crate::hello;
use crate::tls::TlsMaterial;
use crate::{ALPN_RPC, CLOSE_HANDSHAKE_FAILED, CLOSE_SHUTDOWN, RESET_BAD_FRAME};

/// Settings for one transport.
#[derive(Debug, Clone)]
pub struct TransportConfig {
    pub listen: SocketAddr,
    pub tls: TlsMaterial,
    pub node_id: NodeId,
    pub cluster_id: ClusterId,
    /// The capability bits this node offers in `HELLO`.
    pub capabilities: u64,
    /// The largest frame this node accepts. `HELLO` agrees on the smaller offer.
    pub max_frame: usize,
    pub idle_timeout: Duration,
    pub keep_alive: Duration,
    /// Bounds a connect plus `HELLO`, in both directions.
    pub handshake_timeout: Duration,
    /// Concurrent bidirectional streams per connection.
    pub max_streams: u32,
}

impl TransportConfig {
    pub fn new(
        listen: SocketAddr,
        tls: TlsMaterial,
        node_id: NodeId,
        cluster_id: ClusterId,
    ) -> TransportConfig {
        TransportConfig {
            listen,
            tls,
            node_id,
            cluster_id,
            capabilities: caps::KNOWN,
            max_frame: DEFAULT_MAX_FRAME,
            idle_timeout: Duration::from_secs(30),
            keep_alive: Duration::from_secs(5),
            handshake_timeout: Duration::from_secs(2),
            max_streams: 1024,
        }
    }
}

/// What `HELLO` told about a peer, and what both sides agreed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerInfo {
    pub node_id: NodeId,
    pub addr: SocketAddr,
    pub capabilities: u64,
    pub max_frame: usize,
    /// The epoch that the peer reported in `HELLO`.
    pub epoch: EpochId,
}

/// One inbound request.
#[derive(Debug)]
pub struct Inbound {
    pub peer: PeerInfo,
    pub request_id: u64,
    pub trace: TraceContext,
    pub message: Message,
}

/// The node side of the transport: it reports the epoch for `HELLO` and answers requests.
pub trait Service: Send + Sync + 'static {
    fn epoch(&self) -> EpochId;
    fn call(self: Arc<Self>, inbound: Inbound) -> BoxFuture<'static, Message>;
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransportError {
    #[error("cannot connect: {0}")]
    Connect(String),
    #[error("HELLO failed ({code:?}): {reason}")]
    Handshake { code: HandshakeCode, reason: String },
    #[error("the request timed out")]
    Timeout,
    #[error("the connection was lost: {0}")]
    Reset(String),
    #[error("bad frame: {0}")]
    Frame(String),
    #[error("the peer does not support message type {0}")]
    PeerUnsupported(u16),
}

struct Conn {
    conn: quinn::Connection,
    info: PeerInfo,
}

type Slot = Arc<tokio::sync::Mutex<Option<Conn>>>;

struct Inner {
    endpoint: quinn::Endpoint,
    client: quinn::ClientConfig,
    node_id: NodeId,
    cluster_id: ClusterId,
    capabilities: u64,
    max_frame: usize,
    handshake_timeout: Duration,
    service: Arc<dyn Service>,
    pool: Mutex<HashMap<NodeId, Slot>>,
    events: broadcast::Sender<PeerEvent>,
    stats: TransportStats,
    next_request: AtomicU64,
    closed: CancellationToken,
}

/// A bound QUIC endpoint. Cheap to clone.
#[derive(Clone)]
pub struct Transport(Arc<Inner>);

fn quinn_transport(config: &TransportConfig) -> io::Result<Arc<quinn::TransportConfig>> {
    let mut t = quinn::TransportConfig::default();
    t.max_concurrent_bidi_streams(quinn::VarInt::from_u32(config.max_streams));
    t.max_concurrent_uni_streams(quinn::VarInt::from_u32(0));
    let idle = quinn::IdleTimeout::try_from(config.idle_timeout)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
    t.max_idle_timeout(Some(idle));
    t.keep_alive_interval(Some(config.keep_alive));
    Ok(Arc::new(t))
}

fn other(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

impl Transport {
    /// Bind the UDP port in `config.listen` and start accepting.
    pub async fn bind(config: TransportConfig, service: Arc<dyn Service>) -> io::Result<Transport> {
        let socket = std::net::UdpSocket::bind(config.listen)?;
        Transport::bind_socket(config, socket, service)
    }

    /// Start on an already bound UDP socket. Tests use it to learn every address first.
    pub fn bind_socket(
        config: TransportConfig,
        socket: std::net::UdpSocket,
        service: Arc<dyn Service>,
    ) -> io::Result<Transport> {
        let transport = quinn_transport(&config)?;
        let server_tls =
            QuicServerConfig::try_from(config.tls.server_config(&[ALPN_RPC])).map_err(other)?;
        let mut server = quinn::ServerConfig::with_crypto(Arc::new(server_tls));
        server.transport_config(transport.clone());
        let client_tls =
            QuicClientConfig::try_from(config.tls.client_config(ALPN_RPC)).map_err(other)?;
        let mut client = quinn::ClientConfig::new(Arc::new(client_tls));
        client.transport_config(transport);
        let runtime = quinn::default_runtime().ok_or_else(|| other("no async runtime"))?;
        let endpoint = quinn::Endpoint::new(
            quinn::EndpointConfig::default(),
            Some(server),
            socket,
            runtime,
        )?;
        let (events, _) = broadcast::channel(EVENT_CAPACITY);
        let inner = Arc::new(Inner {
            endpoint,
            client,
            node_id: config.node_id,
            cluster_id: config.cluster_id,
            capabilities: config.capabilities,
            max_frame: config.max_frame,
            handshake_timeout: config.handshake_timeout,
            service,
            pool: Mutex::new(HashMap::new()),
            events,
            stats: TransportStats::default(),
            next_request: AtomicU64::new(1),
            closed: CancellationToken::new(),
        });
        tokio::spawn(accept_loop(inner.clone()));
        Ok(Transport(inner))
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.0
            .endpoint
            .local_addr()
            .expect("a bound endpoint has an address")
    }

    pub fn node_id(&self) -> NodeId {
        self.0.node_id
    }

    /// Subscribe to data-path evidence.
    pub fn events(&self) -> broadcast::Receiver<PeerEvent> {
        self.0.events.subscribe()
    }

    pub fn stats(&self) -> &TransportStats {
        &self.0.stats
    }

    /// Close every connection and stop accepting.
    pub fn close(&self) {
        self.0.closed.cancel();
        self.0
            .endpoint
            .close(quinn::VarInt::from_u32(CLOSE_SHUTDOWN), b"shutdown");
    }

    /// Connect to `node` at `addr` and run `HELLO`, unless a live connection exists.
    pub async fn peer(&self, node: NodeId, addr: SocketAddr) -> Result<PeerInfo, TransportError> {
        self.connection(node, addr).await.map(|(_, info)| info)
    }

    /// Send one request on a new stream and wait for its reply frame. `timeout` covers the
    /// request after the connection exists. `write` labels a timeout in the evidence channel.
    pub async fn request(
        &self,
        node: NodeId,
        addr: SocketAddr,
        message: Message,
        trace: TraceContext,
        timeout: Duration,
        write: bool,
    ) -> Result<Frame, TransportError> {
        let (conn, info) = self.connection(node, addr).await?;
        let frame = Frame {
            request_id: self.0.next_request.fetch_add(1, Ordering::Relaxed),
            trace,
            message,
        };
        let sent_type = frame.message.type_code();
        TransportStats::bump(&self.0.stats.requests_out);
        let exchange = async {
            let (mut send, mut recv) = conn.open_bi().await.map_err(|e| (true, e.to_string()))?;
            write_frame(&mut send, &encode_frame(&frame))
                .await
                .map_err(|e| (true, e.to_string()))?;
            let _ = send.finish();
            match read_frame(&mut recv, info.max_frame).await {
                Ok(Some(bytes)) => Ok(bytes),
                Ok(None) => Err((
                    false,
                    "the peer ended the stream without a reply".to_string(),
                )),
                Err(e) => Err((conn.close_reason().is_some(), e.to_string())),
            }
        };
        let bytes = match tokio::time::timeout(timeout, exchange).await {
            Err(_) => {
                self.emit(Some(node), addr, PeerEventKind::ForwardTimeout { write });
                return Err(TransportError::Timeout);
            }
            Ok(Err((lost, reason))) => {
                if lost || conn.close_reason().is_some() {
                    self.forget(node, &conn);
                    self.emit(Some(node), addr, PeerEventKind::Reset(reason.clone()));
                    return Err(TransportError::Reset(reason));
                }
                return Err(TransportError::Frame(reason));
            }
            Ok(Ok(bytes)) => bytes,
        };
        let limits = Limits {
            max_frame: info.max_frame,
        };
        match decode_frame(&bytes, &limits) {
            Ok(Frame {
                message: Message::Unsupported { .. },
                ..
            }) => Err(TransportError::PeerUnsupported(sent_type)),
            Ok(reply) => Ok(reply),
            Err(e) => {
                TransportStats::bump(&self.0.stats.frames_rejected);
                tracing::warn!(peer = %node, %addr, error = %e, "dropped a reply frame");
                Err(TransportError::Frame(e.to_string()))
            }
        }
    }

    fn emit(&self, peer: Option<NodeId>, addr: SocketAddr, kind: PeerEventKind) {
        emit(&self.0, peer, addr, kind);
    }

    fn slot(&self, node: NodeId) -> Slot {
        self.0.pool.lock().entry(node).or_default().clone()
    }

    fn forget(&self, node: NodeId, conn: &quinn::Connection) {
        let slot = self.slot(node);
        if let Ok(mut guard) = slot.try_lock() {
            if guard
                .as_ref()
                .is_some_and(|c| c.conn.stable_id() == conn.stable_id())
            {
                *guard = None;
            }
        };
    }

    async fn connection(
        &self,
        node: NodeId,
        addr: SocketAddr,
    ) -> Result<(quinn::Connection, PeerInfo), TransportError> {
        if self.0.closed.is_cancelled() {
            return Err(TransportError::Connect("the transport is closed".into()));
        }
        let slot = self.slot(node);
        let mut guard = slot.lock().await;
        if let Some(c) = guard.as_ref() {
            if c.conn.close_reason().is_none() {
                return Ok((c.conn.clone(), c.info));
            }
        }
        *guard = None;
        let result = tokio::time::timeout(self.0.handshake_timeout, self.dial(node, addr)).await;
        let result = match result {
            Ok(r) => r,
            Err(_) => Err(TransportError::Connect("the handshake timed out".into())),
        };
        match result {
            Ok(conn) => {
                let out = (conn.conn.clone(), conn.info);
                tokio::spawn(watch(self.0.clone(), node, conn.conn.clone()));
                *guard = Some(conn);
                Ok(out)
            }
            Err(e) => {
                TransportStats::bump(&self.0.stats.handshake_failures);
                self.emit(
                    Some(node),
                    addr,
                    PeerEventKind::HandshakeFailed(e.to_string()),
                );
                Err(e)
            }
        }
    }

    async fn dial(&self, node: NodeId, addr: SocketAddr) -> Result<Conn, TransportError> {
        let connect = |e: &dyn std::fmt::Display| TransportError::Connect(e.to_string());
        let conn = self
            .0
            .endpoint
            .connect_with(self.0.client.clone(), addr, "discus")
            .map_err(|e| connect(&e))?
            .await
            .map_err(|e| connect(&e))?;
        let (mut send, mut recv) = conn.open_bi().await.map_err(|e| connect(&e))?;
        let hello = Frame {
            request_id: 0,
            trace: TraceContext::default(),
            message: Message::Hello(local_hello(&self.0)),
        };
        write_frame(&mut send, &encode_frame(&hello))
            .await
            .map_err(|e| connect(&e))?;
        let _ = send.finish();
        let bytes = read_frame(&mut recv, HELLO_FRAME_LIMIT)
            .await
            .map_err(|e| connect(&e))?
            .ok_or_else(|| TransportError::Connect("the peer closed the HELLO stream".into()))?;
        let reply = decode_frame(
            &bytes,
            &Limits {
                max_frame: HELLO_FRAME_LIMIT,
            },
        )
        .map_err(|e| connect(&e))?;
        let fail = |code: HandshakeCode, reason: String| {
            conn.close(
                quinn::VarInt::from_u32(CLOSE_HANDSHAKE_FAILED),
                reason.as_bytes(),
            );
            TransportError::Handshake { code, reason }
        };
        match reply.message {
            Message::Hello(theirs) => {
                match hello::check(
                    self.0.cluster_id,
                    self.0.capabilities,
                    self.0.max_frame,
                    &theirs,
                    Some(node),
                ) {
                    Ok(agreed) => Ok(Conn {
                        info: PeerInfo {
                            node_id: theirs.node_id,
                            addr,
                            capabilities: agreed.capabilities,
                            max_frame: agreed.max_frame,
                            epoch: theirs.epoch,
                        },
                        conn,
                    }),
                    Err((code, reason)) => Err(fail(code, reason)),
                }
            }
            Message::HandshakeFailed(f) => Err(fail(f.code, f.reason)),
            other => Err(fail(
                HandshakeCode::Other,
                format!("expected HELLO, got message type {}", other.type_code()),
            )),
        }
    }
}

fn emit(inner: &Inner, peer: Option<NodeId>, addr: SocketAddr, kind: PeerEventKind) {
    tracing::debug!(?peer, %addr, ?kind, "peer event");
    let _ = inner.events.send(PeerEvent { peer, addr, kind });
}

fn local_hello(inner: &Inner) -> Hello {
    Hello {
        node_id: inner.node_id,
        cluster_id: inner.cluster_id,
        capabilities: inner.capabilities,
        epoch: inner.service.epoch(),
        max_frame: u32::try_from(inner.max_frame).unwrap_or(u32::MAX),
    }
}

/// Report a reset when an outbound connection ends without a local close.
async fn watch(inner: Arc<Inner>, node: NodeId, conn: quinn::Connection) {
    let reason = conn.closed().await;
    if inner.closed.is_cancelled() || matches!(reason, quinn::ConnectionError::LocallyClosed) {
        return;
    }
    emit(
        &inner,
        Some(node),
        conn.remote_address(),
        PeerEventKind::Reset(reason.to_string()),
    );
}

async fn accept_loop(inner: Arc<Inner>) {
    loop {
        let incoming = tokio::select! {
            _ = inner.closed.cancelled() => return,
            incoming = inner.endpoint.accept() => match incoming {
                Some(i) => i,
                None => return,
            },
        };
        tokio::spawn(serve_connection(inner.clone(), incoming));
    }
}

async fn serve_connection(inner: Arc<Inner>, incoming: quinn::Incoming) {
    let addr = incoming.remote_address();
    let handshake = async {
        let conn = incoming.await.map_err(|e| e.to_string())?;
        let info = accept_hello(&inner, &conn).await?;
        Ok::<_, String>((conn, info))
    };
    let (conn, info) = match tokio::time::timeout(inner.handshake_timeout, handshake).await {
        Ok(Ok(pair)) => pair,
        Ok(Err(reason)) => {
            TransportStats::bump(&inner.stats.handshake_failures);
            emit(&inner, None, addr, PeerEventKind::HandshakeFailed(reason));
            return;
        }
        Err(_) => {
            TransportStats::bump(&inner.stats.handshake_failures);
            emit(
                &inner,
                None,
                addr,
                PeerEventKind::HandshakeFailed("the handshake timed out".into()),
            );
            return;
        }
    };
    loop {
        let stream = tokio::select! {
            _ = inner.closed.cancelled() => return,
            stream = conn.accept_bi() => stream,
        };
        match stream {
            Ok((send, recv)) => {
                tokio::spawn(serve_stream(inner.clone(), info, send, recv));
            }
            Err(_) => return,
        }
    }
}

/// Read the dialer `HELLO` on the first stream and answer it.
async fn accept_hello(inner: &Arc<Inner>, conn: &quinn::Connection) -> Result<PeerInfo, String> {
    let (mut send, mut recv) = conn.accept_bi().await.map_err(|e| e.to_string())?;
    let bytes = read_frame(&mut recv, HELLO_FRAME_LIMIT)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("the dialer closed the HELLO stream")?;
    let limits = Limits {
        max_frame: HELLO_FRAME_LIMIT,
    };
    let checked = match decode_frame(&bytes, &limits) {
        Ok(Frame {
            message: Message::Hello(theirs),
            ..
        }) => hello::check(
            inner.cluster_id,
            inner.capabilities,
            inner.max_frame,
            &theirs,
            None,
        )
        .map(|agreed| (theirs, agreed)),
        Ok(_) | Err(DecodeError::UnknownType { .. }) => Err((
            HandshakeCode::NotHello,
            "the first stream did not carry HELLO".into(),
        )),
        Err(e) => Err((HandshakeCode::NotHello, e.to_string())),
    };
    match checked {
        Ok((theirs, agreed)) => {
            let reply = Frame {
                request_id: 0,
                trace: TraceContext::default(),
                message: Message::Hello(local_hello(inner)),
            };
            write_frame(&mut send, &encode_frame(&reply))
                .await
                .map_err(|e| e.to_string())?;
            let _ = send.finish();
            Ok(PeerInfo {
                node_id: theirs.node_id,
                addr: conn.remote_address(),
                capabilities: agreed.capabilities,
                max_frame: agreed.max_frame,
                epoch: theirs.epoch,
            })
        }
        Err((code, reason)) => {
            let reply = Frame {
                request_id: 0,
                trace: TraceContext::default(),
                message: Message::HandshakeFailed(HandshakeFailed {
                    code,
                    reason: reason.clone(),
                }),
            };
            let _ = write_frame(&mut send, &encode_frame(&reply)).await;
            let _ = send.finish();
            // Let the reply reach the dialer before the close.
            let _ = tokio::time::timeout(Duration::from_millis(500), send.stopped()).await;
            conn.close(
                quinn::VarInt::from_u32(CLOSE_HANDSHAKE_FAILED),
                reason.as_bytes(),
            );
            Err(reason)
        }
    }
}

async fn serve_stream(
    inner: Arc<Inner>,
    peer: PeerInfo,
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
) {
    let reject = |send: &mut quinn::SendStream, recv: &mut quinn::RecvStream, reason: &str| {
        TransportStats::bump(&inner.stats.frames_rejected);
        tracing::warn!(peer = %peer.node_id, addr = %peer.addr, reason, "dropped a frame");
        let code = quinn::VarInt::from_u32(RESET_BAD_FRAME);
        let _ = recv.stop(code);
        let _ = send.reset(code);
    };
    let bytes = match read_frame(&mut recv, peer.max_frame).await {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return,
        Err(e) => return reject(&mut send, &mut recv, &e.to_string()),
    };
    let limits = Limits {
        max_frame: peer.max_frame,
    };
    let reply = match decode_frame(&bytes, &limits) {
        Ok(frame) => {
            TransportStats::bump(&inner.stats.requests_in);
            let message = inner
                .service
                .clone()
                .call(Inbound {
                    peer,
                    request_id: frame.request_id,
                    trace: frame.trace.clone(),
                    message: frame.message,
                })
                .await;
            Frame {
                request_id: frame.request_id,
                trace: frame.trace,
                message,
            }
        }
        Err(DecodeError::UnknownType {
            message_type,
            request_id,
            trace,
        }) => {
            TransportStats::bump(&inner.stats.unsupported);
            Frame {
                request_id,
                trace,
                message: Message::Unsupported { message_type },
            }
        }
        Err(DecodeError::Invalid(reason)) => return reject(&mut send, &mut recv, &reason),
    };
    if write_frame(&mut send, &encode_frame(&reply)).await.is_ok() {
        let _ = send.finish();
    }
}
