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
use futures::StreamExt;
use parking_lot::Mutex;
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};
use tokio_util::sync::CancellationToken;

use crate::events::{PeerEvent, PeerEventKind, TransportStats, EVENT_CAPACITY};
use crate::frame::{codec, read_frame, write_frame};
use crate::hello;
use crate::shared::{self, Out, Shared};
use crate::tls::TlsMaterial;
use crate::{ALPN_GOSSIP, ALPN_RPC, CLOSE_HANDSHAKE_FAILED, CLOSE_SHUTDOWN, RESET_BAD_FRAME};

/// Which pooled connection a request takes. Each peer has one connection per lane, so a slow
/// replication stream does not hold up forwarded commands (spec section 13.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Lane {
    /// Forwarded commands and other small request-reply traffic.
    Command,
    /// Replication, hints, handoff, and repair. A timeout here is not data-path evidence.
    Bulk,
}

/// The largest request that takes the shared stream. A larger one takes its own stream, so it
/// does not hold up the small requests behind it.
const SHARED_REQUEST_BYTES: usize = 64 * 1024;

/// The ALPN of a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Channel {
    /// `discus-rpc`: forwarded commands. `HELLO` requires the same cluster.
    Rpc,
    /// `discus-gossip`: membership. `HELLO` accepts another cluster.
    Gossip,
}

impl Channel {
    fn alpn(self) -> &'static [u8] {
        match self {
            Channel::Rpc => ALPN_RPC,
            Channel::Gossip => ALPN_GOSSIP,
        }
    }
}

/// The channels an endpoint serves and dials.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Channels {
    /// Both ALPNs on one endpoint.
    #[default]
    Both,
    /// Only `discus-gossip`: the control endpoint.
    GossipOnly,
    /// Only `discus-rpc`: the data endpoint.
    RpcOnly,
}

impl Channels {
    fn serves(self, channel: Channel) -> bool {
        matches!(
            (self, channel),
            (Channels::Both, _)
                | (Channels::GossipOnly, Channel::Gossip)
                | (Channels::RpcOnly, Channel::Rpc)
        )
    }
}

#[derive(Debug, Clone)]
pub struct TransportConfig {
    pub listen: SocketAddr,
    pub tls: TlsMaterial,
    pub node_id: NodeId,
    pub cluster_id: ClusterId,
    /// The channels this endpoint serves. A channel that it does not serve is also not dialed.
    pub channels: Channels,
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
            channels: Channels::Both,
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
    /// The cluster that the peer reported in `HELLO`. On the gossip channel it can differ.
    pub cluster_id: ClusterId,
    pub channel: Channel,
}

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
    /// The request never left this node: the stream did not open.
    #[error("the request was not sent: {0}")]
    NotSent(String),
}

impl TransportError {
    /// True when the peer cannot have seen the request, so a resend is always safe (spec
    /// section 7.5).
    pub fn is_unsent(&self) -> bool {
        matches!(
            self,
            TransportError::Connect(_)
                | TransportError::Handshake { .. }
                | TransportError::NotSent(_)
        )
    }
}

/// Why one request-reply exchange on a stream failed.
enum ExchangeError {
    /// The stream did not open, so nothing was sent.
    NotSent(String),
    /// The connection is lost.
    Lost(String),
    /// The stream failed but the connection can live on.
    Stream(String),
}

impl ExchangeError {
    fn reason(self) -> String {
        match self {
            ExchangeError::NotSent(r) | ExchangeError::Lost(r) | ExchangeError::Stream(r) => r,
        }
    }
}

/// Why a gossip request failed. Gossip failures are not data-path evidence, so they never reach
/// the event channel.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GossipError {
    /// No connection: the address does not answer, or the TLS handshake failed.
    #[error("unreachable: {0}")]
    Unreachable(String),
    /// The connection and `HELLO` succeeded, but no reply came in time.
    #[error("node {} did not answer", .0.node_id)]
    Silent(Box<PeerInfo>),
    /// `HELLO` failed, another node answered at the address, or the reply was not usable.
    #[error("refused: {0}")]
    Refused(String),
}

#[derive(Clone)]
struct Conn {
    conn: quinn::Connection,
    info: PeerInfo,
    /// The shared stream of a command-lane connection.
    shared: Option<Arc<Shared>>,
}

type Slot = Arc<tokio::sync::Mutex<Option<Conn>>>;

/// The QUIC client configs of the channels that an endpoint dials.
#[derive(Clone)]
struct Clients {
    rpc: Option<quinn::ClientConfig>,
    gossip: Option<quinn::ClientConfig>,
}

struct Inner {
    endpoint: quinn::Endpoint,
    /// A TLS reload replaces them. Open connections keep the configs of their handshake.
    clients: parking_lot::RwLock<Clients>,
    channels: Channels,
    quic: Arc<quinn::TransportConfig>,
    node_id: NodeId,
    cluster_id: parking_lot::RwLock<ClusterId>,
    capabilities: u64,
    max_frame: usize,
    handshake_timeout: Duration,
    /// Requests of one shared stream that run at the same time, like the stream limit.
    max_streams: usize,
    service: Arc<dyn Service>,
    pool: Mutex<HashMap<NodeId, Slot>>,
    bulk_pool: Mutex<HashMap<NodeId, Slot>>,
    /// Gossip connections by address, because a seed has no known node ID.
    gossip_pool: Mutex<HashMap<SocketAddr, Slot>>,
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
    // The acceptor of a shared stream sends large replies on unidirectional streams.
    t.max_concurrent_uni_streams(quinn::VarInt::from_u32(config.max_streams));
    let idle = quinn::IdleTimeout::try_from(config.idle_timeout)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
    t.max_idle_timeout(Some(idle));
    t.keep_alive_interval(Some(config.keep_alive));
    Ok(Arc::new(t))
}

fn other(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}

/// The QUIC server config and the client configs for `tls`.
fn quic_configs(
    tls: &TlsMaterial,
    channels: Channels,
    quic: &Arc<quinn::TransportConfig>,
) -> io::Result<(quinn::ServerConfig, Clients)> {
    tls.check().map_err(other)?;
    let alpns: Vec<&[u8]> = [Channel::Rpc, Channel::Gossip]
        .into_iter()
        .filter(|c| channels.serves(*c))
        .map(Channel::alpn)
        .collect();
    let server_tls = QuicServerConfig::try_from(tls.server_config(&alpns)).map_err(other)?;
    let mut server = quinn::ServerConfig::with_crypto(Arc::new(server_tls));
    server.transport_config(quic.clone());
    let client_for = |channel: Channel| -> io::Result<Option<quinn::ClientConfig>> {
        if !channels.serves(channel) {
            return Ok(None);
        }
        let tls = QuicClientConfig::try_from(tls.client_config(channel.alpn())).map_err(other)?;
        let mut client = quinn::ClientConfig::new(Arc::new(tls));
        client.transport_config(quic.clone());
        Ok(Some(client))
    };
    let clients = Clients {
        rpc: client_for(Channel::Rpc)?,
        gossip: client_for(Channel::Gossip)?,
    };
    Ok((server, clients))
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
        let quic = quinn_transport(&config)?;
        let (server, clients) = quic_configs(&config.tls, config.channels, &quic)?;
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
            clients: parking_lot::RwLock::new(clients),
            channels: config.channels,
            quic,
            node_id: config.node_id,
            cluster_id: parking_lot::RwLock::new(config.cluster_id),
            capabilities: config.capabilities,
            max_frame: config.max_frame,
            handshake_timeout: config.handshake_timeout,
            max_streams: config.max_streams as usize,
            service,
            pool: Mutex::new(HashMap::new()),
            bulk_pool: Mutex::new(HashMap::new()),
            gossip_pool: Mutex::new(HashMap::new()),
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

    /// The cluster that `HELLO` offers and that the RPC channel requires.
    pub fn cluster_id(&self) -> ClusterId {
        *self.0.cluster_id.read()
    }

    /// Change the cluster, after a join or a lost merge. Pooled RPC connections belong to the old
    /// cluster, so they are dropped.
    pub fn set_cluster_id(&self, cluster_id: ClusterId) {
        *self.0.cluster_id.write() = cluster_id;
        self.0.pool.lock().clear();
        self.0.bulk_pool.lock().clear();
    }

    /// Send one gossip message to `addr` on the gossip channel and wait for the reply. `expected`
    /// is the node that the caller believes is at `addr`.
    pub async fn gossip(
        &self,
        addr: SocketAddr,
        expected: Option<NodeId>,
        message: Message,
        timeout: Duration,
    ) -> Result<(PeerInfo, Message), GossipError> {
        let pool = &self.0.gossip_pool;
        let Conn { conn, info, .. } = self
            .cached_or_dial(pool, addr, None, addr, Channel::Gossip, false)
            .await
            .map_err(|e| match e {
                TransportError::Handshake { reason, .. } => GossipError::Refused(reason),
                e => GossipError::Unreachable(e.to_string()),
            })?;
        if let Some(want) = expected {
            if info.node_id != want {
                return Err(GossipError::Refused(format!(
                    "expected node {want}, found {}",
                    info.node_id
                )));
            }
        }
        let frame = Frame {
            request_id: self.0.next_request.fetch_add(1, Ordering::Relaxed),
            trace: TraceContext::default(),
            message,
        };
        let request = encode_frame(&frame);
        TransportStats::bump(&self.0.stats.requests_out);
        let bytes = match tokio::time::timeout(timeout, exchange(&conn, &info, &request)).await {
            Err(_) => return Err(GossipError::Silent(Box::new(info))),
            Ok(Err(e)) => {
                let reason = e.reason();
                if conn.close_reason().is_some() {
                    forget(&self.0.gossip_pool, addr, &conn);
                    return Err(GossipError::Unreachable(reason));
                }
                return Err(GossipError::Refused(reason));
            }
            Ok(Ok(bytes)) => bytes,
        };
        let limits = Limits {
            max_frame: info.max_frame,
        };
        match decode_frame(&bytes, &limits) {
            Ok(Frame {
                message: Message::Unsupported { message_type },
                ..
            }) => Err(GossipError::Refused(format!(
                "the peer does not support message type {message_type}"
            ))),
            Ok(reply) => Ok((info, reply.message)),
            Err(e) => {
                TransportStats::bump(&self.0.stats.frames_rejected);
                Err(GossipError::Refused(e.to_string()))
            }
        }
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
        let pool = self.pool(Lane::Command);
        let pooled = self
            .cached_or_dial(pool, node, Some(node), addr, Channel::Rpc, true)
            .await?;
        Ok(pooled.info)
    }

    /// Send one request on the command lane. See [`Transport::request_with`].
    pub async fn request(
        &self,
        node: NodeId,
        addr: SocketAddr,
        message: Message,
        trace: TraceContext,
        timeout: Duration,
        write: bool,
    ) -> Result<Frame, TransportError> {
        let build = |_: &PeerInfo| Ok(message);
        self.request_with(Lane::Command, node, addr, build, trace, timeout, write)
            .await
    }

    /// Send one request on `lane` and wait for its reply frame. `build` makes the request from
    /// the `PeerInfo` of the connection, or refuses to send it. `timeout` covers the request
    /// after the connection exists. `write` labels a timeout in the evidence channel. A bulk
    /// timeout is not evidence, because a large batch can be slow on a healthy peer.
    ///
    /// A small request on the command lane takes the shared stream. Any other request takes a
    /// new stream.
    #[allow(clippy::too_many_arguments)]
    pub async fn request_with(
        &self,
        lane: Lane,
        node: NodeId,
        addr: SocketAddr,
        build: impl FnOnce(&PeerInfo) -> Result<Message, TransportError>,
        trace: TraceContext,
        timeout: Duration,
        write: bool,
    ) -> Result<Frame, TransportError> {
        let Conn { conn, info, shared } = self
            .cached_or_dial(
                self.pool(lane),
                node,
                Some(node),
                addr,
                Channel::Rpc,
                lane == Lane::Command,
            )
            .await?;
        let frame = Frame {
            request_id: self.0.next_request.fetch_add(1, Ordering::Relaxed),
            trace,
            message: build(&info)?,
        };
        let request_id = frame.request_id;
        let sent_type = frame.message.type_code();
        let bytes = encode_frame(&frame);
        let shared = shared.filter(|_| bytes.len() <= SHARED_REQUEST_BYTES);
        let limits = Limits {
            max_frame: info.max_frame,
        };
        TransportStats::bump(&self.0.stats.requests_out);
        let sent = async {
            let bytes = match shared {
                Some(s) => match s.exchange(request_id, bytes).await {
                    Ok(reply) => return reply.map_err(ExchangeError::Stream),
                    Err(unsent) => unsent,
                },
                None => bytes,
            };
            let reply = exchange(&conn, &info, &bytes).await?;
            Ok(decode_frame(&reply, &limits))
        };
        let decoded = match tokio::time::timeout(timeout, sent).await {
            Err(_) => {
                if lane == Lane::Command {
                    emit(
                        &self.0,
                        Some(node),
                        addr,
                        PeerEventKind::ForwardTimeout { write },
                    );
                }
                return Err(TransportError::Timeout);
            }
            Ok(Err(e)) => {
                let lost = matches!(e, ExchangeError::NotSent(_) | ExchangeError::Lost(_));
                let unsent = matches!(e, ExchangeError::NotSent(_));
                let reason = e.reason();
                if lost || conn.close_reason().is_some() {
                    forget(self.pool(lane), node, &conn);
                    emit(
                        &self.0,
                        Some(node),
                        addr,
                        PeerEventKind::Reset(reason.clone()),
                    );
                    return Err(if unsent {
                        TransportError::NotSent(reason)
                    } else {
                        TransportError::Reset(reason)
                    });
                }
                return Err(TransportError::Frame(reason));
            }
            Ok(Ok(decoded)) => decoded,
        };
        match decoded {
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

    async fn open_shared(&self, conn: &Conn) -> Option<Arc<Shared>> {
        let inner = self.0.clone();
        let peer = conn.info.node_id;
        let rejected: shared::Rejected = Arc::new(move |reason: &str| {
            TransportStats::bump(&inner.stats.frames_rejected);
            tracing::warn!(%peer, reason, "dropped a reply frame on the shared stream");
        });
        Shared::open(&conn.conn, conn.info.max_frame, rejected).await
    }

    fn client(&self, channel: Channel) -> Option<quinn::ClientConfig> {
        let clients = self.0.clients.read();
        match channel {
            Channel::Rpc => clients.rpc.clone(),
            Channel::Gossip => clients.gossip.clone(),
        }
    }

    /// Use `tls` for every new handshake, in both directions. Open connections keep their
    /// certificates. On an error, the endpoint keeps its current certificates.
    pub fn reload_tls(&self, tls: &TlsMaterial) -> io::Result<()> {
        let (server, clients) = quic_configs(tls, self.0.channels, &self.0.quic)?;
        self.0.endpoint.set_server_config(Some(server));
        *self.0.clients.write() = clients;
        Ok(())
    }

    fn pool(&self, lane: Lane) -> &Mutex<HashMap<NodeId, Slot>> {
        match lane {
            Lane::Command => &self.0.pool,
            Lane::Bulk => &self.0.bulk_pool,
        }
    }

    /// The live pooled connection under `key`, or a new one to `addr`. Concurrent callers wait
    /// on the slot lock, so they share one dial. `node` is the expected peer of an RPC
    /// connection: its connection is watched, and its failed dial is evidence. `share` opens the
    /// shared stream on a new connection.
    async fn cached_or_dial<K: std::hash::Hash + Eq>(
        &self,
        pool: &Mutex<HashMap<K, Slot>>,
        key: K,
        node: Option<NodeId>,
        addr: SocketAddr,
        channel: Channel,
        share: bool,
    ) -> Result<Conn, TransportError> {
        if self.0.closed.is_cancelled() {
            return Err(TransportError::Connect("the transport is closed".into()));
        }
        if self.client(channel).is_none() {
            return Err(TransportError::Connect(format!(
                "this endpoint does not serve {channel:?}"
            )));
        }
        let slot = pool.lock().entry(key).or_default().clone();
        let mut guard = slot.lock().await;
        if let Some(c) = guard.as_ref() {
            if c.conn.close_reason().is_none() {
                return Ok(c.clone());
            }
        }
        *guard = None;
        let dial = tokio::time::timeout(self.0.handshake_timeout, self.dial(node, addr, channel));
        let result = match dial.await {
            Ok(r) => r,
            Err(_) => Err(TransportError::Connect("the handshake timed out".into())),
        };
        match (result, node) {
            (Ok(mut conn), node) => {
                if let Some(node) = node {
                    tokio::spawn(watch(self.0.clone(), node, conn.conn.clone()));
                }
                if share {
                    conn.shared = self.open_shared(&conn).await;
                }
                *guard = Some(conn.clone());
                Ok(conn)
            }
            (Err(e), Some(node)) => {
                TransportStats::bump(&self.0.stats.handshake_failures);
                let kind = PeerEventKind::HandshakeFailed(e.to_string());
                emit(&self.0, Some(node), addr, kind);
                Err(e)
            }
            (Err(e), None) => Err(e),
        }
    }

    async fn dial(
        &self,
        node: Option<NodeId>,
        addr: SocketAddr,
        channel: Channel,
    ) -> Result<Conn, TransportError> {
        let connect = |e: &dyn std::fmt::Display| TransportError::Connect(e.to_string());
        let client = self
            .client(channel)
            .ok_or_else(|| connect(&"this endpoint does not serve the channel"))?;
        let conn = self
            .0
            .endpoint
            .connect_with(client, addr, "discus")
            .map_err(|e| connect(&e))?
            .await
            .map_err(|e| connect(&e))?;
        let (mut send, mut recv) = conn.open_bi().await.map_err(|e| connect(&e))?;
        send_control(&mut send, Message::Hello(local_hello(&self.0)))
            .await
            .map_err(|e| connect(&e))?;
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
            Message::Hello(theirs) => match hello::check(
                *self.0.cluster_id.read(),
                self.0.capabilities,
                self.0.max_frame,
                &theirs,
                addr,
                node,
                channel,
            ) {
                Ok(info) => Ok(Conn {
                    conn,
                    info,
                    shared: None,
                }),
                Err((code, reason)) => Err(fail(code, reason)),
            },
            Message::HandshakeFailed(f) => Err(fail(f.code, f.reason)),
            other => Err(fail(
                HandshakeCode::Other,
                format!("expected HELLO, got message type {}", other.type_code()),
            )),
        }
    }
}

/// Write one handshake frame on `send` and finish the stream.
async fn send_control(send: &mut quinn::SendStream, message: Message) -> std::io::Result<()> {
    let frame = Frame {
        request_id: 0,
        trace: TraceContext::default(),
        message,
    };
    write_frame(send, &encode_frame(&frame)).await?;
    let _ = send.finish();
    Ok(())
}

/// Write one encoded request frame on a new stream and read the reply frame.
async fn exchange(
    conn: &quinn::Connection,
    info: &PeerInfo,
    frame: &[u8],
) -> Result<bytes::Bytes, ExchangeError> {
    let (mut send, mut recv) = conn
        .open_bi()
        .await
        .map_err(|e| ExchangeError::NotSent(e.to_string()))?;
    write_frame(&mut send, frame)
        .await
        .map_err(|e| ExchangeError::Lost(e.to_string()))?;
    let _ = send.finish();
    match read_frame(&mut recv, info.max_frame).await {
        Ok(Some(bytes)) => Ok(bytes),
        Ok(None) => Err(ExchangeError::Stream(
            "the peer ended the stream without a reply".to_string(),
        )),
        Err(e) if conn.close_reason().is_some() => Err(ExchangeError::Lost(e.to_string())),
        Err(e) => Err(ExchangeError::Stream(e.to_string())),
    }
}

/// Drop the pooled connection under `key` if it is still `conn`.
fn forget<K: std::hash::Hash + Eq>(
    pool: &Mutex<HashMap<K, Slot>>,
    key: K,
    conn: &quinn::Connection,
) {
    let Some(slot) = pool.lock().get(&key).cloned() else {
        return;
    };
    if let Ok(mut guard) = slot.try_lock() {
        if guard
            .as_ref()
            .is_some_and(|c| c.conn.stable_id() == conn.stable_id())
        {
            *guard = None;
        }
    };
}

fn emit(inner: &Inner, peer: Option<NodeId>, addr: SocketAddr, kind: PeerEventKind) {
    tracing::debug!(?peer, %addr, ?kind, "peer event");
    let _ = inner.events.send(PeerEvent { peer, addr, kind });
}

fn local_hello(inner: &Inner) -> Hello {
    Hello {
        node_id: inner.node_id,
        cluster_id: *inner.cluster_id.read(),
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
        let gossip = conn
            .handshake_data()
            .and_then(|d| d.downcast::<quinn::crypto::rustls::HandshakeData>().ok())
            .and_then(|d| d.protocol)
            .is_some_and(|p| p == Channel::Gossip.alpn());
        let channel = if gossip {
            Channel::Gossip
        } else {
            Channel::Rpc
        };
        let info = accept_hello(&inner, &conn, channel).await?;
        Ok::<_, String>((conn, info))
    };
    let handshake = tokio::time::timeout(inner.handshake_timeout, handshake)
        .await
        .unwrap_or_else(|_| Err("the handshake timed out".into()));
    let (conn, info) = match handshake {
        Ok(pair) => pair,
        Err(reason) => {
            TransportStats::bump(&inner.stats.handshake_failures);
            emit(&inner, None, addr, PeerEventKind::HandshakeFailed(reason));
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
                tokio::spawn(serve_stream(inner.clone(), conn.clone(), info, send, recv));
            }
            Err(_) => return,
        }
    }
}

/// Read the dialer `HELLO` on the first stream and answer it.
async fn accept_hello(
    inner: &Arc<Inner>,
    conn: &quinn::Connection,
    channel: Channel,
) -> Result<PeerInfo, String> {
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
            *inner.cluster_id.read(),
            inner.capabilities,
            inner.max_frame,
            &theirs,
            conn.remote_address(),
            None,
            channel,
        ),
        Ok(_) | Err(DecodeError::UnknownType { .. }) => Err((
            HandshakeCode::NotHello,
            "the first stream did not carry HELLO".into(),
        )),
        Err(e) => Err((HandshakeCode::NotHello, e.to_string())),
    };
    match checked {
        Ok(info) => {
            send_control(&mut send, Message::Hello(local_hello(inner)))
                .await
                .map_err(|e| e.to_string())?;
            Ok(info)
        }
        Err((code, reason)) => {
            let failed = HandshakeFailed {
                code,
                reason: reason.clone(),
            };
            let _ = send_control(&mut send, Message::HandshakeFailed(failed)).await;
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
    conn: quinn::Connection,
    peer: PeerInfo,
    mut send: quinn::SendStream,
    recv: quinn::RecvStream,
) {
    let mut frames = FramedRead::new(recv, codec(peer.max_frame));
    let request = match frames.next().await {
        None => return,
        Some(Err(e)) => return reject(&inner, &peer, &mut send, frames.get_mut(), &e.to_string()),
        Some(Ok(bytes)) if bytes.is_empty() => {
            return serve_shared(inner, conn, peer, send, frames).await;
        }
        Some(Ok(bytes)) => match decode_request(&inner, &peer, &bytes) {
            Ok(request) => request,
            Err(reason) => return reject(&inner, &peer, &mut send, frames.get_mut(), &reason),
        },
    };
    let reply = reply_to(inner, peer, request).await;
    if write_frame(&mut send, &encode_frame(&reply)).await.is_ok() {
        let _ = send.finish();
    }
}

/// Serve a shared stream until the dialer finishes it. Each request runs in its own task, at
/// most `max_streams` at a time, and its reply goes out when it is ready. At the limit the
/// reader waits, so flow control holds back the dialer as the stream limit does. A reply over
/// the shared limit goes on a new unidirectional stream.
async fn serve_shared(
    inner: Arc<Inner>,
    conn: quinn::Connection,
    peer: PeerInfo,
    send: quinn::SendStream,
    mut frames: FramedRead<quinn::RecvStream, LengthDelimitedCodec>,
) {
    let (out, rx) = mpsc::unbounded_channel();
    tokio::spawn(shared::pump(send, rx, inner.closed.child_token()));
    let limit = Arc::new(Semaphore::new(inner.max_streams));
    loop {
        let next = tokio::select! {
            _ = inner.closed.cancelled() => return,
            next = frames.next() => next,
        };
        let request = match next {
            None => return,
            Some(Ok(bytes)) => decode_request(&inner, &peer, &bytes),
            Some(Err(e)) => Err(e.to_string()),
        };
        let request = match request {
            Ok(request) => request,
            Err(reason) => {
                // The stream carries other requests, so a bad frame ends all of them. The dialer
                // then fails them at once instead of waiting for a timeout.
                rejected(&inner, &peer, &reason);
                let code = quinn::VarInt::from_u32(RESET_BAD_FRAME);
                let _ = frames.get_mut().stop(code);
                let _ = out.send(Out::Reset(code));
                return;
            }
        };
        TransportStats::bump(&inner.stats.shared_in);
        let Ok(permit) = limit.clone().acquire_owned().await else {
            return;
        };
        let inner = inner.clone();
        let conn = conn.clone();
        let out = out.clone();
        tokio::spawn(async move {
            let reply = encode_frame(&reply_to(inner.clone(), peer, request).await);
            if reply.len() > shared::SHARED_REPLY_BYTES {
                TransportStats::bump(&inner.stats.large_replies_out);
                // A failed send leaves the request to time out on the dialer, like a lost stream.
                if let Ok(mut send) = conn.open_uni().await {
                    if write_frame(&mut send, &reply).await.is_ok() {
                        let _ = send.finish();
                    }
                }
            } else {
                let _ = out.send(Out::Frame(reply));
            }
            drop(permit);
        });
    }
}

/// A request frame that passed the verifier.
enum Request {
    /// A message for the service.
    Call(Frame),
    /// An unknown message type, with its `UNSUPPORTED` reply.
    Unsupported(Frame),
}

/// Decode one request frame. `Err` holds why it failed the verifier.
fn decode_request(inner: &Inner, peer: &PeerInfo, bytes: &[u8]) -> Result<Request, String> {
    let limits = Limits {
        max_frame: peer.max_frame,
    };
    match decode_frame(bytes, &limits) {
        Ok(frame) => {
            TransportStats::bump(&inner.stats.requests_in);
            Ok(Request::Call(frame))
        }
        Err(DecodeError::UnknownType {
            message_type,
            request_id,
            trace,
        }) => {
            TransportStats::bump(&inner.stats.unsupported);
            Ok(Request::Unsupported(Frame {
                request_id,
                trace,
                message: Message::Unsupported { message_type },
            }))
        }
        Err(DecodeError::Invalid(reason)) => Err(reason),
    }
}

async fn reply_to(inner: Arc<Inner>, peer: PeerInfo, request: Request) -> Frame {
    match request {
        Request::Unsupported(reply) => reply,
        Request::Call(frame) => {
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
    }
}

fn rejected(inner: &Inner, peer: &PeerInfo, reason: &str) {
    TransportStats::bump(&inner.stats.frames_rejected);
    tracing::warn!(peer = %peer.node_id, addr = %peer.addr, reason, "dropped a frame");
}

/// Drop a bad frame and reset its stream.
fn reject(
    inner: &Inner,
    peer: &PeerInfo,
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
    reason: &str,
) {
    rejected(inner, peer, reason);
    let code = quinn::VarInt::from_u32(RESET_BAD_FRAME);
    let _ = recv.stop(code);
    let _ = send.reset(code);
}
