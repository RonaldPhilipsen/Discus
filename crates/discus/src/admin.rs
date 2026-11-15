//! The admin port: liveness, startup, and readiness probes.
//!
//! - `/healthz` is liveness. It answers 200 while the process runs.
//! - `/startupz` answers 200 once the node has been `NORMAL`, the store is ready, and the counter
//!   tier is settled (spec §15.2). It stays 200 while the node drains.
//! - `/readyz` answers 200 only while the node serves and is `NORMAL` in its epoch, so a drain
//!   (`LEAVING`) and a rejoin stop new connections.
//! - Both wait until every trusted OAuth 2.0 issuer has its keys (spec §14.3).
//! - Routes under `/admin/` need admin credentials (`security::admin`).
//!
//! The server runs on its own thread, `discus-admin`. It takes at most `MAX_CONNECTIONS`
//! connections at once, TLS handshakes included, and serves one request per connection.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{ConnectInfo, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Extension, Router};
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::time::{timeout_at, Instant};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::security::admin::AdminPeer;
use crate::security::identity::peer_identity;
use crate::security::HANDSHAKE_TIMEOUT;
use crate::server::accept::{accept, Backoff};
use crate::{Node, Phase};

fn body(node: &Node, ok: bool) -> (StatusCode, &'static str) {
    let code = if ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, node.phase().name())
}

async fn healthz(State(node): State<Arc<Node>>) -> (StatusCode, &'static str) {
    body(&node, true)
}

async fn startupz(State(node): State<Arc<Node>>) -> (StatusCode, &'static str) {
    let ok = matches!(node.phase(), Phase::Serving | Phase::Leaving)
        && node.security.ready()
        && node.reached_normal()
        && node.store_ready()
        && node.counters_settled();
    body(&node, ok)
}

async fn readyz(State(node): State<Arc<Node>>) -> (StatusCode, &'static str) {
    let ok = node.phase() == Phase::Serving && node.security.ready() && node.is_normal();
    body(&node, ok)
}

/// The most admin connections at once, TLS handshakes included. The node closes a connection
/// above it at once.
pub const MAX_CONNECTIONS: usize = 64;
/// The time a connection gets to send the headers of its request.
pub const HEADER_TIMEOUT: Duration = Duration::from_secs(5);
/// The longest life of one admin connection, from the accept to the close. Keep-alive is off, so
/// a connection serves one request, and this bounds a slow body or a slow reader.
pub const CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);

/// Serve the probes on `listener` until `shutdown` is cancelled. Open connections then finish
/// their request.
pub async fn serve_admin(
    node: Arc<Node>,
    listener: TcpListener,
    shutdown: CancellationToken,
) -> io::Result<()> {
    let tls = node.security.admin_tls();
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/startupz", get(startupz))
        .route("/readyz", get(readyz))
        .merge(crate::security::admin::routes(node.clone()))
        .with_state(node);
    let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let tracker = TaskTracker::new();
    let mut backoff = Backoff::default();
    loop {
        let (tcp, addr) = tokio::select! {
            _ = shutdown.cancelled() => break,
            conn = accept(&listener, &mut backoff, "admin") => conn,
        };
        let Ok(permit) = permits.clone().try_acquire_owned() else {
            tracing::debug!(%addr, "admin connection refused: the connection limit is reached");
            continue;
        };
        let (app, tls, stop) = (app.clone(), tls.clone(), shutdown.clone());
        tracker.spawn(async move {
            let _permit = permit;
            let deadline = Instant::now() + CONNECTION_TIMEOUT;
            let ip = Some(addr.ip());
            match tls {
                None => {
                    let peer = AdminPeer {
                        addr: ip,
                        ..AdminPeer::default()
                    };
                    serve_connection(app, tcp, peer, stop, deadline).await;
                }
                Some(config) => {
                    let acceptor = tokio_rustls::TlsAcceptor::from(config.load_full());
                    let limit = deadline.min(Instant::now() + HANDSHAKE_TIMEOUT);
                    let Ok(Ok(stream)) = timeout_at(limit, acceptor.accept(tcp)).await else {
                        return;
                    };
                    let peer = AdminPeer {
                        tls: true,
                        identity: peer_identity(stream.get_ref().1),
                        addr: ip,
                    };
                    serve_connection(app, stream, peer, stop, deadline).await;
                }
            }
        });
    }
    drop(listener);
    tracker.close();
    tracker.wait().await;
    Ok(())
}

/// Serve one HTTP/1 request on `io` until `deadline`.
async fn serve_connection<I>(
    app: Router,
    io: I,
    peer: AdminPeer,
    stop: CancellationToken,
    deadline: Instant,
) where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let service = TowerToHyperService::new(app.layer(Extension(ConnectInfo(peer))));
    let mut builder = http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(HEADER_TIMEOUT)
        .keep_alive(false);
    let conn = builder.serve_connection(TokioIo::new(io), service);
    let mut conn = std::pin::pin!(conn);
    tokio::select! {
        _ = conn.as_mut() => {}
        _ = stop.cancelled() => {
            conn.as_mut().graceful_shutdown();
            let _ = timeout_at(deadline, conn).await;
        }
        _ = tokio::time::sleep_until(deadline) => {}
    }
}
