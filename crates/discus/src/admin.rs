//! The admin port: liveness, startup, and readiness probes.
//!
//! - `/healthz` is liveness. It answers 200 while the process runs.
//! - `/startupz` answers 200 once the node has been `NORMAL`, the store is ready, and the counter
//!   tier is settled (spec §15.2). It stays 200 while the node drains.
//! - `/readyz` answers 200 only while the node serves and is `NORMAL` in its epoch, so a drain
//!   (`LEAVING`) and a rejoin stop new connections.
//! - Both wait until every trusted OAuth 2.0 issuer has its keys (spec §14.3).
//! - Routes under `/admin/` need admin credentials (`security::admin`).

use std::io;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::security::admin::AdminPeer;
use crate::security::stream::TlsListener;
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

/// Serve the probes on `listener` until `shutdown` is cancelled.
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
    let app = app.into_make_service_with_connect_info::<AdminPeer>();
    let stop = async move { shutdown.cancelled().await };
    match tls {
        Some(config) => {
            axum::serve(TlsListener::new(listener, config), app)
                .with_graceful_shutdown(stop)
                .await
        }
        None => {
            axum::serve(listener, app)
                .with_graceful_shutdown(stop)
                .await
        }
    }
}
