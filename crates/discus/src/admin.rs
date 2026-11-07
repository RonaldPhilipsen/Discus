//! The admin port: liveness, startup, and readiness probes.
//!
//! - `/healthz` is liveness. It answers 200 while the process runs.
//! - `/startupz` answers 200 once the node serves, and stays 200 while it drains.
//! - `/readyz` answers 200 only while the node serves, so a drain stops new connections.
//! - Both wait until every trusted OAuth 2.0 issuer has its keys (spec §14.3).

use std::io;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

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
    let ok = matches!(node.phase(), Phase::Serving | Phase::Leaving) && node.security.ready();
    body(&node, ok)
}

async fn readyz(State(node): State<Arc<Node>>) -> (StatusCode, &'static str) {
    let ok = node.phase() == Phase::Serving && node.security.ready();
    body(&node, ok)
}

/// Serve the probes on `listener` until `shutdown` is cancelled.
pub async fn serve_admin(
    node: Arc<Node>,
    listener: TcpListener,
    shutdown: CancellationToken,
) -> io::Result<()> {
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/startupz", get(startupz))
        .route("/readyz", get(readyz))
        .with_state(node);
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
}
