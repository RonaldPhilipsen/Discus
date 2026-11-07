//! Authentication on the admin port (spec §14.1, §14.2).
//!
//! The probes stay open: the kubelet calls them without credentials, and they show only the
//! phase. Every route under `/admin/` needs an admin certificate or a bearer token with
//! `discus.admin`. The shared token never opens them.

use std::sync::Arc;

use axum::extract::{ConnectInfo, Request, State};
use axum::http::{header, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use tokio::net::TcpListener;

use super::stream::TlsListener;
use crate::Node;

/// What the admin port knows about its peer.
#[derive(Debug, Clone, Default)]
pub struct AdminPeer {
    /// The connection uses TLS.
    pub tls: bool,
    /// The identity of a verified client certificate.
    pub identity: Option<String>,
}

impl axum::extract::connect_info::Connected<axum::serve::IncomingStream<'_, TcpListener>>
    for AdminPeer
{
    fn connect_info(_: axum::serve::IncomingStream<'_, TcpListener>) -> AdminPeer {
        AdminPeer::default()
    }
}

impl axum::extract::connect_info::Connected<axum::serve::IncomingStream<'_, TlsListener>>
    for AdminPeer
{
    fn connect_info(stream: axum::serve::IncomingStream<'_, TlsListener>) -> AdminPeer {
        AdminPeer {
            tls: true,
            identity: stream.remote_addr().identity.clone(),
        }
    }
}

/// One trusted issuer on the status page.
#[derive(Debug, Serialize)]
pub struct IssuerStatus {
    pub issuer: String,
    pub loaded: bool,
    pub last_refresh_unix: Option<i64>,
}

/// The security status page.
#[derive(Debug, Serialize)]
pub struct SecurityStatus {
    pub tls_mode: &'static str,
    pub policy: String,
    pub ready: bool,
    pub issuers: Vec<IssuerStatus>,
}

async fn require_admin(
    State(node): State<Arc<Node>>,
    ConnectInfo(peer): ConnectInfo<AdminPeer>,
    request: Request,
    next: Next,
) -> Response {
    let bearer = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::to_string);
    if node.security.admin_allowed(&peer, bearer.as_deref()).await {
        next.run(request).await
    } else {
        (StatusCode::UNAUTHORIZED, "unauthorized\n").into_response()
    }
}

async fn security_status(State(node): State<Arc<Node>>) -> Json<SecurityStatus> {
    Json(node.security.status())
}

/// Start the drain, as SIGTERM does (spec section 15.3).
async fn drain(State(node): State<Arc<Node>>) -> (StatusCode, &'static str) {
    tracing::info!("drain requested on the admin port");
    node.drain_token().cancel();
    (StatusCode::ACCEPTED, "draining\n")
}

/// The protected admin routes.
pub fn routes(node: Arc<Node>) -> Router<Arc<Node>> {
    Router::new()
        .route("/admin/security", get(security_status))
        .route("/admin/drain", post(drain))
        .route_layer(middleware::from_fn_with_state(node, require_admin))
}
