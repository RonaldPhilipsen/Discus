//! The client listener and the node lifecycle.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use discus_transport::{TlsMaterial, Transport, TransportConfig};

use crate::admin::serve_admin;
use crate::cluster::service::NodeService;
use crate::cluster::ClusterLink;
use crate::security::Security;
use crate::{Config, Node, Phase};

mod connection;
mod order;

/// A node with both listeners bound.
pub struct Server {
    listener: TcpListener,
    admin: TcpListener,
    node: Arc<Node>,
}

impl Server {
    /// Bind the client and admin listeners, and the QUIC port when the node has peers or
    /// certificates.
    pub async fn bind(config: Config) -> io::Result<Server> {
        Server::bind_inner(config, None).await
    }

    /// Like `bind`, with the QUIC port already bound. Tests use it to learn every address
    /// before they configure the peers.
    pub async fn bind_with_cluster_socket(
        config: Config,
        socket: std::net::UdpSocket,
    ) -> io::Result<Server> {
        Server::bind_inner(config, Some(socket)).await
    }

    async fn bind_inner(config: Config, socket: Option<std::net::UdpSocket>) -> io::Result<Server> {
        let invalid = |msg: String| io::Error::new(io::ErrorKind::InvalidInput, msg);
        config.validate().map_err(invalid)?;
        let tls = if config.cluster_enabled() {
            let (Some(ca), Some(cert), Some(key)) = (
                &config.cluster_ca,
                &config.cluster_cert,
                &config.cluster_key,
            ) else {
                return Err(invalid(
                    "the cluster transport needs --cluster-ca, --cluster-cert, and --cluster-key"
                        .into(),
                ));
            };
            let tls = TlsMaterial::from_files(ca, cert, key, &config.cluster_san)
                .map_err(|e| invalid(e.to_string()))?;
            tls.check().map_err(|e| invalid(e.to_string()))?;
            Some(tls)
        } else {
            None
        };
        let security =
            Arc::new(Security::from_config(&config).map_err(|e| invalid(e.to_string()))?);
        let listener = TcpListener::bind(config.listen).await?;
        let admin = TcpListener::bind(config.admin_listen).await?;
        let node = Arc::new(Node::new(config, security));
        if let Some(tls) = tls {
            let cfg = &node.config;
            let mut tc = TransportConfig::new(cfg.cluster_listen, tls, node.id, node.cluster_id());
            tc.capabilities = cfg.cluster_capabilities;
            tc.max_frame = cfg.cluster_max_frame_bytes;
            tc.idle_timeout = Duration::from_millis(cfg.cluster_idle_timeout_ms);
            tc.keep_alive = Duration::from_millis(cfg.cluster_keep_alive_ms);
            tc.handshake_timeout = Duration::from_millis(cfg.cluster_handshake_timeout_ms);
            let service = Arc::new(NodeService(Arc::downgrade(&node)));
            let transport = match socket {
                Some(socket) => Transport::bind_socket(tc, socket, service)?,
                None => Transport::bind(tc, service).await?,
            };
            node.set_link(ClusterLink::new(transport, &node.config));
        }
        Ok(Server {
            listener,
            admin,
            node,
        })
    }

    /// The QUIC address, when the transport runs.
    pub fn cluster_addr(&self) -> Option<SocketAddr> {
        self.node.link().map(|l| l.transport.local_addr())
    }

    /// The address that the client listener is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.listener
            .local_addr()
            .expect("bound listener has an address")
    }

    /// The address that the admin listener is bound to.
    pub fn admin_addr(&self) -> SocketAddr {
        self.admin
            .local_addr()
            .expect("bound listener has an address")
    }

    pub fn node(&self) -> &Arc<Node> {
        &self.node
    }

    /// Serve until the token is cancelled, then drain.
    ///
    /// Draining sets the phase to `Leaving` (so `/readyz` turns false), stops accepting, and
    /// lets open connections finish their commands. After `shutdown_timeout_secs` the remaining
    /// connections are closed.
    pub async fn run(self, shutdown: CancellationToken) -> io::Result<()> {
        let Server {
            listener,
            admin,
            node,
        } = self;
        let stop_background = CancellationToken::new();
        discus_store::spawn_sweeper(
            node.store.clone(),
            Duration::from_millis(100),
            20,
            stop_background.clone(),
        );
        node.security.start(stop_background.clone());
        let admin_task = tokio::spawn(serve_admin(node.clone(), admin, stop_background.clone()));
        let tracker = TaskTracker::new();
        let hard_stop = CancellationToken::new();
        node.set_phase(Phase::Serving);
        tracing::info!(
            node_id = %node.id,
            cluster_id = %node.cluster_id(),
            epoch = %node.epochs().current().id(),
            "bootstrapped"
        );
        tracing::info!(addr = ?listener.local_addr().ok(), "serving");
        if let Some(link) = node.link() {
            tracing::info!(cluster = %link.transport.local_addr(), "node-to-node transport");
            // Dial every peer early, so the first forward does not pay the handshake.
            let epoch = node.epochs().current().clone();
            for m in epoch.members().iter().filter(|m| m.id != node.id) {
                let (transport, id, addr) = (link.transport.clone(), m.id, m.addr);
                tokio::spawn(async move {
                    if let Err(e) = transport.peer(id, addr).await {
                        tracing::warn!(peer = %id, %addr, error = %e, "cannot reach a peer");
                    }
                });
            }
        }

        let result = loop {
            tokio::select! {
                _ = shutdown.cancelled() => break Ok(()),
                accepted = listener.accept() => match accepted {
                    Ok((stream, peer)) => {
                        let _ = stream.set_nodelay(true);
                        tracker.spawn(connection::accept(stream, peer, node.clone(), hard_stop.clone()));
                    }
                    Err(e) => break Err(e),
                },
            }
        };

        node.set_phase(Phase::Leaving);
        tracing::info!(open = node.connected_clients(), "draining");
        drop(listener);
        tracker.close();
        let timeout = Duration::from_secs(node.config.shutdown_timeout_secs);
        if tokio::time::timeout(timeout, tracker.wait()).await.is_err() {
            tracing::warn!("drain timeout, closing remaining connections");
            hard_stop.cancel();
            tracker.wait().await;
        }
        stop_background.cancel();
        if let Some(link) = node.link() {
            link.transport.close();
        }
        let _ = admin_task.await;
        result
    }
}
