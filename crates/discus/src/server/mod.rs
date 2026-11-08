//! The client listener and the node lifecycle.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use discus_membership::{LeaseProvider, NoneLease};
use discus_placement::{Clock, NTP64};
use discus_transport::{TlsMaterial, Transport, TransportConfig};

use crate::admin::serve_admin;
use crate::cluster::service::NodeService;
use crate::cluster::ClusterLink;
use crate::config::LeaseKind;
use crate::membership::{discovery, Membership};
use crate::security::Security;
use crate::{Config, Node, Phase};

/// Parts that tests supply instead of the defaults.
#[derive(Default)]
pub struct BindOptions {
    /// The QUIC socket, already bound, so a test knows every address first.
    pub cluster_socket: Option<std::net::UdpSocket>,
    /// The cluster lease instead of the one `--lease-provider` names.
    pub lease: Option<Arc<dyn LeaseProvider>>,
    /// The HLC clock and the wall clock, for example two halves of one mock clock slot.
    pub hlc_clock: Option<fn() -> NTP64>,
    pub wall: Option<Arc<dyn Clock>>,
}

/// The lease that `--lease-provider` names.
fn lease_provider(config: &Config) -> Result<Arc<dyn LeaseProvider>, String> {
    match config.lease_provider {
        LeaseKind::None => {
            tracing::warn!(
                "--lease-provider none cannot stop two nodes from forming two clusters. \
                 Use it for single-node development only"
            );
            Ok(Arc::new(NoneLease))
        }
        #[cfg(feature = "k8s")]
        LeaseKind::K8s => {
            const NAMESPACE_FILE: &str = "/var/run/secrets/kubernetes.io/serviceaccount/namespace";
            let namespace = config.lease_namespace.clone().unwrap_or_else(|| {
                std::fs::read_to_string(NAMESPACE_FILE)
                    .map(|s| s.trim().to_string())
                    .unwrap_or_else(|_| "default".into())
            });
            let duration = Duration::from_millis(config.lease_duration_ms);
            discus_membership::lease::K8sLease::in_cluster(&namespace, &config.lease_name, duration)
                .map(|l| Arc::new(l) as Arc<dyn LeaseProvider>)
                .map_err(|e| format!("--lease-provider k8s: {e}"))
        }
        #[cfg(not(feature = "k8s"))]
        LeaseKind::K8s => Err("--lease-provider k8s needs the k8s build feature".into()),
    }
}

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
        Server::bind_with(config, BindOptions::default()).await
    }

    /// Like `bind`, with the QUIC port already bound. Tests use it to learn every address
    /// before they configure the peers.
    pub async fn bind_with_cluster_socket(
        config: Config,
        socket: std::net::UdpSocket,
    ) -> io::Result<Server> {
        let opts = BindOptions {
            cluster_socket: Some(socket),
            ..BindOptions::default()
        };
        Server::bind_with(config, opts).await
    }

    pub async fn bind_with(config: Config, opts: BindOptions) -> io::Result<Server> {
        let socket = opts.cluster_socket;
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
        let lease = match (config.gossip_enabled(), opts.lease) {
            (true, Some(lease)) => Some(lease),
            (true, None) => Some(lease_provider(&config).map_err(invalid)?),
            (false, _) => None,
        };
        let node = Arc::new(match (opts.wall, opts.hlc_clock) {
            (Some(wall), Some(hlc_clock)) => Node::with_clocks(config, security, wall, hlc_clock),
            _ => Node::new(config, security),
        });
        node.set_counters(crate::counter::Counters::new(&node));
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
            if let Some(lease) = lease {
                let advertise = discovery::advertise(&node.config);
                let membership = Membership::new(&node, transport.clone(), lease, advertise);
                node.set_membership(membership);
            }
            node.set_link(ClusterLink::new(transport, &node.config));
            node.set_replicator(crate::replication::Replicator::new(&node));
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

    /// Serve until the token or the drain token of the node is cancelled, then drain.
    ///
    /// Draining sets the phase to `Leaving` (so `/readyz` turns false), marks the node `LEAVING`
    /// in gossip, stops accepting, and lets open connections finish their commands. After
    /// `shutdown_timeout_secs` the remaining connections are closed, and the node says goodbye
    /// to a few members and leaves gossip.
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
        if let Some(membership) = node.membership() {
            membership.spawn(stop_background.clone());
        }
        if let Some(replicator) = node.replicator() {
            replicator.spawn(stop_background.clone());
        }
        if let Some(counters) = node.counters() {
            counters.spawn(stop_background.clone());
        }
        if let Some(link) = node.link() {
            tracing::info!(cluster = %link.transport.local_addr(), "node-to-node transport");
            crate::telemetry::observe_transport(discus_telemetry::global(), &link.transport);
            // The transport events feed the peer error and QUIC failure metrics.
            let (transport, stop) = (link.transport.clone(), stop_background.clone());
            tokio::spawn(async move {
                crate::telemetry::observe_peer_events(discus_telemetry::global(), &transport, stop)
                    .await;
            });
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

        let drain = node.drain_token();
        let result = loop {
            tokio::select! {
                _ = shutdown.cancelled() => break Ok(()),
                _ = drain.cancelled() => break Ok(()),
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
        // LEAVING leaves placement in the next epoch, and the node keeps serving its open
        // connections as a handoff source until the deadline (spec section 15.3).
        if let Some(membership) = node.membership() {
            membership.drain();
        }
        tracing::info!(open = node.connected_clients(), "draining");
        drop(listener);
        tracker.close();
        let timeout = Duration::from_secs(node.config.shutdown_timeout_secs);
        // The node stays a handoff source and flushes its queues within the same deadline.
        let drained = async {
            tracker.wait().await;
            if let Some(replicator) = node.replicator() {
                replicator.idle().await;
            }
        };
        if tokio::time::timeout(timeout, drained).await.is_err() {
            tracing::warn!("drain timeout, closing remaining connections");
            hard_stop.cancel();
            tracker.wait().await;
        }
        if let Some(membership) = node.membership() {
            membership.depart().await;
        }
        stop_background.cancel();
        if let Some(link) = node.link() {
            link.transport.close();
        }
        let _ = admin_task.await;
        result
    }
}
