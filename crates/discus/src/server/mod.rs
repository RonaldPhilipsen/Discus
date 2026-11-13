//! The client listener and the node lifecycle.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use discus_membership::{LeaseProvider, NoneLease};
use discus_placement::{Clock, NTP64};
use discus_telemetry::attrs::TransportChannel;
use discus_transport::{Channels, Service, TlsMaterial, Transport, TransportConfig};

use crate::admin::serve_admin;
use crate::cluster::service::NodeService;
use crate::cluster::ClusterLink;
use crate::config::LeaseKind;
use crate::control::ControlLane;
use crate::membership::{discovery, Membership};
use crate::security::Security;
use crate::{Config, Node, Phase};

/// The HLC clock and the wall clock of one node.
pub type Clocks = (fn() -> NTP64, Arc<dyn Clock>);

/// Parts that tests supply instead of the defaults.
#[derive(Default)]
pub struct BindOptions {
    /// The QUIC socket, already bound, so a test knows every address first.
    pub cluster_socket: Option<std::net::UdpSocket>,
    /// The RPC QUIC socket, already bound, with gossip membership. Static membership ignores it.
    pub rpc_socket: Option<std::net::UdpSocket>,
    /// The cluster lease instead of the one `--lease-provider` names.
    pub lease: Option<Arc<dyn LeaseProvider>>,
    /// The HLC clock and the wall clock, for example two halves of one mock clock slot.
    pub clocks: Option<Clocks>,
    /// The counter group leases instead of the ones `--lease-provider` names.
    pub counter_leases: Option<crate::counter::CounterLeases>,
}

/// The Kubernetes lease `name` in the namespace of `--lease-namespace` or of the pod.
#[cfg(feature = "k8s")]
fn k8s_lease(
    config: &Config,
    name: &str,
    duration_ms: u64,
) -> Result<Arc<dyn LeaseProvider>, String> {
    const NAMESPACE_FILE: &str = "/var/run/secrets/kubernetes.io/serviceaccount/namespace";
    let namespace = config.lease_namespace.clone().unwrap_or_else(|| {
        std::fs::read_to_string(NAMESPACE_FILE)
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|_| "default".into())
    });
    let duration = Duration::from_millis(duration_ms);
    discus_membership::lease::K8sLease::in_cluster(&namespace, name, duration)
        .map(|l| Arc::new(l) as Arc<dyn LeaseProvider>)
        .map_err(|e| format!("--lease-provider k8s: {e}"))
}

#[cfg(not(feature = "k8s"))]
fn k8s_lease(_: &Config, _: &str, _: u64) -> Result<Arc<dyn LeaseProvider>, String> {
    Err("--lease-provider k8s needs the k8s build feature".into())
}

/// The counter group leases that `--lease-provider` names: one per group with `k8s`, none with
/// `none` or static membership (spec section 10.1).
fn counter_leases(config: &Config) -> Result<Option<crate::counter::CounterLeases>, String> {
    if !config.gossip_enabled() || config.lease_provider == LeaseKind::None {
        return Ok(None);
    }
    let leases = (0..config.counter_groups)
        .map(|g| {
            let name = config.counter_lease_name(g);
            k8s_lease(config, &name, config.counter_lease_duration_ms)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(Arc::new(move |g: u16| leases[usize::from(g)].clone())))
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
        LeaseKind::K8s => k8s_lease(config, &config.lease_name, config.lease_duration_ms),
    }
}

async fn bind_transport(
    config: TransportConfig,
    socket: Option<std::net::UdpSocket>,
    service: Arc<dyn Service>,
) -> io::Result<Transport> {
    match socket {
        Some(socket) => Transport::bind_socket(config, socket, service),
        None => Transport::bind(config, service).await,
    }
}

mod connection;
mod order;
mod tls_reload;

/// Export the metrics of `transport`. Its events feed the peer error and QUIC failure metrics.
/// The events task runs on the runtime of the caller.
fn observe(transport: &Transport, channel: TransportChannel, stop: CancellationToken) {
    let metrics = discus_telemetry::global();
    crate::telemetry::observe_transport(metrics, transport, channel);
    let transport = transport.clone();
    tokio::spawn(async move {
        crate::telemetry::observe_peer_events(metrics, &transport, channel, stop).await;
    });
}

/// Reload the client and cluster certificates after their files change.
fn watch_certificates(node: &Arc<Node>, transports: Vec<Transport>, stop: CancellationToken) {
    let config = &node.config;
    if config.tls_reload_secs == 0 {
        return;
    }
    let period = Duration::from_secs(config.tls_reload_secs);
    let security = node.security.clone();
    let files = security.client_tls_files();
    tls_reload::watch("client", files, period, stop.clone(), move || {
        security.reload_client_tls()
    });
    let (Some(ca), Some(cert), Some(key)) = (
        config.cluster_ca.clone(),
        config.cluster_cert.clone(),
        config.cluster_key.clone(),
    ) else {
        return;
    };
    if transports.is_empty() {
        return;
    }
    let san = config.cluster_san.clone();
    let files = vec![ca.clone(), cert.clone(), key.clone()];
    tls_reload::watch("cluster", files, period, stop, move || {
        let tls = TlsMaterial::from_files(&ca, &cert, &key, &san).map_err(io::Error::other)?;
        // Check every endpoint before any of them changes, so they never disagree.
        tls.check().map_err(io::Error::other)?;
        for t in &transports {
            t.reload_tls(&tls)?;
        }
        Ok(())
    });
}

/// Accept on `listener`, or wait forever without one.
async fn accept_on(listener: Option<&TcpListener>) -> io::Result<(TcpStream, SocketAddr)> {
    match listener {
        Some(l) => l.accept().await,
        None => std::future::pending().await,
    }
}

/// A node with both listeners bound.
pub struct Server {
    listener: TcpListener,
    redirect: Option<TcpListener>,
    admin: std::net::TcpListener,
    node: Arc<Node>,
    lane: ControlLane,
    /// The QUIC endpoints, for a reload of the cluster certificates.
    transports: Vec<Transport>,
}

impl Server {
    /// Bind the client and admin listeners, and the QUIC port when the node has peers or
    /// certificates.
    pub async fn bind(config: Config) -> io::Result<Server> {
        Server::bind_with(config, BindOptions::default()).await
    }

    pub async fn bind_with(mut config: Config, opts: BindOptions) -> io::Result<Server> {
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
            if config.cluster_san.is_empty() {
                tracing::warn!(
                    "--cluster-san is empty, so every certificate from --cluster-ca is a member. \
                     Use a CA that serves only this cluster"
                );
            }
            Some(tls)
        } else {
            None
        };
        let security =
            Arc::new(Security::from_config(&config).map_err(|e| invalid(e.to_string()))?);
        let listener = TcpListener::bind(config.listen).await?;
        let redirect = match config.redirect_listen {
            Some(addr) => Some(TcpListener::bind(addr).await?),
            None => None,
        };
        // The advertised client address needs the bound ports, which differ from the flags
        // when a flag asks for port 0.
        config.listen = listener.local_addr()?;
        if let Some(redirect) = &redirect {
            config.redirect_listen = Some(redirect.local_addr()?);
        }
        let admin = std::net::TcpListener::bind(config.admin_listen)?;
        admin.set_nonblocking(true)?;
        let lease = match (config.gossip_enabled(), opts.lease) {
            (true, Some(lease)) => Some(lease),
            (true, None) => Some(lease_provider(&config).map_err(invalid)?),
            (false, _) => None,
        };
        let node = Arc::new(match opts.clocks {
            Some((hlc_clock, wall)) => Node::with_clocks(config, security, wall, hlc_clock),
            None => Node::new(config, security),
        });
        let leases = match opts.counter_leases {
            Some(leases) => Some(leases),
            None => counter_leases(&node.config).map_err(invalid)?,
        };
        node.set_counters(crate::counter::Counters::new(&node, leases));
        node.set_data_handle(tokio::runtime::Handle::current());
        let lane = ControlLane::start()?;
        let mut transports = Vec::new();
        if let Some(tls) = tls {
            let cfg = &node.config;
            let mut tc = TransportConfig::new(cfg.cluster_listen, tls, node.id, node.cluster_id());
            tc.max_frame = cfg.cluster_max_frame_bytes;
            tc.idle_timeout = Duration::from_millis(cfg.cluster_idle_timeout_ms);
            tc.keep_alive = Duration::from_millis(cfg.cluster_keep_alive_ms);
            tc.handshake_timeout = Duration::from_millis(cfg.cluster_handshake_timeout_ms);
            let service: Arc<dyn Service> = Arc::new(NodeService(Arc::downgrade(&node)));
            let rpc = match lease {
                // Gossip membership: gossip on `--cluster-listen`, RPC on `--rpc-listen`.
                Some(lease) => {
                    let rpc_tc = TransportConfig {
                        listen: cfg.rpc_listen,
                        channels: Channels::RpcOnly,
                        ..tc.clone()
                    };
                    let rpc = bind_transport(rpc_tc, opts.rpc_socket, service.clone()).await?;
                    let gossip_tc = TransportConfig {
                        channels: Channels::GossipOnly,
                        ..tc
                    };
                    // quinn and the accept loop run on the runtime that binds the endpoint.
                    let gossip = lane
                        .spawn(bind_transport(gossip_tc, socket, service))
                        .await
                        .map_err(io::Error::other)??;
                    transports.push(gossip.clone());
                    let advertise = discovery::advertise(cfg).await;
                    let rpc_advertise = discovery::rpc_advertise(cfg, advertise, rpc.local_addr());
                    let membership = Membership::new(
                        &node,
                        gossip,
                        rpc.clone(),
                        lease,
                        advertise,
                        rpc_advertise,
                    );
                    node.set_membership(membership);
                    rpc
                }
                None => bind_transport(tc, socket, service).await?,
            };
            transports.push(rpc.clone());
            node.set_link(ClusterLink::new(rpc, &node.config));
            node.set_replicator(crate::replication::Replicator::new(&node));
        }
        Ok(Server {
            listener,
            redirect,
            admin,
            node,
            lane,
            transports,
        })
    }

    /// The QUIC address of `--cluster-listen`, when the transport runs. With gossip membership
    /// it serves gossip only.
    pub fn cluster_addr(&self) -> Option<SocketAddr> {
        self.node.cluster_addr()
    }

    /// The QUIC address that serves node-to-node RPC, when the transport runs.
    pub fn rpc_addr(&self) -> Option<SocketAddr> {
        self.node.rpc_addr()
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
            redirect,
            admin,
            node,
            lane,
            transports,
        } = self;
        let stop_background = CancellationToken::new();
        watch_certificates(&node, transports, stop_background.clone());
        {
            let node = node.clone();
            discus_store::every(
                Duration::from_millis(100),
                stop_background.clone(),
                move || {
                    #[cfg(feature = "testing")]
                    if node
                        .faults
                        .hold_sweep
                        .load(std::sync::atomic::Ordering::Relaxed)
                    {
                        return std::future::ready(());
                    }
                    node.store.sweep_expired(20);
                    std::future::ready(())
                },
            );
        }
        node.security.start(stop_background.clone());
        let admin_task = {
            let (node, stop) = (node.clone(), stop_background.clone());
            lane.spawn(async move {
                // A tokio listener belongs to the runtime that registers it, so the conversion
                // runs on the lane.
                let admin = TcpListener::from_std(admin)?;
                serve_admin(node, admin, stop).await
            })
        };
        let tracker = TaskTracker::new();
        let hard_stop = connection::HardStop::new();
        node.set_phase(Phase::Serving);
        tracing::info!(
            node_id = %node.id,
            cluster_id = %node.cluster_id(),
            epoch = %node.epochs().current().id(),
            "bootstrapped"
        );
        tracing::info!(addr = ?listener.local_addr().ok(), "serving");
        if let Some(redirect) = &redirect {
            tracing::info!(addr = ?redirect.local_addr().ok(), "serving redirects");
        }
        node.lag().spawn(stop_background.clone());
        node.spawn_epoch_work(stop_background.clone());
        if let Some(membership) = node.membership() {
            tracing::info!(gossip = %membership.gossip_transport().local_addr(), "gossip transport");
            crate::telemetry::observe_health(discus_telemetry::global(), membership);
            let _lane = lane.handle().enter();
            membership.spawn(stop_background.clone());
            observe(
                membership.gossip_transport(),
                TransportChannel::Gossip,
                stop_background.clone(),
            );
        }
        if let Some(replicator) = node.replicator() {
            replicator.spawn(stop_background.clone());
        }
        if let Some(counters) = node.counters() {
            counters.spawn(stop_background.clone());
        }
        if let Some(link) = node.link() {
            tracing::info!(rpc = %link.transport.local_addr(), "node-to-node transport");
            let channel = if node.membership().is_some() {
                TransportChannel::Rpc
            } else {
                TransportChannel::Cluster
            };
            observe(&link.transport, channel, stop_background.clone());
            // Dial every peer early, so the first forward does not pay the handshake.
            let epoch = node.epochs().current().clone();
            for m in epoch.members().iter().filter(|m| m.id != node.id) {
                let (transport, id, addr) = (link.transport.clone(), m.id, m.rpc_addr);
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
                        tracker.spawn(hard_stop.run(connection::accept(stream, peer, node.clone(), false)));
                    }
                    Err(e) => break Err(e),
                },
                accepted = accept_on(redirect.as_ref()) => match accepted {
                    Ok((stream, peer)) => {
                        let _ = stream.set_nodelay(true);
                        tracker.spawn(hard_stop.run(connection::accept(stream, peer, node.clone(), true)));
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
        drop(redirect);
        tracker.close();
        let timeout = Duration::from_secs(node.config.shutdown_timeout_secs);
        // The node stays a handoff source, flushes its queues, and hands its counter roles over
        // within the same deadline.
        let drained = async {
            tracker.wait().await;
            if let Some(replicator) = node.replicator() {
                replicator.idle().await;
            }
            // With gossip, the leaders move the counter roles of a LEAVING node away. A static
            // node has no such epoch, so it does not wait.
            if let (Some(counters), Some(_)) = (node.counters(), node.membership()) {
                counters.drained().await;
            }
        };
        if tokio::time::timeout(timeout, drained).await.is_err() {
            tracing::warn!("drain timeout, closing remaining connections");
            hard_stop.stop();
            tracker.wait().await;
        }
        if let Some(membership) = node.membership().cloned() {
            let _ = lane.spawn(async move { membership.depart().await }).await;
        }
        // The background tasks send on the transports, so they stop before the transports
        // close.
        stop_background.cancel();
        let _ = admin_task.await;
        if let Some(membership) = node.membership() {
            membership.gossip_transport().close();
        }
        if let Some(link) = node.link() {
            link.transport.close();
        }
        let _ = tokio::task::spawn_blocking(move || lane.shutdown()).await;
        result
    }
}
