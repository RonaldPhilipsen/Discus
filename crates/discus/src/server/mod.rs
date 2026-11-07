//! The client listener and the node lifecycle.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::admin::serve_admin;
use crate::{Config, Node, Phase};

mod connection;

/// A node with both listeners bound.
pub struct Server {
    listener: TcpListener,
    admin: TcpListener,
    node: Arc<Node>,
}

impl Server {
    /// Bind the client and admin listeners.
    pub async fn bind(config: Config) -> io::Result<Server> {
        let listener = TcpListener::bind(config.listen).await?;
        let admin = TcpListener::bind(config.admin_listen).await?;
        Ok(Server {
            listener,
            admin,
            node: Arc::new(Node::new(config)),
        })
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
        let admin_task = tokio::spawn(serve_admin(node.clone(), admin, stop_background.clone()));
        let tracker = TaskTracker::new();
        let hard_stop = CancellationToken::new();
        node.set_phase(Phase::Serving);
        tracing::info!(addr = ?listener.local_addr().ok(), "serving");

        let result = loop {
            tokio::select! {
                _ = shutdown.cancelled() => break Ok(()),
                accepted = listener.accept() => match accepted {
                    Ok((stream, _)) => {
                        let _ = stream.set_nodelay(true);
                        tracker.spawn(connection::serve(stream, node.clone(), hard_stop.clone()));
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
        let _ = admin_task.await;
        result
    }
}
