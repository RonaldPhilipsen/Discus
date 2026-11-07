//! The client listener.

use std::io;
use std::net::SocketAddr;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use std::sync::Arc;

use crate::{Config, Node};

mod connection;

/// A bound client listener.
pub struct Server {
    listener: TcpListener,
    node: Arc<Node>,
}

impl Server {
    /// Bind the client listener.
    pub async fn bind(config: Config) -> io::Result<Server> {
        let listener = TcpListener::bind(config.listen).await?;
        Ok(Server {
            listener,
            node: Arc::new(Node::new(config)),
        })
    }

    /// The address that the listener is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.listener
            .local_addr()
            .expect("bound listener has an address")
    }

    /// Accept connections until the token is cancelled.
    pub async fn run(self, shutdown: CancellationToken) -> io::Result<()> {
        discus_store::spawn_sweeper(
            self.node.store.clone(),
            std::time::Duration::from_millis(100),
            20,
            shutdown.clone(),
        );
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                accepted = self.listener.accept() => {
                    let (stream, _) = accepted?;
                    tokio::spawn(connection::serve(stream, self.node.clone()));
                }
            }
        }
    }
}
