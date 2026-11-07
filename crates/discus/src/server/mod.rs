//! The client listener.

use std::io;
use std::net::SocketAddr;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::Config;

mod connection;

/// A bound client listener.
pub struct Server {
    listener: TcpListener,
    config: Config,
}

impl Server {
    /// Bind the client listener.
    pub async fn bind(config: Config) -> io::Result<Server> {
        let listener = TcpListener::bind(config.listen).await?;
        Ok(Server { listener, config })
    }

    /// The address that the listener is bound to.
    pub fn local_addr(&self) -> SocketAddr {
        self.listener
            .local_addr()
            .expect("bound listener has an address")
    }

    /// Accept connections until the token is cancelled.
    pub async fn run(self, shutdown: CancellationToken) -> io::Result<()> {
        let _ = &self.config;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                accepted = self.listener.accept() => {
                    let (stream, _) = accepted?;
                    tokio::spawn(connection::serve(stream));
                }
            }
        }
    }
}
