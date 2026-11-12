//! A client connection, plain or TLS.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::server::TlsStream;

/// The peer of a TLS connection on an HTTP listener.
#[derive(Debug, Clone)]
pub struct TlsPeer {
    pub addr: SocketAddr,
    /// The identity of a verified client certificate.
    pub identity: Option<String>,
}

/// A TLS listener for `axum::serve`. Each handshake runs in its own task, so a slow client never
/// blocks the accept loop.
pub struct TlsListener {
    rx: mpsc::Receiver<(TlsStream<TcpStream>, TlsPeer)>,
    local: SocketAddr,
}

impl TlsListener {
    pub fn new(tcp: TcpListener, config: Arc<rustls::ServerConfig>) -> TlsListener {
        let local = tcp
            .local_addr()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)));
        let (tx, rx) = mpsc::channel(64);
        let acceptor = tokio_rustls::TlsAcceptor::from(config);
        tokio::spawn(async move {
            loop {
                let (tcp, addr) = tokio::select! {
                    _ = tx.closed() => return,
                    accepted = tcp.accept() => match accepted {
                        Ok(x) => x,
                        Err(_) => {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                            continue;
                        }
                    },
                };
                let (tx, acceptor) = (tx.clone(), acceptor.clone());
                tokio::spawn(async move {
                    let handshake =
                        tokio::time::timeout(super::HANDSHAKE_TIMEOUT, acceptor.accept(tcp)).await;
                    let Ok(Ok(tls)) = handshake else {
                        return;
                    };
                    let identity = super::identity::peer_identity(tls.get_ref().1);
                    let _ = tx.send((tls, TlsPeer { addr, identity })).await;
                });
            }
        });
        TlsListener { rx, local }
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = TlsPeer;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self.rx.recv().await {
            Some(conn) => conn,
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        Ok(TlsPeer {
            addr: self.local,
            identity: None,
        })
    }
}

/// A client connection: plain TCP or TLS.
pub type ClientStream = tokio_util::either::Either<TcpStream, Box<TlsStream<TcpStream>>>;
