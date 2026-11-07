#![allow(dead_code, clippy::unwrap_used)]

use std::net::SocketAddr;

use tokio_util::sync::CancellationToken;

/// Start a node on free ports and return its client address.
pub async fn start_node(args: &[&str]) -> SocketAddr {
    start_node_raw(args).await.0
}

/// Start a node and return its client address, its task, and its shutdown token.
pub async fn start_node_raw(
    args: &[&str],
) -> (
    SocketAddr,
    tokio::task::JoinHandle<std::io::Result<()>>,
    CancellationToken,
) {
    let mut argv = vec![
        "discus",
        "--listen",
        "127.0.0.1:0",
        "--admin-listen",
        "127.0.0.1:0",
    ];
    argv.extend_from_slice(args);
    let config = discus::Config::parse_from(argv).expect("valid test arguments");
    let server = discus::Server::bind(config).await.expect("bind");
    let addr = server.local_addr();
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(server.run(shutdown.clone()));
    (addr, handle, shutdown)
}

/// Start a node and return a multiplexed redis-rs connection to it.
pub async fn connect(args: &[&str]) -> redis::aio::MultiplexedConnection {
    let addr = start_node(args).await;
    let client = redis::Client::open(format!("redis://{addr}")).unwrap();
    client.get_multiplexed_async_connection().await.unwrap()
}

/// Send raw bytes on a fresh connection and return the first reply read.
pub async fn roundtrip(addr: SocketAddr, wire: &[u8]) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(wire).await.unwrap();
    let mut buf = vec![0u8; 8192];
    let n = s.read(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

/// A node with both listeners, plus the handles that a test needs to stop it.
pub struct Running {
    pub addr: SocketAddr,
    pub admin: SocketAddr,
    pub handle: tokio::task::JoinHandle<std::io::Result<()>>,
    pub shutdown: CancellationToken,
}

/// Start a node and return both of its addresses and its shutdown handles.
pub async fn start_node_full(args: &[&str]) -> Running {
    let mut argv = vec![
        "discus",
        "--listen",
        "127.0.0.1:0",
        "--admin-listen",
        "127.0.0.1:0",
    ];
    argv.extend_from_slice(args);
    let config = discus::Config::parse_from(argv).expect("valid test arguments");
    let server = discus::Server::bind(config).await.expect("bind");
    let (addr, admin) = (server.local_addr(), server.admin_addr());
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(server.run(shutdown.clone()));
    Running {
        addr,
        admin,
        handle,
        shutdown,
    }
}

/// GET a path from the admin port and return the status code.
pub async fn status(admin: SocketAddr, path: &str) -> u16 {
    reqwest::get(format!("http://{admin}{path}"))
        .await
        .unwrap()
        .status()
        .as_u16()
}
