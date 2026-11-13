#![allow(dead_code, clippy::unwrap_used)]

use std::net::SocketAddr;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

pub mod client_tls;
pub mod cluster;
pub mod counters;
pub mod gossip;

/// A node with both listeners. Dropping it shuts the node down.
pub struct Running {
    pub addr: SocketAddr,
    pub admin: SocketAddr,
    pub handle: tokio::task::JoinHandle<std::io::Result<()>>,
    pub shutdown: CancellationToken,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

/// Parse `argv`, which starts with the program name, as the flags of a test node. Adds the client
/// TLS flags of [`client_tls::server_args`].
pub fn config<S: AsRef<str>>(argv: &[S]) -> discus::Config {
    let mut all: Vec<String> = argv.iter().map(|a| a.as_ref().to_string()).collect();
    all.extend(client_tls::server_args(argv));
    discus::Config::parse_from(all).expect("valid test arguments")
}

/// Start a node on free ports, or return the reason it refused to start.
pub async fn try_start_full(args: &[&str]) -> Result<Running, String> {
    let mut argv = vec![
        "discus",
        "--listen",
        "127.0.0.1:0",
        "--admin-listen",
        "127.0.0.1:0",
    ];
    argv.extend_from_slice(args);
    let tls = client_tls::server_args(args);
    argv.extend(tls.iter().map(String::as_str));
    let config = discus::Config::parse_from(argv).map_err(|e| e.to_string())?;
    let server = discus::Server::bind(config)
        .await
        .map_err(|e| e.to_string())?;
    let (addr, admin) = (server.local_addr(), server.admin_addr());
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(server.run(shutdown.clone()));
    Ok(Running {
        addr,
        admin,
        handle,
        shutdown,
    })
}

/// Start a node that serves until the test process ends, and return its client address.
pub async fn start_node(args: &[&str]) -> SocketAddr {
    let node = try_start_full(args).await.expect("the node starts");
    let addr = node.addr;
    std::mem::forget(node);
    addr
}

/// Start a node and return a multiplexed redis-rs connection to it.
pub async fn connect(args: &[&str]) -> redis::aio::MultiplexedConnection {
    let addr = start_node(args).await;
    let client = client_tls::redis_client(&format!("rediss://{addr}"));
    client.get_multiplexed_async_connection().await.unwrap()
}

/// A command from its parts.
pub fn cmd(parts: &[&str]) -> Vec<bytes::Bytes> {
    parts
        .iter()
        .map(|p| bytes::Bytes::copy_from_slice(p.as_bytes()))
        .collect()
}

/// Send raw bytes on a fresh connection and return the first reply read.
pub async fn roundtrip(addr: SocketAddr, wire: &[u8]) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = client_tls::connect(addr).await;
    s.write_all(wire).await.unwrap();
    s.flush().await.unwrap();
    let mut buf = vec![0u8; 8192];
    let n = s.read(&mut buf).await.unwrap();
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

/// GET a path from the admin port and return the status code.
pub async fn status(admin: SocketAddr, path: &str) -> u16 {
    reqwest::get(format!("http://{admin}{path}"))
        .await
        .unwrap()
        .status()
        .as_u16()
}

/// Poll `path` on the admin port every 20 ms until it answers `want` or `timeout` passes.
pub async fn wait_status(admin: SocketAddr, path: &str, want: u16, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if status(admin, path).await == want {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
