mod common;

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn probes_reflect_the_phase() {
    let mut node = common::try_start_full(&[]).await.unwrap();
    assert_eq!(common::status(node.admin, "/healthz").await, 200);
    assert_eq!(common::status(node.admin, "/startupz").await, 200);
    assert_eq!(common::status(node.admin, "/readyz").await, 200);

    // An open client connection keeps the drain pending, so the admin port stays up.
    let mut client = common::client_tls::connect(node.addr).await;
    client.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    let mut pong = [0u8; 7];
    client.read_exact(&mut pong).await.unwrap();

    node.shutdown.cancel();
    let second = Duration::from_secs(1);
    assert!(common::wait_status(node.admin, "/readyz", 503, second).await);
    assert_eq!(common::status(node.admin, "/startupz").await, 200);
    assert_eq!(common::status(node.admin, "/healthz").await, 200);
    drop(client);
    tokio::time::timeout(Duration::from_secs(5), &mut node.handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

/// Whether the peer closes `stream` within `within`.
async fn closed_within(stream: &mut tokio::net::TcpStream, within: Duration) -> bool {
    let mut buf = [0u8; 1];
    matches!(
        tokio::time::timeout(within, stream.read(&mut buf)).await,
        Ok(Ok(0)) | Ok(Err(_))
    )
}

#[tokio::test]
async fn the_admin_port_refuses_connections_above_its_limit() {
    let node = common::try_start_full(&[]).await.unwrap();
    let mut held = Vec::new();
    for _ in 0..discus::admin::MAX_CONNECTIONS {
        held.push(tokio::net::TcpStream::connect(node.admin).await.unwrap());
    }
    // Every held connection is idle, so none can be closed by the header timeout yet.
    let mut extra = tokio::net::TcpStream::connect(node.admin).await.unwrap();
    assert!(
        closed_within(&mut extra, Duration::from_secs(2)).await,
        "the connection above the limit stays open"
    );
    drop(held);
    assert!(common::wait_status(node.admin, "/healthz", 200, Duration::from_secs(10)).await);
}

#[tokio::test]
async fn the_admin_port_closes_a_connection_without_a_request() {
    let node = common::try_start_full(&[]).await.unwrap();
    let mut idle = tokio::net::TcpStream::connect(node.admin).await.unwrap();
    idle.write_all(b"GET /healthz HTTP/1.1\r\n").await.unwrap();
    let limit = discus::admin::HEADER_TIMEOUT + Duration::from_secs(10);
    assert!(
        closed_within(&mut idle, limit).await,
        "a connection that never sends its headers stays open"
    );
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn the_admin_server_has_its_own_thread() {
    let node = common::try_start_full(&[]).await.unwrap();
    assert_eq!(common::status(node.admin, "/healthz").await, 200);
    let names: Vec<String> = std::fs::read_dir("/proc/self/task")
        .unwrap()
        .filter_map(|t| std::fs::read_to_string(t.ok()?.path().join("comm")).ok())
        .map(|n| n.trim().to_string())
        .collect();
    assert!(names.iter().any(|n| n == "discus-admin"), "{names:?}");
}
