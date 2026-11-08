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
    let mut client = tokio::net::TcpStream::connect(node.addr).await.unwrap();
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
