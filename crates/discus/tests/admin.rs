mod common;

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn probes_reflect_the_phase() {
    let node = common::start_node_full(&[]).await;
    assert_eq!(common::status(node.admin, "/healthz").await, 200);
    assert_eq!(common::status(node.admin, "/startupz").await, 200);
    assert_eq!(common::status(node.admin, "/readyz").await, 200);

    // An open client connection keeps the drain pending, so the admin port stays up.
    let mut client = tokio::net::TcpStream::connect(node.addr).await.unwrap();
    client.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    let mut pong = [0u8; 7];
    client.read_exact(&mut pong).await.unwrap();

    node.shutdown.cancel();
    let mut ready = 200;
    for _ in 0..50 {
        ready = common::status(node.admin, "/readyz").await;
        if ready == 503 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(ready, 503);
    assert_eq!(common::status(node.admin, "/startupz").await, 200);
    assert_eq!(common::status(node.admin, "/healthz").await, 200);
    drop(client);
    tokio::time::timeout(Duration::from_secs(5), node.handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
