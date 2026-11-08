mod common;

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn in_flight_connection_gets_its_reply_during_drain() {
    let mut node = common::try_start_full(&[]).await.unwrap();
    let (addr, shutdown) = (node.addr, node.shutdown.clone());
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    // Make sure the server accepted the connection before the drain starts.
    s.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    let mut buf = [0u8; 7];
    s.read_exact(&mut buf).await.unwrap();

    shutdown.cancel();
    s.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    s.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"+PONG\r\n");
    drop(s);
    tokio::time::timeout(Duration::from_secs(5), &mut node.handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(tokio::net::TcpStream::connect(addr).await.is_err());
}

#[tokio::test]
async fn drain_gives_up_after_the_timeout_and_closes_idle_connections() {
    let mut node = common::try_start_full(&["--shutdown-timeout-secs", "1"])
        .await
        .unwrap();
    let (addr, shutdown) = (node.addr, node.shutdown.clone());
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    let mut buf = [0u8; 7];
    s.read_exact(&mut buf).await.unwrap();

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), &mut node.handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let n = tokio::time::timeout(Duration::from_secs(2), s.read(&mut buf))
        .await
        .unwrap();
    assert!(
        matches!(n, Ok(0) | Err(_)),
        "the connection must be closed after the drain timeout"
    );
}
