mod common;

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn in_flight_connection_gets_its_reply_during_drain() {
    let mut node = common::try_start_full(&[]).await.unwrap();
    let (addr, shutdown) = (node.addr, node.shutdown.clone());
    let mut s = common::client_tls::connect(addr).await;
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
    let mut s = common::client_tls::connect(addr).await;
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

#[tokio::test]
async fn drain_timeout_closes_a_connection_that_never_reads() {
    let mut node = common::try_start_full(&["--shutdown-timeout-secs", "1"])
        .await
        .unwrap();
    let (addr, shutdown) = (node.addr, node.shutdown.clone());
    let mut s = common::client_tls::connect(addr).await;
    let value = vec![b'x'; 100_000];
    let mut set = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$100000\r\n".to_vec();
    set.extend_from_slice(&value);
    set.extend_from_slice(b"\r\n");
    s.write_all(&set).await.unwrap();
    let mut ok = [0u8; 5];
    s.read_exact(&mut ok).await.unwrap();
    assert_eq!(&ok, b"+OK\r\n");
    // Far more reply bytes than the socket buffers hold, so the server blocks in its write.
    let gets = b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n".repeat(2_000);
    s.write_all(&gets).await.unwrap();

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(20), &mut node.handle)
        .await
        .expect("the hard stop must end a connection that is blocked in a write")
        .unwrap()
        .unwrap();
}
