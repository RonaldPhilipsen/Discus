//! Failed authentication attempts are rate-limited per connection and per source (spec §14.2).
mod common;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

const WRONG: &[u8] = b"*2\r\n$4\r\nAUTH\r\n$5\r\nwrong\r\n";
const RIGHT: &[u8] = b"*2\r\n$4\r\nAUTH\r\n$6\r\ns3cret\r\n";

#[tokio::test]
async fn five_failures_close_the_connection() {
    let addr = common::start_node(&["--shared-token", "s3cret"]).await;
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(&WRONG.repeat(6)).await.unwrap();
    let mut all = Vec::new();
    tokio::time::timeout(std::time::Duration::from_secs(5), s.read_to_end(&mut all))
        .await
        .expect("the node closes the connection")
        .unwrap();
    let text = String::from_utf8_lossy(&all);
    assert_eq!(text.matches("-WRONGPASS").count(), 5, "{text:?}");
}

#[tokio::test]
async fn a_source_over_its_limit_is_refused_without_a_check() {
    let addr = common::start_node(&[
        "--shared-token",
        "s3cret",
        "--auth-max-failures-per-source",
        "3",
    ])
    .await;
    assert_eq!(common::roundtrip(addr, RIGHT).await, "+OK\r\n");
    for _ in 0..3 {
        let r = common::roundtrip(addr, WRONG).await;
        assert!(r.starts_with("-WRONGPASS"), "{r:?}");
    }
    let r = common::roundtrip(addr, RIGHT).await;
    assert!(r.starts_with("-WRONGPASS"), "{r:?}");
}

#[tokio::test]
async fn a_failed_auth_keeps_an_earlier_success() {
    let addr = common::start_node(&["--shared-token", "s3cret"]).await;
    let mut wire = RIGHT.to_vec();
    wire.extend_from_slice(WRONG);
    wire.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(&wire).await.unwrap();
    let mut buf = Vec::new();
    while !String::from_utf8_lossy(&buf).contains("PONG") {
        let mut chunk = [0u8; 256];
        let n = s.read(&mut chunk).await.unwrap();
        assert!(n > 0, "{:?}", String::from_utf8_lossy(&buf));
        buf.extend_from_slice(&chunk[..n]);
    }
    let text = String::from_utf8_lossy(&buf);
    assert!(text.starts_with("+OK\r\n-WRONGPASS"), "{text:?}");
    assert!(text.ends_with("+PONG\r\n"), "{text:?}");
}
