//! The limits of a connection that has not met the authentication policy: a small request, a
//! deadline to authenticate, and the limit on open connections.
#![allow(clippy::unwrap_used)]
mod common;

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

const AUTH: &[u8] = b"*2\r\n$4\r\nAUTH\r\n$6\r\ns3cret\r\n";
const PING: &[u8] = b"*1\r\n$4\r\nPING\r\n";

/// How long a test waits for something that the node does at once on a fast machine.
const PATIENCE: Duration = Duration::from_secs(60);

/// `SET key <value of len bytes>`.
fn set(key: &str, len: usize) -> Vec<u8> {
    let mut wire = format!("*3\r\n$3\r\nSET\r\n${}\r\n{key}\r\n${len}\r\n", key.len()).into_bytes();
    wire.resize(wire.len() + len, b'v');
    wire.extend_from_slice(b"\r\n");
    wire
}

/// Send `wire` and read exactly `want`.
async fn expect<S: AsyncRead + AsyncWrite + Unpin>(s: &mut S, wire: &[u8], want: &str) {
    s.write_all(wire).await.unwrap();
    s.flush().await.unwrap();
    let mut got = vec![0u8; want.len()];
    tokio::time::timeout(PATIENCE, s.read_exact(&mut got))
        .await
        .expect("a reply in time")
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&got), want);
}

/// Read until the node closes the connection.
async fn read_to_close<S: AsyncRead + Unpin>(s: &mut S) -> std::io::Result<String> {
    let mut all = Vec::new();
    tokio::time::timeout(PATIENCE, s.read_to_end(&mut all))
        .await
        .expect("the node closes the connection")?;
    Ok(String::from_utf8_lossy(&all).into_owned())
}

/// Connect until the node serves a connection.
async fn connect_served(
    addr: SocketAddr,
) -> tokio_rustls::client::TlsStream<tokio::net::TcpStream> {
    let deadline = tokio::time::Instant::now() + PATIENCE;
    loop {
        if let Ok(mut s) = common::client_tls::try_connect(addr).await {
            if s.write_all(PING).await.is_ok() {
                let mut got = [0u8; 7];
                if s.read_exact(&mut got).await.is_ok() {
                    assert_eq!(&got, b"+PONG\r\n");
                    return s;
                }
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the node admits a connection again"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn a_large_bulk_before_auth_closes_the_connection() {
    let addr =
        common::start_node(&["--acl-file", common::client_tls::password_file("s3cret")]).await;
    let mut s = common::client_tls::connect(addr).await;
    // Only the header: the node refuses the length before it buffers the value.
    s.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$100000\r\n")
        .await
        .unwrap();
    let text = read_to_close(&mut s).await.unwrap();
    assert!(
        text.starts_with("-ERR Protocol error: bulk length 100000 exceeds the limit of 65536"),
        "{text:?}"
    );
}

#[tokio::test]
async fn a_long_command_before_auth_closes_the_connection() {
    let addr =
        common::start_node(&["--acl-file", common::client_tls::password_file("s3cret")]).await;
    let mut s = common::client_tls::connect(addr).await;
    s.write_all(b"*1000\r\n").await.unwrap();
    let text = read_to_close(&mut s).await.unwrap();
    assert!(
        text.starts_with("-ERR Protocol error: array length 1000 exceeds the limit of 64"),
        "{text:?}"
    );
}

#[tokio::test]
async fn a_large_request_of_small_arguments_before_auth_closes_the_connection() {
    let addr =
        common::start_node(&["--acl-file", common::client_tls::password_file("s3cret")]).await;
    let mut s = common::client_tls::connect(addr).await;
    let mut wire = b"*3\r\n$4\r\nMSET\r\n".to_vec();
    for _ in 0..2 {
        wire.extend_from_slice(b"$40000\r\n");
        wire.resize(wire.len() + 40000, b'v');
        wire.extend_from_slice(b"\r\n");
    }
    // Leave the request unfinished, so the node has to buffer it.
    wire.truncate(wire.len() - 1000);
    // The node closes while request bytes are still on the way, so the write or the read can
    // fail with a reset. The close is what matters.
    let _ = s.write_all(&wire).await;
    if let Ok(text) = read_to_close(&mut s).await {
        assert!(
            text.is_empty() || text.starts_with("-ERR Protocol error: request too large"),
            "{text:?}"
        );
    }
}

#[tokio::test]
async fn the_same_request_after_auth_works() {
    let addr =
        common::start_node(&["--acl-file", common::client_tls::password_file("s3cret")]).await;
    let mut s = common::client_tls::connect(addr).await;
    expect(&mut s, AUTH, "+OK\r\n").await;
    expect(&mut s, &set("k", 100_000), "+OK\r\n").await;
    expect(&mut s, b"*2\r\n$6\r\nSTRLEN\r\n$1\r\nk\r\n", ":100000\r\n").await;
}

#[tokio::test]
async fn auth_and_a_large_set_in_one_pipeline_work() {
    let addr =
        common::start_node(&["--acl-file", common::client_tls::password_file("s3cret")]).await;
    let mut s = common::client_tls::connect(addr).await;
    let mut wire = AUTH.to_vec();
    wire.extend_from_slice(&set("big", 1 << 20));
    wire.extend_from_slice(b"*2\r\n$6\r\nSTRLEN\r\n$3\r\nbig\r\n");
    expect(&mut s, &wire, "+OK\r\n+OK\r\n:1048576\r\n").await;
}

#[tokio::test]
async fn a_failed_auth_keeps_the_small_limits() {
    let addr =
        common::start_node(&["--acl-file", common::client_tls::password_file("s3cret")]).await;
    let mut s = common::client_tls::connect(addr).await;
    s.write_all(b"*2\r\n$4\r\nAUTH\r\n$5\r\nwrong\r\n*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$100000\r\n")
        .await
        .unwrap();
    let text = read_to_close(&mut s).await.unwrap();
    assert!(text.starts_with("-WRONGPASS"), "{text:?}");
    assert!(
        text.contains("-ERR Protocol error: bulk length"),
        "{text:?}"
    );
}

#[tokio::test]
async fn policy_none_keeps_the_normal_limits_and_has_no_deadline() {
    let addr = common::start_node(&["--auth-timeout-secs", "1"]).await;
    let mut s = common::client_tls::connect(addr).await;
    expect(&mut s, &set("k", 100_000), "+OK\r\n").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    expect(&mut s, PING, "+PONG\r\n").await;
}

#[tokio::test]
async fn the_auth_deadline_closes_an_idle_unauthenticated_connection() {
    let addr = common::start_node(&[
        "--acl-file",
        common::client_tls::password_file("s3cret"),
        "--auth-timeout-secs",
        "2",
    ])
    .await;
    let mut s = common::client_tls::connect(addr).await;
    assert_eq!(read_to_close(&mut s).await.unwrap(), "");
}

#[tokio::test]
async fn the_auth_deadline_closes_a_connection_that_sends_no_credential() {
    let addr = common::start_node(&[
        "--acl-file",
        common::client_tls::password_file("s3cret"),
        "--auth-timeout-secs",
        "2",
    ])
    .await;
    let mut s = common::client_tls::connect(addr).await;
    s.write_all(PING).await.unwrap();
    let text = read_to_close(&mut s).await.unwrap();
    assert!(text.starts_with("-NOAUTH"), "{text:?}");
}

#[tokio::test]
async fn an_authenticated_connection_outlives_the_auth_deadline() {
    let addr = common::start_node(&[
        "--acl-file",
        common::client_tls::password_file("s3cret"),
        "--auth-timeout-secs",
        "5",
    ])
    .await;
    let mut s = common::client_tls::connect(addr).await;
    expect(&mut s, AUTH, "+OK\r\n").await;
    tokio::time::sleep(Duration::from_secs(7)).await;
    expect(&mut s, PING, "+PONG\r\n").await;
}

#[tokio::test]
async fn auth_timeout_zero_turns_the_deadline_off() {
    let addr = common::start_node(&[
        "--acl-file",
        common::client_tls::password_file("s3cret"),
        "--auth-timeout-secs",
        "0",
    ])
    .await;
    let mut s = common::client_tls::connect(addr).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    expect(&mut s, AUTH, "+OK\r\n").await;
}

#[tokio::test]
async fn max_clients_refuses_the_next_connection_and_admits_one_after_a_close() {
    let addr = common::start_node(&["--max-clients", "2"]).await;
    let first = connect_served(addr).await;
    let mut second = connect_served(addr).await;
    // The node accepts the TCP connection and closes it, so the TLS handshake fails.
    for _ in 0..3 {
        assert!(common::client_tls::try_connect(addr).await.is_err());
    }
    expect(&mut second, PING, "+PONG\r\n").await;
    drop(first);
    let mut third = connect_served(addr).await;
    expect(&mut third, PING, "+PONG\r\n").await;
    expect(&mut second, PING, "+PONG\r\n").await;
}

#[tokio::test]
async fn a_connection_without_a_handshake_counts_against_max_clients() {
    let addr = common::start_node(&["--max-clients", "1"]).await;
    let silent = tokio::net::TcpStream::connect(addr).await.unwrap();
    assert!(common::client_tls::try_connect(addr).await.is_err());
    drop(silent);
    connect_served(addr).await;
}

#[tokio::test]
async fn max_clients_zero_means_no_limit() {
    let addr = common::start_node(&["--max-clients", "0"]).await;
    let mut open = Vec::new();
    for _ in 0..8 {
        open.push(connect_served(addr).await);
    }
}
