//! Regression tests for the findings of the final whole-branch review.

mod common;

use redis::AsyncCommands;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// C1: one unfinished command must not grow a connection's buffer without bound.
#[tokio::test]
async fn an_unfinished_request_over_the_byte_cap_is_rejected() {
    let addr = common::start_node(&["--max-request-bytes", "100000"]).await;
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(b"*10\r\n").await.unwrap();
    let chunk = vec![b'x'; 50_000];
    let mut closed = false;
    for _ in 0..4 {
        let sent = async {
            s.write_all(b"$50000\r\n").await?;
            s.write_all(&chunk).await?;
            s.write_all(b"\r\n").await
        };
        if sent.await.is_err() {
            closed = true;
            break;
        }
    }
    let mut text = String::new();
    let mut buf = [0u8; 256];
    loop {
        match tokio::time::timeout(Duration::from_secs(2), s.read(&mut buf)).await {
            Ok(Ok(n)) if n > 0 => text.push_str(&String::from_utf8_lossy(&buf[..n])),
            _ => break,
        }
    }
    assert!(
        closed || text.contains("Protocol error"),
        "the node kept buffering: {text:?}"
    );
}

/// C2: counters create keys, so they must respect maxmemory.
#[tokio::test]
async fn counters_respect_maxmemory() {
    let mut c = common::connect(&["--max-memory", "50000"]).await;
    for i in 0..3000 {
        let _: i64 = c.incr(format!("ctr:{i}"), 1).await.unwrap();
    }
    for i in 0..1000 {
        let _: String = redis::cmd("INCRBYFLOAT")
            .arg(format!("f:{i}"))
            .arg("0.5")
            .query_async(&mut c)
            .await
            .unwrap();
    }
    let used: String = redis::cmd("INFO")
        .arg("memory")
        .query_async(&mut c)
        .await
        .unwrap();
    let used: u64 = used
        .lines()
        .find_map(|l| l.strip_prefix("used_memory:"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(used <= 50_000 + 2_000, "used_memory = {used}");
    let n: i64 = redis::cmd("DBSIZE").query_async(&mut c).await.unwrap();
    assert!(n < 4000, "nothing was evicted: {n}");
}

/// I1: the drain must finish even when a client never reads its replies.
#[tokio::test]
async fn drain_finishes_when_a_client_never_reads() {
    let (addr, handle, shutdown) = common::start_node_raw(&["--shutdown-timeout-secs", "1"]).await;
    let client = redis::Client::open(format!("redis://{addr}")).unwrap();
    let mut c = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = c.set("big", "x".repeat(1_000_000)).await.unwrap();

    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut wire = Vec::new();
    for _ in 0..300 {
        wire.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$3\r\nbig\r\n");
    }
    s.write_all(&wire).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    shutdown.cancel();
    let done = tokio::time::timeout(Duration::from_secs(5), handle).await;
    assert!(
        done.is_ok(),
        "the drain hung on a connection blocked in a write"
    );
    drop(s);
}

/// I2: a write that hits the member limit must change nothing.
#[tokio::test]
async fn collection_limit_is_atomic() {
    let mut c = common::connect(&["--max-collection-members", "2"]).await;
    let err = redis::cmd("HSET")
        .arg("h")
        .arg("a")
        .arg(1)
        .arg("b")
        .arg(2)
        .arg("c")
        .arg(3)
        .query_async::<i64>(&mut c)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("collection too large"), "{err}");
    let n: i64 = c.hlen("h").await.unwrap();
    assert_eq!(n, 0);
    let err = c
        .sadd::<_, _, i64>("s", &["a", "b", "c"])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("collection too large"), "{err}");
    let n: i64 = c.scard("s").await.unwrap();
    assert_eq!(n, 0);
    let err = redis::cmd("ZADD")
        .arg("z")
        .arg(1)
        .arg("a")
        .arg(2)
        .arg("b")
        .arg(3)
        .arg("c")
        .query_async::<i64>(&mut c)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("collection too large"), "{err}");
    let n: i64 = c.zcard("z").await.unwrap();
    assert_eq!(n, 0);
    let ok: i64 = c.sadd("s", &["a", "b"]).await.unwrap();
    assert_eq!(ok, 2);
}

/// I3: a negative SRANDMEMBER count must not allocate in proportion to a client number.
#[tokio::test]
async fn srandmember_negative_count_is_capped() {
    let mut c = common::connect(&["--max-collection-members", "10"]).await;
    let _: i64 = c.sadd("s", "a").await.unwrap();
    let ok: Vec<String> = redis::cmd("SRANDMEMBER")
        .arg("s")
        .arg(-10)
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(ok.len(), 10);
    let err = redis::cmd("SRANDMEMBER")
        .arg("s")
        .arg(i64::MIN)
        .query_async::<Vec<String>>(&mut c)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("count"), "{err}");
}
