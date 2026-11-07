mod common;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn replies_arrive_in_order_for_a_pipeline() {
    let addr = common::start_node(&[]).await;
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut wire = Vec::new();
    for i in 0..1000 {
        wire.extend_from_slice(format!("*2\r\n$4\r\nECHO\r\n$4\r\n{i:04}\r\n").as_bytes());
    }
    s.write_all(&wire).await.unwrap();
    let mut expected = Vec::new();
    for i in 0..1000 {
        expected.extend_from_slice(format!("$4\r\n{i:04}\r\n").as_bytes());
    }
    let mut got = vec![0u8; expected.len()];
    s.read_exact(&mut got).await.unwrap();
    assert_eq!(got, expected);
}

#[tokio::test]
async fn pipeline_limit_applies_backpressure_and_keeps_order() {
    let addr = common::start_node(&["--max-pipeline", "100"]).await;
    let s = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut rd, mut wr) = s.into_split();
    let writer = tokio::spawn(async move {
        let one = b"*1\r\n$4\r\nPING\r\n";
        let mut wire = Vec::new();
        for _ in 0..20000 {
            wire.extend_from_slice(one);
        }
        wr.write_all(&wire).await.unwrap();
        wr
    });
    let mut got = vec![0u8; 20000 * 7];
    rd.read_exact(&mut got).await.unwrap();
    assert!(got.chunks(7).all(|c| c == b"+PONG\r\n"));
    writer.await.unwrap();
}

#[tokio::test]
async fn unknown_and_unsupported_commands_return_clear_errors() {
    let addr = common::start_node(&[]).await;
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(b"*1\r\n$5\r\nNOPE!\r\n*1\r\n$5\r\nMULTI\r\n")
        .await
        .unwrap();
    let mut buf = vec![0u8; 200];
    let mut text = String::new();
    while !text.contains("not supported by Discus") {
        let n = s.read(&mut buf).await.unwrap();
        assert!(n > 0, "connection closed early: {text:?}");
        text.push_str(&String::from_utf8_lossy(&buf[..n]));
    }
    assert!(text.starts_with("-ERR unknown command 'NOPE!'"));
    assert!(text.contains("-ERR command not supported by Discus\r\n"));
}

#[tokio::test]
async fn quit_after_a_pipeline_flushes_every_reply() {
    let addr = common::start_node(&[]).await;
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n*2\r\n$3\r\nGET\r\n$1\r\na\r\n*1\r\n$4\r\nQUIT\r\n")
        .await
        .unwrap();
    let mut got = Vec::new();
    s.read_to_end(&mut got).await.unwrap();
    assert_eq!(got, b"+OK\r\n$1\r\n1\r\n+OK\r\n");
}

#[tokio::test]
async fn hello_3_in_a_pipeline_switches_later_replies() {
    let addr = common::start_node(&[]).await;
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n*2\r\n$3\r\nGET\r\n$7\r\nmissing\r\n")
        .await
        .unwrap();
    let mut text = Vec::new();
    let mut buf = vec![0u8; 4096];
    while !text.ends_with(b"_\r\n") {
        let n = s.read(&mut buf).await.unwrap();
        assert!(n > 0, "closed early: {:?}", String::from_utf8_lossy(&text));
        text.extend_from_slice(&buf[..n]);
    }
    assert!(text.starts_with(b"%"));
}
