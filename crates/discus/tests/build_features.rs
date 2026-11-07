mod common;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn info_reports_the_build_features() {
    let addr = common::start_node(&[]).await;
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(b"*2\r\n$4\r\nINFO\r\n$6\r\ndiscus\r\n")
        .await
        .unwrap();
    let mut buf = vec![0u8; 4096];
    let n = s.read(&mut buf).await.unwrap();
    let info = String::from_utf8_lossy(&buf[..n]);
    let features = discus::build::FEATURES.join(",");
    assert!(info.contains(&format!("discus_build:{}\r\n", discus::build::BUILD_ID)));
    assert!(info.contains(&format!("discus_features:{features}\r\n")));
    let tls = discus::build::TLS_BACKEND;
    assert!(info.contains(&format!("discus_tls_backend:{tls}\r\n")));
    let roots = discus::build::TRUST_ROOTS;
    assert!(info.contains(&format!("discus_trust_roots:{roots}\r\n")));
}
