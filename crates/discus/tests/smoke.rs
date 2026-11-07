#[tokio::test]
async fn server_binds_and_accepts_a_connection() {
    let config = discus::Config::parse_from([
        "discus",
        "--listen",
        "127.0.0.1:0",
        "--admin-listen",
        "127.0.0.1:0",
    ])
    .unwrap();
    let server = discus::Server::bind(config).await.unwrap();
    let addr = server.local_addr();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let handle = tokio::spawn(server.run(shutdown.clone()));
    let stream = tokio::net::TcpStream::connect(addr).await;
    assert!(stream.is_ok());
    shutdown.cancel();
    handle.await.unwrap().unwrap();
}
