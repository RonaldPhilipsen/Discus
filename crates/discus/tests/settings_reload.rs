//! The reload of the `--config` settings file on a running node.

#![allow(clippy::unwrap_used)]

mod common;

use std::time::Duration;

use tokio_util::sync::CancellationToken;

/// A rename replaces the file in one step, so the node never reads a half-written file.
fn write_settings(path: &std::path::Path, text: &str) {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, text).unwrap();
    std::fs::rename(&tmp, path).unwrap();
}

async fn info_discus(conn: &mut redis::aio::MultiplexedConnection) -> String {
    redis::cmd("INFO")
        .arg("discus")
        .query_async(conn)
        .await
        .unwrap()
}

// Task 7 adds `config_generation` to INFO and removes the ignore.
#[ignore]
#[tokio::test]
async fn a_running_node_applies_a_changed_hold_budget() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("discus.toml");
    write_settings(&path, "version = 1\n[budget]\nhold-ms = 3000\n");
    let path_arg = path.to_str().unwrap().to_string();
    let mut argv = vec![
        "discus",
        "--listen",
        "127.0.0.1:0",
        "--admin-listen",
        "127.0.0.1:0",
        "--config",
        path_arg.as_str(),
        "--config-reload-secs",
        "1",
    ];
    let tls = common::client_tls::server_args(&argv);
    argv.extend(tls.iter().map(String::as_str));
    let config = discus::Config::load_from(argv).unwrap();
    let server = discus::Server::bind(config).await.unwrap();
    let addr = server.local_addr();
    let shutdown = CancellationToken::new();
    let _serve = tokio::spawn(server.run(shutdown.clone()));
    let _stop = shutdown.drop_guard();

    let client = common::client_tls::redis_client(&format!("rediss://{addr}"));
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let info = info_discus(&mut conn).await;
    assert!(info.contains("config_generation:0"), "{info}");

    write_settings(&path, "version = 1\n[budget]\nhold-ms = 1500\n");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let info = info_discus(&mut conn).await;
        if info.contains("config_generation:1") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the reload did not apply within 30 s: {info}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
