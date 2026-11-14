//! The reload of the `--config` settings file on a running node.

#![allow(clippy::unwrap_used)]

mod security_support;

use std::path::Path;
use std::time::Duration;

use security_support::{common, tls_connect, TestCa};
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
    let node = server.node().clone();
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
    assert_eq!(
        node.live_settings.get().hold_budget,
        Duration::from_millis(1500)
    );
}

#[tokio::test]
async fn a_file_broken_after_the_start_keeps_the_start_version() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("discus.toml");
    write_settings(&path, "version = 1\n[budget]\nhold-ms = 1500\n");
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
        "0",
    ];
    let tls = common::client_tls::server_args(&argv);
    argv.extend(tls.iter().map(String::as_str));
    let config = discus::Config::load_from(argv).unwrap();
    // The startup re-apply of the node reads this broken file and fails.
    write_settings(&path, "version = 1\n[budget]\nhold-ms = -1\n");
    let server = discus::Server::bind(config).await.unwrap();
    let addr = server.local_addr();
    let node = server.node().clone();
    let shutdown = CancellationToken::new();
    let _serve = tokio::spawn(server.run(shutdown.clone()));
    let _stop = shutdown.drop_guard();

    let client = common::client_tls::redis_client(&format!("rediss://{addr}"));
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let info = info_discus(&mut conn).await;
    assert!(info.contains("config_version:1\r\n"), "{info}");
    assert_eq!(
        node.live_settings.get().hold_budget,
        Duration::from_millis(1500)
    );
}

fn copy(from: &Path, to: &Path) {
    std::fs::write(to, std::fs::read(from).unwrap()).unwrap();
}

/// Wait until `ok` is true, for at most 60 s.
async fn eventually<F: std::future::Future<Output = bool>>(what: &str, mut ok: impl FnMut() -> F) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while !ok().await {
        assert!(tokio::time::Instant::now() < deadline, "{what} within 60 s");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn a_zero_tls_period_pauses_and_resumes() {
    let (old, new) = (TestCa::new(), TestCa::new());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("discus.toml");
    write_settings(&path, "version = 1\n[tls]\nreload-secs = 10\n");
    let (cert, key) = (dir.path().join("tls.crt"), dir.path().join("tls.key"));
    let first = old.server_cert();
    copy(&first.cert, &cert);
    copy(&first.key, &key);
    let argv = [
        "discus",
        "--listen",
        "127.0.0.1:0",
        "--admin-listen",
        "127.0.0.1:0",
        "--config",
        path.to_str().unwrap(),
        "--config-reload-secs",
        "1",
        "--tls-cert",
        cert.to_str().unwrap(),
        "--tls-key",
        key.to_str().unwrap(),
    ];
    let config = discus::Config::load_from(argv).unwrap();
    let server = discus::Server::bind(config).await.unwrap();
    let (addr, node) = (server.local_addr(), server.node().clone());
    let shutdown = CancellationToken::new();
    let _serve = tokio::spawn(server.run(shutdown.clone()));
    let _stop = shutdown.drop_guard();
    // The certificate watcher first checks about 10 s from now, with the period of the start.
    let first_check = tokio::time::Instant::now() + Duration::from_secs(10);
    assert!(tls_connect(addr, &old, None).await.is_ok());

    write_settings(&path, "version = 1\n[tls]\nreload-secs = 0\n");
    eventually("the node did not pause the certificate reload", || async {
        node.live_settings.get().tls_reload.is_zero()
    })
    .await;
    let next = new.server_cert();
    copy(&next.key, &key);
    copy(&next.cert, &cert);
    // Wait past the first check too: a node that kept the 10 s period would reload there.
    let quiet = (tokio::time::Instant::now() + Duration::from_secs(3))
        .max(first_check + Duration::from_secs(2));
    tokio::time::sleep_until(quiet).await;
    assert!(
        tls_connect(addr, &old, None).await.is_ok(),
        "a paused node reloaded its certificate"
    );
    assert!(tls_connect(addr, &new, None).await.is_err());

    write_settings(&path, "version = 1\n[tls]\nreload-secs = 1\n");
    eventually("the node did not resume the certificate reload", || async {
        tls_connect(addr, &new, None).await.is_ok()
    })
    .await;
}

#[tokio::test]
async fn info_shows_the_settings_file() {
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
        "0",
    ];
    let tls = common::client_tls::server_args(&argv);
    argv.extend(tls.iter().map(String::as_str));
    let config = discus::Config::load_from(argv).unwrap();
    let server = discus::Server::bind(config).await.unwrap();
    let addr = server.local_addr();
    let node = server.node().clone();
    let shutdown = CancellationToken::new();
    let _serve = tokio::spawn(server.run(shutdown.clone()));
    let _stop = shutdown.drop_guard();

    let client = common::client_tls::redis_client(&format!("rediss://{addr}"));
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let info = info_discus(&mut conn).await;
    assert!(
        info.contains(&format!("config_file:{path_arg}\r\n")),
        "{info}"
    );
    assert!(info.contains("config_version:1\r\n"), "{info}");
    assert!(info.contains("config_generation:0\r\n"), "{info}");
    assert!(info.contains("config_restart_pending:\r\n"), "{info}");

    let reload =
        |text: &str| discus::settings::reload::reload(&node.config, &node.live_settings, text);
    let a = "version = 1\n[budget]\nhold-ms = 3000\n";
    let b = "version = 1\n[budget]\nhold-ms = 1500\n";
    reload(b).unwrap();
    reload(a).unwrap();
    let info = info_discus(&mut conn).await;
    assert!(info.contains("config_generation:2\r\n"), "{info}");
}
