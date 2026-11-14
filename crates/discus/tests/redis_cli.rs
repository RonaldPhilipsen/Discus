mod common;

/// Run the compatibility script against a fresh node. Needs `redis-cli` on the PATH, which
/// `mise run test` provides.
#[tokio::test]
async fn compat_script_passes_against_a_fresh_node() {
    if std::process::Command::new("redis-cli")
        .arg("--version")
        .output()
        .is_err()
    {
        eprintln!("redis-cli not installed, skipping");
        return;
    }
    // The script checks the topology commands and `redis-cli -c`, so the node needs a redirect
    // listener, and its database checks need 3 databases. A start that loses the reserved port to
    // another process tries a fresh one.
    let (node, redirect) = loop {
        let redirect = common::cluster::free_port().to_string();
        let flags = ["--redirect-listen", &redirect, "--databases", "3"];
        match common::try_start_full(&flags).await {
            Ok(node) => break (node, redirect),
            Err(e) if e.contains("Address already in use") => continue,
            Err(e) => panic!("{e}"),
        }
    };
    let output = tokio::process::Command::new("bash")
        .arg("scripts/compat.sh")
        .arg(node.addr.to_string())
        .env("COMPAT_ALLOW_FLUSH", "1")
        .env("COMPAT_CA", common::client_tls::ca_file())
        .env("COMPAT_REDIRECT", &redirect)
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
        .output()
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "compat script failed:\n{text}");
    assert!(
        text.contains("ok   SET"),
        "script did not run its checks:\n{text}"
    );
    assert!(
        text.contains("ok   redis-cli -c SET and GET"),
        "script did not run the cluster client check:\n{text}"
    );
}
