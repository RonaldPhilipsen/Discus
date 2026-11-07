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
    let addr = common::start_node(&[]).await;
    let output = tokio::process::Command::new("bash")
        .arg("scripts/compat.sh")
        .arg(addr.to_string())
        .env("COMPAT_ALLOW_FLUSH", "1")
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
}
