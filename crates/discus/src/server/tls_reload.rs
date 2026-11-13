//! Reloads certificates when their files change, so a rotated certificate needs no restart.
//!
//! A task reads the files every `--tls-reload-secs` and compares their content with the last
//! content it saw. Kubernetes replaces the files of a mounted Secret with one symlink swap, so a
//! read sees either every old file or every new file. A change that does not load, for example a
//! key that does not match its certificate, logs a warning, and the node keeps the certificates
//! that it has. New connections use the new certificates. Open connections keep theirs.

use std::io;
use std::path::PathBuf;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

/// The content of every file, or the first read error.
async fn read_all(files: &[PathBuf]) -> io::Result<Vec<Vec<u8>>> {
    let mut out = Vec::with_capacity(files.len());
    for f in files {
        out.push(tokio::fs::read(f).await?);
    }
    Ok(out)
}

/// Call `reload` after a change to the content of `files`. `what` names the certificates in the
/// log.
pub fn watch(
    what: &'static str,
    files: Vec<PathBuf>,
    period: Duration,
    stop: CancellationToken,
    reload: impl Fn() -> io::Result<()> + Send + 'static,
) {
    tokio::spawn(async move {
        // The node loaded these files at startup, so the first read is the baseline.
        let mut seen = read_all(&files).await.ok();
        let mut ticker = tokio::time::interval(period);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await;
        // A missing file warns once, not on every tick.
        let mut unreadable = false;
        loop {
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = ticker.tick() => {}
            }
            let now = match read_all(&files).await {
                Ok(now) => now,
                Err(e) => {
                    if !unreadable {
                        tracing::warn!(certificates = what, error = %e, "cannot read the certificate files, so the node keeps its certificates");
                    }
                    unreadable = true;
                    continue;
                }
            };
            unreadable = false;
            if seen.as_ref() == Some(&now) {
                continue;
            }
            seen = Some(now);
            match reload() {
                Ok(()) => tracing::info!(certificates = what, "reloaded the certificates"),
                Err(e) => {
                    tracing::warn!(certificates = what, error = %e, "the new certificate files do not load, so the node keeps its certificates")
                }
            }
        }
    });
}
