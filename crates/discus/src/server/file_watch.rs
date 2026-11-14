//! Reloads something from files when their content changes, so a rotated certificate or an edited
//! settings file needs no restart.
//!
//! A task reads the files every period and compares their content with the last content it saw.
//! Kubernetes replaces the files of a mounted Secret or ConfigMap with one symlink swap, so a
//! read sees either every old file or every new file. A change that does not load, for example a
//! key that does not match its certificate, logs a warning, and the node keeps what it has.

use std::io;
use std::path::PathBuf;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

/// The content of every file, or `None` when one cannot be read. The server takes it before it
/// loads the files, so a change between the load and the first check still counts as a change.
pub type Snapshot = Option<Vec<Vec<u8>>>;

pub fn snapshot(files: &[PathBuf]) -> Snapshot {
    files.iter().map(|f| std::fs::read(f).ok()).collect()
}

/// The content of every file, or the first read error.
async fn read_all(files: &[PathBuf]) -> io::Result<Vec<Vec<u8>>> {
    let mut out = Vec::with_capacity(files.len());
    for f in files {
        out.push(tokio::fs::read(f).await?);
    }
    Ok(out)
}

/// Call `reload` after a change to the content of `files`. `what` names the reloaded thing in the
/// log. `period` is read before every check, so a change of the period applies at once. A zero
/// period pauses the checks, and the task reads the period again after one second.
pub fn watch(
    what: &'static str,
    files: Vec<PathBuf>,
    mut seen: Snapshot,
    period: impl Fn() -> Duration + Send + 'static,
    stop: CancellationToken,
    reload: impl Fn() -> io::Result<()> + Send + 'static,
) {
    tokio::spawn(async move {
        // A missing file warns once, not on every check.
        let mut unreadable = false;
        loop {
            let wait = match period() {
                Duration::ZERO => PAUSED_RECHECK,
                p => p,
            };
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = tokio::time::sleep(wait) => {}
            }
            if period().is_zero() {
                continue;
            }
            let now = match read_all(&files).await {
                Ok(now) => now,
                Err(e) => {
                    if !unreadable {
                        tracing::warn!(what, error = %e, "cannot read the {what} files, so the node keeps its current {what}");
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
                Ok(()) => tracing::info!("reloaded {what}"),
                Err(e) => {
                    tracing::warn!(what, error = %e, "the new {what} files do not load, so the node keeps its current {what}")
                }
            }
        }
    });
}

/// How long a paused watcher waits before it reads the period again.
const PAUSED_RECHECK: Duration = Duration::from_secs(1);

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::Arc;

    async fn until(what: &str, cond: impl Fn() -> bool) {
        let limit = tokio::time::Instant::now() + Duration::from_secs(10);
        while !cond() {
            assert!(tokio::time::Instant::now() < limit, "timed out: {what}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn counter() -> (
        Arc<AtomicUsize>,
        impl Fn() -> io::Result<()> + Send + 'static,
    ) {
        let n = Arc::new(AtomicUsize::new(0));
        let n2 = n.clone();
        (n, move || {
            n2.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }

    #[tokio::test]
    async fn a_symlink_swap_reloads_once() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        for (v, content) in [("v1", "one"), ("v2", "two")] {
            std::fs::create_dir(root.join(v)).unwrap();
            std::fs::write(root.join(v).join("file"), content).unwrap();
        }
        std::os::unix::fs::symlink("v1", root.join("..data")).unwrap();
        std::os::unix::fs::symlink("..data/file", root.join("file")).unwrap();
        let files = vec![root.join("file")];
        let seen = snapshot(&files);
        let (count, reload) = counter();
        let stop = CancellationToken::new();
        watch(
            "test files",
            files,
            seen,
            || Duration::from_millis(10),
            stop.clone(),
            reload,
        );
        std::os::unix::fs::symlink("v2", root.join("..data.tmp")).unwrap();
        std::fs::rename(root.join("..data.tmp"), root.join("..data")).unwrap();
        until("the swap reloads", || count.load(Ordering::SeqCst) == 1).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
        stop.cancel();
    }

    #[tokio::test]
    async fn a_missing_file_keeps_going() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        std::fs::write(&path, "one").unwrap();
        let files = vec![path.clone()];
        let seen = snapshot(&files);
        let (count, reload) = counter();
        let stop = CancellationToken::new();
        watch(
            "test files",
            files,
            seen,
            || Duration::from_millis(10),
            stop.clone(),
            reload,
        );
        std::fs::remove_file(&path).unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(count.load(Ordering::SeqCst), 0);
        std::fs::write(&path, "two").unwrap();
        until("the new file reloads", || count.load(Ordering::SeqCst) == 1).await;
        stop.cancel();
    }

    #[tokio::test]
    async fn a_zero_period_pauses_the_checks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        std::fs::write(&path, "one").unwrap();
        let files = vec![path.clone()];
        let seen = snapshot(&files);
        let (count, reload) = counter();
        let millis = Arc::new(AtomicU64::new(0));
        let m = millis.clone();
        let stop = CancellationToken::new();
        watch(
            "test files",
            files,
            seen,
            move || Duration::from_millis(m.load(Ordering::SeqCst)),
            stop.clone(),
            reload,
        );
        std::fs::write(&path, "two").unwrap();
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert_eq!(count.load(Ordering::SeqCst), 0);
        millis.store(10, Ordering::SeqCst);
        until("the resumed checks reload", || {
            count.load(Ordering::SeqCst) == 1
        })
        .await;
        stop.cancel();
    }
}
