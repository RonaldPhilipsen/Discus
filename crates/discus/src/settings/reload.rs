//! Applies a changed settings file to a running node.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::config::restart_only;
use crate::server::file_watch;
use crate::settings::{parse, Live, Reloadable};
use crate::{Config, Node};

/// Merges `text` over `base` and applies the result to `live`, and returns the names of the
/// changed settings. `base` is the startup config, so its `cli_set` keeps every command-line value.
/// An error changes nothing.
pub fn reload(base: &Config, live: &Live, text: &str) -> Result<Vec<&'static str>, String> {
    let file = parse(text, &restart_only)?;
    let merged = base.with_file(&file)?;
    merged.validate()?;
    let applied = live.apply(Reloadable::from_config(&merged));
    live.set_version(file.version);
    Ok(applied)
}

/// Watches the `--config` file and reloads it after each change. Without `--config` it does
/// nothing.
pub fn spawn(node: &Arc<Node>, stop: CancellationToken) {
    let Some(path) = node.config.config.clone() else {
        return;
    };
    let base = node.config.clone();
    let live = node.live.clone();
    let files = vec![path.clone()];
    let seen = file_watch::snapshot(&files);
    // The file can change between the startup load and this snapshot, so the snapshot applies
    // now. This also sets the version of a file that never changes.
    if let Some(content) = seen.as_ref().and_then(|s| s.first()) {
        match std::str::from_utf8(content)
            .map_err(|e| e.to_string())
            .and_then(|text| reload(&base, &live, text))
        {
            Ok(applied) => log_applied(&live, &applied),
            Err(e) => {
                tracing::warn!(error = %e, "the settings file does not load, so the node keeps its current settings")
            }
        }
    }
    let period_secs = base.config_reload_secs;
    file_watch::watch(
        "settings",
        files,
        seen,
        move || Duration::from_secs(period_secs),
        stop,
        move || {
            let result = std::fs::read_to_string(&path)
                .map_err(|e| e.to_string())
                .and_then(|text| reload(&base, &live, &text));
            let metrics = discus_telemetry::global();
            metrics.config_pending(!live.pending().is_empty());
            match result {
                Ok(applied) => {
                    metrics.config_reload(true);
                    log_applied(&live, &applied);
                    Ok(())
                }
                Err(e) => {
                    metrics.config_reload(false);
                    Err(io::Error::other(e))
                }
            }
        },
    );
}

fn log_applied(live: &Live, applied: &[&'static str]) {
    if applied.is_empty() {
        return;
    }
    let now = live.get();
    let values = applied
        .iter()
        .map(|name| format!("{name}={}", value(&now, name)))
        .collect::<Vec<_>>()
        .join(", ");
    tracing::info!(settings = %values, "applied settings from the settings file");
}

/// The value of the setting `name` in the unit of its file key.
fn value(r: &Reloadable, name: &str) -> String {
    match name {
        "budget.forward-read-ms" => r.forward_read_timeout.as_millis().to_string(),
        "budget.forward-write-ms" => r.forward_write_timeout.as_millis().to_string(),
        "budget.hold-ms" => r.hold_budget.as_millis().to_string(),
        "budget.replication-ms" => r.replication_timeout.as_millis().to_string(),
        "reads.from-replicas" => r.read_from_replicas.to_string(),
        "handoff.rate-bytes" => r.handoff_rate_bytes.to_string(),
        "auth.max-failures-per-connection" => r.auth_max_failures_per_connection.to_string(),
        "auth.max-failures-per-source" => r.auth_max_failures_per_source.to_string(),
        "auth.failure-window-secs" => r.auth_failure_window.as_secs().to_string(),
        "tls.reload-secs" => r.tls_reload.as_secs().to_string(),
        _ => unreachable!("Reloadable::changed names {name}, which value does not know"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn start(args: &[&str]) -> (Config, Arc<Live>) {
        let mut argv = vec!["discus"];
        argv.extend(args);
        let base = Config::load_from(argv).unwrap();
        let live = Live::new(Reloadable::from_config(&base));
        (base, live)
    }

    #[test]
    fn a_valid_change_applies() {
        let (base, live) = start(&[]);
        let applied = reload(&base, &live, "version = 1\n[budget]\nhold-ms = 1500").unwrap();
        assert_eq!(applied, ["budget.hold-ms"]);
        assert_eq!(live.get().hold_budget, Duration::from_millis(1500));
        assert_eq!(live.version(), Some(1));
    }

    #[test]
    fn a_bad_value_changes_nothing() {
        let (base, live) = start(&[]);
        let before = live.get();
        reload(&base, &live, "version = 1\n[budget]\nhold-ms = -1").unwrap_err();
        assert_eq!(live.generation(), 0);
        assert_eq!(*live.get(), *before);
        assert_eq!(live.version(), None);
    }

    #[test]
    fn a_command_line_value_never_changes() {
        let (base, live) = start(&["--hold-budget-ms", "2000"]);
        let applied = reload(&base, &live, "version = 1\n[budget]\nhold-ms = 1500").unwrap();
        assert!(applied.is_empty());
        assert_eq!(live.get().hold_budget, Duration::from_millis(2000));
    }
    #[test]
    fn every_changed_setting_has_a_logged_value() {
        let (base, _) = start(&[]);
        let a = Reloadable::from_config(&base);
        let b = Reloadable {
            forward_read_timeout: a.forward_read_timeout + Duration::from_millis(1),
            forward_write_timeout: a.forward_write_timeout + Duration::from_millis(1),
            hold_budget: a.hold_budget + Duration::from_millis(1),
            replication_timeout: a.replication_timeout + Duration::from_millis(1),
            read_from_replicas: !a.read_from_replicas,
            handoff_rate_bytes: a.handoff_rate_bytes + 1,
            auth_max_failures_per_connection: a.auth_max_failures_per_connection + 1,
            auth_max_failures_per_source: a.auth_max_failures_per_source + 1,
            auth_failure_window: a.auth_failure_window + Duration::from_secs(1),
            tls_reload: a.tls_reload + Duration::from_secs(1),
        };
        let changed = a.changed(&b);
        assert_eq!(changed.len(), crate::settings::KEYS.len());
        for name in changed {
            assert!(!value(&b, name).is_empty(), "{name}");
        }
    }
}
