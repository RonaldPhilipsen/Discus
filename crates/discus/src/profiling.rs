//! Continuous CPU profiling: the node pushes pprof profiles to a Pyroscope server. The agent is
//! in-process because an eBPF profiler cannot run when Talos isolates the pod PID namespace.

use crate::Config;

/// Profiling flags. `Config` flattens them.
#[derive(Debug, Clone, clap::Args)]
pub struct ProfilingArgs {
    /// Push CPU profiles to this Pyroscope server, for example http://pyroscope:4040. Needs the
    /// `pyroscope` build feature.
    #[arg(long)]
    pub pyroscope_url: Option<String>,
    /// The `service_name` of the profiles in Pyroscope.
    #[arg(long, default_value = "discus")]
    pub pyroscope_service_name: String,
    /// CPU samples per second.
    #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u32).range(1..=1000))]
    pub pyroscope_sample_rate: u32,
}

impl ProfilingArgs {
    /// Check the flags. `compiled` is true when the binary has the `pyroscope` feature.
    pub fn validate(&self, compiled: bool) -> Result<(), String> {
        let Some(url) = &self.pyroscope_url else {
            return Ok(());
        };
        if !compiled {
            return Err(
                "profiling is enabled (--pyroscope-url) but this binary was built without the \
                 \"pyroscope\" feature"
                    .into(),
            );
        }
        if !url.starts_with("http://") && !url.starts_with("https://") {
            return Err(format!(
                "--pyroscope-url must start with http:// or https://, not {url:?}"
            ));
        }
        Ok(())
    }
}

/// Labels for every profile: the node ID, the build ID and the Kubernetes attributes, with
/// Pyroscope label names. Pyroscope does not accept dots in a label name, so it drops the
/// `otel.scope.version` label that the agent derives from the build ID.
fn labels(config: &Config) -> Vec<(String, String)> {
    let mut labels = vec![
        (
            "node_id".to_string(),
            crate::telemetry::node_id(config.node_id.unwrap_or_default()),
        ),
        (
            "service_version".to_string(),
            crate::build::BUILD_ID.to_string(),
        ),
    ];
    for (attr, value) in discus_telemetry::pipeline::k8s_from_env() {
        let name = match attr {
            "k8s.namespace.name" => "namespace",
            "k8s.pod.name" => "pod",
            "k8s.node.name" => "node",
            _ => continue,
        };
        labels.push((name.to_string(), value));
    }
    labels
}

/// Stops the agent and sends the last profile when dropped.
pub struct ProfilingGuard {
    #[cfg(feature = "pyroscope")]
    agent: Option<pyroscope::PyroscopeAgent<pyroscope::pyroscope::PyroscopeAgentRunning>>,
}

impl Drop for ProfilingGuard {
    fn drop(&mut self) {
        #[cfg(feature = "pyroscope")]
        if let Some(agent) = self.agent.take() {
            match agent.stop() {
                Ok(ready) => ready.shutdown(),
                Err(e) => tracing::warn!(error = %e, "could not stop the profiler"),
            }
        }
    }
}

/// Start the profiler when `--pyroscope-url` is set. Call it after the rustls default provider
/// is installed, because the agent uploads over the process-wide rustls config.
pub fn start(config: &Config) -> Result<ProfilingGuard, String> {
    let args = &config.profiling;
    args.validate(cfg!(feature = "pyroscope"))?;
    #[cfg(feature = "pyroscope")]
    {
        let Some(url) = &args.pyroscope_url else {
            return Ok(ProfilingGuard { agent: None });
        };
        // The agent builds the same client on its own thread and panics there if it fails, so the
        // node would run without profiles. Without system trust roots the build fails.
        reqwest::Client::builder().build().map_err(|e| {
            format!(
                "cannot start the profiler: {e:?} (the profiler needs system CA certificates, \
                 also for an http:// URL)"
            )
        })?;
        let agent = start_agent(url, args, &labels(config)).map_err(|e| {
            format!(
                "cannot start the profiler: {e} (it needs a writable temporary directory, \
                     $TMPDIR or /tmp)"
            )
        })?;
        tracing::info!(url = %url, sample_rate = args.pyroscope_sample_rate, "profiling");
        Ok(ProfilingGuard { agent: Some(agent) })
    }
    #[cfg(not(feature = "pyroscope"))]
    {
        let _ = labels;
        Ok(ProfilingGuard {})
    }
}

#[cfg(feature = "pyroscope")]
fn start_agent(
    url: &str,
    args: &ProfilingArgs,
    labels: &[(String, String)],
) -> pyroscope::Result<pyroscope::PyroscopeAgent<pyroscope::pyroscope::PyroscopeAgentRunning>> {
    use pyroscope::backend::{pprof_backend, BackendConfig, PprofConfig};
    use pyroscope::pyroscope::PyroscopeAgentBuilder;

    let backend = pprof_backend(
        PprofConfig {
            sample_rate: args.pyroscope_sample_rate,
        },
        BackendConfig::default(),
    );
    let tags = labels
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    PyroscopeAgentBuilder::new(
        url,
        &args.pyroscope_service_name,
        args.pyroscope_sample_rate,
        "discus",
        crate::build::BUILD_ID,
        backend,
    )
    .tags(tags)
    .build()?
    .start()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(url: Option<&str>) -> ProfilingArgs {
        ProfilingArgs {
            pyroscope_url: url.map(str::to_string),
            pyroscope_service_name: "discus".into(),
            pyroscope_sample_rate: 100,
        }
    }

    #[test]
    fn no_url_needs_no_feature() {
        assert_eq!(args(None).validate(false), Ok(()));
    }

    #[test]
    fn url_without_feature_is_an_error() {
        let err = args(Some("http://pyroscope:4040"))
            .validate(false)
            .unwrap_err();
        assert!(err.contains("\"pyroscope\" feature"), "{err}");
    }

    #[test]
    fn url_needs_an_http_scheme() {
        assert_eq!(args(Some("http://pyroscope:4040")).validate(true), Ok(()));
        assert_eq!(args(Some("https://pyroscope:4040")).validate(true), Ok(()));
        let err = args(Some("pyroscope:4040")).validate(true).unwrap_err();
        assert!(err.contains("--pyroscope-url"), "{err}");
    }

    #[test]
    fn labels_use_pyroscope_names() {
        let config = Config::parse_from(["discus"]).expect("default config");
        let labels = labels(&config);
        assert_eq!(labels[0].0, "node_id");
        assert!(labels.iter().all(|(k, _)| !k.contains('.')));
    }

    #[test]
    fn labels_carry_the_build_id() {
        let config = Config::parse_from(["discus"]).expect("default config");
        let labels = labels(&config);
        assert!(labels.contains(&(
            "service_version".to_string(),
            crate::build::BUILD_ID.to_string()
        )));
    }
}
