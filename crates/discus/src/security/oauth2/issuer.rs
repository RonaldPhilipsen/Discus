//! OIDC discovery and JWKS loading for one trusted issuer.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;

use super::{KeySet, TrustedIssuer};
use crate::security::tls;

/// The timeout of one HTTP request to an identity provider.
const FETCH_TIMEOUT: Duration = Duration::from_secs(5);
const BACKOFF_START: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Where and how the keys of one issuer load.
pub struct Source {
    pub trusted: Arc<TrustedIssuer>,
    discovery_url: String,
    /// A JWKS address that skips the `jwks_uri` of discovery (the Kubernetes issuer).
    fixed_jwks_url: Option<String>,
    /// A file with a bearer token that every request sends, when the file exists.
    bearer_file: Option<PathBuf>,
    client: reqwest::Client,
    refresh: Duration,
    min_refetch: Duration,
    insecure: bool,
    jwks_uri: Mutex<Option<String>>,
    /// The last refresh that an unknown `kid` caused.
    last_kid_refresh: tokio::sync::Mutex<Option<Instant>>,
    generation: Arc<AtomicU64>,
    /// The `issuer` from discovery, for an issuer that learns its name from the document.
    pub discovered_issuer: Mutex<Option<String>>,
    /// The `introspection_endpoint` from discovery.
    pub introspection_endpoint: Mutex<Option<String>>,
}

/// The settings of a `Source`.
pub struct SourceConfig {
    pub discovery_url: String,
    pub fixed_jwks_url: Option<String>,
    pub bearer_file: Option<PathBuf>,
    pub ca_file: Option<PathBuf>,
    pub refresh: Duration,
    pub min_refetch: Duration,
    pub insecure: bool,
}

/// An `https` URL, or an `http` URL under the development flag.
pub fn check_url(url: &str, insecure: bool) -> Result<(), String> {
    if url.starts_with("https://") || (insecure && url.starts_with("http://")) {
        Ok(())
    } else if url.starts_with("http://") {
        Err(format!(
            "{url}: an http identity provider needs --dev-insecure-oauth2"
        ))
    } else {
        Err(format!("{url}: use an https URL"))
    }
}

impl Source {
    pub fn new(
        trusted: Arc<TrustedIssuer>,
        config: SourceConfig,
        generation: Arc<AtomicU64>,
    ) -> Result<Source, String> {
        check_url(&config.discovery_url, config.insecure)?;
        if let Some(url) = &config.fixed_jwks_url {
            check_url(url, config.insecure)?;
        }
        let tls = tls::client_config(config.ca_file.as_deref()).map_err(|e| e.to_string())?;
        let client = reqwest::Client::builder()
            .use_preconfigured_tls(tls)
            .timeout(FETCH_TIMEOUT)
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Source {
            trusted,
            discovery_url: config.discovery_url,
            fixed_jwks_url: config.fixed_jwks_url,
            bearer_file: config.bearer_file,
            client,
            refresh: config.refresh,
            min_refetch: config.min_refetch,
            insecure: config.insecure,
            jwks_uri: Mutex::new(None),
            last_kid_refresh: tokio::sync::Mutex::new(None),
            generation,
            discovered_issuer: Mutex::new(None),
            introspection_endpoint: Mutex::new(None),
        })
    }

    /// The HTTP client of this issuer, with its trust roots.
    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }

    pub fn insecure(&self) -> bool {
        self.insecure
    }

    async fn get(&self, url: &str) -> Result<bytes::Bytes, String> {
        let mut req = self.client.get(url);
        if let Some(path) = &self.bearer_file {
            if let Ok(token) = std::fs::read_to_string(path) {
                req = req.bearer_auth(token.trim());
            }
        }
        let resp = req.send().await.map_err(|e| format!("{url}: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("{url}: HTTP {}", resp.status()));
        }
        resp.bytes().await.map_err(|e| format!("{url}: {e}"))
    }

    /// Fetch the discovery document and return the `jwks_uri`.
    async fn discover(&self) -> Result<String, String> {
        let body = self.get(&self.discovery_url).await?;
        let doc: serde_json::Value =
            serde_json::from_slice(&body).map_err(|e| format!("discovery: {e}"))?;
        let issuer = doc
            .get("issuer")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if self.trusted.learns_name() {
            if let Ok(mut d) = self.discovered_issuer.lock() {
                *d = Some(issuer.to_string());
            }
            self.trusted.set_name(issuer);
        } else if issuer != self.trusted.rules.issuer {
            return Err(format!(
                "discovery names issuer {issuer:?}, not {:?}",
                self.trusted.rules.issuer
            ));
        }
        if let Some(url) = doc.get("introspection_endpoint").and_then(|v| v.as_str()) {
            if let Ok(mut e) = self.introspection_endpoint.lock() {
                *e = Some(url.to_string());
            }
        }
        if let Some(fixed) = &self.fixed_jwks_url {
            return Ok(fixed.clone());
        }
        let uri = doc
            .get("jwks_uri")
            .and_then(|v| v.as_str())
            .ok_or("discovery has no jwks_uri")?
            .to_string();
        check_url(&uri, self.insecure)?;
        Ok(uri)
    }

    /// Load the keys once: discovery when the `jwks_uri` is unknown, then the JWKS.
    pub async fn load(&self) -> Result<(), String> {
        let cached = self.jwks_uri.lock().ok().and_then(|u| u.clone());
        let uri = match cached {
            Some(u) => u,
            None => self.discover().await?,
        };
        let result = async {
            let keys = KeySet::parse(&self.get(&uri).await?);
            if keys.is_empty() {
                return Err(format!("{uri}: no usable signing key"));
            }
            Ok(keys)
        }
        .await;
        match result {
            Ok(keys) => {
                if let Ok(mut u) = self.jwks_uri.lock() {
                    *u = Some(uri);
                }
                self.trusted.set_keys(keys);
                self.generation.fetch_add(1, Ordering::AcqRel);
                Ok(())
            }
            Err(e) => {
                // Run discovery again next time, in case the provider moved its keys.
                if let Ok(mut u) = self.jwks_uri.lock() {
                    *u = None;
                }
                Err(e)
            }
        }
    }

    /// Load at start, then refresh periodically. A failure retries with backoff.
    pub async fn run(self: Arc<Self>, stop: CancellationToken) {
        let mut backoff = BACKOFF_START;
        loop {
            let wait = match self.load().await {
                Ok(()) => {
                    backoff = BACKOFF_START;
                    self.refresh
                }
                Err(e) => {
                    tracing::warn!(issuer = %self.trusted.name(), error = %e, "JWKS load failed");
                    let wait = backoff;
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                    wait
                }
            };
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = tokio::time::sleep(wait) => {}
            }
        }
    }

    /// Refresh because a token names an unknown `kid`, at most once per `min_refetch`.
    pub async fn refresh_for_unknown_kid(&self) {
        let mut last = self.last_kid_refresh.lock().await;
        if last.is_some_and(|t| t.elapsed() < self.min_refetch) {
            return;
        }
        *last = Some(Instant::now());
        if let Err(e) = self.load().await {
            tracing::warn!(issuer = %self.trusted.name(), error = %e, "JWKS refresh failed");
        }
    }
}
