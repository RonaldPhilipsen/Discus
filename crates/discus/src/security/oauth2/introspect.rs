//! RFC 7662 introspection of opaque tokens (spec §14.3).
//!
//! `prefetch` asks the provider and fills the cache. The synchronous `check` reads only the
//! cache. An entry lives until the token expires or for 300 s, whichever comes first. A refused
//! token is cached for 30 s, so a repeated bad token does not reach the provider each time.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use serde::Deserialize;

use super::issuer::{check_url, Source};
use super::{unix_now, Claims, OneOrMany, RawClaims, Reject};
use crate::security::file::IssuerFile;

const MAX_ENTRIES: usize = 10_000;
const MAX_CACHE_SECS: i64 = 300;
const REFUSED_CACHE_SECS: i64 = 30;
/// A longer refused token is not cached, so the refused entries stay below 10 MiB. The
/// failed-attempt limit still bounds such tokens.
const MAX_REFUSED_TOKEN_BYTES: usize = 1024;

#[derive(Deserialize)]
struct Answer {
    #[serde(default)]
    active: bool,
    exp: Option<i64>,
    nbf: Option<i64>,
    iat: Option<i64>,
    iss: Option<String>,
    aud: Option<OneOrMany>,
    sub: Option<String>,
    jti: Option<String>,
    scope: Option<String>,
    scp: Option<OneOrMany>,
}

pub struct Introspector {
    source: Arc<Source>,
    url: Option<String>,
    client_id: String,
    secret: String,
    skew: i64,
    max_cache_secs: i64,
    /// Token -> checked claims and the time the entry ends.
    cache: Mutex<HashMap<Bytes, (Claims, i64)>>,
    /// Refused token -> the reason and the time the entry ends. A separate map, so a flood of
    /// bad tokens never evicts a good answer.
    refused: Mutex<HashMap<Bytes, (Reject, i64)>>,
}

/// Insert into a bounded cache. A full cache first drops its ended entries, then everything.
fn insert<V>(cache: &Mutex<HashMap<Bytes, (V, i64)>>, token: &[u8], entry: (V, i64), now: i64) {
    let Ok(mut cache) = cache.lock() else {
        return;
    };
    if cache.len() >= MAX_ENTRIES {
        cache.retain(|_, (_, u)| *u >= now);
        if cache.len() >= MAX_ENTRIES {
            cache.clear();
        }
    }
    cache.insert(Bytes::copy_from_slice(token), entry);
}

impl Introspector {
    pub fn new(
        source: Arc<Source>,
        f: &IssuerFile,
        skew_secs: u64,
    ) -> Result<Introspector, String> {
        let client_id = f.client_id.clone().unwrap_or_default();
        let secret = match &f.client_secret_file {
            Some(path) => std::fs::read_to_string(path)
                .map_err(|e| format!("client_secret_file {}: {e}", path.display()))?
                .trim()
                .to_string(),
            None => return Err("client_id needs client_secret_file".into()),
        };
        if let Some(url) = &f.introspection_url {
            check_url(url, source.insecure())?;
        }
        Ok(Introspector {
            source,
            url: f.introspection_url.clone(),
            client_id,
            secret,
            skew: skew_secs as i64,
            max_cache_secs: MAX_CACHE_SECS,
            cache: Mutex::new(HashMap::new()),
            refused: Mutex::new(HashMap::new()),
        })
    }

    /// Use a shorter cache time, so a test does not wait 300 s for a recheck.
    #[cfg(feature = "testing")]
    pub fn set_max_cache_secs(&mut self, secs: u64) {
        self.max_cache_secs = secs as i64;
    }

    fn endpoint(&self) -> Option<String> {
        if let Some(url) = &self.url {
            return Some(url.clone());
        }
        let url = self.source.introspection_endpoint.lock().ok()?.clone()?;
        check_url(&url, self.source.insecure()).ok()?;
        Some(url)
    }

    fn cached(&self, token: &[u8], now: i64) -> Option<Claims> {
        let cache = self.cache.lock().ok()?;
        let (claims, until) = cache.get(token)?;
        (now <= *until).then(|| Claims {
            recheck: Some(*until),
            ..claims.clone()
        })
    }

    fn cached_refusal(&self, token: &[u8], now: i64) -> Option<Reject> {
        let refused = self.refused.lock().ok()?;
        let (reject, until) = refused.get(token)?;
        (now <= *until).then_some(*reject)
    }

    /// Ask the provider about `token`, unless an answer is cached.
    pub async fn prefetch(&self, token: &[u8]) {
        let now = unix_now();
        if self.cached(token, now).is_some() || self.cached_refusal(token, now).is_some() {
            return;
        }
        let Some(url) = self.endpoint() else {
            tracing::debug!("introspection endpoint unknown");
            return;
        };
        let Ok(text) = std::str::from_utf8(token) else {
            return;
        };
        let request = self
            .source
            .client()
            .post(&url)
            .basic_auth(&self.client_id, Some(&self.secret))
            .form(&[("token", text), ("token_type_hint", "access_token")]);
        let answer = match request.send().await {
            Ok(r) if r.status().is_success() => r.json::<Answer>().await,
            Ok(r) => {
                tracing::warn!(status = %r.status(), "introspection refused");
                return;
            }
            Err(e) => {
                tracing::warn!(error = %e, "introspection failed");
                return;
            }
        };
        let Ok(answer) = answer else {
            tracing::warn!("introspection answer does not parse");
            return;
        };
        let now = unix_now();
        match self.judge(answer, now) {
            Ok(claims) => {
                let until = claims.exp.min(now + self.max_cache_secs);
                insert(&self.cache, token, (claims, until), now);
            }
            Err(reject) => {
                tracing::debug!(reason = %reject, "introspection rejected the token");
                // A token that is not yet valid becomes valid without a new answer.
                if reject != Reject::NotYetValid && token.len() <= MAX_REFUSED_TOKEN_BYTES {
                    let until = now + REFUSED_CACHE_SECS;
                    insert(&self.refused, token, (reject, until), now);
                }
            }
        }
    }

    fn judge(&self, a: Answer, now: i64) -> Result<Claims, Reject> {
        let trusted = &self.source.trusted;
        if !a.active {
            return Err(Reject::Inactive);
        }
        let exp = a.exp.ok_or(Reject::Malformed)?;
        if now > exp + self.skew {
            return Err(Reject::Expired);
        }
        if a.nbf.is_some_and(|n| now + self.skew < n) {
            return Err(Reject::NotYetValid);
        }
        if a.iss.as_ref().is_some_and(|iss| *iss != trusted.name()) {
            return Err(Reject::UnknownIssuer);
        }
        let audience = &trusted.rules.audience;
        let aud_ok = match &a.aud {
            None => true,
            Some(OneOrMany::One(s)) => s == audience,
            Some(OneOrMany::Many(v)) => v.contains(audience),
        };
        if !aud_ok {
            return Err(Reject::Audience);
        }
        let raw = RawClaims {
            sub: a.sub,
            jti: a.jti,
            exp,
            nbf: a.nbf,
            iat: a.iat,
            scope: a.scope,
            scp: a.scp,
        };
        let scopes = trusted.scopes(&raw);
        if scopes.is_empty() {
            return Err(Reject::NoScope);
        }
        Ok(Claims {
            sub: raw.sub,
            jti: raw.jti,
            exp,
            nbf: raw.nbf,
            scopes,
            recheck: None,
        })
    }

    /// The cached answer for `token`.
    pub fn check(&self, token: &[u8], now: i64) -> Result<Claims, Reject> {
        self.cached(token, now)
            .ok_or_else(|| self.cached_refusal(token, now).unwrap_or(Reject::Inactive))
    }
}
