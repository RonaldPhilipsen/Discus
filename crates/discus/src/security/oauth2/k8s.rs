//! The Kubernetes service account issuer (spec §14.4).
//!
//! Discovery and the JWKS come from the API server with the cluster CA. The JWKS path is fixed,
//! because the `jwks_uri` of the document often points to an address outside the cluster. Service
//! account tokens have no `scope` claim, so `sub` rules give the scopes. Discus never calls
//! TokenReview, so a deleted pod keeps access until its token expires.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use super::issuer::{Source, SourceConfig};
use super::{parse_algorithms, IssuerRules, SubjectRule, TrustedIssuer};
use crate::security::file::KubernetesFile;
use crate::security::policy::Scopes;

const PREFIX: &str = "system:serviceaccount:";

/// A rule is `system:serviceaccount:<namespace>:<name>` or `system:serviceaccount:<namespace>:*`.
fn check_rule(pattern: &str) -> Result<(), String> {
    let bad = || {
        format!(
            "[[kubernetes.account]] match {pattern:?}: use \
             system:serviceaccount:<namespace>:<name> or system:serviceaccount:<namespace>:*"
        )
    };
    let rest = pattern.strip_prefix(PREFIX).ok_or_else(bad)?;
    let (ns, name) = rest.split_once(':').ok_or_else(bad)?;
    let plain = |s: &str| !s.is_empty() && !s.contains([':', '*']);
    if plain(ns) && (name == "*" || plain(name)) {
        Ok(())
    } else {
        Err(bad())
    }
}

/// The loader of the cluster issuer.
pub fn source(
    k: &KubernetesFile,
    generation: Arc<AtomicU64>,
    insecure: bool,
) -> Result<Arc<Source>, String> {
    if k.account.is_empty() {
        return Err(
            "[kubernetes] lists no [[kubernetes.account]], so every service account would be \
             refused"
                .into(),
        );
    }
    let mut rules = IssuerRules::new(k.issuer.as_deref().unwrap_or_default(), &k.audience);
    if let Some(algs) = &k.algorithms {
        rules.algorithms = parse_algorithms(algs).map_err(|e| format!("[kubernetes]: {e}"))?;
    }
    rules.require_iat = true;
    for a in &k.account {
        check_rule(&a.pattern)?;
        rules.subjects.push(SubjectRule {
            pattern: a.pattern.clone(),
            scopes: Scopes::parse_names(&a.scopes)
                .map_err(|e| format!("[[kubernetes.account]] {:?}: {e}", a.pattern))?,
        });
    }
    let mut trusted = TrustedIssuer::new(rules);
    if k.issuer.is_none() {
        trusted = trusted.learning_name();
    }
    trusted.max_stale = Some(Duration::from_secs(k.jwks_max_stale_secs));
    let api = k.api_server.trim_end_matches('/');
    let config = SourceConfig {
        discovery_url: format!("{api}/.well-known/openid-configuration"),
        fixed_jwks_url: Some(format!("{api}/openid/v1/jwks")),
        bearer_file: Some(k.token_file.clone()),
        ca_file: Some(k.ca_file.clone()),
        refresh: Duration::from_secs(k.jwks_refresh_secs.max(1)),
        min_refetch: Duration::from_secs(k.jwks_min_refetch_secs),
        insecure,
    };
    let source = Source::new(Arc::new(trusted), config, generation)
        .map_err(|e| format!("[kubernetes]: {e}"))?;
    Ok(Arc::new(source))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rules_name_one_account_or_one_namespace() {
        assert!(check_rule("system:serviceaccount:web:frontend").is_ok());
        assert!(check_rule("system:serviceaccount:web:*").is_ok());
        assert!(check_rule("system:serviceaccount:*").is_err());
        assert!(check_rule("system:serviceaccount:*:x").is_err());
        assert!(check_rule("system:serviceaccount:web:front*").is_err());
        assert!(check_rule("user:alice").is_err());
    }
}
