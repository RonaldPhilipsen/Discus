//! OAuth 2.0 resource server: JWT validation against trusted issuers (spec §14.3).
//!
//! `jsonwebtoken` verifies the signature and the registered claims. This module picks the
//! issuer from `iss`, picks the key, checks `iat`, and maps scopes.

pub mod jwks;

use std::collections::HashSet;
use std::fmt;
use std::sync::{Arc, RwLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::{Algorithm, Validation};
use serde::Deserialize;

pub use jwks::KeySet;

use super::identity::pattern_matches;
use super::policy::Scopes;

/// The largest token Discus reads.
const MAX_TOKEN_BYTES: usize = 16 * 1024;

/// Seconds since the Unix epoch.
pub fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

/// Parse an algorithm allowlist. Symmetric algorithms and `none` are refused, because a
/// resource server never holds an issuer secret.
pub fn parse_algorithms(names: &[String]) -> Result<Vec<Algorithm>, String> {
    let mut out = Vec::new();
    for name in names {
        let alg: Algorithm = name
            .parse()
            .map_err(|_| format!("unknown JWT algorithm {name:?}"))?;
        if alg.family() == jsonwebtoken::AlgorithmFamily::Hmac {
            return Err(format!("JWT algorithm {name} is symmetric and not allowed"));
        }
        out.push(alg);
    }
    if out.is_empty() {
        return Err("the JWT algorithm list is empty".into());
    }
    Ok(out)
}

/// Why a token was refused. Only the debug log shows it. The client sees a generic error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reject {
    Malformed,
    Algorithm,
    UnknownIssuer,
    UnknownKid,
    Signature,
    Audience,
    Expired,
    NotYetValid,
    IssuedInFuture,
    NoScope,
    /// The issuer has no keys, or its keys are too old.
    Stale,
    /// Introspection said the token is not active, or did not answer.
    Inactive,
}

impl fmt::Display for Reject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Reject::Malformed => "malformed token or missing claim",
            Reject::Algorithm => "algorithm not allowed",
            Reject::UnknownIssuer => "issuer not trusted",
            Reject::UnknownKid => "no key for the token",
            Reject::Signature => "bad signature",
            Reject::Audience => "wrong audience",
            Reject::Expired => "token expired",
            Reject::NotYetValid => "token not yet valid",
            Reject::IssuedInFuture => "token issued in the future",
            Reject::NoScope => "no Discus scope",
            Reject::Stale => "issuer keys unavailable or stale",
            Reject::Inactive => "token not active",
        })
    }
}

/// A `sub` pattern and the scopes it grants.
#[derive(Debug, Clone)]
pub struct SubjectRule {
    pub pattern: String,
    pub scopes: Scopes,
}

/// What a token from one issuer must satisfy.
#[derive(Debug, Clone)]
pub struct IssuerRules {
    pub issuer: String,
    pub audience: String,
    pub algorithms: Vec<Algorithm>,
    /// Removed from each scope name before it maps to a Discus scope.
    pub scope_prefix: String,
    /// When set, the first rule that matches `sub` gives the scopes, and the `scope` claim is
    /// ignored. An unmatched subject is refused.
    pub subjects: Vec<SubjectRule>,
    pub require_iat: bool,
}

impl IssuerRules {
    pub fn new(issuer: &str, audience: &str) -> IssuerRules {
        IssuerRules {
            issuer: issuer.into(),
            audience: audience.into(),
            algorithms: vec![Algorithm::RS256, Algorithm::ES256],
            scope_prefix: String::new(),
            subjects: Vec::new(),
            require_iat: false,
        }
    }
}

/// The checked claims of a token.
#[derive(Debug, Clone)]
pub struct Claims {
    pub sub: Option<String>,
    pub jti: Option<String>,
    pub exp: i64,
    pub nbf: Option<i64>,
    pub scopes: Scopes,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum OneOrMany {
    One(String),
    Many(Vec<String>),
}

#[derive(Deserialize)]
struct RawClaims {
    sub: Option<String>,
    jti: Option<String>,
    exp: i64,
    nbf: Option<i64>,
    iat: Option<i64>,
    scope: Option<String>,
    scp: Option<OneOrMany>,
}

#[derive(Deserialize)]
struct IssOnly {
    iss: Option<String>,
}

/// One trusted issuer and its current keys.
pub struct TrustedIssuer {
    pub rules: IssuerRules,
    keys: RwLock<Option<(Arc<KeySet>, Instant)>>,
    /// Keys older than this are stale. `None` never goes stale.
    pub max_stale: Option<std::time::Duration>,
}

impl TrustedIssuer {
    pub fn new(rules: IssuerRules) -> TrustedIssuer {
        TrustedIssuer {
            rules,
            keys: RwLock::new(None),
            max_stale: None,
        }
    }

    /// Replace the keys and restart the staleness clock.
    pub fn set_keys(&self, keys: KeySet) {
        if let Ok(mut k) = self.keys.write() {
            *k = Some((Arc::new(keys), Instant::now()));
        }
    }

    /// True once keys have loaded.
    pub fn loaded(&self) -> bool {
        self.keys.read().is_ok_and(|k| k.is_some())
    }

    /// When the keys last loaded.
    pub fn loaded_at(&self) -> Option<Instant> {
        self.keys
            .read()
            .ok()
            .and_then(|k| k.as_ref().map(|(_, at)| *at))
    }

    /// The current keys, unless missing or stale.
    pub fn keys(&self) -> Result<Arc<KeySet>, Reject> {
        let guard = self.keys.read().map_err(|_| Reject::Stale)?;
        let (keys, at) = guard.as_ref().ok_or(Reject::Stale)?;
        if self.max_stale.is_some_and(|max| at.elapsed() > max) {
            return Err(Reject::Stale);
        }
        Ok(keys.clone())
    }

    fn scopes(&self, claims: &RawClaims) -> Scopes {
        if !self.rules.subjects.is_empty() {
            let sub = claims.sub.as_deref().unwrap_or_default();
            return self
                .rules
                .subjects
                .iter()
                .find(|r| pattern_matches(&r.pattern, sub))
                .map_or(Scopes::NONE, |r| r.scopes);
        }
        let mut names: Vec<&str> = claims
            .scope
            .as_deref()
            .unwrap_or_default()
            .split_whitespace()
            .collect();
        match &claims.scp {
            Some(OneOrMany::One(s)) => names.extend(s.split_whitespace()),
            Some(OneOrMany::Many(v)) => names.extend(v.iter().map(String::as_str)),
            None => {}
        }
        let prefix = self.rules.scope_prefix.as_str();
        Scopes::from_names(names.into_iter().filter_map(|n| n.strip_prefix(prefix)))
    }
}

/// Checks JWTs against the trusted issuers.
pub struct Validator {
    issuers: Vec<Arc<TrustedIssuer>>,
    skew: i64,
}

impl Validator {
    pub fn new(issuers: Vec<Arc<TrustedIssuer>>, skew_secs: u64) -> Validator {
        Validator {
            issuers,
            skew: skew_secs as i64,
        }
    }

    pub fn issuers(&self) -> &[Arc<TrustedIssuer>] {
        &self.issuers
    }

    pub fn skew(&self) -> i64 {
        self.skew
    }

    /// The trusted issuer that a token names in `iss`, without checking anything else.
    pub fn issuer_of(&self, token: &[u8]) -> Option<&Arc<TrustedIssuer>> {
        let claims: IssOnly = jsonwebtoken::dangerous::insecure_decode(token).ok()?.claims;
        let iss = claims.iss?;
        self.issuers.iter().find(|i| i.rules.issuer == iss)
    }

    /// Check a token fully: header, issuer, key, signature, registered claims, and scopes.
    pub fn check(&self, token: &[u8], now: i64) -> Result<Claims, Reject> {
        if token.len() > MAX_TOKEN_BYTES {
            return Err(Reject::Malformed);
        }
        let header = jsonwebtoken::decode_header(token).map_err(|_| Reject::Malformed)?;
        let issuer = self.issuer_of(token).ok_or(Reject::UnknownIssuer)?;
        let rules = &issuer.rules;
        if !rules.algorithms.contains(&header.alg) {
            return Err(Reject::Algorithm);
        }
        let keys = issuer.keys()?;
        let key = keys.find(header.kid.as_deref(), header.alg)?;

        let mut v = Validation::new(header.alg);
        v.set_issuer(&[&rules.issuer]);
        v.set_audience(&[&rules.audience]);
        v.leeway = self.skew as u64;
        v.validate_nbf = true;
        v.required_spec_claims = HashSet::from(["exp".into(), "iss".into(), "aud".into()]);
        let data =
            jsonwebtoken::decode::<RawClaims>(token, key, &v).map_err(|e| match e.kind() {
                ErrorKind::InvalidSignature => Reject::Signature,
                ErrorKind::ExpiredSignature => Reject::Expired,
                ErrorKind::ImmatureSignature => Reject::NotYetValid,
                ErrorKind::InvalidAudience => Reject::Audience,
                ErrorKind::InvalidIssuer => Reject::UnknownIssuer,
                ErrorKind::InvalidAlgorithm => Reject::Algorithm,
                _ => Reject::Malformed,
            })?;
        let c = data.claims;
        match c.iat {
            Some(iat) if iat > now + self.skew => return Err(Reject::IssuedInFuture),
            None if rules.require_iat => return Err(Reject::Malformed),
            _ => {}
        }
        let scopes = issuer.scopes(&c);
        if scopes.is_empty() {
            return Err(Reject::NoScope);
        }
        Ok(Claims {
            sub: c.sub,
            jti: c.jti,
            exp: c.exp,
            nbf: c.nbf,
            scopes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::jwk::Jwk;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use serde_json::json;

    const ISS: &str = "https://idp.test";
    const AUD: &str = "discus";

    struct TestKey {
        enc: EncodingKey,
        jwk: serde_json::Value,
        alg: Algorithm,
        kid: String,
    }

    fn es256(kid: &str) -> TestKey {
        let kp = rcgen::KeyPair::generate().unwrap();
        let enc = EncodingKey::from_ec_der(&kp.serialize_der());
        key(enc, Algorithm::ES256, kid)
    }

    fn rs256(kid: &str) -> TestKey {
        let kp = rcgen::KeyPair::generate_for(&rcgen::PKCS_RSA_SHA256).unwrap();
        let enc = EncodingKey::from_rsa_pem(kp.serialize_pem().as_bytes()).unwrap();
        key(enc, Algorithm::RS256, kid)
    }

    fn key(enc: EncodingKey, alg: Algorithm, kid: &str) -> TestKey {
        let mut jwk = serde_json::to_value(Jwk::from_encoding_key(&enc, alg).unwrap()).unwrap();
        jwk["kid"] = json!(kid);
        TestKey {
            enc,
            jwk,
            alg,
            kid: kid.into(),
        }
    }

    fn sign(k: &TestKey, claims: serde_json::Value) -> String {
        let mut h = Header::new(k.alg);
        h.kid = Some(k.kid.clone());
        encode(&h, &claims, &k.enc).unwrap()
    }

    fn now() -> i64 {
        unix_now()
    }

    fn claims(scope: &str) -> serde_json::Value {
        json!({"iss": ISS, "aud": AUD, "sub": "alice", "exp": now() + 300, "iat": now(), "scope": scope})
    }

    fn issuer(rules: IssuerRules, keys: &[&TestKey]) -> Arc<TrustedIssuer> {
        let set: Vec<_> = keys.iter().map(|k| k.jwk.clone()).collect();
        let i = Arc::new(TrustedIssuer::new(rules));
        i.set_keys(KeySet::parse(json!({ "keys": set }).to_string().as_bytes()));
        i
    }

    fn validator(keys: &[&TestKey]) -> Validator {
        Validator::new(vec![issuer(IssuerRules::new(ISS, AUD), keys)], 60)
    }

    #[test]
    fn a_valid_rs256_token_passes_with_its_scopes() {
        let k = rs256("r1");
        let c = validator(&[&k])
            .check(sign(&k, claims("openid discus.read")).as_bytes(), now())
            .unwrap();
        assert_eq!(c.scopes, Scopes::READ);
        assert_eq!(c.sub.as_deref(), Some("alice"));
    }

    #[test]
    fn a_valid_es256_token_passes() {
        let k = es256("e1");
        let c = validator(&[&k])
            .check(sign(&k, claims("discus.write")).as_bytes(), now())
            .unwrap();
        assert!(c.scopes.can_write());
        assert_eq!(c.exp, claims("")["exp"].as_i64().unwrap());
    }

    fn with(mut c: serde_json::Value, key: &str, v: serde_json::Value) -> serde_json::Value {
        c[key] = v;
        c
    }

    fn reject(v: &Validator, k: &TestKey, c: serde_json::Value) -> Reject {
        v.check(sign(k, c).as_bytes(), now()).unwrap_err()
    }

    #[test]
    fn expired_beyond_the_skew_is_rejected() {
        let k = es256("e1");
        let v = validator(&[&k]);
        let c = with(claims("discus.read"), "exp", json!(now() - 61));
        assert_eq!(reject(&v, &k, c), Reject::Expired);
    }

    #[test]
    fn expired_within_the_skew_passes() {
        let k = es256("e1");
        let c = with(claims("discus.read"), "exp", json!(now() - 59));
        assert!(validator(&[&k])
            .check(sign(&k, c).as_bytes(), now())
            .is_ok());
    }

    #[test]
    fn nbf_in_the_future_is_rejected() {
        let k = es256("e1");
        let v = validator(&[&k]);
        let c = with(claims("discus.read"), "nbf", json!(now() + 61));
        assert_eq!(reject(&v, &k, c), Reject::NotYetValid);
    }

    #[test]
    fn iat_in_the_future_is_rejected() {
        let k = es256("e1");
        let v = validator(&[&k]);
        let c = with(claims("discus.read"), "iat", json!(now() + 61));
        assert_eq!(reject(&v, &k, c), Reject::IssuedInFuture);
    }

    #[test]
    fn a_missing_exp_is_rejected() {
        let k = es256("e1");
        let v = validator(&[&k]);
        let mut c = claims("discus.read");
        c.as_object_mut().unwrap().remove("exp");
        assert_eq!(reject(&v, &k, c), Reject::Malformed);
    }

    #[test]
    fn the_wrong_audience_is_rejected() {
        let k = es256("e1");
        let v = validator(&[&k]);
        let c = with(claims("discus.read"), "aud", json!(["other", "api"]));
        assert_eq!(reject(&v, &k, c), Reject::Audience);
        let ok = with(claims("discus.read"), "aud", json!(["other", AUD]));
        assert!(v.check(sign(&k, ok).as_bytes(), now()).is_ok());
    }

    #[test]
    fn an_unknown_issuer_is_rejected() {
        let k = es256("e1");
        let v = validator(&[&k]);
        let c = with(claims("discus.read"), "iss", json!("https://evil.test"));
        assert_eq!(reject(&v, &k, c), Reject::UnknownIssuer);
    }

    #[test]
    fn a_flipped_signature_bit_is_rejected() {
        let k = es256("e1");
        let mut t = sign(&k, claims("discus.read")).into_bytes();
        let last = t.len() - 2;
        t[last] = if t[last] == b'A' { b'B' } else { b'A' };
        assert_eq!(
            validator(&[&k]).check(&t, now()).unwrap_err(),
            Reject::Signature
        );
    }

    #[test]
    fn an_unknown_kid_is_reported() {
        let k = es256("e1");
        let other = es256("e2");
        let v = validator(&[&k]);
        assert_eq!(
            reject(&v, &other, claims("discus.read")),
            Reject::UnknownKid
        );
    }

    #[test]
    fn none_and_hs256_are_rejected() {
        let k = es256("e1");
        let v = validator(&[&k]);
        let mut h = Header::new(Algorithm::HS256);
        h.kid = Some("e1".into());
        let hs = encode(
            &h,
            &claims("discus.read"),
            &EncodingKey::from_secret(b"secret"),
        )
        .unwrap();
        assert_eq!(
            v.check(hs.as_bytes(), now()).unwrap_err(),
            Reject::Algorithm
        );
        use base64::Engine;
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let none = format!(
            "{}.{}.",
            b64.encode(br#"{"alg":"none","kid":"e1"}"#),
            b64.encode(claims("discus.read").to_string())
        );
        assert!(v.check(none.as_bytes(), now()).is_err());
    }

    #[test]
    fn an_allowed_alg_with_the_wrong_key_type_is_rejected() {
        let ec = es256("k1");
        let rsa = rs256("k1");
        // The set has only the EC key, and the token says RS256 with the same kid.
        let v = validator(&[&ec]);
        assert!(v
            .check(sign(&rsa, claims("discus.read")).as_bytes(), now())
            .is_err());
    }

    #[test]
    fn a_token_naming_issuer_b_never_uses_a_key_of_issuer_a() {
        let a = es256("shared-kid");
        let b = es256("other");
        let v = Validator::new(
            vec![
                issuer(IssuerRules::new("https://a.test", AUD), &[&a]),
                issuer(IssuerRules::new("https://b.test", AUD), &[&b]),
            ],
            60,
        );
        // Signed by A's key, but claiming to come from B.
        let c = with(claims("discus.read"), "iss", json!("https://b.test"));
        assert_eq!(reject(&v, &a, c), Reject::UnknownKid);
        let c = with(claims("discus.read"), "iss", json!("https://a.test"));
        assert!(v.check(sign(&a, c).as_bytes(), now()).is_ok());
    }

    #[test]
    fn scp_arrays_and_scope_prefixes_map_to_scopes() {
        let k = es256("e1");
        let mut rules = IssuerRules::new(ISS, AUD);
        rules.scope_prefix = "api://discus/".into();
        let v = Validator::new(vec![issuer(rules, &[&k])], 60);
        let mut c = claims("");
        c.as_object_mut().unwrap().remove("scope");
        c["scp"] = json!(["api://discus/discus.write", "discus.admin"]);
        let got = v.check(sign(&k, c).as_bytes(), now()).unwrap();
        assert_eq!(got.scopes, Scopes::WRITE);
    }

    #[test]
    fn a_token_without_a_discus_scope_is_rejected() {
        let k = es256("e1");
        let v = validator(&[&k]);
        assert_eq!(reject(&v, &k, claims("openid profile")), Reject::NoScope);
    }

    #[test]
    fn subject_rules_replace_the_scope_claim() {
        let k = es256("e1");
        let mut rules = IssuerRules::new(ISS, AUD);
        rules.subjects = vec![SubjectRule {
            pattern: "system:serviceaccount:web:*".into(),
            scopes: Scopes::READ,
        }];
        let v = Validator::new(vec![issuer(rules, &[&k])], 60);
        let c = with(
            claims("discus.write"),
            "sub",
            json!("system:serviceaccount:web:api"),
        );
        assert_eq!(
            v.check(sign(&k, c).as_bytes(), now()).unwrap().scopes,
            Scopes::READ
        );
        let c = with(
            claims("discus.write"),
            "sub",
            json!("system:serviceaccount:db:api"),
        );
        assert_eq!(reject(&v, &k, c), Reject::NoScope);
    }

    #[test]
    fn hs_algorithms_are_config_errors() {
        assert!(parse_algorithms(&["RS256".into(), "ES256".into()]).is_ok());
        assert!(parse_algorithms(&["HS256".into()]).is_err());
        assert!(parse_algorithms(&["none".into()]).is_err());
    }
}
