//! A JWKS: the public keys of one issuer.

use jsonwebtoken::jwk::{AlgorithmParameters, Jwk, PublicKeyUse};
use jsonwebtoken::{Algorithm, DecodingKey};

use super::Reject;

struct Key {
    kid: Option<String>,
    alg: Option<Algorithm>,
    key: DecodingKey,
}

/// The signing keys of one issuer.
#[derive(Default)]
pub struct KeySet {
    keys: Vec<Key>,
}

impl KeySet {
    /// Parse a JWKS document. Each key parses alone, so a key of an unknown type never drops the
    /// rest. Symmetric keys and keys that are not for signatures are skipped.
    pub fn parse(json: &[u8]) -> KeySet {
        let mut set = KeySet::default();
        let Ok(doc) = serde_json::from_slice::<serde_json::Value>(json) else {
            return set;
        };
        let Some(keys) = doc.get("keys").and_then(|k| k.as_array()) else {
            return set;
        };
        for value in keys {
            let Ok(jwk) = serde_json::from_value::<Jwk>(value.clone()) else {
                continue;
            };
            if matches!(jwk.algorithm, AlgorithmParameters::OctetKey(_)) {
                continue;
            }
            if matches!(&jwk.common.public_key_use, Some(u) if *u != PublicKeyUse::Signature) {
                continue;
            }
            let Ok(key) = DecodingKey::from_jwk(&jwk) else {
                continue;
            };
            let alg = jwk
                .common
                .key_algorithm
                .and_then(|a| a.to_string().parse::<Algorithm>().ok());
            set.keys.push(Key {
                kid: jwk.common.key_id.clone(),
                alg,
                key,
            });
        }
        set
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// True when the set has a key with this `kid`.
    pub fn has_kid(&self, kid: &str) -> bool {
        self.keys.iter().any(|k| k.kid.as_deref() == Some(kid))
    }

    /// The key for a token header. Only keys of the right family, and with a matching `alg`
    /// when the key names one, are candidates. Without a `kid`, the only candidate is used.
    pub fn find(&self, kid: Option<&str>, alg: Algorithm) -> Result<&DecodingKey, Reject> {
        let mut candidates = self
            .keys
            .iter()
            .filter(|k| k.key.family() == alg.family() && k.alg.is_none_or(|a| a == alg));
        match kid {
            Some(kid) => candidates
                .find(|k| k.kid.as_deref() == Some(kid))
                .map(|k| &k.key)
                .ok_or(Reject::UnknownKid),
            None => match (candidates.next(), candidates.next()) {
                (Some(k), None) => Ok(&k.key),
                _ => Err(Reject::UnknownKid),
            },
        }
    }
}
