//! A mock OpenID provider over HTTPS: discovery, JWKS, introspection, and the Kubernetes paths.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use jsonwebtoken::jwk::Jwk;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde_json::{json, Value};

use super::TestCa;

struct Key {
    kid: String,
    alg: Algorithm,
    enc: EncodingKey,
    jwk: Value,
}

#[derive(Default)]
struct IdpState {
    issuer: Mutex<String>,
    keys: Mutex<Vec<Key>>,
    down: AtomicBool,
    jwks_fetches: AtomicUsize,
    introspect_calls: AtomicUsize,
    /// Opaque token -> introspection answer.
    opaque: Mutex<HashMap<String, Value>>,
    /// `Authorization` headers seen on any request.
    bearers: Mutex<Vec<String>>,
}

pub struct MockIdp {
    pub issuer: String,
    state: Arc<IdpState>,
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn record(state: &IdpState, headers: &HeaderMap) {
    if let Some(v) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
        state.bearers.lock().unwrap().push(v.to_string());
    }
}

async fn discovery(State(s): State<Arc<IdpState>>, headers: HeaderMap) -> Response {
    record(&s, &headers);
    if s.down.load(Ordering::SeqCst) {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let iss = s.issuer.lock().unwrap().clone();
    Json(json!({
        "issuer": iss,
        "jwks_uri": format!("{iss}/jwks"),
        "introspection_endpoint": format!("{iss}/introspect"),
    }))
    .into_response()
}

async fn jwks(State(s): State<Arc<IdpState>>, headers: HeaderMap) -> Response {
    record(&s, &headers);
    if s.down.load(Ordering::SeqCst) {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    s.jwks_fetches.fetch_add(1, Ordering::SeqCst);
    let keys: Vec<Value> = s
        .keys
        .lock()
        .unwrap()
        .iter()
        .map(|k| k.jwk.clone())
        .collect();
    Json(json!({ "keys": keys })).into_response()
}

async fn introspect(State(s): State<Arc<IdpState>>, headers: HeaderMap, body: String) -> Response {
    record(&s, &headers);
    s.introspect_calls.fetch_add(1, Ordering::SeqCst);
    // Client authentication: Basic discus:secret.
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    if auth != "Basic ZGlzY3VzOnNlY3JldA==" {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let token = body
        .split('&')
        .find_map(|kv| kv.strip_prefix("token="))
        .unwrap_or_default()
        .to_string();
    let answer = s
        .opaque
        .lock()
        .unwrap()
        .get(&token)
        .cloned()
        .unwrap_or(json!({"active": false}));
    Json(answer).into_response()
}

impl MockIdp {
    /// Start the provider with one ES256 key, `k1`.
    pub async fn start(ca: &TestCa) -> MockIdp {
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = tcp.local_addr().unwrap().port();
        let issuer = format!("https://localhost:{port}");
        let state = Arc::new(IdpState::default());
        *state.issuer.lock().unwrap() = issuer.clone();
        let cert = ca.server_cert();
        let config =
            discus::security::tls::server_config(&cert.cert, &cert.key, None, false).unwrap();
        let listener = discus::security::stream::TlsListener::new(tcp, Arc::new(config));
        let app = Router::new()
            .route("/.well-known/openid-configuration", get(discovery))
            .route("/jwks", get(jwks))
            .route("/openid/v1/jwks", get(jwks))
            .route("/introspect", post(introspect))
            .with_state(state.clone());
        tokio::spawn(async move { axum::serve(listener, app).await });
        let idp = MockIdp { issuer, state };
        idp.add_key("k1", Algorithm::ES256);
        idp
    }

    /// Answer discovery with another `issuer` value, as a Kubernetes API server does.
    pub fn set_issuer_name(&self, name: &str) {
        *self.state.issuer.lock().unwrap() = name.to_string();
    }

    pub fn add_key(&self, kid: &str, alg: Algorithm) {
        let enc = match alg {
            Algorithm::RS256 => {
                let kp = rcgen::KeyPair::generate_for(&rcgen::PKCS_RSA_SHA256).unwrap();
                EncodingKey::from_rsa_pem(kp.serialize_pem().as_bytes()).unwrap()
            }
            _ => EncodingKey::from_ec_der(&rcgen::KeyPair::generate().unwrap().serialize_der()),
        };
        let mut jwk = serde_json::to_value(Jwk::from_encoding_key(&enc, alg).unwrap()).unwrap();
        jwk["kid"] = json!(kid);
        jwk["use"] = json!("sig");
        self.state.keys.lock().unwrap().push(Key {
            kid: kid.into(),
            alg,
            enc,
            jwk,
        });
    }

    pub fn remove_key(&self, kid: &str) {
        self.state.keys.lock().unwrap().retain(|k| k.kid != kid);
    }

    pub fn set_down(&self, down: bool) {
        self.state.down.store(down, Ordering::SeqCst);
    }

    pub fn jwks_fetches(&self) -> usize {
        self.state.jwks_fetches.load(Ordering::SeqCst)
    }

    pub fn introspect_calls(&self) -> usize {
        self.state.introspect_calls.load(Ordering::SeqCst)
    }

    pub fn bearers(&self) -> Vec<String> {
        self.state.bearers.lock().unwrap().clone()
    }

    /// Register an opaque token and its introspection answer.
    pub fn add_opaque(&self, token: &str, answer: Value) {
        self.state
            .opaque
            .lock()
            .unwrap()
            .insert(token.into(), answer);
    }

    /// Sign `claims` with key `kid`. The key must exist.
    pub fn sign(&self, kid: &str, claims: &Value) -> String {
        let keys = self.state.keys.lock().unwrap();
        let k = keys.iter().find(|k| k.kid == kid).unwrap();
        let mut h = Header::new(k.alg);
        h.kid = Some(k.kid.clone());
        encode(&h, claims, &k.enc).unwrap()
    }

    /// Standard claims from this issuer with audience `discus`, valid for 300 s.
    pub fn claims(&self, scope: &str) -> Value {
        json!({
            "iss": self.issuer,
            "aud": "discus",
            "sub": "alice",
            "jti": "j-1",
            "iat": now(),
            "exp": now() + 300,
            "scope": scope,
        })
    }

    /// A token from `k1` with `scope`.
    pub fn token(&self, scope: &str) -> String {
        self.sign("k1", &self.claims(scope))
    }

    /// An `[[issuer]]` TOML block that trusts this provider through the test CA.
    pub fn toml(&self, ca: &TestCa, extra: &str) -> String {
        format!(
            "[[issuer]]\nissuer = \"{}\"\naudience = \"discus\"\nca_file = \"{}\"\n{extra}\n",
            self.issuer,
            ca.ca_str()
        )
    }
}

pub fn unix_now() -> i64 {
    now()
}
