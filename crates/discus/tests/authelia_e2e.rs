#![allow(clippy::unwrap_used)]

//! Authelia in a container, with its session store on a three-node cluster (spec section 19.1).

mod common;

use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_lc_rs::hmac;
use common::cluster::{start_cluster, ClusterNode};
use data_encoding::BASE32_NOPAD;
use reqwest::header::{CONTENT_TYPE, COOKIE, SET_COOKIE};
use reqwest::{RequestBuilder, Response, StatusCode};
use serde_json::{json, Value};

// The registry is explicit, because Podman's `docker` shim refuses a short name without a TTY.
const IMAGE: &str = "docker.io/authelia/authelia:4.39.28";
const USER: &str = "alice";
const COOKIE_NAME: &str = "authelia_session";
const COOKIE_DOMAIN: &str = "example.com";
const AUTH_HOST: &str = "auth.example.com";
const TOTP_PERIOD_SECS: u64 = 30;
const SESSION_PREFIX: &str = "authelia-session";

/// Removes the container when the test ends, also after a panic. A panic prints the container
/// logs first, because they hold the only explanation of a failed start.
struct Container {
    name: String,
}

impl Drop for Container {
    fn drop(&mut self) {
        let docker = |args: &[&str]| std::process::Command::new("docker").args(args).output();
        if std::thread::panicking() {
            if let Ok(logs) = docker(&["logs", &self.name]) {
                eprintln!("{}", String::from_utf8_lossy(&logs.stdout));
                eprintln!("{}", String::from_utf8_lossy(&logs.stderr));
            }
        }
        let _ = docker(&["rm", "-f", &self.name]);
    }
}

async fn session_exists(node: &ClusterNode, cookie: &str) -> bool {
    let key = format!("{SESSION_PREFIX}:{cookie}");
    redis::cmd("EXISTS")
        .arg(key)
        .query_async(&mut node.redis().await)
        .await
        .unwrap()
}

// The process API of tokio, because a blocking call here would stop the nodes that share the
// runtime and that Authelia talks to.
async fn docker(args: &[&str]) -> String {
    let output = tokio::process::Command::new("docker")
        .args(args)
        .output()
        .await
        .expect("docker runs");
    assert!(
        output.status.success(),
        "docker {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn free_tcp_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn secret() -> String {
    (0..64).map(|_| fastrand::alphanumeric()).collect()
}

fn write_files(dir: &Path, port: u16, redis: SocketAddr, digest: &str) {
    let config = format!(
        r"certificates_directory: '/config/certs'
server:
  address: 'tcp://127.0.0.1:{port}/'
log:
  level: 'debug'
identity_validation:
  reset_password:
    jwt_secret: '{jwt}'
authentication_backend:
  file:
    path: '/config/users.yml'
access_control:
  default_policy: 'two_factor'
session:
  secret: '{session}'
  cookies:
    - domain: '{COOKIE_DOMAIN}'
      authelia_url: 'https://{AUTH_HOST}'
  redis:
    host: '{host}'
    port: {redis_port}
    database_index: 0
    tls:
      server_name: '{host}'
      minimum_version: 'TLS1.2'
storage:
  encryption_key: '{storage}'
  local:
    path: '/config/db.sqlite3'
notifier:
  filesystem:
    filename: '/config/notification.txt'
",
        jwt = secret(),
        session = secret(),
        storage = secret(),
        host = redis.ip(),
        redis_port = redis.port(),
    );
    std::fs::write(dir.join("configuration.yml"), config).unwrap();
    // Every client uses TLS. Authelia trusts the CA of the test node certificates.
    std::fs::create_dir_all(dir.join("certs")).unwrap();
    std::fs::copy(common::client_tls::ca_file(), dir.join("certs/ca.pem")).unwrap();
    let users = format!(
        "users:\n  {USER}:\n    disabled: false\n    displayname: 'Alice'\n    password: '{digest}'\n    email: 'alice@example.com'\n    groups: []\n"
    );
    std::fs::write(dir.join("users.yml"), users).unwrap();
}

/// RFC 6238 with the defaults of `authelia storage user totp generate`.
fn totp_code(secret: &[u8]) -> String {
    let unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let key = hmac::Key::new(hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, secret);
    let tag = hmac::sign(&key, &(unix / TOTP_PERIOD_SECS).to_be_bytes());
    let digest = tag.as_ref();
    let offset = usize::from(digest[digest.len() - 1] & 0x0f);
    let word = u32::from_be_bytes(digest[offset..offset + 4].try_into().unwrap());
    format!("{:06}", (word & 0x7fff_ffff) % 1_000_000)
}

/// A browser behind a TLS proxy, reduced to what Authelia reads. The client carries the cookie
/// by hand, because the cookie is `Secure` and this connection is plain HTTP.
struct Browser {
    http: reqwest::Client,
    base: String,
    cookie: Option<String>,
}

impl Browser {
    fn request(&self, builder: RequestBuilder) -> RequestBuilder {
        let builder = builder
            .header("X-Forwarded-Proto", "https")
            .header("X-Forwarded-Host", AUTH_HOST)
            .header("X-Forwarded-For", "127.0.0.1");
        match &self.cookie {
            Some(cookie) => builder.header(COOKIE, format!("{COOKIE_NAME}={cookie}")),
            None => builder,
        }
    }

    fn keep_cookie(&mut self, response: &Response) {
        let prefix = format!("{COOKIE_NAME}=");
        for header in response.headers().get_all(SET_COOKIE) {
            let value = header.to_str().unwrap();
            if let Some(rest) = value.strip_prefix(&prefix) {
                self.cookie = Some(rest.split(';').next().unwrap().to_owned());
            }
        }
    }

    async fn post(&mut self, path: &str, body: Value) -> (StatusCode, String) {
        let url = format!("{}{path}", self.base);
        let builder = self.http.post(url).header(CONTENT_TYPE, "application/json");
        let response = self
            .request(builder)
            .body(body.to_string())
            .send()
            .await
            .unwrap();
        self.keep_cookie(&response);
        (response.status(), response.text().await.unwrap())
    }

    async fn state(&mut self) -> Value {
        let url = format!("{}/api/state", self.base);
        let response = self.request(self.http.get(url)).send().await.unwrap();
        self.keep_cookie(&response);
        assert_eq!(response.status(), 200);
        serde_json::from_str(&response.text().await.unwrap()).unwrap()
    }

    async fn wait_healthy(&self, timeout: Duration) {
        let url = format!("{}/api/health", self.base);
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let reply = self.request(self.http.get(&url)).send().await;
            if reply.is_ok_and(|r| r.status() == 200) {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "Authelia did not become healthy"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs Docker"]
async fn sign_in_second_factor_and_sign_out() {
    let cluster = start_cluster(
        3,
        &[
            "--primary-read-prefix",
            SESSION_PREFIX,
            "--replication-factor",
            "3",
            "--hold-budget-ms",
            "2000",
        ],
    )
    .await;

    let dir = tempfile::tempdir().unwrap();
    let password = secret();
    let hashed = docker(&[
        "run",
        "--rm",
        IMAGE,
        "authelia",
        "crypto",
        "hash",
        "generate",
        "argon2",
        "--password",
        &password,
    ])
    .await;
    let digest = hashed.trim().strip_prefix("Digest: ").unwrap();

    let port = free_tcp_port();
    write_files(dir.path(), port, cluster.nodes[0].addr, digest);

    let container = Container {
        name: format!("discus-authelia-e2e-{}", std::process::id()),
    };
    let mount = format!("{}:/config:z", dir.path().display());
    docker(&[
        "run",
        "-d",
        "--name",
        &container.name,
        "--network",
        "host",
        "-v",
        &mount,
        IMAGE,
    ])
    .await;

    let mut browser = Browser {
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .unwrap(),
        base: format!("http://127.0.0.1:{port}"),
        cookie: None,
    };
    browser.wait_healthy(Duration::from_secs(180)).await;

    let totp_secret: Vec<u8> = (0..32).map(|_| fastrand::u8(..)).collect();
    docker(&[
        "exec",
        &container.name,
        "authelia",
        "storage",
        "user",
        "totp",
        "generate",
        USER,
        "--config",
        "/config/configuration.yml",
        "--secret",
        &BASE32_NOPAD.encode(&totp_secret),
    ])
    .await;

    let (status, body) = browser
        .post(
            "/api/firstfactor",
            json!({"username": USER, "password": password, "keepMeLoggedIn": false}),
        )
        .await;
    assert_eq!(status, 200, "first factor: {body}");
    let first_cookie = browser.cookie.clone().expect("a session cookie");

    let (status, body) = browser
        .post(
            "/api/secondfactor/totp",
            json!({"token": totp_code(&totp_secret)}),
        )
        .await;
    assert_eq!(status, 200, "second factor: {body}");
    let cookie = browser.cookie.clone().expect("a session cookie");
    assert_ne!(cookie, first_cookie, "Authelia rotates the session");

    // Authelia talks to node 0 only. A session that another node finds proves that the session
    // is in the cluster and not in a store of Authelia.
    assert!(session_exists(&cluster.nodes[1], &cookie).await);
    assert!(!session_exists(&cluster.nodes[1], &first_cookie).await);

    for _ in 0..20 {
        let state = browser.state().await;
        assert_eq!(state["data"]["authentication_level"], 2, "{state}");
    }

    let (status, body) = browser.post("/api/logout", json!({})).await;
    assert_eq!(status, 200, "logout: {body}");

    browser.cookie = Some(cookie);
    let after = browser.state().await;
    assert_eq!(after["data"]["authentication_level"], 0, "{after}");
    assert!(!session_exists(&cluster.nodes[1], browser.cookie.as_ref().unwrap()).await);
}
