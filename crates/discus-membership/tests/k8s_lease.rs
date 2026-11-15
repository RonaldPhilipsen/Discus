#![cfg(feature = "k8s")]
#![allow(clippy::unwrap_used)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use discus_membership::lease::K8sLease;
use discus_membership::{LeaseHolder, LeaseProvider, LeaseState};
use discus_placement::ClusterId;
use parking_lot::Mutex;
use serde_json::{json, Value};

/// One Lease object with a resourceVersion counter, like the API server keeps it.
#[derive(Default)]
struct Fake {
    lease: Option<Value>,
    version: u64,
    down: bool,
    /// The next PUT finds this lease stored first, as if another node won the race.
    steal: Option<Value>,
    puts: usize,
}

type Shared = Arc<Mutex<Fake>>;

fn status(code: StatusCode, reason: &str) -> (StatusCode, Json<Value>) {
    (
        code,
        Json(
            json!({"kind": "Status", "apiVersion": "v1", "metadata": {}, "status": "Failure",
            "message": reason, "reason": reason, "code": code.as_u16()}),
        ),
    )
}

fn stamp(fake: &mut Fake, mut lease: Value) -> Value {
    fake.version += 1;
    lease["metadata"]["resourceVersion"] = json!(fake.version.to_string());
    lease["kind"] = json!("Lease");
    lease["apiVersion"] = json!("coordination.k8s.io/v1");
    fake.lease = Some(lease.clone());
    lease
}

async fn read(
    State(s): State<Shared>,
    Path((_ns, _name)): Path<(String, String)>,
) -> (StatusCode, Json<Value>) {
    let fake = s.lock();
    if fake.down {
        return status(StatusCode::SERVICE_UNAVAILABLE, "ServiceUnavailable");
    }
    match &fake.lease {
        Some(l) => (StatusCode::OK, Json(l.clone())),
        None => status(StatusCode::NOT_FOUND, "NotFound"),
    }
}

async fn create(
    State(s): State<Shared>,
    Path(_ns): Path<String>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let mut fake = s.lock();
    if fake.down {
        return status(StatusCode::SERVICE_UNAVAILABLE, "ServiceUnavailable");
    }
    if fake.lease.is_some() {
        return status(StatusCode::CONFLICT, "AlreadyExists");
    }
    let stored = stamp(&mut fake, body);
    (StatusCode::CREATED, Json(stored))
}

async fn replace(
    State(s): State<Shared>,
    Path((_ns, _name)): Path<(String, String)>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let mut fake = s.lock();
    if fake.down {
        return status(StatusCode::SERVICE_UNAVAILABLE, "ServiceUnavailable");
    }
    fake.puts += 1;
    if let Some(winner) = fake.steal.take() {
        stamp(&mut fake, winner);
    }
    let current = fake.version.to_string();
    if body["metadata"]["resourceVersion"] != json!(current) {
        return status(StatusCode::CONFLICT, "Conflict");
    }
    let stored = stamp(&mut fake, body);
    (StatusCode::OK, Json(stored))
}

async fn fake_server() -> (Shared, SocketAddr) {
    let shared: Shared = Arc::default();
    let app = Router::new()
        .route(
            "/apis/coordination.k8s.io/v1/namespaces/{ns}/leases/{name}",
            get(read).put(replace),
        )
        .route(
            "/apis/coordination.k8s.io/v1/namespaces/{ns}/leases",
            post(create),
        )
        .with_state(shared.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (shared, addr)
}

fn provider(addr: SocketAddr) -> K8sLease {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let config = kube::Config::new(format!("http://{addr}").parse().unwrap());
    let client = kube::Client::try_from(config).unwrap();
    K8sLease::new(client, "discus", "discus-cluster", Duration::from_secs(15))
        .with_timeout(Duration::from_secs(2))
}

/// A manual monotonic clock: a fixed start plus an offset that the test moves.
#[derive(Clone)]
struct Clock {
    start: Instant,
    offset: Arc<Mutex<Duration>>,
}

impl Default for Clock {
    fn default() -> Clock {
        Clock {
            start: Instant::now(),
            offset: Arc::default(),
        }
    }
}

impl Clock {
    fn advance(&self, by: Duration) {
        *self.offset.lock() += by;
    }

    fn source(&self) -> Arc<dyn Fn() -> Instant + Send + Sync> {
        let c = self.clone();
        Arc::new(move || c.start + *c.offset.lock())
    }
}

fn holder(name: &str, cluster: u128) -> LeaseHolder {
    LeaseHolder {
        identity: name.into(),
        address: "10.0.0.1:7379".parse().unwrap(),
        cluster_id: ClusterId::from_u128(cluster),
    }
}

/// A stored lease held by `who` of `cluster`, renewed `age_s` seconds ago.
fn lease_json(who: &str, cluster: u128, age_s: i64) -> Value {
    let renewed =
        k8s_openapi::jiff::Timestamp::now() - k8s_openapi::jiff::SignedDuration::from_secs(age_s);
    json!({
        "metadata": {
            "name": "discus-cluster",
            "namespace": "discus",
            "annotations": {
                "discus.io/address": "10.0.0.9:7379",
                "discus.io/cluster-id": ClusterId::from_u128(cluster).to_string(),
            },
        },
        "spec": {
            "holderIdentity": who,
            "leaseDurationSeconds": 15,
            "renewTime": renewed.to_string(),
            "acquireTime": renewed.to_string(),
            "leaseTransitions": 3,
        },
    })
}

#[tokio::test]
async fn an_absent_lease_is_created() {
    let (fake, addr) = fake_server().await;
    let me = holder("node-a", 7);
    let state = provider(addr).acquire_or_renew(&me).await.unwrap();
    assert_eq!(state, LeaseState::Held(me.clone()));
    let stored = fake.lock().lease.clone().unwrap();
    assert_eq!(stored["spec"]["holderIdentity"], "node-a");
    assert_eq!(stored["spec"]["leaseDurationSeconds"], 15);
    let ann = &stored["metadata"]["annotations"];
    assert_eq!(ann["discus.io/address"], "10.0.0.1:7379");
    assert_eq!(
        ann["discus.io/cluster-id"],
        ClusterId::from_u128(7).to_string()
    );
}

#[tokio::test]
async fn a_live_lease_of_another_cluster_is_held_by_other() {
    let (fake, addr) = fake_server().await;
    {
        let mut f = fake.lock();
        stamp(&mut f, lease_json("node-z", 9, 2));
    }
    let state = provider(addr)
        .acquire_or_renew(&holder("node-a", 7))
        .await
        .unwrap();
    let LeaseState::HeldByOther(h) = state else {
        panic!("{state:?}")
    };
    assert_eq!(h.identity, "node-z");
    assert_eq!(h.address, "10.0.0.9:7379".parse::<SocketAddr>().unwrap());
    assert_eq!(h.cluster_id, ClusterId::from_u128(9));
}

#[tokio::test]
async fn a_live_lease_of_the_same_cluster_is_held() {
    let (fake, addr) = fake_server().await;
    {
        let mut f = fake.lock();
        stamp(&mut f, lease_json("node-z", 7, 2));
    }
    let state = provider(addr)
        .acquire_or_renew(&holder("node-a", 7))
        .await
        .unwrap();
    assert!(matches!(state, LeaseState::Held(h) if h.identity == "node-z"));
    assert_eq!(fake.lock().puts, 0);
}

#[tokio::test]
async fn the_holder_renews() {
    let (fake, addr) = fake_server().await;
    {
        let mut f = fake.lock();
        stamp(&mut f, lease_json("node-a", 7, 4));
    }
    let me = holder("node-a", 7);
    let state = provider(addr).acquire_or_renew(&me).await.unwrap();
    assert_eq!(state, LeaseState::Held(me));
    let f = fake.lock();
    assert_eq!(f.puts, 1);
    assert_eq!(f.lease.as_ref().unwrap()["spec"]["leaseTransitions"], 3);
}

/// The holder's clock runs 16 s behind: by the local wall clock its fresh renewal looks expired.
/// A contender never compares `renewTime` with its own clock (client-go leader election).
#[tokio::test]
async fn a_renewal_time_in_the_past_is_not_expiry() {
    let (fake, addr) = fake_server().await;
    {
        let mut f = fake.lock();
        stamp(&mut f, lease_json("node-z", 9, 16));
    }
    let state = provider(addr)
        .acquire_or_renew(&holder("node-a", 7))
        .await
        .unwrap();
    assert!(
        matches!(&state, LeaseState::HeldByOther(h) if h.identity == "node-z"),
        "{state:?}"
    );
    assert_eq!(fake.lock().puts, 0);
}

#[tokio::test]
async fn an_expired_lease_is_taken_over() {
    let (fake, addr) = fake_server().await;
    {
        let mut f = fake.lock();
        stamp(&mut f, lease_json("node-z", 9, 0));
    }
    let clock = Clock::default();
    let lease = provider(addr).with_clock(clock.source());
    let me = holder("node-a", 7);
    let state = lease.acquire_or_renew(&me).await.unwrap();
    assert!(matches!(state, LeaseState::HeldByOther(_)), "{state:?}");
    clock.advance(Duration::from_millis(14_999));
    let state = lease.acquire_or_renew(&me).await.unwrap();
    assert!(matches!(state, LeaseState::HeldByOther(_)), "{state:?}");
    clock.advance(Duration::from_millis(1));
    let state = lease.acquire_or_renew(&me).await.unwrap();
    assert_eq!(state, LeaseState::Held(me));
    let stored = fake.lock().lease.clone().unwrap();
    assert_eq!(stored["spec"]["holderIdentity"], "node-a");
    assert_eq!(stored["spec"]["leaseTransitions"], 4);
}

/// A renewal by the holder changes the record, so the wait of a contender starts again.
#[tokio::test]
async fn a_renewed_lease_restarts_the_wait() {
    let (fake, addr) = fake_server().await;
    {
        let mut f = fake.lock();
        stamp(&mut f, lease_json("node-z", 9, 0));
    }
    let clock = Clock::default();
    let lease = provider(addr).with_clock(clock.source());
    let me = holder("node-a", 7);
    assert!(matches!(
        lease.acquire_or_renew(&me).await.unwrap(),
        LeaseState::HeldByOther(_)
    ));
    clock.advance(Duration::from_secs(10));
    {
        let mut f = fake.lock();
        stamp(&mut f, lease_json("node-z", 9, 0));
    }
    assert!(matches!(
        lease.acquire_or_renew(&me).await.unwrap(),
        LeaseState::HeldByOther(_)
    ));
    clock.advance(Duration::from_secs(10));
    assert!(matches!(
        lease.acquire_or_renew(&me).await.unwrap(),
        LeaseState::HeldByOther(_)
    ));
    clock.advance(Duration::from_secs(5));
    assert_eq!(
        lease.acquire_or_renew(&me).await.unwrap(),
        LeaseState::Held(me)
    );
}

#[tokio::test]
async fn a_conflict_loses_the_race() {
    let (fake, addr) = fake_server().await;
    {
        let mut f = fake.lock();
        stamp(&mut f, lease_json("node-z", 9, 16));
    }
    let clock = Clock::default();
    let lease = provider(addr).with_clock(clock.source());
    let me = holder("node-a", 7);
    lease.acquire_or_renew(&me).await.unwrap();
    clock.advance(Duration::from_secs(15));
    fake.lock().steal = Some(lease_json("node-w", 5, 0));
    let state = lease.acquire_or_renew(&me).await.unwrap();
    assert!(
        matches!(&state, LeaseState::HeldByOther(h) if h.identity == "node-w"),
        "{state:?}"
    );
}

#[tokio::test]
async fn an_outage_is_an_error() {
    let (fake, addr) = fake_server().await;
    fake.lock().down = true;
    assert!(provider(addr)
        .acquire_or_renew(&holder("node-a", 7))
        .await
        .is_err());
}
