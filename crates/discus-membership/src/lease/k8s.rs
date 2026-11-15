//! The Kubernetes provider: the `Lease` `discus-cluster` in `coordination.k8s.io/v1` (spec
//! section 6.2). Every write carries the `resourceVersion` of the read, so two nodes never both
//! win. The holder writes `holderIdentity` and the annotations `discus.io/address` and
//! `discus.io/cluster-id`.
//!
//! A contender judges expiry as client-go leader election does
//! (`k8s.io/client-go/tools/leaderelection/leaderelection.go`, `tryAcquireOrRenew` and
//! `isLeaseValid`). It remembers the record it last read and the local monotonic time at which
//! that record first appeared. The lease is valid until `leaseDurationSeconds` after that time.
//! The holder's `renewTime` is never compared with the local wall clock, so clock skew between
//! nodes cannot end a live lease. A node that has not read the record before waits one full
//! duration from its first read.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use discus_placement::ClusterId;
use futures::future::BoxFuture;
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use k8s_openapi::jiff::Timestamp;
use kube::api::{Api, PostParams};
use parking_lot::Mutex;

use super::{LeaseError, LeaseHolder, LeaseProvider, LeaseState};

const ADDRESS: &str = "discus.io/address";
const CLUSTER_ID: &str = "discus.io/cluster-id";
const CONFLICT: u16 = 409;

type Clock = Arc<dyn Fn() -> Instant + Send + Sync>;

/// The parts of a record that change on every write. client-go compares the whole raw record.
/// The `resourceVersion` alone changes on every write, and the holder and the renewal time
/// cover a server that reuses it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordKey {
    resource_version: Option<String>,
    holder: Option<String>,
    renew_time: Option<Timestamp>,
}

impl RecordKey {
    fn of(lease: &Lease) -> RecordKey {
        let spec = lease.spec.as_ref();
        RecordKey {
            resource_version: lease.metadata.resource_version.clone(),
            holder: spec.and_then(|s| s.holder_identity.clone()),
            renew_time: spec.and_then(|s| s.renew_time.as_ref()).map(|t| t.0),
        }
    }
}

struct Observed {
    key: RecordKey,
    /// The local monotonic time at which this node first read `key`.
    at: Instant,
}

pub struct K8sLease {
    api: Api<Lease>,
    name: String,
    duration: Duration,
    timeout: Duration,
    clock: Clock,
    observed: Mutex<Option<Observed>>,
}

fn error(e: impl std::fmt::Display) -> LeaseError {
    LeaseError(e.to_string())
}

fn is_conflict(e: &kube::Error) -> bool {
    matches!(e, kube::Error::Api(s) if s.code == CONFLICT)
}

impl K8sLease {
    pub fn new(client: kube::Client, namespace: &str, name: &str, duration: Duration) -> K8sLease {
        K8sLease {
            api: Api::namespaced(client, namespace),
            name: name.to_string(),
            duration,
            timeout: duration / 3,
            clock: Arc::new(Instant::now),
            observed: Mutex::new(None),
        }
    }

    /// Judge expiry on `clock` instead of the process monotonic clock.
    #[cfg(feature = "testing")]
    pub fn with_clock(mut self, clock: Clock) -> K8sLease {
        self.clock = clock;
        self
    }

    /// Bound one `acquire_or_renew` call. The client retries an unavailable API server on its
    /// own, and a call must end before the next renewal. The default is a third of the duration.
    pub fn with_timeout(mut self, timeout: Duration) -> K8sLease {
        self.timeout = timeout;
        self
    }

    /// A provider on the in-cluster service account: the token, the CA, and the API server
    /// address from the pod environment.
    pub fn in_cluster(
        namespace: &str,
        name: &str,
        duration: Duration,
    ) -> Result<K8sLease, LeaseError> {
        let config = kube::Config::incluster().map_err(error)?;
        let client = kube::Client::try_from(config).map_err(error)?;
        Ok(K8sLease::new(client, namespace, name, duration))
    }

    fn body(&self, me: &LeaseHolder, previous: Option<&Lease>) -> Lease {
        let now = MicroTime(Timestamp::now());
        let old = previous.and_then(|l| l.spec.as_ref());
        let same_holder =
            old.and_then(|s| s.holder_identity.as_deref()) == Some(me.identity.as_str());
        let transitions = old.and_then(|s| s.lease_transitions).unwrap_or(0);
        let mut meta = previous.map(|l| l.metadata.clone()).unwrap_or(ObjectMeta {
            name: Some(self.name.clone()),
            ..ObjectMeta::default()
        });
        let annotations = meta.annotations.get_or_insert_with(BTreeMap::new);
        annotations.insert(ADDRESS.into(), me.address.to_string());
        annotations.insert(CLUSTER_ID.into(), me.cluster_id.to_string());
        Lease {
            metadata: meta,
            spec: Some(LeaseSpec {
                holder_identity: Some(me.identity.clone()),
                lease_duration_seconds: Some(i32::try_from(self.duration.as_secs()).unwrap_or(15)),
                acquire_time: if same_holder {
                    old.and_then(|s| s.acquire_time.clone())
                } else {
                    Some(now.clone())
                },
                renew_time: Some(now),
                lease_transitions: Some(match (previous, same_holder) {
                    (None, _) | (_, true) => transitions,
                    _ => transitions + 1,
                }),
                ..LeaseSpec::default()
            }),
        }
    }

    async fn call(&self, me: &LeaseHolder) -> Result<LeaseState, LeaseError> {
        let current = self.api.get_opt(&self.name).await.map_err(error)?;
        let Some(lease) = current else {
            return match self
                .api
                .create(&PostParams::default(), &self.body(me, None))
                .await
            {
                Ok(stored) => {
                    self.observe(&stored);
                    Ok(LeaseState::Held(me.clone()))
                }
                Err(e) if is_conflict(&e) => self.reread().await,
                Err(e) => Err(error(e)),
            };
        };
        let holder = holder_of(&lease);
        let live = holder.is_some() && self.observe(&lease);
        match holder {
            Some(h) if live && h.identity != me.identity => {
                if h.cluster_id == me.cluster_id && me.cluster_id != ClusterId::NIL {
                    Ok(LeaseState::Held(h))
                } else {
                    Ok(LeaseState::HeldByOther(h))
                }
            }
            _ => {
                let body = self.body(me, Some(&lease));
                match self
                    .api
                    .replace(&self.name, &PostParams::default(), &body)
                    .await
                {
                    Ok(stored) => {
                        self.observe(&stored);
                        Ok(LeaseState::Held(me.clone()))
                    }
                    Err(e) if is_conflict(&e) => self.reread().await,
                    Err(e) => Err(error(e)),
                }
            }
        }
    }

    /// After a lost race: report the winner.
    async fn reread(&self) -> Result<LeaseState, LeaseError> {
        let lease = self.api.get(&self.name).await.map_err(error)?;
        self.observe(&lease);
        holder_of(&lease)
            .map(LeaseState::HeldByOther)
            .ok_or_else(|| LeaseError("the lease has no holder after a conflict".into()))
    }
}

fn holder_of(lease: &Lease) -> Option<LeaseHolder> {
    let identity = lease.spec.as_ref()?.holder_identity.clone()?;
    let annotations = lease.metadata.annotations.as_ref();
    let get = |k: &str| annotations.and_then(|a| a.get(k));
    Some(LeaseHolder {
        identity,
        address: get(ADDRESS)?.parse().ok()?,
        cluster_id: get(CLUSTER_ID)
            .and_then(|c| c.parse().ok())
            .unwrap_or(ClusterId::NIL),
    })
}

impl K8sLease {
    /// Record `lease` as read now, and tell whether it is still valid: less than its
    /// `leaseDurationSeconds` passed on the local monotonic clock since this node first read
    /// this record. As in client-go, the duration comes from the record, and a missing one is 0.
    fn observe(&self, lease: &Lease) -> bool {
        let now = (self.clock)();
        let key = RecordKey::of(lease);
        let mut observed = self.observed.lock();
        let at = match observed.as_ref() {
            Some(o) if o.key == key => o.at,
            _ => {
                *observed = Some(Observed { key, at: now });
                now
            }
        };
        let secs = lease
            .spec
            .as_ref()
            .and_then(|s| s.lease_duration_seconds)
            .unwrap_or(0);
        let duration = Duration::from_secs(u64::try_from(secs).unwrap_or(0));
        now.saturating_duration_since(at) < duration
    }
}

impl LeaseProvider for K8sLease {
    fn acquire_or_renew<'a>(
        &'a self,
        me: &'a LeaseHolder,
    ) -> BoxFuture<'a, Result<LeaseState, LeaseError>> {
        Box::pin(async move {
            tokio::time::timeout(self.timeout, self.call(me))
                .await
                .unwrap_or_else(|_| Err(LeaseError("the API server did not answer in time".into())))
        })
    }

    fn name(&self) -> &'static str {
        "k8s"
    }
}
