//! The Kubernetes provider: the `Lease` `discus-cluster` in `coordination.k8s.io/v1` (spec
//! section 6.2). Every write carries the `resourceVersion` of the read, so two nodes never both
//! win. The holder writes `holderIdentity` and the annotations `discus.io/address` and
//! `discus.io/cluster-id`.

use std::collections::BTreeMap;
use std::time::Duration;

use discus_placement::ClusterId;
use futures::future::BoxFuture;
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use k8s_openapi::jiff::{SignedDuration, Timestamp};
use kube::api::{Api, PostParams};

use super::{LeaseError, LeaseHolder, LeaseProvider, LeaseState};

const ADDRESS: &str = "discus.io/address";
const CLUSTER_ID: &str = "discus.io/cluster-id";
const CONFLICT: u16 = 409;

pub struct K8sLease {
    api: Api<Lease>,
    name: String,
    duration: Duration,
    timeout: Duration,
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
        }
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
                Ok(_) => Ok(LeaseState::Held(me.clone())),
                Err(e) if is_conflict(&e) => self.reread().await,
                Err(e) => Err(error(e)),
            };
        };
        let holder = holder_of(&lease);
        let live = holder.is_some() && !expired(&lease);
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
                    Ok(_) => Ok(LeaseState::Held(me.clone())),
                    Err(e) if is_conflict(&e) => self.reread().await,
                    Err(e) => Err(error(e)),
                }
            }
        }
    }

    /// After a lost race: report the winner.
    async fn reread(&self) -> Result<LeaseState, LeaseError> {
        let lease = self.api.get(&self.name).await.map_err(error)?;
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

/// `renewTime + leaseDurationSeconds` is in the past on the local wall clock.
fn expired(lease: &Lease) -> bool {
    let Some(spec) = lease.spec.as_ref() else {
        return true;
    };
    let (Some(renewed), Some(secs)) = (spec.renew_time.as_ref(), spec.lease_duration_seconds)
    else {
        return true;
    };
    renewed.0 + SignedDuration::from_secs(i64::from(secs)) <= Timestamp::now()
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
