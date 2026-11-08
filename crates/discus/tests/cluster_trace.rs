//! The trace context crosses every hop (spec §15.4). With export off, and in a build without
//! `otel`, a node passes the inbound context on unchanged. A malformed context is dropped.

#![allow(clippy::unwrap_used)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use discus::{Config, Server};
use discus_placement::{ClusterId, EpochId, NodeId};
use discus_resp::Value;
use discus_transport::testing::{TestCa, TEST_SAN};
use discus_transport::{Inbound, Service, TlsMaterial, Transport, TransportConfig};
use discus_wire::{encode_command, Forward, ForwardMode, ForwardReply, Message, TraceContext};
use futures::future::BoxFuture;
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

const TP: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

/// A fake member that answers every request with a `FORWARD_REPLY` of `OK` and keeps the trace
/// context of each forward.
struct Capture {
    epoch: EpochId,
    seen: Mutex<Vec<TraceContext>>,
}

impl Service for Capture {
    fn epoch(&self) -> EpochId {
        self.epoch
    }

    fn call(self: Arc<Self>, inbound: Inbound) -> BoxFuture<'static, Message> {
        // The counter tier of the real node also talks to this member. Only forwards count.
        if matches!(inbound.message, Message::Forward(_)) {
            self.seen.lock().push(inbound.trace.clone());
        }
        let epoch = self.epoch;
        Box::pin(async move {
            Message::ForwardReply(ForwardReply {
                value: Value::ok(),
                applied: None,
                epoch,
                observed: None,
                settled: false,
            })
        })
    }
}

fn tls(ca: &TestCa) -> TlsMaterial {
    let (cert, key) = ca.issue(TEST_SAN);
    TlsMaterial::from_pem(
        ca.ca_pem().as_bytes(),
        cert.as_bytes(),
        key.as_bytes(),
        TEST_SAN,
    )
    .unwrap()
}

struct Setup {
    /// The real node in the middle.
    node: Arc<discus::Node>,
    node_addr: SocketAddr,
    /// The fake sender.
    sender: Transport,
    /// The fake owner that captures what the real node forwards.
    owner: Arc<Capture>,
    owner_id: NodeId,
    shutdown: CancellationToken,
    _owner_transport: Transport,
    _dir: tempfile::TempDir,
}

/// One real node with two fake members: a sender and an owner.
async fn setup() -> Setup {
    let ca = TestCa::new();
    let dir = tempfile::tempdir().unwrap();
    let path = |name: &str| dir.path().join(name).display().to_string();
    let (cert, key) = ca.issue(TEST_SAN);
    std::fs::write(path("ca.pem"), ca.ca_pem()).unwrap();
    std::fs::write(path("cert.pem"), cert).unwrap();
    std::fs::write(path("key.pem"), key).unwrap();

    let cluster_id = ClusterId::mint();
    let (node_id, sender_id, owner_id) = (NodeId::random(), NodeId::random(), NodeId::random());
    let epoch = EpochId {
        generation: 1,
        origin: node_id.min(sender_id).min(owner_id),
    };
    let fake = |id, service: Arc<dyn Service>| {
        let config = TransportConfig::new("127.0.0.1:0".parse().unwrap(), tls(&ca), id, cluster_id);
        Transport::bind(config, service)
    };
    let owner = Arc::new(Capture {
        epoch,
        seen: Mutex::new(Vec::new()),
    });
    let owner_transport = fake(owner_id, owner.clone()).await.unwrap();
    let sender_capture = Arc::new(Capture {
        epoch,
        seen: Mutex::new(Vec::new()),
    });
    let sender = fake(sender_id, sender_capture).await.unwrap();

    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let cluster_addr = socket.local_addr().unwrap();
    let argv = [
        "discus".to_string(),
        "--listen".into(),
        "127.0.0.1:0".into(),
        "--admin-listen".into(),
        "127.0.0.1:0".into(),
        "--cluster-san".into(),
        TEST_SAN.into(),
        "--membership".into(),
        "static".into(),
        "--node-id".into(),
        node_id.to_string(),
        "--cluster-id".into(),
        cluster_id.to_string(),
        "--cluster-listen".into(),
        cluster_addr.to_string(),
        "--cluster-ca".into(),
        path("ca.pem"),
        "--cluster-cert".into(),
        path("cert.pem"),
        "--cluster-key".into(),
        path("key.pem"),
        "--peer".into(),
        format!("{sender_id}@{}", sender.local_addr()),
        "--peer".into(),
        format!("{owner_id}@{}", owner_transport.local_addr()),
        "--replication-factor".into(),
        "1".into(),
        "--forward-read-timeout-ms".into(),
        "2000".into(),
        "--shutdown-timeout-secs".into(),
        "1".into(),
    ];
    let config = Config::parse_from(argv).unwrap();
    let server = Server::bind_with_cluster_socket(config, socket)
        .await
        .unwrap();
    let node = server.node().clone();
    let shutdown = CancellationToken::new();
    tokio::spawn(server.run(shutdown.clone()));
    Setup {
        node,
        node_addr: cluster_addr,
        sender,
        owner,
        owner_id,
        shutdown,
        _owner_transport: owner_transport,
        _dir: dir,
    }
}

/// A key whose primary is the fake owner.
fn owner_key(s: &Setup) -> String {
    (0..)
        .map(|i| format!("trace:{i}"))
        .find(|k| s.node.epochs().current().primary(k.as_bytes()) == Some(s.owner_id))
        .unwrap()
}

/// Send a `GET` for `key` from the fake sender to the real node, with `trace`.
async fn forward_get(s: &Setup, key: &str, trace: TraceContext) -> Value {
    let sender_id = s.sender.node_id();
    let forward = Forward {
        command: encode_command(&["GET".into(), key.to_string().into()]),
        epoch: s.node.epoch_id(),
        epoch_body: None,
        hops: 1,
        path: vec![sender_id],
        mode: ForwardMode::Route,
        track: false,
    };
    let reply = s
        .sender
        .request(
            s.node.id,
            s.node_addr,
            Message::Forward(forward),
            trace,
            Duration::from_secs(5),
            false,
        )
        .await
        .unwrap();
    let Message::ForwardReply(r) = reply.message else {
        panic!("not a reply: {reply:?}");
    };
    r.value
}

#[tokio::test]
async fn without_export_the_context_passes_through_unchanged() {
    let s = setup().await;
    let key = owner_key(&s);
    let inbound = TraceContext {
        traceparent: Some(TP.into()),
        tracestate: Some("congo=t61rcWkgMzE".into()),
    };
    assert_eq!(forward_get(&s, &key, inbound.clone()).await, Value::ok());
    assert_eq!(s.owner.seen.lock().clone(), vec![inbound]);
    s.shutdown.cancel();
}

#[tokio::test]
async fn a_malformed_context_is_not_forwarded() {
    let s = setup().await;
    let key = owner_key(&s);
    let bad = TraceContext {
        traceparent: Some("00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01".into()),
        tracestate: Some("k=v".into()),
    };
    assert_eq!(forward_get(&s, &key, bad).await, Value::ok());
    assert_eq!(s.owner.seen.lock().clone(), vec![TraceContext::default()]);
    s.shutdown.cancel();
}
