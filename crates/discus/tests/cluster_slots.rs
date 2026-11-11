//! Slot-aware routing on the redirect listener: `-MOVED` for keys that another healthy node serves
//! (spec "MOVED and forwarding").

#![allow(clippy::unwrap_used)]

// The security helpers carry the common helpers, and a module loads only once.
mod security_support;

use std::net::SocketAddr;
use std::time::Duration;

use discus_placement::slot;
use security_support::common::cluster::{start_cluster_clients, Cluster};
use security_support::common::gossip::wait_until;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Three static nodes with `--replication-factor 3`. Each has a redirect listener on a reserved
/// port and advertises it, since a static peer learns client addresses only from the list.
async fn start(extra: impl Fn(usize) -> Vec<String>) -> (Cluster, Vec<SocketAddr>) {
    start_authed(extra, None).await
}

/// [`start`], where `token` authenticates the connections that warm the cluster.
async fn start_authed(
    extra: impl Fn(usize) -> Vec<String>,
    token: Option<&str>,
) -> (Cluster, Vec<SocketAddr>) {
    let ports: Vec<SocketAddr> = (0..3)
        .map(|_| {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap()
        })
        .collect();
    let c = start_cluster_clients(
        3,
        |i| Some(ports[i]),
        |i| {
            let mut flags: Vec<String> = vec![
                "--replication-factor".into(),
                "3".into(),
                "--redirect-listen".into(),
                ports[i].to_string(),
                "--client-advertise".into(),
                ports[i].to_string(),
            ];
            flags.extend(extra(i));
            flags
        },
    )
    .await;
    warm(&c, token).await;
    (c, ports)
}

/// A node learns the epoch of a peer from a forward. Forward one write from every node to every
/// other node, so `-MOVED` does not wait for the epoch check.
async fn warm(c: &Cluster, token: Option<&str>) {
    for (i, from) in c.nodes.iter().enumerate() {
        let mut client = Raw::connect(from.addr).await;
        if let Some(token) = token {
            assert_eq!(client.cmd(&["AUTH", token]).await, "+OK");
        }
        for (j, to) in c.nodes.iter().enumerate() {
            if i != j {
                let key = c.key_on(j, &format!("warm{i}"));
                assert_eq!(client.cmd(&["SET", &key, "w"]).await, "+OK");
                let link = from.node.link().unwrap();
                assert!(link.peer_epoch(to.id).is_some());
            }
        }
    }
}

fn wire(parts: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend(format!("${}\r\n{p}\r\n", p.len()).into_bytes());
    }
    out
}

/// The length of the first complete RESP2 reply in `buf`.
fn reply_len(buf: &[u8]) -> Option<usize> {
    let end = buf.windows(2).position(|w| w == b"\r\n")?;
    let line = std::str::from_utf8(&buf[1..end]).ok()?;
    let head = end + 2;
    match buf.first()? {
        b'$' => {
            let n: i64 = line.parse().ok()?;
            let total = if n < 0 { head } else { head + n as usize + 2 };
            (buf.len() >= total).then_some(total)
        }
        b'*' => {
            let n: i64 = line.parse().ok()?;
            let mut at = head;
            for _ in 0..n.max(0) {
                at += reply_len(&buf[at..])?;
            }
            Some(at)
        }
        _ => Some(head),
    }
}

/// A raw RESP connection that reads whole replies.
struct Raw {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Raw {
    async fn connect(addr: SocketAddr) -> Raw {
        Raw {
            stream: TcpStream::connect(addr).await.unwrap(),
            buf: Vec::new(),
        }
    }

    /// Send `wire` and read `n` replies, each without its final CRLF.
    async fn send(&mut self, wire: &[u8], n: usize) -> Vec<String> {
        self.stream.write_all(wire).await.unwrap();
        let mut out = Vec::new();
        let read = async {
            while out.len() < n {
                if let Some(len) = reply_len(&self.buf) {
                    let reply: Vec<u8> = self.buf.drain(..len).collect();
                    out.push(String::from_utf8_lossy(&reply[..len - 2]).into_owned());
                    continue;
                }
                let mut chunk = [0u8; 4096];
                let got = self.stream.read(&mut chunk).await.unwrap();
                assert!(got > 0, "the connection closed");
                self.buf.extend_from_slice(&chunk[..got]);
            }
        };
        tokio::time::timeout(Duration::from_secs(30), read)
            .await
            .expect("the replies never came");
        out
    }

    async fn cmd(&mut self, parts: &[&str]) -> String {
        self.send(&wire(parts), 1).await.remove(0)
    }
}

fn moved(key: &str, to: SocketAddr) -> String {
    format!("-MOVED {} 127.0.0.1:{}", slot(key.as_bytes()), to.port())
}

#[tokio::test]
async fn moved_points_to_the_primary() {
    let (c, ports) = start(|_| vec![]).await;
    let key = c.key_on(1, "moved");
    let mut redirect = Raw::connect(ports[0]).await;
    assert_eq!(
        redirect.cmd(&["SET", &key, "v"]).await,
        moved(&key, ports[1])
    );
    let mut forward = Raw::connect(c.nodes[0].addr).await;
    assert_eq!(forward.cmd(&["SET", &key, "v"]).await, "+OK");
}

#[tokio::test]
async fn a_pipeline_keeps_its_order() {
    let (c, ports) = start(|_| vec![]).await;
    let mut wires = Vec::new();
    let mut want = Vec::new();
    for i in 0..5 {
        let here = c.key_on(0, &format!("here{i}"));
        let there = c.key_on(1, &format!("there{i}"));
        wires.extend(wire(&["SET", &here, "v"]));
        want.push("+OK".to_string());
        wires.extend(wire(&["SET", &there, "v"]));
        want.push(moved(&there, ports[1]));
    }
    let mut redirect = Raw::connect(ports[0]).await;
    assert_eq!(redirect.send(&wires, 10).await, want);
}

#[tokio::test]
async fn readonly_serves_on_a_secondary() {
    let (c, ports) = start(|_| vec![]).await;
    let key = c.key_on(1, "ro");
    let mut forward = Raw::connect(c.nodes[0].addr).await;
    assert_eq!(forward.cmd(&["SET", &key, "v"]).await, "+OK");
    assert!(wait_until(Duration::from_secs(10), || c.nodes[0].holds(key.as_bytes())).await);
    let mut redirect = Raw::connect(ports[0]).await;
    assert_eq!(redirect.cmd(&["GET", &key]).await, moved(&key, ports[1]));
    assert_eq!(redirect.cmd(&["READONLY"]).await, "+OK");
    assert_eq!(redirect.cmd(&["GET", &key]).await, "$1\r\nv");
    assert_eq!(redirect.cmd(&["READWRITE"]).await, "+OK");
    assert_eq!(redirect.cmd(&["GET", &key]).await, moved(&key, ports[1]));
}

#[tokio::test]
async fn readonly_without_replica_reads_still_moves() {
    let (c, ports) = start(|_| vec!["--read-from-replicas".into(), "false".into()]).await;
    let key = c.key_on(1, "ro");
    let mut redirect = Raw::connect(ports[0]).await;
    assert_eq!(redirect.cmd(&["GET", &key]).await, moved(&key, ports[1]));
    assert_eq!(redirect.cmd(&["READONLY"]).await, "+OK");
    assert_eq!(redirect.cmd(&["GET", &key]).await, moved(&key, ports[1]));
    assert_eq!(redirect.cmd(&["ASKING"]).await, "+OK");
}

#[tokio::test]
async fn a_stopped_static_peer_gets_no_moved() {
    let (c, ports) = start(|_| vec!["--hold-budget-ms".into(), "500".into()]).await;
    let (a, b) = (&c.nodes[0], &c.nodes[1]);
    let key = c.key_on(1, "gone");
    let mut redirect = Raw::connect(ports[0]).await;
    assert_eq!(
        redirect.cmd(&["SET", &key, "v"]).await,
        moved(&key, ports[1])
    );

    b.kill();
    let replicator = a.node.replicator().unwrap();
    let mut forward = Raw::connect(a.addr).await;
    let mut i = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !replicator.peer_down(b.id) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the queue to B never went down"
        );
        let local = c.key_on(0, &format!("fill{i}"));
        forward.cmd(&["SET", &local, "v"]).await;
        i += 1;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let reply = redirect.cmd(&["SET", &key, "v"]).await;
    assert!(
        reply.starts_with('-') && !reply.starts_with("-MOVED"),
        "{reply:?}"
    );
}

#[tokio::test]
async fn counter_keys_never_move() {
    let (_c, ports) = start(|_| vec![]).await;
    for port in &ports {
        let mut redirect = Raw::connect(*port).await;
        for k in 0..6 {
            let key = format!("cnt:slots{k}");
            let reply = redirect.cmd(&["INCR", &key]).await;
            assert!(reply.starts_with(':'), "{key}: {reply:?}");
        }
    }
}

#[tokio::test]
async fn one_slot_multi_key_moves_and_cross_slot_fans_out() {
    let (c, ports) = start(|_| vec![]).await;
    let primary = c
        .nodes
        .iter()
        .position(|n| n.id == c.nodes[0].primary(b"{t}"))
        .unwrap();
    let other = (primary + 1) % 3;
    let mut redirect = Raw::connect(ports[other]).await;
    let reply = redirect.cmd(&["MSET", "{t}a", "1", "{t}b", "2"]).await;
    assert_eq!(
        reply,
        format!(
            "-MOVED {} 127.0.0.1:{}",
            slot(b"{t}"),
            ports[primary].port()
        )
    );

    let keys: Vec<String> = (0..3).map(|i| c.key_on(i, "fan")).collect();
    let mut forward = Raw::connect(c.nodes[0].addr).await;
    for k in &keys {
        assert_eq!(forward.cmd(&["SET", k, k]).await, "+OK");
    }
    let mut parts = vec!["MGET"];
    parts.extend(keys.iter().map(String::as_str));
    let got = redirect.cmd(&parts).await;
    let want: String = std::iter::once("*3".to_string())
        .chain(keys.iter().map(|k| format!("${}\r\n{k}", k.len())))
        .collect::<Vec<_>>()
        .join("\r\n");
    assert_eq!(got, want);
}

#[cfg(feature = "oauth2")]
#[tokio::test]
async fn authorization_comes_before_moved() {
    use security_support::idp::MockIdp;
    use security_support::{write_file, TestCa};

    let ca = TestCa::new();
    let idp = MockIdp::start(&ca).await;
    let file = write_file(&ca, "auth.toml", &idp.toml(&ca, ""));
    let writer = idp.token("discus.write");
    let (c, ports) = start_authed(
        |_| {
            [
                "--auth-policy",
                "any:oauth2",
                "--dev-insecure-oauth2",
                "--auth-config",
                &file,
            ]
            .map(String::from)
            .to_vec()
        },
        Some(&writer),
    )
    .await;
    let key = c.key_on(1, "perm");
    let mut redirect = Raw::connect(ports[0]).await;
    assert_eq!(
        redirect.cmd(&["AUTH", &idp.token("discus.read")]).await,
        "+OK"
    );
    let reply = redirect.cmd(&["SET", &key, "v"]).await;
    assert!(reply.starts_with("-NOPERM"), "{reply:?}");
    assert_eq!(redirect.cmd(&["GET", &key]).await, moved(&key, ports[1]));
}
