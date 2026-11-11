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
    let ports: Vec<SocketAddr> = (0..3).map(|_| reserve()).collect();
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

/// A free port for a redirect listener that binds later. The port comes from below the
/// ephemeral range, so the connections and `:0` listeners of parallel tests do not take it in the
/// meantime.
fn reserve() -> SocketAddr {
    loop {
        let port = 20000 + (discus_placement::NodeId::random().as_u128() % 12000) as u16;
        if let Ok(l) = std::net::TcpListener::bind(("127.0.0.1", port)) {
            return l.local_addr().unwrap();
        }
    }
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

/// Write 3000 keys through a forward port. Return how many keys every node holds once
/// replication settles, which includes the keys that `warm` wrote.
async fn fill(c: &Cluster) -> usize {
    let mut forward = Raw::connect(c.nodes[0].addr).await;
    for i in 0..3000 {
        assert_eq!(forward.cmd(&["SET", &format!("k{i}"), "v"]).await, "+OK");
    }
    let total = c.nodes[0].node.store.len();
    assert!(
        wait_until(Duration::from_secs(30), || {
            c.nodes.iter().all(|n| n.node.store.len() == total)
        })
        .await
    );
    total
}

#[tokio::test]
async fn dbsize_over_redirect_ports_sums_to_the_estimate() {
    let (c, ports) = start(|_| vec![]).await;
    fill(&c).await;
    let mut forward = Raw::connect(c.nodes[0].addr).await;
    let want = forward.cmd(&["DBSIZE"]).await[1..].parse::<i64>().unwrap();
    let mut sum = 0i64;
    for p in &ports {
        let mut r = Raw::connect(*p).await;
        sum += r.cmd(&["DBSIZE"]).await[1..].parse::<i64>().unwrap();
    }
    assert!(
        (sum - want).abs() <= 3,
        "the redirect ports sum to {sum}, not {want}"
    );
}

#[tokio::test]
async fn scan_over_primaries_lists_each_key_once() {
    let (c, ports) = start(|_| vec![]).await;
    let total = fill(&c).await;
    let mut seen = std::collections::HashSet::new();
    for p in &ports {
        let mut r = Raw::connect(*p).await;
        let mut cursor = "0".to_string();
        loop {
            let reply = r.cmd(&["SCAN", &cursor, "COUNT", "100"]).await;
            let parts: Vec<&str> = reply.split("\r\n").collect();
            cursor = parts[2].to_string();
            // A page can hold no keys: `*0` ends the reply after the cursor.
            for key in parts.get(5..).unwrap_or_default().iter().step_by(2) {
                assert!(seen.insert(key.to_string()), "{key} listed twice");
            }
            if cursor == "0" {
                break;
            }
        }
    }
    assert_eq!(seen.len(), total);
}

/// A redis-rs connection to `addr`, in RESP3 when `resp3`.
async fn conn(addr: SocketAddr, resp3: bool) -> redis::aio::MultiplexedConnection {
    let url = if resp3 {
        format!("redis://{addr}/?protocol=resp3")
    } else {
        format!("redis://{addr}")
    };
    let config =
        redis::AsyncConnectionConfig::new().set_response_timeout(Some(Duration::from_secs(30)));
    redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection_with_config(&config)
        .await
        .unwrap()
}

async fn query(c: &mut redis::aio::MultiplexedConnection, parts: &[&str]) -> redis::Value {
    let mut cmd = redis::cmd(parts[0]);
    for p in &parts[1..] {
        cmd.arg(*p);
    }
    cmd.query_async(c).await.unwrap()
}

fn text(v: &redis::Value) -> String {
    match v {
        redis::Value::BulkString(b) => String::from_utf8(b.clone()).unwrap(),
        redis::Value::SimpleString(s) => s.clone(),
        redis::Value::VerbatimString { text, .. } => text.clone(),
        other => panic!("not a string: {other:?}"),
    }
}

fn int(v: &redis::Value) -> i64 {
    match v {
        redis::Value::Int(n) => *n,
        other => panic!("not an integer: {other:?}"),
    }
}

fn array(v: &redis::Value) -> &[redis::Value] {
    match v {
        redis::Value::Array(a) => a,
        other => panic!("not an array: {other:?}"),
    }
}

/// One `CLUSTER SLOTS` entry: start, end, and each owner as (host, port, id).
type Entry = (u16, u16, Vec<(String, u16, String)>);

fn parse_slots(v: &redis::Value) -> Vec<Entry> {
    array(v)
        .iter()
        .map(|entry| {
            let entry = array(entry);
            let owners = entry[2..]
                .iter()
                .map(|n| {
                    let n = array(n);
                    (text(&n[0]), int(&n[1]) as u16, text(&n[2]))
                })
                .collect();
            (int(&entry[0]) as u16, int(&entry[1]) as u16, owners)
        })
        .collect()
}

fn redis_id(id: discus_placement::NodeId) -> String {
    format!("{id}00000000")
}

#[tokio::test]
async fn slots_cover_every_slot_once_and_agree() {
    let (c, ports) = start(|_| vec![]).await;
    let port_of = |id: &str| {
        let i = c.nodes.iter().position(|n| redis_id(n.id) == id).unwrap();
        ports[i].port()
    };
    let epoch = c.nodes[0].node.epochs().current().clone();
    let mut replies = Vec::new();
    for p in &ports {
        let mut r = conn(*p, false).await;
        let entries = parse_slots(&query(&mut r, &["CLUSTER", "SLOTS"]).await);
        let mut next = 0u32;
        for (start, end, owners) in &entries {
            assert_eq!(
                u32::from(*start),
                next,
                "a gap or an overlap before {start}"
            );
            next = u32::from(*end) + 1;
            for s in *start..=*end {
                let want: Vec<String> = epoch.slot_owners(s, 3).into_iter().map(redis_id).collect();
                let got: Vec<String> = owners.iter().map(|o| o.2.clone()).collect();
                assert_eq!(got, want, "slot {s}");
            }
            for (host, port, id) in owners {
                assert_eq!(host, "127.0.0.1");
                assert_eq!(*port, port_of(id));
            }
        }
        assert_eq!(next, 16384);
        replies.push(entries);
    }
    assert_eq!(replies[0], replies[1]);
    assert_eq!(replies[0], replies[2]);
}

#[tokio::test]
async fn resp3_topology_parses() {
    let (_c, ports) = start(|_| vec![]).await;
    let mut r = conn(ports[0], true).await;
    let slots = query(&mut r, &["CLUSTER", "SLOTS"]).await;
    for entry in array(&slots) {
        for node in &array(entry)[2..] {
            assert!(matches!(array(node)[3], redis::Value::Map(_)), "{node:?}");
        }
    }
    assert_eq!(
        parse_slots(&slots)
            .iter()
            .map(|e| u32::from(e.1 - e.0) + 1)
            .sum::<u32>(),
        16384
    );
    let shards = query(&mut r, &["CLUSTER", "SHARDS"]).await;
    for shard in array(&shards) {
        let redis::Value::Map(fields) = shard else {
            panic!("not a map: {shard:?}");
        };
        assert_eq!(text(&fields[0].0), "slots");
        assert_eq!(text(&fields[1].0), "nodes");
        for node in array(&fields[1].1) {
            assert!(matches!(node, redis::Value::Map(_)), "{node:?}");
        }
    }

    let urls: Vec<String> = ports.iter().map(|p| format!("redis://{p}")).collect();
    let client = redis::cluster::ClusterClientBuilder::new(urls)
        .use_protocol(redis::ProtocolVersion::RESP3)
        .response_timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let mut cluster = client.get_async_connection().await.unwrap();
    let _: () = redis::AsyncCommands::set(&mut cluster, "resp3", "v")
        .await
        .unwrap();
    let got: String = redis::AsyncCommands::get(&mut cluster, "resp3")
        .await
        .unwrap();
    assert_eq!(got, "v");
}

#[tokio::test]
async fn shards_nodes_info_pass_format_checks() {
    let (c, ports) = start(|_| vec![]).await;
    let mut r = conn(ports[1], false).await;
    let info = text(&query(&mut r, &["CLUSTER", "INFO"]).await);
    for line in [
        "cluster_state:ok",
        "cluster_slots_assigned:16384",
        "cluster_known_nodes:3",
    ] {
        assert!(info.contains(&format!("{line}\r\n")), "{line} in {info}");
    }
    let nodes = text(&query(&mut r, &["CLUSTER", "NODES"]).await);
    let lines: Vec<&str> = nodes.lines().collect();
    assert_eq!(lines.len(), 3, "{nodes}");
    for line in &lines {
        let fields: Vec<&str> = line.split(' ').collect();
        assert!(fields.len() >= 9, "{line}");
        if fields[0] == redis_id(c.nodes[1].id) {
            assert_eq!(fields[2], "myself,master");
            assert!(fields[1].starts_with(&format!("127.0.0.1:{}@", ports[1].port())));
        } else {
            assert_eq!(fields[2], "master");
        }
    }
    let shards = query(&mut r, &["CLUSTER", "SHARDS"]).await;
    let mut covered = 0;
    for shard in array(&shards) {
        let shard = array(shard);
        assert_eq!(text(&shard[0]), "slots");
        for pair in array(&shard[1]).chunks(2) {
            covered += int(&pair[1]) - int(&pair[0]) + 1;
        }
        assert_eq!(text(&shard[2]), "nodes");
        assert_eq!(array(&shard[3]).len(), 3);
    }
    assert_eq!(covered, 16384);
}

#[tokio::test]
async fn keyslot_and_myid() {
    let (c, ports) = start(|_| vec![]).await;
    let mut r = Raw::connect(ports[2]).await;
    assert_eq!(r.cmd(&["CLUSTER", "KEYSLOT", "foo"]).await, ":12182");
    let id = r.cmd(&["CLUSTER", "MYID"]).await;
    assert_eq!(id, format!("$40\r\n{}", redis_id(c.nodes[2].id)));
}

#[tokio::test]
async fn unknown_subcommand_is_refused() {
    let (_c, ports) = start(|_| vec![]).await;
    let mut r = Raw::connect(ports[0]).await;
    assert_eq!(
        r.cmd(&["CLUSTER", "MEET", "10.0.0.1", "7000"]).await,
        "-ERR Discus places slots by itself. CLUSTER MEET is not supported"
    );
}

fn forwards(c: &Cluster) -> Vec<u64> {
    c.nodes
        .iter()
        .map(|n| {
            n.node
                .faults
                .forwards
                .load(std::sync::atomic::Ordering::Relaxed)
        })
        .collect()
}

#[tokio::test]
async fn cluster_client_routes_directly() {
    let (c, ports) = start(|_| vec![]).await;
    let before = forwards(&c);
    let urls: Vec<String> = ports.iter().map(|p| format!("redis://{p}")).collect();
    let client = redis::cluster::ClusterClientBuilder::new(urls)
        .response_timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let mut cluster = client.get_async_connection().await.unwrap();
    for i in 0..1000 {
        let key = format!("direct{i}");
        let _: () = redis::AsyncCommands::set(&mut cluster, &key, i)
            .await
            .unwrap();
    }
    for i in 0..1000 {
        let got: i64 = redis::AsyncCommands::get(&mut cluster, format!("direct{i}"))
            .await
            .unwrap();
        assert_eq!(got, i);
    }
    assert_eq!(forwards(&c), before);
}

#[tokio::test]
async fn topology_needs_auth() {
    let (_c, ports) = start_authed(
        |_| vec!["--shared-token".into(), "s3cret".into()],
        Some("s3cret"),
    )
    .await;
    let mut r = Raw::connect(ports[0]).await;
    let reply = r.cmd(&["CLUSTER", "SLOTS"]).await;
    assert!(reply.starts_with("-NOAUTH"), "{reply:?}");
}

#[tokio::test]
async fn single_node_answers_one_range() {
    let port = reserve();
    let node = security_support::common::try_start_full(&["--redirect-listen", &port.to_string()])
        .await
        .unwrap();
    let mut r = conn(port, false).await;
    let id = text(&query(&mut r, &["CLUSTER", "MYID"]).await);
    let entries = parse_slots(&query(&mut r, &["CLUSTER", "SLOTS"]).await);
    assert_eq!(
        entries,
        vec![(0, 16383, vec![("127.0.0.1".to_string(), port.port(), id)])]
    );

    let client = redis::cluster::ClusterClientBuilder::new(vec![format!("redis://{port}")])
        .response_timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let mut cluster = client.get_async_connection().await.unwrap();
    for i in 0..20 {
        let reply: String = redis::cmd("SET")
            .arg(format!("alone{i}"))
            .arg("v")
            .query_async(&mut cluster)
            .await
            .unwrap();
        assert_eq!(reply, "OK");
    }
    drop(node);
}

#[tokio::test]
async fn info_has_a_cluster_section() {
    let (_c, ports) = start(|_| vec![]).await;
    let mut r = conn(ports[0], false).await;
    let info = text(&query(&mut r, &["INFO", "cluster"]).await);
    assert!(
        info.contains("# Cluster\r\ncluster_enabled:1\r\n"),
        "{info}"
    );
    assert!(!info.contains("# discus"), "{info}");
    let discus = text(&query(&mut r, &["INFO", "discus"]).await);
    assert!(
        discus.contains(&format!("discus_redirect_addr:{}\r\n", ports[0])),
        "{discus}"
    );
    assert!(
        discus.contains(&format!("discus_client_addr:{}\r\n", ports[0])),
        "{discus}"
    );

    let plain = security_support::common::start_node(&[]).await;
    let mut p = conn(plain, false).await;
    let info = text(&query(&mut p, &["INFO", "cluster"]).await);
    assert!(info.contains("cluster_enabled:0\r\n"), "{info}");
}
