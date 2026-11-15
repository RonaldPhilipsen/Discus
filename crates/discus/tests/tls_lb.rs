//! The `lb` TLS mode: PROXY protocol from a trusted load balancer over mTLS (spec §14.1).
#![allow(clippy::unwrap_used)]
mod security_support;

use ppp::v2::{Builder, Command, IPv4, Protocol, Version};
use security_support::{cmd, exchange, start, tls_over, try_start, CertFiles, TestCa};
use tokio::io::AsyncWriteExt;

fn lb_args<'a>(ca: &'a TestCa, server: &'a CertFiles, source: &'a str) -> Vec<&'a str> {
    vec![
        "--tls-mode",
        "lb",
        "--tls-cert",
        server.cert_str(),
        "--tls-key",
        server.key_str(),
        "--tls-client-ca",
        ca.ca_str(),
        "--lb-trusted-source",
        source,
    ]
}

/// A PROXY v2 header for client 10.1.2.3, with an SSL TLV when `cn` is set.
fn header(cn: Option<&str>) -> Vec<u8> {
    match cn {
        Some(cn) => header_with(&[(0x20, ssl_tlv(0x01 | 0x02, 0, Some(cn)))]),
        None => header_with(&[]),
    }
}

/// A PROXY v2 header for client 10.1.2.3 with these TLVs.
fn header_with(tlvs: &[(u8, Vec<u8>)]) -> Vec<u8> {
    let mut b = Builder::with_addresses(
        Version::Two | Command::Proxy,
        Protocol::Stream,
        IPv4::new([10, 1, 2, 3], [10, 0, 0, 1], 51000, 6379),
    );
    for (kind, value) in tlvs {
        b = b.write_tlv(*kind, value).unwrap();
    }
    b.build().unwrap()
}

/// The value of a `PP2_TYPE_SSL` TLV, with a CN sub-TLV when `cn` is set.
fn ssl_tlv(client: u8, verify: u32, cn: Option<&str>) -> Vec<u8> {
    let mut v = vec![client];
    v.extend_from_slice(&verify.to_be_bytes());
    if let Some(cn) = cn {
        v.push(0x22);
        v.extend_from_slice(&(cn.len() as u16).to_be_bytes());
        v.extend_from_slice(cn.as_bytes());
    }
    v
}

/// Connect as the load balancer: PROXY header in clear text, then TLS.
async fn via_lb(
    node: &security_support::Node,
    ca: &TestCa,
    lb_cert: Option<&CertFiles>,
    proxy: &[u8],
    wire: &[u8],
) -> String {
    let mut tcp = tokio::net::TcpStream::connect(node.addr).await.unwrap();
    if tcp.write_all(proxy).await.is_err() {
        return String::new();
    }
    match tls_over(tcp, ca, lb_cert).await {
        Ok(mut tls) => exchange(&mut tls, wire).await,
        Err(_) => String::new(),
    }
}

#[tokio::test]
async fn a_trusted_load_balancer_forwards_the_client_identity() {
    let ca = TestCa::new();
    let server = ca.server_cert();
    let lb = ca.client_cert("spiffe://test/lb");
    let mut args = lb_args(&ca, &server, "127.0.0.1/32");
    args.extend(["--auth-policy", "any:mtls"]);
    let node = start(&args).await;
    let reply = via_lb(&node, &ca, Some(&lb), &header(Some("app")), &cmd(&["PING"])).await;
    assert_eq!(reply, "+PONG\r\n");
}

#[tokio::test]
async fn a_load_balancer_without_its_certificate_is_rejected() {
    let ca = TestCa::new();
    let server = ca.server_cert();
    let mut args = lb_args(&ca, &server, "127.0.0.1/32");
    args.extend(["--auth-policy", "any:mtls"]);
    let node = start(&args).await;
    let reply = via_lb(&node, &ca, None, &header(Some("app")), &cmd(&["PING"])).await;
    assert_eq!(reply, "");
}

#[tokio::test]
async fn lb_san_pins_the_load_balancer_certificate() {
    let ca = TestCa::new();
    let server = ca.server_cert();
    let lb = ca.client_cert("spiffe://test/lb");
    let other = ca.client_cert("spiffe://test/app");
    let mut args = lb_args(&ca, &server, "127.0.0.1/32");
    args.extend(["--auth-policy", "any:mtls", "--lb-san", "spiffe://test/lb"]);
    let node = start(&args).await;
    let reply = via_lb(&node, &ca, Some(&lb), &header(Some("app")), &cmd(&["PING"])).await;
    assert_eq!(reply, "+PONG\r\n");
    let reply = via_lb(
        &node,
        &ca,
        Some(&other),
        &header(Some("app")),
        &cmd(&["PING"]),
    )
    .await;
    assert_eq!(reply, "");
}

#[tokio::test]
async fn an_untrusted_source_is_closed_before_the_header_is_read() {
    let ca = TestCa::new();
    let server = ca.server_cert();
    let lb = ca.client_cert("spiffe://test/lb");
    let mut args = lb_args(&ca, &server, "10.0.0.0/8");
    args.extend(["--auth-policy", "any:mtls"]);
    let node = start(&args).await;
    let reply = via_lb(&node, &ca, Some(&lb), &header(Some("app")), &cmd(&["PING"])).await;
    assert_eq!(reply, "");
}

#[tokio::test]
async fn a_header_without_a_client_certificate_gives_no_mtls_factor() {
    let ca = TestCa::new();
    let server = ca.server_cert();
    let lb = ca.client_cert("spiffe://test/lb");
    let mut args = lb_args(&ca, &server, "127.0.0.1/32");
    args.extend(["--auth-policy", "any:mtls"]);
    let node = start(&args).await;
    let reply = via_lb(&node, &ca, Some(&lb), &header(None), &cmd(&["PING"])).await;
    assert!(reply.starts_with("-NOAUTH"), "{reply:?}");
}

#[tokio::test]
async fn a_forwarded_identity_follows_the_client_cert_rules() {
    let ca = TestCa::new();
    let server = ca.server_cert();
    let lb = ca.client_cert("spiffe://test/lb");
    let file = ca.path("auth.toml");
    std::fs::write(
        &file,
        r#"
[[client_cert]]
identity = "ro-*"
scopes = ["discus.read"]
"#,
    )
    .unwrap();
    let mut args = lb_args(&ca, &server, "127.0.0.1/32");
    args.extend([
        "--auth-policy",
        "any:mtls",
        "--auth-config",
        file.to_str().unwrap(),
    ]);
    let node = start(&args).await;
    let mut wire = cmd(&["SET", "k", "v"]);
    wire.extend(cmd(&["GET", "k"]));
    let matched = via_lb(&node, &ca, Some(&lb), &header(Some("ro-reporting")), &wire).await;
    assert!(matched.starts_with("-NOPERM"), "{matched:?}");
    assert!(matched.ends_with("$-1\r\n"), "{matched:?}");
    let unmatched = via_lb(&node, &ca, Some(&lb), &header(Some("app")), &cmd(&["PING"])).await;
    assert!(unmatched.starts_with("-NOAUTH"), "{unmatched:?}");
}

async fn lb_identity_node(ca: &TestCa, server: &CertFiles) -> security_support::Node {
    let mut args = lb_args(ca, server, "127.0.0.1/32");
    args.extend(["--auth-policy", "any:mtls", "--lb-identity-tlv", "0xE0"]);
    start(&args).await
}

#[tokio::test]
async fn a_custom_identity_tlv_meets_mtls() {
    let ca = TestCa::new();
    let server = ca.server_cert();
    let lb = ca.client_cert("spiffe://test/lb");
    let node = lb_identity_node(&ca, &server).await;
    let alone = header_with(&[(0xE0, b"spiffe://x/app".to_vec())]);
    let reply = via_lb(&node, &ca, Some(&lb), &alone, &cmd(&["PING"])).await;
    assert_eq!(reply, "+PONG\r\n");
    let verified = header_with(&[
        (0x20, ssl_tlv(0x01 | 0x02, 0, None)),
        (0xE0, b"spiffe://x/app".to_vec()),
    ]);
    let reply = via_lb(&node, &ca, Some(&lb), &verified, &cmd(&["PING"])).await;
    assert_eq!(reply, "+PONG\r\n");
}

#[tokio::test]
async fn an_empty_custom_identity_tlv_gives_no_mtls_factor() {
    let ca = TestCa::new();
    let server = ca.server_cert();
    let lb = ca.client_cert("spiffe://test/lb");
    let node = lb_identity_node(&ca, &server).await;
    let empty = header_with(&[(0xE0, Vec::new())]);
    let reply = via_lb(&node, &ca, Some(&lb), &empty, &cmd(&["PING"])).await;
    assert!(reply.starts_with("-NOAUTH"), "{reply:?}");
}

#[tokio::test]
async fn a_custom_identity_tlv_next_to_an_unverified_certificate_gives_no_mtls_factor() {
    let ca = TestCa::new();
    let server = ca.server_cert();
    let lb = ca.client_cert("spiffe://test/lb");
    let node = lb_identity_node(&ca, &server).await;
    for ssl in [ssl_tlv(0x01 | 0x02, 1, Some("app")), ssl_tlv(0x01, 0, None)] {
        let unverified = header_with(&[(0x20, ssl), (0xE0, b"spiffe://x/app".to_vec())]);
        let reply = via_lb(&node, &ca, Some(&lb), &unverified, &cmd(&["PING"])).await;
        assert!(reply.starts_with("-NOAUTH"), "{reply:?}");
    }
}

#[tokio::test]
async fn a_v1_header_with_a_password_works() {
    let ca = TestCa::new();
    let server = ca.server_cert();
    let lb = ca.client_cert("spiffe://test/lb");
    let mut args = lb_args(&ca, &server, "127.0.0.1/32");
    args.extend([
        "--auth-policy",
        "all:password",
        "--acl-file",
        security_support::common::client_tls::password_file("s3cret"),
    ]);
    let node = start(&args).await;
    let mut wire = cmd(&["AUTH", "s3cret"]);
    wire.extend(cmd(&["PING"]));
    let v1 = b"PROXY TCP4 10.1.2.3 10.0.0.1 51000 6379\r\n";
    assert_eq!(
        via_lb(&node, &ca, Some(&lb), v1, &wire).await,
        "+OK\r\n+PONG\r\n"
    );
}

#[tokio::test]
async fn garbage_instead_of_a_header_is_closed() {
    let ca = TestCa::new();
    let server = ca.server_cert();
    let lb = ca.client_cert("spiffe://test/lb");
    let mut args = lb_args(&ca, &server, "127.0.0.1/32");
    args.extend(["--auth-policy", "any:mtls"]);
    let node = start(&args).await;
    let reply = via_lb(&node, &ca, Some(&lb), b"HELLO\r\n", &cmd(&["PING"])).await;
    assert_eq!(reply, "");
}

#[tokio::test]
async fn lb_mode_needs_a_trusted_source_and_a_ca() {
    let ca = TestCa::new();
    let server = ca.server_cert();
    let args = [
        "--tls-mode",
        "lb",
        "--tls-cert",
        server.cert_str(),
        "--tls-key",
        server.key_str(),
        "--tls-client-ca",
        ca.ca_str(),
        "--auth-policy",
        "any:mtls",
    ];
    let e = try_start(&args).await.err().unwrap();
    assert!(e.contains("--lb-trusted-source"), "{e}");
}
