//! The `lb` TLS mode: PROXY protocol from a trusted load balancer over mTLS (spec §14.1).
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
    let mut b = Builder::with_addresses(
        Version::Two | Command::Proxy,
        Protocol::Stream,
        IPv4::new([10, 1, 2, 3], [10, 0, 0, 1], 51000, 6379),
    );
    if let Some(cn) = cn {
        let mut v = vec![0x01 | 0x02, 0, 0, 0, 0, 0x22];
        v.extend_from_slice(&(cn.len() as u16).to_be_bytes());
        v.extend_from_slice(cn.as_bytes());
        b = b.write_tlv(0x20u8, &v).unwrap();
    }
    b.build().unwrap()
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
async fn a_v1_header_with_tls_and_the_shared_token_works() {
    let ca = TestCa::new();
    let server = ca.server_cert();
    let lb = ca.client_cert("spiffe://test/lb");
    let mut args = lb_args(&ca, &server, "127.0.0.1/32");
    args.extend([
        "--auth-policy",
        "all:tls,shared_token",
        "--shared-token",
        "s3cret",
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
