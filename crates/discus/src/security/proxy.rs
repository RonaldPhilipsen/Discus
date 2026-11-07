//! PROXY protocol v1 and v2 from the load balancer (spec §14.1, `lb` mode).
//!
//! `ppp` parses the header. This module frames it: it reads exactly the header bytes, because
//! the TLS handshake follows on the same socket.

use std::io;
use std::net::SocketAddr;

use ppp::v2::TypeLengthValues;
use tokio::io::{AsyncRead, AsyncReadExt};

const V2_SIGNATURE: &[u8; 12] = b"\r\n\r\n\0\r\nQUIT\n";
/// The longest v1 line, including `\r\n`.
const V1_MAX: usize = 107;
/// The largest v2 body that Discus accepts.
const V2_MAX_BODY: usize = 4096;

const PP2_TYPE_SSL: u8 = 0x20;
const PP2_SUBTYPE_SSL_CN: u8 = 0x22;
const PP2_CLIENT_SSL: u8 = 0x01;
const PP2_CLIENT_CERT_CONN: u8 = 0x02;
const PP2_CLIENT_CERT_SESS: u8 = 0x04;

/// What the load balancer says about the client.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProxyHeader {
    /// The client address. `None` for `LOCAL` and `UNKNOWN`.
    pub source: Option<SocketAddr>,
    /// From the SSL TLV: whether the client used TLS. `None` without the TLV.
    pub client_tls: Option<bool>,
    /// The client presented a certificate that the load balancer verified.
    pub client_cert_verified: bool,
    /// The client identity: the custom identity TLV, else the CN of a verified certificate.
    pub identity: Option<String>,
}

fn bad(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("PROXY header: {msg}"))
}

/// Read one PROXY header and nothing after it. `identity_tlv` is the custom TLV type that
/// carries the client identity, if any.
pub async fn read_proxy_header<R: AsyncRead + Unpin>(
    r: &mut R,
    identity_tlv: Option<u8>,
) -> io::Result<ProxyHeader> {
    let mut buf = vec![r.read_u8().await?];
    match buf[0] {
        b'P' => loop {
            buf.push(r.read_u8().await?);
            if buf.ends_with(b"\r\n") {
                break;
            }
            if buf.len() >= V1_MAX {
                return Err(bad("the v1 line is too long"));
            }
        },
        b'\r' => {
            buf.resize(16, 0);
            r.read_exact(&mut buf[1..]).await?;
            if &buf[..12] != V2_SIGNATURE {
                return Err(bad("bad v2 signature"));
            }
            let len = u16::from_be_bytes([buf[14], buf[15]]) as usize;
            if len > V2_MAX_BODY {
                return Err(bad("the v2 header is too long"));
            }
            buf.resize(16 + len, 0);
            r.read_exact(&mut buf[16..]).await?;
        }
        _ => return Err(bad("no PROXY header")),
    }
    parse(&buf, identity_tlv)
}

fn parse(buf: &[u8], identity_tlv: Option<u8>) -> io::Result<ProxyHeader> {
    let mut out = ProxyHeader::default();
    match ppp::HeaderResult::parse(buf) {
        ppp::HeaderResult::V1(Ok(h)) => {
            out.source = match h.addresses {
                ppp::v1::Addresses::Tcp4(a) => Some((a.source_address, a.source_port).into()),
                ppp::v1::Addresses::Tcp6(a) => Some((a.source_address, a.source_port).into()),
                ppp::v1::Addresses::Unknown => None,
            };
        }
        ppp::HeaderResult::V2(Ok(h)) => {
            if h.command == ppp::v2::Command::Proxy {
                out.source = match h.addresses {
                    ppp::v2::Addresses::IPv4(a) => Some((a.source_address, a.source_port).into()),
                    ppp::v2::Addresses::IPv6(a) => Some((a.source_address, a.source_port).into()),
                    _ => None,
                };
            }
            let mut cn = None;
            let mut custom = None;
            for tlv in h.tlvs() {
                let tlv = tlv.map_err(|e| bad(&e.to_string()))?;
                if tlv.kind == PP2_TYPE_SSL {
                    cn = ssl(&tlv.value, &mut out)?;
                } else if Some(tlv.kind) == identity_tlv {
                    let id = std::str::from_utf8(&tlv.value)
                        .map_err(|_| bad("the identity TLV is not UTF-8"))?;
                    custom = Some(id.to_string());
                }
            }
            out.identity = custom.or(cn.filter(|_| out.client_cert_verified));
        }
        ppp::HeaderResult::V1(Err(e)) => return Err(bad(&e.to_string())),
        ppp::HeaderResult::V2(Err(e)) => return Err(bad(&e.to_string())),
    }
    Ok(out)
}

/// Read a `PP2_TYPE_SSL` value and return its CN sub-TLV.
fn ssl(value: &[u8], out: &mut ProxyHeader) -> io::Result<Option<String>> {
    if value.len() < 5 {
        return Err(bad("short SSL TLV"));
    }
    let client = value[0];
    let verify = u32::from_be_bytes([value[1], value[2], value[3], value[4]]);
    out.client_tls = Some(client & PP2_CLIENT_SSL != 0);
    let has_cert = client & (PP2_CLIENT_CERT_CONN | PP2_CLIENT_CERT_SESS) != 0;
    out.client_cert_verified = has_cert && verify == 0;
    let mut cn = None;
    for sub in TypeLengthValues::from(&value[5..]) {
        let sub = sub.map_err(|e| bad(&e.to_string()))?;
        if sub.kind == PP2_SUBTYPE_SSL_CN {
            cn = std::str::from_utf8(&sub.value).ok().map(str::to_string);
        }
    }
    Ok(cn)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ppp::v2::{Builder, Command, IPv4, Protocol, Version};

    async fn parse(bytes: &[u8]) -> io::Result<ProxyHeader> {
        let mut r = bytes;
        read_proxy_header(&mut r, Some(0xE0)).await
    }

    fn ssl_tlv(client: u8, verify: u32, cn: &str) -> Vec<u8> {
        let mut v = vec![client];
        v.extend_from_slice(&verify.to_be_bytes());
        v.push(0x22);
        v.extend_from_slice(&(cn.len() as u16).to_be_bytes());
        v.extend_from_slice(cn.as_bytes());
        v
    }

    fn v2(tlvs: &[(u8, Vec<u8>)]) -> Vec<u8> {
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

    #[tokio::test]
    async fn v1_tcp4_gives_the_source() {
        let h = parse(b"PROXY TCP4 10.1.2.3 10.0.0.1 51000 6379\r\n")
            .await
            .unwrap();
        assert_eq!(h.source, Some("10.1.2.3:51000".parse().unwrap()));
        assert_eq!(h.client_tls, None);
        assert_eq!(h.identity, None);
    }

    #[tokio::test]
    async fn v2_with_ssl_tlv_gives_the_cn_and_the_tls_flag() {
        let h = parse(&v2(&[(0x20, ssl_tlv(0x01 | 0x02, 0, "app"))]))
            .await
            .unwrap();
        assert_eq!(h.source, Some("10.1.2.3:51000".parse().unwrap()));
        assert_eq!(h.client_tls, Some(true));
        assert!(h.client_cert_verified);
        assert_eq!(h.identity.as_deref(), Some("app"));
    }

    #[tokio::test]
    async fn an_unverified_client_certificate_gives_no_identity() {
        let h = parse(&v2(&[(0x20, ssl_tlv(0x01 | 0x02, 1, "app"))]))
            .await
            .unwrap();
        assert!(!h.client_cert_verified);
        assert_eq!(h.identity, None);
    }

    #[tokio::test]
    async fn v2_custom_tlv_gives_the_identity() {
        let h = parse(&v2(&[
            (0x20, ssl_tlv(0x01 | 0x02, 0, "cn")),
            (0xE0, b"spiffe://x/app".to_vec()),
        ]))
        .await
        .unwrap();
        assert_eq!(h.identity.as_deref(), Some("spiffe://x/app"));
    }

    #[tokio::test]
    async fn v2_local_has_no_source() {
        let local = Builder::with_addresses(
            Version::Two | Command::Local,
            Protocol::Unspecified,
            ppp::v2::Addresses::Unspecified,
        )
        .build()
        .unwrap();
        assert_eq!(parse(&local).await.unwrap().source, None);
        let unknown = parse(b"PROXY UNKNOWN\r\n").await.unwrap();
        assert_eq!(unknown.source, None);
    }

    #[tokio::test]
    async fn a_bad_signature_or_an_oversize_header_fails() {
        assert!(parse(b"GET / HTTP/1.1\r\n\r\n").await.is_err());
        assert!(parse(&[b'P'; 200]).await.is_err());
        let mut big = v2(&[]);
        big[14..16].copy_from_slice(&5000u16.to_be_bytes());
        assert!(parse(&big).await.is_err());
    }

    #[tokio::test]
    async fn the_reader_stops_at_the_end_of_the_header() {
        let mut wire = b"PROXY TCP4 10.1.2.3 10.0.0.1 51000 6379\r\n".to_vec();
        wire.extend_from_slice(b"\x16\x03\x01rest");
        let mut r = wire.as_slice();
        read_proxy_header(&mut r, None).await.unwrap();
        assert_eq!(r, b"\x16\x03\x01rest");
        let mut wire = v2(&[]);
        wire.extend_from_slice(b"tls");
        let mut r = wire.as_slice();
        read_proxy_header(&mut r, None).await.unwrap();
        assert_eq!(r, b"tls");
    }
}
