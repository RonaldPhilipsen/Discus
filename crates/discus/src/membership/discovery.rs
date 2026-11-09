//! Discovery seeds and the advertised address (spec section 6.2).

use std::net::{SocketAddr, UdpSocket};

use crate::Config;

/// The static seeds of `--peer`.
pub fn static_seeds(config: &Config) -> Vec<SocketAddr> {
    config.peers.iter().map(|p| p.addr).collect()
}

/// The host and port of `--discovery-dns`. The port defaults to the cluster listen port.
pub fn dns_target(config: &Config) -> Option<(String, u16)> {
    let name = config.discovery_dns.as_ref()?;
    let port = config.cluster_listen.port();
    Some(match name.rsplit_once(':') {
        Some((host, p)) if p.parse::<u16>().is_ok() => {
            (host.to_string(), p.parse().unwrap_or(port))
        }
        _ => (name.clone(), port),
    })
}

/// Resolve the headless Service. Every ready and not-ready pod is one address, because the
/// Service sets `publishNotReadyAddresses`.
pub async fn resolve(target: &(String, u16)) -> Vec<SocketAddr> {
    match tokio::net::lookup_host((target.0.as_str(), target.1)).await {
        Ok(addrs) => addrs.collect(),
        Err(e) => {
            tracing::debug!(name = %target.0, error = %e, "discovery lookup failed");
            Vec::new()
        }
    }
}

/// The node-to-node address that other members use: `--cluster-advertise`, else a specified
/// `--cluster-listen`, else the local address of a UDP socket connected to the first seed, else
/// `127.0.0.1` with the listen port.
pub async fn advertise(config: &Config) -> SocketAddr {
    if let Some(a) = config.cluster_advertise {
        return a;
    }
    let listen = config.cluster_listen;
    if !listen.ip().is_unspecified() {
        return listen;
    }
    let mut seeds = static_seeds(config);
    if let Some(target) = dns_target(config) {
        seeds.extend(resolve(&target).await);
    }
    // Connecting a UDP socket sends nothing. It only picks the outbound interface.
    let local = seeds.iter().find_map(|seed| {
        let any: SocketAddr = if seed.is_ipv4() {
            ([0, 0, 0, 0], 0).into()
        } else {
            ([0u16; 8], 0).into()
        };
        let socket = UdpSocket::bind(any).ok()?;
        socket.connect(seed).ok()?;
        socket.local_addr().ok().map(|a| a.ip())
    });
    SocketAddr::new(
        local.unwrap_or_else(|| [127, 0, 0, 1].into()),
        listen.port(),
    )
}
