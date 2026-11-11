//! The redirect rule: when hop 0 of a redirect connection returns `-MOVED` instead of forwarding
//! (spec "MOVED and forwarding").

use discus_placement::{slot, EpochId, Member, MemberState, NodeId, Version};

use super::{candidates, Hop};
use crate::command::CommandError;
use crate::node::Node;

/// The client address that a `-MOVED` reply names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub host: String,
    pub port: u16,
}

/// Why the node forwards a command instead of returning `-MOVED`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Misroute {
    /// The target has not reported the current epoch of this node or a newer one.
    Epoch,
    /// The target is not up (spec "Target health").
    Down,
    /// The current epoch does not list the target as `NORMAL` with a client address.
    NoAddress,
}

/// The client endpoint of `member`: the host name when one is set, else the IP.
pub fn endpoint(member: &Member) -> Option<Endpoint> {
    let addr = member.client_addr?;
    Some(Endpoint {
        host: member
            .client_host
            .clone()
            .unwrap_or_else(|| addr.ip().to_string()),
        port: addr.port(),
    })
}

/// Step 4 of the redirect rule. `member` is the target in the current epoch, `alive` its health,
/// and `peer_epoch` the epoch it last reported. A peer reports only epochs it adopted, so every
/// `-MOVED` points to a node with the same epoch or a newer one.
pub fn decide(
    member: Option<&Member>,
    alive: bool,
    peer_epoch: Option<EpochId>,
    current: EpochId,
) -> Result<Endpoint, Misroute> {
    let to = member
        .filter(|m| m.state == MemberState::Normal)
        .and_then(endpoint)
        .ok_or(Misroute::NoAddress)?;
    if !alive {
        return Err(Misroute::Down);
    }
    if peer_epoch.is_none_or(|e| e < current) {
        return Err(Misroute::Epoch);
    }
    Ok(to)
}

/// The redirect rule applies to this hop: hop 0 of a redirect connection. A held retry already
/// forwarded once, so it keeps forwarding.
pub(crate) fn applies(redirect: bool, hop: &Hop) -> bool {
    redirect && hop.hops == 0 && !hop.held
}

/// The target of `key` on a redirect connection. A write goes to the primary. A read follows the
/// write set rule first, then goes to this node only after `READONLY`, with
/// `--read-from-replicas`, when this node owns the slot (spec "Reads and READONLY").
pub(crate) fn target(
    node: &Node,
    key: &[u8],
    owners: &[NodeId],
    write: bool,
    need: Option<Version>,
    readonly: bool,
) -> Option<NodeId> {
    if !write && need.is_some() {
        return candidates(node, key, owners, write, 0, need)
            .first()
            .copied();
    }
    let replicas = node.link().is_some_and(|l| l.read_from_replicas);
    if !write && readonly && replicas && owners.contains(&node.id) {
        return Some(node.id);
    }
    owners.first().copied()
}

/// `-MOVED` to `target` for the slot of `key`, or why the node forwards instead.
pub(crate) fn moved(node: &Node, key: &[u8], target: NodeId) -> Result<CommandError, Misroute> {
    // Health reads other locks, so it runs before the epoch lock is taken.
    let alive = node.target_up(target);
    let epoch = node.epochs().current().clone();
    let peer_epoch = node.link().and_then(|l| l.peer_epoch(target));
    let to = decide(epoch.member(target), alive, peer_epoch, epoch.id())?;
    Ok(CommandError::Moved {
        slot: slot(key),
        to,
    })
}

/// `-MOVED` for a multi-key command whose keys all fall in one slot that another healthy node
/// serves. `None` when the command runs as on the forward listener: its keys span several
/// slots, the write set splits the targets, the target is this node, or step 4 fails.
pub(crate) fn multi_moved(
    node: &Node,
    keys: &[&[u8]],
    write: bool,
    need: impl Fn(&[u8]) -> Option<Version>,
    readonly: bool,
) -> Option<CommandError> {
    let first = *keys.first()?;
    let at = slot(first);
    if keys.iter().any(|k| slot(k) != at) {
        return None;
    }
    let owners = node.owners(first);
    let mut to = None;
    for key in keys {
        let t = target(node, key, &owners, write, need(key), readonly)?;
        if to.is_some_and(|prev| prev != t) {
            return None;
        }
        to = Some(t);
    }
    let to = to.filter(|t| *t != node.id)?;
    moved(node, first, to)
        .inspect_err(|misroute| {
            tracing::trace!(?misroute, target = %to, "forwarding instead of MOVED");
        })
        .ok()
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use discus_placement::Weight;
    use proptest::prelude::*;

    use super::*;

    fn member(id: NodeId, client: Option<SocketAddr>) -> Member {
        let addr: SocketAddr = "10.0.0.1:7000".parse().unwrap();
        Member {
            id,
            addr,
            rpc_addr: addr,
            client_addr: client,
            client_host: None,
            state: MemberState::Normal,
            weight: Weight::default(),
        }
    }

    fn epoch(generation: u64) -> EpochId {
        EpochId {
            generation,
            origin: NodeId::NIL,
        }
    }

    #[test]
    fn decide_table() {
        let client: SocketAddr = "10.0.0.2:6380".parse().unwrap();
        let current = epoch(5);
        let peers = [
            ("unknown", None),
            ("older", Some(epoch(4))),
            ("equal", Some(epoch(5))),
            ("newer", Some(epoch(6))),
        ];
        for alive in [true, false] {
            for (name, peer) in peers {
                for addr in [Some(client), None] {
                    let m = member(NodeId::random(), addr);
                    let got = decide(Some(&m), alive, peer, current);
                    let want = if addr.is_none() {
                        Err(Misroute::NoAddress)
                    } else if !alive {
                        Err(Misroute::Down)
                    } else if matches!(name, "unknown" | "older") {
                        Err(Misroute::Epoch)
                    } else {
                        Ok(Endpoint {
                            host: "10.0.0.2".into(),
                            port: 6380,
                        })
                    };
                    assert_eq!(got, want, "alive {alive}, peer {name}, address {addr:?}");
                }
            }
        }
        assert_eq!(
            decide(None, true, Some(current), current),
            Err(Misroute::NoAddress)
        );
        let mut leaving = member(NodeId::random(), Some(client));
        leaving.state = MemberState::Leaving;
        assert_eq!(
            decide(Some(&leaving), true, Some(current), current),
            Err(Misroute::NoAddress)
        );
    }

    #[test]
    fn endpoint_prefers_the_host_name() {
        let mut m = member(NodeId::random(), Some("10.0.0.2:6380".parse().unwrap()));
        assert_eq!(endpoint(&m).unwrap().host, "10.0.0.2");
        m.client_host = Some("discus-1.peers".into());
        assert_eq!(
            endpoint(&m),
            Some(Endpoint {
                host: "discus-1.peers".into(),
                port: 6380,
            })
        );
        m.client_addr = None;
        assert_eq!(endpoint(&m), None);
    }

    /// A model cluster for one slot. `held[i]` is the generation index node `i` holds, `targets[g]`
    /// the node that generation `g` names, `known[i][j]` what node `i` knows of the epoch of node
    /// `j` (never newer than `held[j]`), and `alive[j]` the health of node `j`.
    #[derive(Debug, Clone)]
    struct Model {
        held: Vec<usize>,
        targets: Vec<usize>,
        known: Vec<Vec<Option<usize>>>,
        alive: Vec<bool>,
    }

    fn model() -> impl Strategy<Value = Model> {
        (2usize..=5, 1usize..=4).prop_flat_map(|(n, g)| {
            let held = proptest::collection::vec(0..g, n);
            let targets = proptest::collection::vec(0..n, g);
            let alive = proptest::collection::vec(any::<bool>(), n);
            let picks = proptest::collection::vec(
                proptest::collection::vec((any::<bool>(), any::<u8>()), n),
                n,
            );
            (held, targets, alive, picks).prop_map(|(held, targets, alive, picks)| {
                let known = picks
                    .iter()
                    .map(|row| {
                        row.iter()
                            .zip(&held)
                            .map(|((seen, pick), real)| seen.then(|| *pick as usize % (real + 1)))
                            .collect()
                    })
                    .collect();
                Model {
                    held,
                    targets,
                    known,
                    alive,
                }
            })
        })
    }

    proptest! {
        #[test]
        fn moved_chains_end(m in model(), start in 0usize..5) {
            let n = m.held.len();
            let ids: Vec<NodeId> = (0..n).map(|_| NodeId::random()).collect();
            let members: Vec<Member> = (0..n)
                .map(|i| member(ids[i], Some(SocketAddr::from(([10, 0, 0, i as u8 + 1], 6380)))))
                .collect();
            let gen = |g: usize| epoch(g as u64 + 1);
            let mut at = start % n;
            let mut seen = vec![m.held[at]];
            let mut redirects = 0;
            loop {
                let here = m.held[at];
                let target = m.targets[here];
                if target == at {
                    break;
                }
                let decided = decide(
                    Some(&members[target]),
                    m.alive[target],
                    m.known[at][target].map(gen),
                    gen(here),
                );
                let Ok(to) = decided else {
                    break;
                };
                prop_assert_eq!(to.port, 6380);
                redirects += 1;
                at = target;
                prop_assert!(m.held[at] >= here, "the epoch went down along the chain");
                if !seen.contains(&m.held[at]) {
                    seen.push(m.held[at]);
                }
                prop_assert!(redirects <= seen.len(), "more redirects than distinct epochs");
            }
        }
    }
}
