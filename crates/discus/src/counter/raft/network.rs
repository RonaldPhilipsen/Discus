//! The Raft network: openraft RPCs as `RAFT_*` messages on the bulk lane (spec section 13.2).

use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::sync::Arc;
use std::time::{Duration, Instant};

use discus_placement::NodeId;
use discus_wire::{Message, RaftHeader, RaftReply, Regime};
use futures::future::BoxFuture;
use openraft::errors::{RPCError, RaftError, Unreachable};
use openraft::network::{Backoff, RPCOption, RaftNetworkFactory};
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest, VoteResponse};
use openraft::AnyError;
use openraft_legacy::network_v1::{
    Adapter, InstallSnapshotError, InstallSnapshotRequest, InstallSnapshotResponse, RaftNetwork,
};
use parking_lot::Mutex;

use super::convert;
use super::{Peer, TypeConfig};

/// Sends one counter message to a node and returns its reply. The node implements it over the
/// transport, and tests implement it in memory.
pub trait RaftLink: Send + Sync + 'static {
    fn send(
        &self,
        to: NodeId,
        message: Message,
        timeout: Duration,
    ) -> BoxFuture<'static, Result<Message, String>>;
}

/// What the leader knows about each replica: its last successful Raft reply, and whether a live
/// peer refused it. Silence counts against the voter timeout. A refusal is final (spec section
/// 10.2): a replica ID is never used twice, and a node answers `RAFT_CONFIG` with an ID only
/// after it installed that replica, so "not started yet" is impossible. A reply that succeeded
/// before the refusal can arrive after it, so a contact does not clear the mark.
#[derive(Debug, Default)]
pub struct Contacts {
    seen: Mutex<HashMap<u64, Instant>>,
    refused: Mutex<HashSet<u64>>,
}

impl Contacts {
    pub fn saw(&self, replica: u64) {
        self.seen.lock().insert(replica, Instant::now());
    }

    /// The time since the last reply, counted from `since` at the earliest. A reply from before
    /// `since` is from an earlier term of this leader, which sent nothing in between.
    pub fn silent_for(&self, replica: u64, since: Instant) -> Duration {
        let seen = self.seen.lock().get(&replica).copied();
        seen.map_or(since, |s| s.max(since)).elapsed()
    }

    pub fn refuse(&self, replica: u64) {
        self.refused.lock().insert(replica);
    }

    pub fn refused(&self, replica: u64) -> bool {
        self.refused.lock().contains(&replica)
    }
}

#[derive(Clone)]
pub struct NetFactory {
    pub link: Arc<dyn RaftLink>,
    pub group: u16,
    pub regime: Regime,
    pub contacts: Arc<Contacts>,
}

/// The leader sends a snapshot in chunks of `snapshot_max_chunk_size`, as `RAFT_SNAPSHOT` carries
/// one chunk. `openraft-legacy` keeps that protocol of openraft 0.9.
pub type NetConn = Adapter<TypeConfig, NetClient, Cursor<Vec<u8>>>;

impl RaftNetworkFactory<TypeConfig> for NetFactory {
    type Network = NetConn;

    async fn new_client(&mut self, target: u64, node: &Peer) -> NetConn {
        Adapter::new(NetClient {
            link: self.link.clone(),
            header: RaftHeader {
                group: self.group,
                regime: self.regime,
                target,
            },
            to: node.node,
            contacts: self.contacts.clone(),
        })
    }
}

pub struct NetClient {
    link: Arc<dyn RaftLink>,
    header: RaftHeader,
    to: NodeId,
    contacts: Arc<Contacts>,
}

fn unreachable<E: std::error::Error>(why: impl std::fmt::Display) -> RPCError<TypeConfig, E> {
    RPCError::Unreachable(Unreachable::new(&AnyError::error(why.to_string())))
}

impl NetClient {
    /// One RPC: send `message`, read the reply with `of`, and count the contact.
    #[allow(clippy::result_large_err)] // the error type of `RaftNetwork`
    async fn rpc<R, E: std::error::Error>(
        &self,
        message: Message,
        option: &RPCOption,
        of: fn(RaftReply) -> Option<R>,
    ) -> Result<R, RPCError<TypeConfig, E>> {
        // A live replica also refuses a snapshot chunk that does not fit, so only the other
        // RPCs prove that the replica is gone.
        let definite = !matches!(message, Message::RaftSnapshot(_));
        let reply = match self.link.send(self.to, message, option.hard_ttl()).await {
            Ok(Message::RaftReply(r)) => match of(r) {
                Some(reply) => reply,
                None => {
                    if definite {
                        self.contacts.refuse(self.header.target);
                    }
                    return Err(unreachable("refused"));
                }
            },
            Ok(other) => {
                return Err(unreachable(format!(
                    "expected RAFT_REPLY, got message type {}",
                    other.type_code()
                )))
            }
            Err(e) => return Err(unreachable(e)),
        };
        self.contacts.saw(self.header.target);
        Ok(reply)
    }
}

impl RaftNetwork<TypeConfig> for NetClient {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig, RaftError<TypeConfig>>>
    {
        let message = Message::RaftAppend(convert::append_to(self.header, &rpc));
        self.rpc(message, &option, convert::append_reply_of).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<TypeConfig>,
        RPCError<TypeConfig, RaftError<TypeConfig, InstallSnapshotError>>,
    > {
        let message = Message::RaftSnapshot(convert::snapshot_to(self.header, rpc));
        self.rpc(message, &option, convert::snapshot_reply_of).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig, RaftError<TypeConfig>>> {
        let message = Message::RaftVote(convert::vote_req_to(self.header, &rpc));
        self.rpc(message, &option, convert::vote_reply_of).await
    }

    /// The constant 500 ms of openraft 0.9. The `openraft-legacy` default is 200 ms.
    fn backoff(&self) -> Backoff {
        Backoff::new(std::iter::repeat(Duration::from_millis(500)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_refusal_stays_after_a_later_contact() {
        let contacts = Contacts::default();
        assert!(!contacts.refused(1));
        contacts.saw(1);
        contacts.refuse(1);
        contacts.saw(1);
        assert!(contacts.refused(1));
        assert!(!contacts.refused(2));
    }

    #[test]
    fn silence_counts_from_the_later_of_the_last_reply_and_since() {
        let contacts = Contacts::default();
        contacts.saw(1);
        std::thread::sleep(Duration::from_millis(10));
        let since = Instant::now();
        assert!(contacts.silent_for(1, since) <= since.elapsed());
        std::thread::sleep(Duration::from_millis(10));
        contacts.saw(1);
        assert!(contacts.silent_for(1, since) < since.elapsed());
    }
}
