//! The Raft network: openraft RPCs as `RAFT_*` messages on the bulk lane (spec section 13.2).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use discus_placement::NodeId;
use discus_wire::{Message, RaftHeader, RaftReply, Regime};
use futures::future::BoxFuture;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::AnyError;
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

/// The last successful Raft reply of each replica. The leader removes a voter that stays silent
/// for the voter timeout (spec section 10.2).
#[derive(Debug, Default)]
pub struct Contacts(Mutex<HashMap<u64, Instant>>);

impl Contacts {
    pub fn saw(&self, replica: u64) {
        self.0.lock().insert(replica, Instant::now());
    }

    /// The time since the last reply. A replica without a reply counts from `since`.
    pub fn silent_for(&self, replica: u64, since: Instant) -> Duration {
        self.0
            .lock()
            .get(&replica)
            .copied()
            .unwrap_or(since)
            .elapsed()
    }
}

#[derive(Clone)]
pub struct NetFactory {
    pub link: Arc<dyn RaftLink>,
    pub group: u16,
    pub regime: Regime,
    pub contacts: Arc<Contacts>,
}

impl RaftNetworkFactory<TypeConfig> for NetFactory {
    type Network = NetClient;

    async fn new_client(&mut self, target: u64, node: &Peer) -> NetClient {
        NetClient {
            link: self.link.clone(),
            header: RaftHeader {
                group: self.group,
                regime: self.regime,
                target,
            },
            to: node.node,
            contacts: self.contacts.clone(),
        }
    }
}

pub struct NetClient {
    link: Arc<dyn RaftLink>,
    header: RaftHeader,
    to: NodeId,
    contacts: Arc<Contacts>,
}

fn unreachable<E: std::error::Error>(why: impl std::fmt::Display) -> RPCError<u64, Peer, E> {
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
    ) -> Result<R, RPCError<u64, Peer, E>> {
        let reply = match self.link.send(self.to, message, option.hard_ttl()).await {
            Ok(Message::RaftReply(r)) => of(r).ok_or_else(|| unreachable("refused"))?,
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
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, Peer, RaftError<u64>>> {
        let message = Message::RaftAppend(convert::append_to(self.header, &rpc));
        self.rpc(message, &option, convert::append_reply_of).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, Peer, RaftError<u64, InstallSnapshotError>>,
    > {
        let message = Message::RaftSnapshot(convert::snapshot_to(self.header, rpc));
        self.rpc(message, &option, convert::snapshot_reply_of).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, Peer, RaftError<u64>>> {
        let message = Message::RaftVote(convert::vote_req_to(self.header, &rpc));
        self.rpc(message, &option, convert::vote_reply_of).await
    }
}
