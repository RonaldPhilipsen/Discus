//! `RENAME` whose keys can have different primaries: a take on the source, then a put on the
//! destination.
//!
//! The source goes first, so a failure can lose the value but never keeps the old key. Authelia
//! changes a session ID with `RENAME` to stop session fixation, so the old ID must not survive.

use std::sync::Arc;

use bytes::Bytes;
use discus_resp::{Command, Value};

use super::{run_hop, Dispatch, Hop, Job, Reply, Writes};
use crate::command::internal::{PUT, TAKE};
use crate::command::util::check_key;
use crate::command::{self, CommandError, CommandSpec};
use crate::node::Node;
use crate::telemetry;

/// What each step of one `RENAME` shares.
struct Steps {
    node: Arc<Node>,
    rr: u64,
    /// The database of the `RENAME`. Both steps run in it.
    db: u32,
    writes: Option<Writes>,
    hop: Hop,
}

impl Steps {
    async fn run(&self, spec: &'static CommandSpec, db: u32, cmd: Command) -> Reply {
        let job = Job {
            cmd,
            spec,
            rr: self.rr,
            // The take and the put are internal steps, so they forward and never return `-MOVED`.
            redirect: false,
            readonly: false,
            db,
        };
        let hop = Hop {
            trace: self.hop.trace.clone(),
            ..Hop::default()
        };
        run_hop(
            &self.node,
            job,
            hop,
            self.writes.as_ref(),
            telemetry::route_span(),
        )
        .finish()
        .await
    }
}

/// Answer `RENAME key key`, which changes nothing, once the key is known to exist. An error says
/// nothing about the key, so it goes to the client.
async fn same_key(steps: &Steps, key: Bytes) -> Reply {
    let Some(spec) = command::lookup(b"TYPE") else {
        return Reply::try_again();
    };
    let reply = steps
        .run(spec, steps.db, vec![Bytes::from_static(b"TYPE"), key])
        .await;
    match reply.value {
        Value::Error(_) => reply,
        Value::Simple(ref kind) if kind.as_ref() == b"none" => {
            Reply::error(CommandError::NoSuchKey)
        }
        _ => Reply::value(Value::ok()),
    }
}

async fn move_entry(steps: &Steps, from: Bytes, to: Bytes) -> Reply {
    let name = Bytes::from_static(TAKE.name.as_bytes());
    let taken = steps.run(TAKE, steps.db, vec![name, from]).await;
    let blob = match taken.value {
        Value::Null => return Reply::error(CommandError::NoSuchKey),
        Value::Bulk(ref blob) => blob.clone(),
        // The take did not succeed, so the error tells the client what happened.
        _ => return taken,
    };
    let name = Bytes::from_static(PUT.name.as_bytes());
    let put = steps.run(PUT, steps.db, vec![name, to, blob]).await;
    if matches!(put.value, Value::Error(_)) || put.failed.is_some() {
        return Reply::try_again();
    }
    Reply::value(Value::ok())
}

/// Run a `RENAME` from the entry node.
pub fn run(node: &Arc<Node>, job: Job, hop: Hop, writes: Option<&Writes>) -> Dispatch {
    let (from, to) = (job.cmd[1].clone(), job.cmd[2].clone());
    // The take removes the source, so a destination that the put must refuse is refused now.
    if let Err(e) = check_key(node, &to) {
        return Dispatch::Ready(Reply::error(e));
    }
    let steps = Steps {
        node: node.clone(),
        rr: job.rr,
        db: job.db,
        writes: writes.cloned(),
        hop,
    };
    Dispatch::Pending(Box::pin(async move {
        if from == to {
            same_key(&steps, from).await
        } else {
            move_entry(&steps, from, to).await
        }
    }))
}
