//! `COPY` whose keys can have different primaries or databases: a peek on the source, then a put
//! on the destination. The source never changes, so a failure between the steps loses nothing.

use std::sync::Arc;

use bytes::Bytes;
use discus_resp::Value;

use super::rename::Steps;
use super::{Dispatch, Hop, Job, Reply, Writes};
use crate::command::internal::{PEEK, PUT};
use crate::command::keys::copy_target;
use crate::node::Node;

/// Run a `COPY` from the entry node.
pub fn run(node: &Arc<Node>, job: Job, hop: Hop, writes: Option<&Writes>) -> Dispatch {
    let target = match copy_target(node, job.db, &job.cmd) {
        Ok(target) => target,
        Err(e) => return Dispatch::Ready(Reply::error(e)),
    };
    let (from, to) = (job.cmd[1].clone(), job.cmd[2].clone());
    let steps = Steps {
        node: node.clone(),
        rr: job.rr,
        db: job.db,
        writes: writes.cloned(),
        hop,
    };
    Dispatch::Pending(Box::pin(async move {
        let name = Bytes::from_static(PEEK.name.as_bytes());
        let peeked = steps.run(PEEK, steps.db, vec![name, from]).await;
        let blob = match peeked.value {
            Value::Null => return Reply::value(Value::Integer(0)),
            Value::Bulk(ref blob) => blob.clone(),
            _ => return peeked,
        };
        let mut put = vec![Bytes::from_static(PUT.name.as_bytes()), to, blob];
        if !target.replace {
            put.push(Bytes::from_static(b"NX"));
        }
        let put = steps.run(PUT, target.db, put).await;
        match put.value {
            Value::Null => Reply::value(Value::Integer(0)),
            Value::Error(_) => put,
            _ => Reply::value(Value::Integer(1)),
        }
    }))
}
