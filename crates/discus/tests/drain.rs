#![allow(clippy::unwrap_used)]

mod common;

use std::time::Duration;

use common::gossip::{agreed, wait_until, GossipNode, Net};
use discus_placement::MemberState;
use redis::AsyncCommands;

const FORM: Duration = Duration::from_secs(10);

fn listed_as(n: &GossipNode, of: &GossipNode) -> Option<MemberState> {
    n.node.epochs().current().member(of.id).map(|m| m.state)
}

async fn three(net: &Net, extra: &[&str]) -> Vec<GossipNode> {
    let mut args = vec!["--shutdown-timeout-secs", "2"];
    args.extend_from_slice(extra);
    let nodes = net.start_all(3, &args).await;
    assert!(wait_until(FORM, || agreed(&nodes.iter().collect::<Vec<_>>())).await);
    nodes
}

#[tokio::test]
async fn leaving_turns_readiness_off() {
    let net = Net::new();
    let nodes = three(&net, &[]).await;
    let mut client = nodes[2].redis().await;
    let _: () = client.set("k", "v").await.unwrap();
    nodes[2].shutdown.cancel();
    let half = Duration::from_millis(500);
    assert!(common::wait_status(nodes[2].admin, "/readyz", 503, half).await);
    assert!(
        wait_until(Duration::from_secs(1), || {
            listed_as(&nodes[0], &nodes[2]) == Some(MemberState::Leaving)
                && listed_as(&nodes[1], &nodes[2]) == Some(MemberState::Leaving)
        })
        .await
    );
    // The open connection keeps working, and the handoff or the previous-epoch fallback keeps
    // the value readable.
    let v: Option<String> = client.get("k").await.unwrap();
    assert_eq!(v.as_deref(), Some("v"));
    let pong: String = redis::cmd("PING").query_async(&mut client).await.unwrap();
    assert_eq!(pong, "PONG");
}

#[tokio::test]
async fn a_leaving_node_departs_after_the_deadline() {
    let net = Net::new();
    // A slow suspicion timeout: only the goodbye can remove the node in time.
    let slow = [
        "--suspicion-min-ms",
        "8000",
        "--suspicion-max-ms",
        "8000",
        "--phi-dead",
        "16",
    ];
    let nodes = three(&net, &slow).await;
    let _client = nodes[2].redis().await;
    nodes[2].shutdown.cancel();
    let rest = [&nodes[0], &nodes[1]];
    // The open connection holds the drain for the 2 s deadline.
    assert!(wait_until(Duration::from_secs(4), || agreed(&rest)).await);
    assert_eq!(listed_as(&nodes[0], &nodes[2]), None);
}
