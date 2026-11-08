#![allow(clippy::unwrap_used)]

mod common;

use std::time::Duration;

use common::gossip::{wait_agreed, wait_until, GossipNode, Net};
use discus_placement::MemberState;
use redis::AsyncCommands;

fn listed_as(n: &GossipNode, of: &GossipNode) -> Option<MemberState> {
    n.node.epochs().current().member(of.id).map(|m| m.state)
}

async fn three(net: &Net, extra: &[&str]) -> Vec<GossipNode> {
    let mut args = vec!["--shutdown-timeout-secs", "2"];
    args.extend_from_slice(extra);
    let nodes = net.start_agreed(3, &args).await;
    nodes
}

#[tokio::test]
async fn leaving_turns_readiness_off() {
    let net = Net::new();
    // The open connection holds the drain until this deadline, so the waits below must end
    // before it does.
    let nodes = net
        .start_agreed(3, &["--shutdown-timeout-secs", "60"])
        .await;
    let mut client = nodes[2].redis().await;
    let _: () = client.set("k", "v").await.unwrap();
    nodes[2].shutdown.cancel();
    let wait = Duration::from_secs(15);
    assert!(common::wait_status(nodes[2].admin, "/readyz", 503, wait).await);
    assert!(
        wait_until(wait, || {
            listed_as(&nodes[0], &nodes[2]) == Some(MemberState::Leaving)
                && listed_as(&nodes[1], &nodes[2]) == Some(MemberState::Leaving)
        })
        .await
    );
    // The open connection keeps working and the value stays in the cluster. A read that lands
    // while the handoff is in flight can miss for a few milliseconds, so this polls.
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        let v: Option<String> = client.get("k").await.unwrap();
        if v.as_deref() == Some("v") {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "{v:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
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
    wait_agreed(&rest, Duration::from_secs(4)).await;
    assert_eq!(listed_as(&nodes[0], &nodes[2]), None);
}
