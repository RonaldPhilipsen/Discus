//! Authorization runs on the entry node only. A forwarded command arrives over cluster mTLS and
//! the owner does not check the client again.

#![allow(clippy::unwrap_used)]

mod common;

use common::cluster::start_cluster_with;

#[tokio::test]
async fn the_entry_node_authorizes_and_the_owner_trusts_the_forward() {
    // Each node has its own token, so a check on the owner would refuse the entry node's client.
    let c = start_cluster_with(2, |i| {
        vec![
            "--shared-token".into(),
            if i == 0 { "entry-token" } else { "owner-token" }.into(),
        ]
    })
    .await;
    let key = c.key_on(1, "auth");
    let mut entry = c.nodes[0].redis().await;

    let refused = redis::cmd("GET")
        .arg(&key)
        .query_async::<Option<String>>(&mut entry)
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Some("NOAUTH"), "{refused}");
    assert!(!c.nodes[1].holds(key.as_bytes()));

    let _: () = redis::cmd("AUTH")
        .arg("entry-token")
        .query_async(&mut entry)
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg(&key)
        .arg("v")
        .query_async(&mut entry)
        .await
        .unwrap();
    assert!(c.nodes[1].holds(key.as_bytes()));
    assert!(!c.nodes[0].holds(key.as_bytes()));
    let got: String = redis::cmd("GET")
        .arg(&key)
        .query_async(&mut entry)
        .await
        .unwrap();
    assert_eq!(got, "v");
}
