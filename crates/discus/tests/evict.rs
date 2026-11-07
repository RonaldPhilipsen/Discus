mod common;

use redis::AsyncCommands;

#[tokio::test]
async fn writes_beyond_maxmemory_evict_instead_of_failing() {
    let mut c = common::connect(&["--max-memory", "200000"]).await;
    for i in 0..400 {
        let _: () = c.set(format!("k{i}"), "x".repeat(1_000)).await.unwrap();
    }
    let present: i64 = redis::cmd("DBSIZE").query_async(&mut c).await.unwrap();
    assert!(present < 400 && present > 50, "present = {present}");
    let err = c
        .set::<_, _, ()>("huge", "x".repeat(300_000))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("OOM"), "{err}");
    let kept: i64 = c.exists("k399").await.unwrap();
    assert_eq!(kept, 1);
}
