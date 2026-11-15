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

fn used_memory(info: &str) -> u64 {
    info.lines()
        .find_map(|l| l.strip_prefix("used_memory:"))
        .and_then(|v| v.trim().parse().ok())
        .expect("INFO memory has used_memory")
}

/// A write that adds many small members admits the bookkeeping of each member, so the store
/// stays within `maxmemory` after it.
#[tokio::test]
async fn a_write_of_many_small_members_stays_within_maxmemory() {
    let max = 200_000;
    let mut c = common::connect(&["--max-memory", &max.to_string()]).await;
    for i in 0..400 {
        let _: () = c.set(format!("k{i}"), "x".repeat(1_000)).await.unwrap();
    }
    let members: Vec<String> = (0..1_000).map(|i| i.to_string()).collect();
    let _: () = redis::cmd("SADD")
        .arg("s")
        .arg(&members)
        .query_async(&mut c)
        .await
        .unwrap();
    let fields: Vec<(String, String)> = (0..500).map(|i| (i.to_string(), "v".into())).collect();
    let _: () = redis::cmd("HSET")
        .arg("h")
        .arg(&fields)
        .query_async(&mut c)
        .await
        .unwrap();
    let info: String = redis::cmd("INFO")
        .arg("memory")
        .query_async(&mut c)
        .await
        .unwrap();
    assert!(used_memory(&info) <= max, "{info}");
}
