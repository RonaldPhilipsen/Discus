mod common;

use redis::AsyncCommands;
use std::collections::HashSet;

#[tokio::test]
async fn scan_visits_every_key_exactly_once() {
    let mut c = common::connect(&["--shards", "8"]).await;
    for i in 0..500 {
        let _: () = c.set(format!("k{i}"), "v").await.unwrap();
    }
    let mut seen = HashSet::new();
    let mut cursor: u64 = 0;
    loop {
        let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("COUNT")
            .arg(37)
            .query_async(&mut c)
            .await
            .unwrap();
        for k in keys {
            assert!(seen.insert(k), "key visited twice");
        }
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    assert_eq!(seen.len(), 500);
    let n: i64 = redis::cmd("DBSIZE").query_async(&mut c).await.unwrap();
    assert_eq!(n, 500);
}

#[tokio::test]
async fn scan_match_randomkey_and_flush() {
    let mut c = common::connect(&[]).await;
    let none: Option<String> = redis::cmd("RANDOMKEY").query_async(&mut c).await.unwrap();
    assert_eq!(none, None);
    let _: () = c.set("user:1", "a").await.unwrap();
    let _: () = c.set("order:1", "b").await.unwrap();
    let (cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
        .arg(0)
        .arg("MATCH")
        .arg("user:*")
        .arg("COUNT")
        .arg(1000)
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(cursor, 0);
    assert_eq!(keys, vec!["user:1"]);
    let any: Option<String> = redis::cmd("RANDOMKEY").query_async(&mut c).await.unwrap();
    assert!(any.is_some());
    let _: () = redis::cmd("FLUSHALL").query_async(&mut c).await.unwrap();
    let n: i64 = redis::cmd("DBSIZE").query_async(&mut c).await.unwrap();
    assert_eq!(n, 0);
    let _: () = c.set("again", "1").await.unwrap();
    let _: () = redis::cmd("FLUSHDB")
        .arg("ASYNC")
        .query_async(&mut c)
        .await
        .unwrap();
    let n: i64 = redis::cmd("DBSIZE").query_async(&mut c).await.unwrap();
    assert_eq!(n, 0);
}

#[tokio::test]
async fn scan_rejects_a_bad_cursor_and_skips_expired_keys() {
    let mut c = common::connect(&[]).await;
    let err = redis::cmd("SCAN")
        .arg("abc")
        .query_async::<(u64, Vec<String>)>(&mut c)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("invalid cursor"), "{err}");
    let _: () = redis::cmd("SET")
        .arg("gone")
        .arg("v")
        .arg("PX")
        .arg(20)
        .query_async(&mut c)
        .await
        .unwrap();
    let _: () = c.set("kept", "v").await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    let (_, keys): (u64, Vec<String>) = redis::cmd("SCAN")
        .arg(0)
        .arg("COUNT")
        .arg(1000)
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(keys, vec!["kept"]);
}
