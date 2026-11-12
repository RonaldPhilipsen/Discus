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

#[tokio::test]
async fn del_then_exists_type_dbsize_ignore_the_tombstone() {
    let mut c = common::connect(&[]).await;
    let _: () = c.set("gone", "v").await.unwrap();
    let _: () = c.set("kept", "v").await.unwrap();
    let n: i64 = c.del("gone").await.unwrap();
    assert_eq!(n, 1);
    let n: i64 = c.del("gone").await.unwrap();
    assert_eq!(n, 0);
    let n: i64 = c.exists("gone").await.unwrap();
    assert_eq!(n, 0);
    let t: String = redis::cmd("TYPE")
        .arg("gone")
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(t, "none");
    let n: i64 = redis::cmd("DBSIZE").query_async(&mut c).await.unwrap();
    assert_eq!(n, 1);
    let info: String = redis::cmd("INFO")
        .arg("discus")
        .query_async(&mut c)
        .await
        .unwrap();
    assert!(info.contains("discus_tombstones:1"), "{info}");
    let (_, keys): (u64, Vec<String>) = redis::cmd("SCAN")
        .arg(0)
        .arg("COUNT")
        .arg(100)
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(keys, vec!["kept".to_string()]);
}

#[tokio::test]
async fn rename_moves_the_value_and_keeps_the_deadline() {
    let mut c = common::connect(&[]).await;
    let _: () = redis::cmd("SET")
        .arg("a")
        .arg("v")
        .arg("EX")
        .arg(100)
        .query_async(&mut c)
        .await
        .unwrap();
    let reply: String = redis::cmd("RENAME")
        .arg("a")
        .arg("b")
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(reply, "OK");
    let get_b: String = c.get("b").await.unwrap();
    let exists_a: i64 = c.exists("a").await.unwrap();
    let pttl_b: i64 = redis::cmd("PTTL")
        .arg("b")
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(get_b, "v");
    assert_eq!(exists_a, 0);
    assert!(pttl_b > 0 && pttl_b <= 100_000, "{pttl_b}");
}

#[tokio::test]
async fn rename_of_a_missing_key_fails() {
    let mut c = common::connect(&[]).await;
    let err = redis::cmd("RENAME")
        .arg("a")
        .arg("b")
        .query_async::<String>(&mut c)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("no such key"), "{err}");
}

#[tokio::test]
async fn rename_overwrites_the_destination() {
    let mut c = common::connect(&[]).await;
    let _: () = c.set("a", "from-a").await.unwrap();
    let _: () = c.set("b", "from-b").await.unwrap();
    let _: () = redis::cmd("RENAME")
        .arg("a")
        .arg("b")
        .query_async(&mut c)
        .await
        .unwrap();
    let get_b: String = c.get("b").await.unwrap();
    assert_eq!(get_b, "from-a");
}

#[tokio::test]
async fn rename_to_the_same_key_changes_nothing() {
    let mut c = common::connect(&[]).await;
    let _: () = c.set("a", "v").await.unwrap();
    let reply: String = redis::cmd("RENAME")
        .arg("a")
        .arg("a")
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(reply, "OK");
    let get_a: String = c.get("a").await.unwrap();
    assert_eq!(get_a, "v");
}

#[tokio::test]
async fn rename_of_a_counter_key_is_refused() {
    let mut c = common::connect(&[]).await;
    let err = redis::cmd("RENAME")
        .arg("cnt:a")
        .arg("cnt:b")
        .query_async::<String>(&mut c)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("counter keys"), "{err}");
}
