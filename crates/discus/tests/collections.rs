mod common;

use redis::AsyncCommands;
use std::collections::HashMap;

#[tokio::test]
async fn hash_set_and_zset_round_trips() {
    let mut c = common::connect(&[]).await;
    let _: () = c.hset("h", "f", "1").await.unwrap();
    let n: i64 = c.hincr("h", "f", 2).await.unwrap();
    assert_eq!(n, 3);
    let all: HashMap<String, String> = c.hgetall("h").await.unwrap();
    assert_eq!(all.get("f").map(String::as_str), Some("3"));
    let added: i64 = c.sadd("s", &["a", "b", "a"]).await.unwrap();
    assert_eq!(added, 2);
    let is: Vec<bool> = c.smismember("s", &["a", "z"]).await.unwrap();
    assert_eq!(is, vec![true, false]);
    let _: () = c.zadd("z", "m1", 1.5).await.unwrap();
    let _: () = c.zadd("z", "m2", 0.5).await.unwrap();
    let r: Vec<String> = c.zrange("z", 0, -1).await.unwrap();
    assert_eq!(r, vec!["m2", "m1"]);
    let count: i64 = c.zcount("z", 1.0, "+inf").await.unwrap();
    assert_eq!(count, 1);
    let ws: Vec<(String, f64)> = c.zrange_withscores("z", 0, -1).await.unwrap();
    assert_eq!(ws, vec![("m2".to_string(), 0.5), ("m1".to_string(), 1.5)]);
    let by: Vec<String> = c.zrangebyscore("z", "(0.5", "+inf").await.unwrap();
    assert_eq!(by, vec!["m1"]);
    let s: f64 = c.zincr("z", "m2", 1.0).await.unwrap();
    assert_eq!(s, 1.5);
}

#[tokio::test]
async fn wrong_type_and_empty_collection_deletion() {
    let mut c = common::connect(&[]).await;
    let _: () = c.set("k", "v").await.unwrap();
    let err = c.hset::<_, _, _, ()>("k", "f", "v").await.unwrap_err();
    assert!(err.to_string().contains("WRONGTYPE"), "{err}");
    let _: () = c.sadd("s", "only").await.unwrap();
    let _: i64 = c.srem("s", "only").await.unwrap();
    let exists: i64 = c.exists("s").await.unwrap();
    assert_eq!(exists, 0);
    let t: String = redis::cmd("TYPE")
        .arg("s")
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(t, "none");
}

#[tokio::test]
async fn hash_errors_spop_and_collection_size_guard() {
    let mut c = common::connect(&["--max-collection-members", "3"]).await;
    let _: () = c.hset("h", "f", "abc").await.unwrap();
    let err = c.hincr::<_, _, _, i64>("h", "f", 1).await.unwrap_err();
    assert!(err.to_string().contains("not an integer"), "{err}");
    let _: i64 = c.sadd("s", &["a", "b", "c"]).await.unwrap();
    let err = c.sadd::<_, _, i64>("s", "d").await.unwrap_err();
    assert!(err.to_string().contains("collection too large"), "{err}");
    let popped: String = c.spop("s").await.unwrap();
    assert!(["a", "b", "c"].contains(&popped.as_str()));
    let left: i64 = c.scard("s").await.unwrap();
    assert_eq!(left, 2);
    let some: Vec<String> = redis::cmd("SRANDMEMBER")
        .arg("s")
        .arg(-3)
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(some.len(), 3);
}
