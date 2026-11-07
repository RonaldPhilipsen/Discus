mod common;

use redis::AsyncCommands;

#[tokio::test]
async fn expire_ttl_persist_and_past_deadline() {
    let mut c = common::connect(&[]).await;
    let _: () = c.set("k", "v").await.unwrap();
    let ttl: i64 = c.ttl("k").await.unwrap();
    assert_eq!(ttl, -1);
    let set: i64 = c.expire("k", 100).await.unwrap();
    assert_eq!(set, 1);
    let ttl: i64 = c.ttl("k").await.unwrap();
    assert!((99..=100).contains(&ttl));
    let persisted: i64 = c.persist("k").await.unwrap();
    assert_eq!(persisted, 1);
    let gone: i64 = redis::cmd("EXPIREAT")
        .arg("k")
        .arg(1)
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(gone, 1);
    let exists: i64 = c.exists("k").await.unwrap();
    assert_eq!(exists, 0);
    let missing: i64 = c.ttl("k").await.unwrap();
    assert_eq!(missing, -2);
    let none: i64 = c.expire("k", 10).await.unwrap();
    assert_eq!(none, 0);
}

#[tokio::test]
async fn set_keepttl_preserves_the_deadline() {
    let mut c = common::connect(&[]).await;
    let _: () = redis::cmd("SET")
        .arg("k")
        .arg("v")
        .arg("EX")
        .arg("50")
        .query_async(&mut c)
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg("k")
        .arg("w")
        .arg("KEEPTTL")
        .query_async(&mut c)
        .await
        .unwrap();
    let ttl: i64 = c.ttl("k").await.unwrap();
    assert!((49..=50).contains(&ttl));
    let _: () = c.set("k", "x").await.unwrap();
    let ttl: i64 = c.ttl("k").await.unwrap();
    assert_eq!(ttl, -1);
}

#[tokio::test]
async fn pttl_and_px_expiry_are_removed_by_the_background_sweep() {
    let mut c = common::connect(&[]).await;
    let _: () = redis::cmd("SET")
        .arg("k")
        .arg("v")
        .arg("PX")
        .arg("150")
        .query_async(&mut c)
        .await
        .unwrap();
    let pttl: i64 = redis::cmd("PTTL")
        .arg("k")
        .query_async(&mut c)
        .await
        .unwrap();
    assert!((1..=150).contains(&pttl));
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    let n: i64 = c.exists("k").await.unwrap();
    assert_eq!(n, 0);
}
