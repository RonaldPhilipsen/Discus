#![allow(clippy::unwrap_used)]

mod common;

use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;

async fn select(c: &mut MultiplexedConnection, db: i64) -> redis::RedisResult<()> {
    redis::cmd("SELECT").arg(db).query_async(c).await
}

async fn client(addr: std::net::SocketAddr) -> MultiplexedConnection {
    common::client_tls::redis_client(&format!("rediss://{addr}"))
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

#[tokio::test]
async fn the_same_key_in_three_databases() {
    let mut c = common::connect(&["--databases", "3"]).await;
    for n in 0..3 {
        select(&mut c, n).await.unwrap();
        let _: () = c.set("k", format!("v{n}")).await.unwrap();
    }
    for n in 0..3 {
        select(&mut c, n).await.unwrap();
        let v: String = c.get("k").await.unwrap();
        assert_eq!(v, format!("v{n}"));
    }
}

#[tokio::test]
async fn a_pipelined_select_applies_to_the_commands_after_it() {
    let mut c = common::connect(&["--databases", "3"]).await;
    let replies: (
        String,
        String,
        Option<String>,
        String,
        String,
        Option<String>,
    ) = redis::pipe()
        .cmd("SET")
        .arg("k")
        .arg("a")
        .cmd("SELECT")
        .arg(1)
        .cmd("GET")
        .arg("k")
        .cmd("SET")
        .arg("k")
        .arg("b")
        .cmd("SELECT")
        .arg(0)
        .cmd("GET")
        .arg("k")
        .query_async(&mut c)
        .await
        .unwrap();
    let ok = || "OK".to_string();
    assert_eq!(
        replies,
        (ok(), ok(), None, ok(), ok(), Some("a".to_string()))
    );
}

#[tokio::test]
async fn a_default_node_keeps_one_database() {
    let mut c = common::connect(&[]).await;
    let err = select(&mut c, 1).await.unwrap_err();
    assert!(
        err.to_string().contains("DB index is out of range"),
        "{err}"
    );
    let got: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("databases")
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(got, ["databases", "1"]);
}

#[tokio::test]
async fn a_client_without_select_uses_database_zero() {
    let addr = common::start_node(&["--databases", "3"]).await;
    let mut writer = client(addr).await;
    let _: () = writer.set("k", "zero").await.unwrap();
    let mut reader = client(addr).await;
    select(&mut reader, 1).await.unwrap();
    let none: Option<String> = reader.get("k").await.unwrap();
    assert_eq!(none, None);
    select(&mut reader, 0).await.unwrap();
    let v: String = reader.get("k").await.unwrap();
    assert_eq!(v, "zero");
}
