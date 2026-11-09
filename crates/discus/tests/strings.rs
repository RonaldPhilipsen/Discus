mod common;

use redis::AsyncCommands;

#[tokio::test]
async fn set_get_incr_and_errors() {
    let mut c = common::connect(&[]).await;
    let _: () = c.set("n", 41).await.unwrap();
    let n: i64 = c.incr("n", 1).await.unwrap();
    assert_eq!(n, 42);
    let _: () = c.set("s", "abc").await.unwrap();
    let err = c.incr::<_, _, i64>("s", 1).await.unwrap_err();
    assert!(err.to_string().contains("not an integer"), "{err}");
    let s: String = c.get("s").await.unwrap();
    assert_eq!(s, "abc");
    let _: () = c.set("max", i64::MAX).await.unwrap();
    let err = c.incr::<_, _, i64>("max", 1).await.unwrap_err();
    assert!(err.to_string().contains("overflow"), "{err}");
    let still: i64 = c.get("max").await.unwrap();
    assert_eq!(still, i64::MAX);
}

#[tokio::test]
async fn set_options_nx_xx_get_and_syntax_errors() {
    let mut c = common::connect(&[]).await;
    let first: Option<String> = redis::cmd("SET")
        .arg("k")
        .arg("1")
        .arg("NX")
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(first.as_deref(), Some("OK"));
    let second: Option<String> = redis::cmd("SET")
        .arg("k")
        .arg("2")
        .arg("NX")
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(second, None);
    let old: Option<String> = redis::cmd("SET")
        .arg("k")
        .arg("3")
        .arg("GET")
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(old.as_deref(), Some("1"));
    let err = redis::cmd("SET")
        .arg("k")
        .arg("4")
        .arg("NX")
        .arg("XX")
        .query_async::<String>(&mut c)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("syntax error"), "{err}");
    let err = redis::cmd("SET")
        .arg("k")
        .arg("4")
        .arg("EX")
        .arg("1")
        .arg("PX")
        .arg("1")
        .query_async::<String>(&mut c)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("syntax error"), "{err}");
}

#[tokio::test]
async fn multi_key_and_misc_string_commands() {
    let mut c = common::connect(&[]).await;
    let _: () = redis::cmd("MSET")
        .arg("a")
        .arg("1")
        .arg("b")
        .arg("2")
        .query_async(&mut c)
        .await
        .unwrap();
    let vals: Vec<Option<String>> = c.mget(&["a", "zz", "b"]).await.unwrap();
    assert_eq!(vals, vec![Some("1".into()), None, Some("2".into())]);
    let n: i64 = c.append("a", "xyz").await.unwrap();
    assert_eq!(n, 4);
    let l: i64 = c.strlen("a").await.unwrap();
    assert_eq!(l, 4);
    let t: String = redis::cmd("TYPE")
        .arg("a")
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(t, "string");
    let got: Option<String> = c.get_del("a").await.unwrap();
    assert_eq!(got.as_deref(), Some("1xyz"));
    let removed: i64 = c.del(&["a", "b", "nope"]).await.unwrap();
    assert_eq!(removed, 1);
    let f: String = redis::cmd("INCRBYFLOAT")
        .arg("f")
        .arg("1.5")
        .query_async(&mut c)
        .await
        .unwrap();
    assert_eq!(f, "1.5");
}

/// Redis adds in a `long double` (binary128 here, as on 64-bit ARM Linux) and prints 17 places
/// without trailing zeros, so these sums differ from the shortest f64 text.
#[tokio::test]
async fn incrbyfloat_formats_like_redis() {
    let mut c = common::connect(&[]).await;
    for (start, by, want) in [
        ("0.1", "0.2", "0.3"),
        ("0", "1e30", "1000000000000000000000000000000"),
        ("0", "1e-20", "0"),
        ("12345.678", "0", "12345.678"),
    ] {
        let _: () = c.set("f", start).await.unwrap();
        let got: String = redis::cmd("INCRBYFLOAT")
            .arg("f")
            .arg(by)
            .query_async(&mut c)
            .await
            .unwrap();
        assert_eq!(got, want, "{start} + {by}");
        let stored: String = c.get("f").await.unwrap();
        assert_eq!(stored, want);
    }
}

#[tokio::test]
async fn key_and_value_limits_are_enforced() {
    let mut c = common::connect(&["--max-key-bytes", "8", "--max-value-bytes", "16"]).await;
    let err = c
        .set::<_, _, ()>("a-key-that-is-long", "v")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("key too large"), "{err}");
    let err = c.set::<_, _, ()>("k", "x".repeat(17)).await.unwrap_err();
    assert!(err.to_string().contains("value too large"), "{err}");
    let _: () = c.set("k", "x".repeat(16)).await.unwrap();
}

#[tokio::test]
async fn incr_with_extra_arguments_keeps_the_redis_error() {
    let mut c = common::connect(&[]).await;
    for args in [
        &["INCR", "n", "EX", "10"][..],
        &["INCRBY", "n", "1", "EX", "10"][..],
    ] {
        let mut cmd = redis::cmd(args[0]);
        for a in &args[1..] {
            cmd.arg(*a);
        }
        let err = cmd.query_async::<i64>(&mut c).await.unwrap_err();
        assert!(
            err.to_string().contains("wrong number of arguments"),
            "{err}"
        );
    }
}
