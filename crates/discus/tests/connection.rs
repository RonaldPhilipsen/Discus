mod common;

#[tokio::test]
async fn hello_switches_to_resp3_and_reports_version() {
    let addr = common::start_node(&[]).await;
    let reply = common::roundtrip(addr, b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n").await;
    assert!(reply.starts_with('%'), "{reply:?}");
    assert!(
        reply.contains("$7\r\nversion\r\n$5\r\n7.2.0\r\n"),
        "{reply:?}"
    );
    assert!(reply.contains("$5\r\nproto\r\n:3\r\n"), "{reply:?}");
    let bad = common::roundtrip(addr, b"*2\r\n$5\r\nHELLO\r\n$1\r\n4\r\n").await;
    assert!(bad.starts_with("-NOPROTO"), "{bad:?}");
}

#[tokio::test]
async fn auth_is_required_when_a_token_is_configured() {
    let addr = common::start_node(&["--shared-token", "s3cret"]).await;
    let denied = common::roundtrip(addr, b"*1\r\n$4\r\nPING\r\n").await;
    assert!(denied.starts_with("-NOAUTH"), "{denied:?}");
    let wire = b"*2\r\n$4\r\nAUTH\r\n$6\r\ns3cret\r\n*1\r\n$4\r\nPING\r\n";
    let ok = common::roundtrip(addr, wire).await;
    assert_eq!(ok, "+OK\r\n+PONG\r\n");
    let wrong = common::roundtrip(addr, b"*2\r\n$4\r\nAUTH\r\n$5\r\nwrong\r\n").await;
    assert!(wrong.starts_with("-WRONGPASS"), "{wrong:?}");
    let two = common::roundtrip(addr, b"*3\r\n$4\r\nAUTH\r\n$4\r\nuser\r\n$6\r\ns3cret\r\n").await;
    assert_eq!(two, "+OK\r\n");
    let hello = common::roundtrip(
        addr,
        b"*5\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$4\r\nAUTH\r\n$4\r\nuser\r\n$6\r\ns3cret\r\n",
    )
    .await;
    assert!(hello.starts_with('%'), "{hello:?}");
}

#[tokio::test]
async fn auth_without_a_configured_token_is_an_error() {
    let addr = common::start_node(&[]).await;
    let reply = common::roundtrip(addr, b"*2\r\n$4\r\nAUTH\r\n$1\r\nx\r\n").await;
    assert!(
        reply.starts_with("-ERR AUTH <password> called without any password"),
        "{reply:?}"
    );
}

#[tokio::test]
async fn info_select_config_and_cluster() {
    let addr = common::start_node(&[]).await;
    let info = common::roundtrip(addr, b"*1\r\n$4\r\nINFO\r\n").await;
    assert!(info.contains("redis_version:7.2.0"), "{info:?}");
    assert!(info.contains("# discus"), "{info:?}");
    assert!(info.contains("db0:keys=0"), "{info:?}");
    let sel = common::roundtrip(addr, b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n").await;
    assert!(sel.starts_with("-ERR DB index is out of range"), "{sel:?}");
    let sel0 = common::roundtrip(addr, b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n").await;
    assert_eq!(sel0, "+OK\r\n");
    let cfg = common::roundtrip(
        addr,
        b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$9\r\ndatabases\r\n",
    )
    .await;
    assert_eq!(cfg, "*2\r\n$9\r\ndatabases\r\n$1\r\n1\r\n");
    let cl = common::roundtrip(addr, b"*2\r\n$7\r\nCLUSTER\r\n$4\r\nINFO\r\n").await;
    assert!(
        cl.starts_with("-ERR Discus is not a Redis Cluster"),
        "{cl:?}"
    );
}

#[tokio::test]
async fn client_commands_and_command_listing() {
    let addr = common::start_node(&[]).await;
    let wire = b"*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$3\r\nfoo\r\n*2\r\n$6\r\nCLIENT\r\n$7\r\nGETNAME\r\n";
    let reply = common::roundtrip(addr, wire).await;
    assert_eq!(reply, "+OK\r\n$3\r\nfoo\r\n");
    let count = common::roundtrip(addr, b"*2\r\n$7\r\nCOMMAND\r\n$5\r\nCOUNT\r\n").await;
    assert!(count.starts_with(':'), "{count:?}");
    let id = common::roundtrip(addr, b"*2\r\n$6\r\nCLIENT\r\n$2\r\nID\r\n").await;
    assert!(id.starts_with(':'), "{id:?}");
}

#[tokio::test]
async fn command_info_and_getkeys() {
    let addr = common::start_node(&[]).await;
    let info = common::roundtrip(addr, b"*3\r\n$7\r\nCOMMAND\r\n$4\r\nINFO\r\n$3\r\nget\r\n").await;
    assert!(
        info.starts_with("*1\r\n*10\r\n$3\r\nget\r\n:2\r\n*1\r\n+readonly\r\n:1\r\n:1\r\n:1\r\n"),
        "{info:?}"
    );
    let unknown =
        common::roundtrip(addr, b"*3\r\n$7\r\nCOMMAND\r\n$4\r\nINFO\r\n$3\r\nnop\r\n").await;
    assert_eq!(unknown, "*1\r\n$-1\r\n");
    let keys = common::roundtrip(
        addr,
        b"*7\r\n$7\r\nCOMMAND\r\n$7\r\nGETKEYS\r\n$4\r\nMSET\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n",
    )
    .await;
    assert_eq!(keys, "*2\r\n$1\r\na\r\n$1\r\nb\r\n");
    let none = common::roundtrip(
        addr,
        b"*3\r\n$7\r\nCOMMAND\r\n$7\r\nGETKEYS\r\n$4\r\nPING\r\n",
    )
    .await;
    assert!(none.starts_with("-ERR"), "{none:?}");
}
