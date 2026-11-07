use bytes::BytesMut;
use discus_resp::{encode, Protocol, Value};

fn enc(v: &Value, p: Protocol) -> Vec<u8> {
    let mut out = BytesMut::new();
    encode(v, p, &mut out);
    out.to_vec()
}

#[test]
fn encodes_scalars_identically_in_both_protocols() {
    assert_eq!(enc(&Value::ok(), Protocol::Resp2), b"+OK\r\n");
    assert_eq!(enc(&Value::error("ERR x"), Protocol::Resp3), b"-ERR x\r\n");
    assert_eq!(enc(&Value::Integer(-7), Protocol::Resp2), b":-7\r\n");
    assert_eq!(enc(&Value::bulk("ab"), Protocol::Resp3), b"$2\r\nab\r\n");
}

#[test]
fn null_map_boolean_and_double_downgrade_in_resp2() {
    assert_eq!(enc(&Value::Null, Protocol::Resp2), b"$-1\r\n");
    assert_eq!(enc(&Value::Null, Protocol::Resp3), b"_\r\n");
    let map = Value::Map(vec![(Value::bulk("k"), Value::Integer(1))]);
    assert_eq!(enc(&map, Protocol::Resp2), b"*2\r\n$1\r\nk\r\n:1\r\n");
    assert_eq!(enc(&map, Protocol::Resp3), b"%1\r\n$1\r\nk\r\n:1\r\n");
    assert_eq!(enc(&Value::Boolean(true), Protocol::Resp2), b":1\r\n");
    assert_eq!(enc(&Value::Boolean(true), Protocol::Resp3), b"#t\r\n");
    assert_eq!(enc(&Value::Double(1.5), Protocol::Resp2), b"$3\r\n1.5\r\n");
    assert_eq!(enc(&Value::Double(1.5), Protocol::Resp3), b",1.5\r\n");
}

#[test]
fn encodes_nested_arrays() {
    let v = Value::Array(vec![Value::Array(vec![]), Value::Null]);
    assert_eq!(enc(&v, Protocol::Resp2), b"*2\r\n*0\r\n$-1\r\n");
}
