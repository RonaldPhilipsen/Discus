use discus_resp::{Limits, ParseError, Parser};

fn cmd(parts: &[&str]) -> Vec<bytes::Bytes> {
    parts.iter().map(|p| bytes::Bytes::copy_from_slice(p.as_bytes())).collect()
}

#[test]
fn parses_a_complete_command() {
    let mut p = Parser::new(Limits::default());
    p.feed(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");
    assert_eq!(p.next().unwrap(), Some(cmd(&["GET", "k"])));
    assert_eq!(p.next().unwrap(), None);
}

#[test]
fn parses_across_arbitrary_chunk_boundaries() {
    let wire = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\nvv\r\n*1\r\n$4\r\nPING\r\n";
    for split in 1..wire.len() {
        let mut p = Parser::new(Limits::default());
        p.feed(&wire[..split]);
        p.feed(&wire[split..]);
        assert_eq!(p.next().unwrap(), Some(cmd(&["SET", "k", "vv"])));
        assert_eq!(p.next().unwrap(), Some(cmd(&["PING"])));
        assert_eq!(p.next().unwrap(), None);
    }
}

#[test]
fn rejects_oversize_bulk_before_allocating() {
    let mut p = Parser::new(Limits { max_bulk_bytes: 8, max_array_len: 16 });
    p.feed(b"*2\r\n$3\r\nSET\r\n$999999999\r\n");
    let err = p.next().unwrap_err();
    assert!(matches!(err, ParseError::BulkTooLarge { len: 999999999, limit: 8 }));
}

#[test]
fn rejects_negative_lengths_and_nested_arrays() {
    let mut p = Parser::new(Limits::default());
    p.feed(b"*1\r\n$-5\r\n");
    assert!(matches!(p.next().unwrap_err(), ParseError::Protocol(_)));
    let mut p = Parser::new(Limits::default());
    p.feed(b"*1\r\n*1\r\n$1\r\nx\r\n");
    assert!(matches!(p.next().unwrap_err(), ParseError::Protocol(_)));
}

#[test]
fn rejects_oversize_array() {
    let mut p = Parser::new(Limits { max_bulk_bytes: 64, max_array_len: 4 });
    p.feed(b"*5\r\n");
    assert!(matches!(p.next().unwrap_err(), ParseError::ArrayTooLarge { len: 5, limit: 4 }));
}
