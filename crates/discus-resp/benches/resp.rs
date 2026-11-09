use bytes::{Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use discus_resp::{encode, Limits, Parser, Protocol, Value};
use std::hint::black_box;

const VALUE: [u8; 256] = [b'x'; 256];

fn set_command() -> Vec<u8> {
    let mut wire = b"*3\r\n$3\r\nSET\r\n$16\r\nkey:000000000042\r\n$256\r\n".to_vec();
    wire.extend_from_slice(&VALUE);
    wire.extend_from_slice(b"\r\n");
    wire
}

const GET_COMMAND: &[u8] = b"*2\r\n$3\r\nGET\r\n$16\r\nkey:000000000042\r\n";

fn parse_all(parser: &mut Parser, wire: &[u8]) -> usize {
    parser.feed(wire);
    let mut commands = 0;
    while let Some(command) = parser.next().expect("the benchmark input is valid RESP") {
        black_box(command);
        commands += 1;
    }
    commands
}

fn parser(c: &mut Criterion) {
    let set = set_command();
    let pipeline = set.repeat(16);
    let mut group = c.benchmark_group("parser");

    for (name, wire, commands) in [
        ("get", GET_COMMAND, 1),
        ("set_256b", set.as_slice(), 1),
        ("set_256b_pipeline_16", pipeline.as_slice(), 16),
    ] {
        group.throughput(Throughput::Elements(commands));
        group.bench_function(name, |b| {
            // One parser for the whole run, as one connection keeps one parser.
            let mut parser = Parser::new(Limits::default());
            b.iter(|| parse_all(&mut parser, black_box(wire)));
        });
    }
    group.finish();
}

fn encoder(c: &mut Criterion) {
    let bulk = Value::bulk(Bytes::from_static(&VALUE));
    let mut group = c.benchmark_group("encoder");
    group.throughput(Throughput::Elements(1));
    group.bench_function("bulk_256b", |b| {
        b.iter_batched_ref(
            || BytesMut::with_capacity(512),
            |out| encode(black_box(&bulk), Protocol::Resp2, out),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(benches, parser, encoder);
criterion_main!(benches);
