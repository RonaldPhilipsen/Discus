use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use discus_store::{Entry, Store, StoreConfig, SystemClock, Value, Version};

const KEYS: u64 = 100_000;
const THREADS: u64 = 8;
static NEXT_ITERATION: AtomicU64 = AtomicU64::new(0);

fn key(i: u64) -> Vec<u8> {
    format!("key:{:012}", i % KEYS).into_bytes()
}

fn put(store: &Store, key: &[u8], value: &Bytes) {
    store.write(0, key, Version::zero(), |slot| {
        slot.set(Some(Entry::new(Value::Str(value.clone()), None)));
    });
}

fn filled_store() -> (Arc<Store>, Bytes) {
    let store = Arc::new(Store::new(StoreConfig::default(), Arc::new(SystemClock)));
    let value = Bytes::from(vec![b'x'; 256]);
    for i in 0..KEYS {
        put(&store, &key(i), &value);
    }
    (store, value)
}

// Keys are spread by a multiplication so that consecutive operations do not share a cache line.
fn spread(i: u64) -> u64 {
    i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 16
}

/// Runs `op` on `THREADS` threads and returns the slowest thread's time, so that the reported
/// rate is the rate of the whole group.
fn contended(iters: u64, op: impl Fn(u64, u64) + Send + Sync + 'static) -> Duration {
    let op = Arc::new(op);
    let barrier = Arc::new(Barrier::new(THREADS as usize));
    // Each sample continues where the last one ended. If every sample restarted at zero, short
    // samples would touch the same keys again and read them from the CPU cache.
    let first = NEXT_ITERATION.fetch_add(iters, Ordering::Relaxed);
    let threads: Vec<_> = (0..THREADS)
        .map(|t| {
            let (op, barrier) = (op.clone(), barrier.clone());
            thread::spawn(move || {
                barrier.wait();
                let start = Instant::now();
                for i in first..first + iters / THREADS {
                    op(t, i);
                }
                start.elapsed()
            })
        })
        .collect();
    threads
        .into_iter()
        .map(|t| t.join().expect("a benchmark thread panicked"))
        .max()
        .unwrap_or_default()
}

fn store(c: &mut Criterion) {
    let (store, value) = filled_store();
    let mut group = c.benchmark_group("store");
    group.throughput(Throughput::Elements(1));

    let mut i = 0;
    group.bench_function("read_hit", |b| {
        b.iter(|| {
            i += 1;
            store.read(0, &key(spread(i)), |entry| {
                black_box(entry.is_some());
            })
        });
    });
    group.bench_function("write_replace", |b| {
        b.iter(|| {
            i += 1;
            put(&store, &key(spread(i)), &value);
        });
    });
    group.bench_function("read_hit_8_threads", |b| {
        b.iter_custom(|iters| {
            let store = store.clone();
            contended(iters, move |t, i| {
                store.read(0, &key(spread(i + (t << 40))), |entry| {
                    black_box(entry.is_some());
                });
            })
        });
    });
    group.bench_function("write_replace_8_threads", |b| {
        b.iter_custom(|iters| {
            let (store, value) = (store.clone(), value.clone());
            contended(iters, move |t, i| {
                put(&store, &key(spread(i + (t << 40))), &value);
            })
        });
    });
    group.finish();
}

criterion_group!(benches, store);
criterion_main!(benches);
