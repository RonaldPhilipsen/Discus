use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use discus::lag::LagProbe;
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lag_probe_reports_a_blocked_runtime() {
    let stop = CancellationToken::new();
    let probe = LagProbe::new();
    probe.spawn(stop.clone());
    tokio::time::sleep(Duration::from_millis(100)).await;

    let blocked = Arc::new(AtomicBool::new(true));
    let poller = {
        let (probe, blocked) = (probe.clone(), blocked.clone());
        std::thread::spawn(move || {
            let mut seen = false;
            while blocked.load(Ordering::Acquire) {
                seen |= probe.lag_ms() >= 400;
                std::thread::sleep(Duration::from_millis(5));
            }
            seen
        })
    };
    let blockers: Vec<_> = (0..2)
        .map(|_| tokio::spawn(async { std::thread::sleep(Duration::from_secs(2)) }))
        .collect();
    for b in blockers {
        b.await.unwrap();
    }
    blocked.store(false, Ordering::Release);
    assert!(poller.join().unwrap(), "a blocked runtime must show as lag");

    let give_up = Instant::now() + Duration::from_secs(30);
    while probe.lag_ms() >= 400 {
        assert!(Instant::now() < give_up, "lag did not recover");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    stop.cancel();
}
