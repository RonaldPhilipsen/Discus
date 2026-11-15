//! Accept loops that survive accept errors.
//!
//! `accept` fails for reasons that pass, for example `EMFILE` at the descriptor limit or
//! `ECONNABORTED` for a client that left before the accept. The listener keeps working, so the
//! node waits and accepts again instead of stopping.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::time::Instant;

const FIRST_DELAY: Duration = Duration::from_millis(10);
const MAX_DELAY: Duration = Duration::from_secs(1);

/// A listener that accepts one connection at a time.
pub(crate) trait Accept {
    type Conn;
    fn accept(&self) -> impl Future<Output = io::Result<Self::Conn>> + Send;
}

impl Accept for TcpListener {
    type Conn = (TcpStream, SocketAddr);
    fn accept(&self) -> impl Future<Output = io::Result<Self::Conn>> + Send {
        TcpListener::accept(self)
    }
}

/// The wait after consecutive accept errors: 10 ms, doubled per error up to 1 s, and reset by a
/// success. The deadline lives here, so a select that drops the accept future during the wait
/// does not shorten it.
#[derive(Debug, Default)]
pub(crate) struct Backoff {
    delay: Duration,
    resume_at: Option<Instant>,
}

impl Backoff {
    fn failed(&mut self) -> Duration {
        self.delay = if self.delay.is_zero() {
            FIRST_DELAY
        } else {
            (self.delay * 2).min(MAX_DELAY)
        };
        self.resume_at = Some(Instant::now() + self.delay);
        self.delay
    }
}

/// The next connection from `listener`. An error is logged and retried after the backoff.
pub(crate) async fn accept<A: Accept>(
    listener: &A,
    backoff: &mut Backoff,
    name: &'static str,
) -> A::Conn {
    loop {
        if let Some(at) = backoff.resume_at {
            tokio::time::sleep_until(at).await;
            backoff.resume_at = None;
        }
        match listener.accept().await {
            Ok(conn) => {
                backoff.delay = Duration::ZERO;
                return conn;
            }
            Err(e) => {
                let wait = backoff.failed();
                tracing::warn!(
                    listener = name,
                    error = %e,
                    retry_ms = wait.as_millis() as u64,
                    "accept failed, retrying"
                );
            }
        }
    }
}

/// Like `accept`, for a listener that may be absent. Without one, it never returns.
pub(crate) async fn accept_on<A: Accept>(
    listener: Option<&A>,
    backoff: &mut Backoff,
    name: &'static str,
) -> A::Conn {
    match listener {
        Some(l) => accept(l, backoff, name).await,
        None => std::future::pending().await,
    }
}

/// Raise the soft `RLIMIT_NOFILE` to the hard limit. Each client holds one descriptor, so warn
/// when the result leaves too little room for `max_clients` plus the descriptors of the node.
pub fn raise_nofile_limit(max_clients: usize) {
    const HEADROOM: u64 = 1024;
    let (soft, hard) = match rlimit::increase_nofile_limit(u64::MAX) {
        Ok(soft) => (
            soft,
            rlimit::Resource::NOFILE.get().map_or(soft, |(_, h)| h),
        ),
        Err(e) => {
            tracing::warn!(error = %e, "cannot raise the open file limit");
            match rlimit::Resource::NOFILE.get() {
                Ok(limits) => limits,
                Err(_) => return,
            }
        }
    };
    if let Some(needed) = nofile_shortfall(soft, max_clients, HEADROOM) {
        tracing::warn!(
            soft_limit = soft,
            hard_limit = hard,
            max_clients,
            needed,
            "the open file limit is below --max-clients plus the descriptors of the node, so \
             accept fails before the client limit is reached"
        );
    }
}

/// The descriptors needed when `soft` is below `max_clients` plus `headroom`. A `max_clients`
/// of 0 means no limit, and then nothing is too low.
fn nofile_shortfall(soft: u64, max_clients: usize, headroom: u64) -> Option<u64> {
    if max_clients == 0 {
        return None;
    }
    let needed = (max_clients as u64).saturating_add(headroom);
    (soft < needed).then_some(needed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A listener whose first `fail` accepts return `EMFILE`.
    struct Flaky {
        inner: TcpListener,
        fail: AtomicU32,
    }

    impl Accept for Flaky {
        type Conn = (TcpStream, SocketAddr);
        async fn accept(&self) -> io::Result<Self::Conn> {
            if self
                .fail
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(io::Error::from_raw_os_error(EMFILE));
            }
            self.inner.accept().await
        }
    }

    /// `EMFILE` on Linux and macOS.
    const EMFILE: i32 = 24;

    #[tokio::test]
    async fn an_accept_error_does_not_stop_the_listener() {
        let inner = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = inner.local_addr().unwrap();
        let listener = Flaky {
            inner,
            fail: AtomicU32::new(1),
        };
        let client = tokio::spawn(TcpStream::connect(addr));
        let mut backoff = Backoff::default();
        let (_, peer) = accept(&listener, &mut backoff, "client").await;
        let client = client.await.unwrap().unwrap();
        assert_eq!(peer, client.local_addr().unwrap());
        assert_eq!(listener.fail.load(Ordering::Relaxed), 0);
        assert!(backoff.delay.is_zero(), "a success resets the backoff");
    }

    #[test]
    fn the_backoff_doubles_up_to_one_second() {
        let mut b = Backoff::default();
        let waits: Vec<u64> = (0..9).map(|_| b.failed().as_millis() as u64).collect();
        assert_eq!(waits, [10, 20, 40, 80, 160, 320, 640, 1000, 1000]);
    }

    #[test]
    fn a_low_file_limit_is_reported() {
        assert_eq!(nofile_shortfall(1024, 10_000, 1024), Some(11_024));
        assert_eq!(nofile_shortfall(1 << 20, 10_000, 1024), None);
        assert_eq!(nofile_shortfall(16, 0, 1024), None);
    }
}
