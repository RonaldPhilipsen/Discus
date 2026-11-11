//! The control lane: one OS thread with a current-thread runtime for membership, the gossip
//! endpoint, and the admin server.
//!
//! These tasks must keep running when the workers of the data runtime are saturated or blocked,
//! so a peer does not declare the node dead and the kubelet does not restart it.

use std::future::Future;
use std::io;
use std::thread::JoinHandle as ThreadHandle;

use tokio::runtime::{Builder, Handle};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub struct ControlLane {
    handle: Handle,
    stop: Option<oneshot::Sender<()>>,
    thread: Option<ThreadHandle<()>>,
}

impl ControlLane {
    /// Start the `discus-control` thread.
    pub fn start() -> io::Result<ControlLane> {
        let runtime = Builder::new_current_thread().enable_all().build()?;
        let handle = runtime.handle().clone();
        let (stop, stopped) = oneshot::channel::<()>();
        let thread = std::thread::Builder::new()
            .name("discus-control".into())
            .spawn(move || {
                runtime.block_on(async {
                    let _ = stopped.await;
                });
            })?;
        Ok(ControlLane {
            handle,
            stop: Some(stop),
            thread: Some(thread),
        })
    }

    pub fn handle(&self) -> &Handle {
        &self.handle
    }

    pub fn spawn<F>(&self, f: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(f)
    }

    /// Stop the runtime, which drops its remaining tasks, and join the thread.
    pub fn shutdown(mut self) {
        self.signal();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }

    fn signal(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

impl Drop for ControlLane {
    /// A lane dropped without `shutdown` stops without a join, so a drop inside an async
    /// context never blocks on the thread.
    fn drop(&mut self) {
        self.signal();
    }
}
