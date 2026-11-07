//! A batch span processor with a bounded queue that counts what it drops (spec §15.4).
//!
//! The SDK `BatchSpanProcessor` keeps its drop count private, and the spec requires that dropped
//! spans are counted. This processor has the same shape: a bounded channel, one export thread,
//! and batches. `on_end` never blocks. When the queue is full, the span is dropped and counted.

use std::sync::atomic::Ordering;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use opentelemetry::Context;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::trace::{Span, SpanData, SpanExporter, SpanProcessor};
use opentelemetry_sdk::Resource;

use crate::metrics::Counters;

/// The largest batch handed to the exporter. A smaller queue also caps the batch, so at most
/// `2 * queue` spans wait in memory: the queue plus the batch in export.
const MAX_BATCH: usize = 512;
/// A non-empty batch is exported at least this often.
const MAX_DELAY: Duration = Duration::from_secs(1);
/// How often the worker checks for control messages while it waits for spans.
const POLL: Duration = Duration::from_millis(20);
/// How long a flush or a shutdown waits for the worker.
const CONTROL_TIMEOUT: Duration = Duration::from_secs(5);

enum Control {
    Resource(Resource),
    Flush(SyncSender<OTelSdkResult>),
    Shutdown(SyncSender<OTelSdkResult>),
}

/// See the module documentation.
pub struct BoundedSpanProcessor {
    spans: SyncSender<SpanData>,
    control: SyncSender<Control>,
    worker: Mutex<Option<JoinHandle<()>>>,
    counters: Arc<Counters>,
}

impl std::fmt::Debug for BoundedSpanProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundedSpanProcessor")
            .finish_non_exhaustive()
    }
}

impl BoundedSpanProcessor {
    /// Start the export thread. `queue` is the most spans that wait for export.
    pub fn new<E: SpanExporter + 'static>(
        exporter: E,
        queue: usize,
        counters: Arc<Counters>,
    ) -> BoundedSpanProcessor {
        let queue = queue.max(1);
        let (spans, span_rx) = mpsc::sync_channel(queue);
        let (control, control_rx) = mpsc::sync_channel(8);
        let worker_counters = counters.clone();
        let max_batch = queue.min(MAX_BATCH);
        let worker = std::thread::Builder::new()
            .name("discus-span-export".into())
            .spawn(move || {
                Worker::new(exporter, worker_counters, max_batch).run(span_rx, control_rx)
            })
            .ok();
        BoundedSpanProcessor {
            spans,
            control,
            worker: Mutex::new(worker),
            counters,
        }
    }

    fn call(&self, make: impl FnOnce(SyncSender<OTelSdkResult>) -> Control) -> OTelSdkResult {
        let (tx, rx) = mpsc::sync_channel(1);
        self.control
            .send(make(tx))
            .map_err(|_| OTelSdkError::AlreadyShutdown)?;
        rx.recv_timeout(CONTROL_TIMEOUT)
            .map_err(|_| OTelSdkError::Timeout(CONTROL_TIMEOUT))?
    }
}

impl SpanProcessor for BoundedSpanProcessor {
    fn on_start(&self, _span: &mut Span, _cx: &Context) {}

    fn on_end(&self, span: SpanData) {
        if !span.span_context.is_sampled() {
            return;
        }
        match self.spans.try_send(span) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.counters.spans_dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.call(Control::Flush)
    }

    fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
        let result = self.call(Control::Shutdown);
        // Join only a worker that answered. A worker stuck in a hung export is left behind, so
        // shutdown never blocks for longer than the control timeout.
        if result.is_ok() {
            if let Some(handle) = self.worker.lock().ok().and_then(|mut w| w.take()) {
                let _ = handle.join();
            }
        }
        result
    }

    fn set_resource(&mut self, resource: &Resource) {
        let _ = self.control.send(Control::Resource(resource.clone()));
    }
}

struct Worker<E> {
    exporter: E,
    counters: Arc<Counters>,
    batch: Vec<SpanData>,
    max_batch: usize,
    last_export: Instant,
}

impl<E: SpanExporter> Worker<E> {
    fn new(exporter: E, counters: Arc<Counters>, max_batch: usize) -> Worker<E> {
        Worker {
            exporter,
            counters,
            batch: Vec::with_capacity(max_batch),
            max_batch,
            last_export: Instant::now(),
        }
    }

    fn run(mut self, spans: Receiver<SpanData>, control: Receiver<Control>) {
        loop {
            match control.try_recv() {
                Ok(Control::Resource(r)) => self.exporter.set_resource(&r),
                Ok(Control::Flush(reply)) => {
                    let result = self.drain(&spans);
                    let _ = reply.send(result.and_then(|()| self.exporter.force_flush()));
                }
                Ok(Control::Shutdown(reply)) => {
                    let result = self.drain(&spans);
                    let _ = reply.send(result.and_then(|()| self.exporter.shutdown()));
                    return;
                }
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => {
                    let _ = self.drain(&spans);
                    return;
                }
            }
            match spans.recv_timeout(POLL) {
                Ok(span) => {
                    self.batch.push(span);
                    if self.batch.len() >= self.max_batch {
                        let _ = self.export();
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    let _ = self.export();
                    return;
                }
            }
            if !self.batch.is_empty() && self.last_export.elapsed() >= MAX_DELAY {
                let _ = self.export();
            }
        }
    }

    /// Export every queued span.
    fn drain(&mut self, spans: &Receiver<SpanData>) -> OTelSdkResult {
        let mut result = Ok(());
        while let Ok(span) = spans.try_recv() {
            self.batch.push(span);
            if self.batch.len() >= self.max_batch {
                result = result.and(self.export());
            }
        }
        result.and(self.export())
    }

    fn export(&mut self) -> OTelSdkResult {
        self.last_export = Instant::now();
        if self.batch.is_empty() {
            return Ok(());
        }
        let batch = std::mem::replace(&mut self.batch, Vec::with_capacity(self.max_batch));
        let result = futures_executor::block_on(self.exporter.export(batch));
        if result.is_err() {
            self.counters
                .span_export_failures
                .fetch_add(1, Ordering::Relaxed);
        }
        result
    }
}
