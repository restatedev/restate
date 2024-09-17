use std::{
    collections::VecDeque,
    sync::{Arc, Weak},
    time::Duration,
};

use restate_core::{cancellation_token, ShutdownError, TaskCenter, TaskKind};
use tokio::sync::mpsc;

use restate_types::{
    logs::{LogletOffset, Record, SequenceNumber},
    net::log_server::{Store, StoreFlags, Stored},
};

use crate::loglet::Resolver;

use super::{
    node::{Node, NodeClient, ReplicationChecker, Tracker},
    Event, SequencerGlobalState,
};

#[derive(Debug)]
pub(crate) struct Payload {
    pub first_offset: LogletOffset,
    pub records: Arc<[Record]>,
}

impl Payload {
    pub fn inflight_tail(&self) -> Option<LogletOffset> {
        let len = u32::try_from(self.records.len()).ok()?;
        self.first_offset.checked_add(len).map(Into::into)
    }
}

#[derive(derive_more::Debug)]
pub(crate) struct Batch<T>
where
    T: Tracker,
{
    pub payload: Arc<Payload>,
    pub tracker: T,
    #[debug(ignore)]
    pub resolver: Resolver,
}

pub struct SendPermit<'a> {
    inner: mpsc::Permit<'a, Weak<Payload>>,
}

impl<'a> SendPermit<'a> {
    pub(crate) fn send(self, payload: Weak<Payload>) {
        self.inner.send(payload)
    }
}

pub(crate) enum WorkerEvent {
    Stored(Stored),
}

#[derive(Clone, Debug)]
pub struct NodeWorkerHandle {
    batch_tx: mpsc::Sender<Weak<Payload>>,
    event_tx: mpsc::Sender<WorkerEvent>,
}

impl NodeWorkerHandle {
    /// reserve a send slot on the worker queue
    pub fn reserve(&self) -> Result<SendPermit, mpsc::error::TrySendError<()>> {
        Ok(SendPermit {
            inner: self.batch_tx.try_reserve()?,
        })
    }

    pub(crate) async fn notify(&self, event: WorkerEvent) {
        let _ = self.event_tx.send(event).await;
    }
}

pub(crate) struct NodeWorker<C> {
    batch_rx: mpsc::Receiver<Weak<Payload>>,
    event_rx: mpsc::Receiver<WorkerEvent>,
    node: Node<C>,
    global: Arc<SequencerGlobalState>,
    buffer: VecDeque<Weak<Payload>>,
}

impl<C> NodeWorker<C>
where
    C: NodeClient + Send + Sync + 'static,
{
    pub fn start(
        tc: &TaskCenter,
        node: Node<C>,
        queue_size: usize,
        global: Arc<SequencerGlobalState>,
    ) -> Result<NodeWorkerHandle, ShutdownError> {
        // we create the channel at a 10% capacity of the full buffer size
        // since pending batches will be queued in a VecDequeue
        let (batch_tx, batch_rx) = mpsc::channel(std::cmp::max(1, queue_size / 10));
        let (event_tx, event_rx) = mpsc::channel(1);
        let handle = NodeWorkerHandle { batch_tx, event_tx };

        let buffer = VecDeque::with_capacity(queue_size);
        let worker = NodeWorker {
            batch_rx,
            event_rx,
            node,
            global,
            buffer,
        };

        tc.spawn_unmanaged(TaskKind::Disposable, "appender", None, worker.run())?;

        Ok(handle)
    }

    async fn run(mut self) {
        let token = cancellation_token();

        loop {
            // the first loop, we try to process
            // all batches as they arrive and at the same
            // time handle events regarding batches
            // being processed by the node.
            loop {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => {
                        return;
                    }
                    Some(event) = self.event_rx.recv() => {
                        self.process_event(event);
                    }
                    Some(batch) = self.batch_rx.recv() => {
                        self.process_batch(batch).await;
                    }
                }

                // there is a chance here that buffer got filled up with pending batches
                // that has never received a `stored` event.
                // Hence we need to break out of this loop to stop accepting more batches
                // to write!
                //
                // note: this should be == comparison but just in case
                if self.buffer.len() >= self.buffer.capacity() {
                    break;
                }
            }

            // we can only reach here if we stopped receiving `stored` events
            // in that case we will stop receiving more batches and only wait
            // for the stored events or retry
            let mut timer = tokio::time::interval(Duration::from_millis(250));
            loop {
                // in this loop we only handle events (in case we can drain finally)
                // but we don't accept any more batches. This will put back pressure
                // since the replication policy will not be able to reserve this
                // node anymore!
                tokio::select! {
                    _ = token.cancelled() => {
                        return;
                    }
                    Some(event) = self.event_rx.recv() => {
                        self.process_event(event);
                    }
                    _ = timer.tick() => {
                        self.retry().await;
                    }
                }

                if self.buffer.len() < self.buffer.capacity() {
                    // we made progress and we can break out of this inner
                    // loop.
                    break;
                }
            }
        }
    }

    async fn retry(&self) {
        // retry to send all items in the batch
        for batch in self.buffer.iter() {
            self.process_once(batch).await;
        }
    }

    fn process_event(&mut self, event: WorkerEvent) {
        match event {
            WorkerEvent::Stored(stored) => {
                self.drain(stored);
            }
        }
    }

    fn drain(&mut self, event: Stored) {
        let mut trim = 0;
        for (i, batch) in self.buffer.iter().enumerate() {
            let Some(batch) = batch.upgrade() else {
                // batch has been resolved externally and we can ignore it
                trim = i + 1;
                continue;
            };

            if batch.inflight_tail().unwrap() > event.local_tail {
                // no confirmation for this batch yet.
                break;
            }
            trim = i + 1;
        }

        self.buffer.drain(..trim);
    }

    async fn process_batch(&mut self, batch: Weak<Payload>) {
        if self.process_once(&batch).await {
            self.buffer.push_back(batch);
        }
    }

    async fn process_once(&self, batch: &Weak<Payload>) -> bool {
        let batch = match batch.upgrade() {
            Some(batch) => batch,
            None => return false,
        };

        let inflight_tail = batch.inflight_tail().expect("valid inflight tail");
        if inflight_tail <= self.global.committed_tail() {
            // todo: (question) batch is already committed and we can safely ignore it?
            return false;
        }

        let store = Store {
            first_offset: batch.first_offset,
            flags: StoreFlags::empty(),
            known_archived: LogletOffset::INVALID,
            known_global_tail: self.global.committed_tail(),
            loglet_id: self.global.loglet_id,
            sequencer: self.global.node_id,
            timeout_at: None,
            // todo: (question) better way to do this?
            payloads: Vec::from_iter(batch.records.iter().map(|r| r.clone())),
        };

        if let Err(err) = self.node.client().enqueue_store(store).await {
            //todo: retry
            tracing::error!(error = %err, "failed to send store to node");
        }

        // batch is sent but there is a chance that we need to retry
        true
    }
}
