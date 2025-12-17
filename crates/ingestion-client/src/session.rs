// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{collections::VecDeque, sync::Arc, time::Duration};

use dashmap::DashMap;
use futures::{FutureExt, StreamExt, future::OptionFuture, ready};
use tokio::sync::{OwnedSemaphorePermit, mpsc, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace};

use restate_core::{
    TaskCenter, TaskKind,
    network::{
        ConnectError, Connection, ConnectionClosed, NetworkSender, Networking, ReplyRx, Swimlane,
        TransportConnect,
    },
    partitions::PartitionRouting,
};
use restate_types::{
    identifiers::PartitionId,
    net::ingest::{IngestRecord, IngestRequest, IngestResponse, ResponseStatus},
    retries::RetryPolicy,
};

use crate::chunks_size::ChunksSize;

/// Error returned when attempting to use a session that has already been closed.
#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("Partition session is closed")]
pub struct SessionClosed;

/// Commitment failures that can be observed when waiting on [`RecordCommit`].
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("commit cancelled")]
pub struct CancelledError;

/// Future that is resolved to the commit result
/// A [`CommitError::Cancelled`] might be returned
/// if [`IngestionClient`] is closed while record is in
/// flight. This does not guarantee that the record
/// was not processed or committed.
#[pin_project::pin_project]
pub struct RecordCommit<V = ()> {
    v: Option<V>,
    #[pin]
    rx: oneshot::Receiver<Result<(), CancelledError>>,
}

impl<V> Future for RecordCommit<V>
where
    V: Send + 'static,
{
    type Output = Result<V, CancelledError>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut this = self.project();
        match ready!(this.rx.poll_unpin(cx)) {
            Ok(result) => std::task::Poll::Ready(result.map(|_| {
                this.v
                    .take()
                    .expect("future should not be polled after completion")
            })),
            Err(_) => std::task::Poll::Ready(Err(CancelledError)),
        }
    }
}

impl RecordCommit {
    fn new(permit: OwnedSemaphorePermit) -> (Self, RecordCommitResolver) {
        let (tx, rx) = oneshot::channel();
        (
            Self { v: Some(()), rx },
            RecordCommitResolver {
                tx,
                _permit: permit,
            },
        )
    }
}

impl<V> RecordCommit<V> {
    pub fn map<F, T>(self, f: F) -> RecordCommit<T>
    where
        F: FnOnce(V) -> T,
    {
        let RecordCommit { v, rx } = self;
        RecordCommit { v: v.map(f), rx }
    }
}

struct RecordCommitResolver {
    tx: oneshot::Sender<Result<(), CancelledError>>,
    _permit: OwnedSemaphorePermit,
}

impl RecordCommitResolver {
    /// Resolve the [`RecordCommit`] to committed.
    pub fn committed(self) {
        let _ = self.tx.send(Ok(()));
    }

    /// explicitly cancel the RecordCommit
    /// If resolver is dropped, the RecordCommit
    /// will resolve to [`CommitError::Cancelled`]
    #[allow(dead_code)]
    pub fn cancelled(self) {
        let _ = self.tx.send(Err(CancelledError));
    }
}

struct IngestionBatch {
    records: Arc<[IngestRecord]>,
    resolvers: Vec<RecordCommitResolver>,

    reply_rx: Option<ReplyRx<IngestResponse>>,
}

impl IngestionBatch {
    fn new(batch: impl IntoIterator<Item = (RecordCommitResolver, IngestRecord)>) -> Self {
        let (resolvers, records): (Vec<_>, Vec<_>) = batch.into_iter().unzip();
        let records: Arc<[IngestRecord]> = Arc::from(records);

        Self {
            records,
            resolvers,
            reply_rx: None,
        }
    }

    /// Marks every tracked record in the batch as committed.
    fn committed(self) {
        for resolver in self.resolvers {
            resolver.committed();
        }
    }

    fn len(&self) -> usize {
        self.records.len()
    }
}

/// Tunable parameters for batching and networking behaviour of partition sessions.
#[derive(Debug, Clone)]
pub struct SessionOptions {
    /// Maximum batch size in `bytes`
    pub batch_size: usize,
    /// Connection retry policy
    /// Retry policy must be infinite (retries forever)
    /// If not, the retry will fallback to 2 seconds intervals
    pub connection_retry_policy: RetryPolicy,
    /// Connection swimlane
    pub swimlane: Swimlane,
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            // The default batch size of 50KB is to avoid
            // overwhelming the PP on the hot path.
            batch_size: 50 * 1024, // 50 KB
            swimlane: Swimlane::IngressData,
            connection_retry_policy: RetryPolicy::exponential(
                Duration::from_millis(10),
                2.0,
                None,
                Some(Duration::from_secs(1)),
            ),
        }
    }
}

/// Cloneable sender that enqueues records for a specific partition session.
#[derive(Clone)]
pub struct SessionHandle {
    tx: mpsc::UnboundedSender<(RecordCommitResolver, IngestRecord)>,
}

impl SessionHandle {
    /// Enqueues an ingest request along with the owned permit and returns a future tracking commit outcome.
    pub fn ingest(
        &self,
        permit: OwnedSemaphorePermit,
        record: IngestRecord,
    ) -> Result<RecordCommit, SessionClosed> {
        let (commit, resolver) = RecordCommit::new(permit);
        self.tx
            .send((resolver, record))
            .map_err(|_| SessionClosed)?;

        Ok(commit)
    }
}

enum SessionState {
    Connecting,
    Connected { connection: Connection },
    Shutdown,
}

/// Background task that drives the lifecycle of a single partition connection.
pub struct PartitionSession<T> {
    partition: PartitionId,
    partition_routing: PartitionRouting,
    networking: Networking<T>,
    opts: SessionOptions,
    rx: UnboundedReceiverStream<(RecordCommitResolver, IngestRecord)>,
    tx: mpsc::UnboundedSender<(RecordCommitResolver, IngestRecord)>,
    inflight: VecDeque<IngestionBatch>,
}

impl<T> PartitionSession<T> {
    fn new(
        networking: Networking<T>,
        partition_routing: PartitionRouting,
        partition: PartitionId,
        opts: SessionOptions,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let rx = UnboundedReceiverStream::new(rx);

        Self {
            partition,
            partition_routing,
            networking,
            opts,
            inflight: Default::default(),
            rx,
            tx,
        }
    }

    /// Returns a handle that can be used by callers to enqueue new records.
    pub fn handle(&self) -> SessionHandle {
        SessionHandle {
            tx: self.tx.clone(),
        }
    }
}

impl<T> PartitionSession<T>
where
    T: TransportConnect,
{
    /// Runs the session state machine until shut down, reacting to cancellation and connection errors.
    pub async fn start(self, cancellation: CancellationToken) {
        debug!(
            partition_id = %self.partition,
            "Starting ingestion partition session",
        );

        cancellation.run_until_cancelled(self.run_inner()).await;
    }

    /// Runs the session state machine until shut down, reacting to cancellation and connection errors.
    async fn run_inner(mut self) {
        let mut state = SessionState::Connecting;
        debug!(
            partition_id = %self.partition,
            "Starting ingestion partition session",
        );

        loop {
            state = match state {
                SessionState::Connecting => {
                    let mut retry = self.opts.connection_retry_policy.iter();
                    loop {
                        match self.connect().await {
                            Some(state) => break state,
                            None => {
                                // retry
                                // this assumes that retry policy is infinite. If it's not it falls back
                                // to a fixed 2 seconds sleep between retries
                                tokio::time::sleep(retry.next().unwrap_or(Duration::from_secs(2)))
                                    .await;
                            }
                        }
                    }
                }
                SessionState::Connected { connection } => self.connected(connection).await,
                SessionState::Shutdown => {
                    self.rx.close();
                    break;
                }
            }
        }
    }

    async fn connect(&self) -> Option<SessionState> {
        let node_id = self
            .partition_routing
            .get_node_by_partition(self.partition)?;

        let result = self
            .networking
            .get_connection(node_id, self.opts.swimlane)
            .await;

        match result {
            Ok(connection) => {
                debug!("Connection established to node {node_id}");
                Some(SessionState::Connected { connection })
            }
            Err(ConnectError::Shutdown(_)) => Some(SessionState::Shutdown),
            Err(err) => {
                debug!("Failed to connect to node {node_id}: {err}");
                None
            }
        }
    }

    /// Re-sends all inflight batches after a connection is restored.
    async fn replay(&mut self, connection: &Connection) -> Result<(), ConnectionClosed> {
        // todo(azmy): to avoid all the inflight batches again and waste traffic
        //  maybe test the connection first by sending an empty batch and wait for response
        //  before proceeding?

        let total = self.inflight.iter().fold(0, |v, i| v + i.len());
        trace!(
            partition = %self.partition,
            batches = self.inflight.len(),
            records = total,
            "Replaying inflight records after connection was restored"
        );

        for batch in self.inflight.iter_mut() {
            let Some(permit) = connection.reserve().await else {
                return Err(ConnectionClosed);
            };

            // resend batch
            let reply_rx = permit
                .send_rpc(
                    IngestRequest::from(Arc::clone(&batch.records)),
                    Some(self.partition.into()),
                )
                .expect("encoding version to match");
            batch.reply_rx = Some(reply_rx);
        }

        Ok(())
    }

    async fn connected(&mut self, connection: Connection) -> SessionState {
        if self.replay(&connection).await.is_err() {
            return SessionState::Connecting;
        }

        let mut chunked = ChunksSize::new(&mut self.rx, self.opts.batch_size, |(_, item)| {
            item.estimate_size()
        });

        let state = loop {
            let head: OptionFuture<_> = self
                .inflight
                .front_mut()
                .and_then(|batch| batch.reply_rx.as_mut())
                .into();

            tokio::select! {
                _ = connection.closed() => {
                    break SessionState::Connecting;
                }
                Some(batch) = chunked.next() => {
                    let batch = IngestionBatch::new(batch);
                    let records = Arc::clone(&batch.records);

                    self.inflight.push_back(batch);

                    let Some(permit) = connection.reserve().await else {
                        break SessionState::Connecting;
                    };

                    trace!("Sending ingest batch, len: {}", records.len());
                    let reply_rx = permit
                        .send_rpc(IngestRequest::from(records), Some(self.partition.into()))
                        .expect("encoding version to match");

                    self.inflight.back_mut().expect("to exist").reply_rx = Some(reply_rx);
                }
                Some(result) = head => {
                    match result.map(|r|r.status) {
                        Ok(ResponseStatus::Ack) => {
                            let batch = self.inflight.pop_front().expect("not empty");
                            batch.committed();
                        }
                        Ok(response) => {
                            // Handle any other response code as a connection loss
                            // and retry all inflight batches.
                            debug!("Ingestion response from {}: {:?}", connection.peer(), response);
                            break SessionState::Connecting;
                        }
                        Err(err) => {
                            // we can assume that for any error
                            // we need to retry all the inflight batches.
                            // special case for load shedding we could
                            // throttle the stream a little bit then
                            // speed up over a period of time.

                            debug!("Ingestion error from {}: {}", connection.peer(),  err);
                            break SessionState::Connecting;
                        }
                    }
                }
            }
        };

        // state == Connecting
        assert!(matches!(state, SessionState::Connecting));

        // don't lose the buffered batch
        let remainder = chunked.into_remainder();
        if !remainder.is_empty() {
            self.inflight.push_back(IngestionBatch::new(remainder));
        }

        state
    }
}

struct SessionManagerInner<T> {
    networking: Networking<T>,
    partition_routing: PartitionRouting,
    opts: SessionOptions,
    // Since ingestion sessions are started on demand
    // we make sure we decouple the session cancellation
    // from the initiating task. Hence the session manager
    // keep it's own cancellation token that is passed to
    // all the sessions.
    cancellation: CancellationToken,
    handles: DashMap<PartitionId, SessionHandle>,
}

impl<T> SessionManagerInner<T>
where
    T: TransportConnect,
{
    /// Gets or start a new session to partition with given partition id.
    /// It guarantees that only one session is started per partition id.
    pub fn get(&self, id: PartitionId) -> SessionHandle {
        self.handles
            .entry(id)
            .or_insert_with(|| {
                let session = PartitionSession::new(
                    self.networking.clone(),
                    self.partition_routing.clone(),
                    id,
                    self.opts.clone(),
                );

                let handle = session.handle();

                let cancellation = self.cancellation.child_token();
                let _ = TaskCenter::spawn(
                    TaskKind::Background,
                    "ingestion-partition-session",
                    async move {
                        session.start(cancellation).await;
                        Ok(())
                    },
                );

                handle
            })
            .value()
            .clone()
    }
}

impl<T> Drop for SessionManagerInner<T> {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

/// Manager that owns all partition sessions and caches their handles.
#[derive(Clone)]
pub struct SessionManager<T> {
    inner: Arc<SessionManagerInner<T>>,
}

impl<T> SessionManager<T> {
    /// Creates a new session manager with optional overrides for session behaviour.
    pub fn new(
        networking: Networking<T>,
        partition_routing: PartitionRouting,
        opts: Option<SessionOptions>,
    ) -> Self {
        let inner = SessionManagerInner {
            networking,
            partition_routing,
            opts: opts.unwrap_or_default(),
            handles: Default::default(),
            cancellation: CancellationToken::new(),
        };

        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn partition_routing(&self) -> &PartitionRouting {
        &self.inner.partition_routing
    }

    pub fn networking(&self) -> &Networking<T> {
        &self.inner.networking
    }
}

impl<T> SessionManager<T>
where
    T: TransportConnect,
{
    /// Returns a handle to the session for the given partition, creating it if needed.
    pub fn get(&self, id: PartitionId) -> SessionHandle {
        self.inner.get(id)
    }

    /// Signals all sessions to shut down and prevents new work from being scheduled.
    pub fn close(&self) {
        self.inner.cancellation.cancel();
    }
}
