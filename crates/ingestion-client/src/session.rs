// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{mem, num::NonZeroUsize, sync::Arc, time::Duration};

use dashmap::DashMap;
use futures::{FutureExt, StreamExt, ready, stream};
use tokio::sync::{OwnedSemaphorePermit, mpsc, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, trace, warn};

use restate_core::{
    TaskCenter, TaskKind,
    network::{ConnectError, Connection, NetworkSender, Networking, Swimlane, TransportConnect},
    partitions::PartitionRouting,
};
use restate_types::{
    identifiers::PartitionId,
    net::ingest::{IngestRecord, IngestRequest, ResponseStatus},
    retries::RetryPolicy,
};

use crate::chunks_size::ChunksSize;

#[cfg(any(test, feature = "test-util"))]
const DEFAULT_MAX_RECORD_SIZE: usize = 32 * 1024 * 1024; // 32MiB

/// Error returned when attempting to use a session that has already been closed.
#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("Partition session is closed")]
pub struct SessionClosed;

/// Commitment failures that can be observed when waiting on [`RecordCommit`].
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("commit cancelled")]
pub struct CancelledError;

/// Future that is resolved to the commit result
/// A [`CancelledError`] might be returned
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
    /// will resolve to [`CancelledError`]
    #[allow(dead_code)]
    pub fn cancelled(self) {
        let _ = self.tx.send(Err(CancelledError));
    }
}

struct IngestionBatch {
    records: Arc<[IngestRecord]>,
    resolvers: Vec<RecordCommitResolver>,
}

impl IngestionBatch {
    fn new(batch: impl IntoIterator<Item = (RecordCommitResolver, IngestRecord)>) -> Self {
        let (resolvers, records): (Vec<_>, Vec<_>) = batch.into_iter().unzip();
        let records: Arc<[IngestRecord]> = Arc::from(records);

        Self { records, resolvers }
    }

    /// Marks every tracked record in the batch as committed.
    fn committed(self) {
        for resolver in self.resolvers {
            resolver.committed();
        }
    }
}

/// Tunable parameters for batching and networking behaviour of partition sessions.
#[derive(Debug, Clone, derive_builder::Builder)]
#[builder(pattern = "owned")]
pub struct SessionOptions {
    /// Maximum batch size in `bytes`
    #[builder(default=NonZeroUsize::new(50 * 1024).unwrap())]
    pub(crate) batch_size: NonZeroUsize,
    /// Connection retry policy
    /// Retry policy must be infinite (retries forever)
    /// If not, the retry will fallback to 3 seconds intervals
    #[builder(default=RetryPolicy::exponential(
        Duration::from_millis(250),
        2.0,
        None,
        Some(Duration::from_secs(3)),
    ))]
    pub(crate) connection_retry_policy: RetryPolicy,
    /// Backoff applied before reconnecting after a connected session made no
    /// progress (e.g. an immediate `NotLeader` rejection). Must be infinite;
    /// it is reset once a batch is acknowledged. This avoids a busy reconnect
    /// loop while `PartitionRouting` catches up to a new leader, while still
    /// retrying quickly the first few times in case the rejecting node wins
    /// leadership shortly.
    #[builder(default=RetryPolicy::exponential(
        Duration::from_millis(25),
        2.0,
        None,
        Some(Duration::from_secs(1)),
    ))]
    pub(crate) reconnect_retry_policy: RetryPolicy,
    /// Maximum allowed record size
    #[cfg_attr(any(test, feature = "test-util"), builder(default=NonZeroUsize::new(DEFAULT_MAX_RECORD_SIZE).unwrap()))]
    pub(crate) record_size_limit: NonZeroUsize,
    /// Connection swimlane
    #[cfg_attr(any(test, feature = "test-util"), builder(default=Swimlane::General))]
    pub(crate) swimlane: Swimlane,
}

impl SessionOptions {
    pub fn builder() -> SessionOptionsBuilder {
        SessionOptionsBuilder::default()
    }
}

#[cfg(any(test, feature = "test-util"))]
impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            // The default batch size of 50KB is to avoid
            // overwhelming the PP on the hot path.
            batch_size: NonZeroUsize::new(50 * 1024).unwrap(), // 50 KB
            swimlane: Swimlane::IngressData,
            record_size_limit: NonZeroUsize::new(DEFAULT_MAX_RECORD_SIZE).unwrap(),
            connection_retry_policy: RetryPolicy::exponential(
                Duration::from_millis(10),
                2.0,
                None,
                Some(Duration::from_secs(1)),
            ),
            reconnect_retry_policy: RetryPolicy::exponential(
                Duration::from_millis(10),
                2.0,
                None,
                Some(Duration::from_millis(100)),
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
    manager: Arc<SessionManagerInner<T>>,
    partition: PartitionId,
    partition_routing: PartitionRouting,
    networking: Networking<T>,
    opts: SessionOptions,
    rx: UnboundedReceiverStream<(RecordCommitResolver, IngestRecord)>,
    tx: mpsc::UnboundedSender<(RecordCommitResolver, IngestRecord)>,
    // It carries the single un-acked batch and is sent ahead of what's stored in carry over to
    // ensure ingestion batches can't overtake each other.
    in_flight: Option<IngestionBatch>,
    // Records pulled from `rx` while detecting a batch boundary but not yet sent. Carried across
    // reconnects and fed back into the chunker so they lead the next batch. See #4810.
    carry_over: Vec<(RecordCommitResolver, IngestRecord)>,
}

impl<T> PartitionSession<T> {
    fn new(
        manager: Arc<SessionManagerInner<T>>,
        networking: Networking<T>,
        partition_routing: PartitionRouting,
        partition: PartitionId,
        opts: SessionOptions,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let rx = UnboundedReceiverStream::new(rx);

        Self {
            manager,
            partition,
            partition_routing,
            networking,
            opts,
            rx,
            tx,
            in_flight: None,
            carry_over: Vec::default(),
        }
    }

    /// Returns a handle that can be used by callers to enqueue new records.
    pub fn handle(&self) -> SessionHandle {
        SessionHandle {
            tx: self.tx.clone(),
        }
    }
}

impl<T> Drop for PartitionSession<T> {
    fn drop(&mut self) {
        self.manager.handles.remove(&self.partition);
    }
}

impl<T> PartitionSession<T>
where
    T: TransportConnect,
{
    /// Runs the session state machine until shut down, reacting to cancellation and connection errors.
    pub async fn start(self, cancellation: CancellationToken) {
        let partition_id = self.partition;
        debug!(
            partition_id = %partition_id,
            "Starting ingestion partition session",
        );

        cancellation.run_until_cancelled(self.run_inner()).await;
        debug!("Ingestion session for partition {partition_id} stopped");
    }

    /// Runs the session state machine until shut down, reacting to cancellation and connection errors.
    async fn run_inner(mut self) {
        let mut state = SessionState::Connecting;

        // Backoff applied before reconnecting when a connected session made no progress
        // (e.g. an immediate `NotLeader` rejection). Cloned into a local so the iterator
        // doesn't borrow `self` across the `&mut self` calls below. Reset on every acked
        // batch; advanced on every no-progress return.
        let reconnect_policy = self.opts.reconnect_retry_policy.clone();
        let mut reconnect_retry = reconnect_policy.iter();

        loop {
            state = match state {
                SessionState::Connecting => {
                    let retry_policy = self.opts.connection_retry_policy.clone();
                    let mut retry = retry_policy.iter();
                    loop {
                        match self.connect().await {
                            Some(state) => break state,
                            None => {
                                // retry
                                // this assumes that retry policy is infinite. If it's not it falls back
                                // to a fixed 3 seconds sleep between retries
                                tokio::time::sleep(retry.next().unwrap_or(Duration::from_secs(3)))
                                    .await;
                            }
                        }
                    }
                }
                SessionState::Connected { connection } => {
                    if self.connected_sequential_mode(connection).await {
                        // Committed at least one batch: reset the no-progress backoff.
                        reconnect_retry = reconnect_policy.iter();
                    } else {
                        // No batch committed: likely an immediate rejection (e.g.
                        // `NotLeader`) from a stale/candidate leader that `connect()` will
                        // reach again right away. Back off (escalating) before reconnecting
                        // so we don't busy-loop while `PartitionRouting` catches up, while
                        // still retrying quickly the first few times in case the rejecting
                        // node wins leadership shortly.
                        tokio::time::sleep(
                            reconnect_retry.next().unwrap_or(Duration::from_secs(1)),
                        )
                        .await;
                    }
                    SessionState::Connecting
                }
                SessionState::Shutdown => {
                    self.rx.close();
                    break;
                }
            }
        }
    }

    async fn connect(&mut self) -> Option<SessionState> {
        // Resolve the node separately: routing returns the current leader if known, otherwise it
        // falls back to a live replica-set member.
        let node_id = self
            .partition_routing
            .get_node_by_partition(self.partition)?;

        let result = self
            .networking
            .get_connection(node_id, self.opts.swimlane)
            .await;

        match result {
            Ok(connection) => {
                debug!("Connection established to node {}", node_id);
                Some(SessionState::Connected { connection })
            }
            Err(ConnectError::Shutdown(_)) => Some(SessionState::Shutdown),
            Err(err) => {
                debug!("Failed to connect to node {}: {err}", node_id);
                None
            }
        }
    }

    /// Processes batches one at a time, keeping at most a single batch inflight.
    ///
    /// Each batch must be acknowledged (committed) before the next one is sent.
    ///
    /// The method only returns when the connection is lost or an error is
    /// encountered. On return, any batch that has not yet been committed remains
    /// in [`self.in_flight`] and is replayed first on the next connection.
    ///
    /// Returns `true` if at least one batch was acknowledged during this session,
    /// `false` if it returned without making progress (used by the caller to decide
    /// whether to back off before reconnecting).
    #[instrument(
        level="debug",
        skip_all,
        name="connected",
        fields(
            mode="sequential",
            partition=%self.partition,
        )
    )]
    async fn connected_sequential_mode(&mut self, connection: Connection) -> bool {
        let mut made_progress = false;
        let mut chunked = ChunksSize::with_buffered(
            &mut self.rx,
            self.opts.batch_size.get(),
            |(_, item)| item.estimate_size(),
            mem::take(&mut self.carry_over),
        );

        // prepend a possible in_flight batch to make sure that this is being sent first!
        let mut batch_stream =
            stream::iter(self.in_flight.take()).chain((&mut chunked).map(IngestionBatch::new));

        while let Some(batch) = batch_stream.next().await {
            let records = Arc::clone(&batch.records);

            let Some(permit) = connection.reserve().await else {
                debug!(
                    "Connection to {} closed while reserving capacity; will reconnect and replay",
                    connection.peer()
                );
                self.in_flight = Some(batch);
                break;
            };

            trace!("Sending ingest batch, len: {}", records.len());
            let reply_rx = permit
                .send_rpc(IngestRequest { records }, Some(self.partition.into()))
                .expect("encoding version to match");

            match reply_rx.await.map(|r| r.status) {
                Ok(ResponseStatus::Ack) => {
                    batch.committed();
                    made_progress = true;
                }

                Ok(ResponseStatus::Internal { msg }) => {
                    warn!("Ingestion internal error from {}: {msg}", connection.peer());
                    self.in_flight = Some(batch);
                    break;
                }
                Ok(response) => {
                    // Handle any other response code as a connection loss
                    // and retry all inflight batches.
                    debug!(
                        "Ingestion response error status from {}: {:?}",
                        connection.peer(),
                        response
                    );
                    self.in_flight = Some(batch);
                    break;
                }
                Err(err) => {
                    // we can assume that for any error
                    // we need to retry all the inflight batches.
                    // special case for load shedding we could
                    // throttle the stream a little bit then
                    // speed up over a period of time.
                    debug!("Ingestion RPC error from {}: {}", connection.peer(), err);
                    self.in_flight = Some(batch);
                    break;
                }
            }
        }

        // Carry over the record the chunker over-pulled to detect the batch boundary so it leads
        // the next connection's first batch instead of being dropped (which would surface to the
        // caller as a spurious cancellation).
        self.carry_over = chunked.into_remainder();

        made_progress
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
    pub fn get(self: &Arc<Self>, id: PartitionId) -> SessionHandle {
        self.handles
            .entry(id)
            .or_insert_with(|| self.start_session(id))
            .value()
            .clone()
    }

    fn start_session(self: &Arc<Self>, id: PartitionId) -> SessionHandle {
        let session = PartitionSession::new(
            Arc::clone(self),
            self.networking.clone(),
            self.partition_routing.clone(),
            id,
            self.opts.clone(),
        );

        let handle = session.handle();

        let cancellation = self.cancellation.child_token();
        let _ = TaskCenter::spawn_unmanaged(
            TaskKind::IngestionSession,
            format!("ingestion-partition-session-p{id}"),
            session.start(cancellation),
        );

        handle
    }
}

impl<T> Drop for SessionManagerInner<T> {
    fn drop(&mut self) {
        self.cancellation.cancel();
        trace!("Session manager cancelled");
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
        opts: SessionOptions,
    ) -> Self {
        let inner = SessionManagerInner {
            networking,
            partition_routing,
            opts,
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

    pub fn options(&self) -> &SessionOptions {
        &self.inner.opts
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
