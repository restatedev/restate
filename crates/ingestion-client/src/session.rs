// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{collections::VecDeque, num::NonZeroUsize, sync::Arc, time::Duration};

use dashmap::DashMap;
use futures::{FutureExt, StreamExt, future::OptionFuture, ready};
use tokio::sync::{OwnedSemaphorePermit, mpsc, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, trace, warn};

use restate_core::{
    TaskCenter, TaskKind,
    network::{
        ConnectError, Connection, ConnectionClosed, NetworkSender, Networking, ReplyRx, Swimlane,
        TransportConnect,
    },
    partitions::PartitionRouting,
};
use restate_types::{
    identifiers::{LeaderEpoch, PartitionId},
    net::{
        ProtocolVersion,
        ingest::{IngestRecord, IngestRequest, IngestResponse, ResponseStatus},
    },
    retries::RetryPolicy,
};

use crate::chunks_size::ChunksSize;

const DEFAULT_MAX_RECORD_SIZE: usize = 32 * 1024 * 1024; // 32MB

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

    fn len(&self) -> usize {
        self.records.len()
    }
}

/// Tunable parameters for batching and networking behaviour of partition sessions.
#[derive(Debug, Clone, derive_builder::Builder)]
#[builder(pattern = "owned", build_fn(name = "_build", private))]
pub struct SessionOptions {
    /// Maximum batch size in `bytes`
    #[builder(default=NonZeroUsize::new(50 * 1024).unwrap())]
    pub(crate) batch_size: NonZeroUsize,
    /// Connection retry policy
    /// Retry policy must be infinite (retries forever)
    /// If not, the retry will fallback to 2 seconds intervals
    #[builder(default=RetryPolicy::exponential(
        Duration::from_millis(10),
        2.0,
        None,
        Some(Duration::from_secs(1)),
    ))]
    pub(crate) connection_retry_policy: RetryPolicy,
    /// Maximum allowed record size
    #[builder(default=NonZeroUsize::new(DEFAULT_MAX_RECORD_SIZE).unwrap())]
    pub(crate) record_size_limit: NonZeroUsize,
    /// Connection swimlane
    #[builder(default=Swimlane::IngressData)]
    pub(crate) swimlane: Swimlane,
}

impl SessionOptions {
    pub fn builder() -> SessionOptionsBuilder {
        SessionOptionsBuilder::default()
    }
}

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
        }
    }
}

impl SessionOptionsBuilder {
    pub fn build(self) -> SessionOptions {
        self._build().unwrap()
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
    ConnectedSequentialMode { connection: Connection },
    ConnectedPipeliningMode { connection: Connection },
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
    // Batches that were unacked (or never sent) when the previous connection ended, kept in
    // produced order. On the next connection they are re-sent ahead of any newly chunked batch so
    // records reach the partition log in the order they were produced. Includes the record the
    // chunker over-pulled to detect a batch boundary, wrapped as its own trailing batch.
    carry_over: VecDeque<IngestionBatch>,
    target_leader_epoch: LeaderEpoch,
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
            carry_over: VecDeque::default(),
            target_leader_epoch: LeaderEpoch::INITIAL,
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
                                // to a fixed 2 seconds sleep between retries
                                tokio::time::sleep(retry.next().unwrap_or(Duration::from_secs(2)))
                                    .await;
                            }
                        }
                    }
                }
                SessionState::ConnectedSequentialMode { connection } => {
                    self.connected_sequential_mode(connection).await;
                    SessionState::Connecting
                }
                SessionState::ConnectedPipeliningMode { connection } => {
                    self.connected_pipelining(connection).await;
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
        // `PartitionRouting` is the source of truth for the current leader epoch; the leadership state
        // we last learned (from routing, or reported back by a `NotLeaderWithEpoch`) is used used
        // to update the the partition replica set state so this should have the most known up to date
        // leadership state

        let (node_id, leader_epoch) = self.partition_routing.get_leader(self.partition)?;

        self.target_leader_epoch = leader_epoch;

        let result = self
            .networking
            .get_connection(node_id, self.opts.swimlane)
            .await;

        match result {
            Ok(connection) => {
                debug!("Connection established to node {}", node_id);
                if connection.protocol_version() <= ProtocolVersion::V3 {
                    Some(SessionState::ConnectedSequentialMode { connection })
                } else {
                    Some(SessionState::ConnectedPipeliningMode { connection })
                }
            }
            Err(ConnectError::Shutdown(_)) => Some(SessionState::Shutdown),
            Err(err) => {
                debug!("Failed to connect to node {}: {err}", node_id);
                None
            }
        }
    }

    #[instrument(
        level="debug",
        skip_all,
        name="connected",
        fields(
            mode="sequential", 
            partition=%self.partition,
            leader_epoch=%self.target_leader_epoch,
        )
    )]
    async fn connected_sequential_mode(&mut self, connection: Connection) {
        // replay carry over one by one
        debug!("Carry-over batches: {}", self.carry_over.len());

        while let Some(batch) = self.carry_over.front() {
            let records = Arc::clone(&batch.records);

            let Some(permit) = connection.reserve().await else {
                return;
            };

            trace!("Sending ingest batch, len: {}", records.len());
            // Nodes running protocol version V3 doesn't support
            // target_leader_epoch. We mainly sending this to be
            // able to use sequential mode against nodes with >= V4
            // if needed
            let reply_rx = permit
                .send_rpc(
                    IngestRequest {
                        records,
                        target_leader_epoch: Some(self.target_leader_epoch),
                    },
                    Some(self.partition.into()),
                )
                .expect("encoding version to match");

            match reply_rx.await.map(|r| r.status) {
                Ok(ResponseStatus::Ack) => {
                    // remove committed batch from carry_over
                    let batch = self.carry_over.pop_front().unwrap();
                    batch.committed();
                }
                Ok(ResponseStatus::NotLeaderWithEpoch {
                    last_seen_leadership_state,
                    ..
                }) => {
                    // Note that this error kind is never returned
                    // by partition processor < v1.7. This is mainly
                    // defensive coding, but also allows us to switch
                    // to sequential mode for protocol version >= V4
                    // if needed.
                    self.partition_routing
                        .partition_replica_set_state()
                        .note_observed_leader(self.partition, last_seen_leadership_state);
                    return;
                }
                Ok(ResponseStatus::Internal { msg }) => {
                    warn!("Ingestion internal error from {}: {msg}", connection.peer());
                    return;
                }
                Ok(response) => {
                    // Handle any other response code as a connection loss
                    // and retry all inflight batches.
                    trace!(
                        "Ingestion response from {}: {:?}",
                        connection.peer(),
                        response
                    );

                    return;
                }
                Err(err) => {
                    // we can assume that for any error
                    // we need to retry all the inflight batches.
                    // special case for load shedding we could
                    // throttle the stream a little bit then
                    // speed up over a period of time.
                    trace!("Ingestion error from {}: {}", connection.peer(), err);
                    return;
                }
            }
        }

        debug_assert!(self.carry_over.is_empty());

        let mut chunked = ChunksSize::new(&mut self.rx, self.opts.batch_size.get(), |(_, item)| {
            item.estimate_size()
        });

        while let Some(batch) = chunked.next().await {
            let batch = IngestionBatch::new(batch);
            let records = Arc::clone(&batch.records);

            let Some(permit) = connection.reserve().await else {
                self.carry_over.push_back(batch);
                break;
            };

            trace!("Sending ingest batch, len: {}", records.len());
            let reply_rx = permit
                .send_rpc(
                    IngestRequest {
                        records,
                        target_leader_epoch: Some(self.target_leader_epoch),
                    },
                    Some(self.partition.into()),
                )
                .expect("encoding version to match");

            match reply_rx.await.map(|r| r.status) {
                Ok(ResponseStatus::Ack) => {
                    batch.committed();
                }
                Ok(ResponseStatus::NotLeaderWithEpoch {
                    last_seen_leadership_state,
                    ..
                }) => {
                    // Note that this error kind is never returned
                    // by partition processor < v1.7. This is mainly
                    // defensive coding, but also allows us to switch
                    // to sequential mode for protocol version >= V4
                    // if needed.
                    self.partition_routing
                        .partition_replica_set_state()
                        .note_observed_leader(self.partition, last_seen_leadership_state);

                    self.carry_over.push_back(batch);
                    break;
                }
                Ok(ResponseStatus::Internal { msg }) => {
                    warn!("Ingestion internal error from {}: {msg}", connection.peer());
                    self.carry_over.push_back(batch);
                    break;
                }
                Ok(response) => {
                    // Handle any other response code as a connection loss
                    // and retry all inflight batches.
                    trace!(
                        "Ingestion response from {}: {:?}",
                        connection.peer(),
                        response
                    );
                    self.carry_over.push_back(batch);
                    break;
                }
                Err(err) => {
                    // we can assume that for any error
                    // we need to retry all the inflight batches.
                    // special case for load shedding we could
                    // throttle the stream a little bit then
                    // speed up over a period of time.
                    trace!("Ingestion error from {}: {}", connection.peer(), err);
                    self.carry_over.push_back(batch);
                    break;
                }
            }
        }

        // Carry over the record the chunker over-pulled to detect the batch boundary so it leads
        // the next connection's first batch instead of being dropped (which would surface to the
        // caller as a spurious cancellation). Mirrors the pipelining path.
        let remainder = chunked.into_remainder();
        if !remainder.is_empty() {
            self.carry_over.push_back(IngestionBatch::new(remainder));
        }
    }

    #[instrument(
        level="debug",
        skip_all,
        name="connected",
        fields(
            mode="pipelining", 
            partition=%self.partition,
            leader_epoch=%self.target_leader_epoch,
        )
    )]
    async fn connected_pipelining(&mut self, connection: Connection) {
        debug!("Carry-over batches: {}", self.carry_over.len(),);

        let Ok(mut inflight) = self.replay(&connection).await else {
            return;
        };

        debug_assert!(self.carry_over.is_empty());

        // Carry-over batches were already re-sent in order by `replay()`; new batches are chunked
        // fresh from the stream.
        let mut chunked = ChunksSize::new(&mut self.rx, self.opts.batch_size.get(), |(_, item)| {
            item.estimate_size()
        });

        loop {
            // Multiple batches may be in flight at once. Ordering is preserved without an
            // at-most-one-in-flight cap because: the connection is FIFO, the leader appends each
            // batch sequentially in arrival order, and every in-flight batch carries the same
            // `target_leader_epoch` so a leadership change rejects the whole pipeline atomically
            // (rather than rejecting an earlier batch while appending a later one, which the dedup
            // high-water-mark would then silently drop — see #4810). Replies are consumed strictly
            // head-first, so on any rejection the head and everything behind it are carried over and
            // replayed in produced order.
            let head: OptionFuture<_> = inflight
                .front_mut()
                .and_then(|(_, reply_rx)| reply_rx.as_mut())
                .into();

            tokio::select! {
                Some(batch) = chunked.next() => {
                    let batch = IngestionBatch::new(batch);
                    let records = Arc::clone(&batch.records);

                    inflight.push_back((batch, None));

                    let Some(permit) = connection.reserve().await else {
                        break;
                    };

                    trace!("Sending ingest batch, len: {}", records.len());
                    let reply_rx = permit
                        .send_rpc(
                            IngestRequest{
                                records,
                                target_leader_epoch: Some(self.target_leader_epoch)
                            },
                            Some(self.partition.into())
                        ).expect("encoding version to match");

                    inflight.back_mut().expect("to exist").1 = Some(reply_rx);
                }
                Some(result) = head => {
                    match result.map(|r|r.status) {
                        Ok(ResponseStatus::Ack) => {
                            let batch = inflight.pop_front().expect("not empty").0;
                            batch.committed();
                        }
                        Ok(ResponseStatus::NotLeaderWithEpoch{
                            last_seen_leadership_state,
                            ..
                        })=> {
                            self.partition_routing
                                .partition_replica_set_state()
                                .note_observed_leader(self.partition, last_seen_leadership_state);
                            break;
                        }
                        Ok(ResponseStatus::Internal{msg}) => {
                            warn!("Ingestion internal error from {}: {msg}", connection.peer());
                            break;
                        }
                        Ok(response) => {
                            // Handle any other response code as a connection loss
                            // and retry all inflight batches.
                            debug!("Ingestion response from {}: {:?}", connection.peer(), response);
                            break;
                        }
                        Err(err) => {
                            // we can assume that for any error
                            // we need to retry all the inflight batches.
                            // special case for load shedding we could
                            // throttle the stream a little bit then
                            // speed up over a period of time.

                            trace!("Ingestion error from {}: {}", connection.peer(),  err);
                            break;
                        }
                    }
                }
            }
        }

        for batch in inflight.into_iter().map(|(batch, _)| batch) {
            self.carry_over.push_back(batch);
        }

        let remainder = chunked.into_remainder();
        if !remainder.is_empty() {
            self.carry_over.push_back(IngestionBatch::new(remainder));
        }
    }

    /// Re-sends all inflight batches after a connection is restored.
    async fn replay(&mut self, connection: &Connection) -> Result<InflightQueue, ConnectionClosed> {
        // todo(azmy): to avoid all the inflight batches again and waste traffic
        //  maybe test the connection first by sending an empty batch and wait for response
        //  before proceeding?

        let total = self.carry_over.iter().fold(0, |v, i| v + i.len());
        trace!(
            partition = %self.partition,
            batches = self.carry_over.len(),
            records = total,
            "Replaying inflight records after connection was restored"
        );

        let mut inflight = InflightQueue::new();

        while let Some(batch) = self.carry_over.front() {
            let Some(permit) = connection.reserve().await else {
                // restore the carry over back to original state.
                // maintaining the original order.

                while let Some((batch, _)) = inflight.pop_back() {
                    self.carry_over.push_front(batch);
                }

                return Err(ConnectionClosed);
            };

            // resend batch
            let reply_rx = permit
                .send_rpc(
                    IngestRequest {
                        records: Arc::clone(&batch.records),
                        target_leader_epoch: Some(self.target_leader_epoch),
                    },
                    Some(self.partition.into()),
                )
                .expect("encoding version to match");

            let batch = self.carry_over.pop_front().unwrap();
            inflight.push_back((batch, Some(reply_rx)));
        }

        Ok(inflight)
    }
}

type InflightQueue = VecDeque<(IngestionBatch, Option<ReplyRx<IngestResponse>>)>;

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
        debug!("Session manager cancelled");
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
