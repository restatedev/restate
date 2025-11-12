// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    collections::{HashMap, VecDeque},
    ops::Deref,
    sync::{Arc, OnceLock},
    time::Duration,
};

use arc_swap::ArcSwap;
use dashmap::DashMap;
use futures::{FutureExt, future::OptionFuture, ready};
use tokio::sync::{OwnedSemaphorePermit, mpsc, oneshot};
use tokio_stream::{StreamExt, wrappers::UnboundedReceiverStream};
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace};

use restate_core::{
    TaskCenter, TaskKind, cancellation_token,
    network::{
        ConnectError, Connection, ConnectionClosed, NetworkSender, Networking, ReplyRx, Swimlane,
        TransportConnect,
    },
    partitions::PartitionRouting,
};
use restate_types::{
    identifiers::PartitionId,
    net::ingress::{IngestRecord, IngestRequest, IngestResponse},
    retries::{RetryIter, RetryPolicy},
};

use crate::chunks_timeout::ChunksTimeout;

/// Error returned when attempting to use a session that has already been closed.
#[derive(Clone, Copy, Debug, thiserror::Error)]
#[error("Partition session is closed")]
pub struct SessionClosed;

/// Commitment failures that can be observed when waiting on [`RecordCommit`].
#[derive(Debug, Clone, Copy, thiserror::Error)]
pub enum CommitError {
    #[error("commit cancelled")]
    Cancelled,
}

/// Future that is resolved to the commit result
/// A [`CommitError::Cancelled`] might be returned
/// if [`crate::Ingress`] is closed while record is in
/// flight. This does not guarantee that the record
/// was ont processed or committed.
pub struct RecordCommit {
    rx: oneshot::Receiver<Result<(), CommitError>>,
}

impl Future for RecordCommit {
    type Output = Result<(), CommitError>;
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match ready!(self.rx.poll_unpin(cx)) {
            Ok(result) => std::task::Poll::Ready(result),
            Err(_) => std::task::Poll::Ready(Err(CommitError::Cancelled)),
        }
    }
}

impl RecordCommit {
    fn new() -> (Self, RecordCommitResolver) {
        let (tx, rx) = oneshot::channel();
        (Self { rx }, RecordCommitResolver { tx })
    }
}

struct RecordCommitResolver {
    tx: oneshot::Sender<Result<(), CommitError>>,
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
        let _ = self.tx.send(Err(CommitError::Cancelled));
    }
}

struct IngressBatch {
    records: Arc<[IngestRecord]>,
    trackers: Vec<RecordTracker>,
    reply_rx: Option<ReplyRx<IngestResponse>>,
}

impl IngressBatch {
    /// Marks every tracked record in the batch as committed.
    pub fn committed(self) {
        for tracker in self.trackers {
            tracker.resolver.committed();
        }
    }
}

/// Tunable parameters for batching and networking behaviour of partition sessions.
#[derive(Debug, Clone)]
pub struct SessionOptions {
    /// Maximum batch size
    pub batch_size: usize,
    /// Maximum batch timeout before it's sent
    pub batch_timeout: Duration,
    /// connection retry policy
    pub connect_retry_policy: RetryPolicy,
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            batch_size: 250,
            batch_timeout: Duration::from_millis(100),
            connect_retry_policy: RetryPolicy::exponential(
                Duration::from_millis(10),
                2.0,
                None,
                Some(Duration::from_secs(1)),
            ),
        }
    }
}

struct RecordTracker {
    _permit: OwnedSemaphorePermit,
    resolver: RecordCommitResolver,
}

/// Cloneable sender that enqueues records for a specific partition session.
#[derive(Clone)]
pub struct SessionHandle {
    tx: mpsc::UnboundedSender<(RecordTracker, IngestRecord)>,
}

impl SessionHandle {
    /// Enqueues an ingest request along with the owned permit and returns a future tracking commit outcome.
    pub fn ingest(
        &self,
        permit: OwnedSemaphorePermit,
        record: IngestRecord,
    ) -> Result<RecordCommit, SessionClosed> {
        let (commit, resolver) = RecordCommit::new();
        self.tx
            .send((
                RecordTracker {
                    _permit: permit,
                    resolver,
                },
                record,
            ))
            .map_err(|_| SessionClosed)?;

        Ok(commit)
    }
}

enum SessionState {
    Connecting { retry: RetryIter<'static> },
    Connected { connection: Connection },
    Disconnected,
    Shutdown,
}

/// Background task that drives the lifecycle of a single partition connection.
pub struct PartitionSession<T> {
    partition: PartitionId,
    partition_routing: PartitionRouting,
    networking: Networking<T>,
    opts: SessionOptions,
    rx: UnboundedReceiverStream<(RecordTracker, IngestRecord)>,
    tx: mpsc::UnboundedSender<(RecordTracker, IngestRecord)>,
    inflight: VecDeque<IngressBatch>,
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
    pub async fn start(mut self, ctx: CancellationToken) {
        let mut state = SessionState::Connecting {
            retry: self.opts.connect_retry_policy.clone().into_iter(),
        };

        debug!(
            partition_id = %self.partition,
            "Starting ingress partition session",
        );

        loop {
            state = match state {
                SessionState::Connecting { retry } => self.connect(&ctx, retry).await,
                SessionState::Connected { connection } => self.connected(&ctx, connection).await,
                SessionState::Disconnected => SessionState::Connecting {
                    retry: self.opts.connect_retry_policy.clone().into_iter(),
                },
                SessionState::Shutdown => {
                    self.rx.close();
                    break;
                }
            }
        }
    }

    async fn connect(
        &self,
        ctx: &CancellationToken,
        mut retry: RetryIter<'static>,
    ) -> SessionState {
        let Some(node_id) = self.partition_routing.get_node_by_partition(self.partition) else {
            tokio::time::sleep(retry.next().unwrap_or_else(|| Duration::from_secs(1))).await;

            return SessionState::Connecting { retry };
        };

        tokio::select! {
            result = self
                .networking
                .get_connection(node_id, Swimlane::IngressData) => {
                    match result {
                        Ok(connection) => SessionState::Connected { connection },
                        Err(ConnectError::Shutdown(_)) => SessionState::Shutdown,
                        Err(err) => {
                            debug!("Failed to connect to node {node_id}: {err}");
                            tokio::time::sleep(retry.next().unwrap_or_else(|| Duration::from_secs(1))).await;
                            SessionState::Connecting { retry }
                        }
                    }
            }
            _ = ctx.cancelled() => {
                SessionState::Shutdown
            }
        }
    }

    /// Re-sends all inflight batches after a connection is restored.
    async fn replay(&mut self, connection: &Connection) -> Result<(), ConnectionClosed> {
        //todo(azmy): to avoid all the inflight batches again and waste traffic
        //maybe test the connection first by sending an empty batch and wait for response
        //before proceeding?

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
            batch.reply_rx = reply_rx.into();
        }

        Ok(())
    }

    async fn connected(&mut self, ctx: &CancellationToken, connection: Connection) -> SessionState {
        if self.replay(&connection).await.is_err() {
            return SessionState::Disconnected;
        }

        let chunked =
            ChunksTimeout::new(&mut self.rx, self.opts.batch_size, self.opts.batch_timeout);
        tokio::pin!(chunked);

        let state = loop {
            let head: OptionFuture<_> = self
                .inflight
                .front_mut()
                .and_then(|batch| batch.reply_rx.as_mut())
                .into();

            tokio::select! {
                Some(batch) = chunked.next() => {
                    let (trackers, records): (Vec<_>, Vec<_>) = batch.into_iter().unzip();
                    let batch: Arc<[IngestRecord]> = Arc::from(records);

                    let Some(permit) = connection.reserve().await else {
                        break SessionState::Disconnected;
                    };

                    trace!("Sending ingest batch, len: {}", batch.len());
                    let reply_rx = permit
                        .send_rpc(IngestRequest::from(Arc::clone(&batch)), Some(self.partition.into()))
                        .expect("encoding version to match");

                    let batch = IngressBatch{records: batch, trackers, reply_rx: Some(reply_rx)};
                    self.inflight.push_back(batch);
                }
                Some(result) = head => {
                    match result {
                        Ok(IngestResponse::Ack) => {
                            let batch = self.inflight.pop_front().expect("not empty");
                            batch.committed();
                        }
                        Ok(response) => {
                            // Handle any other error as a connection loss
                            // and retry all inflight batches.
                            debug!("Ingest response '{:?}'", response);
                            break SessionState::Disconnected;
                        }
                        Err(_err) => {
                            // we can assume that for any error
                            // we need to retry all the inflight bathes.
                            // special case for load shedding we could
                            // throttle the stream a little bit then
                            // speed up over a period of time.

                            break SessionState::Disconnected;
                        }
                    }
                }
                _ = ctx.cancelled() => {
                    // relies on auto drain and drop of the inflight
                    // batches to notify callers that records has been
                    // cancelled.
                    return SessionState::Shutdown;
                }
            }
        };

        // don't lose the buffered batch
        let remainder = chunked.into_remainder();
        if !remainder.is_empty() {
            let (trackers, records): (Vec<_>, Vec<_>) = remainder.into_iter().unzip();
            let batch: Arc<[IngestRecord]> = Arc::from(records);
            self.inflight.push_back(IngressBatch {
                records: batch,
                trackers,
                reply_rx: None,
            });
        }

        state
    }
}

struct SessionManagerInner<T> {
    networking: Networking<T>,
    partition_routing: PartitionRouting,
    opts: SessionOptions,
    ctx: CancellationToken,
    published: ArcSwap<HashMap<PartitionId, SessionHandle>>,
    locks: DashMap<PartitionId, OnceLock<SessionHandle>>,
}

impl<T> SessionManagerInner<T>
where
    T: TransportConnect,
{
    /// Gets or start a new session to partition with given partition id.
    pub fn get(&self, id: PartitionId) -> SessionHandle {
        let inner = self.published.load();
        match inner.get(&id) {
            Some(handle) => handle.clone(),
            None => {
                let once = self.locks.entry(id).or_default();

                let handle = once.get_or_init(|| {
                    let session = PartitionSession::new(
                        self.networking.clone(),
                        self.partition_routing.clone(),
                        id,
                        self.opts.clone(),
                    );

                    let handle = session.handle();

                    //todo(azmy): handle spawn result
                    let ctx = self.ctx.clone();
                    let _ = TaskCenter::spawn_child(
                        TaskKind::Background,
                        "ingress-partition-session",
                        async move {
                            session.start(ctx).await;
                            Ok(())
                        },
                    );

                    handle
                });

                self.published.rcu(|current| {
                    let mut current = current.deref().clone();
                    current.entry(id).or_insert(handle.clone());
                    current
                });

                handle.clone()
            }
        }
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
            published: Default::default(),
            locks: Default::default(),
            ctx: cancellation_token().child_token(),
        };

        Self {
            inner: Arc::new(inner),
        }
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
        self.inner.ctx.cancel();
    }
}
