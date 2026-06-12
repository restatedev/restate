// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{marker::PhantomData, num::NonZeroUsize, sync::Arc, task::Poll};

use bytes::BytesMut;
use futures::{FutureExt, future::BoxFuture, ready};
use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore};

use restate_core::{
    network::{Networking, TransportConnect},
    partitions::PartitionRouting,
};
use restate_types::{
    identifiers::PartitionKey,
    live::Live,
    logs::{HasRecordKeys, Keys},
    net::ingest::IngestRecord,
    partitions::{FindPartition, PartitionTable, PartitionTableError},
    storage::{StorageCodec, StorageEncode},
};

use crate::{
    RecordCommit, SessionOptions,
    session::{SessionHandle, SessionManager},
};

/// Errors that can be observed when interacting with the ingestion facade.
#[derive(Debug, thiserror::Error)]
pub enum IngestionError {
    #[error("Ingestion client closed: {0}")]
    Closed(&'static str),
    #[error(transparent)]
    PartitionTableError(#[from] PartitionTableError),
    #[error("Record of size {size} bytes exceeds maximum allowed size of {limit} bytes")]
    RecordMaxSizeExceeded { size: usize, limit: usize },
}

/// High-level ingestion entry point that allocates permits and hands out session handles per partition.
/// [`IngestionClient`] can be cloned and shared across different routines. All users will share the same budget
/// and underlying partition sessions.
pub struct IngestionClient<T, V> {
    manager: SessionManager<T>,
    partition_table: Live<PartitionTable>,
    // memory budget for inflight invocations.
    permits: Arc<Semaphore>,
    memory_budget: NonZeroUsize,
    arena: BytesMut,
    _phantom: PhantomData<V>,
}

impl<T, V> Clone for IngestionClient<T, V>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Self {
            manager: self.manager.clone(),
            partition_table: self.partition_table.clone(),
            permits: Arc::clone(&self.permits),
            memory_budget: self.memory_budget,
            arena: BytesMut::default(),
            _phantom: PhantomData,
        }
    }
}

impl<T, V> IngestionClient<T, V> {
    /// Builds a new ingestion facade with the provided networking stack, partition metadata, and
    /// budget (in bytes) for inflight records.
    pub fn new(
        networking: Networking<T>,
        partition_table: Live<PartitionTable>,
        partition_routing: PartitionRouting,
        memory_budget: NonZeroUsize,
        opts: SessionOptions,
    ) -> Self {
        Self {
            manager: SessionManager::new(networking, partition_routing, opts),
            partition_table,
            permits: Arc::new(Semaphore::new(memory_budget.get())),
            memory_budget,
            arena: BytesMut::default(),
            _phantom: PhantomData,
        }
    }
}

impl<T, V> IngestionClient<T, V>
where
    T: TransportConnect,
    V: StorageEncode,
{
    pub fn partition_routing(&self) -> &PartitionRouting {
        self.manager.partition_routing()
    }

    pub fn partition_table(&self) -> &Live<PartitionTable> {
        &self.partition_table
    }

    pub fn networking(&self) -> &Networking<T> {
        self.manager.networking()
    }

    /// Ingest a record with `partition_key`.
    #[must_use]
    pub fn ingest(
        &mut self,
        partition_key: PartitionKey,
        record: impl Into<InputRecord<V>>,
    ) -> IngestFuture {
        let record = record.into().into_record(&mut self.arena);

        if record.estimate_size() > self.manager.options().record_size_limit.get() {
            return IngestFuture::error(IngestionError::RecordMaxSizeExceeded {
                size: record.estimate_size(),
                limit: self.manager.options().record_size_limit.get(),
            });
        }

        let budget = record.estimate_size().min(self.memory_budget.get());

        let partition_id = match self
            .partition_table
            .pinned()
            .find_partition_id(partition_key)
        {
            Ok(partition_id) => partition_id,
            Err(err) => return IngestFuture::error(err.into()),
        };

        let handle = self.manager.get(partition_id);

        let acquire = self.permits.clone().acquire_many_owned(budget as u32);

        IngestFuture::awaiting_permits(record, handle, acquire)
    }

    /// Once closed, calls to ingest will return [`IngestionError::Closed`].
    /// Inflight records might still get committed.
    pub fn close(&self) {
        self.permits.close();
        self.manager.close();
    }
}

/// Future returned by [`IngestionClient::ingest`]
#[pin_project::pin_project(project=IngestFutureStateProj)]
enum IngestFutureState {
    Error {
        err: Option<IngestionError>,
    },
    AwaitingPermit {
        record: Option<IngestRecord>,
        handle: SessionHandle,
        acquire: BoxFuture<'static, Result<OwnedSemaphorePermit, AcquireError>>,
    },
    Done,
}

#[pin_project::pin_project]
pub struct IngestFuture {
    #[pin]
    state: IngestFutureState,
}

impl IngestFuture {
    /// create a "ready" ingestion future that will resolve to error
    fn error(err: IngestionError) -> Self {
        IngestFuture {
            state: IngestFutureState::Error { err: Some(err) },
        }
    }

    /// create a pending ingestion future that will eventually resolve to
    /// [`RecordCommit`] or error
    fn awaiting_permits<F>(record: IngestRecord, handle: SessionHandle, acquire: F) -> Self
    where
        F: Future<Output = Result<OwnedSemaphorePermit, AcquireError>> + Send + 'static,
    {
        Self {
            state: IngestFutureState::AwaitingPermit {
                record: Some(record),
                handle,
                acquire: acquire.boxed(),
            },
        }
    }
}

impl Future for IngestFuture {
    type Output = Result<RecordCommit, IngestionError>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut this = self.project();
        let result = match this.state.as_mut().project() {
            IngestFutureStateProj::Error { err } => Poll::Ready(Err(err.take().unwrap())),
            IngestFutureStateProj::AwaitingPermit {
                record,
                handle,
                acquire,
            } => {
                let output = match ready!(acquire.as_mut().poll(cx)) {
                    Ok(permit) => {
                        let record = record.take().unwrap();
                        handle
                            .ingest(permit, record)
                            .map_err(|_| IngestionError::Closed("partition session closed"))
                    }
                    Err(_) => Err(IngestionError::Closed("permits semaphore closed")),
                };

                Poll::Ready(output)
            }
            IngestFutureStateProj::Done => {
                panic!("polled IngestFuture after completion");
            }
        };

        this.state.set(IngestFutureState::Done);
        result
    }
}

pub struct InputRecord<T> {
    keys: Keys,
    record: T,
}

impl<T> InputRecord<T>
where
    T: StorageEncode,
{
    fn into_record(self, buf: &mut BytesMut) -> IngestRecord {
        StorageCodec::encode(&self.record, buf).expect("encode to pass");

        IngestRecord::new(self.keys, buf.split().freeze())
    }
}

impl<T> From<T> for InputRecord<T>
where
    T: HasRecordKeys + StorageEncode,
{
    fn from(value: T) -> Self {
        InputRecord {
            keys: value.record_keys(),
            record: value,
        }
    }
}

impl InputRecord<String> {
    #[cfg(test)]
    fn from_str(s: impl Into<String>) -> Self {
        InputRecord {
            keys: Keys::None,
            record: s.into(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::{num::NonZeroUsize, time::Duration};

    use bytes::BytesMut;
    use futures::{FutureExt, StreamExt};
    use googletest::prelude::*;
    use test_log::test;

    use restate_core::{
        Metadata, TaskCenter, TestCoreEnvBuilder,
        network::{
            BackPressureMode, FailingConnector, Incoming, Rpc, ServiceMessage, ServiceStream,
        },
        partitions::PartitionRouting,
    };

    use restate_types::{
        GenerationalNodeId, Version,
        identifiers::{LeaderEpoch, PartitionId},
        net::{
            self, RpcRequest,
            ingest::{ReceivedIngestRequest, ResponseStatus},
            partition_processor::PartitionLeaderService,
        },
        partitions::{
            PartitionTable,
            state::{LeadershipState, PartitionReplicaSetStates},
        },
    };

    use crate::{CancelledError, IngestionClient, SessionOptions, client::InputRecord};

    async fn init_env(
        batch_size: usize,
    ) -> (
        ServiceStream<PartitionLeaderService>,
        IngestionClient<FailingConnector, String>,
    ) {
        let (incoming, client, _states, _my_node_id) = init_env_with_states(batch_size).await;
        (incoming, client)
    }

    /// Like [`init_env`] but also returns the partition replica-set states (so tests can simulate
    /// leadership changes via `note_observed_leader`) and the node id acting as leader.
    async fn init_env_with_states(
        batch_size: usize,
    ) -> (
        ServiceStream<PartitionLeaderService>,
        IngestionClient<FailingConnector, String>,
        PartitionReplicaSetStates,
        GenerationalNodeId,
    ) {
        let mut builder = TestCoreEnvBuilder::with_incoming_only_connector()
            .add_mock_nodes_config()
            .set_partition_table(PartitionTable::with_equally_sized_partitions(
                Version::MIN,
                4,
            ));

        let partition_replica_set_states = PartitionReplicaSetStates::default();
        for i in 0..4 {
            partition_replica_set_states.note_observed_leader(
                i.into(),
                LeadershipState {
                    current_leader: builder.my_node_id,
                    current_leader_epoch: LeaderEpoch::INITIAL,
                },
            );
        }

        let svc = builder
            .router_builder
            .register_service::<net::partition_processor::PartitionLeaderService>(
                BackPressureMode::PushBack,
            );

        let incoming = svc.start();

        let my_node_id = builder.my_node_id;
        let env = builder.build().await;
        let client = IngestionClient::new(
            env.networking,
            env.metadata.updateable_partition_table(),
            PartitionRouting::new(partition_replica_set_states.clone(), TaskCenter::current()),
            NonZeroUsize::new(10 * 1024 * 1024).unwrap(), // 10MB
            SessionOptions::builder()
                .batch_size(NonZeroUsize::new(batch_size).unwrap())
                .build()
                .unwrap(),
        );

        (incoming, client, partition_replica_set_states, my_node_id)
    }

    async fn must_next(
        recv: &mut ServiceStream<PartitionLeaderService>,
    ) -> Incoming<Rpc<ReceivedIngestRequest>> {
        let Some(ServiceMessage::Rpc(msg)) = recv.next().await else {
            panic!("stream closed");
        };

        assert_eq!(msg.msg_type(), ReceivedIngestRequest::TYPE);
        msg.into_typed()
    }

    #[test(restate_core::test)]
    async fn client_single_record() {
        let (mut incoming, mut client) = init_env(10).await;
        let mut buf = BytesMut::new();

        let commit = client
            .ingest(0, InputRecord::from_str("hello world"))
            .await
            .unwrap();

        let msg = must_next(&mut incoming).await;
        let (rx, body) = msg.split();
        assert_that!(
            body.records,
            all!(
                len(eq(1)),
                contains(eq(
                    InputRecord::from_str("hello world").into_record(&mut buf)
                ))
            )
        );

        rx.send(ResponseStatus::Ack.into());

        commit.await.expect("to resolve");
    }

    #[test(restate_core::test)]
    async fn client_single_record_retry() {
        let (mut incoming, mut client) = init_env(10).await;
        let mut buf = BytesMut::new();

        let mut commit = client
            .ingest(0, InputRecord::from_str("hello world"))
            .await
            .unwrap();

        let msg = must_next(&mut incoming).await;
        let (rx, _) = msg.split();
        rx.send(ResponseStatus::NotLeader { of: 0.into() }.into());

        assert!((&mut commit).now_or_never().is_none());

        // ingestion will retry automatically so we must receive another message
        let msg = must_next(&mut incoming).await;
        let (rx, body) = msg.split();
        assert_that!(
            body.records,
            all!(
                len(eq(1)),
                contains(eq(
                    InputRecord::from_str("hello world").into_record(&mut buf)
                ))
            )
        );
        // lets acknowledge it this time
        rx.send(ResponseStatus::Ack.into());

        commit.await.expect("to resolve");
    }

    // A persistent `NotLeader` rejection must not produce a busy reconnect loop: the
    // session backs off (escalating) before replaying the in-flight batch. With the
    // clock paused, the elapsed virtual time proves a sleep was inserted between the
    // rejection and the replay; an `Ack` then resolves the commit.
    #[test(restate_core::test(start_paused = true))]
    async fn not_leader_backs_off_before_replay() {
        let (mut incoming, mut client) = init_env(10).await;

        let mut commit = client
            .ingest(0, InputRecord::from_str("hello world"))
            .await
            .unwrap();

        // First attempt reaches the (stale) leader and is rejected.
        let msg = must_next(&mut incoming).await;
        let (rx, _) = msg.split();
        rx.send(ResponseStatus::NotLeader { of: 0.into() }.into());
        assert!((&mut commit).now_or_never().is_none());

        // The replay must be preceded by a backoff (>= the 25ms first step) rather than
        // an immediate resend that would tight-loop while routing is stale.
        let start = tokio::time::Instant::now();
        let msg = must_next(&mut incoming).await;
        assert!(
            start.elapsed() >= Duration::from_millis(25),
            "expected a backoff before replay, but the resend was immediate"
        );

        let (rx, _) = msg.split();
        rx.send(ResponseStatus::Ack.into());
        commit.await.expect("to resolve");
    }

    #[test(restate_core::test)]
    async fn client_close() {
        let (_, mut client) = init_env(10).await;

        let commit = client
            .ingest(0, InputRecord::from_str("hello world"))
            .await
            .unwrap();

        client.close();

        assert!(matches!(commit.await, Err(CancelledError)));
    }

    #[test(restate_core::test(start_paused = true))]
    async fn client_dispatch() {
        let (mut incoming, mut client) = init_env(10).await;

        let pt = Metadata::with_current(|p| p.partition_table_snapshot());

        for p in 0..4 {
            let partition_id = PartitionId::from(p);
            let partition = pt.get(&partition_id).unwrap();
            client
                .ingest(
                    partition.key_range.start(),
                    InputRecord::from_str(format!("partition {p}")),
                )
                .await
                .unwrap();
        }

        tokio::time::advance(Duration::from_millis(10)).await; // batch timeout

        // what happens is that we still get 4 different messages because each targets
        // a single partition.
        let mut received = vec![];
        for _ in 0..4 {
            let msg = must_next(&mut incoming).await;
            received.push(msg.sort_code());
        }

        assert_that!(
            received,
            all!(
                len(eq(4)), //4 messages for 4 partitions
                contains(eq(Some(0))),
                contains(eq(Some(1))),
                contains(eq(Some(2))),
                contains(eq(Some(3))),
            )
        );
    }

    // Sequential mode is the only mode the session runs today (see `connected_sequential_mode`):
    // it keeps at most one batch in flight, sending the next batch only after the head is acked.
    // This pins that invariant — the tail batch must not reach the wire while the head is unacked —
    // and that every batch carries the leader epoch. It is the dual of the (ignored)
    // `pipelines_multiple_unacked_batches`.
    #[test(restate_core::test(start_paused = true))]
    async fn sequential_mode_one_batch_in_flight() {
        let mut buf = BytesMut::new();
        // Cap fits exactly one record, so r0 and r1 form two separate batches.
        let one_record = InputRecord::from_str("r0")
            .into_record(&mut buf)
            .estimate_size();
        let (mut incoming, mut client) = init_env(one_record).await;

        // Queue both records before the session forms a batch: `ingest().await` does not yield to
        // the session task, so both are in the channel when the chunker first runs.
        let c0 = client.ingest(0, InputRecord::from_str("r0")).await.unwrap();
        let c1 = client.ingest(0, InputRecord::from_str("r1")).await.unwrap();

        // Only the head batch B1=[r0] reaches the wire; r1 is held back by the chunker.
        let head = must_next(&mut incoming).await;
        let (head_rx, head_body) = head.split();
        assert_that!(
            head_body.records,
            all!(
                len(eq(1)),
                contains(eq(InputRecord::from_str("r0").into_record(&mut buf)))
            )
        );

        // While the head is unacked, the tail must not be sent. Advance time to let the session
        // settle, then assert nothing else is on the wire.
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(
            incoming.next().now_or_never().is_none(),
            "tail batch must not be sent before the head is acked"
        );

        // Ack the head; only now does the tail batch B2=[r1] reach the wire.
        head_rx.send(ResponseStatus::Ack.into());
        c0.await.expect("r0 commits");

        let tail = must_next(&mut incoming).await;
        let (tail_rx, tail_body) = tail.split();
        assert_that!(
            tail_body.records,
            all!(
                len(eq(1)),
                contains(eq(InputRecord::from_str("r1").into_record(&mut buf)))
            )
        );

        tail_rx.send(ResponseStatus::Ack.into());
        c1.await.expect("r1 commits");
    }

    // Regression coverage for #4810 under sequential mode (the mode in force today): a leadership
    // transition must not reorder or drop records. The head batch is rejected with
    // `NotLeaderWithEpoch`; the session carries it over together with the record the chunker
    // over-pulled, then replays both — in produced order — against the new epoch, still one batch
    // at a time. Nothing is failed/cancelled (the Kafka ingress and shuffle have no cheap retry),
    // so out-of-order appends the dedup high-water-mark would silently drop cannot happen. This is
    // the sequential dual of the (ignored) `leadership_change_replays_inflight_in_order`.
    #[test(restate_core::test(start_paused = true))]
    async fn sequential_mode_leadership_change_replays_in_order() {
        // Cap fits exactly one record, so r0 and r1 form two separate batches.
        let mut buf = BytesMut::new();
        let one_record = InputRecord::from_str("r0")
            .into_record(&mut buf)
            .estimate_size();
        let (mut incoming, mut client, states, my_node_id) = init_env_with_states(one_record).await;

        let c0 = client.ingest(0, InputRecord::from_str("r0")).await.unwrap();
        let c1 = client.ingest(0, InputRecord::from_str("r1")).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Sequential mode: only the head batch B1=[r0] is on the wire; r1 is held by the chunker.
        let b1 = must_next(&mut incoming).await;
        let (b1_rx, b1_body) = b1.split();
        assert_that!(
            b1_body.records,
            all!(
                len(eq(1)),
                contains(eq(InputRecord::from_str("r0").into_record(&mut buf)))
            )
        );

        // No second batch is in flight while the head is unacked.
        assert!(
            incoming.next().now_or_never().is_none(),
            "tail batch must not be in flight under sequential mode"
        );

        // A new leader wins the election. Make routing observe the new epoch so the session can
        // reconnect, then reject the head with the new epoch.
        let new_epoch = LeaderEpoch::INITIAL.next();
        let leadership_state = LeadershipState {
            current_leader: my_node_id,
            current_leader_epoch: new_epoch,
        };
        states.note_observed_leader(PartitionId::from(0), leadership_state);

        b1_rx.send(ResponseStatus::NotLeader { of: 0.into() }.into());
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Replay re-sends B1=[r0] then B2=[r1] in produced order at the new epoch — and still one
        // at a time, so B2 only appears after B1 is acked.
        let b1 = must_next(&mut incoming).await;
        let (b1_rx, b1_body) = b1.split();
        assert_that!(
            b1_body.records,
            all!(
                len(eq(1)),
                contains(eq(InputRecord::from_str("r0").into_record(&mut buf)))
            )
        );
        b1_rx.send(ResponseStatus::Ack.into());

        let b2 = must_next(&mut incoming).await;
        let (b2_rx, b2_body) = b2.split();
        assert_that!(
            b2_body.records,
            all!(
                len(eq(1)),
                contains(eq(InputRecord::from_str("r1").into_record(&mut buf)))
            )
        );
        b2_rx.send(ResponseStatus::Ack.into());

        // Nothing was failed/cancelled: both records eventually commit, in order.
        c0.await.expect("r0 commits");
        c1.await.expect("r1 commits");
    }
}
