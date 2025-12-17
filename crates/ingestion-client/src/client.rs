// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{marker::PhantomData, num::NonZeroUsize, sync::Arc, task::Poll};

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
    storage::StorageEncode,
};

use crate::{
    RecordCommit, SessionOptions,
    session::{SessionHandle, SessionManager},
};

/// Errors that can be observed when interacting with the ingestion facade.
#[derive(Debug, thiserror::Error)]
pub enum IngestionError {
    #[error("Ingestion closed")]
    Closed,
    #[error(transparent)]
    PartitionTableError(#[from] PartitionTableError),
}

/// High-level ingestion entry point that allocates permits and hands out session handles per partition.
/// [`IngestionClient`] can be cloned and shared across different routines. All users will share the same budget
/// and underlying partition sessions.
#[derive(Clone)]
pub struct IngestionClient<T, V> {
    manager: SessionManager<T>,
    partition_table: Live<PartitionTable>,
    // memory budget for inflight invocations.
    permits: Arc<Semaphore>,
    memory_budget: NonZeroUsize,
    _phantom: PhantomData<V>,
}

impl<T, V> IngestionClient<T, V> {
    /// Builds a new ingestion facade with the provided networking stack, partition metadata, and
    /// budget (in bytes) for inflight records.
    pub fn new(
        networking: Networking<T>,
        partition_table: Live<PartitionTable>,
        partition_routing: PartitionRouting,
        memory_budget: NonZeroUsize,
        opts: Option<SessionOptions>,
    ) -> Self {
        Self {
            manager: SessionManager::new(networking, partition_routing, opts),
            partition_table,
            permits: Arc::new(Semaphore::new(memory_budget.get())),
            memory_budget,
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
        &self,
        partition_key: PartitionKey,
        record: impl Into<InputRecord<V>>,
    ) -> IngestFuture {
        let record = record.into().into_record();

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
                            .map_err(|_| IngestionError::Closed)
                    }
                    Err(_) => Err(IngestionError::Closed),
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
    fn into_record(self) -> IngestRecord {
        IngestRecord::from_parts(self.keys, self.record)
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
        Version,
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
                10,
                BackPressureMode::PushBack,
            );

        let incoming = svc.start();

        let env = builder.build().await;
        let client = IngestionClient::new(
            env.networking,
            env.metadata.updateable_partition_table(),
            PartitionRouting::new(partition_replica_set_states, TaskCenter::current()),
            NonZeroUsize::new(10 * 1024 * 1024).unwrap(), // 10MB
            SessionOptions {
                batch_size,
                ..Default::default()
            }
            .into(),
        );

        (incoming, client)
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
    async fn test_client_single_record() {
        let (mut incoming, client) = init_env(10).await;

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
                contains(eq(InputRecord::from_str("hello world").into_record()))
            )
        );

        rx.send(ResponseStatus::Ack.into());

        commit.await.expect("to resolve");
    }

    #[test(restate_core::test)]
    async fn test_client_single_record_retry() {
        let (mut incoming, client) = init_env(10).await;

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
                contains(eq(InputRecord::from_str("hello world").into_record()))
            )
        );
        // lets acknowledge it this time
        rx.send(ResponseStatus::Ack.into());

        commit.await.expect("to resolve");
    }

    #[test(restate_core::test)]
    async fn test_client_close() {
        let (_, client) = init_env(10).await;

        let commit = client
            .ingest(0, InputRecord::from_str("hello world"))
            .await
            .unwrap();

        client.close();

        assert!(matches!(commit.await, Err(CancelledError)));
    }

    #[test(restate_core::test(start_paused = true))]
    async fn test_client_dispatch() {
        let (mut incoming, client) = init_env(10).await;

        let pt = Metadata::with_current(|p| p.partition_table_snapshot());

        for p in 0..4 {
            let partition_id = PartitionId::from(p);
            let partition = pt.get(&partition_id).unwrap();
            client
                .ingest(
                    *partition.key_range.start(),
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
}
