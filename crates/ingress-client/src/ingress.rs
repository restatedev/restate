// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use restate_core::{
    network::{Networking, TransportConnect},
    partitions::PartitionRouting,
};
use restate_types::{
    identifiers::PartitionKey,
    live::Live,
    net::ingress::IngestRecord,
    partitions::{FindPartition, PartitionTable, PartitionTableError},
};

use crate::{RecordCommit, SessionOptions, session::SessionManager};

/// Errors that can be observed when interacting with the ingress facade.
#[derive(Debug, thiserror::Error)]
pub enum IngestionError {
    #[error("Ingress closed")]
    Closed,
    #[error(transparent)]
    PartitionTableError(#[from] PartitionTableError),
}

/// High-level ingress entry point that allocates permits and hands out session handles per partition.
/// IngressClient can be cloned and shared across different routines. All users will share the same budget
/// and underlying partition sessions.
#[derive(Clone)]
pub struct IngressClient<T> {
    manager: SessionManager<T>,
    partition_table: Live<PartitionTable>,
    // budget for inflight invocations.
    // this should be a memory budget but it's
    // not possible atm to compute the serialization
    // size of an invocation.
    permits: Arc<Semaphore>,
}

impl<T> IngressClient<T> {
    /// Builds a new ingress facade with the provided networking stack, partition metadata, and
    /// budget for inflight records.
    pub fn new(
        networking: Networking<T>,
        partition_table: Live<PartitionTable>,
        partition_routing: PartitionRouting,
        budget: usize,
        opts: Option<SessionOptions>,
    ) -> Self {
        Self {
            manager: SessionManager::new(networking, partition_routing, opts),
            partition_table,
            permits: Arc::new(Semaphore::new(budget)),
        }
    }
}

impl<T> IngressClient<T>
where
    T: TransportConnect,
{
    /// Reserves capacity to send exactly one record.
    pub async fn reserve(&self) -> Result<IngressPermit<'_, T>, IngestionError> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| IngestionError::Closed)?;

        Ok(IngressPermit {
            permit,
            ingress: self,
        })
    }

    /// Once closed, calls to ingest will return [`IngestionError::Closed`].
    /// Inflight records might still get committed.
    pub fn close(&self) {
        self.permits.close();
        self.manager.close();
    }
}

/// Permit that owns capacity for a single record ingest against an [`Ingress`] instance.
pub struct IngressPermit<'a, T> {
    permit: OwnedSemaphorePermit,
    ingress: &'a IngressClient<T>,
}

impl<'a, T> IngressPermit<'a, T>
where
    T: TransportConnect,
{
    /// Sends a record to the partition derived from the supplied [`PartitionKey`], consuming the permit.
    pub fn ingest(
        self,
        partition_key: PartitionKey,
        record: impl Into<IngestRecord>,
    ) -> Result<RecordCommit, IngestionError> {
        let partition_id = self
            .ingress
            .partition_table
            .pinned()
            .find_partition_id(partition_key)?;

        let handle = self.ingress.manager.get(partition_id);

        handle
            .ingest(self.permit, record.into())
            .map_err(|_| IngestionError::Closed)
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use bytes::Bytes;
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
        logs::Keys,
        net::{
            self, RpcRequest,
            ingress::{IngestRecord, IngestResponse, ReceivedIngestRequest},
            partition_processor::PartitionLeaderService,
        },
        partitions::{
            PartitionTable,
            state::{LeadershipState, PartitionReplicaSetStates},
        },
        retries::RetryPolicy,
    };

    use crate::{CommitError, IngressClient, SessionOptions};

    async fn init_env(
        batch_size: usize,
        batch_timeout: Duration,
    ) -> (
        ServiceStream<PartitionLeaderService>,
        IngressClient<FailingConnector>,
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
        let client = IngressClient::new(
            env.networking,
            env.metadata.updateable_partition_table(),
            PartitionRouting::new(partition_replica_set_states, TaskCenter::current()),
            1000,
            SessionOptions {
                batch_size,
                batch_timeout,
                connect_retry_policy: RetryPolicy::fixed_delay(Duration::from_millis(10), None),
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
        let (mut incoming, client) = init_env(10, Duration::ZERO).await;

        let permit = client.reserve().await.unwrap();

        let commit = permit
            .ingest(
                0,
                IngestRecord {
                    keys: Keys::None,
                    record: Bytes::from("hello world"),
                },
            )
            .unwrap();

        let msg = must_next(&mut incoming).await;
        let (rx, body) = msg.split();
        assert_that!(
            body.records,
            all!(
                len(eq(1)),
                contains(predicate(|v: &IngestRecord| v.record == "hello world"))
            )
        );

        rx.send(IngestResponse::Ack);

        commit.await.expect("to resolve");
    }

    #[test(restate_core::test)]
    async fn test_client_single_record_retry() {
        let (mut incoming, client) = init_env(10, Duration::ZERO).await;

        let permit = client.reserve().await.unwrap();

        let mut commit = permit
            .ingest(
                0,
                IngestRecord {
                    keys: Keys::None,
                    record: Bytes::from("hello world"),
                },
            )
            .unwrap();

        let msg = must_next(&mut incoming).await;
        let (rx, _) = msg.split();
        rx.send(IngestResponse::NotLeader { of: 0.into() });

        assert!((&mut commit).now_or_never().is_none());

        // ingress will retry automatically so we must receive another message
        let msg = must_next(&mut incoming).await;
        let (rx, body) = msg.split();
        assert_that!(
            body.records,
            all!(
                len(eq(1)),
                contains(predicate(|v: &IngestRecord| v.record == "hello world"))
            )
        );
        // lets acknowledge it this time
        rx.send(IngestResponse::Ack);

        commit.await.expect("to resolve");
    }

    #[test(restate_core::test(start_paused = true))]
    async fn test_client_batching() {
        let (mut incoming, client) = init_env(10, Duration::from_millis(10)).await;

        for i in 0..2 {
            let permit = client.reserve().await.unwrap();
            permit
                .ingest(
                    0,
                    IngestRecord {
                        keys: Keys::None,
                        record: Bytes::from(format!("msg {}", i)),
                    },
                )
                .unwrap();
        }

        let permit = client.reserve().await.unwrap();
        let commit = permit
            .ingest(
                0,
                IngestRecord {
                    keys: Keys::None,
                    record: Bytes::from("marker"),
                },
            )
            .unwrap();

        // advance time to force batch timeout.
        tokio::time::advance(Duration::from_millis(10)).await;

        let msg = must_next(&mut incoming).await;
        let (rx, body) = msg.split();
        assert_that!(
            body.records,
            all!(
                len(eq(3)), //2 messages + marker for the test
                contains(predicate(|v: &IngestRecord| v.record == "msg 0")),
                contains(predicate(|v: &IngestRecord| v.record == "marker"))
            )
        );

        rx.send(IngestResponse::Ack);

        commit.await.expect("to resolve");

        for i in 0..9 {
            let permit = client.reserve().await.unwrap();
            permit
                .ingest(
                    0,
                    IngestRecord {
                        keys: Keys::None,
                        record: Bytes::from(format!("msg {}", i)),
                    },
                )
                .unwrap();
        }

        let permit = client.reserve().await.unwrap();
        let commit = permit
            .ingest(
                0,
                IngestRecord {
                    keys: Keys::None,
                    record: Bytes::from("marker"),
                },
            )
            .unwrap();

        // NOT advancing time, the batch has reached
        // max size of 10

        let msg = must_next(&mut incoming).await;
        let (rx, body) = msg.split();
        assert_that!(
            body.records,
            all!(
                len(eq(10)), //9 messages + marker for the test
                contains(predicate(|v: &IngestRecord| v.record == "marker"))
            )
        );

        rx.send(IngestResponse::Ack);

        commit.await.expect("to resolve");
    }

    #[test(restate_core::test)]
    async fn test_client_close() {
        let (_, client) = init_env(10, Duration::ZERO).await;

        let permit = client.reserve().await.unwrap();

        let commit = permit
            .ingest(
                0,
                IngestRecord {
                    keys: Keys::None,
                    record: Bytes::from("hello world"),
                },
            )
            .unwrap();

        client.close();

        assert!(matches!(commit.await, Err(CommitError::Cancelled)));
    }

    #[test(restate_core::test(start_paused = true))]
    async fn test_client_dispatch() {
        let (mut incoming, client) = init_env(10, Duration::from_millis(10)).await;

        let pt = Metadata::with_current(|p| p.partition_table_snapshot());

        for p in 0..4 {
            let partition_id = PartitionId::from(p);
            let permit = client.reserve().await.unwrap();
            let partition = pt.get(&partition_id).unwrap();
            permit
                .ingest(
                    *partition.key_range.start(),
                    IngestRecord {
                        keys: Keys::None,
                        record: Bytes::from(format!("partition {p}")),
                    },
                )
                .unwrap();
        }

        tokio::time::advance(Duration::from_millis(10)).await; // batch timeout

        // what happens is that we still get 4 different message because each targets
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
