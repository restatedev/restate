// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The processor's side of query reads: it admits each one between two committed
//! turns, checking that it still hosts the partition, is ready, and leads if the
//! read needs the leader, and captures what the read asks for (see
//! `restate_worker_api::partition_capture`). When the processor stops, it admits
//! no more and cancels the snapshots it handed out; closing the partition store
//! then waits for them to be released.

use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use restate_partition_store::PartitionStore;
use restate_types::GenerationalNodeId;
use restate_worker_api::{
    CapturedPartitionRead, CapturedPartitionState, PartitionCaptureRequirements,
    PartitionReadError, PartitionReadRequest,
};

/// Whether a processor admits query reads: not before its loop starts, and not
/// once it stops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueryReadiness {
    Initializing,
    Ready,
    Stopping,
}

/// A processor's queue of query reads, its readiness, and the cancellation of
/// the snapshots it admitted.
pub(super) struct QueryReads {
    requests: mpsc::Receiver<PartitionReadRequest>,
    ready: watch::Sender<QueryReadiness>,
    cancel: CancellationToken,
}

impl QueryReads {
    pub fn new(
        requests: mpsc::Receiver<PartitionReadRequest>,
        ready: watch::Sender<QueryReadiness>,
    ) -> Self {
        Self {
            requests,
            ready,
            cancel: CancellationToken::new(),
        }
    }

    pub fn mark_ready(&self) {
        self.ready.send_replace(QueryReadiness::Ready);
    }

    pub async fn next(&mut self) -> Option<PartitionReadRequest> {
        self.requests.recv().await
    }

    /// Whether requests wait for admission, without taking any.
    pub fn has_pending(&self) -> bool {
        !self.requests.is_empty()
    }

    /// Admits `request` between two committed turns and answers it: checks that
    /// this processor still hosts the partition, is ready, and leads if the read
    /// needs the leader, then captures the storage snapshot and, through
    /// `capture`, the live state it asks for. The query's metadata reaches here
    /// unchanged; admission doesn't use it.
    pub async fn admit<F, Fut>(
        &mut self,
        store: &PartitionStore,
        is_leader: bool,
        leader_hint: Option<GenerationalNodeId>,
        mut request: PartitionReadRequest,
        capture: F,
    ) where
        F: FnOnce(PartitionCaptureRequirements) -> Fut,
        Fut: Future<Output = Result<CapturedPartitionState, PartitionReadError>>,
    {
        let error = if request.partition != store.partition_id() {
            Some(PartitionReadError::PartitionMismatch {
                requested: request.partition,
                actual: store.partition_id(),
            })
        } else if self.cancel.is_cancelled() || *self.ready.borrow() == QueryReadiness::Stopping {
            Some(PartitionReadError::Stopping(request.partition))
        } else if *self.ready.borrow() == QueryReadiness::Initializing {
            Some(PartitionReadError::Initializing(request.partition))
        } else if (request.require_leader || request.capture.needs_live_state()) && !is_leader {
            Some(PartitionReadError::NotLeader {
                partition: request.partition,
                leader: leader_hint,
            })
        } else {
            None
        };
        if let Some(error) = error {
            let _ = request.response.send(Err(error));
            return;
        }
        if request.response.is_closed() {
            return;
        }
        let storage = if request.capture.storage {
            match store.query_read(self.cancel.child_token()) {
                Ok((read, _)) => Some(read),
                Err(_) => {
                    let _ = request
                        .response
                        .send(Err(PartitionReadError::Stopping(request.partition)));
                    return;
                }
            }
        } else {
            None
        };
        let live = if request.capture.needs_live_state() {
            tokio::select! {
                _ = request.response.closed() => return,
                _ = self.cancel.cancelled() => {
                    let _ = request.response.send(Err(PartitionReadError::Stopping(request.partition)));
                    return;
                }
                result = capture(request.capture) => result,
            }
        } else {
            Ok(CapturedPartitionState::default())
        };
        let read = live.and_then(|live| {
            if !live.satisfies(request.capture) {
                return Err(PartitionReadError::LiveCaptureUnavailable(
                    request.partition,
                ));
            }
            Ok(CapturedPartitionRead {
                partition: request.partition,
                storage,
                live,
            })
        });
        let _ = request.response.send(read);
    }

    pub async fn shutdown(&mut self) {
        self.ready.send_replace(QueryReadiness::Stopping);
        self.requests.close();
        // Storage session close owns drainage of the admitted snapshots.
        self.cancel.cancel();
        while let Some(request) = self.requests.recv().await {
            let _ = request
                .response
                .send(Err(PartitionReadError::Stopping(request.partition)));
        }
    }
}

impl Drop for QueryReads {
    fn drop(&mut self) {
        self.ready.send_replace(QueryReadiness::Stopping);
        self.cancel.cancel();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use futures::FutureExt;
    use tokio::sync::oneshot;

    use restate_clock::UniqueTimestamp;
    use restate_partition_store::PartitionStoreManager;
    use restate_rocksdb::RocksDbManager;
    use restate_storage_query_api::QueryMetadata;
    use restate_types::partitions::Partition;
    use restate_types::sharding::{KeyRange, PartitionId};
    use restate_worker_api::{ProcessorsManagerCommand, ProcessorsManagerHandle};

    use crate::partition_processor_manager::PartitionLeaderHandlesRegistry;
    use crate::rule_book_cache::RuleBookCacheHandle;

    use super::*;

    #[restate_core::test]
    async fn processor_query_admission_preserves_metadata_and_drains_after_cancellation() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        let metadata = Arc::new(QueryMetadata {
            session_id: "original-session".into(),
            query_ts: UniqueTimestamp::MIN,
            headers: Default::default(),
            redacted_sql: "SELECT * FROM state".into(),
            planning_duration: Duration::from_millis(19),
        });
        let (ppm_tx, mut ppm_rx) = mpsc::channel(4);
        let handle = ProcessorsManagerHandle::new(
            ppm_tx,
            Arc::new(RuleBookCacheHandle::detached()),
            Arc::new(PartitionLeaderHandlesRegistry::default()),
        );
        let (query_tx, query_rx) = mpsc::channel(1);
        let (ready_tx, ready_rx) = watch::channel(QueryReadiness::Initializing);
        let mut reads = QueryReads::new(query_rx, ready_tx);
        let request = |require_leader| {
            let (response, rx) = oneshot::channel();
            (
                PartitionReadRequest {
                    partition: PartitionId::MIN,
                    require_leader,
                    metadata: Arc::clone(&metadata),
                    capture: PartitionCaptureRequirements {
                        storage: true,
                        ..Default::default()
                    },
                    response,
                },
                rx,
            )
        };
        let (unready, response) = request(false);
        reads
            .admit(&store, true, None, unready, |_| async {
                panic!("unready capture")
            })
            .await;
        assert!(
            matches!(
                response.await.unwrap(),
                Err(PartitionReadError::Initializing(_))
            ),
            "processor_rechecks_readiness"
        );
        reads.mark_ready();
        assert_eq!(
            *ready_rx.borrow(),
            QueryReadiness::Ready,
            "readiness_published_after_initialization"
        );
        for leader in [None, Some(GenerationalNodeId::new(2, 3))] {
            let (leader_only, response) = request(true);
            reads
                .admit(&store, false, leader, leader_only, |_| async {
                    panic!("follower capture")
                })
                .await;
            assert_eq!(
                response.await.unwrap().unwrap_err(),
                PartitionReadError::NotLeader {
                    partition: PartitionId::MIN,
                    leader
                },
                "processor_preserves_observed_leader_without_inventing_one"
            );
        }

        // Capture only requested sources, once, and keep their owned rows with
        // the storage snapshot. A live-only read does not open a storage snapshot.
        for storage in [true, false] {
            let (mut capture_request, response) = request(false);
            capture_request.capture = PartitionCaptureRequirements {
                storage,
                scheduler: true,
                invoker: true,
                user_limits: true,
            };
            reads
                .admit(&store, true, None, capture_request, |required| async move {
                    assert!(required.invoker && required.scheduler && required.user_limits);
                    let mut captured = CapturedPartitionState::empty(required);
                    captured.scheduler = Some(
                        vec![(
                            restate_types::vqueues::VQueueId::custom(0, "queue"),
                            Default::default(),
                        )]
                        .into(),
                    );
                    Ok(captured)
                })
                .await;
            let captured = response.await.unwrap().unwrap();
            assert_eq!(captured.storage.is_some(), storage);
            let repeated = captured.clone();
            assert!(Arc::ptr_eq(
                captured.live.scheduler.as_ref().unwrap(),
                repeated.live.scheduler.as_ref().unwrap()
            ));
            assert_eq!(captured.live.scheduler.as_ref().unwrap().len(), 1);
            assert!(captured.live.invoker.as_ref().unwrap().is_empty());
            assert!(captured.live.user_limits.as_ref().unwrap().is_empty());
        }
        let (mut unavailable, response) = request(false);
        unavailable.capture.invoker = true;
        reads
            .admit(&store, true, None, unavailable, |_| async {
                Ok(CapturedPartitionState::default())
            })
            .await;
        assert_eq!(
            response.await.unwrap().unwrap_err(),
            PartitionReadError::LiveCaptureUnavailable(PartitionId::MIN)
        );

        let (mut follower, response) = request(false);
        follower.capture.scheduler = true;
        reads
            .admit(&store, false, None, follower, |_| async {
                panic!("live capture requires leader even without explicit flag")
            })
            .await;
        assert!(matches!(
            response.await.unwrap(),
            Err(PartitionReadError::NotLeader { .. })
        ));

        let (mut abandoned, response) = request(false);
        abandoned.capture.invoker = true;
        let admission = reads.admit(&store, true, None, abandoned, |_| std::future::pending());
        tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(admission, async {
                tokio::task::yield_now().await;
                drop(response);
            });
        })
        .await
        .expect("dropping the requester cancels a pending capture");
        let (mut wrong_partition, response) = request(false);
        wrong_partition.partition = PartitionId::from(2);
        reads
            .admit(&store, true, None, wrong_partition, |_| async {
                panic!("wrong partition capture")
            })
            .await;
        assert_eq!(
            response.await.unwrap().unwrap_err(),
            PartitionReadError::PartitionMismatch {
                requested: PartitionId::from(2),
                actual: PartitionId::MIN
            },
            "misrouted_request_is_not_a_readiness_failure"
        );

        let mut result =
            Box::pin(handle.read_partition(PartitionId::MIN, false, Arc::clone(&metadata)));
        assert!(result.as_mut().now_or_never().is_none());
        let ProcessorsManagerCommand::ReadPartition(forwarded) = ppm_rx.recv().await.unwrap()
        else {
            panic!("expected admission")
        };
        assert!(
            Arc::ptr_eq(&forwarded.metadata, &metadata),
            "ppm_preserves_original_metadata"
        );
        query_tx.send(forwarded).await.unwrap();
        let forwarded = reads.next().await.unwrap();
        assert!(
            Arc::ptr_eq(&forwarded.metadata, &metadata),
            "processor_receives_original_metadata"
        );
        reads
            .admit(&store, false, None, forwarded, |_| async {
                panic!("storage-only capture")
            })
            .await;
        let read = result.await.unwrap();
        let cancelled = read.cancellation_token();
        let (queued, response) = request(false);
        query_tx.send(queued).await.unwrap();
        reads.shutdown().await;
        assert!(
            cancelled.is_cancelled(),
            "shutdown_cancels_admitted_subtrees"
        );
        assert_eq!(
            *ready_rx.borrow(),
            QueryReadiness::Stopping,
            "shutdown_closes_admission_before_draining"
        );
        assert!(query_tx.is_closed());
        assert_eq!(
            response.await.unwrap().unwrap_err(),
            PartitionReadError::Stopping(PartitionId::MIN),
            "shutdown_rejects_queued_requests_explicitly"
        );
        drop(read);

        let mut result =
            Box::pin(handle.read_partition(PartitionId::MIN, false, Arc::clone(&metadata)));
        assert!(result.as_mut().now_or_never().is_none());
        drop(ppm_rx.recv().await.unwrap());
        assert_eq!(
            result.await.unwrap_err(),
            PartitionReadError::ResponseDropped(PartitionId::MIN),
            "lost_response_does_not_claim_a_lifecycle_phase"
        );
        drop(ppm_rx);
        assert_eq!(
            handle
                .read_partition(PartitionId::MIN, false, metadata)
                .await
                .unwrap_err(),
            PartitionReadError::ManagerClosed(PartitionId::MIN)
        );
    }

    /// Admitted reads get a snapshot bound to their admission: a statistic that
    /// isn't registered yet fails to read, and a registered one shows what was
    /// committed before the admission, not later.
    #[restate_core::test]
    async fn admitted_reads_are_bound_to_their_snapshot_and_its_statistics() {
        use std::ops::ControlFlow;

        use restate_partition_store::keys::selection::KeySelection;
        use restate_partition_store::stats::StatValueCodec;
        use restate_partition_store::stats::aggregated::{ServiceStats, StageStatusCounts};
        use restate_partition_store::{PartitionStore, ReadSnapshot};
        use restate_storage_api::StorageError;
        use restate_storage_api::Transaction;
        use restate_storage_api::vqueue_table::stats::EntryStatistics;
        use restate_storage_api::vqueue_table::{
            EntryContext, EntryKey, EntryMetadata, EntryStateRef, Stage, Status, WriteVQueueTable,
        };
        use restate_storage_query_api::scan::BlockingScan;
        use restate_types::clock::RoughTimestamp;
        use restate_types::config::Configuration;
        use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef, VQueueId};

        async fn admitted(store: &PartitionStore, reads: &mut QueryReads) -> ReadSnapshot {
            let (response, rx) = oneshot::channel();
            let request = PartitionReadRequest {
                partition: PartitionId::MIN,
                require_leader: false,
                metadata: Arc::new(QueryMetadata {
                    session_id: "registered".into(),
                    query_ts: UniqueTimestamp::MIN,
                    headers: Default::default(),
                    redacted_sql: "SELECT * FROM sys_service_stats".into(),
                    planning_duration: Duration::ZERO,
                }),
                capture: PartitionCaptureRequirements {
                    storage: true,
                    ..Default::default()
                },
                response,
            };
            reads
                .admit(store, true, None, request, |_| async {
                    panic!("storage-only capture")
                })
                .await;
            rx.await.unwrap().unwrap().storage.unwrap()
        }
        /// The service statistic's total in `snapshot`, or `Err` if it can't be
        /// read there.
        fn total(snapshot: &ReadSnapshot) -> Result<u64, StorageError> {
            let mut counted = 0;
            <ReadSnapshot as BlockingScan<ServiceStats>>::scan_blocking(
                snapshot,
                Arc::new(KeySelection::all()),
                |(_, payload)| {
                    let counts = StageStatusCounts::deserialize_from(payload).unwrap();
                    counted += counts.iter().map(|(_, count)| count).sum::<u64>();
                    ControlFlow::Continue(())
                },
            )
            .map(|()| counted)
        }

        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        let write = async |store: &mut PartitionStore, n: u8| {
            let qid = VQueueId::custom(3337, "admitted");
            let key = EntryKey::new(
                false,
                RoughTimestamp::new(1),
                u64::from(n),
                EntryId::new(EntryKind::StateMutation, [n; 16]),
            );
            let mut tx = store.transaction();
            tx.create_vqueue_entry_status(
                &EntryContext {
                    qid: &qid,
                    target: &EntryTargetRef::Service {
                        scope: None,
                        service: "svc",
                        handler: "run",
                    },
                },
                EntryStateRef {
                    stage: Stage::Inbox,
                    status: Status::New,
                    entry_key: &key,
                    metadata: &EntryMetadata::default(),
                    stats: &EntryStatistics::new(UniqueTimestamp::MIN, key.run_at()),
                },
            );
            tx.commit().await.unwrap();
        };
        let (_query_tx, query_rx) = mpsc::channel(1);
        let (ready_tx, _ready_rx) = watch::channel(QueryReadiness::Initializing);
        let mut reads = QueryReads::new(query_rx, ready_tx);
        reads.mark_ready();

        // Before the statistic is registered, reading it fails.
        let unregistered = admitted(&store, &mut reads).await;
        assert!(total(&unregistered).is_err());

        // Registered over an empty partition, it's Ready at once. Each admitted
        // snapshot shows the entries committed before it, and keeps showing them.
        let mut config = Configuration::default();
        config.common.experimental.set_track_status_gauges(true);
        store
            .verify_and_run_migrations(Default::default(), &config)
            .await
            .unwrap();
        write(&mut store, 1).await;
        let first = admitted(&store, &mut reads).await;
        write(&mut store, 2).await;
        let second = admitted(&store, &mut reads).await;
        assert_eq!(
            [&first, &second].map(|snapshot| total(snapshot).unwrap()),
            [1, 2]
        );
        assert!(total(&unregistered).is_err(), "stays bound to its snapshot");
    }
}
