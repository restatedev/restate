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
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use restate_partition_store::PartitionStore;
use restate_types::GenerationalNodeId;
use restate_worker_api::{
    CaptureSpan, CaptureTiming, CapturedPartitionRead, CapturedPartitionState,
    PartitionCaptureRequirements, PartitionReadError, PartitionReadRequest,
};

/// Whether a processor admits query reads: not before its loop starts, and not
/// once it stops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueryReadiness {
    Initializing,
    Ready,
    Stopping,
}

/// Whether this processor leads, as far as a query read is concerned. Live state
/// exists only once the processor is the leader: while it is still becoming the
/// leader, reads that need the leader are told to retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Leading {
    No,
    Becoming,
    Yes,
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
    /// `capture`, the live state it asks for. Admission doesn't wait for anything:
    /// it runs in the processor's loop. The query's metadata reaches here
    /// unchanged; admission doesn't use it.
    pub fn admit(
        &mut self,
        store: &PartitionStore,
        leading: Leading,
        leader_hint: Option<GenerationalNodeId>,
        request: PartitionReadRequest,
        capture: impl FnOnce(
            PartitionCaptureRequirements,
        ) -> Result<CapturedPartitionState, PartitionReadError>,
    ) {
        let mut timing = CaptureTiming::admitted_at(Instant::now());
        let error = if request.partition != store.partition_id() {
            Some(PartitionReadError::PartitionMismatch {
                requested: request.partition,
                actual: store.partition_id(),
            })
        } else if self.cancel.is_cancelled() || *self.ready.borrow() == QueryReadiness::Stopping {
            Some(PartitionReadError::Stopping(request.partition))
        } else if *self.ready.borrow() == QueryReadiness::Initializing {
            Some(PartitionReadError::Initializing(request.partition))
        } else if request.require_leader || request.capture.needs_live_state() {
            match leading {
                Leading::Yes => None,
                Leading::Becoming => Some(PartitionReadError::Initializing(request.partition)),
                Leading::No => Some(PartitionReadError::NotLeader {
                    partition: request.partition,
                    leader: leader_hint,
                }),
            }
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
            let start = Instant::now();
            match store.query_read(self.cancel.child_token()) {
                Ok((read, _)) => {
                    timing.snapshot = Some(CaptureSpan::since(start));
                    Some(read)
                }
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
        // The processor captures only the state it owns. The manager's handle reads
        // the invoker's rows after admission (`ProcessorsManagerHandle::capture_partition`).
        let owned = PartitionCaptureRequirements {
            invoker: false,
            ..request.capture
        };
        let live = if owned.needs_live_state() {
            let start = Instant::now();
            let live = capture(owned);
            timing.live = Some(CaptureSpan::since(start));
            live
        } else {
            Ok(CapturedPartitionState::default())
        };
        let read = live.and_then(|live| {
            if !live.satisfies(owned) {
                return Err(PartitionReadError::LiveCaptureUnavailable(
                    request.partition,
                ));
            }
            Ok(CapturedPartitionRead {
                partition: request.partition,
                storage,
                live,
                timing,
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

    /// An invoker that never answers status requests.
    struct StalledInvoker;

    impl restate_worker_api::PartitionQueryAccess for StalledInvoker {
        fn scan_invoker_status(
            &self,
            _: PartitionId,
            _: KeyRange,
        ) -> restate_worker_api::PartitionQueryStream<
            restate_worker_api::invoker::InvocationStatusReport,
        > {
            futures::StreamExt::boxed(futures::stream::pending())
        }

        fn scan_scheduler_status(
            &self,
            _: PartitionId,
            _: KeyRange,
        ) -> restate_worker_api::PartitionQueryStream<restate_worker_api::SchedulerStatusEntry>
        {
            unreachable!("the processor captures the scheduler")
        }

        fn scan_user_limit_counters(
            &self,
            _: PartitionId,
            _: KeyRange,
        ) -> restate_worker_api::PartitionQueryStream<restate_worker_api::UserLimitCounterEntry>
        {
            unreachable!("the processor captures the user limits")
        }
    }

    /// An invoker whose status scan ends without rows once `0` is notified.
    struct SlowInvoker(Arc<tokio::sync::Notify>);

    impl restate_worker_api::PartitionQueryAccess for SlowInvoker {
        fn scan_invoker_status(
            &self,
            _: PartitionId,
            _: KeyRange,
        ) -> restate_worker_api::PartitionQueryStream<
            restate_worker_api::invoker::InvocationStatusReport,
        > {
            let done = Arc::clone(&self.0);
            let scan = futures::stream::once(async move { done.notified().await });
            futures::StreamExt::boxed(futures::StreamExt::filter_map(scan, |()| async { None }))
        }

        fn scan_scheduler_status(
            &self,
            _: PartitionId,
            _: KeyRange,
        ) -> restate_worker_api::PartitionQueryStream<restate_worker_api::SchedulerStatusEntry>
        {
            unreachable!("the processor captures the scheduler")
        }

        fn scan_user_limit_counters(
            &self,
            _: PartitionId,
            _: KeyRange,
        ) -> restate_worker_api::PartitionQueryStream<restate_worker_api::UserLimitCounterEntry>
        {
            unreachable!("the processor captures the user limits")
        }
    }

    use super::*;

    /// A request that waits for its processor's turn, say behind a commit, shows
    /// that wait as its time to admission, apart from its snapshot and from the
    /// invoker read that follows the processor's answer. The clock moves only
    /// when the test moves it.
    #[restate_core::test]
    async fn capture_timing_separates_the_wait_for_a_turn_from_the_snapshot() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        let (ppm_tx, mut ppm_rx) = mpsc::channel(1);
        let invoker_done = Arc::new(tokio::sync::Notify::new());
        let handle = ProcessorsManagerHandle::new(
            ppm_tx,
            Arc::new(RuleBookCacheHandle::detached()),
            Arc::new(SlowInvoker(Arc::clone(&invoker_done))),
        );
        let (_query_tx, query_rx) = mpsc::channel(1);
        let (ready_tx, _ready_rx) = watch::channel(QueryReadiness::Initializing);
        let mut reads = QueryReads::new(query_rx, ready_tx);
        reads.mark_ready();
        let metadata = Arc::new(QueryMetadata {
            session_id: "timed".into(),
            query_ts: UniqueTimestamp::MIN,
            headers: Default::default(),
            redacted_sql: "SELECT * FROM sys_invocation".into(),
            planning_duration: Duration::ZERO,
        });

        tokio::time::pause();
        let requested = Instant::now();
        let mut capture = Box::pin(handle.capture_partition(
            PartitionId::MIN,
            false,
            metadata,
            PartitionCaptureRequirements {
                storage: true,
                invoker: true,
                ..Default::default()
            },
        ));
        assert!(capture.as_mut().now_or_never().is_none());
        let ProcessorsManagerCommand::ReadPartition(request) = ppm_rx.recv().await.unwrap() else {
            panic!("expected admission")
        };
        // The processor's turn, a command batch's commit say, holds the request.
        tokio::time::advance(Duration::from_millis(40)).await;
        reads.admit(&store, Leading::Yes, None, request, |_| {
            panic!("the processor owns none of the requested state")
        });
        // With the answer, the handle reads the invoker's rows, which take 7ms.
        assert!(capture.as_mut().now_or_never().is_none());
        tokio::time::advance(Duration::from_millis(7)).await;
        invoker_done.notify_one();
        let read = capture.await.unwrap();
        let timing = read.timing;
        assert_eq!(
            timing.admitted - requested,
            Duration::from_millis(40),
            "the_wait_for_the_turn_is_the_time_to_admission"
        );
        let snapshot = timing
            .snapshot
            .expect("a storage capture times its snapshot");
        assert_eq!(
            (snapshot.start, snapshot.duration()),
            (timing.admitted, Duration::ZERO),
            "the_snapshot_is_taken_at_admission_and_its_call_is_timed_on_its_own"
        );
        let invoker = timing.invoker.expect("an invoker capture times its read");
        assert_eq!(
            (invoker.start, invoker.duration()),
            (snapshot.end, Duration::from_millis(7)),
            "the_invoker_is_read_after_the_answer_and_timed_on_its_own"
        );
        assert!(read.live.invoker.unwrap().is_empty());
    }

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
        reads.admit(&store, Leading::Yes, None, unready, |_| {
            panic!("unready capture")
        });
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
            reads.admit(&store, Leading::No, leader, leader_only, |_| {
                panic!("follower capture")
            });
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
            reads.admit(&store, Leading::Yes, None, capture_request, |required| {
                assert!(!required.invoker && required.scheduler && required.user_limits);
                let mut captured = CapturedPartitionState::empty(required);
                captured.scheduler = Some(
                    vec![(
                        restate_types::vqueues::VQueueId::custom(0, "queue"),
                        Default::default(),
                    )]
                    .into(),
                );
                Ok(captured)
            });
            let captured = response.await.unwrap().unwrap();
            assert_eq!(captured.storage.is_some(), storage);
            let repeated = captured.clone();
            assert!(Arc::ptr_eq(
                captured.live.scheduler.as_ref().unwrap(),
                repeated.live.scheduler.as_ref().unwrap()
            ));
            assert_eq!(captured.live.scheduler.as_ref().unwrap().len(), 1);
            assert!(
                captured.live.invoker.is_none(),
                "the manager's handle reads the invoker after admission"
            );
            assert!(captured.live.user_limits.as_ref().unwrap().is_empty());
        }
        let (mut unavailable, response) = request(false);
        unavailable.capture.scheduler = true;
        reads.admit(&store, Leading::Yes, None, unavailable, |_| {
            Ok(CapturedPartitionState::default())
        });
        assert_eq!(
            response.await.unwrap().unwrap_err(),
            PartitionReadError::LiveCaptureUnavailable(PartitionId::MIN)
        );

        let (mut follower, response) = request(false);
        follower.capture.scheduler = true;
        reads.admit(&store, Leading::No, None, follower, |_| {
            panic!("live capture requires leader even without explicit flag")
        });
        assert!(matches!(
            response.await.unwrap(),
            Err(PartitionReadError::NotLeader { .. })
        ));

        // While still becoming the leader, reads that need it are told to retry.
        let (becoming, response) = request(true);
        reads.admit(&store, Leading::Becoming, None, becoming, |_| {
            panic!("becoming-leader capture")
        });
        assert_eq!(
            response.await.unwrap().unwrap_err(),
            PartitionReadError::Initializing(PartitionId::MIN)
        );
        let (mut wrong_partition, response) = request(false);
        wrong_partition.partition = PartitionId::from(2);
        reads.admit(&store, Leading::Yes, None, wrong_partition, |_| {
            panic!("wrong partition capture")
        });
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
        reads.admit(&store, Leading::No, None, forwarded, |_| {
            panic!("storage-only capture")
        });
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
        // The processor is only asked to lead; the handle reads the invoker's rows
        // after admission, through the leader handles, which have no invoker here.
        let mut capture = Box::pin(handle.capture_partition(
            PartitionId::MIN,
            false,
            Arc::clone(&metadata),
            PartitionCaptureRequirements {
                invoker: true,
                ..Default::default()
            },
        ));
        assert!(capture.as_mut().now_or_never().is_none());
        let ProcessorsManagerCommand::ReadPartition(forwarded) = ppm_rx.recv().await.unwrap()
        else {
            panic!("expected admission")
        };
        assert!(
            forwarded.require_leader && forwarded.capture.invoker,
            "the_processor_sees_the_whole_capture_and_checks_leadership"
        );
        let _ = forwarded.response.send(Ok(CapturedPartitionRead {
            partition: PartitionId::MIN,
            storage: None,
            live: Default::default(),
            timing: CaptureTiming::admitted_at(Instant::now()),
        }));
        assert_eq!(
            capture.await.unwrap_err(),
            PartitionReadError::LiveCaptureUnavailable(PartitionId::MIN)
        );

        // A capture waiting for a stalled invoker gives up its snapshot when the
        // processor cancels it, instead of holding storage open.
        let (stalled_tx, mut stalled_rx) = mpsc::channel(1);
        let stalled = ProcessorsManagerHandle::new(
            stalled_tx,
            Arc::new(RuleBookCacheHandle::detached()),
            Arc::new(StalledInvoker),
        );
        let mut capture = Box::pin(stalled.capture_partition(
            PartitionId::MIN,
            false,
            Arc::clone(&metadata),
            PartitionCaptureRequirements {
                storage: true,
                invoker: true,
                ..Default::default()
            },
        ));
        assert!(capture.as_mut().now_or_never().is_none());
        let ProcessorsManagerCommand::ReadPartition(forwarded) = stalled_rx.recv().await.unwrap()
        else {
            panic!("expected admission")
        };
        let (snapshot, closed) = store
            .query_read(tokio_util::sync::CancellationToken::new())
            .unwrap();
        let cancel = snapshot.cancellation_token();
        let _ = forwarded.response.send(Ok(CapturedPartitionRead {
            partition: PartitionId::MIN,
            storage: Some(snapshot),
            live: Default::default(),
            timing: CaptureTiming::admitted_at(Instant::now()),
        }));
        assert!(
            capture.as_mut().now_or_never().is_none(),
            "waits_for_the_invoker"
        );
        cancel.cancel();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), capture)
                .await
                .expect("the_cancelled_capture_stops_waiting")
                .unwrap_err(),
            PartitionReadError::Stopping(PartitionId::MIN)
        );
        tokio::time::timeout(Duration::from_secs(5), closed)
            .await
            .expect("the_cancelled_capture_releases_its_snapshot");

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
            reads.admit(store, Leading::Yes, None, request, |_| {
                panic!("storage-only capture")
            });
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
        config.common.experimental.set_indexes(true);
        // A partition fully migrated to vqueues, which the indexes need.
        store.mark_fully_migrated_to_vqueues().await;
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
