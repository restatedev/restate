// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeSet, HashMap};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::util::display::array_value_to_string;
use futures::{FutureExt, StreamExt, TryStreamExt};
use tokio::sync::mpsc;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

use restate_clock::UniqueTimestamp;
use restate_core::test_env::TestCoreEnv;
use restate_partition_store::PartitionStoreManager;
use restate_partition_store::keys::{EncodeTableKey, KeyKind};
use restate_partition_store::state_table::{ScopedStateKey, StateKey};
use restate_platform::sync::Mutex;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_query_api::{QueryMetadata, QueryOptions, SessionOptions};
use restate_types::errors::GenericError;
use restate_types::partition_table::{Partition, PartitionTable};
use restate_types::{Version, identifiers::PartitionId};
use restate_util_string::ReString;
use restate_worker_api::{ProcessorsManagerCommand, ProcessorsManagerHandle};

use crate::context::{DataFusionQueryEngine, SelectPartitions};
use crate::source_registry::SourceRegistry;
use crate::table_providers::{PartitionReadContext, PreparedPartitionRead, ScanPartition};
use crate::tables::state::{LegacyStateSource, ScopedStateSource, StateTable};

use super::super::tests::{UnusedRuleBook, environment};

#[derive(Debug)]
struct CaptureSource {
    inner: Arc<dyn ScanPartition>,
    requirements: restate_worker_api::PartitionCaptureRequirements,
    gate: Option<Arc<Gate>>,
}

/// Parks the `park_at`-th admission's requirements call, which a lane makes
/// after checking that its consumer is still there and before it asks for a
/// snapshot, until the test releases it, drops the release's sender, or 10 s
/// pass. It traces which.
#[derive(Debug)]
struct Gate {
    calls: AtomicUsize,
    park_at: usize,
    parked: mpsc::UnboundedSender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    trace: Arc<Trace>,
}

impl Gate {
    fn pass(&self) {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call == self.park_at {
            self.trace.push(Event::Parked);
            let _ = self.parked.send(());
            let unparked = match self
                .release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
            {
                Ok(()) => Unparked::Released,
                Err(RecvTimeoutError::Timeout) => Unparked::TimedOut,
                Err(RecvTimeoutError::Disconnected) => Unparked::Disconnected,
            };
            self.trace.push(Event::Unparked(unparked));
        }
    }
}

/// Why the gate let its parked call go.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Unparked {
    Released,
    TimedOut,
    Disconnected,
}

/// An execution: its session and its full query timestamp, whose logical part
/// tells apart executions at the same wall time, mocked or not.
type Execution = (ReString, UniqueTimestamp);

fn execution(metadata: &QueryMetadata) -> Execution {
    (metadata.session_id.clone(), metadata.query_ts)
}

/// What the gate and the dispatcher saw, in order: at most 400 events.
#[derive(Debug, Default)]
struct Trace(Mutex<Vec<Event>>);

impl Trace {
    fn push(&self, event: Event) {
        let mut events = self.0.lock();
        if events.len() < 400 {
            events.push(event);
        }
    }

    fn events(&self) -> Vec<Event> {
        self.0.lock().clone()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Event {
    /// The gate parked a requirements call.
    Parked,
    /// The gate let it go.
    Unparked(Unparked),
    /// The dispatcher answered a barrier, after every read queued before it.
    Barrier,
    /// The dispatcher received a read.
    Received(Execution, PartitionId),
    /// It took the read's snapshot: `live` of its execution's snapshots, and
    /// `total` in all, were then open.
    Admitted {
        execution: Execution,
        partition: PartitionId,
        live: usize,
        total: usize,
    },
    /// Whether `oneshot::Sender::send` of its reply succeeded. It fails when
    /// the lane has dropped its receiver; success doesn't mean the lane used
    /// the snapshot.
    Replied {
        execution: Execution,
        partition: PartitionId,
        sent: bool,
    },
    /// A snapshot the dispatcher found closed, at a later admission or when
    /// it stopped.
    Closed(Execution, PartitionId),
}

/// The most reads a test's dispatcher admits: past it, it stops and fails the
/// test, keeping what it recorded.
const ADMISSIONS: usize = 256;

/// The longest a test's final accounting may take, in all.
const FINAL: Duration = Duration::from_secs(30);

/// Waits for `future` until `deadline`, and at most 5 s.
async fn within<F: std::future::Future>(
    deadline: tokio::time::Instant,
    future: F,
    what: &str,
) -> F::Output {
    let by = deadline.min(tokio::time::Instant::now() + Duration::from_secs(5));
    tokio::time::timeout_at(by, future)
        .await
        .unwrap_or_else(|_| panic!("{what}"))
}

/// A snapshot the dispatcher took, until it closes.
type Open = (
    Execution,
    PartitionId,
    Pin<Box<WaitForCancellationFutureOwned>>,
);

impl ScanPartition for CaptureSource {
    fn supports_primary_lookup(&self) -> bool {
        self.inner.supports_primary_lookup()
    }

    fn prepare_read(
        self: Arc<Self>,
        access: crate::access::PrimaryRead,
        projection: datafusion::arrow::datatypes::SchemaRef,
        predicate: Option<Arc<dyn datafusion::physical_plan::PhysicalExpr>>,
        limit: Option<usize>,
    ) -> anyhow::Result<Arc<dyn PreparedPartitionRead>> {
        Ok(Arc::new(CaptureRead {
            inner: Arc::clone(&self.inner).prepare_read(access, projection, predicate, limit)?,
            requirements: self.requirements,
            gate: self.gate.clone(),
        }))
    }
}

#[derive(Debug)]
struct CaptureRead {
    inner: Arc<dyn PreparedPartitionRead>,
    requirements: restate_worker_api::PartitionCaptureRequirements,
    gate: Option<Arc<Gate>>,
}

impl PreparedPartitionRead for CaptureRead {
    fn capture_requirements(&self) -> restate_worker_api::PartitionCaptureRequirements {
        if let Some(gate) = &self.gate {
            gate.pass();
        }
        self.requirements
    }

    fn execute(
        &self,
        context: PartitionReadContext,
        batch_size: usize,
        metrics: crate::table_providers::ReadMetrics,
    ) -> anyhow::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        // Both layouts receive the combined capture, even though each requested
        // a different live source. Neither requests user-limit counters.
        assert!(context.captured().invoker.is_some());
        assert!(context.captured().scheduler.is_some());
        assert!(context.captured().user_limits.is_none());
        let repeated = context.clone();
        assert!(Arc::ptr_eq(
            context.captured().scheduler.as_ref().unwrap(),
            repeated.captured().scheduler.as_ref().unwrap()
        ));
        self.inner.execute(context, batch_size, metrics)
    }
}

#[derive(Clone, Debug)]
struct Partitions(Vec<(PartitionId, Partition)>);

#[async_trait]
impl SelectPartitions for Partitions {
    async fn get_live_partitions(&self) -> Result<Vec<(PartitionId, Partition)>, GenericError> {
        Ok(self.0.clone())
    }
}

struct Admission {
    partition: PartitionId,
    metadata: Arc<QueryMetadata>,
    cancel: CancellationToken,
    closed: std::pin::Pin<Box<WaitForCancellationFutureOwned>>,
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_scopes_bound_open_reads_share_layouts_and_cancel_idle_consumers() {
    RocksDbManager::init();
    let core = TestCoreEnv::create_with_single_node(1, 1).await;
    let stores = PartitionStoreManager::create(true).await.unwrap();
    let table = PartitionTable::with_equally_sized_partitions(Version::MIN, 24);
    let partitions = Partitions(
        table
            .iter()
            .map(|(id, partition)| (*id, partition.clone()))
            .collect(),
    );
    for (id, partition) in &partitions.0 {
        let mut store = stores.open(partition, None).await.unwrap();
        let mut tx = store.transaction();
        // Two dense leading partitions; otherwise empty until the final sparse match.
        let count = if u16::from(*id) < 2 {
            100
        } else {
            usize::from(u16::from(*id) == 23)
        };
        for n in 0..count {
            let mut key = Vec::new();
            if n % 2 == 0 {
                StateKey {
                    partition_key: partition.key_range.start(),
                    service_name: "svc".into(),
                    service_key: "object".into(),
                    state_key: Bytes::from(n.to_string()),
                }
                .serialize_to(&mut key);
                tx.raw_put_cf(
                    KeyKind::State,
                    key,
                    if u16::from(*id) == 23 {
                        b"wanted".as_slice()
                    } else {
                        b"other".as_slice()
                    },
                );
            } else {
                ScopedStateKey {
                    partition_key: partition.key_range.start(),
                    scope: None,
                    service_name: "svc".into(),
                    service_key: "object".into(),
                    state_key: Bytes::from(n.to_string()),
                }
                .serialize_to(&mut key);
                tx.raw_put_cf(KeyKind::ScopedState, key, b"other");
            }
        }
        tx.commit().await.unwrap();
    }
    let (commands, mut receiver) = mpsc::channel(32);
    let processors = ProcessorsManagerHandle::new(
        commands,
        Arc::new(UnusedRuleBook),
        crate::distributed::tests::TestInvokers::empty(),
    );
    let sources = SourceRegistry::local_only(core.metadata);
    sources.register_processor_reads(processors.clone(), crate::mocks::query_io());
    sources.register_declared_scanner::<LegacyStateSource>(Arc::new(CaptureSource {
        inner: Arc::new(crate::schema_scan::LookupScanner::<
            restate_partition_store::state_table::StateBinding,
        >::new()),
        requirements: restate_worker_api::PartitionCaptureRequirements {
            storage: true,
            invoker: true,
            ..Default::default()
        },
        gate: None,
    }));
    sources.register_declared_scanner::<ScopedStateSource>(Arc::new(CaptureSource {
        inner: Arc::new(crate::schema_scan::LookupScanner::<
            restate_partition_store::state_table::ScopedStateBinding,
        >::new()),
        requirements: restate_worker_api::PartitionCaptureRequirements {
            storage: true,
            scheduler: true,
            ..Default::default()
        },
        gate: None,
    }));
    let admissions = Arc::new(Mutex::new(Vec::<Admission>::new()));
    // The invariant: the most snapshots of one execution live at once, by
    // execution, at every admission, late ones included.
    let per_execution = Arc::new(Mutex::new(HashMap::<Execution, usize>::new()));
    // Diagnostics: the most snapshots live at once across executions, a
    // bounded trace of what the dispatcher saw, and the live snapshots, by
    // query and partition, whenever more than two were live.
    let peak = Arc::new(Mutex::new(0usize));
    let trace = Arc::new(Mutex::new(Vec::<String>::new()));
    let exceeded = Arc::new(Mutex::new(Vec::<String>::new()));
    let (opened, mut opened_rx) = mpsc::unbounded_channel::<Execution>();
    let stop = CancellationToken::new();
    let dispatch = tokio::spawn({
        let admissions = Arc::clone(&admissions);
        let per_execution = Arc::clone(&per_execution);
        let peak = Arc::clone(&peak);
        let trace = Arc::clone(&trace);
        let exceeded = Arc::clone(&exceeded);
        let stop = stop.clone();
        async move {
            let started = std::time::Instant::now();
            let event = move |text: String| {
                let mut trace = trace.lock();
                if trace.len() < 400 {
                    trace.push(format!("{:>8}us {text}", started.elapsed().as_micros()));
                }
            };
            let mut active = Vec::new();
            let mut admitted_reads = 0;
            loop {
                let command = tokio::select! {
                    biased;
                    () = stop.cancelled() => None,
                    command = receiver.recv() => command,
                };
                let request = match command {
                    None => break Ok(()),
                    Some(ProcessorsManagerCommand::ReadPartition(request)) => request,
                    // A barrier: it is answered after every read queued before it.
                    Some(ProcessorsManagerCommand::GetState(reply)) => {
                        event("barrier".to_owned());
                        let _ = reply.send(Default::default());
                        continue;
                    }
                };
                admitted_reads += 1;
                if admitted_reads > ADMISSIONS {
                    break Err(format!("more than {ADMISSIONS} admissions"));
                }
                let identity = format!(
                    "p{} ts={:?} sql={:?}",
                    u16::from(request.partition),
                    request.metadata.query_ts,
                    request.metadata.redacted_sql
                );
                event(format!("admit {identity}"));
                let store = stores.get_partition_store(request.partition).await.unwrap();
                assert!(
                    request.capture.storage && request.capture.invoker && request.capture.scheduler
                );
                assert!(!request.capture.user_limits);
                assert!(request.require_leader);
                let cancel = CancellationToken::new();
                let (read, closed) = store.query_read(cancel.clone()).unwrap();
                active.retain_mut(
                    |(_, live, completion): &mut (
                        Execution,
                        String,
                        Pin<Box<WaitForCancellationFutureOwned>>,
                    )| {
                        let open = completion.as_mut().now_or_never().is_none();
                        if !open {
                            event(format!("closed {live}"));
                        }
                        open
                    },
                );
                let admitted = execution(&request.metadata);
                active.push((admitted.clone(), identity.clone(), Box::pin(read.closed())));
                let live = active.iter().filter(|(of, ..)| *of == admitted).count();
                {
                    let mut per_execution = per_execution.lock();
                    let peak = per_execution.entry(admitted.clone()).or_default();
                    *peak = (*peak).max(live);
                }
                {
                    let mut peak = peak.lock();
                    *peak = (*peak).max(active.len());
                }
                if active.len() > 2 {
                    exceeded.lock().push(format!(
                        "{} live at the admission of {identity}: {:#?}",
                        active.len(),
                        active.iter().map(|(_, live, _)| live).collect::<Vec<_>>()
                    ));
                }
                admissions.lock().push(Admission {
                    partition: request.partition,
                    metadata: request.metadata,
                    cancel,
                    closed: Box::pin(closed),
                });
                let _ = request
                    .response
                    .send(Ok(restate_worker_api::CapturedPartitionRead {
                        partition: request.partition,
                        storage: Some(read),
                        live: restate_worker_api::CapturedPartitionState::empty(request.capture),
                    }));
                // Told only once its admission is recorded and answered.
                let _ = opened.send(admitted);
            }
        }
    });
    let env = environment(2, 2);
    env.register_table::<StateTable>(
        "public",
        "state",
        StateTable::create_provider(partitions, &sources),
    )
    .unwrap();
    let engine = DataFusionQueryEngine::from_env(env, None);
    let mut options = SessionOptions::default();
    options
        .headers
        .append("x-test-context", "first".parse().unwrap());
    options
        .headers
        .append("x-test-context", "second".parse().unwrap());
    let session = engine.create_session(options).unwrap();
    // Every execution has its own identity, though the session is the same
    // and the clock may be mocked.
    let mut executions = BTreeSet::new();
    let bounded = |after: &str| {
        let per_execution = per_execution.lock().clone();
        assert!(
            per_execution.values().all(|&live| live <= 2),
            "parallelism_bounds_actual_snapshot_lifetimes_per_execution: after {after}: \
             {per_execution:#?}\nglobal peak (diagnostic): {}\nexceeded: {:#?}\ntrace:\n{}",
            *peak.lock(),
            exceeded.lock(),
            trace.lock().join("\n")
        );
    };
    #[rustfmt::skip]
    let cases = [
        ("SELECT * FROM state LIMIT 10", 10, 2),
        ("SELECT * FROM state LIMIT 0", 0, 0),
        ("SELECT * FROM state WHERE value_utf8 = 'wanted' LIMIT 1", 1, 24),
        ("SELECT partition_key FROM state ORDER BY partition_key DESC LIMIT 1 OFFSET 1", 1, 24),
        ("SELECT COUNT(*) FROM state", 1, 24),
    ];
    for (sql, expected_rows, max_scopes) in cases {
        let result = session.execute(sql, QueryOptions {}).await.unwrap();
        let metadata = result.metadata;
        let current = execution(&metadata);
        assert!(
            executions.insert(current.clone()),
            "every_execution_has_its_own_identity: {sql}"
        );
        let batches = tokio::time::timeout(
            Duration::from_secs(10),
            result.stream.try_collect::<Vec<_>>(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            expected_rows,
            "sql_cardinality: {sql}"
        );
        if sql.contains("COUNT(*)") {
            assert_eq!(
                array_value_to_string(batches[0].column(0), 0).unwrap(),
                "201",
                "both_layouts_and_every_partition_are_covered"
            );
        }
        // A query that stops early can leave reads queued at the processors; the
        // barrier queues behind them, so all of this query's reads are counted.
        // A late read of an earlier execution is released here, not counted
        // again.
        tokio::time::timeout(Duration::from_secs(10), processors.get_state())
            .await
            .unwrap()
            .unwrap();
        let (admitted, late): (Vec<_>, Vec<_>) = std::mem::take(&mut *admissions.lock())
            .into_iter()
            .partition(|read| execution(&read.metadata) == current);
        for read in late {
            assert!(
                executions.contains(&execution(&read.metadata)),
                "late_reads_belong_to_earlier_executions: {sql}"
            );
            tokio::time::timeout(Duration::from_secs(5), read.closed)
                .await
                .unwrap();
        }
        assert!(
            admitted.len() <= max_scopes,
            "limit_avoids_opening_later_partitions: {sql}: {}",
            admitted.len()
        );
        if max_scopes == 24 && !sql.contains("WHERE") {
            assert_eq!(admitted.len(), 24, "full_scan_visits_all_partitions_once");
        }
        if sql.contains("WHERE") {
            assert!(
                admitted.iter().any(|read| u16::from(read.partition) == 23),
                "residual_rejection_does_not_exhaust_limit"
            );
        }
        let unique: std::collections::BTreeSet<_> =
            admitted.iter().map(|read| read.partition).collect();
        assert_eq!(
            unique.len(),
            admitted.len(),
            "physical_layouts_share_one_admission"
        );
        for read in admitted {
            assert_eq!(read.metadata.session_id, metadata.session_id);
            assert_eq!(read.metadata.query_ts, metadata.query_ts);
            assert_eq!(read.metadata.headers, metadata.headers);
            assert_eq!(read.metadata.redacted_sql, metadata.redacted_sql);
            assert_eq!(read.metadata.planning_duration, metadata.planning_duration);
            tokio::time::timeout(Duration::from_secs(5), read.closed)
                .await
                .unwrap();
        }
        bounded(sql);
        while opened_rx.try_recv().is_ok() {}
    }
    // Keep the result alive but stop polling it. Processor cancellation must
    // release all snapshots without relying on consumer progress or stream drop.
    let mut result = session
        .execute("SELECT * FROM state", QueryOptions {})
        .await
        .unwrap();
    let idle = execution(&result.metadata);
    assert!(
        executions.insert(idle.clone()),
        "every_execution_has_its_own_identity: idle consumer"
    );
    tokio::time::timeout(Duration::from_secs(10), result.stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    // Its two reads, told apart by execution from any late one; the wait is
    // bounded as a whole.
    let by = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut seen = 0;
    while seen < 2 {
        let of = tokio::time::timeout_at(by, opened_rx.recv())
            .await
            .expect("the idle consumer's two reads are admitted")
            .unwrap();
        seen += usize::from(of == idle);
    }
    let (admitted, late): (Vec<_>, Vec<_>) = std::mem::take(&mut *admissions.lock())
        .into_iter()
        .partition(|read| execution(&read.metadata) == idle);
    assert_eq!(admitted.len(), 2);
    for read in late {
        assert!(executions.contains(&execution(&read.metadata)));
        tokio::time::timeout(Duration::from_secs(5), read.closed)
            .await
            .unwrap();
    }
    for read in &admitted {
        read.cancel.cancel();
    }
    for read in admitted {
        tokio::time::timeout(Duration::from_secs(5), read.closed)
            .await
            .expect("idle_consumer_must_not_block_processor_shutdown");
    }
    let error = tokio::time::timeout(
        Duration::from_secs(10),
        result.stream.try_collect::<Vec<_>>(),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(
        error.to_string().contains("cancelled"),
        "cancellation_is_terminal: {error}"
    );
    bounded("the idle consumer");
    // A worker reports it as a cancelled task failure, through the wire format.
    let reply = crate::distributed::worker::task_reply(
        Err(error),
        restate_types::GenerationalNodeId::new(2, 1),
    );
    let bytes = restate_types::net::codec::WireEncode::encode_to_bytes(
        &reply,
        restate_types::net::ProtocolVersion::V7,
    )
    .unwrap();
    let failure = <restate_types::net::distributed_query::QueryTaskReply as restate_types::net::codec::WireDecode>::try_decode(
        bytes,
        restate_types::net::ProtocolVersion::V7,
    )
    .unwrap()
    .into_result()
    .unwrap_err();
    assert_eq!(
        failure.kind,
        restate_types::net::distributed_query::QueryFailureKind::CANCELLED,
        "cancellation_is_reported_as_such: {failure}"
    );
    // Final accounting, once nothing more is admitted: a barrier, then the
    // dispatcher stops and is awaited, every snapshot it took since the last
    // check closes, and the per-execution bound held to the end.
    let deadline = tokio::time::Instant::now() + FINAL;
    within(deadline, processors.get_state(), "the final barrier")
        .await
        .unwrap();
    stop.cancel();
    let stopped = within(deadline, dispatch, "the dispatcher stops").await;
    assert_eq!(stopped.unwrap(), Ok(()));
    let remaining = std::mem::take(&mut *admissions.lock());
    for read in remaining {
        within(deadline, read.closed, "every snapshot closes").await;
    }
    bounded("the end");
}

/// A lane can pass its check that the consumer is still there, then ask for
/// its snapshot only after the query has ended: here it's held between the two
/// while another lane satisfies the query's LIMIT, the test's barrier passes,
/// and the next query holds its two snapshots. Its late snapshot then overlaps
/// them: per query, at most two snapshots are live, but across queries three
/// can be, briefly, until the abandoned lane's cancellation drops it.
#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_abandoned_lane_can_admit_after_its_query_ended() {
    RocksDbManager::init();
    let core = TestCoreEnv::create_with_single_node(1, 1).await;
    let stores = PartitionStoreManager::create(true).await.unwrap();
    let table = PartitionTable::with_equally_sized_partitions(Version::MIN, 4);
    let partitions = Partitions(
        table
            .iter()
            .map(|(id, partition)| (*id, partition.clone()))
            .collect(),
    );
    for (id, partition) in &partitions.0 {
        let mut store = stores.open(partition, None).await.unwrap();
        let mut tx = store.transaction();
        for n in 0..100 {
            let mut key = Vec::new();
            StateKey {
                partition_key: partition.key_range.start(),
                service_name: "svc".into(),
                service_key: "object".into(),
                state_key: Bytes::from(format!("{}-{n}", u16::from(*id))),
            }
            .serialize_to(&mut key);
            tx.raw_put_cf(KeyKind::State, key, b"value".as_slice());
        }
        tx.commit().await.unwrap();
    }
    let (commands, mut receiver) = mpsc::channel(32);
    let processors = ProcessorsManagerHandle::new(
        commands,
        Arc::new(UnusedRuleBook),
        crate::distributed::tests::TestInvokers::empty(),
    );
    let trace = Arc::new(Trace::default());
    let (parked, mut parked_rx) = mpsc::unbounded_channel();
    // Dropped on any failure, the sender unblocks a parked gate at once.
    let (release, released) = std::sync::mpsc::channel();
    // Each admission asks both layouts for their requirements: the third call
    // is the second admission's.
    let gate = Arc::new(Gate {
        calls: Default::default(),
        park_at: 3,
        parked,
        release: std::sync::Mutex::new(released),
        trace: Arc::clone(&trace),
    });
    let sources = SourceRegistry::local_only(core.metadata);
    sources.register_processor_reads(processors.clone(), crate::mocks::query_io());
    let requirements = |invoker, scheduler| restate_worker_api::PartitionCaptureRequirements {
        storage: true,
        invoker,
        scheduler,
        ..Default::default()
    };
    sources.register_declared_scanner::<LegacyStateSource>(Arc::new(CaptureSource {
        inner: Arc::new(crate::schema_scan::LookupScanner::<
            restate_partition_store::state_table::StateBinding,
        >::new()),
        requirements: requirements(true, false),
        gate: Some(Arc::clone(&gate)),
    }));
    sources.register_declared_scanner::<ScopedStateSource>(Arc::new(CaptureSource {
        inner: Arc::new(crate::schema_scan::LookupScanner::<
            restate_partition_store::state_table::ScopedStateBinding,
        >::new()),
        requirements: requirements(false, true),
        gate: Some(Arc::clone(&gate)),
    }));
    // The dispatcher: every snapshot it takes, kept until the test sees it
    // close, and what was live whenever more than two were. Stopped, it
    // returns the snapshots it still saw open.
    let exceeded = Arc::new(Mutex::new(
        Vec::<(Execution, Vec<(Execution, PartitionId)>)>::new(),
    ));
    // The invariant, apart from the bounded trace: the most snapshots of one
    // execution live at once, at every admission.
    let per_execution = Arc::new(Mutex::new(HashMap::<Execution, usize>::new()));
    let closures = Arc::new(Mutex::new(Vec::<Open>::new()));
    let (opened, mut opened_rx) = mpsc::unbounded_channel::<(Execution, PartitionId)>();
    let stop = CancellationToken::new();
    let dispatch = tokio::spawn({
        let trace = Arc::clone(&trace);
        let exceeded = Arc::clone(&exceeded);
        let per_execution = Arc::clone(&per_execution);
        let closures = Arc::clone(&closures);
        let stop = stop.clone();
        async move {
            let prune = |active: &mut Vec<Open>| {
                active.retain_mut(|(execution, partition, completion)| {
                    let open = completion.as_mut().now_or_never().is_none();
                    if !open {
                        trace.push(Event::Closed(execution.clone(), *partition));
                    }
                    open
                });
            };
            let mut active = Vec::new();
            let mut admitted_reads = 0;
            let outcome = loop {
                let command = tokio::select! {
                    biased;
                    () = stop.cancelled() => None,
                    command = receiver.recv() => command,
                };
                let request = match command {
                    None => break Ok(()),
                    Some(ProcessorsManagerCommand::ReadPartition(request)) => request,
                    Some(ProcessorsManagerCommand::GetState(reply)) => {
                        trace.push(Event::Barrier);
                        let _ = reply.send(Default::default());
                        continue;
                    }
                };
                let (admitted, partition) = (execution(&request.metadata), request.partition);
                trace.push(Event::Received(admitted.clone(), partition));
                admitted_reads += 1;
                if admitted_reads > ADMISSIONS {
                    break Err(format!("more than {ADMISSIONS} admissions"));
                }
                let store = tokio::select! {
                    biased;
                    () = stop.cancelled() => break Ok(()),
                    store = stores.get_partition_store(partition) => store.unwrap(),
                };
                let (read, closed) = store.query_read(CancellationToken::new()).unwrap();
                closures
                    .lock()
                    .push((admitted.clone(), partition, Box::pin(closed)));
                prune(&mut active);
                active.push((admitted.clone(), partition, Box::pin(read.closed())));
                let live = active.iter().filter(|(of, ..)| *of == admitted).count();
                {
                    let mut per_execution = per_execution.lock();
                    let peak = per_execution.entry(admitted.clone()).or_default();
                    *peak = (*peak).max(live);
                }
                trace.push(Event::Admitted {
                    execution: admitted.clone(),
                    partition,
                    live,
                    total: active.len(),
                });
                if active.len() > 2 {
                    exceeded.lock().push((
                        admitted.clone(),
                        active
                            .iter()
                            .map(|(execution, partition, _)| (execution.clone(), *partition))
                            .collect(),
                    ));
                }
                let sent = request
                    .response
                    .send(Ok(restate_worker_api::CapturedPartitionRead {
                        partition,
                        storage: Some(read),
                        live: restate_worker_api::CapturedPartitionState::empty(request.capture),
                    }))
                    .is_ok();
                trace.push(Event::Replied {
                    execution: admitted.clone(),
                    partition,
                    sent,
                });
                let _ = opened.send((admitted, partition));
            };
            prune(&mut active);
            (outcome, active)
        }
    });
    let env = environment(2, 2);
    env.register_table::<StateTable>(
        "public",
        "state",
        StateTable::create_provider(partitions, &sources),
    )
    .unwrap();
    let engine = DataFusionQueryEngine::from_env(env, None);
    let session = engine.create_session(SessionOptions::default()).unwrap();

    // The first query's LIMIT is met by one lane; the other is held at its
    // second admission's requirements, after its consumer check.
    let result = session
        .execute("SELECT * FROM state LIMIT 10", QueryOptions {})
        .await
        .unwrap();
    let ended = execution(&result.metadata);
    let rows: usize = tokio::time::timeout(
        Duration::from_secs(10),
        result.stream.try_collect::<Vec<_>>(),
    )
    .await
    .unwrap()
    .unwrap()
    .iter()
    .map(|batch| batch.num_rows())
    .sum();
    assert_eq!(rows, 10);
    tokio::time::timeout(Duration::from_secs(10), parked_rx.recv())
        .await
        .expect("a lane is held within 10 s")
        .expect("a lane is held between its check and its admission");
    tokio::time::timeout(Duration::from_secs(10), processors.get_state())
        .await
        .unwrap()
        .unwrap();

    // The next query holds two snapshots: its consumer doesn't pull.
    let result = session
        .execute("SELECT * FROM state", QueryOptions {})
        .await
        .unwrap();
    let next = execution(&result.metadata);
    assert_ne!(ended, next, "every execution has its own identity");
    let idle = result.stream;
    let by = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut seen = 0;
    while seen < 2 {
        let (of, _) = tokio::time::timeout_at(by, opened_rx.recv())
            .await
            .expect("the next execution's two reads are admitted")
            .unwrap();
        seen += usize::from(of == next);
    }

    // Released, the abandoned lane admits its snapshot while those two are live.
    release.send(()).unwrap();
    let (late, partition) = tokio::time::timeout(Duration::from_secs(10), opened_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let events = trace.events();
    let exceeded = exceeded.lock().clone();
    eprintln!("late admission: {late:?} p{partition}\nexceeded: {exceeded:#?}\ntrace: {events:#?}");
    assert_eq!(late, ended, "the late admission is the ended execution's");
    assert!(
        events.contains(&Event::Unparked(Unparked::Released)),
        "released, not timed out or disconnected: {events:#?}"
    );
    let [(at, live)] = exceeded.as_slice() else {
        panic!("one overlap: {exceeded:#?}\n{events:#?}")
    };
    assert_eq!(at, &ended);
    assert_eq!(live.len(), 3, "{live:?}");
    assert_eq!(
        live.iter().filter(|(of, _)| *of == next).count(),
        2,
        "{live:?}"
    );
    // Per execution, at most two at every admission.
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, Event::Admitted { live, .. } if *live > 2)),
        "{events:#?}"
    );
    // The ended execution's first admission, the barrier, then the parked
    // lane's, whose reply couldn't be sent: the lane had dropped its receiver.
    let barrier = events.iter().position(|event| *event == Event::Barrier);
    let first = events.iter().position(
        |event| matches!(event, Event::Admitted { execution, .. } if *execution == ended),
    );
    let parked = events.iter().rposition(|event| {
        matches!(event, Event::Admitted { execution, partition: at, .. }
            if *execution == ended && *at == partition)
    });
    let (Some(first), Some(barrier), Some(parked)) = (first, barrier, parked) else {
        panic!("{events:#?}")
    };
    assert!(first < barrier && barrier < parked, "{events:#?}");
    let reply = events[parked..].iter().find_map(|event| match event {
        Event::Replied {
            execution,
            partition: at,
            sent,
        } if *execution == ended && *at == partition => Some(*sent),
        _ => None,
    });
    assert_eq!(reply, Some(false), "{events:#?}");

    // Cleanup, within one deadline: with the idle output dropped, every
    // snapshot taken closes, the rejected one and the next execution's
    // included. Stopped, the dispatcher takes no more, and whatever it took
    // since, or still saw open, closes too. The per-execution bound held at
    // every admission, to the end.
    drop(idle);
    let deadline = tokio::time::Instant::now() + FINAL;
    within(deadline, processors.get_state(), "the final barrier")
        .await
        .unwrap();
    let taken = std::mem::take(&mut *closures.lock());
    assert!(
        taken
            .iter()
            .any(|(of, at, _)| *of == ended && *at == partition)
    );
    assert!(taken.iter().filter(|(of, ..)| *of == next).count() >= 2);
    stop.cancel();
    let (outcome, open) = within(deadline, dispatch, "the dispatcher stops")
        .await
        .unwrap();
    assert_eq!(outcome, Ok(()));
    let rest = std::mem::take(&mut *closures.lock());
    for (of, at, closed) in taken.into_iter().chain(open).chain(rest) {
        within(
            deadline,
            closed,
            &format!("the snapshot of {of:?} on {at} closes"),
        )
        .await;
    }
    let per_execution = per_execution.lock().clone();
    assert!(
        per_execution.values().all(|&live| live <= 2),
        "{per_execution:#?}"
    );
}
