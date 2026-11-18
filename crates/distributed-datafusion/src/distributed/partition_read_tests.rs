// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::util::display::array_value_to_string;
use futures::{FutureExt, StreamExt, TryStreamExt};
use tokio::sync::mpsc;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

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
}

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
        }))
    }
}

#[derive(Debug)]
struct CaptureRead {
    inner: Arc<dyn PreparedPartitionRead>,
    requirements: restate_worker_api::PartitionCaptureRequirements,
}

impl PreparedPartitionRead for CaptureRead {
    fn capture_requirements(&self) -> restate_worker_api::PartitionCaptureRequirements {
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
    }));
    let admissions = Arc::new(Mutex::new(Vec::<Admission>::new()));
    let peak = Arc::new(Mutex::new(0usize));
    // Diagnostics: a bounded trace of what the dispatcher saw, and the live
    // snapshots, by query and partition, whenever more than two were live.
    let trace = Arc::new(Mutex::new(Vec::<String>::new()));
    let exceeded = Arc::new(Mutex::new(Vec::<String>::new()));
    let (opened, mut opened_rx) = mpsc::unbounded_channel();
    let dispatch = tokio::spawn({
        let admissions = Arc::clone(&admissions);
        let peak = Arc::clone(&peak);
        let trace = Arc::clone(&trace);
        let exceeded = Arc::clone(&exceeded);
        async move {
            let started = std::time::Instant::now();
            let event = move |text: String| {
                let mut trace = trace.lock();
                if trace.len() < 400 {
                    trace.push(format!("{:>8}us {text}", started.elapsed().as_micros()));
                }
            };
            let mut active = Vec::new();
            while let Some(command) = receiver.recv().await {
                let request = match command {
                    ProcessorsManagerCommand::ReadPartition(request) => request,
                    // A barrier: it is answered after every read queued before it.
                    ProcessorsManagerCommand::GetState(reply) => {
                        event("barrier".to_owned());
                        let _ = reply.send(Default::default());
                        continue;
                    }
                };
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
                    |(live, completion): &mut (
                        String,
                        std::pin::Pin<Box<WaitForCancellationFutureOwned>>,
                    )| {
                        let open = completion.as_mut().now_or_never().is_none();
                        if !open {
                            event(format!("closed {live}"));
                        }
                        open
                    },
                );
                active.push((identity.clone(), Box::pin(read.closed())));
                {
                    let mut peak = peak.lock();
                    *peak = (*peak).max(active.len());
                }
                if active.len() > 2 {
                    exceeded.lock().push(format!(
                        "{} live at the admission of {identity}: {:#?}",
                        active.len(),
                        active.iter().map(|(live, _)| live).collect::<Vec<_>>()
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
                opened.send(()).unwrap();
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
        // A late read of an earlier query is released here, not counted again.
        processors.get_state().await.unwrap();
        let (admitted, late): (Vec<_>, Vec<_>) = std::mem::take(&mut *admissions.lock())
            .into_iter()
            .partition(|read| read.metadata.redacted_sql == metadata.redacted_sql);
        for read in late {
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
        assert!(
            *peak.lock() <= 2,
            "parallelism_bounds_actual_snapshot_lifetimes: peak {} after {sql}\nexceeded: {:#?}\ntrace:\n{}",
            *peak.lock(),
            exceeded.lock(),
            trace.lock().join("\n")
        );
        while opened_rx.try_recv().is_ok() {}
    }
    // Keep the result alive but stop polling it. Processor cancellation must
    // release all snapshots without relying on consumer progress or stream drop.
    let mut result = session
        .execute("SELECT * FROM state", QueryOptions {})
        .await
        .unwrap();
    result.stream.next().await.unwrap().unwrap();
    for _ in 0..2 {
        opened_rx.recv().await.unwrap();
    }
    let admitted = std::mem::take(&mut *admissions.lock());
    assert_eq!(admitted.len(), 2);
    for read in &admitted {
        read.cancel.cancel();
    }
    for read in admitted {
        tokio::time::timeout(Duration::from_secs(5), read.closed)
            .await
            .expect("idle_consumer_must_not_block_processor_shutdown");
    }
    let error = result.stream.try_collect::<Vec<_>>().await.unwrap_err();
    assert!(
        error.to_string().contains("cancelled"),
        "cancellation_is_terminal: {error}"
    );
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
    dispatch.abort();
}
