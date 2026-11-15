// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shard-local JOINs prove colocation from declared sharding columns, never from
//! names or types, and give both inputs one admission per partition.

use restate_clock::{UniqueTimestamp, time::MillisSinceEpoch};
use restate_limiter::{Level, LimitKey};
use restate_storage_api::invocation_status_table::{
    InFlightInvocationMetadata, InvocationStatus, WriteInvocationStatusTable,
};
use restate_storage_api::vqueue_table::metadata::{VQueueLink, VQueueMeta};
use restate_storage_api::vqueue_table::{
    EntryContext, EntryKey, EntryMetadata, EntryStateRef, EntryValue, Stage, Status,
    WriteVQueueTable, stats::EntryStatistics,
};
use restate_types::config::Configuration;
use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionKey, WithPartitionKey};
use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef, VQueueId};
use restate_types::{PlainNodeId, Scope, ServiceName};
use restate_worker_api::invoker::InvocationStatusReport;
use restate_worker_api::invoker::status_handle::InvocationStatusReportInner;
use restate_worker_api::{
    CapturedPartitionRead, CapturedPartitionState, PartitionCaptureRequirements,
    ProcessorsManagerCommand, ProcessorsManagerHandle, UserLimitCounterEntry,
    VQueueSchedulerStatus,
};

use crate::UserTables;
use crate::tables::invocation_state::SysInvocationStateTable;
use crate::tables::invocation_status::SysInvocationStatusTable;

use super::*;

/// A scope whose hash falls into `range`, so that a scoped invocation's ID and its
/// scope agree on the owning partition, as they do in a real cluster.
fn scope_in(range: KeyRange) -> Scope {
    (0..)
        .map(|n| Scope::try_non_interned(&format!("scope-{n}")).unwrap())
        .find(|scope| (range.start()..=range.end()).contains(&scope.partition_key()))
        .unwrap()
}

/// The invoker rows of `partition`: two in-flight invocations on its first keys.
/// The manager's handle reads them after admission.
fn invoker_rows(partition: PartitionId) -> Vec<InvocationStatusReport> {
    let start = partition_start(partition);
    (0..2)
        .map(|n| {
            InvocationStatusReport::new(
                InvocationId::from_parts(start + n, InvocationUuid::from_u128(n.into())),
                InvocationStatusReportInner::default(),
            )
        })
        .collect()
}

/// The live state the processor of the partition starting at `start` captures:
/// two scheduled vqueues, and one counter for the partition's scope.
fn capture_live(
    start: PartitionKey,
    scope: &Scope,
    requirements: PartitionCaptureRequirements,
) -> CapturedPartitionState {
    CapturedPartitionState {
        invoker: None,
        scheduler: requirements.scheduler.then(|| {
            (0..2)
                .map(|n| {
                    (
                        VQueueId::custom(start + n, "join"),
                        VQueueSchedulerStatus::default(),
                    )
                })
                .collect()
        }),
        user_limits: requirements.user_limits.then(|| {
            [UserLimitCounterEntry {
                partition_key: scope.partition_key(),
                scope: scope.as_str().to_owned(),
                l1: None,
                l2: None,
                level: Level::Scope,
                usage: 1,
                concurrency_limit: None,
                rule_pattern: None,
                num_waiters: 0,
            }]
            .into()
        }),
    }
}

type Captures = Arc<Mutex<Vec<PartitionCaptureRequirements>>>;

/// Storage and live state for every partition, with each admission's capture
/// requirements recorded.
async fn cross_schema_fixture() -> (MultiFixture<impl NetworkSender>, Captures) {
    let manager = Arc::new(OnceLock::<Arc<PartitionStoreManager>>::new());
    let captures = Arc::new(Mutex::new(Vec::<PartitionCaptureRequirements>::new()));
    let fixture = setup_with_scanners({
        let manager = Arc::clone(&manager);
        let captures = Arc::clone(&captures);
        move |node, sources| {
            sources.register_declared_scanner::<SysInvocationStatusTable>(Arc::new(
                SysInvocationStatusTable::create_local_scanner(),
            ));
            crate::local_scanners::register_live_scanners(sources);
            let (commands, mut receiver) = tokio::sync::mpsc::channel(32);
            sources.register_processor_reads(
                ProcessorsManagerHandle::new(
                    commands,
                    Arc::new(crate::distributed::tests::UnusedRuleBook),
                    Arc::new(crate::distributed::tests::TestInvokers(Box::new(
                        invoker_rows,
                    ))),
                ),
                crate::mocks::query_io(),
            );
            let manager = Arc::clone(&manager);
            let captures = Arc::clone(&captures);
            tokio::spawn(async move {
                while let Some(command) = receiver.recv().await {
                    let ProcessorsManagerCommand::ReadPartition(request) = command else {
                        panic!("expected capture")
                    };
                    assert_eq!(node, owner(request.partition));
                    captures.lock().push(request.capture);
                    let store = manager
                        .get()
                        .unwrap()
                        .get_partition_store(request.partition)
                        .await
                        .unwrap();
                    let range = store.partition_key_range();
                    let read = CapturedPartitionRead {
                        partition: request.partition,
                        storage: request.capture.storage.then(|| {
                            store
                                .query_read(tokio_util::sync::CancellationToken::new())
                                .unwrap()
                                .0
                        }),
                        live: capture_live(range.start(), &scope_in(range), request.capture),
                    };
                    let _ = request.response.send(Ok(read));
                }
            });
        }
    })
    .await;
    assert!(manager.set(Arc::clone(&fixture.store_manager)).is_ok());
    let now = MillisSinceEpoch::new(1_744_010_000_000);
    let at = UniqueTimestamp::try_from_unix_millis(now).unwrap();
    for mut store in fixture.stores.iter().cloned() {
        let range = store.partition_key_range();
        let start = range.start();
        let mut tx = store.transaction();
        // Three unscoped invocations, two of them in flight, and one scoped one.
        for n in 0..3 {
            tx.put_invocation_status(
                &InvocationId::from_parts(start + n, InvocationUuid::from_u128(n.into())),
                &InvocationStatus::Invoked(InFlightInvocationMetadata::mock()),
            )
            .unwrap();
        }
        let scope = scope_in(range);
        let mut scoped = InFlightInvocationMetadata::mock();
        scoped.invocation_target = scoped.invocation_target.with_scope(Some(scope.clone()));
        tx.put_invocation_status(
            &InvocationId::from_parts(scope.partition_key(), InvocationUuid::from_u128(9)),
            &InvocationStatus::Invoked(scoped),
        )
        .unwrap();
        // One entry, in the first of the partition's two scheduled vqueues.
        let qid = VQueueId::custom(start, "join");
        tx.create_vqueue(
            &qid,
            &VQueueMeta::new(
                at,
                None,
                LimitKey::None,
                VQueueLink::Service(ServiceName::new("join")),
            ),
        );
        let key = EntryKey::new(false, now, 1, EntryId::new(EntryKind::Invocation, [1; 16]));
        // Its stage key, as the scheduler writes it, for index access.
        tx.put_vqueue_inbox(
            &qid,
            Stage::Inbox,
            &key,
            &EntryValue {
                status: Status::New,
                metadata: EntryMetadata::default(),
                stats: EntryStatistics::new(at, key.run_at()),
            },
        );
        tx.create_vqueue_entry_status(
            &EntryContext {
                qid: &qid,
                target: &EntryTargetRef::Service {
                    scope: None,
                    service: "join",
                    handler: "run",
                },
            },
            EntryStateRef {
                stage: Stage::Inbox,
                status: Status::New,
                entry_key: &key,
                metadata: &EntryMetadata::default(),
                stats: &EntryStatistics::new(at, key.run_at()),
            },
        );
        tx.commit().await.unwrap();
    }
    (fixture, captures)
}

/// Both captures a colocated invocation JOIN needs.
fn invoker_and_storage() -> PartitionCaptureRequirements {
    PartitionCaptureRequirements {
        storage: true,
        invoker: true,
        ..Default::default()
    }
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn sharding_columns_colocate_cross_schema_joins_with_shared_admission() {
    let (fixture, captures) = cross_schema_fixture().await;
    let mut config = Configuration::pinned().clone();
    config.common.experimental.set_query_index_access(true);
    Configuration::set(config);
    let env = environment(4, 1).with_distributed_execution(fixture.network.clone());
    env.register_tables(UserTables::new(
        fixture.partitions.clone(),
        fixture.scanners.clone(),
    ))
    .await
    .unwrap();
    let engine = DataFusionQueryEngine::from_env(env, None);
    let session = engine.create_session(SessionOptions::default()).unwrap();
    let partitions = fixture.partitions.0.len();
    let storage_and = |live: PartitionCaptureRequirements| PartitionCaptureRequirements {
        storage: true,
        ..live
    };
    let invoker = invoker_and_storage();
    let scheduler = storage_and(PartitionCaptureRequirements {
        scheduler: true,
        ..Default::default()
    });
    let user_limits = storage_and(PartitionCaptureRequirements {
        user_limits: true,
        ..Default::default()
    });
    // Each colocated query admits every partition once, capturing both inputs.
    #[rustfmt::skip]
    let colocated = [
        // Invocation IDs alone, without a redundant partition-key equality.
        ("SELECT COUNT(*) FROM sys_invocation_status s JOIN sys_invocation_state l ON s.id = l.id", "16", Some(invoker)),
        // Vqueue IDs across the scheduler and vqueue entries.
        ("SELECT COUNT(*) FROM sys_scheduler q JOIN sys_vqueue_entry_status e ON q.id = e.vqueue_id", "8", Some(scheduler)),
        // Scope-derived counters and scoped invocations.
        ("SELECT COUNT(*) FROM sys_user_limits u JOIN sys_invocation_status s ON u.scope = s.scope", "8", Some(user_limits)),
        // Lineage follows aliases and reordered projections: `partition_key` here is an invocation ID.
        ("SELECT COUNT(*) FROM (SELECT created_at, id AS partition_key FROM sys_invocation_status) s \
          RIGHT JOIN (SELECT retry_count, id AS invocation FROM sys_invocation_state) l ON s.partition_key = l.invocation", "16", Some(invoker)),
        // Preserved sides keep unmatched rows.
        ("SELECT COUNT(*) FROM sys_invocation_status s LEFT JOIN sys_invocation_state l ON s.id = l.id", "32", Some(invoker)),
        ("SELECT COUNT(*) FROM sys_invocation_status s FULL JOIN sys_invocation_state l ON s.id = l.id", "32", Some(invoker)),
        // A filtered input misses most partitions; they capture storage only.
        ("SELECT COUNT(*) FROM (SELECT id FROM sys_invocation_state WHERE partition_key = 0) l \
          RIGHT JOIN sys_invocation_status s ON l.id = s.id", "32", None),
        // An explicit partition-key equality still proves colocation.
        ("SELECT COUNT(*) FROM sys_invocation_status s JOIN sys_invocation_state l \
          ON s.partition_key = l.partition_key AND s.id = l.id", "16", Some(invoker)),
    ];
    for (sql, expected, capture) in colocated {
        captures.lock().clear();
        assert_sql(
            session.as_ref(),
            "colocated_join_results",
            sql,
            &[&[expected]],
        )
        .await;
        let captures = captures.lock();
        assert_eq!(
            captures.len(),
            partitions,
            "one_admission_per_partition: {sql}"
        );
        if let Some(capture) = capture {
            assert!(
                captures.iter().all(|c| *c == capture),
                "combined_capture: {sql}"
            );
        }
    }
    // `NOT IN` sees its whole subquery: the NULL scopes of the first partition's
    // unscoped invocations empty the result, although the subquery has no rows
    // on the other partitions.
    let boundary = fixture.partitions.0[1].1.key_range.start();
    assert_sql(
        session.as_ref(),
        "null_aware_anti_join_stays_global",
        &format!(
            "SELECT COUNT(*) FROM sys_invocation_status s WHERE s.scope NOT IN \
             (SELECT scope FROM sys_invocation_status WHERE partition_key < {boundary})"
        ),
        &[&["0"]],
    )
    .await;
    // A queue-restricted input reads the stage index inside the shared admission.
    let qid = VQueueId::custom(fixture.partitions.0[0].1.key_range.start(), "join");
    let sql = format!(
        "SELECT COUNT(*) FROM sys_scheduler q JOIN sys_vqueue_entry_status e \
         ON q.id = e.vqueue_id WHERE e.vqueue_id = '{qid}'"
    );
    captures.lock().clear();
    assert_sql(session.as_ref(), "index_join_results", &sql, &[&["1"]]).await;
    assert_eq!(
        *captures.lock(),
        [storage_and(PartitionCaptureRequirements {
            scheduler: true,
            ..Default::default()
        })],
        "index_join_shares_one_admission"
    );
    let plan = session
        .execute(&format!("EXPLAIN {sql}"), QueryOptions {})
        .await
        .unwrap()
        .stream
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| array_value_to_string(batch.column(1), row).unwrap())
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(plan.contains("IndexScanExec"), "{plan}");

    // Without a sharding-column proof, each input is admitted separately.
    #[rustfmt::skip]
    let global = [
        // `invoked_by_id` holds invocation IDs too, but names the caller.
        ("SELECT COUNT(*) FROM sys_invocation_status s JOIN sys_invocation_state l ON s.invoked_by_id = l.id", "0"),
        // A column literally named `partition_key` proves nothing.
        ("SELECT COUNT(*) FROM (SELECT status AS partition_key FROM sys_invocation_status) a \
          JOIN (SELECT status AS partition_key FROM sys_invocation_status) b ON a.partition_key = b.partition_key", "1024"),
        // Expressions may change which values compare equal.
        ("SELECT COUNT(*) FROM sys_invocation_status s JOIN sys_invocation_state l ON trim(s.id) = l.id", "16"),
        // NULL scopes match each other but determine no shard.
        ("SELECT COUNT(*) FROM sys_invocation_status a JOIN sys_invocation_status b \
          ON a.scope IS NOT DISTINCT FROM b.scope", "584"),
    ];
    for (sql, expected) in global {
        captures.lock().clear();
        assert_sql(session.as_ref(), "global_join_results", sql, &[&[expected]]).await;
        assert_eq!(
            captures.lock().len(),
            2 * partitions,
            "separate_admissions: {sql}"
        );
    }
}

/// Serves `first` on the first read and `then` afterwards. Two providers sharing one
/// selector plan the inputs of one JOIN against different snapshots.
#[derive(Clone, Debug)]
struct Reread {
    reads: Arc<std::sync::atomic::AtomicUsize>,
    first: Partitions,
    then: Partitions,
}

impl Reread {
    fn new(first: Partitions, then: Partitions) -> Self {
        Self {
            reads: Default::default(),
            first,
            then,
        }
    }
}

#[async_trait]
impl SelectPartitions for Reread {
    async fn get_live_partitions(
        &self,
    ) -> Result<Vec<(PartitionId, Partition)>, restate_types::errors::GenericError> {
        let read = self
            .reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(if read == 0 { &self.first } else { &self.then }.0.clone())
    }
}

/// Runs `sql`, returning its single value, with each table reading partitions
/// through its own selector.
async fn count_with_selectors(
    fixture: &MultiFixture<impl NetworkSender>,
    (status, state): (Reread, Reread),
    sql: &str,
) -> String {
    let env = environment(4, 1).with_distributed_execution(fixture.network.clone());
    env.register_table::<SysInvocationStatusTable>(
        "public",
        "sys_invocation_status",
        SysInvocationStatusTable::create_provider(status, &fixture.scanners),
    )
    .unwrap();
    env.register_table::<SysInvocationStateTable>(
        "public",
        "sys_invocation_state",
        SysInvocationStateTable::create_provider(state, &fixture.scanners),
    )
    .unwrap();
    let engine = DataFusionQueryEngine::from_env(env, None);
    let session = engine.create_session(SessionOptions::default()).unwrap();
    let result = session.execute(sql, QueryOptions {}).await.unwrap();
    let batches = result.stream.try_collect::<Vec<_>>().await.unwrap();
    array_value_to_string(batches[0].column(0), 0).unwrap()
}

/// Case name, selectors, query prefix, expected value, admissions, combined captures.
type TopologyCase<'a> = (
    &'a str,
    &'a dyn Fn() -> (Reread, Reread),
    &'a str,
    &'a str,
    usize,
    usize,
);

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_inputs_keep_their_sources_and_require_consistent_topologies() {
    let (fixture, captures) = cross_schema_fixture().await;
    let all = fixture.partitions.clone();
    let only = |ids: std::ops::RangeInclusive<u16>| {
        Partitions(
            all.0
                .iter()
                .filter(|(id, _)| ids.contains(&u16::from(*id)))
                .cloned()
                .collect(),
        )
    };
    // A split moved the boundary between the last two partitions.
    let split = {
        let mut split = all.clone();
        let last = split.0.len() - 1;
        let (left, right) = (split.0[last - 1].1.key_range, split.0[last].1.key_range);
        let mid = right.start() + (right.end() - right.start()) / 2;
        split.0[last - 1].1.key_range = KeyRange::new(left.start(), mid);
        split.0[last].1.key_range = KeyRange::new(mid + 1, right.end());
        split
    };
    let fixed = |status: Partitions, state: Partitions| {
        (
            Reread::new(status.clone(), status),
            Reread::new(state.clone(), state),
        )
    };
    let shared = |first: Partitions, then: Partitions| {
        let selector = Reread::new(first, then);
        (selector.clone(), selector)
    };
    let inner = "SELECT COUNT(*) FROM sys_invocation_status s JOIN sys_invocation_state l";
    let outer = "SELECT COUNT(*) FROM sys_invocation_status s LEFT JOIN sys_invocation_state l";
    #[rustfmt::skip]
    let cases: [TopologyCase; 7] = [
        // Disjoint sources stay disjoint: no partition borrows the other's universe.
        ("disjoint", &|| fixed(only(0..=0), only(7..=7)), inner, "0", 2, 0),
        ("disjoint", &|| fixed(only(0..=0), only(7..=7)), outer, "4", 2, 0),
        // A source restricted to a subset reads only its own partitions.
        ("subset", &|| fixed(all.clone(), only(0..=3)), inner, "8", 8, 4),
        ("subset", &|| fixed(all.clone(), only(0..=3)), outer, "32", 8, 4),
        // A later read without a partition still agrees on every shared range.
        ("shrunk", &|| shared(all.clone(), only(0..=6)), inner, "14", 8, 7),
        // A boundary that moved between the two reads keeps ordinary execution.
        ("split", &|| shared(all.clone(), split.clone()), inner, "16", 16, 0),
        ("split", &|| shared(all.clone(), split.clone()), outer, "32", 16, 0),
    ];
    for (case, selectors, query, expected, admissions, combined) in cases {
        captures.lock().clear();
        let actual =
            count_with_selectors(&fixture, selectors(), &format!("{query} ON s.id = l.id")).await;
        let observed = std::mem::take(&mut *captures.lock());
        let control = count_with_selectors(
            &fixture,
            selectors(),
            &format!("{query} ON trim(s.id) = l.id"),
        )
        .await;
        assert_eq!(
            (&actual, &control),
            (&expected.to_owned(), &expected.to_owned()),
            "{case}: {query}"
        );
        assert_eq!(observed.len(), admissions, "{case}: {query}: {observed:?}");
        assert_eq!(
            observed
                .iter()
                .filter(|c| **c == invoker_and_storage())
                .count(),
            combined,
            "{case}: {query}: {observed:?}"
        );
    }
}

restate_storage_query_api::define_schema! {
    flaky_node_rows_schema {
        name: "test_flaky_node_rows",
        sharding: ShardingScheme::ByNode(None),
        sharding_columns { plain_node_id: PlainNodeId, gen_node_id: GenerationalNodeId },
        columns {
            plain_node_id: ReString => DataType::Utf8,
            gen_node_id: ReString => DataType::Utf8,
        }
    }
}

restate_storage_query_api::define_schema! {
    late_node_rows_schema {
        name: "test_late_node_rows",
        sharding: ShardingScheme::ByNode(None),
        sharding_columns { plain_node_id: PlainNodeId, gen_node_id: GenerationalNodeId },
        columns {
            plain_node_id: ReString => DataType::Utf8,
            gen_node_id: ReString => DataType::Utf8,
        }
    }
}

/// Node rows whose scan on N2 emits a row for a stale generation, `N2:9`, and then
/// fails.
#[derive(Debug, Clone)]
struct LateNodeRows(NodeRows);

impl QuerySource for LateNodeRows {
    type Schema = LateNodeRowsSchema;
}

impl Scan for LateNodeRows {
    fn scan(
        &self,
        projection: SchemaRef,
        filters: &[datafusion::logical_expr::Expr],
        batch_size: usize,
        limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        if self.0.node.as_plain() != PlainNodeId::new(2) {
            return self.0.scan(projection, filters, batch_size, limit);
        }
        let schema = NodeRows::schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["N2"])),
                Arc::new(StringArray::from(vec!["N2:9"])),
            ],
        )
        .unwrap();
        let indices: Vec<_> = projection
            .fields()
            .iter()
            .map(|field| schema.index_of(field.name()).unwrap())
            .collect();
        Box::pin(RecordBatchStreamAdapter::new(
            projection,
            stream::iter([
                batch.project(&indices).map_err(Into::into),
                Err(datafusion::error::DataFusionError::Execution(
                    "scan failed".to_owned(),
                )),
            ]),
        ))
    }
}

/// Node rows whose scan fails on N2.
#[derive(Debug, Clone)]
struct FlakyNodeRows(NodeRows);

impl QuerySource for FlakyNodeRows {
    type Schema = FlakyNodeRowsSchema;
}

impl Scan for FlakyNodeRows {
    fn scan(
        &self,
        projection: SchemaRef,
        filters: &[datafusion::logical_expr::Expr],
        batch_size: usize,
        limit: Option<usize>,
    ) -> SendableRecordBatchStream {
        if self.0.node.as_plain() == PlainNodeId::new(2) {
            return Box::pin(RecordBatchStreamAdapter::new(
                projection,
                stream::iter([Err(datafusion::error::DataFusionError::Execution(
                    "scan failed".to_owned(),
                ))]),
            ));
        }
        self.0.scan(projection, filters, batch_size, limit)
    }
}

/// Sorted rows, with columns joined by `|`, and sorted warned nodes.
async fn rows_and_warnings(session: &dyn QuerySession, sql: &str) -> (Vec<String>, Vec<String>) {
    let result = session.execute(sql, QueryOptions {}).await.unwrap();
    let diagnostics = result.diagnostics;
    let batches = result.stream.try_collect::<Vec<_>>().await.unwrap();
    let mut rows: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                batch
                    .columns()
                    .iter()
                    .map(|column| array_value_to_string(column, row).unwrap())
                    .collect::<Vec<_>>()
                    .join("|")
            })
        })
        .collect();
    rows.sort();
    let mut warned: Vec<_> = diagnostics
        .warnings()
        .iter()
        .map(|warning| warning.node_id.to_string())
        .collect();
    warned.sort();
    (rows, warned)
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_sharding_columns_join_per_node_and_drop_failed_nodes() {
    let fixture = setup_with_scanners(|node, sources| {
        sources.register_node_scanner::<FlakyNodeRows>(Arc::new(FlakyNodeRows(NodeRows {
            node,
            reads: Default::default(),
        })));
        sources.register_node_scanner::<LateNodeRows>(Arc::new(LateNodeRows(NodeRows {
            node,
            reads: Default::default(),
        })));
    })
    .await;
    let engine = |distributed: bool| {
        let env = environment(4, 1);
        let env = if distributed {
            env.with_distributed_execution(fixture.network.clone())
        } else {
            env
        };
        env.register_table::<NodeRows>(
            "public",
            "nodes",
            Arc::new(NodeFanOutTableProvider::declared::<NodeRows>(
                fixture.metadata.clone(),
                fixture.scanners.clone(),
                None,
            )),
        )
        .unwrap();
        env.register_table::<FlakyNodeRows>(
            "public",
            "flaky_nodes",
            Arc::new(NodeFanOutTableProvider::declared::<FlakyNodeRows>(
                fixture.metadata.clone(),
                fixture.scanners.clone(),
                None,
            )),
        )
        .unwrap();
        env.register_table::<LateNodeRows>(
            "public",
            "late_nodes",
            Arc::new(NodeFanOutTableProvider::declared::<LateNodeRows>(
                fixture.metadata.clone(),
                fixture.scanners.clone(),
                None,
            )),
        )
        .unwrap();
        DataFusionQueryEngine::from_env(env, None)
    };
    let strings = |values: &[&str]| values.iter().map(|v| (*v).to_owned()).collect::<Vec<_>>();
    let distributed = engine(true);
    let session = distributed
        .create_session(SessionOptions::default())
        .unwrap();
    // EXPLAIN shows the join each node runs, although node tasks are opaque.
    let explain = session
        .execute(
            "EXPLAIN SELECT a.gen_node_id FROM nodes a JOIN nodes b ON a.plain_node_id = b.plain_node_id",
            QueryOptions {},
        )
        .await
        .unwrap()
        .stream
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .iter()
        .flat_map(|batch| (0..batch.num_rows()).map(|row| array_value_to_string(batch.column(1), row).unwrap()))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        explain.contains("task=[shard-local HashJoinExec: mode=CollectLeft"),
        "{explain}"
    );
    // One task per node runs both scans and the join; N4 has no node scanner.
    for (sql, tasks, warned) in [
        (
            "SELECT a.gen_node_id FROM nodes a JOIN nodes b ON a.plain_node_id = b.plain_node_id",
            3,
            &["N4"][..],
        ),
        // An expression proves nothing: each input fans out on its own.
        (
            "SELECT a.gen_node_id FROM nodes a JOIN nodes b ON trim(a.plain_node_id) = b.plain_node_id",
            6,
            &["N4", "N4"][..],
        ),
    ] {
        let before = fixture.tasks.lock().len();
        assert_eq!(
            rows_and_warnings(session.as_ref(), sql).await,
            (strings(&["N1:1", "N2:1", "N3:1"]), strings(warned)),
            "{sql}"
        );
        assert_eq!(fixture.tasks.lock().len() - before, tasks, "{sql}");
    }
    // A failed node drops its joined rows rather than NULL-extending the left side.
    assert_eq!(
        rows_and_warnings(
            session.as_ref(),
            "SELECT a.gen_node_id FROM nodes a LEFT JOIN flaky_nodes b ON a.plain_node_id = b.plain_node_id",
        )
        .await,
        (strings(&["N1:1", "N3:1"]), strings(&["N2", "N4"])),
    );
    // A node failing after it emitted output keeps that output and stops: here the
    // stale N2 row joins on the plain node ID...
    assert_eq!(
        rows_and_warnings(
            session.as_ref(),
            "SELECT a.gen_node_id, b.gen_node_id FROM nodes a LEFT JOIN late_nodes b ON a.plain_node_id = b.plain_node_id",
        )
        .await,
        (
            strings(&["N1:1|N1:1", "N2:1|N2:9", "N3:1|N3:1"]),
            strings(&["N2", "N4"])
        ),
    );
    // ...but not on the generation, and the failure that follows never turns into
    // a NULL-extended N2 row.
    assert_eq!(
        rows_and_warnings(
            session.as_ref(),
            "SELECT a.gen_node_id, b.gen_node_id FROM nodes a LEFT JOIN late_nodes b ON a.gen_node_id = b.gen_node_id",
        )
        .await,
        (strings(&["N1:1|N1:1", "N3:1|N3:1"]), strings(&["N2", "N4"])),
    );
    // Without distribution, only the coordinator's own node can run its task.
    let local = engine(false);
    let session = local.create_session(SessionOptions::default()).unwrap();
    assert_eq!(
        rows_and_warnings(
            session.as_ref(),
            "SELECT a.gen_node_id FROM nodes a JOIN nodes b ON a.gen_node_id = b.gen_node_id",
        )
        .await,
        (strings(&["N1:1"]), strings(&["N2", "N3", "N4"])),
    );
}

/// Query, row count, rows it may return, and admissions when shard-local.
type LimitCase<'a> = (
    String,
    usize,
    Option<&'a [String]>,
    Option<std::ops::RangeInclusive<usize>>,
);

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn limits_keep_joins_shard_local_when_any_preserved_rows_suffice() {
    let (fixture, captures) = cross_schema_fixture().await;
    // One lane per owner group and production-sized batches, so a lane hands over
    // a partition's rows without waiting and stopping early is observable.
    let env = environment(1, 8192).with_distributed_execution(fixture.network.clone());
    env.register_tables(UserTables::new(
        fixture.partitions.clone(),
        fixture.scanners.clone(),
    ))
    .await
    .unwrap();
    let engine = DataFusionQueryEngine::from_env(env, None);
    let session = engine.create_session(SessionOptions::default()).unwrap();
    let rows = async |sql: &str| rows_and_warnings(session.as_ref(), sql).await.0;
    let left = "SELECT s.id, l.id FROM sys_invocation_status s LEFT JOIN sys_invocation_state l ON s.id = l.id";
    let inner =
        "SELECT s.id, l.id FROM sys_invocation_status s JOIN sys_invocation_state l ON s.id = l.id";
    let view = "SELECT id, status FROM sys_invocation";
    let (all_left, all_inner, all_view) = (rows(left).await, rows(inner).await, rows(view).await);
    #[rustfmt::skip]
    let cases: [LimitCase; 7] = [
        // Any 3 preserved rows give 3 rows: each owner opens at most one partition
        // of 4, and an owner that starts after the limit is met opens none.
        (format!("{left} LIMIT 3"), 3, Some(&all_left), Some(1..=3)),
        ("SELECT l.id, s.id FROM sys_invocation_state l RIGHT JOIN sys_invocation_status s ON l.id = s.id LIMIT 3".to_owned(), 3, None, Some(1..=3)),
        (format!("{view} LIMIT 3"), 3, Some(&all_view), Some(1..=3)),
        // An OFFSET needs its skipped rows too.
        (format!("{left} LIMIT 3 OFFSET 30"), 2, Some(&all_left), Some(8..=8)),
        // Inner JOINs bound no input; an owner stops once it produced 3 rows, which
        // takes two partitions of 2.
        (format!("{inner} LIMIT 3"), 3, Some(&all_inner), Some(2..=6)),
        // A smaller bound on the preserved side selects rows globally.
        ("SELECT s.id, l.id FROM (SELECT id FROM sys_invocation_status LIMIT 1) s LEFT JOIN sys_invocation_state l ON s.id = l.id LIMIT 3".to_owned(), 1, Some(&all_left), None),
        // So does any bound on the other side.
        ("SELECT s.id, l.id FROM sys_invocation_status s LEFT JOIN (SELECT id FROM sys_invocation_state LIMIT 1) l ON s.id = l.id LIMIT 3".to_owned(), 3, None, None),
    ];
    for (sql, expected, valid, admissions) in cases {
        captures.lock().clear();
        let actual = rows(&sql).await;
        let observed = std::mem::take(&mut *captures.lock());
        assert_eq!(actual.len(), expected, "{sql}: {actual:?}");
        if let Some(valid) = valid {
            assert!(
                actual.iter().all(|row| valid.contains(row)),
                "{sql}: {actual:?}"
            );
        }
        let combined = observed
            .iter()
            .filter(|c| **c == invoker_and_storage())
            .count();
        match admissions {
            // Shard-local: every admission captures both inputs.
            Some(admissions) => {
                assert!(admissions.contains(&observed.len()), "{sql}: {observed:?}");
                assert_eq!(combined, observed.len(), "{sql}: {observed:?}");
            }
            None => assert_eq!(combined, 0, "{sql}: {observed:?}"),
        }
    }
}
