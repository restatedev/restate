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
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::invocation_status_table::{
    InFlightInvocationMetadata, InvocationStatus, WriteInvocationStatusTable,
};
use restate_storage_api::journal_events::{EventView, WriteJournalEventsTable};
use restate_storage_api::journal_table::{JournalEntry, WriteJournalTable};
use restate_storage_api::journal_table_v2::WriteJournalTable as WriteJournalTableV2;
use restate_storage_api::vqueue_table::metadata::{VQueueLink, VQueueMeta};
use restate_storage_api::vqueue_table::{
    EntryContext, EntryKey, EntryMetadata, EntryStateRef, EntryValue, Stage, Status,
    WriteVQueueTable, stats::EntryStatistics,
};
use restate_types::identifiers::{InvocationId, InvocationUuid, PartitionKey, WithPartitionKey};
use restate_types::journal::CompletionResult;
use restate_types::journal_events::raw::RawEvent;
use restate_types::journal_events::{Event, PausedEvent};
use restate_types::journal_v2::{Entry, SleepCommand};
use restate_types::storage::{StoredRawEntry, StoredRawEntryHeader};
use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef, HandlerRef, VQueueId};
use restate_types::{PlainNodeId, Scope, ServiceName};
use restate_util_string::ReString;
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
use crate::tables::journal::SysJournalTable;
use crate::tables::journal_events::SysJournalEventsTable;
use crate::tables::vqueue_meta::SysVqueueMetaTable;

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

/// A query execution: its session and query timestamp.
type Execution = (ReString, UniqueTimestamp);

/// Each admission's capture requirements, with the execution that requested it
/// and the partition it admitted.
///
/// A query that stops early, at a LIMIT, can return while some of its admissions
/// are still on their way to an owner. They are recorded whenever they arrive,
/// possibly while the next query runs, so a test of such queries counts only its
/// current execution's admissions ([`take_admissions`]).
type Captures = Arc<Mutex<Vec<(Execution, PartitionId, PartitionCaptureRequirements)>>>;

/// The capture requirements of every admission recorded so far.
fn requirements(captures: &Captures) -> Vec<PartitionCaptureRequirements> {
    captures
        .lock()
        .iter()
        .map(|(_, _, capture)| *capture)
        .collect()
}

/// Takes every recorded admission: the capture requirements of `execution`'s, and
/// the executions the others came from.
fn take_admissions(
    captures: &Captures,
    execution: &Execution,
) -> (Vec<PartitionCaptureRequirements>, Vec<Execution>) {
    let (current, others): (Vec<_>, Vec<_>) = std::mem::take(&mut *captures.lock())
        .into_iter()
        .partition(|(admitted, _, _)| admitted == execution);
    (
        current.into_iter().map(|(_, _, capture)| capture).collect(),
        others
            .into_iter()
            .map(|(admitted, _, _)| admitted)
            .collect(),
    )
}

/// Records one admission late, as if it had reached its owner after its query
/// returned: the first admission, at any owner, of a query whose SQL starts with
/// `prefix`. The read itself proceeds at once, so the query never waits for the
/// gate; only the record waits, until [`Self::release`] gets a permit.
struct AdmissionGate {
    prefix: &'static str,
    /// The delayed admission's capture. Set, under its lock, by the admission
    /// that claims the gate, so that only the first one is delayed and the
    /// capture is there as soon as the claim is.
    delayed: Mutex<Option<PartitionCaptureRequirements>>,
    /// The delayed record is added on a permit.
    release: tokio::sync::Semaphore,
    /// A permit once the delayed record is added.
    recorded: tokio::sync::Semaphore,
}

impl AdmissionGate {
    fn new(prefix: &'static str) -> Self {
        Self {
            prefix,
            delayed: Mutex::default(),
            release: tokio::sync::Semaphore::new(0),
            recorded: tokio::sync::Semaphore::new(0),
        }
    }
}

/// Storage and live state for every partition, with each admission's capture
/// requirements recorded.
async fn cross_schema_fixture() -> (MultiFixture<impl NetworkSender>, Captures) {
    cross_schema_fixture_with(None).await
}

/// [`cross_schema_fixture`], delaying the admission `gate` selects.
async fn cross_schema_fixture_with(
    gate: Option<Arc<AdmissionGate>>,
) -> (MultiFixture<impl NetworkSender>, Captures) {
    let manager = Arc::new(OnceLock::<Arc<PartitionStoreManager>>::new());
    let captures: Captures = Arc::default();
    let fixture = setup_with_scanners({
        let manager = Arc::clone(&manager);
        let captures = Arc::clone(&captures);
        move |node, sources| {
            sources.register_declared_scanner::<SysInvocationStatusTable>(Arc::new(
                SysInvocationStatusTable::create_local_scanner(),
            ));
            SysJournalTable::register_scanners(sources);
            sources.register_declared_scanner::<SysJournalEventsTable>(Arc::new(
                SysJournalEventsTable::create_local_scanner(),
            ));
            sources.register_declared_scanner::<SysVqueueMetaTable>(Arc::new(
                SysVqueueMetaTable::create_local_scanner(),
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
            let gate = gate.clone();
            tokio::spawn(async move {
                while let Some(command) = receiver.recv().await {
                    let ProcessorsManagerCommand::ReadPartition(request) = command else {
                        panic!("expected capture")
                    };
                    assert_eq!(node, owner(request.partition));
                    let execution = (
                        request.metadata.session_id.clone(),
                        request.metadata.query_ts,
                    );
                    let delayed = gate.as_ref().filter(|gate| {
                        request.metadata.redacted_sql.starts_with(gate.prefix) && {
                            let mut delayed = gate.delayed.lock();
                            let first = delayed.is_none();
                            if first {
                                *delayed = Some(request.capture);
                            }
                            first
                        }
                    });
                    match delayed {
                        Some(gate) => {
                            let (gate, captures) = (Arc::clone(gate), Arc::clone(&captures));
                            let (partition, capture) = (request.partition, request.capture);
                            tokio::spawn(async move {
                                gate.release.acquire().await.unwrap().forget();
                                captures.lock().push((execution, partition, capture));
                                gate.recorded.add_permits(1);
                            });
                        }
                        None => {
                            captures
                                .lock()
                                .push((execution, request.partition, request.capture))
                        }
                    }
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
                        timing: restate_worker_api::CaptureTiming::admitted_at(
                            tokio::time::Instant::now(),
                        ),
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
        // One entry, the first invocation's, in the first of the partition's two
        // scheduled vqueues.
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
        let first = InvocationId::from_parts(start, InvocationUuid::from_u128(0));
        let key = EntryKey::new(false, now, 1, EntryId::from(&first));
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
        // Different derivations of one shard-key space: an invocation and its entry.
        ("SELECT COUNT(*) FROM sys_invocation_status s JOIN sys_vqueue_entry_status e ON s.id = e.entry_id", "8", Some(storage_and(Default::default()))),
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
        let captures = requirements(&captures);
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
        requirements(&captures),
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
        // A scope's shard key is its hash, an ID's is the key it encodes.
        ("SELECT COUNT(*) FROM sys_user_limits u JOIN sys_invocation_status s ON u.scope = s.id", "0"),
    ];
    for (sql, expected) in global {
        captures.lock().clear();
        assert_sql(session.as_ref(), "global_join_results", sql, &[&[expected]]).await;
        assert_eq!(
            requirements(&captures).len(),
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
        let observed = requirements(&captures);
        captures.lock().clear();
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
    let (rows, warned, _) = rows_of_execution(session, sql).await;
    (rows, warned)
}

/// [`rows_and_warnings`], with the execution that produced them.
async fn rows_of_execution(
    session: &dyn QuerySession,
    sql: &str,
) -> (Vec<String>, Vec<String>, Execution) {
    let result = session.execute(sql, QueryOptions {}).await.unwrap();
    let execution = (result.metadata.session_id.clone(), result.metadata.query_ts);
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
    (rows, warned, execution)
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
    // One admission of the RIGHT JOIN is recorded late, during the next case, as
    // a query that stops early can leave admissions in flight. Each case counts
    // only its own execution's admissions.
    let gate = Arc::new(AdmissionGate::new("SELECT l.id, s.id"));
    let (fixture, captures) = cross_schema_fixture_with(Some(Arc::clone(&gate))).await;
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
    let delayed_into = format!("{view} LIMIT 3");
    let mut delayed_from = None;
    for (sql, expected, valid, admissions) in cases {
        captures.lock().clear();
        if sql == delayed_into {
            // The RIGHT JOIN's delayed admission is recorded now, during this case.
            gate.release.add_permits(1);
            gate.recorded.acquire().await.unwrap().forget();
        }
        let (actual, _, execution) = rows_of_execution(session.as_ref(), &sql).await;
        let (mut observed, others) = take_admissions(&captures, &execution);
        if sql == delayed_into {
            // Recorded during this case, the late admission still counts as the
            // RIGHT JOIN's, not this query's.
            let delayed_from = delayed_from.as_ref().unwrap();
            assert!(others.contains(delayed_from), "{sql}: {others:?}");
        }
        if sql.starts_with(gate.prefix) {
            // Its first admission is recorded later, during the next case.
            let delayed = *gate.delayed.lock();
            observed.extend(Some(
                delayed.expect("the RIGHT JOIN's first admission is delayed"),
            ));
            delayed_from = Some(execution);
        }
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

/// Operators below the partition reads, as a shard-local JOIN places them,
/// run as a fresh copy for every partition. Each partition holds a different
/// population: partition `p` adds `p + 1` invocations that join nothing, and
/// on even partitions one of its two joined invocations targets a service the
/// query's filter rejects. Under EXPLAIN ANALYZE, with one lane per owner,
/// which runs each partition's copy in turn, and with a lane per partition,
/// each owner's stage shows exactly its own partitions' counts, every copy
/// merged once: its scan read
/// every status, its filter kept the qualifying ones, and its join output the
/// joined ones. Each stage is the one task its owner installed for this query,
/// every task completes, and no metric shows as missing, partial, ambiguous or
/// truncated.
#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn operators_below_the_partition_reads_report_every_copy() {
    let (mut fixture, captures) = cross_schema_fixture().await;
    // Per owner: statuses scanned, kept by the filter, joined.
    let mut expected: HashMap<GenerationalNodeId, [usize; 3]> = HashMap::new();
    for store in fixture.stores.iter_mut() {
        let partition = store.partition_id();
        let p = u64::from(u16::from(partition));
        let start = store.partition_key_range().start();
        let rejected = p % 2 == 0;
        let mut tx = store.transaction();
        if rejected {
            let mut dropped = InFlightInvocationMetadata::mock();
            dropped.invocation_target =
                restate_types::invocation::InvocationTarget::service("Drop", "h");
            tx.put_invocation_status(
                &InvocationId::from_parts(start, InvocationUuid::from_u128(0)),
                &InvocationStatus::Invoked(dropped),
            )
            .unwrap();
        }
        for n in 0..=p {
            tx.put_invocation_status(
                &InvocationId::from_parts(
                    start + 10 + n,
                    InvocationUuid::from_u128(100 + u128::from(n)),
                ),
                &InvocationStatus::Invoked(InFlightInvocationMetadata::mock()),
            )
            .unwrap();
        }
        tx.commit().await.unwrap();
        // The fixture's four statuses, two of them joined, and these.
        let scanned = 4 + p as usize + 1;
        let counts = expected.entry(owner(partition)).or_default();
        counts[0] += scanned;
        counts[1] += scanned - usize::from(rejected);
        counts[2] += 2 - usize::from(rejected);
    }
    assert_eq!(expected.len(), 3);
    // One lane per owner, which runs a fresh copy for each of its partitions
    // in turn; then a lane per partition.
    for target_partitions in [1, 4] {
        let env =
            environment(target_partitions, 1).with_distributed_execution(fixture.network.clone());
        env.register_tables(UserTables::new(
            fixture.partitions.clone(),
            fixture.scanners.clone(),
        ))
        .await
        .unwrap();
        let engine = DataFusionQueryEngine::from_env(env, None);
        let session = engine.create_session(SessionOptions::default()).unwrap();
        let text = async |sql: &str| {
            let result = session.execute(sql, QueryOptions {}).await.unwrap();
            let metadata = result.metadata;
            let lines = result
                .stream
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .iter()
                .flat_map(|batch| {
                    let column = batch.num_columns() - 1;
                    (0..batch.num_rows())
                        .map(move |row| array_value_to_string(batch.column(column), row).unwrap())
                })
                .collect::<Vec<_>>();
            (metadata, lines)
        };
        let join = "FROM sys_invocation_status s JOIN sys_invocation_state l ON s.id = l.id \
        WHERE upper(s.target_service_name) <> 'DROP'";
        let sql = format!("EXPLAIN ANALYZE SELECT s.id {join}");
        let (metadata, lines) = text(&sql).await;
        let plan = lines.join("\n");
        eprintln!("target_partitions={target_partitions}\n{sql}\n{plan}");
        let metric = |line: &str, name: &str| -> usize {
            let start = line
                .find(&format!("{name}="))
                .unwrap_or_else(|| panic!("{name}: {line}"));
            line[start + name.len() + 1..]
                .chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>()
                .parse()
                .unwrap()
        };
        let stages: Vec<Vec<&str>> = plan.lines().fold(Vec::new(), |mut stages, line| {
            if line.contains("NetworkCoalesceExec") {
                stages.push(vec![line]);
            } else if let Some(stage) = stages.last_mut() {
                stage.push(line);
            }
            stages
        });
        assert_eq!(stages.len(), 3, "{plan}");
        // This execution admitted every partition exactly once, each at its
        // owner.
        let execution = (metadata.session_id.clone(), metadata.query_ts);
        let mut admitted: Vec<PartitionId> = std::mem::take(&mut *captures.lock())
            .into_iter()
            .filter(|(admitting, _, _)| *admitting == execution)
            .map(|(_, partition, _)| partition)
            .collect();
        admitted.sort();
        let all: Vec<PartitionId> = fixture
            .stores
            .iter()
            .map(|store| store.partition_id())
            .collect();
        assert_eq!(admitted, all, "{plan}");
        // The tasks this execution installed, by their full IDs: three, one
        // per owner, of one runtime query.
        let tasks: Vec<(
            GenerationalNodeId,
            restate_types::net::distributed_query::QueryTaskId,
        )> = fixture
            .tasks
            .lock()
            .iter()
            .filter(|task| {
                task.id.session_id == metadata.session_id && task.id.query_ts == metadata.query_ts
            })
            .map(|task| (task.owner, task.id.clone()))
            .collect();
        assert_eq!(tasks.len(), 3, "{tasks:#?}");
        assert!(
            tasks
                .iter()
                .all(|(_, id)| id.runtime_query_id == tasks[0].1.runtime_query_id && id.task == 0),
            "{tasks:#?}"
        );
        let installed: HashMap<GenerationalNodeId, u64> =
            tasks.iter().map(|(owner, id)| (*owner, id.stage)).collect();
        assert_eq!(installed.len(), 3, "{tasks:#?}");
        let mut owners = std::collections::HashSet::new();
        for stage in &stages {
            let find = |prefix: &str| {
                stage
                    .iter()
                    .position(|line| line.trim_start().starts_with(prefix))
                    .unwrap_or_else(|| panic!("{prefix}: {plan}"))
            };
            let reads = find("PartitionReadExec");
            let joined_at = stage
                .iter()
                .position(|line| {
                    let line = line.trim_start();
                    line.starts_with("HashJoinExec") || line.starts_with("SortMergeJoinExec")
                })
                .unwrap_or_else(|| panic!("JOIN: {plan}"));
            let filter = find("FilterExec: upper(target_service_name");
            let scan = stage
                .iter()
                .rposition(|line| line.contains("TableScanExec: table=sys_invocation_status"))
                .unwrap();
            // Below the partition reads, on this stage's owner.
            assert!(
                reads < joined_at && joined_at < filter && filter < scan,
                "{plan}"
            );
            let owner = *expected
                .keys()
                .find(|owner| stage[scan].contains(&format!("owner={owner},")))
                .unwrap_or_else(|| panic!("{plan}"));
            assert!(owners.insert(owner), "{plan}");
            let number = stage[0]
                .split("[Stage ")
                .nth(1)
                .and_then(|rest| rest.split(']').next())
                .and_then(|number| number.parse::<u64>().ok())
                .unwrap_or_else(|| panic!("{plan}"));
            assert_eq!(installed[&owner], number, "{plan}");
            let [scanned, kept, joined] = expected[&owner];
            assert_eq!(
                (
                    metric(stage[scan], "output_rows"),
                    metric(stage[filter], "output_rows"),
                    metric(stage[joined_at], "output_rows"),
                    metric(stage[reads], "output_rows"),
                ),
                (scanned, kept, joined, joined),
                "{owner}: {plan}"
            );
            assert!(stage[0].contains("tasks_completed=1"), "{plan}");
            if target_partitions == 1 {
                assert!(stage[reads].contains("parallelism=1,"), "{plan}");
            }
        }
        for marker in [
            "metrics_missing",
            "metrics_partial",
            "metrics_ambiguous",
            "metrics_truncated",
        ] {
            assert!(!plan.contains(marker), "{marker}: {plan}");
        }
        // The same totals, counted by the queries themselves.
        let count = async |sql: String| text(&sql).await.1[0].parse::<usize>().unwrap();
        let total = |at: usize| expected.values().map(|counts| counts[at]).sum::<usize>();
        assert_eq!(
        count(
            "SELECT COUNT(*) FROM sys_invocation_status WHERE upper(target_service_name) <> 'DROP'"
                .to_owned()
        )
        .await,
        total(1)
    );
        assert_eq!(count(format!("SELECT COUNT(*) {join}")).await, total(2));
    }
}

/// The ancestors of the operator at line `at` of `plan`, an EXPLAIN's physical
/// plan, nearest first.
fn ancestors<'a>(lines: &[&'a str], at: usize) -> Vec<&'a str> {
    let indent = |line: &str| line.len() - line.trim_start().len();
    let mut depth = indent(lines[at]);
    let mut ancestors = Vec::new();
    for line in lines[..at].iter().rev() {
        if indent(line) < depth {
            depth = indent(line);
            ancestors.push(*line);
        }
    }
    ancestors
}

fn is_join(line: &str) -> bool {
    line.contains("HashJoinExec") || line.contains("SortMergeJoinExec")
}

/// Whether every JOIN of `plan`, an EXPLAIN's physical plan, runs inside a
/// partition read: shard by shard.
fn joins_in_partition_reads(plan: &str) -> bool {
    let lines: Vec<&str> = plan.lines().collect();
    let joins: Vec<_> = (0..lines.len()).filter(|&at| is_join(lines[at])).collect();
    !joins.is_empty()
        && joins.iter().all(|&at| {
            ancestors(&lines, at)
                .iter()
                .any(|line| line.contains("PartitionReadExec"))
        })
}

/// Whether a union in `plan`, an EXPLAIN's physical plan, is a JOIN's input
/// inside a partition read: read shard by shard, not above the shards.
fn union_in_shard_join(plan: &str) -> bool {
    let lines: Vec<&str> = plan.lines().collect();
    (0..lines.len())
        .filter(|&at| lines[at].contains("UnionExec"))
        .any(|at| {
            let ancestors = ancestors(&lines, at);
            ancestors
                .iter()
                .position(|line| is_join(line))
                .is_some_and(|join| {
                    ancestors[join..]
                        .iter()
                        .any(|line| line.contains("PartitionReadExec"))
                })
        })
}

/// A JOIN input that unions a table's layouts, the journal's V1 and V2 tables, is
/// shard-local when its column is the same sharding column in every layout: each
/// shard reads both layouts' share of its partitions. Its rows are the global
/// JOIN's. Each partition's first invocation has three entries in the V2 layout,
/// and its second invocation two in the V1 layout; at the first one's partition
/// key, two invocations without a status have entries too, one in each layout,
/// so that the layouts' rows interleave under one key.
#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn unions_of_a_tables_layouts_join_shard_locally() {
    let (fixture, _captures) = cross_schema_fixture().await;
    let now = MillisSinceEpoch::new(1_744_010_000_000);
    for mut store in fixture.stores.iter().cloned() {
        let start = store.partition_key_range().start();
        let invocation =
            |n: u64| InvocationId::from_parts(start + n, InvocationUuid::from_u128(n.into()));
        // Uuids 5 and 9 at the first invocation's key: V2, V1, V2 in key order.
        let at_first = |uuid| InvocationId::from_parts(start, InvocationUuid::from_u128(uuid));
        let mut tx = store.transaction();
        for id in [invocation(0), at_first(9)] {
            for index in 0..3 {
                let entry = StoredRawEntry::new(
                    StoredRawEntryHeader::new(now),
                    Entry::from(SleepCommand {
                        wake_up_time: 1.into(),
                        completion_id: index,
                        name: "nap".into(),
                    })
                    .encode::<ServiceProtocolV4Codec>(),
                );
                WriteJournalTableV2::put_journal_entry(&mut tx, &id, index, &entry, &[]).unwrap();
            }
        }
        for id in [invocation(1), at_first(5)] {
            for index in 0..2 {
                let entry = JournalEntry::Completion(CompletionResult::Empty);
                WriteJournalTable::put_journal_entry(&mut tx, &id, index, &entry).unwrap();
            }
        }
        tx.commit().await.unwrap();
    }
    let env = environment(4, 1).with_distributed_execution(fixture.network.clone());
    env.register_tables(UserTables::new(
        fixture.partitions.clone(),
        fixture.scanners.clone(),
    ))
    .await
    .unwrap();
    let engine = DataFusionQueryEngine::from_env(env, None);
    let session = engine.create_session(SessionOptions::default()).unwrap();
    let explain = async |sql: &str| {
        session
            .execute(&format!("EXPLAIN {sql}"), QueryOptions {})
            .await
            .unwrap()
            .stream
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows())
                    .map(|row| array_value_to_string(batch.column(1), row).unwrap())
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    #[rustfmt::skip]
    let joins = [
        ("SELECT s.id, j.index FROM sys_invocation_status s JOIN sys_journal j ON {on}", "s.id", "j.id", 40),
        ("SELECT s.id, j.index FROM sys_invocation_status s LEFT JOIN sys_journal j ON {on}", "s.id", "j.id", 56),
        ("SELECT a.id, a.index FROM sys_journal a JOIN sys_journal b ON {on} AND a.index = b.index", "a.id", "b.id", 80),
    ];
    // Journal entries come by invocation, then index: the merge's key takes both.
    let plan = explain(
        "SELECT a.id FROM sys_journal a JOIN sys_journal b ON a.id = b.id AND a.index = b.index",
    )
    .await;
    assert!(
        plan.contains("on=[(invocation_id(id)@2, invocation_id(id)@2), (index@1, index@1)]")
            && !plan.contains("filter="),
        "merges_on_both_terms: {plan}"
    );
    for (sql, left, right, count) in joins {
        let local = sql.replace("{on}", &format!("{left} = {right}"));
        // A cast hides the sharding columns: one global JOIN.
        let global = sql.replace(
            "{on}",
            &format!("CAST({left} AS VARCHAR) = CAST({right} AS VARCHAR)"),
        );
        let plan = explain(&local).await;
        assert!(
            joins_in_partition_reads(&plan) && union_in_shard_join(&plan),
            "shard_local: {plan}"
        );
        // Both layouts' rows come in the ID's order: combined, they still merge.
        assert!(
            plan.contains("SortMergeJoinExec") && !plan.contains("SortExec:"),
            "merges_in_order: {plan}"
        );
        assert!(
            !plan.contains("invocation_id(id@"),
            "no_text_parsing: {plan}"
        );
        let global_plan = explain(&global).await;
        assert!(
            !joins_in_partition_reads(&global_plan) && !union_in_shard_join(&global_plan),
            "global: {global_plan}"
        );
        let (rows, _) = rows_and_warnings(session.as_ref(), &local).await;
        assert_eq!(rows.len(), count, "{local}");
        assert_eq!(
            rows,
            rows_and_warnings(session.as_ref(), &global).await.0,
            "same_rows_as_global: {local}"
        );
    }
}

/// Shard-local JOINs on ID columns whose reads come in a common native order merge
/// them on that order's function: no hash table, no split of a partition's rows,
/// no sort, and the rows a hash JOIN gives. Other JOINs keep hashing. Each
/// partition holds four invocations, the first with an entry, until the last
/// part adds more of both.
#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn id_joins_merge_on_native_orders() {
    let (fixture, _captures) = cross_schema_fixture().await;
    let env = environment(4, 1).with_distributed_execution(fixture.network.clone());
    env.register_tables(UserTables::new(
        fixture.partitions.clone(),
        fixture.scanners.clone(),
    ))
    .await
    .unwrap();
    let engine = DataFusionQueryEngine::from_env(env, None);
    let session = engine.create_session(SessionOptions::default()).unwrap();
    let explain = async |sql: &str| {
        session
            .execute(&format!("EXPLAIN {sql}"), QueryOptions {})
            .await
            .unwrap()
            .stream
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .iter()
            .flat_map(|batch| {
                (0..batch.num_rows())
                    .map(|row| array_value_to_string(batch.column(1), row).unwrap())
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    const STATUS_ENTRY: &str =
        "FROM sys_invocation_status s LEFT JOIN sys_vqueue_entry_status e ON s.id = e.entry_id";
    #[rustfmt::skip]
    let merged = [
        ("SELECT COUNT(*) FROM sys_invocation_status s JOIN sys_vqueue_entry_status e ON s.id = e.entry_id", "8", "base_entry_id"),
        (&format!("SELECT COUNT(*) {STATUS_ENTRY}"), "32", "base_entry_id"),
        (&format!("SELECT COUNT(*) FROM (SELECT s.id {STATUS_ENTRY} LIMIT 5)"), "5", "base_entry_id"),
        ("SELECT COUNT(*) FROM sys_invocation_status s WHERE EXISTS \
          (SELECT 1 FROM sys_vqueue_entry_status e WHERE e.entry_id = s.id)", "8", "base_entry_id"),
        // A filter on an input keeps its order.
        ("SELECT COUNT(*) FROM sys_invocation_status s JOIN sys_vqueue_entry_status e \
          ON s.id = e.entry_id WHERE e.status = 'new'", "8", "base_entry_id"),
        // The partition keys' equality holds for every merged pair.
        ("SELECT COUNT(*) FROM sys_invocation_status s JOIN sys_vqueue_entry_status e \
          ON s.partition_key = e.partition_key AND s.id = e.entry_id", "8", "base_entry_id"),
        // A table ordered by both functions joins itself on the first.
        ("SELECT COUNT(*) FROM sys_invocation_status a JOIN sys_invocation_status b ON a.id = b.id", "32", "invocation_id"),
        // The live invoker state reports its invocations in ID order.
        ("SELECT COUNT(*) FROM sys_invocation_status s JOIN sys_invocation_state l ON s.id = l.id", "16", "invocation_id"),
        // The view most queries read, which joins them on ID and partition key.
        ("SELECT COUNT(*) FROM sys_invocation WHERE status <> 'x'", "32", "invocation_id"),
    ];
    for (sql, expected, function) in merged {
        let plan = explain(sql).await;
        // On key fields the reads emit, not on a function of the IDs' text.
        assert!(
            plan.contains("SortMergeJoinExec") && plan.contains(&format!("{function}(id)@")),
            "merges_on_{function}: {plan}"
        );
        assert!(
            !plan.contains(&format!("{function}(id@")),
            "no_text_parsing: {plan}"
        );
        assert!(!plan.contains("HashJoinExec"), "no_hash_join: {plan}");
        assert!(!plan.contains("SortExec:"), "no_sort: {plan}");
        assert!(!plan.contains("filter="), "no_checked_equality: {plan}");
        assert_sql(session.as_ref(), "merged_join_results", sql, &[&[expected]]).await;
    }
    // Key fields never reach a query's output.
    let batches = session
        .execute(
            "SELECT * FROM sys_invocation_status s JOIN sys_vqueue_entry_status e ON s.id = e.entry_id",
            QueryOptions {},
        )
        .await
        .unwrap()
        .stream
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let schema = batches[0].schema();
    assert!(
        schema
            .fields()
            .iter()
            .all(|field| !field.name().contains("base_entry_id(")),
        "no_key_fields_in_output: {schema:?}"
    );
    // Another equality is checked on merged pairs.
    let sql = format!("SELECT COUNT(*) {STATUS_ENTRY} AND e.vqueue_id = s.target_service_name");
    let plan = explain(&sql).await;
    assert!(
        plan.contains("SortMergeJoinExec") && plan.contains("filter="),
        "{plan}"
    );
    assert_sql(
        session.as_ref(),
        "checked_equality_results",
        &sql,
        &[&["32"]],
    )
    .await;
    // No common order: scopes have no ordering function, and `NOT IN` needs the
    // whole subquery.
    #[rustfmt::skip]
    let hashed = [
        ("SELECT COUNT(*) FROM sys_user_limits u JOIN sys_invocation_status s ON u.scope = s.scope", "8"),
        ("SELECT COUNT(*) FROM sys_invocation_status s WHERE s.id NOT IN \
          (SELECT entry_id FROM sys_vqueue_entry_status)", "24"),
    ];
    for (sql, expected) in hashed {
        let plan = explain(sql).await;
        assert!(
            plan.contains("HashJoinExec") && !plan.contains("SortMergeJoinExec"),
            "hashes: {plan}"
        );
        assert_sql(session.as_ref(), "hashed_join_results", sql, &[&[expected]]).await;
    }
    // A filter that fixes an ID makes any order on it hold trivially; the live
    // scheduler still declares none, writes no key fields, and hashes.
    let fixed = VQueueId::custom(fixture.partitions.0[0].1.key_range.start(), "join");
    let sql = format!(
        "SELECT COUNT(*) FROM sys_vqueue_meta m JOIN sys_scheduler q \
         ON m.id = q.id WHERE q.id = '{fixed}'"
    );
    let plan = explain(&sql).await;
    assert!(
        plan.contains("HashJoinExec") && !plan.contains("SortMergeJoinExec"),
        "undeclared_orders_hash: {plan}"
    );
    assert_sql(session.as_ref(), "fixed_id_join_results", &sql, &[&["1"]]).await;
    // An owner whose entry read uses the stage index hashes: index reads come in
    // index order.
    let qid = VQueueId::custom(fixture.partitions.0[0].1.key_range.start(), "join");
    let sql = format!(
        "SELECT COUNT(*) FROM sys_invocation_status s JOIN sys_vqueue_entry_status e \
         ON s.id = e.entry_id WHERE e.vqueue_id = '{qid}'"
    );
    let plan = explain(&sql).await;
    assert!(
        plan.contains("IndexScanExec") && plan.contains("HashJoinExec"),
        "index_reads_hash: {plan}"
    );
    assert!(
        !plan.contains("SortMergeJoinExec"),
        "index_reads_hash: {plan}"
    );
    assert_sql(session.as_ref(), "index_join_results", &sql, &[&["1"]]).await;
    // Merges united on one owner keep their templates on one partition.
    let plan = explain(
        "SELECT s.id FROM sys_invocation_status s WHERE EXISTS \
         (SELECT 1 FROM sys_vqueue_entry_status e WHERE e.entry_id = s.id) UNION ALL \
         SELECT a.id FROM sys_invocation_status a WHERE EXISTS \
         (SELECT 1 FROM sys_invocation_status b WHERE b.id = a.id)",
    )
    .await;
    assert!(
        plan.matches("SortMergeJoinExec").count() >= 2 && !plan.contains("RepartitionExec"),
        "united_merges_keep_one_partition: {plan}"
    );

    // Merges match the rows hash JOINs do. Each partition gets two more
    // invocations at its first key, uuids 1 and 256, whose IDs' text sorts the
    // other way, and three more entries at that key: uuid 256's, one of an
    // invocation that doesn't exist, and a state mutation's.
    let now = MillisSinceEpoch::new(1_744_010_000_000);
    let at = UniqueTimestamp::try_from_unix_millis(now).unwrap();
    for mut store in fixture.stores.iter().cloned() {
        let start = store.partition_key_range().start();
        let qid = VQueueId::custom(start, "join");
        let invocation = |uuid| InvocationId::from_parts(start, InvocationUuid::from_u128(uuid));
        let mut tx = store.transaction();
        for uuid in [1, 256] {
            tx.put_invocation_status(
                &invocation(uuid),
                &InvocationStatus::Invoked(InFlightInvocationMetadata::mock()),
            )
            .unwrap();
        }
        let entries = [
            EntryId::from(&invocation(256)),
            EntryId::from(&invocation(512)),
            EntryId::new(EntryKind::StateMutation, [1; 16]),
        ];
        for (seq, entry) in (2..).zip(entries) {
            let target = if entry.kind() == EntryKind::StateMutation {
                EntryTargetRef::VirtualObject {
                    scope: None,
                    service: "join",
                    key: "k",
                    handler: HandlerRef::StateMutation,
                }
            } else {
                EntryTargetRef::Service {
                    scope: None,
                    service: "join",
                    handler: "run",
                }
            };
            let key = EntryKey::new(false, now, seq, entry);
            tx.create_vqueue_entry_status(
                &EntryContext {
                    qid: &qid,
                    target: &target,
                },
                EntryStateRef {
                    stage: Stage::Inbox,
                    status: Status::New,
                    entry_key: &key,
                    metadata: &EntryMetadata::default(),
                    stats: &EntryStatistics::new(at, key.run_at()),
                },
            );
        }
        // Events of both new invocations.
        for uuid in [1, 256] {
            for lsn in 0..2 {
                let event = RawEvent::from(Event::Paused(PausedEvent { last_failure: None }));
                tx.put_journal_event(
                    &invocation(uuid),
                    EventView::new(now, lsn as u32, event),
                    lsn,
                )
                .unwrap();
            }
        }
        tx.commit().await.unwrap();
    }
    const FROM_STATUS: &str = "FROM sys_invocation_status s";
    // The new invocations of the first two partitions.
    let looked_up = fixture.partitions.0[..2]
        .iter()
        .flat_map(|(_, partition)| {
            [1, 256].map(|uuid| {
                format!(
                    "'{}'",
                    InvocationId::from_parts(
                        partition.key_range.start(),
                        InvocationUuid::from_u128(uuid)
                    )
                )
            })
        })
        .collect::<Vec<_>>()
        .join(", ");
    #[rustfmt::skip]
    let joins = [
        (format!("SELECT s.id, e.entry_id {FROM_STATUS} JOIN sys_vqueue_entry_status e ON {{on}}"), "s.id", "e.entry_id"),
        (format!("SELECT s.id, e.entry_id {FROM_STATUS} LEFT JOIN sys_vqueue_entry_status e ON {{on}}"), "s.id", "e.entry_id"),
        (format!("SELECT s.id, e.entry_id {FROM_STATUS} RIGHT JOIN sys_vqueue_entry_status e ON {{on}}"), "s.id", "e.entry_id"),
        (format!("SELECT s.id, e.entry_id {FROM_STATUS} FULL JOIN sys_vqueue_entry_status e ON {{on}}"), "s.id", "e.entry_id"),
        (format!("SELECT s.id {FROM_STATUS} WHERE EXISTS (SELECT 1 FROM sys_vqueue_entry_status e WHERE {{on}})"), "e.entry_id", "s.id"),
        (format!("SELECT s.id {FROM_STATUS} WHERE NOT EXISTS (SELECT 1 FROM sys_vqueue_entry_status e WHERE {{on}})"), "e.entry_id", "s.id"),
        ("SELECT a.id, b.id FROM sys_invocation_status a JOIN sys_invocation_status b ON {on}".to_owned(), "a.id", "b.id"),
        // A checked equality that holds for the scoped invocations only, NULL for
        // the others: their rows come padded.
        ("SELECT a.id, b.id FROM sys_invocation_status a RIGHT JOIN sys_invocation_status b ON {on} AND a.scope = b.scope".to_owned(), "a.id", "b.id"),
        ("SELECT a.id, b.id FROM sys_invocation_status a FULL JOIN sys_invocation_status b ON {on} AND a.scope = b.scope".to_owned(), "a.id", "b.id"),
        // Invocations looked up by ID.
        (format!("SELECT s.id, e.entry_id {FROM_STATUS} LEFT JOIN sys_vqueue_entry_status e ON {{on}} WHERE s.id IN ({looked_up})"), "s.id", "e.entry_id"),
        // Every source's reads write the key fields of their records.
        (format!("SELECT s.id, e.id {FROM_STATUS} JOIN sys_journal_events e ON {{on}}"), "s.id", "e.id"),
        ("SELECT a.id, b.id FROM sys_vqueue_meta a JOIN sys_vqueue_meta b ON {on}".to_owned(), "a.id", "b.id"),
        (format!("SELECT s.id, l.id {FROM_STATUS} LEFT JOIN sys_invocation_state l ON {{on}}"), "s.id", "l.id"),
    ];
    let plan = explain(&format!(
        "SELECT s.id {FROM_STATUS} JOIN sys_vqueue_entry_status e ON s.id = e.entry_id \
         WHERE s.id IN ({looked_up})"
    ))
    .await;
    assert!(
        plan.contains("MultiGetExec") && plan.contains("SortMergeJoinExec"),
        "lookups_merge: {plan}"
    );
    for (sql, function) in [
        (
            format!("SELECT COUNT(*) {FROM_STATUS} JOIN sys_journal_events e ON s.id = e.id"),
            "invocation_id",
        ),
        (
            "SELECT COUNT(*) FROM sys_vqueue_meta a JOIN sys_vqueue_meta b ON a.id = b.id"
                .to_owned(),
            "vqueue_id",
        ),
    ] {
        let plan = explain(&sql).await;
        assert!(
            plan.contains(&format!("on=[({function}(id)@1, {function}(id)@1)]")),
            "key_fields_on_both_sides: {plan}"
        );
        assert!(
            !plan.contains(&format!("{function}(id@")),
            "no_text_parsing: {plan}"
        );
    }
    // A column named like a key field is none: the merge still compares the key
    // fields the reads emit.
    let keyed = |value: &str| {
        format!(
            "SELECT s.id, s.\"base_entry_id(id)\", e.entry_id FROM (SELECT id, {value} AS \"base_entry_id(id)\" \
             FROM sys_invocation_status) s JOIN sys_vqueue_entry_status e ON {{on}}"
        )
    };
    let named_like_keys = [
        format!("arrow_cast(X'{}', 'FixedSizeBinary(25)')", "00".repeat(25)),
        "'x'".to_owned(),
        "id".to_owned(),
    ]
    .map(|value| (keyed(&value), "s.id", "e.entry_id"));
    let joins = joins.into_iter().chain(named_like_keys);
    for (sql, left, right) in joins {
        let merged = sql.replace("{on}", &format!("{left} = {right}"));
        // A cast hides the sharding columns: one global hash JOIN.
        let hashed = sql.replace(
            "{on}",
            &format!("CAST({left} AS VARCHAR) = CAST({right} AS VARCHAR)"),
        );
        let plan = explain(&merged).await;
        assert!(
            plan.contains("SortMergeJoinExec") && !plan.contains("HashJoinExec"),
            "merges: {plan}"
        );
        assert!(explain(&hashed).await.contains("HashJoinExec"), "{hashed}");
        let (rows, _) = rows_and_warnings(session.as_ref(), &merged).await;
        assert!(!rows.is_empty(), "{merged}");
        assert_eq!(
            rows,
            rows_and_warnings(session.as_ref(), &hashed).await.0,
            "same_rows_as_hashed: {merged}"
        );
    }
}
