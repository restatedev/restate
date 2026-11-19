// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Statistics use the production schema scanners over the full owner-task transport.

use tokio_util::sync::CancellationToken;

use restate_clock::{UniqueTimestamp, time::MillisSinceEpoch};
use restate_partition_store::PartitionStore;
use restate_partition_store::keys::KeyKind;
use restate_partition_store::keys::header::IndexScope;
use restate_partition_store::registry::IndexKind;
use restate_partition_store::stats::Stat;
use restate_partition_store::stats::aggregated::{
    DeploymentStats, DeploymentStatsKey, ServiceStats, ServiceStatsKey, VirtualObjectStats,
    VirtualObjectStatsKey,
};
use restate_storage_api::vqueue_table::{
    EntryContext, EntryKey, EntryMetadata, EntryStateRef, ReadVQueueTable, Stage, Status,
    StoredEntryRef, WriteVQueueTable, stats::EntryStatistics,
};
use restate_types::config::Configuration;
use restate_types::identifiers::DeploymentId;
use restate_types::vqueues::{EntryTargetRef, HandlerRef, VQueueId};

use crate::UserTables;
use crate::tables::stats::deployment_stats::schema::{
    DeploymentStatsSource, SysDeploymentStatsTable,
};
use crate::tables::stats::service_stats::schema::{ServiceStatsSource, SysServiceStatsTable};
use crate::tables::stats::virtual_object_stats::schema::{
    SysVirtualObjectStatsTable, VirtualObjectStatsSource,
};

use super::*;

pub(super) fn register_scanners(
    scanners: &SourceRegistry,
    node: GenerationalNodeId,
    reads: &Arc<Mutex<Reads>>,
) {
    let checked = |inner| {
        Arc::new(CheckedScanner {
            node,
            check_owner: true,
            inner,
            reads: Arc::clone(reads),
        })
    };
    scanners.register_partition_scanner::<ServiceStatsSource>(checked(Arc::new(
        SysServiceStatsTable::create_local_scanner(),
    )));
    scanners.register_partition_scanner::<DeploymentStatsSource>(checked(Arc::new(
        SysDeploymentStatsTable::create_local_scanner(),
    )));
    scanners.register_partition_scanner::<VirtualObjectStatsSource>(checked(Arc::new(
        SysVirtualObjectStatsTable::create_local_scanner(),
    )));
}

/// Opens `store` with `configure` applied, as a restart with that configuration.
async fn reopen(store: &mut PartitionStore, configure: impl FnOnce(&mut Configuration)) {
    let mut config = Configuration::pinned().as_ref().clone();
    configure(&mut config);
    store
        .verify_and_run_migrations(Default::default(), &config)
        .await
        .unwrap();
}

/// Tracks the status gauges of `store` and returns its Complete indexes: the
/// service, deployment and virtual-object gauges. `store` has no entries, so
/// there's nothing to backfill and the gauges are Complete at once.
async fn complete(store: &mut PartitionStore) -> [IndexScope; 3] {
    reopen(store, |config| {
        config.common.experimental.set_track_status_gauges(true)
    })
    .await;
    indexes(store)
}

/// Creates (`exists`) or deletes one invocation entry of `store`.
async fn set_entry(store: &mut PartitionStore, exists: bool) {
    let now = MillisSinceEpoch::new(1_744_010_000_000);
    let pk = store.partition_key_range().start() + 1;
    let qid = VQueueId::custom(pk, "statistics");
    let context = EntryContext {
        qid: &qid,
        target: &EntryTargetRef::VirtualObject {
            scope: None,
            service: "seeded",
            key: "object",
            handler: HandlerRef::UserHandler("run"),
        },
    };
    let key = EntryKey::new(false, now, 1, EntryId::new(EntryKind::Invocation, [1; 16]));
    let mut tx = store.transaction();
    if exists {
        let created_at = UniqueTimestamp::try_from_unix_millis(now).unwrap();
        tx.create_vqueue_entry_status(
            &context,
            EntryStateRef {
                stage: Stage::Inbox,
                status: Status::New,
                entry_key: &key,
                metadata: &EntryMetadata::default(),
                stats: &EntryStatistics::new(created_at, key.run_at()),
            },
        );
    } else {
        let header = tx
            .get_vqueue_entry_status(&key.entry_id().to_base_id(pk))
            .await
            .unwrap()
            .unwrap();
        tx.delete_vqueue_entry_status(&context, StoredEntryRef::from_header(&header));
    }
    tx.commit().await.unwrap();
}

/// The Complete status-gauge indexes of `store`.
fn indexes(store: &PartitionStore) -> [IndexScope; 3] {
    let (snapshot, _) = store.query_read(CancellationToken::new()).unwrap();
    [
        ServiceStats::DEFINITION,
        DeploymentStats::DEFINITION,
        VirtualObjectStats::DEFINITION,
    ]
    .map(|definition| IndexScope {
        partition: store.partition_id(),
        index: snapshot
            .complete_index(definition, IndexKind::Aggregate)
            .unwrap(),
    })
}

fn full_keys(
    [service_stats, deployment_stats, object_stats]: [IndexScope; 3],
    deployment: &str,
    service: &str,
    pk: u64,
) -> [Vec<u8>; 3] {
    let mut keys = [Vec::new(), Vec::new(), Vec::new()];
    ServiceStats::encode_key(
        service_stats,
        ServiceStatsKey::borrowed(service, Some("run"), EntryKind::Invocation),
        &mut keys[0],
    );
    DeploymentStats::encode_key(
        deployment_stats,
        DeploymentStatsKey::borrowed(deployment, service),
        &mut keys[1],
    );
    VirtualObjectStats::encode_key(
        object_stats,
        VirtualObjectStatsKey::borrowed(
            service,
            Some("tenant"),
            "object",
            Some("run"),
            EntryKind::Invocation,
            pk,
        ),
        &mut keys[2],
    );
    keys
}

const TABLES: [&str; 3] = [
    "sys_service_stats",
    "sys_deployment_stats",
    "sys_virtual_object_stats",
];

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn statistics_queries_fail_until_every_replica_is_complete() {
    let mut config = Configuration::default();
    config.common.experimental.set_query_stats_tables(true);
    Configuration::set(config);
    let mut fixture = setup().await;
    let env = environment(2, 2).with_distributed_execution(fixture.network.clone());
    env.register_tables(UserTables::new(
        fixture.partitions.clone(),
        fixture.scanners.clone(),
    ))
    .await
    .unwrap();
    let engine = DataFusionQueryEngine::from_env(env, None);
    // One session throughout: completion is checked by each read, not when the
    // session or the worker starts.
    let session = engine.create_session(SessionOptions::default()).unwrap();
    let fails = async |reasons: &[&str]| {
        for table in TABLES {
            let sql = format!("SELECT COALESCE(SUM(num_entries), 0) FROM {table}");
            let error = match session.execute(&sql, QueryOptions {}).await {
                Ok(result) => result
                    .stream
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap_err()
                    .to_string(),
                Err(error) => error.to_string(),
            };
            assert!(
                reasons.iter().all(|reason| error.contains(reason)),
                "partitions_that_are_not_complete_fail_the_query: {sql}: {error}"
            );
            // An owner's storage error reads once, naming the owner and the kind,
            // not as a DataFusion bug or a doubly wrapped error.
            assert!(
                error.starts_with("External error: N")
                    && error.contains(": storage: generic storage error: ")
                    && !error.contains("bug in DataFusion"),
                "remote_failures_name_the_owner_and_kind: {error}"
            );
        }
    };

    // Nothing is registered anywhere.
    fails(&["is not complete", "nothing is registered"]).await;
    // Every replica but one is Complete; the last one only maintains its gauges.
    // Registered over an existing entry, they wait for a backfill, which no
    // walk driver runs here.
    let (last, completed) = fixture.stores.split_last_mut().unwrap();
    for store in completed {
        complete(store).await;
    }
    set_entry(last, true).await;
    reopen(last, |config| {
        config.common.experimental.set_track_status_gauges(true)
    })
    .await;
    let not_complete = format!("is not complete on partition {}", last.partition_id());
    fails(&[&not_complete, "is still being completed"]).await;
    // Reset on that replica, its rebuilt gauges get index IDs that differ
    // from the other replicas'; each read resolves its own.
    reopen(last, |config| {
        config.common.experimental.set_status_gauges_reset(true)
    })
    .await;
    fails(&[&not_complete, "no maintained index"]).await;
    set_entry(last, false).await;
    let rebuilt = complete(last).await;
    assert_ne!(
        rebuilt.map(|scope| scope.index),
        complete(&mut fixture.stores[0])
            .await
            .map(|scope| scope.index)
    );
    for table in TABLES {
        assert_sql(
            session.as_ref(),
            "complete_replicas_answer_without_reopening",
            &format!("SELECT COALESCE(SUM(num_entries), 0) FROM {table}"),
            &[&["0"]],
        )
        .await;
    }

    // Counts on one remote owner's Complete indexes.
    let store = &mut fixture.stores[1];
    let keys = full_keys(
        complete(store).await,
        &DeploymentId::new().to_string(),
        "partial",
        store.partition_key_range().start() + 1,
    );
    let mut counts = vec![Stage::Running as u8, Status::Started as u8];
    counts.extend_from_slice(&3u64.to_be_bytes());
    let mut tx = store.transaction();
    for key in keys {
        tx.raw_put_cf(KeyKind::Stats, key, &counts);
    }
    tx.commit().await.unwrap();
    for table in TABLES {
        assert_sql(
            session.as_ref(),
            "reads_resolve_each_replica_s_instances",
            &format!("SELECT COALESCE(SUM(num_entries), 0) FROM {table}"),
            &[&["3"]],
        )
        .await;
    }
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn workers_without_ready_admission_reject_statistics_work() {
    let mut config = Configuration::default();
    config.common.experimental.set_query_stats_tables(true);
    Configuration::set(config);
    for (source, table) in [
        (
            ServiceStatsSource::identity(),
            SysServiceStatsTable::identity(),
        ),
        (
            DeploymentStatsSource::identity(),
            SysDeploymentStatsTable::identity(),
        ),
        (
            VirtualObjectStatsSource::identity(),
            SysVirtualObjectStatsTable::identity(),
        ),
    ] {
        // Workers that predate the registry serve the table under its own name.
        assert_eq!(source, format!("{table}.v2"));
    }
    let remote = GenerationalNodeId::new(2, 1);
    let old_worker = Arc::new(Mutex::new(None));
    let mut fixture = setup_with_scanners({
        let old_worker = Arc::clone(&old_worker);
        move |node, scanners| {
            if node == remote {
                *old_worker.lock() = Some(scanners.clone());
            }
        }
    })
    .await;
    for store in &mut fixture.stores {
        complete(store).await;
    }
    // That worker serves the gauges under the identity that predates the
    // registry, as its binary did, and not the registered one.
    let old_worker = old_worker.lock().take().unwrap();
    old_worker.unregister_partition_scanner::<ServiceStatsSource>();
    old_worker.register_partition_scanner::<SysServiceStatsTable>(Arc::new(
        SysServiceStatsTable::create_local_scanner(),
    ));
    let env = environment(2, 2).with_distributed_execution(fixture.network.clone());
    env.register_tables(UserTables::new(
        fixture.partitions.clone(),
        fixture.scanners.clone(),
    ))
    .await
    .unwrap();
    let engine = DataFusionQueryEngine::from_env(env, None);
    let session = engine.create_session(SessionOptions::default()).unwrap();
    let error = session
        .execute("SELECT COUNT(*) FROM sys_service_stats", QueryOptions {})
        .await
        .unwrap()
        .stream
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("local query source 'sys_service_stats.v2' is unavailable"),
        "{error}"
    );
    assert_sql(
        session.as_ref(),
        "workers_with_ready_admission_still_answer",
        "SELECT COALESCE(SUM(num_entries), 0) FROM sys_deployment_stats",
        &[&["0"]],
    )
    .await;
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordered_count_limits_apply_after_cross_partition_sums() {
    let mut config = Configuration::default();
    config.common.experimental.set_query_stats_tables(true);
    Configuration::set(config);
    let mut fixture = setup().await;
    // Uneven counts: the first partition's largest bucket (gamma, 3) isn't the
    // global leader. alpha only wins once its one-per-partition counts are summed,
    // so a TopK applied per partition before the sum would change the result.
    let deployment = DeploymentId::new().to_string();
    for (index, store) in fixture.stores.iter_mut().enumerate() {
        let pk = store.partition_key_range().start() + 1;
        let indexes = complete(store).await;
        let mut tx = store.transaction();
        for (service, count) in [("alpha", 1u64), ("gamma", 3)] {
            if service == "gamma" && index != 0 {
                continue;
            }
            let [service_key, _, _] = full_keys(indexes, &deployment, service, pk);
            let mut value = vec![Stage::Running as u8, Status::Started as u8];
            value.extend_from_slice(&count.to_be_bytes());
            tx.raw_put_cf(KeyKind::Stats, service_key, &value);
        }
        tx.commit().await.unwrap();
    }
    let alpha = fixture.stores.len().to_string();
    assert!(
        fixture.stores.len() > 3,
        "alpha must outgrow gamma only when summed"
    );
    let env = environment(2, 2).with_distributed_execution(fixture.network.clone());
    env.register_tables(UserTables::new(
        fixture.partitions.clone(),
        fixture.scanners.clone(),
    ))
    .await
    .unwrap();
    let engine = DataFusionQueryEngine::from_env(env, None);
    let session = engine.create_session(SessionOptions::default()).unwrap();
    assert_sql(
        session.as_ref(),
        "descending_limit_uses_the_summed_count",
        "SELECT service_name, num_entries FROM sys_service_stats ORDER BY num_entries DESC LIMIT 1",
        &[&["alpha", alpha.as_str()]],
    )
    .await;
    assert_sql(
        session.as_ref(),
        "ascending_limit_uses_the_summed_count",
        "SELECT service_name, num_entries FROM sys_service_stats ORDER BY num_entries ASC LIMIT 1",
        &[&["gamma", "3"]],
    )
    .await;
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 4)]
async fn schema_statistics_preserve_sql_pruning_and_owner_execution() {
    let mut config = Configuration::pinned().as_ref().clone();
    config.common.experimental.set_query_stats_tables(true);
    Configuration::set(config);
    let mut fixture = setup().await;
    let deployment = DeploymentId::new().to_string();
    let other_deployment = DeploymentId::new().to_string();
    let now = MillisSinceEpoch::new(1_744_010_000_000);
    let created_at = UniqueTimestamp::try_from_unix_millis(now).unwrap();
    for store in &mut fixture.stores {
        let indexes = complete(store).await;
        let pk = store.partition_key_range().start() + 1;
        let qid = VQueueId::custom(pk, "statistics");
        let mut tx = store.transaction();
        #[rustfmt::skip]
        let rows = [
            // service scope          key        handler                        stage           status           deployment
            ("alpha", None,           "object",  HandlerRef::StateMutation,      Stage::Inbox,   Status::New,     None),
            ("alpha", Some("tenant"), "object",  HandlerRef::UserHandler("run"), Stage::Inbox,   Status::New,     Some(&deployment)),
            ("alpha", Some("tenant"), "object",  HandlerRef::UserHandler("run"), Stage::Running, Status::Started, Some(&other_deployment)),
            ("beta",  Some(""),       "another", HandlerRef::UserHandler("run"), Stage::Paused,  Status::Yielded, Some(&deployment)),
            ("gamma", None,           "other",   HandlerRef::UserHandler("get"), Stage::Running, Status::Started, Some(&deployment)),
        ];
        for (index, (service, scope, object_key, handler, stage, status, deployment)) in
            rows.into_iter().enumerate()
        {
            let kind = if matches!(handler, HandlerRef::StateMutation) {
                EntryKind::StateMutation
            } else {
                EntryKind::Invocation
            };
            let key = EntryKey::new(false, now, 1, EntryId::new(kind, [index as u8 + 1; 16]));
            tx.create_vqueue_entry_status(
                &EntryContext {
                    qid: &qid,
                    target: &EntryTargetRef::VirtualObject {
                        scope,
                        service,
                        key: object_key,
                        handler,
                    },
                },
                EntryStateRef {
                    stage,
                    status,
                    entry_key: &key,
                    metadata: &EntryMetadata {
                        deployment: deployment.map(|id| id.as_str().into()),
                        ..Default::default()
                    },
                    stats: &EntryStatistics::new(created_at, key.run_at()),
                },
            );
        }
        // A foreign physical header must never override this store's ownership,
        // even for scans without any SQL key restriction.
        let foreign = indexes.map(|scope| IndexScope {
            partition: PartitionId::from(u16::from(scope.partition) + 100),
            ..scope
        });
        for key in full_keys(foreign, &deployment, "alpha", pk) {
            tx.raw_put_cf(KeyKind::Stats, key, b"malformed");
        }
        // Persisted empty gauges must not create a zero-valued logical row.
        for key in full_keys(indexes, &deployment, "zero", pk) {
            tx.raw_put_cf(KeyKind::Stats, key, []);
        }
        tx.commit().await.unwrap();
    }
    let env = environment(2, 2)
        .with_storage_placement(StoragePlacementOptions {
            require_leader: true,
        })
        .with_distributed_execution(fixture.network.clone());
    env.register_tables(UserTables::new(
        fixture.partitions.clone(),
        fixture.scanners.clone(),
    ))
    .await
    .unwrap();
    let engine = DataFusionQueryEngine::from_env(env, None);
    let session = engine.create_session(SessionOptions::default()).unwrap();
    let session = session.as_ref();

    // Each bucket contributes one per physical partition. No individual owner
    // has enough contributions to satisfy the public count predicate (> 4).
    #[rustfmt::skip]
    let cases: &[(&str, &str, &[&[&str]])] = &[
        ("service_counts_follow_cross_owner_sum",
         "SELECT service_name, num_entries FROM sys_service_stats WHERE num_entries > 4",
         &[
             &["alpha", "8"], &["alpha", "8"], &["alpha", "8"], &["beta", "8"], &["gamma", "8"],
         ]),
        ("deployment_counts_follow_cross_owner_sum",
         "SELECT service_name, num_entries FROM sys_deployment_stats WHERE num_entries > 4",
         &[&["alpha", "8"], &["alpha", "8"], &["beta", "8"], &["gamma", "8"]]),
        ("object_counts_remain_partition_local",
         "SELECT COUNT(*), SUM(num_entries) FROM sys_virtual_object_stats",
         &[&["40", "40"]]),
        ("residual_dependencies_survive_projection",
         "SELECT service_name FROM sys_service_stats WHERE handler IS NULL AND upper(status) = 'NEW'",
         &[&["alpha"]]),
        ("residuals_filter_expanded_buckets",
         "SELECT COUNT(*) FROM sys_virtual_object_stats WHERE upper(status) = 'STARTED'",
         &[&["16"]]),
        ("null_dimensions_are_not_empty_strings",
         "SELECT COUNT(*) FROM sys_virtual_object_stats WHERE scope IS NULL OR scope = ''",
         &[&["24"]]),
        ("prefix_and_suffix_constraints_combine",
         "SELECT SUM(num_entries) FROM sys_service_stats WHERE service_name LIKE 'alp%' AND handler LIKE 'r%'",
         &[&["16"]]),
        ("unsupported_boolean_alternative_keeps_all_candidates",
         "SELECT SUM(num_entries) FROM sys_service_stats WHERE service_name = 'gamma' OR stage = 'paused'",
         &[&["16"]]),
        ("invalid_enum_literal_does_not_discard_valid_alternative",
         "SELECT SUM(num_entries) FROM sys_service_stats WHERE kind IN ('invalid', 'state-mutation')",
         &[&["8"]]),
        ("contradictory_dimensions_are_empty",
         "SELECT service_name FROM sys_service_stats WHERE service_name = 'alpha' AND service_name > 'gamma'",
         &[]),
        ("limit_follows_final_count_residual",
         "SELECT service_name FROM sys_deployment_stats WHERE num_entries > 4 ORDER BY service_name DESC LIMIT 1 OFFSET 1",
         &[&["beta"]]),
        ("count_equality_uses_the_public_sum",
         "SELECT service_name FROM sys_service_stats WHERE num_entries = 1",
         &[]),
        ("count_upper_bound_uses_the_public_sum",
         "SELECT service_name FROM sys_deployment_stats WHERE num_entries < 8",
         &[]),
        ("count_or_dimension_keeps_both_alternatives",
         "SELECT service_name, num_entries FROM sys_service_stats WHERE service_name = 'beta' OR num_entries > 8",
         &[&["beta", "8"]]),
        ("count_alternative_survives_contradictory_dimensions",
         "SELECT service_name FROM sys_service_stats WHERE (service_name = 'alpha' AND service_name = 'beta') OR num_entries > 4",
         &[&["alpha"], &["alpha"], &["alpha"], &["beta"], &["gamma"]]),
        ("correlated_dimension_alternatives_preserve_sql_truth",
         "SELECT service_name, stage FROM sys_service_stats WHERE (service_name = 'alpha' AND stage = 'inbox') OR (service_name = 'gamma' AND stage = 'paused')",
         &[&["alpha", "inbox"], &["alpha", "inbox"]]),
        ("positive_membership_drops_unknown_alternatives",
         "SELECT service_name FROM sys_service_stats WHERE handler IN ('run', NULL) AND num_entries > 4",
         &[&["alpha"], &["alpha"], &["beta"]]),
        ("negated_membership_with_null_never_matches",
         "SELECT service_name FROM sys_service_stats WHERE handler NOT IN ('get', NULL)",
         &[]),
        ("negated_membership_excludes_null_handlers",
         "SELECT service_name FROM sys_service_stats WHERE handler NOT IN ('get')",
         &[&["alpha"], &["alpha"], &["beta"]]),
        ("empty_prefix_excludes_null_handlers",
         "SELECT service_name FROM sys_service_stats WHERE starts_with(handler, '')",
         &[&["alpha"], &["alpha"], &["beta"], &["gamma"]]),
        ("null_prefix_is_unknown",
         "SELECT service_name FROM sys_service_stats WHERE starts_with(handler, NULL)",
         &[]),
        ("unsupported_like_pattern_retains_sql_residual",
         "SELECT service_name FROM sys_service_stats WHERE service_name LIKE 'al_h%'",
         &[&["alpha"], &["alpha"], &["alpha"]]),
        ("escaped_percent_is_a_literal",
         r"SELECT service_name FROM sys_service_stats WHERE service_name LIKE 'alp\%%'",
         &[]),
        ("like_is_case_sensitive",
         "SELECT COUNT(*) FROM sys_service_stats WHERE service_name LIKE 'ALP%'",
         &[&["0"]]),
        ("reordered_projection_keeps_values_with_their_columns",
         "SELECT num_entries, service_name FROM sys_service_stats WHERE num_entries > 4",
         &[
             &["8", "alpha"], &["8", "alpha"], &["8", "alpha"], &["8", "beta"], &["8", "gamma"],
         ]),
        ("count_star_counts_summed_rows",
         "SELECT COUNT(*) FROM sys_service_stats",
         &[&["5"]]),
    ];
    for &(invariant, sql, expected) in cases {
        assert_sql(session, invariant, sql, expected).await;
    }

    // Object statistics aren't grouped by a view, so COUNT(*) sends an empty
    // projection to the real stats reader. With a batch target of 1, the reader
    // must split keys that hold two buckets (alpha's inbox and running entries)
    // across batches, with or without projected columns.
    let env = environment(2, 1).with_distributed_execution(fixture.network.clone());
    env.register_tables(UserTables::new(
        fixture.partitions.clone(),
        fixture.scanners.clone(),
    ))
    .await
    .unwrap();
    let single_row_engine = DataFusionQueryEngine::from_env(env, None);
    let single_row_session = single_row_engine
        .create_session(SessionOptions::default())
        .unwrap();
    for (sql, expected, columns) in [
        ("SELECT COUNT(*) FROM sys_virtual_object_stats", "40", 0),
        (
            "SELECT COUNT(DISTINCT stage || status) FROM sys_virtual_object_stats",
            "3",
            2,
        ),
    ] {
        fixture.reads.lock().batches.clear();
        assert_sql(
            single_row_session.as_ref(),
            "reader_batches_follow_the_batch_target",
            sql,
            &[&[expected]],
        )
        .await;
        let batches = std::mem::take(&mut fixture.reads.lock().batches);
        assert_eq!(
            batches.iter().map(|&(_, rows, _)| rows).sum::<usize>(),
            40,
            "every_bucket_reaches_the_reader: {sql}"
        );
        assert!(
            batches
                .iter()
                .all(|&(cols, rows, target)| cols == columns && target == 1 && rows <= 1),
            "bucket_expansion_respects_the_batch_target: {sql}: {batches:?}"
        );
    }

    // Count source rows before native residuals or aggregation. These SQL cases
    // retain the old scanner's useful work-avoidance checks on the production
    // owner path, including suffix constraints under multiple parent keys.
    #[rustfmt::skip]
    let pruning = [
        // table                       predicate                                       SQL sum candidates
        ("sys_service_stats",         "service_name = 'alpha'",                         "24", 24),
        ("sys_service_stats",         "service_name > 'beta'",                           "8",  8),
        ("sys_service_stats",         "service_name >= 'beta'",                         "16", 16),
        ("sys_service_stats",         "service_name < 'beta'",                          "24", 24),
        ("sys_service_stats",         "service_name <= 'beta'",                         "32", 32),
        ("sys_service_stats",         "'beta' <= service_name AND service_name < 'gamma'", "8", 8),
        ("sys_service_stats",         "service_name LIKE 'alp%' AND service_name > 'alpha'", "0", 0),
        ("sys_service_stats",         "service_name >= 'beta' AND service_name < 'beta'", "0", 0),
        ("sys_service_stats",         "handler < 'run'",                                 "8",  8),
        ("sys_service_stats",         "handler <= 'run'",                               "32", 32),
        ("sys_service_stats",         "handler LIKE 'r%'",                              "24", 24),
        // Optimized non-null checks remain residuals in the conjunctive selection compiler.
        ("sys_service_stats",         "handler LIKE '%'",                               "32", 40),
        ("sys_service_stats",         "handler IS NULL",                                 "8",  8),
        ("sys_service_stats",         "kind = 'state-mutation'",                          "8",  8),
        ("sys_service_stats",         "kind = 'invalid'",                                 "0",  0),
        ("sys_service_stats",         "stage = 'running'",                              "16", 40),
        ("sys_service_stats",         "stage < 'queued'",                               "24", 40),
        ("sys_service_stats",         "stage = 'queued'",                                "0",  0),
        ("sys_service_stats",         "stage != 'queued'",                              "40", 40),
        ("sys_service_stats",         "stage IN ('running', 'queued', NULL)",           "16", 40),
        ("sys_service_stats",         "stage NOT IN ('running', 'queued', NULL)",        "0",  0),
        ("sys_service_stats",         "status = 'yielded'",                               "8", 40),
        ("sys_service_stats",         "status < 'queued'",                              "16", 40),
        ("sys_service_stats",         "status IN ('started', 'queued')",                "16", 40),
        ("sys_service_stats",         "status NOT IN ('started', 'queued')",            "24", 40),
        ("sys_service_stats",         "status NOT IN ('started', 'queued', NULL)",       "0",  0),
        ("sys_service_stats",         "num_entries > 4",                                "40", 40),
        ("sys_service_stats",         "service_name ILIKE 'ALP%'",                       "24", 40),
        ("sys_deployment_stats",      "service_name LIKE 'alp%'",                        "16", 16),
        ("sys_deployment_stats",      "service_name IN ('alpha', 'gamma')",              "24", 24),
        ("sys_virtual_object_stats",  "scope IS NULL",                                  "16", 16),
        ("sys_virtual_object_stats",  "scope LIKE '%'",                                 "24", 40),
        ("sys_virtual_object_stats",  "scope < 'zzz'",                                  "24", 24),
        ("sys_virtual_object_stats",  "key LIKE 'obj%'",                                "24", 24),
        ("sys_virtual_object_stats",  "key IN ('object', 'absent')",                     "24", 24),
        ("sys_virtual_object_stats",  "key >= 'object' AND key < 'other'",               "24", 24),
        ("sys_virtual_object_stats",  "starts_with(handler, 'r')",                       "24", 24),
        ("sys_virtual_object_stats",  "handler IS NULL",                                 "8",  8),
    ];
    for (table, predicate, sum, candidates) in pruning {
        fixture.reads.lock().rows = 0;
        let sql = format!("SELECT COALESCE(SUM(num_entries), 0) FROM {table} WHERE {predicate}");
        assert_sql(session, "sql_truth_after_key_pruning", &sql, &[&[sum]]).await;
        assert_eq!(
            fixture.reads.lock().rows,
            candidates,
            "physical_selection_precedes_bucket_residuals: {sql}"
        );
    }
    for (predicate, sum, candidates) in [
        (
            format!("deployment_id >= '{deployment}' AND deployment_id <= '{deployment}'"),
            "24",
            24,
        ),
        (
            format!("starts_with(deployment_id, '{other_deployment}')"),
            "8",
            8,
        ),
    ] {
        fixture.reads.lock().rows = 0;
        assert_sql(
            session,
            "deployment_identity_bounds_precede_materialization",
            &format!("SELECT SUM(num_entries) FROM sys_deployment_stats WHERE {predicate}"),
            &[&[sum]],
        )
        .await;
        assert_eq!(
            fixture.reads.lock().rows,
            candidates,
            "deployment_key_pruning"
        );
    }
    let metadata = assert_sql(session, "native_owner_filter_and_partial_sum",
        "SELECT num_entries FROM sys_service_stats WHERE service_name = 'alpha' AND handler = 'run' AND upper(status) = 'STARTED' AND num_entries > 4",
        &[&["8"]]).await;
    {
        let tasks = fixture.tasks.lock();
        let tasks: Vec<_> = tasks
            .iter()
            .filter(|task| {
                task.id.session_id == metadata.session_id && task.id.query_ts == metadata.query_ts
            })
            .collect();
        assert_eq!(
            tasks
                .iter()
                .map(|task| task.owner.raw_id())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([1, 2, 3]),
            "every_contributing_owner_executes"
        );
        for task in tasks {
            assert!(
                task.plan.contains("FilterExec"),
                "native_residual_runs_on_owner: {}",
                task.plan
            );
            assert!(
                task.plan.contains("AggregateExec: mode=Partial"),
                "partial_sum_runs_on_owner: {}",
                task.plan
            );
        }
    }

    let target = &fixture.stores[1];
    let pk = target.partition_key_range().start() + 1;
    let metadata = assert_sql(session, "object_routing_deduplicates_selected_keys",
        &format!("SELECT service_name FROM sys_virtual_object_stats WHERE partition_key IN ({pk}, {pk}) AND upper(status) = 'STARTED' ORDER BY service_name LIMIT 1"),
        &[&["alpha"]]).await;
    {
        let tasks = fixture.tasks.lock();
        let tasks: Vec<_> = tasks
            .iter()
            .filter(|task| {
                task.id.session_id == metadata.session_id && task.id.query_ts == metadata.query_ts
            })
            .collect();
        assert_eq!(tasks.len(), 1, "selected_object_partition_has_one_task");
        assert_eq!(
            tasks[0].owner,
            owner(target.partition_id()),
            "object_query_reaches_only_selected_owner"
        );
        assert!(
            tasks[0].plan.contains("FilterExec"),
            "limit_keeps_owner_residual"
        );
        assert!(
            !tasks[0].plan.contains("AggregateExec")
                && !tasks[0].plan.contains("partitioning=Hash("),
            "partition_local_object_counts_need_no_aggregation_or_hash_exchange: {}",
            tasks[0].plan
        );
    }
    assert_sql(session, "unrestricted_or_branch_keeps_other_partitions",
        &format!("SELECT COUNT(*) FROM sys_virtual_object_stats WHERE partition_key = {pk} OR scope IS NULL"),
         &[&["19"]]).await;

    let other = &fixture.stores[2];
    let other_pk = other.partition_key_range().start() + 1;
    for (predicate, expected, partitions) in [
        (
            format!("partition_key IN ({pk}, {})", pk + 2),
            "5",
            vec![target.partition_id()],
        ),
        (
            format!("partition_key IN ({pk}, {other_pk})"),
            "10",
            vec![target.partition_id(), other.partition_id()],
        ),
        (
            format!(
                "partition_key = {pk} OR partition_key BETWEEN {} AND {}",
                pk + 2,
                pk + 3
            ),
            "5",
            vec![target.partition_id()],
        ),
    ] {
        fixture.reads.lock().partitions.clear();
        assert_sql(
            session,
            "scan_selection_preserves_sql_truth",
            &format!("SELECT COUNT(*) FROM sys_virtual_object_stats WHERE {predicate}"),
            &[&[expected]],
        )
        .await;
        let mut actual: Vec<_> = fixture
            .reads
            .lock()
            .partitions
            .iter()
            .map(|(_, id)| *id)
            .collect();
        actual.sort_unstable();
        assert_eq!(
            actual, partitions,
            "one_physical_scan_per_selected_partition: {predicate}"
        );
    }

    // The same-partition IN-list is navigated by the prepared physical selection.
    // A rejected key must not reach payload decoding or require another scan.
    let gap = full_keys(indexes(target), &deployment, "alpha", pk + 1)[2].clone();
    let target_id = target.partition_id();
    let mut tx = fixture.stores[1].transaction();
    tx.raw_put_cf(KeyKind::Stats, &gap, b"malformed");
    tx.commit().await.unwrap();
    drop(tx);
    fixture.reads.lock().partitions.clear();
    assert_sql(
        session,
        "same_partition_gaps_are_rejected_before_payload_decode",
        &format!(
            "SELECT COUNT(*) FROM sys_virtual_object_stats WHERE partition_key IN ({pk}, {})",
            pk + 2
        ),
        &[&["5"]],
    )
    .await;
    assert_eq!(
        fixture.reads.lock().partitions,
        [(owner(target_id), target_id)],
        "disjoint_keys_use_one_cursor"
    );
    let mut tx = fixture.stores[1].transaction();
    tx.raw_delete_cf(KeyKind::Stats, gap);
    tx.commit().await.unwrap();
    drop(tx);

    // Invalid keys before the requested range and inside IN-list gaps detect
    // lost seek bounds. Valid gap keys with corrupt payloads detect missed key
    // rejection before materialization, including a deployment suffix constraint.
    for store in &mut fixture.stores {
        let indexes = indexes(store);
        let pk = store.partition_key_range().start() + 1;
        let mut keys = full_keys(indexes, &deployment, "charlie", pk).to_vec();
        for service in ["0", "delta"] {
            let mut service_key = Vec::new();
            ServiceStatsKey::prefix(indexes[0], &mut service_key).service_name(service);
            service_key.pop();
            let mut object_key = Vec::new();
            VirtualObjectStatsKey::prefix(indexes[2], &mut object_key).service_name(service);
            object_key.pop();
            keys.extend([service_key, object_key]);
        }
        let mut tx = store.transaction();
        for key in keys {
            tx.raw_put_cf(KeyKind::Stats, key, b"malformed");
        }
        tx.commit().await.unwrap();
    }
    for (table, in_sum, prefix_sum) in [
        ("sys_service_stats", "32", "24"),
        ("sys_deployment_stats", "24", "16"),
        ("sys_virtual_object_stats", "32", "24"),
    ] {
        for (predicate, expected) in [
            ("service_name IN ('alpha', 'gamma')", in_sum),
            ("service_name LIKE 'alp%'", prefix_sum),
        ] {
            assert_sql(
                session,
                "key_pruning_precedes_payload_decode",
                &format!("SELECT SUM(num_entries) FROM {table} WHERE {predicate}"),
                &[&[expected]],
            )
            .await;
        }
        let error = tokio::time::timeout(Duration::from_secs(10), async {
            session
                .execute(
                    &format!("SELECT SUM(num_entries) FROM {table}"),
                    QueryOptions {},
                )
                .await
                .unwrap()
                .stream
                .try_collect::<Vec<_>>()
                .await
                .unwrap_err()
        })
        .await
        .expect("corrupt statistics must fail without hanging");
        assert!(
            error.to_string().contains("integrity"),
            "unpruned_corruption_is_an_error: {error}"
        );
    }
}
