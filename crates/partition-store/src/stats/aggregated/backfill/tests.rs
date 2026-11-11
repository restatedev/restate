// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rocksdb::ReadOptions;
use tokio_util::sync::CancellationToken;
use zerocopy::IntoBytes;

use restate_clock::{RoughTimestamp, UniqueTimestamp};
use restate_rocksdb::RocksDbManager;
use restate_storage_api::fsm_table::WriteFsmTable;
use restate_storage_api::invocation_status_table::{
    InFlightInvocationMetadata, InvocationStatus, WriteInvocationStatusTable,
};
use restate_storage_api::vqueue_table::stats::EntryStatistics;
use restate_storage_api::vqueue_table::{
    EntryContext, EntryKey, EntryMetadata, EntryStateRef, ReadVQueueTable, Stage, Status,
    StoredEntryRef, WriteVQueueTable,
};
use restate_storage_api::{StorageError, Transaction};
use restate_storage_query_api::SchemaBinding;
use restate_storage_query_api::scan::BlockingScan;
use restate_storage_query_api::selection::Selection;
use restate_types::Scope;
use restate_types::config::Configuration;
use restate_types::identifiers::{BaseEntryId, InvocationId, InvocationUuid, PartitionId};
use restate_types::invocation::{InvocationTarget, VirtualObjectHandlerType, WorkflowHandlerType};
use restate_types::logs::Lsn;
use restate_types::partition_table::Partition;
use restate_types::sharding::KeyRange;
use restate_types::state_mut::StateMutationInput;
use restate_types::vqueues::{EntryId, EntryTargetExt};

use crate::fsm_table::{
    get_min_restate_version_from_partition_db, get_registry_from_partition_db,
    get_storage_features_from_partition_db, put_storage_features_json,
};
use crate::keys::KeyKind;
use crate::registry::{NotReady, RegistrationState, Registry};
use crate::stats::aggregated::{
    DeploymentStatsBinding, ServiceStatsBinding, StageStatusCounts, VirtualObjectStatsBinding,
};
use crate::stats::{RegisteredStatKeyPrefix, StatId, StatKeyPrefix, StatKind, StatValueCodec};
use crate::{PartitionStore, PartitionStoreManager};

use super::*;

/// 147 entries over three partition keys, with invocation statuses and a state
/// mutation input as their targets.
pub(crate) async fn seed(store: &mut PartitionStore) -> (InvocationId, InvocationStatus) {
    let mut tx = store.transaction();
    let mut last = None;
    for partition_key in [10, 11, 12] {
        let qid = VQueueId::custom(partition_key, "backfill");
        // These rows sort before and after the live entries and must not be counted.
        for byte in [0, 255] {
            tx.put_invocation_status(
                &InvocationId::from_parts(partition_key, InvocationUuid::from_bytes([byte; 16])),
                &InvocationStatus::Invoked(InFlightInvocationMetadata::mock()),
            )
            .unwrap();
        }
        for i in 1..=48u8 {
            let id = InvocationId::from_parts(partition_key, InvocationUuid::from_bytes([i; 16]));
            let target = match i % 4 {
                0 => InvocationTarget::service("service", "run"),
                1 => InvocationTarget::workflow(
                    "workflow",
                    "key",
                    "run",
                    WorkflowHandlerType::Workflow,
                ),
                // A large object group and many different objects sharing a partition key.
                2 => InvocationTarget::virtual_object(
                    "object",
                    "shared",
                    "run",
                    VirtualObjectHandlerType::Exclusive,
                ),
                _ => InvocationTarget::virtual_object(
                    "object",
                    format!("key-{i}"),
                    "run",
                    VirtualObjectHandlerType::Exclusive,
                ),
            }
            .with_scope(Some(Scope::try_non_interned("tenant").unwrap()));
            let mut metadata = InFlightInvocationMetadata::mock_with_vqueue(partition_key);
            metadata.vqueue_id = Some(qid.clone());
            metadata.invocation_target = target.clone();
            let invocation = InvocationStatus::Invoked(metadata);
            tx.put_invocation_status(&id, &invocation).unwrap();
            let key = EntryKey::new(
                false,
                RoughTimestamp::new(10),
                u64::from(i),
                EntryId::from(id),
            );
            let (stage, status) = match i % 6 {
                0 => (Stage::Finished, Status::Succeeded),
                1 => (Stage::Inbox, Status::New),
                2 => (Stage::Inbox, Status::Scheduled),
                3 => (Stage::Running, Status::Started),
                4 => (Stage::Suspended, Status::Started),
                _ => (Stage::Paused, Status::Started),
            };
            let entry_metadata = EntryMetadata {
                deployment: (i % 3 != 0).then(|| format!("dp_{}", i % 5).into()),
                ..EntryMetadata::default()
            };
            let stats = EntryStatistics::new(
                UniqueTimestamp::try_from_parts(100, 1).unwrap(),
                key.run_at(),
            );
            tx.create_vqueue_entry_status(
                &EntryContext {
                    qid: &qid,
                    target: &target.entry_target_ref(),
                },
                EntryStateRef {
                    stage,
                    status,
                    entry_key: &key,
                    metadata: &entry_metadata,
                    stats: &stats,
                },
            );
            last = Some((id, invocation));
        }

        let input = StateMutationInput {
            service_id: ServiceId::new(
                Some(Scope::try_non_interned("tenant").unwrap()),
                "object",
                "shared",
            ),
            version: None,
            state: [(
                bytes::Bytes::from_static(b"state"),
                bytes::Bytes::from(vec![42; 65536]),
            )]
            .into(),
        };
        let key = EntryKey::new(
            false,
            RoughTimestamp::new(10),
            49u64,
            EntryId::new(EntryKind::StateMutation, [49; 16]),
        );
        let stats = EntryStatistics::new(
            UniqueTimestamp::try_from_parts(100, 1).unwrap(),
            key.run_at(),
        );
        tx.create_vqueue_entry_status(
            &EntryContext {
                qid: &qid,
                target: &input.entry_target_ref(),
            },
            EntryStateRef {
                stage: Stage::Inbox,
                status: Status::New,
                entry_key: &key,
                metadata: &EntryMetadata::default(),
                stats: &stats,
            },
        );
        tx.put_vqueue_input_payload(&qid, key.seq(), key.entry_id(), input);
    }
    tx.put_applied_lsn(Lsn::from(100u64)).unwrap();
    tx.commit().await.unwrap();
    last.unwrap()
}

/// Whether `key` is in the persisted tier of this partition's store.
fn durable(store: &PartitionStore, key: &[u8]) -> bool {
    let db = store.partition_db();
    let mut options = ReadOptions::default();
    options.set_read_tier(rocksdb::ReadTier::Persisted);
    db.rocksdb()
        .inner()
        .as_raw_db()
        .get_pinned_cf_opt(db.cf_handle(), key, &options)
        .unwrap()
        .is_some()
}

/// The keys of `kind` in this partition's store.
fn keys(store: &PartitionStore, kind: KeyKind) -> Vec<(Vec<u8>, Vec<u8>)> {
    let db = store.partition_db();
    let mut options = ReadOptions::default();
    options.set_total_order_seek(true);
    options.set_iterate_range(rocksdb::PrefixRange(kind.as_bytes()));
    let mut it = db
        .rocksdb()
        .inner()
        .as_raw_db()
        .raw_iterator_cf_opt(db.cf_handle(), options);
    it.seek_to_first();
    let mut result = Vec::new();
    while let Some((key, value)) = it.item() {
        result.push((key.to_vec(), value.to_vec()));
        it.next();
    }
    it.status().unwrap();
    result
}

/// Every non-empty gauge, by definition and key payload: comparable across
/// replicas whatever physical IDs their registries issued.
fn read_all(store: &PartitionStore) -> BTreeMap<(String, Vec<u8>), StageStatusCounts> {
    let registry = get_registry_from_partition_db(store.partition_db())
        .unwrap()
        .unwrap_or_default();
    keys(store, KeyKind::RegisteredStats)
        .into_iter()
        .filter_map(|(key, value)| {
            let (prefix, kind, payload) = RegisteredStatKeyPrefix::decode(&key).unwrap();
            assert_eq!(prefix.partition_id(), store.partition_id());
            assert_eq!(kind, StatKind::StageStatusBucketedGauge);
            let counts = StageStatusCounts::deserialize_from(&value).unwrap();
            let definition = registry.get(prefix.id()).unwrap().definition.to_string();
            (!counts.is_empty()).then(|| ((definition, payload.to_vec()), counts))
        })
        .collect()
}

fn ids(store: &PartitionStore) -> Vec<(u32, RegistrationState)> {
    let registry = get_registry_from_partition_db(store.partition_db())
        .unwrap()
        .unwrap_or_default();
    [
        "service_stats/1",
        "deployment_stats/1",
        "virtual_object_stats/1",
    ]
    .map(|name| {
        let instance = registry.active(name).unwrap();
        (instance.id, instance.state)
    })
    .to_vec()
}

fn is_not_ready(err: &StorageError) -> bool {
    matches!(err, StorageError::Generic(err) if err.downcast_ref::<NotReady>().is_some())
}

/// The totals of the three statistics tables, which a store read and a snapshot
/// read must agree on. `None` when they fail because the gauges are not Ready.
async fn totals(store: &PartitionStore) -> Option<[u64; 3]> {
    let (snapshot, _) = store.query_read(CancellationToken::new()).unwrap();
    macro_rules! total {
        ($binding:ty) => {{
            let prepared = <$binding>::prepare(&Selection::All).unwrap();
            let count = Arc::new(AtomicU64::new(0));
            let read = match <$binding>::read(store, &prepared, {
                let count = Arc::clone(&count);
                move |row| {
                    count.fetch_add(row.num_entries, Ordering::Relaxed);
                    ControlFlow::Continue(())
                }
            }) {
                Ok(read) => read.await.map(|()| count.load(Ordering::Relaxed)),
                Err(err) => Err(err),
            };
            let mut count = 0;
            let scanned = <crate::ReadSnapshot as BlockingScan<
                <$binding as SchemaBinding>::Table,
            >>::scan_blocking(
                &snapshot,
                Arc::clone(prepared.request()),
                |record| {
                    <$binding>::materialize(record, &mut |row| {
                        count += row.num_entries;
                        ControlFlow::Continue(())
                    })
                },
            )
            .map(|()| count);
            match (read, scanned) {
                (Ok(read), Ok(scanned)) => {
                    assert_eq!(read, scanned, "store_and_snapshot_reads_agree");
                    read
                }
                (Err(read), Err(scanned)) => {
                    assert!(
                        is_not_ready(&read) && is_not_ready(&scanned),
                        "{read:?}, {scanned:?}"
                    );
                    return None;
                }
                other => panic!("store and snapshot reads disagree: {other:?}"),
            }
        }};
    }
    Some([
        total!(ServiceStatsBinding),
        total!(DeploymentStatsBinding),
        total!(VirtualObjectStatsBinding),
    ])
}

async fn open(store: &mut PartitionStore, config: &Configuration) {
    // Requested completion finishes at open.
    let mut config = config.clone();
    config.worker.storage.enable_eager_migrations_on_startup = true;
    store
        .verify_and_run_migrations(CancellationToken::new(), &config)
        .await
        .unwrap();
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_gauges_match_live_maintenance_and_rebuild_after_reset() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let partition = Partition::new(PartitionId::MIN, KeyRange::FULL);
    let mut store = manager.open(&partition, None).await.unwrap();
    let mut reference = manager
        .open(&Partition::new(PartitionId::from(2), KeyRange::FULL), None)
        .await
        .unwrap();

    // The reference maintains its gauges before any entry exists; the store
    // completes its gauges over existing entries.
    let mut config = Configuration::default();
    open(&mut store, &config).await;
    config.common.experimental.set_track_status_gauges(true);
    open(&mut reference, &config).await;
    let (last_id, last_status) = seed(&mut store).await;
    seed(&mut reference).await;
    assert!(read_all(&store).is_empty());
    assert!(!read_all(&reference).is_empty());
    assert_eq!(totals(&store).await, None, "absent_gauges_are_not_ready");
    assert_eq!(
        totals(&reference).await,
        None,
        "maintained_gauges_are_not_ready"
    );

    config.common.experimental.set_complete_status_gauges(true);
    for store in [&mut store, &mut reference] {
        open(store, &config).await;
        assert!(
            ids(store)
                .iter()
                .all(|(_, state)| *state == RegistrationState::Ready)
        );
    }
    let expected = read_all(&reference);
    assert_eq!(read_all(&store), expected, "completion_matches_maintenance");
    // Per partition key: 48 invocations and one mutation, 32 pinned invocations,
    // and 24 virtual-object invocations and one mutation (workflows excluded).
    assert_eq!(totals(&store).await, Some([147, 96, 75]));
    assert_eq!(totals(&reference).await, Some([147, 96, 75]));

    // Live changes apply to the completed counts. The last entry is an unpinned
    // service invocation.
    for store in [&mut store, &mut reference] {
        let mut tx = store.transaction();
        let header = tx
            .get_vqueue_entry_status(&BaseEntryId::from(last_id))
            .await
            .unwrap()
            .unwrap();
        let qid = VQueueId::custom(last_id.partition_key(), "backfill");
        tx.delete_vqueue_entry_status(
            &EntryContext {
                qid: &qid,
                target: &last_status.invocation_target().unwrap().entry_target_ref(),
            },
            StoredEntryRef::from_header(&header),
        );
        tx.delete_invocation_status(&last_id).unwrap();
        tx.commit().await.unwrap();
    }
    assert_eq!(read_all(&store), read_all(&reference));
    assert_eq!(totals(&store).await, Some([146, 96, 75]));

    // Reopening keeps the Ready gauges.
    let completed = ids(&store);
    store.partition_db().flush_memtables(true).await.unwrap();
    drop(store);
    manager.close_partition_store(partition.id()).await;
    let mut store = manager.open(&partition, None).await.unwrap();
    open(&mut store, &config).await;
    assert_eq!(ids(&store), completed);
    assert_eq!(read_all(&store), read_all(&reference));

    // Reset deletes the gauges, and queries fail instead of reading nothing.
    config.common.experimental.set_status_gauges_reset(true);
    open(&mut store, &config).await;
    assert!(keys(&store, KeyKind::RegisteredStats).is_empty());
    assert_eq!(totals(&store).await, None, "reset_gauges_are_not_ready");

    // Turned off again, the gauges are rebuilt under fresh IDs, which differ
    // from the reference replica's without changing what queries read.
    config.common.experimental.set_status_gauges_reset(false);
    open(&mut store, &config).await;
    let rebuilt = ids(&store);
    assert!(
        rebuilt
            .iter()
            .zip(&completed)
            .all(|(new, old)| new.0 > old.0)
    );
    assert_ne!(rebuilt, ids(&reference));
    assert_eq!(read_all(&store), read_all(&reference));
    assert_eq!(totals(&store).await, Some([146, 96, 75]));
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_gauges_are_deleted_before_any_instance_is_built() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut config = Configuration::default();
    config.common.experimental.set_complete_status_gauges(true);
    let legacy = |partition: PartitionId, id: u16| {
        let mut key = StatKeyPrefix::new(partition, id, StatKind::StageStatusBucketedGauge)
            .as_bytes()
            .to_vec();
        key.extend_from_slice(b"payload");
        key
    };

    // A marker only (after a reset by the legacy binary), keys only (an
    // interrupted legacy backfill), both, and neither. Legacy gauges are the
    // static IDs 1–3 of the partition.
    for (case, (marker, legacy_keys)) in
        [(true, false), (false, true), (true, true), (false, false)]
            .into_iter()
            .enumerate()
    {
        let partition = Partition::new(PartitionId::from(10 + case as u16), KeyRange::FULL);
        let mut store = manager.open(&partition, None).await.unwrap();
        // Gauges already registered by an earlier open, and an unknown feature.
        open(&mut store, &Configuration::default()).await;
        seed(&mut store).await;
        open(&mut store, &config).await;
        let registered = ids(&store);
        let expected = read_all(&store);
        let mut ledger = serde_json::to_value(
            get_storage_features_from_partition_db(store.partition_db())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        ledger["features"]["future-feature"] = serde_json::json!({"min_required_version": "1.8.0"});
        let kept = ledger.clone();
        if marker {
            ledger["features"]["status-gauges"] = serde_json::json!({
                "min_required_version": *restate_types::RESTATE_VERSION_1_8_2,
            });
        }
        put_storage_features_json(
            &mut store,
            partition.id(),
            &serde_json::to_vec(&ledger).unwrap(),
        )
        .await
        .unwrap();
        let raw = store.partition_db().rocksdb().inner().as_raw_db();
        let cf = store.partition_db().cf_handle();
        let legacy_ids = if legacy_keys {
            vec![StatId::ServiceStats.as_u16(), 2, 3]
        } else {
            Vec::new()
        };
        for &id in &legacy_ids {
            raw.put_cf(cf, legacy(partition.id(), id), b"value")
                .unwrap();
        }
        // Another static ID of this partition, and a gauge of another partition.
        let kept_keys = [
            legacy(partition.id(), 1023),
            legacy(PartitionId::from(11 + case as u16), 1),
        ];
        for key in &kept_keys {
            raw.put_cf(cf, key, b"value").unwrap();
        }
        store.partition_db().flush_memtables(true).await.unwrap();
        assert!(
            legacy_ids
                .iter()
                .all(|id| durable(&store, &legacy(partition.id(), *id)))
        );
        let min_version = get_min_restate_version_from_partition_db(store.partition_db()).unwrap();

        let mut reopened = Configuration::default();
        reopened
            .common
            .experimental
            .set_complete_status_gauges(true);
        open(&mut store, &reopened).await;
        let ledger = serde_json::to_value(
            get_storage_features_from_partition_db(store.partition_db())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(ledger, kept, "only_the_marker_is_removed: case {case}");
        assert!(!store.storage_features().is_track_status_gauges);
        assert_eq!(
            keys(&store, KeyKind::Stats),
            kept_keys.map(|key| (key, b"value".to_vec())),
            "only_ids_1_to_3_of_this_partition_are_deleted: case {case}"
        );
        assert!(
            legacy_ids
                .iter()
                .all(|id| !durable(&store, &legacy(partition.id(), *id))),
            "durably: case {case}"
        );
        assert_eq!(
            get_min_restate_version_from_partition_db(store.partition_db()).unwrap(),
            min_version
        );
        assert_eq!(ids(&store), registered, "instances_are_kept: case {case}");
        assert_eq!(read_all(&store), expected);
        assert!(
            get_registry_from_partition_db(store.partition_db())
                .unwrap()
                .is_some_and(|registry: Registry| registry.entries().len() == 3)
        );
    }
}
