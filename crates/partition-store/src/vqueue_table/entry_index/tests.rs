// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rocksdb::ReadOptions;
use tokio_util::sync::CancellationToken;

use restate_clock::UniqueTimestamp;
use restate_clock::time::MillisSinceEpoch;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::vqueue_table::stats::EntryStatistics;
use restate_storage_api::vqueue_table::{
    EntryKey, EntryMetadata, ReadVQueueTable, Stage, Status, StoredEntryRef, WriteVQueueTable,
};
use restate_types::config::Configuration;
use restate_types::identifiers::PartitionId;
use restate_types::partitions::Partition;
use restate_types::sharding::KeyRange;
use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef, HandlerRef, VQueueId};

use crate::index::SecondaryIndexKey;
use crate::keys::header::IndexScope;
use crate::keys::{IndexKeyPrefix, KeyKind};
use crate::registry::IndexStatus;
use crate::registry::records::get_registry;
use crate::stats::aggregated::backfill::tests::seed;
use crate::walk::engine;
use crate::{PartitionStore, PartitionStoreManager};

use super::*;

/// Opens as a partition does, then runs the walks the open started to their
/// ends, as the background driver does in time.
async fn open(store: &mut PartitionStore, config: &Configuration) {
    store
        .verify_and_run_migrations(CancellationToken::new(), config)
        .await
        .unwrap();
    engine::run_all(store, 3).await.unwrap();
}

/// The maintained index of `entry_by_created/1`, which holds every entry: its
/// ID and status.
fn index(store: &PartitionStore) -> Option<(u32, IndexStatus)> {
    get_registry(store.partition_db())
        .unwrap()
        .unwrap_or_default()
        .maintained(ENTRY_BY_CREATED.name)
        .map(|index| (index.id, index.state))
}

/// The keys of every secondary index in this partition's store.
fn index_keys(store: &PartitionStore) -> Vec<Vec<u8>> {
    index_entries(store)
        .into_iter()
        .map(|(key, _)| key)
        .collect()
}

/// An index key and its value.
type Entry = (Vec<u8>, Vec<u8>);

/// The keys and values of every secondary index in this partition's store.
fn index_entries(store: &PartitionStore) -> Vec<Entry> {
    let db = store.partition_db();
    let mut options = ReadOptions::default();
    options.set_total_order_seek(true);
    options.set_iterate_range(rocksdb::PrefixRange(KeyKind::SecondaryIndex.as_bytes()));
    let mut it = db
        .rocksdb()
        .inner()
        .as_raw_db()
        .raw_iterator_cf_opt(db.cf_handle(), options);
    it.seek_to_first();
    let mut entries = Vec::new();
    while let Some(key) = it.key() {
        entries.push((key.to_vec(), it.value().unwrap().to_vec()));
        it.next();
    }
    it.status().unwrap();
    entries
}

/// The entry indexes' keys without their index header, with their values, by
/// definition: comparable across replicas whatever partition and index IDs they
/// have.
fn payloads(store: &PartitionStore) -> Vec<(&'static str, Vec<Entry>)> {
    let registry = get_registry(store.partition_db())
        .unwrap()
        .unwrap_or_default();
    ENTRY_INDEXES
        .definitions
        .iter()
        .map(|definition| {
            let keys = registry
                .maintained(definition.name)
                .map_or_else(Vec::new, |index| {
                    entries_of(store, index.id)
                        .into_iter()
                        .map(|(key, value)| {
                            (key[IndexKeyPrefix::SERIALIZED_LENGTH..].to_vec(), value)
                        })
                        .collect()
                });
            (definition.name, keys)
        })
        .collect()
}

/// How many keys each entry index holds, by definition.
fn counts(store: &PartitionStore) -> Vec<(&'static str, usize)> {
    payloads(store)
        .into_iter()
        .map(|(name, keys)| (name, keys.len()))
        .collect()
}

fn tracking() -> Configuration {
    let mut config = Configuration::default();
    config.common.experimental.set_indexes(true);
    config
}

/// `entry_by_created/1` keys every entry by creation hour, newest first, then
/// service and newest created first, each with its handler and scope as the
/// value. Stage and status changes don't write the index; deleting the entry
/// removes its key.
#[restate_core::test]
async fn keys_order_entries_and_change_only_on_create_and_delete() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    // An empty partition has nothing to backfill: the index is Complete at once.
    open(&mut store, &tracking()).await;
    let (id, status) = index(&store).unwrap();
    assert_eq!(status, IndexStatus::Complete);

    let qid = VQueueId::custom(7, "q");
    let call = EntryTargetRef::Service {
        scope: None,
        service: "Utils",
        handler: "call",
    };
    let run = EntryTargetRef::Service {
        scope: Some("tenant-a"),
        service: "Utils",
        handler: "run",
    };
    let mutation = EntryTargetRef::VirtualObject {
        scope: None,
        service: "Utils",
        key: "k",
        handler: HandlerRef::StateMutation,
    };
    let other = EntryTargetRef::Service {
        scope: None,
        service: "Other",
        handler: "call",
    };
    // (target, kind, id byte, created at)
    let entries = [
        (&call, EntryKind::Invocation, 1, 1_000),
        (&call, EntryKind::Invocation, 2, 3_000),
        (&run, EntryKind::Invocation, 3, 2_000),
        (&mutation, EntryKind::StateMutation, 4, 500),
        // Two hours later than the others.
        (&other, EntryKind::Invocation, 5, 7_209_000),
    ]
    .map(|(target, kind, byte, created)| {
        let created = UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(
            1_744_010_000_000 + created,
        ))
        .unwrap();
        let key = EntryKey::new(
            false,
            created.to_unix_millis(),
            u64::from(byte),
            EntryId::new(kind, [byte; 16]),
        );
        (target, key, EntryStatistics::new(created, key.run_at()))
    });
    let mut tx = store.transaction();
    for (target, key, stats) in &entries {
        tx.create_vqueue_entry_status(
            &EntryContext { qid: &qid, target },
            EntryStateRef {
                stage: Stage::Inbox,
                status: Status::New,
                entry_key: key,
                metadata: &EntryMetadata::default(),
                stats,
            },
        );
    }
    tx.commit().await.unwrap();

    let created_id = id;
    let expected_created = |order: &[usize]| {
        order
            .iter()
            .map(|&i| {
                let (target, key, stats) = &entries[i];
                let mut bytes = Vec::new();
                EntryByCreatedKey::borrowed(
                    Reverse(hour_bucket(stats.created_at)),
                    target.service(),
                    Reverse(stats.created_at),
                    key.to_canonical_entry_id(qid.partition_key()),
                )
                .encode_key(
                    IndexScope {
                        partition: PartitionId::MIN,
                        index: created_id,
                    },
                    &mut bytes,
                );
                let value = (
                    target.handler().map(str::to_owned),
                    target.scope().map(str::to_owned),
                );
                (bytes, value)
            })
            .collect::<Vec<_>>()
    };
    let created_entries = |store: &PartitionStore| {
        entries_of(store, created_id)
            .into_iter()
            .map(|(key, value)| {
                let value = EntryTargetValueRef::decode(&value).unwrap();
                let value = (
                    value.handler.map(str::to_owned),
                    value.scope.map(str::to_owned),
                );
                (key, value)
            })
            .collect::<Vec<_>>()
    };
    // The entry two hours later leads; the others share an hour and one
    // service, newest first, with the state mutation's handler NULL and the
    // scoped target's scope in the value.
    assert_eq!(created_entries(&store), expected_created(&[4, 1, 2, 0, 3]));

    // Running an entry changes its stage, status and transition time, not its key.
    let mut tx = store.transaction();
    let (target, key, stats) = &entries[0];
    let mut moved = stats.clone();
    moved.transitioned_at =
        UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_744_020_000_000)).unwrap();
    let header = tx
        .get_vqueue_entry_status(&key.entry_id().to_base_id(qid.partition_key()))
        .await
        .unwrap()
        .unwrap();
    tx.update_vqueue_entry_status(
        &EntryContext { qid: &qid, target },
        StoredEntryRef::from_header(&header),
        EntryStateRef {
            stage: Stage::Running,
            status: Status::Started,
            entry_key: key,
            metadata: &EntryMetadata::default(),
            stats: &moved,
        },
    );
    tx.commit().await.unwrap();
    assert_eq!(created_entries(&store), expected_created(&[4, 1, 2, 0, 3]));

    // Deleting an entry removes exactly its key.
    let mut tx = store.transaction();
    let (target, key, _) = &entries[1];
    let header = tx
        .get_vqueue_entry_status(&key.entry_id().to_base_id(qid.partition_key()))
        .await
        .unwrap()
        .unwrap();
    tx.delete_vqueue_entry_status(
        &EntryContext { qid: &qid, target },
        StoredEntryRef::from_header(&header),
    );
    tx.commit().await.unwrap();
    assert_eq!(created_entries(&store), expected_created(&[4, 2, 0, 3]));
}

/// Backfilling existing entries gives the same keys as maintaining the indexes
/// from the start: invocations through their invocation status, state
/// mutations through their input. A reset deletes the keys; tracking again
/// rebuilds them under new IDs. Values are compared too.
#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn backfill_matches_live_maintenance_and_rebuilds_after_reset() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    let mut reference = manager
        .open(&Partition::new(PartitionId::from(2), KeyRange::FULL), None)
        .await
        .unwrap();

    // The reference indexes from the start; the store only after the entries exist.
    open(&mut store, &Configuration::default()).await;
    open(&mut reference, &tracking()).await;
    seed(&mut store).await;
    seed(&mut reference).await;
    unusual(&mut store).await;
    unusual(&mut reference).await;
    assert_eq!(index(&store), None);
    assert!(index_keys(&store).is_empty());

    let mut config = tracking();
    open(&mut store, &config).await;
    let (built, status) = index(&store).unwrap();
    assert_eq!(status, IndexStatus::Complete);
    // 48 invocations and a state mutation on each of three partition keys, and
    // the two unusual entries: every one in the creation index, all but the one
    // in an unknown stage in the two status indexes.
    let [created, status_created, status_transitioned] = <[_; 3]>::try_from(counts(&reference))
        .unwrap()
        .map(|(_, count)| count);
    assert_eq!(
        (created, status_created, status_transitioned),
        (149, 148, 148)
    );
    assert_eq!(payloads(&store), payloads(&reference));

    // Turning indexes off deletes the keys and the maintained index.
    config.common.experimental.set_indexes(false);
    open(&mut store, &config).await;
    assert_eq!(index(&store), None);
    assert!(index_keys(&store).is_empty());

    // Turning them on again rebuilds the same keys under a new ID.
    config.common.experimental.set_indexes(true);
    open(&mut store, &config).await;
    let (rebuilt, status) = index(&store).unwrap();
    assert_eq!(status, IndexStatus::Complete);
    assert!(rebuilt > built);
    assert_eq!(payloads(&store), payloads(&reference));
}

/// Two entries the shared seed lacks, with resolvable invocations: one Finished
/// with a status that isn't an outcome, and one in a stage this binary doesn't
/// know.
async fn unusual(store: &mut PartitionStore) {
    use restate_storage_api::invocation_status_table::{
        InFlightInvocationMetadata, InvocationStatus, WriteInvocationStatusTable,
    };
    use restate_types::identifiers::{InvocationId, InvocationUuid};
    use restate_types::invocation::InvocationTarget;
    use restate_types::vqueues::EntryTargetExt;

    let qid = VQueueId::custom(13, "unusual");
    let mut tx = store.transaction();
    for (byte, stage, status) in [
        (1u8, Stage::Finished, Status::Started),
        (2, Stage::Unknown, Status::New),
    ] {
        let id = InvocationId::from_parts(13, InvocationUuid::from_bytes([byte; 16]));
        let target = InvocationTarget::service("unusual", "run");
        let mut metadata = InFlightInvocationMetadata::mock_with_vqueue(13);
        metadata.vqueue_id = Some(qid.clone());
        metadata.invocation_target = target.clone();
        tx.put_invocation_status(&id, &InvocationStatus::Invoked(metadata))
            .unwrap();
        let created = UniqueTimestamp::try_from_parts(200, u16::from(byte).into()).unwrap();
        let key = EntryKey::new(
            false,
            created.to_unix_millis(),
            u64::from(byte),
            EntryId::from(id),
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
                metadata: &EntryMetadata::default(),
                stats: &EntryStatistics::new(created, key.run_at()),
            },
        );
    }
    tx.commit().await.unwrap();
}

/// The ID of the maintained index of `definition`.
fn maintained(store: &PartitionStore, definition: &IndexDefinition) -> u32 {
    get_registry(store.partition_db())
        .unwrap()
        .unwrap()
        .maintained(definition.name)
        .unwrap()
        .id
}

/// The keys of index `id`, in storage order.
fn keys_of(store: &PartitionStore, id: u32) -> Vec<Vec<u8>> {
    entries_of(store, id)
        .into_iter()
        .map(|(key, _)| key)
        .collect()
}

/// The keys of index `id` with their values, in storage order.
fn entries_of(store: &PartitionStore, id: u32) -> Vec<Entry> {
    index_entries(store)
        .into_iter()
        .filter(|(key, _)| IndexKeyPrefix::decode_prefix(key).unwrap().0.index_id_raw() == id)
        .collect()
}

/// The indexes keyed by stage and status key each entry as its stored state
/// says: entries in every known stage by stage, status, hour and service,
/// newest first, once by creation and once by transition, with the handler and
/// scope as the value. A move between
/// stages, or a new status, moves or removes the key once; an entry in an
/// unknown stage has no key in any of them, and leaving or entering an unknown
/// stage adds or removes its keys once.
#[restate_core::test]
async fn stage_and_status_keys_follow_each_entry() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    open(&mut store, &tracking()).await;
    let [all, status_created, status_transitioned] = [
        &ENTRY_BY_CREATED,
        &ENTRY_BY_STATUS_CREATED,
        &ENTRY_BY_STATUS_TRANSITIONED,
    ]
    .map(|definition| maintained(&store, definition));

    let qid = VQueueId::custom(7, "q");
    let call = EntryTargetRef::Service {
        scope: None,
        service: "Utils",
        handler: "call",
    };
    let run = EntryTargetRef::Service {
        scope: Some("tenant-a"),
        service: "Utils",
        handler: "run",
    };
    let mutation = EntryTargetRef::VirtualObject {
        scope: None,
        service: "Utils",
        key: "k",
        handler: HandlerRef::StateMutation,
    };
    let other = EntryTargetRef::Service {
        scope: None,
        service: "Other",
        handler: "call",
    };
    let at = |millis: u64| {
        UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_744_010_000_000 + millis))
            .unwrap()
    };
    // (target, kind, id byte, created at, stage, status, transitioned at)
    let entries = [
        (
            &call,
            EntryKind::Invocation,
            1,
            1_000,
            Stage::Inbox,
            Status::New,
            1_000,
        ),
        (
            &call,
            EntryKind::Invocation,
            2,
            3_000,
            Stage::Running,
            Status::Started,
            3_500,
        ),
        (
            &run,
            EntryKind::Invocation,
            3,
            2_000,
            Stage::Suspended,
            Status::Started,
            2_500,
        ),
        (
            &mutation,
            EntryKind::StateMutation,
            4,
            500,
            Stage::Paused,
            Status::New,
            600,
        ),
        // Finished two hours after it was created.
        (
            &other,
            EntryKind::Invocation,
            5,
            9_000,
            Stage::Finished,
            Status::Succeeded,
            7_300_000,
        ),
        // A Finished entry with a status that isn't an outcome still belongs.
        (
            &call,
            EntryKind::Invocation,
            6,
            4_000,
            Stage::Finished,
            Status::Started,
            8_000,
        ),
        (
            &run,
            EntryKind::Invocation,
            7,
            5_000,
            Stage::Unknown,
            Status::New,
            5_000,
        ),
        // Created and finished two hours later than the others.
        (
            &call,
            EntryKind::Invocation,
            8,
            7_209_000,
            Stage::Finished,
            Status::Succeeded,
            7_210_000,
        ),
    ]
    .map(
        |(target, kind, byte, created, stage, status, transitioned)| {
            let key = EntryKey::new(
                false,
                at(created).to_unix_millis(),
                u64::from(byte),
                EntryId::new(kind, [byte; 16]),
            );
            let mut stats = EntryStatistics::new(at(created), key.run_at());
            stats.transitioned_at = at(transitioned);
            (target, key, stats, stage, status)
        },
    );
    let mut tx = store.transaction();
    for (target, key, stats, stage, status) in &entries {
        tx.create_vqueue_entry_status(
            &EntryContext { qid: &qid, target },
            EntryStateRef {
                stage: *stage,
                status: *status,
                entry_key: key,
                metadata: &EntryMetadata::default(),
                stats,
            },
        );
    }
    tx.commit().await.unwrap();

    let canonical = |i: usize| entries[i].1.to_canonical_entry_id(qid.partition_key());
    // The status indexes' entries: the key, then the target's handler and scope.
    let status_entry = |index: u32, key: &dyn Fn(&mut Vec<u8>, IndexScope), i: usize| {
        let target = entries[i].0;
        let (mut bytes, mut value) = (Vec::new(), Vec::new());
        key(
            &mut bytes,
            IndexScope {
                partition: PartitionId::MIN,
                index,
            },
        );
        EntryTargetValueRef {
            handler: target.handler(),
            scope: target.scope(),
        }
        .encode(&mut value);
        (bytes, value)
    };
    let by_created = |stage: Stage, status: Status, i: usize| {
        let created_at = entries[i].2.created_at;
        let key = |bytes: &mut Vec<u8>, scope| {
            EntryByStatusCreatedKey::borrowed(
                stage,
                status,
                Reverse(hour_bucket(created_at)),
                entries[i].0.service(),
                Reverse(created_at),
                canonical(i),
            )
            .encode_key(scope, bytes)
        };
        status_entry(status_created, &key, i)
    };
    let by_transitioned = |stage: Stage, status: Status, i: usize, transitioned: u64| {
        let key = |bytes: &mut Vec<u8>, scope| {
            EntryByStatusTransitionedKey::borrowed(
                stage,
                status,
                Reverse(hour_bucket(at(transitioned))),
                entries[i].0.service(),
                Reverse(at(transitioned)),
                canonical(i),
            )
            .encode_key(scope, bytes)
        };
        status_entry(status_transitioned, &key, i)
    };
    assert_eq!(
        keys_of(&store, all).len(),
        8,
        "every entry is in the creation index"
    );
    // Stages in rank order, then statuses in codec order, then hours newest
    // first, then services. Among the succeeded: by creation, the entry created
    // two hours later leads; by transition both finished in that hour, so
    // "Other" sorts first. No key for the entry in an unknown stage.
    assert_eq!(
        entries_of(&store, status_created),
        [
            by_created(Stage::Finished, Status::Started, 5),
            by_created(Stage::Finished, Status::Succeeded, 7),
            by_created(Stage::Finished, Status::Succeeded, 4),
            by_created(Stage::Inbox, Status::New, 0),
            by_created(Stage::Paused, Status::New, 3),
            by_created(Stage::Running, Status::Started, 1),
            by_created(Stage::Suspended, Status::Started, 2),
        ]
    );
    assert_eq!(
        entries_of(&store, status_transitioned),
        [
            by_transitioned(Stage::Finished, Status::Started, 5, 8_000),
            by_transitioned(Stage::Finished, Status::Succeeded, 4, 7_300_000),
            by_transitioned(Stage::Finished, Status::Succeeded, 7, 7_210_000),
            by_transitioned(Stage::Inbox, Status::New, 0, 1_000),
            by_transitioned(Stage::Paused, Status::New, 3, 600),
            by_transitioned(Stage::Running, Status::Started, 1, 3_500),
            by_transitioned(Stage::Suspended, Status::Started, 2, 2_500),
        ]
    );

    // Moves: Inbox → Running, Running → Finished (killed), Unknown → Inbox,
    // Suspended → Unknown, and a new status in the same stage, Paused. Then the
    // Finished "other" entry is deleted.
    let moves = [
        (0, Stage::Running, Status::Started, 1_100),
        (1, Stage::Finished, Status::Killed, 7_000),
        (6, Stage::Inbox, Status::New, 5_100),
        (2, Stage::Unknown, Status::New, 2_600),
        (3, Stage::Paused, Status::BackingOff, 600),
    ];
    let mut tx = store.transaction();
    for (i, stage, status, transitioned) in moves {
        let (target, key, stats, ..) = &entries[i];
        let header = tx
            .get_vqueue_entry_status(&key.entry_id().to_base_id(qid.partition_key()))
            .await
            .unwrap()
            .unwrap();
        let mut moved = stats.clone();
        moved.transitioned_at = at(transitioned);
        tx.update_vqueue_entry_status(
            &EntryContext { qid: &qid, target },
            StoredEntryRef::from_header(&header),
            EntryStateRef {
                stage,
                status,
                entry_key: key,
                metadata: &EntryMetadata::default(),
                stats: &moved,
            },
        );
    }
    let (target, key, ..) = &entries[4];
    let header = tx
        .get_vqueue_entry_status(&key.entry_id().to_base_id(qid.partition_key()))
        .await
        .unwrap()
        .unwrap();
    tx.delete_vqueue_entry_status(
        &EntryContext { qid: &qid, target },
        StoredEntryRef::from_header(&header),
    );
    tx.commit().await.unwrap();

    assert_eq!(keys_of(&store, all).len(), 7);
    assert_eq!(
        entries_of(&store, status_created),
        [
            by_created(Stage::Finished, Status::Killed, 1),
            by_created(Stage::Finished, Status::Started, 5),
            by_created(Stage::Finished, Status::Succeeded, 7),
            by_created(Stage::Inbox, Status::New, 6),
            by_created(Stage::Paused, Status::BackingOff, 3),
            by_created(Stage::Running, Status::Started, 0),
        ]
    );
    assert_eq!(
        entries_of(&store, status_transitioned),
        [
            by_transitioned(Stage::Finished, Status::Killed, 1, 7_000),
            by_transitioned(Stage::Finished, Status::Started, 5, 8_000),
            by_transitioned(Stage::Finished, Status::Succeeded, 7, 7_210_000),
            by_transitioned(Stage::Inbox, Status::New, 6, 5_100),
            by_transitioned(Stage::Paused, Status::BackingOff, 3, 600),
            by_transitioned(Stage::Running, Status::Started, 0, 1_100),
        ]
    );

    // A lane's selection holds only its own stage and status, whatever the
    // query selects, nothing included: read as one run per stage and status,
    // each lane is one run of its own keys.
    use crate::ordered_runs::{OrderedRunsDiscovery, OrderedRunsLimits};
    use restate_storage_query_api::scan::{CursorStep, ReadDirection};

    let (snapshot, _closed) = store.query_read(CancellationToken::new()).unwrap();
    for (stage, status, expected) in [
        (Stage::Running, Status::Started, 1),
        (Stage::Finished, Status::Killed, 1),
        (Stage::Suspended, Status::Started, 0),
    ] {
        let lane =
            EntryByStatusCreatedKey::prepare_lanes(&Selection::All, &[stage], &[status]).unwrap();
        let OrderedRunsDiscovery::Ready(mut runs) = snapshot
            .ordered_runs::<EntryByStatusCreated>(
                std::sync::Arc::new(lane),
                2,
                ReadDirection::Forward,
                OrderedRunsLimits {
                    max_runs: 8,
                    max_discovery_keys: 1_000,
                    buffer: std::num::NonZeroUsize::new(4).unwrap(),
                    step_budget: std::num::NonZeroUsize::new(64).unwrap(),
                    bucket_fields: 0,
                },
                Box::new(()),
            )
            .unwrap()
        else {
            panic!("one lane is one run");
        };
        let mut read = Vec::new();
        while runs.advance().unwrap() != CursorStep::Done {
            if let Ok((key, _)) = runs.current() {
                let key = key.decode_all().unwrap();
                read.push((key.stage, key.status));
            }
        }
        assert_eq!(
            read,
            vec![(stage, status); expected],
            "{stage:?} {status:?}"
        );
    }
}

/// The status indexes' stages and statuses are listed in their codecs' byte
/// order, every stage they hold and every status. An invocation query reads
/// the lanes its entries can be in: each public status the pairs whose
/// effective status tells it, plus the pairs no code path writes that may show
/// it as their stored status; failed ones Finished in any stored status but
/// Succeeded. Conditions intersect, alternatives unite, ranges and unknown
/// values don't widen past what they can match.
#[test]
fn invocation_conditions_select_the_lanes_their_entries_can_be_in() {
    use restate_storage_query_api::tables::index::EntryByStatusCreatedClause::{
        CompletionResult, Status as Public,
    };
    use restate_util_string::ReString;
    use strum::VariantArray;

    use crate::keys::FieldCodec;

    let encoded = |encode: &dyn Fn(&mut Vec<u8>)| {
        let mut bytes = Vec::new();
        encode(&mut bytes);
        bytes
    };
    let stages = KEYED_STAGES.map(|stage| encoded(&|b| StageRankCodec::encode_ref(&stage, b)));
    assert!(stages.is_sorted_by(|a, b| a < b));
    assert_eq!(
        KEYED_STAGES.len(),
        Stage::VARIANTS
            .iter()
            .filter(|stage| status_indexes_hold(**stage))
            .count()
    );
    let statuses = KEYED_STATUSES.map(|status| encoded(&|b| StatusCodec::encode_ref(&status, b)));
    assert!(statuses.is_sorted_by(|a, b| a < b));
    assert_eq!(KEYED_STATUSES.len(), Status::VARIANTS.len());

    let lanes = |selection: Selection<InvocationsByStatus>| {
        EntryByStatusCreatedKey::lanes(&selection).collect::<Vec<_>>()
    };
    let public = |values: &[&str]| {
        Public(ValuePredicate::In(
            values.iter().copied().map(ReString::from).collect(),
        ))
    };
    let result =
        |value: Option<&str>| CompletionResult(ValuePredicate::Equal(value.map(ReString::from)));
    let finished = |except: &[Status]| {
        KEYED_STATUSES
            .into_iter()
            .filter(|status| !except.contains(status))
            .map(|status| (Stage::Finished, status))
            .collect::<Vec<_>>()
    };

    assert_eq!(lanes(Selection::All).len(), 50);
    assert!(lanes(Selection::Empty).is_empty());
    assert_eq!(
        lanes(Selection::All.and(public(&["completed"]))),
        finished(&[])
    );
    let failed = finished(&[Status::Succeeded]);
    assert_eq!(lanes(Selection::All.and(result(Some("failure")))), failed);
    assert_eq!(
        lanes(
            Selection::All
                .and(public(&["completed"]))
                .and(result(Some("failure")))
        ),
        failed
    );
    // Each status's lanes, written out: what its effective statuses come from,
    // and the inbox pairs no code path writes, which show their stored status.
    let every = |stage| KEYED_STATUSES.map(|status| (stage, status)).to_vec();
    let inbox = |statuses: &[Status]| {
        let unwritten = [
            Status::Cancelled,
            Status::Failed,
            Status::Killed,
            Status::Succeeded,
            Status::Unknown,
        ];
        let mut pairs: Vec<_> = statuses
            .iter()
            .chain(&unwritten)
            .map(|status| (Stage::Inbox, *status))
            .collect();
        pairs.sort_by_key(|&(_, status)| status_bit(status));
        pairs
    };
    let expected = [
        ("inboxed", inbox(&[Status::New])),
        ("scheduled", inbox(&[Status::Scheduled])),
        (
            "invoked",
            [
                inbox(&[Status::BackingOff, Status::Started, Status::Yielded]),
                every(Stage::Running),
            ]
            .concat(),
        ),
        ("suspended", [inbox(&[]), every(Stage::Suspended)].concat()),
        ("paused", [inbox(&[]), every(Stage::Paused)].concat()),
        ("completed", finished(&[])),
    ];
    for (status, pairs) in &expected {
        assert_eq!(
            &lanes(Selection::All.and(public(&[status]))),
            pairs,
            "{status}"
        );
    }
    // Each result's lanes: a success is only a succeeded entry, a failure a
    // failed, killed or cancelled one, and an invocation without a result any
    // that hasn't finished; Finished pairs no code path writes may show either
    // stored result.
    let results = [
        (
            Some("success"),
            finished(&[Status::Cancelled, Status::Failed, Status::Killed]),
        ),
        (Some("failure"), finished(&[Status::Succeeded])),
        (
            None,
            [
                inbox(&[
                    Status::BackingOff,
                    Status::New,
                    Status::Scheduled,
                    Status::Started,
                    Status::Yielded,
                ]),
                every(Stage::Paused),
                every(Stage::Running),
                every(Stage::Suspended),
            ]
            .concat(),
        ),
    ];
    for (value, pairs) in &results {
        assert_eq!(
            &lanes(Selection::All.and(result(*value))),
            pairs,
            "{value:?}"
        );
    }
    // The same as the shared mapping: a written pair is in the lanes of the
    // status and the result its effective status tells, and of no other.
    for (stage, stored) in lanes(Selection::All) {
        let effective = EffectiveStatus::new(stage, stored);
        let Some(shown) = effective.invocation_status() else {
            continue;
        };
        for (status, pairs) in &expected {
            assert_eq!(
                pairs.contains(&(stage, stored)),
                *status == shown,
                "{stage:?} {stored:?} in {status}"
            );
        }
        for (value, pairs) in &results {
            assert_eq!(
                pairs.contains(&(stage, stored)),
                *value == effective.completion_result(),
                "{stage:?} {stored:?} in {value:?}"
            );
        }
    }
    let active = lanes(Selection::All.and(public(&["invoked", "suspended"])));
    assert_eq!(active.len(), 28);
    assert_eq!(active[0], (Stage::Inbox, Status::BackingOff));
    assert!(active.iter().all(|(stage, _)| *stage != Stage::Finished));
    assert_eq!(lanes(Selection::All.and(result(None))).len(), 40);
    // Either: every lane of either.
    assert_eq!(
        lanes(Selection::All.and(public(&["completed", "paused"]))).len(),
        25
    );
    // Contradictions and values no invocation has select nothing.
    assert!(lanes(Selection::All.and(public(&["completed"])).and(result(None))).is_empty());
    assert!(lanes(Selection::All.and(public(&["running"]))).is_empty());
    assert!(lanes(Selection::All.and(result(Some("partial")))).is_empty());
    // A range on the public status doesn't restrict it.
    assert_eq!(
        lanes(Selection::All.and(Public(ValuePredicate::Range {
            lower: Bound::Included(ReString::from("a")),
            upper: Bound::Unbounded,
        })))
        .len(),
        50
    );
}

/// A query on entries reads exactly the lanes whose entries have the effective
/// statuses it asks for, as an effective status is a function of its stored
/// stage and status: one lane for `ready`, every stored status of Paused for
/// `paused`. Stage and status conditions intersect. The pairs no code path
/// writes make the `unknown` sentinel, which no status condition names: only a
/// stage condition reads their lanes.
#[test]
fn entry_conditions_select_exactly_their_effective_lanes() {
    use restate_storage_query_api::tables::index::EntryStatusByStatusCreatedClause as Clause;

    let lanes = |selection: Selection<EntriesByStatusCreated>| {
        EntryByStatusCreatedKey::entry_lanes(&selection).collect::<Vec<_>>()
    };
    let named = |names: &[&str]| {
        Clause::Status(ValuePredicate::In(
            names.iter().map(|name| name.parse().unwrap()).collect(),
        ))
    };
    let inbox = || Clause::Stage(ValuePredicate::Equal(Stage::Inbox));

    assert_eq!(lanes(Selection::All).len(), 50);
    assert_eq!(
        lanes(Selection::All.and(named(&["ready"]))),
        [(Stage::Inbox, Status::Started)]
    );
    assert_eq!(
        lanes(Selection::All.and(named(&["paused"]))),
        KEYED_STATUSES.map(|status| (Stage::Paused, status))
    );
    assert!(lanes(Selection::All.and(inbox())).contains(&(Stage::Inbox, Status::Unknown)));
    // A condition every value satisfies, as a column without one comes,
    // restricts nothing: the sentinel's lanes stay for the caller to check.
    assert_eq!(
        lanes(Selection::All.and(Clause::Status(ValuePredicate::In(
            EffectiveStatus::VALUES.to_vec()
        ))))
        .len(),
        50
    );
    assert_eq!(
        lanes(Selection::All.and(inbox()).and(named(&["new", "running"]))),
        [(Stage::Inbox, Status::New)]
    );
    assert!(lanes(Selection::All.and(inbox()).and(named(&["failed"]))).is_empty());
    // A name no value has, such as `started`, converts to no value at all.
    assert!(lanes(Selection::All.and(Clause::Status(ValuePredicate::In(Vec::new())))).is_empty());
    // Every value reads exactly the lanes whose pairs make it.
    for wanted in EffectiveStatus::VALUES {
        let read = lanes(Selection::All.and(Clause::Status(ValuePredicate::Equal(wanted))));
        let expected: Vec<_> = KEYED_STAGES
            .into_iter()
            .flat_map(|stage| KEYED_STATUSES.map(|status| (stage, status)))
            .filter(|&(stage, status)| EffectiveStatus::new(stage, status) == wanted)
            .collect();
        assert!(!expected.is_empty(), "{wanted}");
        assert_eq!(read, expected, "{wanted}");
    }
}
