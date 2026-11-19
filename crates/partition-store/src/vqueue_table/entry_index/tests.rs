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
use restate_types::identifiers::PartitionId;
use restate_types::partitions::Partition;
use restate_types::sharding::KeyRange;
use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef, HandlerRef, Seq, VQueueId};

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

/// The maintained index of `entry_by_service_created/1`: its ID and status.
fn index(store: &PartitionStore) -> Option<(u32, IndexStatus)> {
    get_registry(store.partition_db())
        .unwrap()
        .unwrap_or_default()
        .maintained(ENTRY_BY_SERVICE_CREATED.name)
        .map(|index| (index.id, index.state))
}

/// The keys of every secondary index in this partition's store.
fn index_keys(store: &PartitionStore) -> Vec<Vec<u8>> {
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
    let mut keys = Vec::new();
    while let Some(key) = it.key() {
        keys.push(key.to_vec());
        it.next();
    }
    it.status().unwrap();
    keys
}

/// The entry indexes' keys without their index header, by definition:
/// comparable across replicas whatever partition and index IDs they have.
fn payloads(store: &PartitionStore) -> Vec<(&'static str, Vec<Vec<u8>>)> {
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
                    keys_of(store, index.id)
                        .into_iter()
                        .map(|key| key[IndexKeyPrefix::SERIALIZED_LENGTH..].to_vec())
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
    config.common.experimental.set_track_entry_indexes(true);
    config
}

/// Every entry has one key, ordered by service, then handler (NULL, a state
/// mutation, first), then newest created first. Stage and status changes don't
/// write the index; deleting the entry removes its key.
#[restate_core::test]
async fn keys_order_entries_and_change_only_on_create_and_delete() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    // An empty partition has nothing to backfill: the index is Ready at once.
    open(&mut store, &tracking()).await;
    let (id, status) = index(&store).unwrap();
    assert_eq!(status, IndexStatus::Ready);

    let qid = VQueueId::custom(7, "q");
    let call = EntryTargetRef::Service {
        scope: None,
        service: "Utils",
        handler: "call",
    };
    let run = EntryTargetRef::Service {
        scope: None,
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
        (&other, EntryKind::Invocation, 5, 9_000),
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

    let scope = IndexScope {
        partition: PartitionId::MIN,
        index: id,
    };
    let expected = |order: &[usize]| {
        order
            .iter()
            .map(|&i| {
                let (target, key, stats) = &entries[i];
                let mut bytes = Vec::new();
                EntryByServiceCreatedKey::borrowed(
                    target.service(),
                    target.handler(),
                    Reverse(stats.created_at),
                    key.to_canonical_entry_id(qid.partition_key()),
                )
                .encode_key(scope, &mut bytes);
                bytes
            })
            .collect::<Vec<_>>()
    };
    // "Other" sorts before "Utils"; within Utils the state mutation (NULL
    // handler) comes first, then "call" newest first, then "run".
    assert_eq!(keys_of(&store, id), expected(&[4, 3, 1, 0, 2]));

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
    assert_eq!(keys_of(&store, id), expected(&[4, 3, 1, 0, 2]));

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
    assert_eq!(keys_of(&store, id), expected(&[4, 3, 0, 2]));
}

/// Backfilling existing entries gives the same keys as maintaining the indexes
/// from the start: invocations through their invocation status, state
/// mutations through their input. A reset deletes the keys; tracking again
/// rebuilds them under new IDs. On a store whose first index is already Ready,
/// the two later ones get their own full pass, and the first stays as it was.
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
    assert_eq!(status, IndexStatus::Ready);
    // 48 invocations and a state mutation on each of three partition keys, each
    // in either the finished or the active index, and the two unusual entries:
    // the Finished one in the finished index, the one in an unknown stage in
    // neither.
    let [all, finished, active] = <[_; 3]>::try_from(counts(&reference))
        .unwrap()
        .map(|(_, count)| count);
    assert_eq!((all, finished + active), (149, 148));
    assert!(finished > 0 && active > 0, "{:?}", counts(&reference));
    assert_eq!(payloads(&store), payloads(&reference));

    // A reset deletes the keys and the maintained index.
    config.common.experimental.set_entry_indexes_reset(true);
    open(&mut store, &config).await;
    assert_eq!(index(&store), None);
    assert!(index_keys(&store).is_empty());

    // Tracking again rebuilds the same keys under a new ID.
    config.common.experimental.set_entry_indexes_reset(false);
    open(&mut store, &config).await;
    let (rebuilt, status) = index(&store).unwrap();
    assert_eq!(status, IndexStatus::Ready);
    assert!(rebuilt > built);
    assert_eq!(payloads(&store), payloads(&reference));

    // A store of the previous release: only the first index, Ready.
    let mut late = manager
        .open(&Partition::new(PartitionId::from(3), KeyRange::FULL), None)
        .await
        .unwrap();
    open(&mut late, &Configuration::default()).await;
    seed(&mut late).await;
    unusual(&mut late).await;
    maintain_only(&mut late, &[&ENTRY_BY_SERVICE_CREATED]).await;
    engine::run_all(&mut late, 3).await.unwrap();
    let (first, status) = index(&late).unwrap();
    assert_eq!(status, IndexStatus::Ready);
    let before = payloads(&late);
    assert_eq!(before[0], payloads(&reference)[0]);
    assert!(before[1].1.is_empty() && before[2].1.is_empty());
    // This release adds the other two: they backfill on their own, and the
    // first keeps its ID and keys.
    open(&mut late, &tracking()).await;
    assert_eq!(index(&late), Some((first, IndexStatus::Ready)));
    for definition in [&FINISHED_ENTRY_BY_OUTCOME, &ACTIVE_ENTRY_BY_STAGE] {
        let id = maintained(&late, definition);
        let registry = get_registry(late.partition_db()).unwrap().unwrap();
        assert_eq!(registry.get(id).unwrap().state, IndexStatus::Ready);
    }
    assert_eq!(payloads(&late), payloads(&reference));
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

/// Reconciles `store`'s registry to maintain only `definitions`, as an earlier
/// release that knew only those would.
async fn maintain_only(store: &mut PartitionStore, definitions: &[&'static IndexDefinition]) {
    use crate::features::LoadedStorageFeatures;
    use crate::fsm_table::{
        get_min_restate_version_from_partition_db, get_storage_features_from_partition_db,
        get_storage_version_from_partition_db,
    };
    use crate::registry::{FenceState, Requests};
    use restate_types::SemanticRestateVersion;

    let db = store.partition_db().clone();
    let mut features = LoadedStorageFeatures::load(
        get_storage_features_from_partition_db(&db).unwrap(),
        get_storage_version_from_partition_db(&db).unwrap(),
        SemanticRestateVersion::current(),
    )
    .unwrap();
    let mut min = get_min_restate_version_from_partition_db(&db).unwrap();
    store
        .reconcile_registry(
            definitions,
            &Requests {
                maintain: definitions.to_vec(),
                ..Default::default()
            },
            FenceState {
                features: &mut features,
                current_version: SemanticRestateVersion::current(),
                min_restate_version: &mut min,
            },
        )
        .await
        .unwrap();
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
    index_keys(store)
        .into_iter()
        .filter(|key| IndexKeyPrefix::decode_prefix(key).unwrap().0.index_id_raw() == id)
        .collect()
}

/// The two partial indexes key each entry as its stored state says. Finished
/// entries, whatever their stored status, by status, service, handler and
/// newest transition first; entries in the four known active stages by stage,
/// service, handler and newest created first. A move between stages moves or
/// removes the key once; an entry in an unknown stage has no key in either, and
/// leaving or entering an unknown stage adds or removes its active key once.
#[restate_core::test]
async fn partial_indexes_follow_each_entry_stage() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    open(&mut store, &tracking()).await;
    let [all, finished, active] = [
        &ENTRY_BY_SERVICE_CREATED,
        &FINISHED_ENTRY_BY_OUTCOME,
        &ACTIVE_ENTRY_BY_STAGE,
    ]
    .map(|definition| maintained(&store, definition));

    let qid = VQueueId::custom(7, "q");
    let call = EntryTargetRef::Service {
        scope: None,
        service: "Utils",
        handler: "call",
    };
    let run = EntryTargetRef::Service {
        scope: None,
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
        (
            &other,
            EntryKind::Invocation,
            5,
            9_000,
            Stage::Finished,
            Status::Succeeded,
            9_900,
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
    let finished_key = |status: Status, i: usize, transitioned: u64| {
        let target = entries[i].0;
        let mut bytes = Vec::new();
        FinishedEntryByOutcomeKey::borrowed(
            status,
            target.service(),
            target.handler(),
            Reverse(at(transitioned)),
            canonical(i),
        )
        .encode_key(
            IndexScope {
                partition: PartitionId::MIN,
                index: finished,
            },
            &mut bytes,
        );
        bytes
    };
    let active_key = |stage: Stage, i: usize| {
        let target = entries[i].0;
        let mut bytes = Vec::new();
        ActiveEntryByStageKey::borrowed(
            stage,
            target.service(),
            target.handler(),
            Reverse(entries[i].2.created_at),
            canonical(i),
        )
        .encode_key(
            IndexScope {
                partition: PartitionId::MIN,
                index: active,
            },
            &mut bytes,
        );
        bytes
    };
    assert_eq!(
        keys_of(&store, all).len(),
        7,
        "every entry is in the first index"
    );
    // Statuses in their codec order: Started (70) before Succeeded (80).
    assert_eq!(
        keys_of(&store, finished),
        [
            finished_key(Status::Started, 5, 8_000),
            finished_key(Status::Succeeded, 4, 9_900)
        ]
    );
    // Stages in rank order: Inbox, Paused, Running, Suspended.
    assert_eq!(
        keys_of(&store, active),
        [
            active_key(Stage::Inbox, 0),
            active_key(Stage::Paused, 3),
            active_key(Stage::Running, 1),
            active_key(Stage::Suspended, 2),
        ]
    );

    // Moves: Inbox → Running, Running → Finished (killed), Unknown → Inbox,
    // Suspended → Unknown. Then the Finished "other" entry is deleted.
    let moves = [
        (0, Stage::Running, Status::Started, 1_100),
        (1, Stage::Finished, Status::Killed, 7_000),
        (6, Stage::Inbox, Status::New, 5_100),
        (2, Stage::Unknown, Status::New, 2_600),
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

    assert_eq!(keys_of(&store, all).len(), 6);
    assert_eq!(
        keys_of(&store, finished),
        [
            finished_key(Status::Killed, 1, 7_000),
            finished_key(Status::Started, 5, 8_000),
        ]
    );
    assert_eq!(
        keys_of(&store, active),
        [
            active_key(Stage::Inbox, 6),
            active_key(Stage::Paused, 3),
            active_key(Stage::Running, 0),
        ]
    );
}

// `finished_entry_by_outcome/1` as it was accepted, before its key fields took
// the entry table's names (`outcome` → `status`, `finished_at` →
// `transitioned_at`).
crate::index::macros::define_secondary_index!(
    AcceptedFinishedEntryByOutcome,
    definition: "finished_entry_by_outcome/1",
    introduced: (1, 8, 2),
    key: AcceptedFinishedEntryByOutcomeKey {
        outcome: StatusCodec,
        target_service_name: ServiceNameCodec => str,
        target_handler_name: Option<StringCodec> => str,
        finished_at: ReverseUniqueTimestampCodec,
        canonical_id: CanonicalEntryIdCodec (primary_key),
    }
);

/// Renaming the finished index's key fields changed no byte. Over every
/// stored status, a handler or none, timestamps tied at a millisecond and apart
/// only by their logical clock, and two incarnations, today's keys are exactly
/// the bytes the change that introduced the index wrote
/// (`finished_entry_by_outcome_v1.keys`): its header, with zero padding, the
/// partition and the index, then every field. Both declarations decode those
/// bytes to the values encoded.
#[test]
fn finished_keys_encode_as_before_their_fields_were_renamed() {
    use std::cmp::Reverse;

    use zerocopy::IntoBytes;

    let mut frozen = include_str!("finished_entry_by_outcome_v1.keys")
        .lines()
        .filter(|line| !line.starts_with('#'))
        .map(|line| {
            let (case, hex) = line.rsplit_once(' ').unwrap();
            let bytes: Vec<u8> = (0..hex.len())
                .step_by(2)
                .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).unwrap())
                .collect();
            (case.to_owned(), bytes)
        });
    let scope = IndexScope {
        partition: PartitionId::MIN,
        index: 7,
    };
    const BASE: u64 = 1_744_010_000_000;
    let statuses = [
        Status::Unknown,
        Status::New,
        Status::Scheduled,
        Status::Started,
        Status::BackingOff,
        Status::Yielded,
        Status::Killed,
        Status::Cancelled,
        Status::Failed,
        Status::Succeeded,
    ];
    let qid = VQueueId::custom(7, "q");
    let mut compared = 0;
    for status in statuses {
        for handler in [Some("call"), None] {
            for (millis, logical) in [(0, 0), (0, 1), (1, 0)] {
                let at = UniqueTimestamp::from_parts_unchecked(BASE + millis, logical);
                for seq in [1, 2] {
                    let kind = if handler.is_some() {
                        EntryKind::Invocation
                    } else {
                        EntryKind::StateMutation
                    };
                    let canonical =
                        EntryKey::new(false, at.to_unix_millis(), seq, EntryId::new(kind, [3; 16]))
                            .to_canonical_entry_id(qid.partition_key());
                    let (case, frozen) = frozen.next().unwrap();
                    assert_eq!(
                        case,
                        format!(
                            "{status:?} {} +{millis}ms/{logical} seq{seq}",
                            handler.unwrap_or("-")
                        )
                    );
                    let (mut renamed, mut accepted) = (Vec::new(), Vec::new());
                    FinishedEntryByOutcomeKey::borrowed(
                        status,
                        "Utils",
                        handler,
                        Reverse(at),
                        canonical,
                    )
                    .encode_key(scope, &mut renamed);
                    AcceptedFinishedEntryByOutcomeKey::borrowed(
                        status,
                        "Utils",
                        handler,
                        Reverse(at),
                        canonical,
                    )
                    .encode_key(scope, &mut accepted);
                    assert_eq!((&renamed, &accepted), (&frozen, &frozen), "{case}");
                    let (prefix, payload) = IndexKeyPrefix::decode_prefix(&frozen).unwrap();
                    assert_eq!(
                        prefix.as_bytes(),
                        IndexKeyPrefix::new(PartitionId::MIN, 7).as_bytes()
                    );
                    let new = payload
                        .into_decoder::<FinishedEntryByOutcomeKey>()
                        .decode_all()
                        .unwrap();
                    let old = payload
                        .into_decoder::<AcceptedFinishedEntryByOutcomeKey>()
                        .decode_all()
                        .unwrap();
                    let encoded = (status, "Utils", handler, Reverse(at), canonical);
                    assert_eq!(
                        (
                            new.status,
                            new.target_service_name.as_str(),
                            new.target_handler_name.as_deref(),
                            new.transitioned_at,
                            new.canonical_id
                        ),
                        encoded,
                        "{case}"
                    );
                    assert_eq!(
                        (
                            old.outcome,
                            old.target_service_name.as_str(),
                            old.target_handler_name.as_deref(),
                            old.finished_at,
                            old.canonical_id
                        ),
                        encoded,
                        "{case}"
                    );
                    assert_eq!(new.canonical_id.seq(), Seq::new(seq), "{case}");
                    compared += 1;
                }
            }
        }
    }
    assert!(frozen.next().is_none());
    assert_eq!(compared, 10 * 2 * 3 * 2);
}
