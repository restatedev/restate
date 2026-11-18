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
use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef, HandlerRef, VQueueId};

use crate::fsm_table::get_registry_from_partition_db;
use crate::index::SecondaryIndexKey;
use crate::keys::header::IndexScope;
use crate::keys::{IndexKeyPrefix, KeyKind};
use crate::registry::IndexStatus;
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
    get_registry_from_partition_db(store.partition_db())
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

/// The keys without their index header: comparable across replicas whatever
/// partition and index IDs they have.
fn payloads(store: &PartitionStore) -> Vec<Vec<u8>> {
    index_keys(store)
        .into_iter()
        .map(|key| key[IndexKeyPrefix::SERIALIZED_LENGTH..].to_vec())
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
    assert_eq!(index_keys(&store), expected(&[4, 3, 1, 0, 2]));

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
    assert_eq!(index_keys(&store), expected(&[4, 3, 1, 0, 2]));

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
    assert_eq!(index_keys(&store), expected(&[4, 3, 0, 2]));
}

/// Backfilling existing entries gives the same keys as maintaining the index
/// from the start: invocations through their invocation status, state
/// mutations through their input. A reset deletes the keys; tracking again
/// rebuilds them under a new ID.
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
    assert_eq!(index(&store), None);
    assert!(index_keys(&store).is_empty());

    let mut config = tracking();
    open(&mut store, &config).await;
    let (built, status) = index(&store).unwrap();
    assert_eq!(status, IndexStatus::Ready);
    // 48 invocations and a state mutation on each of three partition keys.
    assert_eq!(payloads(&reference).len(), 147);
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
}
