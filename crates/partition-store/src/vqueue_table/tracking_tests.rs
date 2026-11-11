// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tracking IDs through the entry-status boundary: every write adopts the entry
//! into each active instance, and stores the Maintaining instances' IDs.

use std::cmp::Reverse;

use restate_clock::UniqueTimestamp;
use restate_clock::time::MillisSinceEpoch;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::vqueue_table::stats::EntryStatistics;
use restate_storage_api::vqueue_table::{
    EntryContext, EntryKey, EntryMetadata, EntryStateRef, EntryStatusHeader, RawStatusHeader,
    ReadVQueueTable, Stage, Status, StoredEntryRef, WriteVQueueTable,
};
use restate_storage_api::{StorageError, Transaction};
use restate_types::identifiers::{BaseEntryId, PartitionId};
use restate_types::partitions::Partition;
use restate_types::sharding::KeyRange;
use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef, VQueueId};
use tokio_util::sync::CancellationToken;

use restate_storage_query_api::scan::ReadCursor;

use crate::index::EntryByStageKey;
use crate::keys::{EncodeTableKeyPrefix, IndexKeyPrefix, KeyKind};
use crate::registry::{ActiveSet, Definition, RegistrationKind, Registry};
use crate::vqueue_table::{EntryStatusKey, EntryStatusScan};
use crate::{PartitionStore, PartitionStoreManager, PartitionStoreTransaction, StorageAccess};

const TARGET: EntryTargetRef<'static> = EntryTargetRef::Service {
    scope: None,
    service: "svc",
    handler: "run",
};

fn tracked(header: &impl EntryStatusHeader) -> Vec<u32> {
    header.tracked().ids().unwrap().collect()
}

/// Rewrites the stored header of `id` with `tracked` bytes, as the registry will.
fn store_tracked(tx: &mut PartitionStoreTransaction<'_>, id: BaseEntryId, tracked: &[u8]) {
    let key = EntryStatusKey::from(id).serialize();
    let mut header = {
        let value = tx
            .get(crate::TableKind::VQueue, &key)
            .unwrap()
            .expect("stored status");
        RawStatusHeader::decode_stored(value.as_ref()).unwrap()
    };
    header.tracked = tracked.to_vec();
    tx.raw_put_cf(
        KeyKind::VQueueEntryStatus,
        key,
        bilrost::Message::encode_length_delimited_to_vec(&header),
    );
}

async fn setup() -> (
    PartitionStore,
    VQueueId,
    BaseEntryId,
    EntryKey,
    EntryStatistics,
) {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    let at =
        UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(1_744_010_000_000)).unwrap();
    let qid = VQueueId::custom(7, "q");
    let key = EntryKey::new(
        false,
        at.to_unix_millis(),
        1,
        EntryId::new(EntryKind::Invocation, [1; 16]),
    );
    let id = key.entry_id().to_base_id(qid.partition_key());
    (store, qid, id, key, EntryStatistics::new(at, key.run_at()))
}

/// A test index of inbox entries only, so entries can be tracked nonmembers.
fn apply_inbox_index(
    storage: &mut PartitionStoreTransaction<'_>,
    id: u32,
    context: &EntryContext<'_>,
    before: Option<&EntryStateRef<'_>>,
    after: Option<&EntryStateRef<'_>>,
) {
    let project = |entry: &EntryStateRef<'_>| {
        (entry.stage == Stage::Inbox).then(|| {
            (
                Reverse(entry.stats.transitioned_at),
                entry
                    .entry_key
                    .to_canonical_entry_id(context.qid.partition_key()),
            )
        })
    };
    let (old, new) = (before.and_then(project), after.and_then(project));
    if old != new {
        let key = |(at, id)| EntryByStageKey::borrowed(Stage::Inbox, at, id);
        storage.update_secondary_index_with_id(id, old.map(key).as_ref(), new.map(key).as_ref());
    }
}

fn apply_nothing(
    _: &mut PartitionStoreTransaction<'_>,
    _: u32,
    _: &EntryContext<'_>,
    _: Option<&EntryStateRef<'_>>,
    _: Option<&EntryStateRef<'_>>,
) {
}

static INBOX_A: Definition = Definition {
    name: "inbox_a/1",
    kind: RegistrationKind::Index,
    apply: apply_inbox_index,
};
static INBOX_B: Definition = Definition {
    name: "inbox_b/1",
    kind: RegistrationKind::Index,
    apply: apply_inbox_index,
};
static NOOP: Definition = Definition {
    name: "noop/1",
    kind: RegistrationKind::Statistic,
    apply: apply_nothing,
};

/// Raw keys of the index instance `id`.
fn instance_keys(store: &PartitionStore, id: u32) -> Vec<Vec<u8>> {
    let prefix = IndexKeyPrefix::new(PartitionId::MIN, id);
    let prefix = zerocopy::IntoBytes::as_bytes(&prefix);
    let db = store.partition_db().rocksdb().inner().as_raw_db();
    let cf = db.cf_handle(store.partition().cf_name().as_ref()).unwrap();
    let mut options = rocksdb::ReadOptions::default();
    options.set_total_order_seek(true);
    let mut iterator = db.raw_iterator_cf_opt(&cf, options);
    iterator.seek(prefix);
    let mut keys = Vec::new();
    while let Some(key) = iterator.key().filter(|key| key.starts_with(prefix)) {
        keys.push(key.to_vec());
        iterator.next();
    }
    iterator.status().unwrap();
    keys
}

#[restate_core::test]
async fn every_write_stores_the_maintaining_instances() {
    let (mut store, qid, id, key, stats) = setup().await;
    let mut registry = Registry::default();
    let maintaining = [
        registry
            .register("noop/1", RegistrationKind::Statistic)
            .unwrap(),
        registry
            .register("unknown/1", RegistrationKind::Index)
            .unwrap(),
    ];
    assert_eq!(maintaining, [64, 65]);
    // Unknown definitions are not maintained, so entries do not list them.
    store.set_active_set(ActiveSet::new(
        &registry,
        &crate::walk::Walks::default(),
        &[&NOOP],
    ));
    let context = EntryContext {
        qid: &qid,
        target: &TARGET,
    };
    let metadata = EntryMetadata::default();
    let state = |stage, status| EntryStateRef {
        stage,
        status,
        entry_key: &key,
        metadata: &metadata,
        stats: &stats,
    };
    let mut tx = store.transaction();
    tx.create_vqueue_entry_status(&context, state(Stage::Inbox, Status::New));
    let header = tx.get_vqueue_entry_status(&id).await.unwrap().unwrap();
    assert_eq!(tracked(&header), [64], "insertions_are_adopted");
    // An instance that is no longer active drops out on the next write.
    store_tracked(&mut tx, id, &[0, 0, 0, 64, 0, 0, 0, 70]);
    tx.commit().await.unwrap();
    drop(tx);

    // Two updates in one transaction, each from the header read after the last write.
    let mut tx = store.transaction();
    for (stage, status) in [
        (Stage::Running, Status::Started),
        (Stage::Paused, Status::BackingOff),
    ] {
        let header = tx.get_vqueue_entry_status(&id).await.unwrap().unwrap();
        tx.update_vqueue_entry_status(
            &context,
            StoredEntryRef::from_header(&header),
            state(stage, status),
        );
        let header = tx.get_vqueue_entry_status(&id).await.unwrap().unwrap();
        assert_eq!(tracked(&header), [64], "stale_ids_drop_out");
    }
    tx.commit().await.unwrap();
    drop(tx);

    // Once every instance is Ready, writes store no list.
    registry.set_ready(64).unwrap();
    store.set_active_set(ActiveSet::new(
        &registry,
        &crate::walk::Walks::default(),
        &[&NOOP],
    ));
    let mut tx = store.transaction();
    let header = tx.get_vqueue_entry_status(&id).await.unwrap().unwrap();
    tx.update_vqueue_entry_status(
        &context,
        StoredEntryRef::from_header(&header),
        state(Stage::Inbox, Status::New),
    );
    let header = tx.get_vqueue_entry_status(&id).await.unwrap().unwrap();
    assert!(
        header.tracked().is_empty(),
        "all_ready_writes_store_no_list"
    );

    // Deleting and re-creating in one transaction leaves only the new entry.
    tx.delete_vqueue_entry_status(&context, StoredEntryRef::from_header(&header));
    assert!(tx.get_vqueue_entry_status(&id).await.unwrap().is_none());
    tx.create_vqueue_entry_status(&context, state(Stage::Inbox, Status::New));
    assert!(tx.get_vqueue_entry_status(&id).await.unwrap().is_some());
    tx.commit().await.unwrap();
}

#[restate_core::test]
async fn untracked_entries_are_adopted_and_tracked_ones_maintained() {
    let (mut store, qid, id, key, stats) = setup().await;
    let context = EntryContext {
        qid: &qid,
        target: &TARGET,
    };
    let metadata = EntryMetadata::default();
    let at = |seconds: u64| {
        let mut stats = stats.clone();
        stats.transitioned_at = UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(
            1_744_010_000_000 + seconds * 1_000,
        ))
        .unwrap();
        stats
    };
    let stats = [at(1), at(2), at(3)];
    let state = |stage, stats| EntryStateRef {
        stage,
        status: Status::New,
        entry_key: &key,
        metadata: &metadata,
        stats,
    };
    let read = async |store: &mut PartitionStore| {
        let header = store
            .transaction()
            .get_vqueue_entry_status(&id)
            .await
            .unwrap()
            .unwrap();
        (header.stage(), tracked(&header))
    };

    // An entry written before any instance existed is untracked.
    let mut tx = store.transaction();
    tx.create_vqueue_entry_status(&context, state(Stage::Inbox, &stats[0]));
    tx.commit().await.unwrap();
    drop(tx);
    let mut registry = Registry::default();
    let maintaining = registry
        .register("inbox_a/1", RegistrationKind::Index)
        .unwrap();
    let ready = registry
        .register("inbox_b/1", RegistrationKind::Index)
        .unwrap();
    registry.set_ready(ready).unwrap();
    store.set_active_set(ActiveSet::new(
        &registry,
        &crate::walk::Walks::default(),
        &[&INBOX_A, &INBOX_B],
    ));
    // A Ready instance is complete: its pass already indexed the entry.
    let mut tx = store.transaction();
    let canonical = key.to_canonical_entry_id(qid.partition_key());
    tx.update_secondary_index_with_id(
        ready,
        None,
        Some(&EntryByStageKey::borrowed(
            Stage::Inbox,
            Reverse(stats[0].transitioned_at),
            canonical,
        )),
    );
    tx.commit().await.unwrap();
    drop(tx);

    let update = async |store: &mut PartitionStore, stage, stats| {
        let mut tx = store.transaction();
        let header = tx.get_vqueue_entry_status(&id).await.unwrap().unwrap();
        tx.update_vqueue_entry_status(
            &context,
            StoredEntryRef::from_header(&header),
            state(stage, stats),
        );
        tx.commit().await.unwrap();
    };
    let counts = |store: &PartitionStore| {
        (
            instance_keys(store, maintaining).len(),
            instance_keys(store, ready).len(),
        )
    };

    // Adopted by the Maintaining instance: Put only. The Ready one moves its key.
    update(&mut store, Stage::Inbox, &stats[1]).await;
    assert_eq!(read(&mut store).await, (Stage::Inbox, vec![maintaining]));
    assert_eq!(counts(&store), (1, 1), "adoption_puts_and_ready_moves");
    let after_adoption = instance_keys(&store, maintaining);
    assert_eq!(
        after_adoption,
        instance_keys(&store, ready)
            .iter()
            .map(|key| {
                let mut key = key.clone();
                key[6..10].copy_from_slice(&maintaining.to_be_bytes());
                key
            })
            .collect::<Vec<_>>(),
        "both_instances_hold_the_same_entry"
    );

    // Tracked now: the next change moves the key in both instances.
    update(&mut store, Stage::Inbox, &stats[2]).await;
    assert_eq!(counts(&store), (1, 1), "tracked_keys_move");
    assert_ne!(instance_keys(&store, maintaining), after_adoption);

    // Leaving the index removes both keys; the entry stays tracked as a nonmember.
    update(&mut store, Stage::Running, &stats[2]).await;
    assert_eq!(counts(&store), (0, 0), "tracked_members_leave");
    assert_eq!(read(&mut store).await, (Stage::Running, vec![maintaining]));
    update(&mut store, Stage::Inbox, &stats[2]).await;
    assert_eq!(counts(&store), (1, 1), "tracked_nonmembers_join");

    // Deleting a tracked entry removes its keys.
    let mut tx = store.transaction();
    let header = tx.get_vqueue_entry_status(&id).await.unwrap().unwrap();
    tx.delete_vqueue_entry_status(&context, StoredEntryRef::from_header(&header));
    tx.commit().await.unwrap();
    drop(tx);
    assert_eq!(counts(&store), (0, 0), "tracked_deletions_remove_keys");

    // Deleting an entry the Maintaining instance does not track deletes nothing
    // from it: an untracked entry has no key there. A planted key makes a stray
    // deletion visible. The Ready instance is complete, so it holds the entry.
    store.set_active_set(ActiveSet::default());
    let mut tx = store.transaction();
    tx.create_vqueue_entry_status(&context, state(Stage::Inbox, &stats[0]));
    let key = EntryByStageKey::borrowed(Stage::Inbox, Reverse(stats[0].transitioned_at), canonical);
    tx.update_secondary_index_with_id(maintaining, None, Some(&key));
    tx.update_secondary_index_with_id(ready, None, Some(&key));
    tx.commit().await.unwrap();
    drop(tx);
    store.set_active_set(ActiveSet::new(
        &registry,
        &crate::walk::Walks::default(),
        &[&INBOX_A, &INBOX_B],
    ));
    assert_eq!(counts(&store), (1, 1));
    let mut tx = store.transaction();
    let header = tx.get_vqueue_entry_status(&id).await.unwrap().unwrap();
    tx.delete_vqueue_entry_status(&context, StoredEntryRef::from_header(&header));
    tx.commit().await.unwrap();
    drop(tx);
    assert_eq!(
        counts(&store),
        (1, 0),
        "untracked_deletions_delete_nothing_and_ready_ones_delete_their_key"
    );
}

#[restate_core::test]
async fn stale_before_images_are_detected_in_tests() {
    let (mut store, qid, id, key, stats) = setup().await;
    let metadata = EntryMetadata::default();
    let mut tx = store.transaction();
    tx.create_vqueue_entry_status(
        &EntryContext {
            qid: &qid,
            target: &TARGET,
        },
        EntryStateRef {
            stage: Stage::Inbox,
            status: Status::New,
            entry_key: &key,
            metadata: &metadata,
            stats: &stats,
        },
    );
    let stale = tx.get_vqueue_entry_status(&id).await.unwrap().unwrap();
    store_tracked(&mut tx, id, &[0, 0, 0, 64]);
    let fresh = tx.get_vqueue_entry_status(&id).await.unwrap().unwrap();
    let key: [u8; EntryStatusKey::serialized_length_fixed()] = EntryStatusKey::from(id)
        .serialize()
        .as_ref()
        .try_into()
        .unwrap();
    // Updates and deletions assert this before writing, in test builds.
    assert!(
        !tx.is_current_tracking(key, stale.tracked()),
        "stale_before_image"
    );
    assert!(
        tx.is_current_tracking(key, fresh.tracked()),
        "fresh_before_image"
    );
}

#[restate_core::test]
async fn malformed_tracking_is_a_data_integrity_error() {
    let (mut store, qid, id, key, stats) = setup().await;
    let metadata = EntryMetadata::default();
    let mut tx = store.transaction();
    tx.create_vqueue_entry_status(
        &EntryContext {
            qid: &qid,
            target: &TARGET,
        },
        EntryStateRef {
            stage: Stage::Inbox,
            status: Status::New,
            entry_key: &key,
            metadata: &metadata,
            stats: &stats,
        },
    );
    // Unsorted IDs.
    store_tracked(&mut tx, id, &[0, 0, 0, 70, 0, 0, 0, 64]);
    tx.commit().await.unwrap();
    drop(tx);
    assert!(matches!(
        store.transaction().get_vqueue_entry_status(&id).await,
        Err(StorageError::DataIntegrityError)
    ));
    let (snapshot, _completed) = store.query_read(CancellationToken::new()).unwrap();
    let mut cursor = snapshot
        .entry_status_cursor(EntryStatusScan::Filter(
            restate_storage_api::vqueue_table::filters::ScanEntryIdFilter::EntryIdSet([id].into()),
        ))
        .unwrap();
    cursor.advance().unwrap();
    assert!(matches!(
        cursor.current(),
        Err(StorageError::DataIntegrityError)
    ));
}
