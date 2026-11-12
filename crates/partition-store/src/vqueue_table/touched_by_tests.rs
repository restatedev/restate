// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Stored tokens survive every write through the entry-status boundary.
//! Nothing assigns them yet, so the tests write them directly.

use restate_clock::UniqueTimestamp;
use restate_clock::time::MillisSinceEpoch;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::vqueue_table::filters::ScanEntryIdFilter;
use restate_storage_api::vqueue_table::stats::EntryStatistics;
use restate_storage_api::vqueue_table::{
    EntryContext, EntryKey, EntryMetadata, EntryStateRef, EntryStatusHeader, RawStatusHeader,
    ReadVQueueTable, ScanVQueueEntryStatusTable, Stage, Status, StoredEntryRef, WriteVQueueTable,
};
use restate_storage_api::{StorageError, Transaction};
use restate_types::identifiers::{BaseEntryId, PartitionId};
use restate_types::partitions::Partition;
use restate_types::sharding::KeyRange;
use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef, VQueueId};

use crate::keys::{EncodeTableKeyPrefix, KeyKind};
use crate::vqueue_table::EntryStatusKey;
use crate::{PartitionStore, PartitionStoreManager, PartitionStoreTransaction, StorageAccess};

const TARGET: EntryTargetRef<'static> = EntryTargetRef::Service {
    scope: None,
    service: "svc",
    handler: "run",
};

fn touched_by(header: &impl EntryStatusHeader) -> Vec<u32> {
    header.touched_by().ids().unwrap().collect()
}

/// Rewrites the stored header of `id` with `touched` bytes, as the registry will.
fn store_tracked(tx: &mut PartitionStoreTransaction<'_>, id: BaseEntryId, touched_by: &[u8]) {
    let key = EntryStatusKey::from(id).serialize();
    let mut header = {
        let value = tx
            .get(crate::TableKind::VQueue, &key)
            .unwrap()
            .expect("stored status");
        RawStatusHeader::decode_stored(value.as_ref()).unwrap()
    };
    header.touched_by = touched_by.to_vec();
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

#[restate_core::test]
async fn stored_tokens_survive_chained_updates_and_end_with_deletion() {
    let (mut store, qid, id, key, stats) = setup().await;
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
        assert_eq!(touched_by(&header), [64, 70]);
        tx.update_vqueue_entry_status(
            &context,
            StoredEntryRef::from_header(&header),
            state(stage, status),
        );
    }
    tx.commit().await.unwrap();
    drop(tx);
    let mut tx = store.transaction();
    let header = tx.get_vqueue_entry_status(&id).await.unwrap().unwrap();
    assert_eq!(
        (header.stage(), touched_by(&header)),
        (Stage::Paused, vec![64, 70]),
        "updates_keep_the_stored_list_across_commits"
    );

    // Deleting and re-creating in one transaction starts without tokens.
    tx.delete_vqueue_entry_status(&context, StoredEntryRef::from_header(&header));
    assert!(tx.get_vqueue_entry_status(&id).await.unwrap().is_none());
    tx.create_vqueue_entry_status(&context, state(Stage::Inbox, Status::New));
    let header = tx.get_vqueue_entry_status(&id).await.unwrap().unwrap();
    assert!(
        header.touched_by().is_empty(),
        "insertions_start_without_tokens"
    );
    tx.commit().await.unwrap();
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
        !tx.touched_by_is_current(key, stale.touched_by()),
        "stale_before_image"
    );
    assert!(
        tx.touched_by_is_current(key, fresh.touched_by()),
        "fresh_before_image"
    );
}

#[restate_core::test]
async fn malformed_tokens_are_a_data_integrity_error() {
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
    for filter in [
        ScanEntryIdFilter::PartitionKey(KeyRange::FULL),
        ScanEntryIdFilter::EntryIdSet([id].into()),
    ] {
        let result = store
            .for_each_vqueue_entry_status(filter, |_, _| {
                panic!("malformed tokens must be rejected before invoking the callback")
            })
            .unwrap()
            .await;
        assert!(matches!(result, Err(StorageError::DataIntegrityError)));
    }
}
