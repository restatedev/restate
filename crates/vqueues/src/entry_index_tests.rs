// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The entry index by service and handler (`entry_by_service_created/1`), kept
//! up to date through the real vqueue operations rather than raw table writes:
//! each entry has exactly its one key from creation to deletion, an entry whose
//! sequence changes moves to its new key, and changes to anything else the key
//! doesn't contain write nothing to the index.

use std::cmp::Reverse;
use std::time::Duration;

use restate_clock::{RoughTimestamp, UniqueTimestamp};
use restate_partition_store::index::{RegisteredIndex, SecondaryIndexKey};
use restate_partition_store::keys::KeyKind;
use restate_partition_store::keys::header::IndexScope;
use restate_partition_store::registry::IndexKind;
use restate_partition_store::vqueue_table::{EntryByServiceCreated, EntryByServiceCreatedKey};
use restate_partition_store::{PartitionStore, PartitionStoreManager};
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::vqueue_table::metadata::{VQueueLink, VQueueMeta};
use restate_storage_api::vqueue_table::stats::{EntryStatistics, WaitStats};
use restate_storage_api::vqueue_table::{
    EntryContext, EntryKey, EntryMetadata, EntryStateRef, EntryStatusHeader, ReadVQueueTable,
    Stage, Status, StoredEntryRef, WriteVQueueTable,
};
use restate_types::LimitKey;
use restate_types::config::Configuration;
use restate_types::identifiers::{CanonicalEntryId, PartitionId, PartitionKey};
use restate_types::partitions::Partition;
use restate_types::sharding::KeyRange;
use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef, HandlerRef, Seq, VQueueId};

use crate::{VQueue, VQueueEvent, VQueuesMetaCache, YieldReason};

/// A service, a scoped virtual object, a workflow, and a state mutation (no
/// handler).
fn target(case: u8) -> EntryTargetRef<'static> {
    match case {
        0 => EntryTargetRef::Service {
            scope: None,
            service: "svc",
            handler: "handle",
        },
        1 => EntryTargetRef::VirtualObject {
            scope: Some("tenant"),
            service: "vo",
            key: "key",
            handler: HandlerRef::UserHandler("handle"),
        },
        2 => EntryTargetRef::Workflow {
            scope: None,
            service: "wf",
            key: "key",
            handler: "handle",
        },
        _ => EntryTargetRef::VirtualObject {
            scope: None,
            service: "vo",
            key: "key",
            handler: HandlerRef::StateMutation,
        },
    }
}

/// An empty store that tracks the entry indexes, and the scope of the Ready
/// index by service and handler.
async fn tracking_store() -> (PartitionStore, IndexScope) {
    RocksDbManager::init();
    let mut store = PartitionStoreManager::create(true)
        .await
        .unwrap()
        .open(
            &Partition::new(PartitionId::MIN, KeyRange::new(0, PartitionKey::MAX - 1)),
            None,
        )
        .await
        .unwrap();
    let mut config = Configuration::default();
    config.common.experimental.set_track_entry_indexes(true);
    store
        .verify_and_run_migrations(Default::default(), &config)
        .await
        .unwrap();
    let (snapshot, _) = store.query_read(Default::default()).unwrap();
    let index = snapshot
        .ready_index(EntryByServiceCreated::DEFINITION, IndexKind::Secondary)
        .unwrap();
    drop(snapshot);
    let scope = IndexScope {
        partition: store.partition_id(),
        index,
    };
    (store, scope)
}

/// Every secondary index key in the store.
fn index_keys(store: &PartitionStore) -> Vec<Vec<u8>> {
    let db = store.partition_db();
    let mut options = rocksdb::ReadOptions::default();
    options.set_total_order_seek(true);
    options.set_iterate_range(rocksdb::PrefixRange(KeyKind::SecondaryIndex.as_bytes()));
    let mut iterator = db
        .rocksdb()
        .inner()
        .as_raw_db()
        .raw_iterator_cf_opt(db.cf_handle(), options);
    iterator.seek_to_first();
    let mut keys = Vec::new();
    while let Some(key) = iterator.key() {
        keys.push(key.to_vec());
        iterator.next();
    }
    iterator.status().unwrap();
    keys
}

/// The one key of an entry of `target`, created at `created_at`.
fn expected_key(
    scope: IndexScope,
    target: &EntryTargetRef<'_>,
    created_at: UniqueTimestamp,
    id: CanonicalEntryId,
) -> Vec<Vec<u8>> {
    let mut key = Vec::new();
    EntryByServiceCreatedKey::borrowed(target.service(), target.handler(), Reverse(created_at), id)
        .encode_key(scope, &mut key);
    vec![key]
}

/// RocksDB's latest sequence number: each write in a batch takes one, so a
/// commit that writes only the entry status advances it by one.
fn sequence(store: &PartitionStore) -> u64 {
    store
        .partition_db()
        .rocksdb()
        .inner()
        .as_raw_db()
        .latest_sequence_number()
}

/// Through enqueueing, metadata and status updates, yielding and running, and
/// every way an entry ends (retained then deleted, deleted at once, finished
/// inline), an entry has exactly its one key until it is deleted, and updates
/// that don't change the key write only the entry status.
#[restate_core::test]
async fn entry_index_follows_vqueue_operations() {
    let (mut store, scope) = tracking_store().await;
    let mut cache = VQueuesMetaCache::new_empty(16);
    let at = |logical| UniqueTimestamp::try_from_parts(100, logical).unwrap();

    // Service, virtual object and workflow invocations, then state mutations,
    // each kind through all three endings.
    for case in 0..6u8 {
        let kind = if case >= 3 {
            EntryKind::StateMutation
        } else {
            EntryKind::Invocation
        };
        let target = target(case);
        let qid = VQueueId::custom(3337, format!("index-{case}"));
        let entry_id = EntryId::new(kind, [case + 1; EntryId::REMAINDER_LEN]);
        let base_id = entry_id.to_base_id(qid.partition_key());
        let expected = expected_key(scope, &target, at(0), base_id.canonicalize(Seq::new(1)));

        let mut tx = store.transaction();
        VQueue::<VQueueEvent, _>::get_or_insert_with(&qid, &mut tx, &mut cache, || {
            VQueueMeta::new(at(0), None, LimitKey::None, VQueueLink::None)
        })
        .await
        .unwrap()
        .enqueue_new(
            at(0),
            &target,
            1u64,
            None,
            entry_id,
            EntryMetadata::default(),
        );
        tx.commit().await.unwrap();
        drop(tx);
        assert_eq!(index_keys(&store), expected, "case {case}: enqueued");

        // Metadata and status changes write only the entry status.
        let before = sequence(&store);
        let mut tx = store.transaction();
        let header = tx.get_vqueue_entry_status(&base_id).await.unwrap().unwrap();
        let mut metadata = header.metadata().clone();
        metadata.retry_attempts = 1;
        VQueue::<VQueueEvent, _>::get(&qid, &mut tx, &mut cache, None)
            .await
            .unwrap()
            .unwrap()
            .update_entry_metadata(&header, &target, &metadata);
        tx.commit().await.unwrap();
        drop(tx);
        assert_eq!(sequence(&store), before + 1, "case {case}: metadata");
        let before = sequence(&store);
        let mut tx = store.transaction();
        let header = tx.get_vqueue_entry_status(&base_id).await.unwrap().unwrap();
        tx.update_vqueue_entry_status(
            &EntryContext {
                qid: &qid,
                target: &target,
            },
            StoredEntryRef::from_header(&header),
            EntryStateRef {
                status: Status::Started,
                ..EntryStateRef::from_header(&header)
            },
        );
        tx.commit().await.unwrap();
        drop(tx);
        assert_eq!(sequence(&store), before + 1, "case {case}: status");
        assert_eq!(index_keys(&store), expected, "case {case}: updated");

        if case % 3 < 2 {
            // Yielding and running change stage and transition time, not the key.
            let mut tx = store.transaction();
            let header = tx.get_vqueue_entry_status(&base_id).await.unwrap().unwrap();
            VQueue::<VQueueEvent, _>::get(&qid, &mut tx, &mut cache, None)
                .await
                .unwrap()
                .unwrap()
                .yield_entry(
                    at(1),
                    &header,
                    &target,
                    None,
                    YieldReason::PartitionLeaderChange,
                );
            tx.commit().await.unwrap();
            drop(tx);
            assert_eq!(index_keys(&store), expected, "case {case}: yielded");
            let mut tx = store.transaction();
            let header = tx.get_vqueue_entry_status(&base_id).await.unwrap().unwrap();
            VQueue::<VQueueEvent, _>::get(&qid, &mut tx, &mut cache, None)
                .await
                .unwrap()
                .unwrap()
                .run_entry(at(2), &header, &target, WaitStats::default());
            tx.commit().await.unwrap();
            drop(tx);
            assert_eq!(index_keys(&store), expected, "case {case}: ran");
        }

        let mut tx = store.transaction();
        let header = tx.get_vqueue_entry_status(&base_id).await.unwrap().unwrap();
        let queue = VQueue::<VQueueEvent, _>::get(&qid, &mut tx, &mut cache, None)
            .await
            .unwrap()
            .unwrap();
        match case % 3 {
            0 => queue.end(
                at(3),
                &header,
                &target,
                Status::Succeeded,
                Duration::from_secs(1),
            ),
            1 => queue.end(at(3), &header, &target, Status::Succeeded, Duration::ZERO),
            _ => queue.run_then_finish(
                at(3),
                &header,
                &target,
                WaitStats::default(),
                Status::Succeeded,
            ),
        }
        tx.commit().await.unwrap();
        drop(tx);

        if case % 3 == 0 {
            // A retained finished entry keeps its key until it is deleted.
            assert_eq!(index_keys(&store), expected, "case {case}: retained");
            let mut tx = store.transaction();
            let header = tx.get_vqueue_entry_status(&base_id).await.unwrap().unwrap();
            assert_eq!(header.stage(), Stage::Finished);
            VQueue::<VQueueEvent, _>::get(&qid, &mut tx, &mut cache, None)
                .await
                .unwrap()
                .unwrap()
                .delete(
                    at(4),
                    &EntryContext {
                        qid: &qid,
                        target: &target,
                    },
                    StoredEntryRef::from_header(&header),
                );
            tx.commit().await.unwrap();
            drop(tx);
        }
        assert!(
            index_keys(&store).is_empty(),
            "case {case} left index keys behind"
        );
        assert!(
            store
                .transaction()
                .get_vqueue_entry_status(&base_id)
                .await
                .unwrap()
                .is_none()
        );
    }
}

/// The key holds the entry's sequence: when it changes, the entry moves to its
/// new key, even through an intermediate sequence that was never committed.
/// Changes to anything else the key doesn't hold (an unchanged entry, the
/// schedule, the status, the transition time, the stage) write only the entry
/// status.
#[restate_core::test]
async fn entry_index_moves_with_the_sequence_and_ignores_the_rest() {
    let (mut store, scope) = tracking_store().await;
    for case in [0, 1] {
        let target = target(case);
        let qid = VQueueId::custom(3337, "incarnations");
        let context = EntryContext {
            qid: &qid,
            target: &target,
        };
        let id = EntryId::new(EntryKind::Invocation, [1; EntryId::REMAINDER_LEN]);
        let base = id.to_base_id(qid.partition_key());
        let at = UniqueTimestamp::try_from_parts(100, 1).unwrap();
        let keys = [Seq::new(0), Seq::new(1), Seq::MAX]
            .map(|seq| EntryKey::new(false, RoughTimestamp::new(0), seq, id));
        let metadata = EntryMetadata::default();
        let stats = EntryStatistics::new(at, keys[0].run_at());
        let states = keys.each_ref().map(|entry_key| EntryStateRef {
            stage: Stage::Inbox,
            status: Status::New,
            entry_key,
            metadata: &metadata,
            stats: &stats,
        });
        let expected = keys.map(|key| {
            expected_key(
                scope,
                &target,
                at,
                key.to_canonical_entry_id(qid.partition_key()),
            )
        });
        let update = async |store: &mut PartitionStore, after: EntryStateRef<'_>| {
            let before = sequence(store);
            let mut tx = store.transaction();
            let stored = tx.get_vqueue_entry_status(&base).await.unwrap().unwrap();
            tx.update_vqueue_entry_status(&context, StoredEntryRef::from_header(&stored), after);
            tx.commit().await.unwrap();
            sequence(store) - before
        };

        let mut tx = store.transaction();
        tx.create_vqueue_entry_status(&context, states[0]);
        tx.commit().await.unwrap();
        drop(tx);
        assert_eq!(index_keys(&store), expected[0], "case {case}");

        // Two sequence changes in one transaction leave only the last key.
        let mut tx = store.transaction();
        let stored = tx.get_vqueue_entry_status(&base).await.unwrap().unwrap();
        tx.update_vqueue_entry_status(&context, StoredEntryRef::from_header(&stored), states[1]);
        let stored = tx.get_vqueue_entry_status(&base).await.unwrap().unwrap();
        tx.update_vqueue_entry_status(&context, StoredEntryRef::from_header(&stored), states[2]);
        tx.commit().await.unwrap();
        drop(tx);
        assert_eq!(index_keys(&store), expected[2], "case {case}");

        // None of these changes the key: each writes the entry status alone.
        let rescheduled_key = keys[2].set_run_at(Some(RoughTimestamp::MAX));
        let rescheduled = EntryStateRef {
            entry_key: &rescheduled_key,
            ..states[2]
        };
        let scheduled = EntryStateRef {
            status: Status::Scheduled,
            ..rescheduled
        };
        let mut later_stats = stats.clone();
        later_stats.transitioned_at = UniqueTimestamp::try_from_parts(101, 1).unwrap();
        let later = EntryStateRef {
            stats: &later_stats,
            ..scheduled
        };
        let running = EntryStateRef {
            stage: Stage::Running,
            ..later
        };
        for (change, after) in [
            ("unchanged", states[2]),
            ("schedule", rescheduled),
            ("status", scheduled),
            ("transition time", later),
            ("stage", running),
        ] {
            assert_eq!(update(&mut store, after).await, 1, "case {case}: {change}");
            assert_eq!(index_keys(&store), expected[2], "case {case}: {change}");
        }

        let mut tx = store.transaction();
        let stored = tx.get_vqueue_entry_status(&base).await.unwrap().unwrap();
        tx.delete_vqueue_entry_status(&context, StoredEntryRef::from_header(&stored));
        tx.commit().await.unwrap();
        drop(tx);
        assert!(index_keys(&store).is_empty(), "case {case}");
    }
}
