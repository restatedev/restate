// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Reverse;
use std::time::Duration;

use restate_clock::{RoughTimestamp, UniqueTimestamp};
use restate_partition_store::PartitionStore;
use restate_partition_store::index::{
    EntryByServiceStageKey, EntryByStageKey, EntryNextAtByStageKey, SecondaryIndexKey,
};
use restate_partition_store::keys::KeyKind;
use restate_storage_api::Transaction;
use restate_storage_api::vqueue_table::metadata::{VQueueLink, VQueueMeta};
use restate_storage_api::vqueue_table::stats::{EntryStatistics, WaitStats};
use restate_storage_api::vqueue_table::{
    EntryContext, EntryKey, EntryMetadata, EntryStateRef, EntryStatusHeader, ReadVQueueTable,
    Stage, Status, WriteVQueueTable,
};
use restate_types::LimitKey;
use restate_types::config::Configuration;
use restate_types::identifiers::CanonicalEntryId;
use restate_types::sharding::PartitionId;
use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef, HandlerRef, Seq, VQueueId};

use crate::{VQueue, VQueueEvent, VQueuesMetaCache, YieldReason};

use super::storage_test_environment;

fn target(case: u8) -> EntryTargetRef<'static> {
    match case {
        0 => EntryTargetRef::Service {
            scope: None,
            service: "svc",
            handler: "handle",
        },
        1 => EntryTargetRef::VirtualObject {
            scope: None,
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

fn index_keys(store: &PartitionStore) -> Vec<Vec<u8>> {
    let prefix = KeyKind::SecondaryIndex.as_bytes();
    let db = store.partition_db().rocksdb().inner().as_raw_db();
    let cf = db.cf_handle(store.partition().cf_name().as_ref()).unwrap();
    let mut iterator = db.raw_iterator_cf(&cf);
    iterator.seek(prefix);
    let mut keys = Vec::new();
    while let Some(key) = iterator.key().filter(|key| key.starts_with(prefix)) {
        assert!(iterator.value().unwrap().is_empty());
        keys.push(key.to_vec());
        iterator.next();
    }
    iterator.status().unwrap();
    keys
}

fn expected_keys(
    partition: PartitionId,
    service: &str,
    stage: Stage,
    transitioned_at: UniqueTimestamp,
    next_at: RoughTimestamp,
    status: Status,
    id: CanonicalEntryId,
) -> Vec<Vec<u8>> {
    let mut keys = vec![Vec::new(); 3];
    EntryByServiceStageKey::borrowed(service, stage, Reverse(transitioned_at), id)
        .encode_key(partition, &mut keys[0]);
    EntryByStageKey::borrowed(stage, Reverse(transitioned_at), status, id)
        .encode_key(partition, &mut keys[1]);
    EntryNextAtByStageKey::borrowed(stage, next_at, status, id).encode_key(partition, &mut keys[2]);
    keys
}

#[restate_core::test]
async fn entry_indexes_track_sequence_schedule_and_status_independently() {
    let mut store = storage_test_environment().await;
    let mut config = Configuration::default();
    config.common.experimental.set_indexes_v1(true);
    store
        .verify_and_run_migrations(Default::default(), &config)
        .await
        .unwrap();

    let qid = VQueueId::custom(3337, "incarnations");
    let target = target(0);
    let context = EntryContext {
        qid: &qid,
        target: &target,
    };
    let id = EntryId::new(EntryKind::Invocation, [1; EntryId::REMAINDER_LEN]);
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
    let encoded = keys.map(|key| {
        expected_keys(
            store.partition_id(),
            target.service(),
            Stage::Inbox,
            at,
            key.run_at(),
            Status::New,
            key.to_canonical_entry_id(qid.partition_key()),
        )
    });

    let mut tx = store.transaction();
    tx.create_vqueue_entry_status(&context, states[0]);
    tx.commit().await.unwrap();
    drop(tx);
    assert_eq!(index_keys(&store), encoded[0]);

    // Neither stage nor transition timestamp changes. Each update must remove
    // the preceding incarnation, including the intermediate uncommitted one.
    let mut tx = store.transaction();
    tx.update_vqueue_entry_status(&context, states[0], states[1]);
    tx.update_vqueue_entry_status(&context, states[1], states[2]);
    tx.commit().await.unwrap();
    drop(tx);
    assert_eq!(index_keys(&store), encoded[2]);
    let header = store
        .transaction()
        .get_vqueue_entry_status(&id.to_base_id(qid.partition_key()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        header.canonical_entry_id(),
        keys[2].to_canonical_entry_id(qid.partition_key())
    );

    // An identical projection writes only the source status, not a second index Put.
    let sequence = store
        .partition_db()
        .rocksdb()
        .inner()
        .as_raw_db()
        .latest_sequence_number();
    let mut tx = store.transaction();
    tx.update_vqueue_entry_status(&context, states[2], states[2]);
    tx.commit().await.unwrap();
    drop(tx);
    assert_eq!(
        store
            .partition_db()
            .rocksdb()
            .inner()
            .as_raw_db()
            .latest_sequence_number(),
        sequence + 1
    );
    assert_eq!(index_keys(&store), encoded[2]);

    // A schedule-only change rewrites just the next-at index (delete + put)
    // and the source status. In particular, the unchanged service index must
    // not gate maintenance of the other indexes.
    let rescheduled_key = keys[2].set_run_at(Some(RoughTimestamp::MAX));
    let rescheduled = EntryStateRef {
        entry_key: &rescheduled_key,
        ..states[2]
    };
    let mut tx = store.transaction();
    tx.update_vqueue_entry_status(&context, states[2], rescheduled);
    tx.commit().await.unwrap();
    drop(tx);
    assert_eq!(
        store
            .partition_db()
            .rocksdb()
            .inner()
            .as_raw_db()
            .latest_sequence_number(),
        sequence + 4
    );
    let expected = |status| {
        expected_keys(
            store.partition_id(),
            target.service(),
            Stage::Inbox,
            at,
            RoughTimestamp::MAX,
            status,
            rescheduled_key.to_canonical_entry_id(qid.partition_key()),
        )
    };
    let rescheduled_keys = expected(Status::New);
    let scheduled_keys = expected(Status::Scheduled);
    assert_eq!(index_keys(&store), rescheduled_keys);

    // A status-only change rewrites both status-bearing indexes, leaving the
    // service index alone. It also writes the source and updates service stats.
    let scheduled = EntryStateRef {
        status: Status::Scheduled,
        ..rescheduled
    };
    let mut tx = store.transaction();
    tx.update_vqueue_entry_status(&context, rescheduled, scheduled);
    tx.commit().await.unwrap();
    drop(tx);
    assert_eq!(
        store
            .partition_db()
            .rocksdb()
            .inner()
            .as_raw_db()
            .latest_sequence_number(),
        sequence + 10
    );
    assert_eq!(index_keys(&store), scheduled_keys);

    let mut tx = store.transaction();
    tx.delete_vqueue_entry_status(&context, scheduled);
    tx.commit().await.unwrap();
    drop(tx);
    assert!(index_keys(&store).is_empty());
}

#[restate_core::test]
async fn entry_index_follows_vqueue_transitions_and_all_deletion_paths() {
    let mut store = storage_test_environment().await;
    let mut config = Configuration::default();
    config.common.experimental.set_indexes_v1(true);
    store
        .verify_and_run_migrations(Default::default(), &config)
        .await
        .unwrap();
    let mut cache = VQueuesMetaCache::new_empty(16);
    let at = |logical| UniqueTimestamp::try_from_parts(100, logical).unwrap();

    // Cover service, VO, and workflow invocations, plus state mutations through
    // retained completion/purge, immediate deletion, and inline completion.
    for case in 0..6 {
        let kind = if case >= 3 {
            EntryKind::StateMutation
        } else {
            EntryKind::Invocation
        };
        let qid = VQueueId::custom(3337, format!("index-{case}"));
        let entry_id = EntryId::new(kind, [case + 1; EntryId::REMAINDER_LEN]);
        let base_id = entry_id.to_base_id(qid.partition_key());
        let partition = store.partition_id();
        let initial_run_at = RoughTimestamp::from(at(0));
        let expected = |stage, timestamp, status, next_at| -> Vec<Vec<u8>> {
            expected_keys(
                partition,
                target(case).service(),
                stage,
                timestamp,
                next_at,
                status,
                base_id.canonicalize(Seq::new(1)),
            )
        };

        let mut tx = store.transaction();
        VQueue::<VQueueEvent, _>::get_or_insert_with(&qid, &mut tx, &mut cache, || {
            VQueueMeta::new(at(0), None, LimitKey::None, VQueueLink::None)
        })
        .await
        .unwrap()
        .enqueue_new(
            at(0),
            &target(case),
            1u64,
            None,
            entry_id,
            EntryMetadata::default(),
        );
        tx.commit().await.unwrap();
        drop(tx);
        assert_eq!(
            index_keys(&store),
            expected(Stage::Inbox, at(0), Status::New, initial_run_at)
        );

        // Only the source status record is written: metadata changes must not
        // overwrite the index key and break its Put/SingleDelete lifetime.
        let sequence = store
            .partition_db()
            .rocksdb()
            .inner()
            .as_raw_db()
            .latest_sequence_number();
        let mut tx = store.transaction();
        let header = tx.get_vqueue_entry_status(&base_id).await.unwrap().unwrap();
        let mut metadata = header.metadata().clone();
        metadata.retry_attempts = 1;
        VQueue::<VQueueEvent, _>::get(&qid, &mut tx, &mut cache, None)
            .await
            .unwrap()
            .unwrap()
            .update_entry_metadata(&header, &target(case), &metadata);
        tx.commit().await.unwrap();
        drop(tx);
        assert_eq!(
            store
                .partition_db()
                .rocksdb()
                .inner()
                .as_raw_db()
                .latest_sequence_number(),
            sequence + 1
        );
        assert_eq!(
            index_keys(&store),
            expected(Stage::Inbox, at(0), Status::New, initial_run_at)
        );

        // Both updates write the source and one stats merge. A status-only update
        // also rewrites the two status-bearing indexes, but not the service index.
        for status_only in [true, false] {
            let sequence = store
                .partition_db()
                .rocksdb()
                .inner()
                .as_raw_db()
                .latest_sequence_number();
            let mut tx = store.transaction();
            let header = tx.get_vqueue_entry_status(&base_id).await.unwrap().unwrap();
            let before = EntryStateRef::from_header(&header);
            let mut metadata = header.metadata().clone();
            let after = if status_only {
                EntryStateRef {
                    status: Status::Started,
                    ..before
                }
            } else {
                metadata.deployment = Some("dp_test".into());
                EntryStateRef {
                    metadata: &metadata,
                    ..before
                }
            };
            tx.update_vqueue_entry_status(
                &EntryContext {
                    qid: &qid,
                    target: &target(case),
                },
                before,
                after,
            );
            tx.commit().await.unwrap();
            drop(tx);
            assert_eq!(
                store
                    .partition_db()
                    .rocksdb()
                    .inner()
                    .as_raw_db()
                    .latest_sequence_number(),
                sequence + if status_only { 6 } else { 2 }
            );
            assert_eq!(
                index_keys(&store),
                expected(Stage::Inbox, at(0), Status::Started, initial_run_at)
            );
        }

        if case % 3 < 2 {
            // Yielding an inbox entry updates its timestamp without changing stage.
            let mut tx = store.transaction();
            let header = tx.get_vqueue_entry_status(&base_id).await.unwrap().unwrap();
            VQueue::<VQueueEvent, _>::get(&qid, &mut tx, &mut cache, None)
                .await
                .unwrap()
                .unwrap()
                .yield_entry(
                    at(1),
                    &header,
                    &target(case),
                    None,
                    YieldReason::PartitionLeaderChange,
                );
            tx.commit().await.unwrap();
            drop(tx);
            assert_eq!(
                index_keys(&store),
                expected(Stage::Inbox, at(1), Status::Yielded, initial_run_at)
            );

            let mut tx = store.transaction();
            let header = tx.get_vqueue_entry_status(&base_id).await.unwrap().unwrap();
            VQueue::<VQueueEvent, _>::get(&qid, &mut tx, &mut cache, None)
                .await
                .unwrap()
                .unwrap()
                .run_entry(at(2), &header, &target(case), WaitStats::default());
            tx.commit().await.unwrap();
            drop(tx);
            assert_eq!(
                index_keys(&store),
                expected(Stage::Running, at(2), Status::Started, initial_run_at)
            );
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
                &target(case),
                Status::Succeeded,
                Duration::from_secs(1),
            ),
            1 => queue.end(
                at(3),
                &header,
                &target(case),
                Status::Succeeded,
                Duration::ZERO,
            ),
            _ => queue.run_then_finish(
                at(3),
                &header,
                &target(case),
                WaitStats::default(),
                Status::Succeeded,
            ),
        }
        tx.commit().await.unwrap();
        drop(tx);

        if case % 3 == 0 {
            assert_eq!(
                index_keys(&store),
                expected(
                    Stage::Finished,
                    at(3),
                    Status::Succeeded,
                    RoughTimestamp::from(at(3).to_unix_millis() + Duration::from_secs(1))
                )
            );
            let mut tx = store.transaction();
            let header = tx.get_vqueue_entry_status(&base_id).await.unwrap().unwrap();
            VQueue::<VQueueEvent, _>::get(&qid, &mut tx, &mut cache, None)
                .await
                .unwrap()
                .unwrap()
                .delete(
                    at(4),
                    &EntryContext {
                        qid: &qid,
                        target: &target(case),
                    },
                    EntryStateRef::from_header(&header),
                );
            tx.commit().await.unwrap();
            drop(tx);
        }
        assert!(
            index_keys(&store).is_empty(),
            "case {case} left stale index entries"
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
