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
    BusyVQueueKey, EntryByStageKey, EntryByStageServiceKey, EntryByVirtualObjectStageKey,
    EntryNextAtByStageKey, EntryNextAtByStageServiceKey, EntryNextAtByVirtualObjectStageKey,
    IndexId, SecondaryIndexKey,
};
use restate_partition_store::keys::{IndexKeyPrefix, KeyKind};
use restate_partition_store::stats::StatValueCodec;
use restate_partition_store::stats::aggregated::StageCounts;
use restate_storage_api::Transaction;
use restate_storage_api::vqueue_table::metadata::{
    Action, MoveMetrics, Update, VQueueLink, VQueueMeta,
};
use restate_storage_api::vqueue_table::stats::{EntryStatistics, WaitStats};
use restate_storage_api::vqueue_table::{
    EntryContext, EntryKey, EntryMetadata, EntryStateRef, EntryStatusHeader, ReadVQueueTable,
    Stage, Status, VQueueDisposition, WriteVQueueTable,
};
use restate_types::config::Configuration;
use restate_types::identifiers::CanonicalEntryId;
use restate_types::sharding::PartitionId;
use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef, HandlerRef, Seq, VQueueId};
use restate_types::{LimitKey, Scope};

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

fn index_keys(store: &PartitionStore) -> Vec<Vec<u8>> {
    let prefix = KeyKind::SecondaryIndex.as_bytes();
    let db = store.partition_db().rocksdb().inner().as_raw_db();
    let cf = db.cf_handle(store.partition().cf_name().as_ref()).unwrap();
    let mut iterator = db.raw_iterator_cf(&cf);
    iterator.seek(prefix);
    let mut keys = Vec::new();
    while let Some(key) = iterator.key().filter(|key| key.starts_with(prefix)) {
        if IndexKeyPrefix::decode_prefix(key).unwrap().0.index_id() == Some(IndexId::BusyVQueue) {
            iterator.next();
            continue;
        }
        assert!(iterator.value().unwrap().is_empty());
        keys.push(key.to_vec());
        iterator.next();
    }
    iterator.status().unwrap();
    keys
}

fn expected_keys(
    partition: PartitionId,
    target: &EntryTargetRef<'_>,
    stage: Stage,
    transitioned_at: UniqueTimestamp,
    next_at: RoughTimestamp,
    id: CanonicalEntryId,
) -> Vec<Vec<u8>> {
    let mut keys = vec![Vec::new(); 4];
    EntryByStageServiceKey::borrowed(stage, target.service(), Reverse(transitioned_at), id)
        .encode_key(partition, &mut keys[0]);
    EntryByStageKey::borrowed(stage, Reverse(transitioned_at), id)
        .encode_key(partition, &mut keys[1]);
    EntryNextAtByStageKey::borrowed(stage, next_at, id.seq(), id)
        .encode_key(partition, &mut keys[2]);
    EntryNextAtByStageServiceKey::borrowed(stage, target.service(), next_at, id.seq(), id)
        .encode_key(partition, &mut keys[3]);
    if let Some(key) = target.virtual_object_key() {
        let mut transitioned = Vec::new();
        EntryByVirtualObjectStageKey::borrowed(
            target.service(),
            target.scope(),
            key,
            stage,
            Reverse(transitioned_at),
            id,
        )
        .encode_key(partition, &mut transitioned);
        keys.push(transitioned);
        let mut next = Vec::new();
        EntryNextAtByVirtualObjectStageKey::borrowed(
            target.service(),
            target.scope(),
            key,
            stage,
            next_at,
            id.seq(),
            id,
        )
        .encode_key(partition, &mut next);
        keys.push(next);
    }
    keys.sort();
    keys
}

fn busy_vqueue_rows(store: &PartitionStore) -> Vec<(BusyVQueueKey, StageCounts)> {
    let mut prefix = Vec::new();
    BusyVQueueKey::prefix(store.partition_id(), &mut prefix);
    let db = store.partition_db().rocksdb().inner().as_raw_db();
    let cf = db.cf_handle(store.partition().cf_name().as_ref()).unwrap();
    let mut iterator = db.raw_iterator_cf(&cf);
    iterator.seek(&prefix);
    let mut rows = Vec::new();
    while let Some(key) = iterator.key().filter(|key| key.starts_with(&prefix)) {
        let (_, payload) = IndexKeyPrefix::decode_prefix(key).unwrap();
        rows.push((
            payload
                .into_decoder::<BusyVQueueKey>()
                .decode_all()
                .unwrap(),
            StageCounts::deserialize_from(iterator.value().unwrap()).unwrap(),
        ));
        iterator.next();
    }
    iterator.status().unwrap();
    rows
}

#[restate_core::test]
async fn busy_vqueue_index_covers_counts_and_tracks_metadata_lifetime() {
    let mut store = storage_test_environment().await;
    let qid = VQueueId::custom(3337, "busy-index");
    let at = |logical| UniqueTimestamp::try_from_parts(100, logical).unwrap();
    let new_meta = || {
        VQueueMeta::new(
            at(0),
            Some(Scope::try_non_interned("tenant").unwrap()),
            LimitKey::None,
            VQueueLink::None,
        )
    };
    let mut meta = new_meta();

    // Feature-disabled lifecycle writes must not create index entries.
    let mut tx = store.transaction();
    tx.create_vqueue(&qid, &meta);
    tx.delete_vqueue(&qid, &meta);
    tx.commit().await.unwrap();
    drop(tx);
    assert!(busy_vqueue_rows(&store).is_empty());

    let mut config = Configuration::default();
    config.common.experimental.set_indexes_v1(true);
    store
        .verify_and_run_migrations(Default::default(), &config)
        .await
        .unwrap();
    let observer = store.clone();
    let assert_row = |meta: &VQueueMeta, expected: &[(Stage, u64)]| {
        let rows = busy_vqueue_rows(&observer);
        assert_eq!(rows.len(), 1, "exactly one entry, with no stale keys");
        let (key, counts) = &rows[0];
        assert_eq!(key.vqueue_id, qid);
        assert_eq!(key.scope.as_deref(), Some("tenant"));
        assert_eq!(key.total_non_completed.0, meta.len());
        assert_eq!(key.last_modified.0, meta.stats().last_modified_at());
        assert_eq!(counts.iter().collect::<Vec<_>>(), expected);
    };
    let mut tx = store.transaction();
    tx.create_vqueue(&qid, &meta);
    assert!(busy_vqueue_rows(&observer).is_empty());
    tx.commit().await.unwrap();
    drop(tx);
    assert_row(&meta, &[]);

    let move_to = |prev_stage, next_stage, ts| {
        Update::new(
            ts,
            Action::Move {
                prev_stage,
                next_stage,
                metrics: MoveMetrics {
                    last_transition_at: at(0),
                    has_started: true,
                    first_runnable_at: at(0).to_unix_millis(),
                    scheduler_wait_stats: None,
                },
            },
        )
    };
    for (previous, next, ts) in [
        (None, Stage::Inbox, at(1)),
        (Some(Stage::Inbox), Stage::Running, at(2)),
        (Some(Stage::Running), Stage::Suspended, at(3)),
    ] {
        let mut tx = store.transaction();
        assert_eq!(
            tx.update_vqueue(&qid, &mut meta, &move_to(previous, next, ts)),
            VQueueDisposition::Retained
        );
        tx.commit().await.unwrap();
        drop(tx);
        assert_row(&meta, &[(next, 1)]);
    }

    // Counts change at the same key, repeatedly in one batch. Each replacement
    // must start a new Put/SingleDelete lifetime, even without a timestamp change.
    let rocksdb = observer.partition_db().rocksdb().clone();
    rocksdb.clone().flush_all().await.unwrap();
    let timestamp = meta.stats().last_modified_at();
    let mut tx = store.transaction();
    for (previous, next) in [
        (Stage::Suspended, Stage::Paused),
        (Stage::Paused, Stage::Suspended),
        (Stage::Suspended, Stage::Paused),
    ] {
        assert_eq!(
            tx.update_vqueue(&qid, &mut meta, &move_to(Some(previous), next, at(4))),
            VQueueDisposition::Retained
        );
        assert_eq!(meta.stats().last_modified_at(), timestamp);
    }
    tx.commit().await.unwrap();
    drop(tx);
    rocksdb.clone().flush_all().await.unwrap();
    rocksdb
        .clone()
        .compact_all(Default::default())
        .await
        .unwrap();
    assert_row(&meta, &[(Stage::Paused, 1)]);

    // Pausing the queue changes no projected fields: only metadata is written.
    let db = observer.partition_db().rocksdb().inner().as_raw_db();
    let sequence = db.latest_sequence_number();
    let mut tx = store.transaction();
    assert_eq!(
        tx.update_vqueue(&qid, &mut meta, &Update::new(at(5), Action::PauseVQueue {})),
        VQueueDisposition::Retained
    );
    tx.commit().await.unwrap();
    drop(tx);
    assert_eq!(db.latest_sequence_number(), sequence + 1);
    assert_row(&meta, &[(Stage::Paused, 1)]);

    // Rolling back the transaction also rolls back covering-value changes.
    let mut uncommitted = meta.clone();
    let mut tx = store.transaction();
    let _ = tx.update_vqueue(
        &qid,
        &mut uncommitted,
        &move_to(Some(Stage::Paused), Stage::Finished, at(6)),
    );
    drop(tx);
    assert_row(&meta, &[(Stage::Paused, 1)]);

    let mut tx = store.transaction();
    assert_eq!(
        tx.update_vqueue(
            &qid,
            &mut meta,
            &move_to(Some(Stage::Paused), Stage::Finished, at(6))
        ),
        VQueueDisposition::Retained
    );
    tx.commit().await.unwrap();
    drop(tx);
    assert_eq!(meta.len(), 0);
    assert_row(&meta, &[(Stage::Finished, 1)]);

    // A paused empty queue remains indexed; resuming it purges metadata and index.
    let mut tx = store.transaction();
    assert_eq!(
        tx.update_vqueue(
            &qid,
            &mut meta,
            &Update::new(
                at(7),
                Action::RemoveEntry {
                    stage: Stage::Finished
                }
            )
        ),
        VQueueDisposition::Retained
    );
    tx.commit().await.unwrap();
    drop(tx);
    assert_row(&meta, &[]);
    let mut tx = store.transaction();
    assert_eq!(
        tx.update_vqueue(
            &qid,
            &mut meta,
            &Update::new(at(8), Action::ResumeVQueue {})
        ),
        VQueueDisposition::Purged
    );
    assert!(tx.get_vqueue(&qid).await.unwrap().is_none());
    tx.commit().await.unwrap();
    drop(tx);
    assert!(busy_vqueue_rows(&store).is_empty());

    // Both cached and uncached explicit deletion paths have the previous metadata.
    // Recreating and deleting the same key in that batch must leave no stale row.
    for cached in [false, true] {
        let meta = new_meta();
        let mut cache = VQueuesMetaCache::new_empty(16);
        let mut tx = store.transaction();
        tx.create_vqueue(&qid, &meta);
        if cached {
            assert!(
                VQueue::<VQueueEvent, _>::get(&qid, &mut tx, &mut cache, None)
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        assert!(cache.purge_meta_if_obsolete(&mut tx, &qid).await.unwrap());
        tx.create_vqueue(&qid, &meta);
        tx.delete_vqueue(&qid, &meta);
        tx.commit().await.unwrap();
        drop(tx);
        assert!(busy_vqueue_rows(&store).is_empty());
    }
    rocksdb.clone().flush_all().await.unwrap();
    rocksdb.compact_all(Default::default()).await.unwrap();
    assert!(busy_vqueue_rows(&store).is_empty());
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
    for case in [0, 1] {
        let qid = VQueueId::custom(3337, "incarnations");
        let target = target(case);
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
                &target,
                Stage::Inbox,
                at,
                key.run_at(),
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

        // A schedule-only change rewrites only the next-at indexes and source status.
        let schedule_writes = if target.virtual_object_key().is_some() {
            7
        } else {
            5
        };
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
            sequence + 1 + schedule_writes
        );
        let rescheduled_keys = expected_keys(
            store.partition_id(),
            &target,
            Stage::Inbox,
            at,
            RoughTimestamp::MAX,
            rescheduled_key.to_canonical_entry_id(qid.partition_key()),
        );
        assert_eq!(index_keys(&store), rescheduled_keys);

        // Status is not an index dimension: only source status and service stats change.
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
            sequence + 1 + schedule_writes + 2
        );
        assert_eq!(index_keys(&store), rescheduled_keys);

        // A transition-time-only change rewrites only the newest-transition indexes.
        let mut later_stats = stats.clone();
        later_stats.transitioned_at = UniqueTimestamp::try_from_parts(101, 1).unwrap();
        let later = EntryStateRef {
            stats: &later_stats,
            ..scheduled
        };
        let mut tx = store.transaction();
        tx.update_vqueue_entry_status(&context, scheduled, later);
        tx.commit().await.unwrap();
        drop(tx);
        assert_eq!(
            index_keys(&store),
            expected_keys(
                store.partition_id(),
                &target,
                Stage::Inbox,
                later_stats.transitioned_at,
                RoughTimestamp::MAX,
                rescheduled_key.to_canonical_entry_id(qid.partition_key()),
            )
        );
        assert_eq!(
            store
                .partition_db()
                .rocksdb()
                .inner()
                .as_raw_db()
                .latest_sequence_number(),
            sequence + 1 + schedule_writes + 2 + schedule_writes
        );

        let mut tx = store.transaction();
        tx.delete_vqueue_entry_status(&context, later);
        tx.commit().await.unwrap();
        drop(tx);
        assert!(index_keys(&store).is_empty());
    }
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
        let expected = |stage, timestamp, next_at| -> Vec<Vec<u8>> {
            expected_keys(
                partition,
                &target(case),
                stage,
                timestamp,
                next_at,
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
            expected(Stage::Inbox, at(0), initial_run_at)
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
            expected(Stage::Inbox, at(0), initial_run_at)
        );

        // Both updates write the source and one stats merge, leaving every index unchanged.
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
                sequence + 2
            );
            assert_eq!(
                index_keys(&store),
                expected(Stage::Inbox, at(0), initial_run_at)
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
                expected(Stage::Inbox, at(1), initial_run_at)
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
                expected(Stage::Running, at(2), initial_run_at)
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
