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

use restate_clock::UniqueTimestamp;
use restate_partition_store::PartitionStore;
use restate_partition_store::index::{InvocationByServiceStageKey, SecondaryIndexKey};
use restate_storage_api::Transaction;
use restate_storage_api::vqueue_table::metadata::{VQueueLink, VQueueMeta};
use restate_storage_api::vqueue_table::stats::WaitStats;
use restate_storage_api::vqueue_table::{
    EntryContext, EntryMetadata, EntryStateRef, EntryStatusHeader, ReadVQueueTable, Stage, Status,
    WriteVQueueTable,
};
use restate_types::LimitKey;
use restate_types::config::Configuration;
use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef, HandlerRef, VQueueId};

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
    let mut prefix = Vec::new();
    InvocationByServiceStageKey::prefix(store.partition_id(), &mut prefix);
    let db = store.partition_db().rocksdb().inner().as_raw_db();
    let cf = db.cf_handle(store.partition().cf_name().as_ref()).unwrap();
    let mut iterator = db.raw_iterator_cf(&cf);
    iterator.seek(&prefix);
    let mut keys = Vec::new();
    while let Some(key) = iterator.key().filter(|key| key.starts_with(&prefix)) {
        assert!(iterator.value().unwrap().is_empty());
        keys.push(key.to_vec());
        iterator.next();
    }
    iterator.status().unwrap();
    keys
}

#[restate_core::test]
async fn invocation_index_follows_vqueue_transitions_and_all_deletion_paths() {
    let mut store = storage_test_environment().await;
    let mut config = Configuration::default();
    config.common.experimental.set_indexes_v1(true);
    store
        .verify_and_run_migrations(Default::default(), &config)
        .await
        .unwrap();
    let mut cache = VQueuesMetaCache::new_empty(16);
    let at = |logical| UniqueTimestamp::try_from_parts(100, logical).unwrap();

    // Service, VO, and workflow invocations are indexed. State mutations are not.
    // Exercise retained completion/purge, immediate deletion, and inline completion.
    for case in 0..4 {
        let kind = if case == 3 {
            EntryKind::StateMutation
        } else {
            EntryKind::Invocation
        };
        let qid = VQueueId::custom(3337, format!("index-{case}"));
        let entry_id = EntryId::new(kind, [case + 1; EntryId::REMAINDER_LEN]);
        let base_id = entry_id.to_base_id(qid.partition_key());
        let partition = store.partition_id();
        let expected = |stage, timestamp| -> Vec<Vec<u8>> {
            entry_id
                .to_invocation_id(qid.partition_key())
                .map(|id| {
                    let mut key = Vec::new();
                    InvocationByServiceStageKey::borrowed(
                        target(case).service(),
                        stage,
                        Reverse(timestamp),
                        id,
                    )
                    .encode_key(partition, &mut key);
                    key
                })
                .into_iter()
                .collect()
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
        assert_eq!(index_keys(&store), expected(Stage::Inbox, at(0)));

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
        assert_eq!(index_keys(&store), expected(Stage::Inbox, at(0)));

        // An unchanged index projection must not suppress another consumer: each
        // status-only or deployment-only update writes the source and one merge.
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
            assert_eq!(index_keys(&store), expected(Stage::Inbox, at(0)));
        }

        if case < 2 {
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
            assert_eq!(index_keys(&store), expected(Stage::Inbox, at(1)));

            let mut tx = store.transaction();
            let header = tx.get_vqueue_entry_status(&base_id).await.unwrap().unwrap();
            VQueue::<VQueueEvent, _>::get(&qid, &mut tx, &mut cache, None)
                .await
                .unwrap()
                .unwrap()
                .run_entry(at(2), &header, &target(case), WaitStats::default());
            tx.commit().await.unwrap();
            drop(tx);
            assert_eq!(index_keys(&store), expected(Stage::Running, at(2)));
        }

        let mut tx = store.transaction();
        let header = tx.get_vqueue_entry_status(&base_id).await.unwrap().unwrap();
        let queue = VQueue::<VQueueEvent, _>::get(&qid, &mut tx, &mut cache, None)
            .await
            .unwrap()
            .unwrap();
        match case {
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

        if case == 0 {
            assert_eq!(index_keys(&store), expected(Stage::Finished, at(3)));
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
