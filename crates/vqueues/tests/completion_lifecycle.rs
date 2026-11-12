// Copyright (c) 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use restate_clock::{UniqueTimestamp, time::MillisSinceEpoch};
use restate_partition_store::PartitionStoreManager;
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::lock_table::LoadLocks;
use restate_storage_api::vqueue_table::metadata::{VQueueLink, VQueueMeta};
use restate_storage_api::vqueue_table::stats::WaitStats;
use restate_storage_api::vqueue_table::{
    EntryContext, EntryMetadata, EntryStateRef, EntryStatusHeader, ReadVQueueTable,
    ScanVQueueTable, Stage, Status, WriteVQueueTable,
};
use restate_types::partition_table::Partition;
use restate_types::sharding::{KeyRange, PartitionId};
use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef, HandlerRef, VQueueId};
use restate_types::{LimitKey, LockName, Scope};
use restate_vqueues::{EventDetails, VQueue, VQueueEvent, VQueuesMetaCache};

/// Immediate completion must preserve cleanup, scheduler events and queue metrics,
/// including metadata changed earlier in the same transaction. Retained completion
/// must persist its Finished row and inactive queue state until deletion in a later
/// transaction. A neighboring queue survives.
#[restate_core::test]
async fn finishing_preserves_cleanup_events_and_queue_metrics() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let mut store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    let at = |seconds: u64| {
        UniqueTimestamp::try_from_unix_millis(MillisSinceEpoch::new(
            1_744_000_000_000 + seconds * 1_000,
        ))
        .unwrap()
    };
    let waits = WaitStats {
        blocked_on_lock_ms: 37,
        ..WaitStats::default()
    };

    for (n, (service, stage)) in (1u8..).zip([
        ("inbox", Stage::Inbox),
        ("running", Stage::Running),
        ("suspended", Stage::Suspended),
        ("paused", Stage::Paused),
        ("inline", Stage::Inbox),
        ("retained", Stage::Running),
    ]) {
        let inline = service == "inline";
        let retained = service == "retained";
        let kind = if inline {
            EntryKind::StateMutation
        } else {
            EntryKind::Invocation
        };
        let handler = if inline {
            HandlerRef::StateMutation
        } else {
            HandlerRef::UserHandler("run")
        };
        let target = EntryTargetRef::VirtualObject {
            service,
            key: "key",
            scope: Some("tenant"),
            handler,
        };
        let qid = VQueueId::custom(3337, service);
        let neighbor_qid = VQueueId::custom(3337, format!("{service}-neighbor"));
        let lock = LockName::parse_unchecked(&format!("{service}/key")).unwrap();
        let scope = Some(Scope::try_non_interned("tenant").unwrap());
        let entry = EntryId::new(kind, [n; EntryId::REMAINDER_LEN]);
        let neighbor = EntryId::new(kind, [n + 20; EntryId::REMAINDER_LEN]);
        let id = entry.to_base_id(qid.partition_key());
        let mut cache = VQueuesMetaCache::new_empty(16);
        let metadata = EntryMetadata {
            deployment: Some("dp_old".into()),
            ..Default::default()
        };
        let mut tx = store.transaction();
        for (queue_id, entry_id) in [(&qid, entry), (&neighbor_qid, neighbor)] {
            VQueue::<VQueueEvent, _>::get_or_insert_with(queue_id, &mut tx, &mut cache, || {
                VQueueMeta::new(
                    at(0),
                    scope.clone(),
                    LimitKey::None,
                    VQueueLink::Lock(lock.clone()),
                )
            })
            .await
            .unwrap()
            .enqueue_new(at(0), &target, 7u64, None, entry_id, metadata.clone());
            tx.put_vqueue_input_payload(queue_id, 7u64, &entry_id, metadata.clone());
        }
        if stage != Stage::Inbox {
            let header = tx.get_vqueue_entry_status(&id).await.unwrap().unwrap();
            VQueue::<VQueueEvent, _>::get(&qid, &mut tx, &mut cache, None)
                .await
                .unwrap()
                .unwrap()
                .run_entry(at(2), &header, &target, waits);
            let header = tx.get_vqueue_entry_status(&id).await.unwrap().unwrap();
            let mut queue = VQueue::<VQueueEvent, _>::get(&qid, &mut tx, &mut cache, None)
                .await
                .unwrap()
                .unwrap();
            match stage {
                Stage::Suspended => queue.suspend_entry(at(3), &header, &target),
                Stage::Paused => queue.pause_entry(at(3), &header, &target),
                _ => {}
            }
        }
        tx.commit().await.unwrap();
        drop(tx);
        let mut locks = Vec::new();
        store
            .partition_db()
            .scan_all_locked(|s, l| locks.push((s, l)))
            .unwrap();
        assert_eq!(
            locks.contains(&(scope.clone(), lock.clone())),
            stage != Stage::Inbox
        );

        let mut tx = store.transaction();
        let header = tx.get_vqueue_entry_status(&id).await.unwrap().unwrap();
        let new_metadata = EntryMetadata {
            deployment: Some("dp_new".into()),
            ..metadata.clone()
        };
        VQueue::<VQueueEvent, _>::get(&qid, &mut tx, &mut cache, None)
            .await
            .unwrap()
            .unwrap()
            .update_entry_metadata(&header, &target, &new_metadata);
        let header = tx.get_vqueue_entry_status(&id).await.unwrap().unwrap();
        assert_eq!(header.metadata().deployment.as_deref(), Some("dp_new"));
        let mut events = Vec::new();
        let queue = VQueue::<VQueueEvent, _>::get(&qid, &mut tx, &mut cache, Some(&mut events))
            .await
            .unwrap()
            .unwrap();
        let handle = queue.handle();
        if inline {
            queue.run_then_finish(at(5), &header, &target, waits, Status::Succeeded);
        } else {
            queue.end(
                at(5),
                &header,
                &target,
                Status::Cancelled,
                if retained {
                    Duration::from_secs(60)
                } else {
                    Duration::ZERO
                },
            );
        }
        let mut removed_keys = vec![*header.entry_key()];
        let stats = cache.get(handle).unwrap().meta().stats();
        assert_eq!(stats.last_finish_at(), Some(at(5)), "{service}");
        assert_eq!(
            (
                stats.num_inbox(),
                stats.num_running(),
                stats.num_paused(),
                stats.num_suspended(),
                stats.num_finished()
            ),
            (0, 0, 0, 0, u64::from(retained))
        );
        if inline || stage != Stage::Inbox {
            assert_eq!(stats.avg_wait_stats().blocked_on_lock_ms, 37);
            assert_eq!(
                stats.avg_queue_duration_ms(),
                if inline { 5_000 } else { 2_000 }
            );
        }
        if inline || stage == Stage::Running {
            assert_eq!(stats.avg_end_to_end_duration_ms(), 5_000);
            assert_eq!(stats.avg_run_duration_ms(), if inline { 0 } else { 3_000 });
        }
        let details = events
            .iter()
            .flat_map(|event| {
                assert_eq!(event.queue, handle);
                &event.updates
            })
            .collect::<Vec<_>>();
        assert_eq!(
            details.len(),
            if retained { 1 } else { 2 },
            "{service}: {events:?}"
        );
        if stage == Stage::Inbox {
            assert!(
                matches!(details[0], EventDetails::RemovedFromInbox(key) if key == header.entry_key())
            );
        } else {
            assert!(
                matches!(details[0], EventDetails::LockReleased { scope: s, lock_name } if s == &scope && lock_name == &lock)
            );
        }
        if retained {
            tx.commit().await.unwrap();
            drop(tx);

            let mut active = Vec::new();
            store
                .partition_db()
                .scan_active_vqueues(|qid, _| active.push(qid))
                .unwrap();
            assert!(!active.contains(&qid));
            assert!(active.contains(&neighbor_qid));

            // Reload committed state rather than reusing completion's cache or write batch.
            events.clear();
            cache = VQueuesMetaCache::new_empty(16);
            tx = store.transaction();
            let finished = tx.get_vqueue_entry_status(&id).await.unwrap().unwrap();
            assert_eq!(finished.stage(), Stage::Finished);
            assert!(!finished.has_lock());
            assert_eq!(finished.status(), Status::Cancelled);
            assert_eq!(finished.metadata().deployment.as_deref(), Some("dp_new"));
            assert!(
                tx.get_vqueue_inbox(&qid, Stage::Finished, finished.entry_key())
                    .unwrap()
                    .is_some()
            );
            removed_keys.push(*finished.entry_key());
            let queue = VQueue::<VQueueEvent, _>::get(&qid, &mut tx, &mut cache, Some(&mut events))
                .await
                .unwrap()
                .unwrap();
            assert!(!queue.meta().is_active());
            assert_eq!(queue.meta().stats().num_finished(), 1);
            assert_eq!(queue.meta().stats().last_finish_at(), Some(at(5)));
            let handle = queue.handle();
            queue.delete(
                at(6),
                &EntryContext {
                    qid: &qid,
                    target: &target,
                },
                EntryStateRef::from_header(&finished),
            );
            assert_eq!(cache.get(handle).unwrap().meta().stats().num_finished(), 0);
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].queue, handle);
            assert!(matches!(
                events[0].updates.as_slice(),
                [EventDetails::VQueuePurged]
            ));
        } else {
            assert!(matches!(details[1], EventDetails::VQueuePurged));
        }
        tx.commit().await.unwrap();
        drop(tx);

        let mut tx = store.transaction();
        assert!(tx.get_vqueue_entry_status(&id).await.unwrap().is_none());
        assert!(tx.get_vqueue(&qid).await.unwrap().is_none());
        assert!(
            tx.get_vqueue_input_payload::<EntryMetadata>(&qid, 7u64, &entry)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            tx.get_vqueue_input_payload::<EntryMetadata>(&neighbor_qid, 7u64, &neighbor)
                .await
                .unwrap()
                .is_some()
        );
        for key in removed_keys {
            for stage in [
                Stage::Inbox,
                Stage::Running,
                Stage::Suspended,
                Stage::Paused,
                Stage::Finished,
            ] {
                assert!(tx.get_vqueue_inbox(&qid, stage, &key).unwrap().is_none());
            }
        }
        drop(tx);
        let mut locks = Vec::new();
        store
            .partition_db()
            .scan_all_locked(|s, l| locks.push((s, l)))
            .unwrap();
        assert!(!locks.contains(&(scope, lock)));
        let mut active = Vec::new();
        store
            .partition_db()
            .scan_active_vqueues(|qid, _| active.push(qid))
            .unwrap();
        assert!(!active.contains(&qid));
        assert!(active.contains(&neighbor_qid));
    }
}
