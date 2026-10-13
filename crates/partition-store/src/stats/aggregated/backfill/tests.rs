// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::ControlFlow;

use restate_clock::{RoughTimestamp, UniqueTimestamp};
use restate_rocksdb::RocksDbManager;
use restate_storage_api::Transaction;
use restate_storage_api::filter::Filter;
use restate_storage_api::fsm_table::WriteFsmTable;
use restate_storage_api::invocation_status_table::{
    InFlightInvocationMetadata, InvocationStatus, WriteInvocationStatusTable,
};
use restate_storage_api::vqueue_table::stats::EntryStatistics;
use restate_storage_api::vqueue_table::{
    EntryContext, EntryKey, EntryMetadata, EntryStateRef, ReadVQueueTable, Stage, Status,
    WriteVQueueTable,
};
use restate_types::Scope;
use restate_types::config::Configuration;
use restate_types::identifiers::{BaseEntryId, InvocationId, InvocationUuid};
use restate_types::invocation::{InvocationTarget, VirtualObjectHandlerType, WorkflowHandlerType};
use restate_types::logs::Lsn;
use restate_types::partition_table::Partition;
use restate_types::sharding::KeyRange;
use restate_types::state_mut::StateMutationInput;
use restate_types::vqueues::{EntryId, EntryTargetExt};

use crate::stats::StatValueCodec;
use crate::stats::aggregated::StageStatusCounts;
use crate::{PartitionStoreManager, StorageAccess, TableKind};

use super::*;

async fn seed(store: &mut PartitionStore) -> (InvocationId, InvocationStatus) {
    let mut tx = store.transaction();
    let mut last = None;
    for partition_key in [10, 11, 12] {
        let qid = VQueueId::custom(partition_key, "backfill");
        // These rows sort before and after the live entries and must not be counted.
        for byte in [0, 255] {
            tx.put_invocation_status(
                &InvocationId::from_parts(partition_key, InvocationUuid::from_bytes([byte; 16])),
                &InvocationStatus::Invoked(InFlightInvocationMetadata::mock()),
            )
            .unwrap();
        }
        for i in 1..=48u8 {
            let id = InvocationId::from_parts(partition_key, InvocationUuid::from_bytes([i; 16]));
            let target = match i % 4 {
                0 => InvocationTarget::service("service", "run"),
                1 => InvocationTarget::workflow(
                    "workflow",
                    "key",
                    "run",
                    WorkflowHandlerType::Workflow,
                ),
                // A large object group and many different objects sharing a partition key.
                2 => InvocationTarget::virtual_object(
                    "object",
                    "shared",
                    "run",
                    VirtualObjectHandlerType::Exclusive,
                ),
                _ => InvocationTarget::virtual_object(
                    "object",
                    format!("key-{i}"),
                    "run",
                    VirtualObjectHandlerType::Exclusive,
                ),
            }
            .with_scope(Some(Scope::try_non_interned("tenant").unwrap()));
            let mut metadata = InFlightInvocationMetadata::mock_with_vqueue(partition_key);
            metadata.vqueue_id = Some(qid.clone());
            metadata.invocation_target = target.clone();
            let invocation = InvocationStatus::Invoked(metadata);
            tx.put_invocation_status(&id, &invocation).unwrap();
            let key = EntryKey::new(
                false,
                RoughTimestamp::new(10),
                u64::from(i),
                EntryId::from(id),
            );
            let (stage, status) = match i % 6 {
                0 => (Stage::Finished, Status::Succeeded),
                1 => (Stage::Inbox, Status::New),
                2 => (Stage::Inbox, Status::Scheduled),
                3 => (Stage::Running, Status::Started),
                4 => (Stage::Suspended, Status::Started),
                _ => (Stage::Paused, Status::Started),
            };
            let entry_metadata = EntryMetadata {
                deployment: (i % 3 != 0).then(|| format!("dp_{}", i % 5).into()),
                ..EntryMetadata::default()
            };
            let stats = EntryStatistics::new(
                UniqueTimestamp::try_from_parts(100, 1).unwrap(),
                key.run_at(),
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
                    metadata: &entry_metadata,
                    stats: &stats,
                },
            );
            last = Some((id, invocation));
        }

        let input = StateMutationInput {
            service_id: ServiceId::new(
                Some(Scope::try_non_interned("tenant").unwrap()),
                "object",
                "shared",
            ),
            version: None,
            state: [(
                bytes::Bytes::from_static(b"state"),
                bytes::Bytes::from(vec![42; 65536]),
            )]
            .into(),
        };
        let key = EntryKey::new(
            false,
            RoughTimestamp::new(10),
            49u64,
            EntryId::new(EntryKind::StateMutation, [49; 16]),
        );
        let stats = EntryStatistics::new(
            UniqueTimestamp::try_from_parts(100, 1).unwrap(),
            key.run_at(),
        );
        tx.create_vqueue_entry_status(
            &EntryContext {
                qid: &qid,
                target: &input.entry_target_ref(),
            },
            EntryStateRef {
                stage: Stage::Inbox,
                status: Status::New,
                entry_key: &key,
                metadata: &EntryMetadata::default(),
                stats: &stats,
            },
        );
        tx.put_vqueue_input_payload(&qid, key.seq(), key.entry_id(), input);
    }
    tx.put_applied_lsn(Lsn::from(100u64)).unwrap();
    tx.commit().await.unwrap();
    last.unwrap()
}

fn read_all(store: &PartitionStore) -> BTreeMap<(u16, Vec<u8>), StageStatusCounts> {
    let db = store.partition_db();
    let mut options = ReadOptions::default();
    options.set_total_order_seek(true);
    options.set_iterate_range(rocksdb::PrefixRange(KeyKind::Stats.as_bytes()));
    let mut it = db
        .rocksdb()
        .inner()
        .as_raw_db()
        .raw_iterator_cf_opt(db.cf_handle(), options);
    it.seek_to_first();
    let mut result = BTreeMap::new();
    while let Some((key, value)) = it.item() {
        let (prefix, _) = StatKeyPrefix::decode_prefix(key).unwrap();
        assert_eq!(prefix.partition_id(), store.partition_id());
        let counts = StageStatusCounts::deserialize_from(value).unwrap();
        if !counts.is_empty() {
            result.insert(
                (
                    prefix.stat_id_raw(),
                    key[StatKeyPrefix::SERIALIZED_LENGTH..].to_vec(),
                ),
                counts,
            );
        }
        it.next();
    }
    it.status().unwrap();
    result
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn spills_retry_and_activation_match_live_maintenance() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let partition = Partition::new(PartitionId::MIN, KeyRange::FULL);
    let mut store = manager.open(&partition, None).await.unwrap();
    let mut reference = manager
        .open(&Partition::new(PartitionId::from(2), KeyRange::FULL), None)
        .await
        .unwrap();
    let mut config = Configuration::default();
    store
        .verify_and_run_migrations(CancellationToken::new(), &config)
        .await
        .unwrap();
    config.common.experimental.set_track_status_gauges(true);
    reference
        .verify_and_run_migrations(CancellationToken::new(), &config)
        .await
        .unwrap();
    let (last_id, last_status) = seed(&mut store).await;
    seed(&mut reference).await;
    assert!(read_all(&store).is_empty());
    let expected = read_all(&reference);
    assert_eq!(
        expected
            .iter()
            .filter(|((id, _), _)| *id == ServiceLoad::STAT_ID.as_u16())
            .flat_map(|(_, counts)| counts.iter())
            .map(|(_, n)| n)
            .sum::<u64>(),
        147
    );

    // Fail after multiple files have already been ingested. The missing feature marker
    // must let the next attempt clear all partial contributions rather than double count.
    let mut tx = store.transaction();
    tx.delete_invocation_status(&last_id).unwrap();
    tx.commit().await.unwrap();
    drop(tx);
    let err = build(store.partition_db(), &CancellationToken::new(), 512).unwrap_err();
    assert!(err.to_string().contains("missing invocation status"));
    assert!(!read_all(&store).is_empty());
    assert!(!store.storage_features().is_track_status_gauges);
    // SQL must fail rather than expose a partially ingested baseline during startup.
    assert!(
        store
            .scan_service_load(&Filter::All, |_, _| ControlFlow::Continue(()))
            .is_err()
    );
    assert!(
        store
            .scan_deployment_load(&Filter::All, |_, _| ControlFlow::Continue(()))
            .is_err()
    );
    assert!(
        store
            .scan_virtual_object_load(&Filter::All, |_, _| ControlFlow::Continue(()))
            .is_err()
    );
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(matches!(
        run(&store, &cancelled).await,
        Err(MigrationError::MigrationCancelled)
    ));
    assert!(!store.storage_features().is_track_status_gauges);

    let mut tx = store.transaction();
    tx.put_invocation_status(&last_id, &last_status).unwrap();
    tx.commit().await.unwrap();
    drop(tx);
    let files = build(store.partition_db(), &CancellationToken::new(), 512).unwrap();
    assert!(files > 3, "small buffers must force overlapping spills");
    assert_eq!(read_all(&store), expected);

    // A successful unmarked build is also safe to retry, through the real enable path.
    store
        .verify_and_run_migrations(CancellationToken::new(), &config)
        .await
        .unwrap();
    assert!(store.storage_features().is_track_status_gauges);
    assert!(
        store
            .get_storage_features_names()
            .iter()
            .all(|name| name.as_str() != "indexes-v1")
    );
    assert_eq!(read_all(&store), expected);
    // The source was explicitly flushed before ingestion, even though ordinary commits
    // bypass the WAL. It must be readable from RocksDB's persisted tier.
    let mut key = Vec::new();
    InvocationStatusKey::builder()
        .partition_key(last_id.partition_key())
        .invocation_uuid(last_id.invocation_uuid())
        .serialize_to(&mut key);
    assert!(
        store
            .get_durable(TableKind::InvocationStatus, &key)
            .unwrap()
            .is_some()
    );

    // Subsequent live deletion must subtract from the ingested baseline correctly.
    for store in [&mut store, &mut reference] {
        let mut tx = store.transaction();
        let header = tx
            .get_vqueue_entry_status(&BaseEntryId::from(last_id))
            .await
            .unwrap()
            .unwrap();
        let qid = VQueueId::custom(last_id.partition_key(), "backfill");
        tx.delete_vqueue_entry_status(
            &EntryContext {
                qid: &qid,
                target: &last_status.invocation_target().unwrap().entry_target_ref(),
            },
            EntryStateRef::from_header(&header),
        );
        tx.delete_invocation_status(&last_id).unwrap();
        tx.commit().await.unwrap();
    }
    assert_eq!(read_all(&store), read_all(&reference));
    store.partition_db().flush_memtables(true).await.unwrap();
    drop(store);
    manager.close_partition_store(partition.id()).await;
    let mut reopened = manager.open(&partition, None).await.unwrap();
    reopened
        .verify_and_run_migrations(CancellationToken::new(), &config)
        .await
        .unwrap();
    assert!(reopened.storage_features().is_track_status_gauges);
    assert_eq!(read_all(&reopened), read_all(&reference));
}

#[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_source_clears_partial_output_and_cancellation_stops_ingestion() {
    RocksDbManager::init();
    let manager = PartitionStoreManager::create(true).await.unwrap();
    let store = manager
        .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
        .await
        .unwrap();
    let db = store.partition_db();
    let cancel = CancellationToken::new();
    let mut sink = SstSink::new(db, &cancel, 512).unwrap();
    let mut key = Vec::new();
    ServiceLoad::encode_key(
        store.partition_id(),
        ServiceLoadKey::borrowed("stale", Some("run"), EntryKind::Invocation),
        &mut key,
    );
    sink.pending
        .add(&key, StageStatus::new(Stage::Inbox, Status::New), 3)
        .unwrap();
    sink.spill().unwrap();
    assert_eq!(read_all(&store).len(), 1);
    sink.pending
        .add(&key, StageStatus::new(Stage::Inbox, Status::New), 4)
        .unwrap();
    cancel.cancel();
    assert!(sink.spill().unwrap_err().is::<MigrationError>());
    drop(sink);
    assert_eq!(build(db, &CancellationToken::new(), 512).unwrap(), 0);
    assert!(read_all(&store).is_empty());
}
