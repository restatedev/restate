// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::stats::deployment_load::DeploymentLoad;
use restate_storage_api::stats::service_load::ServiceLoad;
use restate_storage_api::stats::virtual_object_load::VirtualObjectLoad;
use restate_storage_api::vqueue_table::{EntryChange, EntryContext, Stage};

use crate::PartitionStoreTransaction;
use crate::stats::EncodeStatKey;
use crate::stats::aggregated::{
    AggregatedStat, AggregatedStatsMut, DeploymentLoadKey, ServiceLoadKey, StageGauge, StageStatus,
    VirtualObjectLoadKey,
};

/// Updates each aggregate's contribution independently from the same source change.
pub(super) fn on_entry_change(
    storage: &mut PartitionStoreTransaction<'_>,
    context: &EntryContext<'_>,
    change: &EntryChange<'_>,
) {
    let kind = match change {
        EntryChange::Insert { after } | EntryChange::Update { after, .. } => after.entry_key.kind(),
        EntryChange::Delete { before } => before.entry_key.kind(),
    };
    let target = context.target;
    let before = change.before();
    let after = change.after();
    let mut stats = storage.aggregated_stats();

    let old_bucket = before.map(|entry| StageStatus::new(entry.stage, entry.status));
    let new_bucket = after.map(|entry| StageStatus::new(entry.stage, entry.status));
    if old_bucket != new_bucket {
        let key = ServiceLoadKey::borrowed(target.service(), target.handler(), kind);
        match (old_bucket, new_bucket) {
            (None, Some(new)) => stats.increment_stage_status::<ServiceLoad, _>(key, new),
            (Some(old), None) => stats.decrement_stage_status::<ServiceLoad, _>(key, old),
            (Some(old), Some(new)) => {
                stats.transition_stage_status::<ServiceLoad, _>(key, old, new)
            }
            (None, None) => {}
        }
    }

    if let Some(key) = target.virtual_object_key() {
        update_stage::<VirtualObjectLoad, _>(
            &mut stats,
            VirtualObjectLoadKey::borrowed(
                target.service(),
                target.scope(),
                key,
                target.handler(),
                kind,
                context.qid.partition_key(),
            ),
            before.map(|entry| entry.stage),
            after.map(|entry| entry.stage),
        );
    }

    let old_deployment = before.and_then(|entry| {
        entry
            .metadata
            .deployment
            .as_deref()
            .map(|id| (id, entry.stage))
    });
    let new_deployment = after.and_then(|entry| {
        entry
            .metadata
            .deployment
            .as_deref()
            .map(|id| (id, entry.stage))
    });
    match (old_deployment, new_deployment) {
        (Some((old, old_stage)), Some((new, new_stage))) if old == new => {
            update_stage::<DeploymentLoad, _>(
                &mut stats,
                DeploymentLoadKey::borrowed(old, target.service()),
                Some(old_stage),
                Some(new_stage),
            );
        }
        (old, new) => {
            if let Some((id, stage)) = old {
                stats.decrement_stage::<DeploymentLoad, _>(
                    DeploymentLoadKey::borrowed(id, target.service()),
                    stage,
                );
            }
            if let Some((id, stage)) = new {
                stats.increment_stage::<DeploymentLoad, _>(
                    DeploymentLoadKey::borrowed(id, target.service()),
                    stage,
                );
            }
        }
    }
}

/// A stable aggregate key needs at most one merge, including bucket transitions.
fn update_stage<S, K>(
    stats: &mut AggregatedStatsMut<'_, '_>,
    key: K,
    before: Option<Stage>,
    after: Option<Stage>,
) where
    S: AggregatedStat<Aggregation = StageGauge>,
    K: EncodeStatKey<S>,
{
    if before == after {
        return;
    }
    match (before, after) {
        (None, Some(new)) => stats.increment_stage::<S, _>(key, new),
        (Some(old), None) => stats.decrement_stage::<S, _>(key, old),
        (Some(old), Some(new)) => stats.transition_stage::<S, _>(key, old, new),
        (None, None) => {}
    }
}

#[cfg(test)]
mod tests {
    use restate_clock::{RoughTimestamp, UniqueTimestamp};
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::Transaction;
    use restate_storage_api::vqueue_table::stats::EntryStatistics;
    use restate_storage_api::vqueue_table::{
        EntryKey, EntryMetadata, EntryStateRef, Status, WriteVQueueTable,
    };
    use restate_types::config::Configuration;
    use restate_types::partition_table::Partition;
    use restate_types::sharding::{KeyRange, PartitionId};
    use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef, HandlerRef, VQueueId};

    use crate::stats::{Stat, StatValueCodec};
    use crate::{PartitionStoreManager, StorageAccess, TableKind};

    use super::*;

    fn read<S: Stat>(tx: &PartitionStoreTransaction<'_>, key: impl EncodeStatKey<S>) -> S::Value
    where
        S::Value: Default,
    {
        let mut encoded = Vec::new();
        S::encode_key(tx.partition_id(), key, &mut encoded);
        tx.get(TableKind::Stats, encoded)
            .unwrap()
            .map(|value| S::Value::deserialize_from(&value).unwrap())
            .unwrap_or_default()
    }

    #[restate_core::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lifecycle_changes_update_independent_contributions() {
        RocksDbManager::init();
        let manager = PartitionStoreManager::create(true).await.unwrap();
        let mut store = manager
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap();
        let mut config = Configuration::default();
        config.common.experimental.set_indexes_v1(true);
        store
            .verify_and_run_migrations(Default::default(), &config)
            .await
            .unwrap();

        let qid = VQueueId::custom(3337, "stats-lifecycle");
        let target = EntryTargetRef::VirtualObject {
            service: "counter",
            key: "a",
            scope: Some("tenant"),
            handler: HandlerRef::UserHandler("increment"),
        };
        let context = EntryContext {
            qid: &qid,
            target: &target,
        };
        let key = EntryKey::new(
            false,
            RoughTimestamp::new(10),
            1u64,
            EntryId::new(EntryKind::Invocation, [1; 16]),
        );
        let stats = EntryStatistics::new(
            UniqueTimestamp::try_from_parts(100, 1).unwrap(),
            key.run_at(),
        );
        let metadata = EntryMetadata::default();
        let initial = EntryStateRef {
            stage: Stage::Inbox,
            status: Status::New,
            entry_key: &key,
            metadata: &metadata,
            stats: &stats,
        };
        let service_key =
            ServiceLoadKey::borrowed("counter", Some("increment"), EntryKind::Invocation);
        let object_key = VirtualObjectLoadKey::borrowed(
            "counter",
            Some("tenant"),
            "a",
            Some("increment"),
            EntryKind::Invocation,
            qid.partition_key(),
        );
        let deployment_a = DeploymentLoadKey::borrowed("dp_a", "counter");
        let deployment_b = DeploymentLoadKey::borrowed("dp_b", "counter");
        let mut tx = store.transaction();
        tx.create_vqueue_entry_status(&context, initial);
        assert_eq!(
            read::<ServiceLoad>(&tx, &service_key)
                .iter()
                .collect::<Vec<_>>(),
            [(StageStatus::new(Stage::Inbox, Status::New), 1)]
        );
        assert_eq!(
            read::<VirtualObjectLoad>(&tx, &object_key)
                .iter()
                .collect::<Vec<_>>(),
            [(Stage::Inbox, 1)]
        );

        // A status-only update must not be gated by stage or transition timestamp.
        let started = EntryStateRef {
            status: Status::Started,
            ..initial
        };
        tx.update_vqueue_entry_status(&context, initial, started);
        assert_eq!(
            read::<ServiceLoad>(&tx, &service_key)
                .iter()
                .collect::<Vec<_>>(),
            [(StageStatus::new(Stage::Inbox, Status::Started), 1)]
        );
        assert_eq!(
            read::<VirtualObjectLoad>(&tx, &object_key)
                .iter()
                .collect::<Vec<_>>(),
            [(Stage::Inbox, 1)]
        );

        let mut current = started;
        // Assign, repin, and remove a deployment without changing stage/status.
        let metadata_a = EntryMetadata {
            deployment: Some("dp_a".into()),
            ..metadata.clone()
        };
        let metadata_b = EntryMetadata {
            deployment: Some("dp_b".into()),
            ..metadata.clone()
        };
        for (next_metadata, expected_a, expected_b) in
            [(&metadata_a, 1, 0), (&metadata_b, 0, 1), (&metadata, 0, 0)]
        {
            let next = EntryStateRef {
                metadata: next_metadata,
                ..current
            };
            tx.update_vqueue_entry_status(&context, current, next);
            assert_eq!(
                read::<DeploymentLoad>(&tx, &deployment_a)
                    .iter()
                    .map(|(_, count)| count)
                    .sum::<u64>(),
                expected_a
            );
            assert_eq!(
                read::<DeploymentLoad>(&tx, &deployment_b)
                    .iter()
                    .map(|(_, count)| count)
                    .sum::<u64>(),
                expected_b
            );
            current = next;
        }

        // Changes irrelevant to all three projections must produce no stats writes.
        let mut newer_stats = stats.clone();
        newer_stats.transitioned_at = UniqueTimestamp::try_from_parts(200, 1).unwrap();
        let newer = EntryStateRef {
            stats: &newer_stats,
            ..current
        };
        let size = tx.estimated_size_in_bytes();
        on_entry_change(
            &mut tx,
            &context,
            &EntryChange::Update {
                before: current,
                after: newer,
            },
        );
        assert_eq!(tx.estimated_size_in_bytes(), size);

        let finished = EntryStateRef {
            stage: Stage::Finished,
            status: Status::Succeeded,
            metadata: &metadata_a,
            stats: &newer_stats,
            ..current
        };
        tx.update_vqueue_entry_status(&context, current, finished);
        assert_eq!(
            read::<ServiceLoad>(&tx, &service_key)
                .iter()
                .collect::<Vec<_>>(),
            [(StageStatus::new(Stage::Finished, Status::Succeeded), 1)]
        );
        assert_eq!(
            read::<VirtualObjectLoad>(&tx, &object_key)
                .iter()
                .collect::<Vec<_>>(),
            [(Stage::Finished, 1)]
        );
        assert_eq!(
            read::<DeploymentLoad>(&tx, &deployment_a)
                .iter()
                .collect::<Vec<_>>(),
            [(Stage::Finished, 1)]
        );
        tx.delete_vqueue_entry_status(&context, finished);
        assert!(read::<ServiceLoad>(&tx, &service_key).is_empty());
        assert!(read::<VirtualObjectLoad>(&tx, &object_key).is_empty());
        assert!(read::<DeploymentLoad>(&tx, &deployment_a).is_empty());
        tx.commit().await.unwrap();
    }
}
