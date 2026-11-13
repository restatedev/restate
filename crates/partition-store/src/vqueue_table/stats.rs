// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The status gauges: current entry counts, maintained in the same transaction as
//! the source write.
//!
//! Each gauge is a registered statistic: every entry write applies its change to
//! each maintained index (see [`crate::registry`]), with the before-image of an
//! entry the index does not reflect yet treated as absent. Service and object
//! dimensions are fixed by `EntryContext`; deployment can change independently of
//! stage/status. Each projection removes the before-contribution and adds the
//! after-contribution, skipping writes when its membership is unchanged. Finished
//! entries remain counted until deletion. Timestamps and attempt counters do not
//! affect these gauges.

use restate_storage_api::vqueue_table::{EntryContext, EntryStateRef};
use restate_types::config::Configuration;
use restate_types::vqueues::EntryKind;

use crate::PartitionStoreTransaction;
use crate::registry::{Family, IndexDefinition, IndexKind};
use crate::stats::aggregated::{
    AggregatedStat, AggregatedStatsMut, DeploymentStats, DeploymentStatsKey, ServiceStats,
    ServiceStatsKey, StageStatus, StageStatusGauge, VirtualObjectStats, VirtualObjectStatsKey,
};
use crate::stats::{EncodeStatKey, Stat};

/// Counts by service, handler and entry kind.
pub(crate) static SERVICE_STATS: IndexDefinition = IndexDefinition {
    name: ServiceStats::DEFINITION,
    kind: IndexKind::Aggregate,
    apply: apply_service_stats,
};

/// Counts by pinned deployment and service.
pub(crate) static DEPLOYMENT_STATS: IndexDefinition = IndexDefinition {
    name: DeploymentStats::DEFINITION,
    kind: IndexKind::Aggregate,
    apply: apply_deployment_stats,
};

/// Counts by virtual object, handler and entry kind.
pub(crate) static VIRTUAL_OBJECT_STATS: IndexDefinition = IndexDefinition {
    name: VirtualObjectStats::DEFINITION,
    kind: IndexKind::Aggregate,
    apply: apply_virtual_object_stats,
};

/// The status gauges, released and controlled together.
pub(crate) const STATUS_GAUGES: Family = Family {
    definitions: &[&SERVICE_STATS, &DEPLOYMENT_STATS, &VIRTUAL_OBJECT_STATS],
    maintain: track_status_gauges,
    complete: complete_status_gauges,
    reset: status_gauges_reset,
};

fn track_status_gauges(config: &Configuration) -> bool {
    config.common.experimental.is_track_status_gauges_enabled()
}

fn complete_status_gauges(config: &Configuration) -> bool {
    config
        .common
        .experimental
        .is_complete_status_gauges_enabled()
}

fn status_gauges_reset(config: &Configuration) -> bool {
    config.common.experimental.is_status_gauges_reset_enabled()
}

fn kind(
    before: Option<&EntryStateRef<'_>>,
    after: Option<&EntryStateRef<'_>>,
) -> Option<EntryKind> {
    after.or(before).map(|entry| entry.entry_key.kind())
}

fn bucket(entry: &EntryStateRef<'_>) -> StageStatus {
    StageStatus::new(entry.stage, entry.status)
}

fn apply_service_stats(
    storage: &mut PartitionStoreTransaction<'_>,
    id: u32,
    context: &EntryContext<'_>,
    before: Option<&EntryStateRef<'_>>,
    after: Option<&EntryStateRef<'_>>,
) {
    let Some(kind) = kind(before, after) else {
        return;
    };
    let target = context.target;
    update_stage_status::<ServiceStats, _>(
        &mut storage.aggregated_stats(id),
        ServiceStatsKey::borrowed(target.service(), target.handler(), kind),
        before.map(bucket),
        after.map(bucket),
    );
}

fn apply_virtual_object_stats(
    storage: &mut PartitionStoreTransaction<'_>,
    id: u32,
    context: &EntryContext<'_>,
    before: Option<&EntryStateRef<'_>>,
    after: Option<&EntryStateRef<'_>>,
) {
    let target = context.target;
    let (Some(kind), Some(key)) = (kind(before, after), target.virtual_object_key()) else {
        return;
    };
    update_stage_status::<VirtualObjectStats, _>(
        &mut storage.aggregated_stats(id),
        VirtualObjectStatsKey::borrowed(
            target.service(),
            target.scope(),
            key,
            target.handler(),
            kind,
            context.qid.partition_key(),
        ),
        before.map(bucket),
        after.map(bucket),
    );
}

/// The deployment an entry is pinned to, with its bucket. Unpinned entries do
/// not contribute.
fn pinned<'a>(entry: &EntryStateRef<'a>) -> Option<(&'a str, StageStatus)> {
    let metadata: &'a _ = entry.metadata;
    metadata.deployment.as_deref().map(|id| (id, bucket(entry)))
}

fn apply_deployment_stats(
    storage: &mut PartitionStoreTransaction<'_>,
    id: u32,
    context: &EntryContext<'_>,
    before: Option<&EntryStateRef<'_>>,
    after: Option<&EntryStateRef<'_>>,
) {
    let service = context.target.service();
    let mut stats = storage.aggregated_stats(id);
    match (before.and_then(pinned), after.and_then(pinned)) {
        (Some((old, old_stage)), Some((new, new_stage))) if old == new => {
            update_stage_status::<DeploymentStats, _>(
                &mut stats,
                DeploymentStatsKey::borrowed(old, service),
                Some(old_stage),
                Some(new_stage),
            );
        }
        (old, new) => {
            if let Some((deployment, stage)) = old {
                stats.decrement_stage_status::<DeploymentStats, _>(
                    DeploymentStatsKey::borrowed(deployment, service),
                    stage,
                );
            }
            if let Some((deployment, stage)) = new {
                stats.increment_stage_status::<DeploymentStats, _>(
                    DeploymentStatsKey::borrowed(deployment, service),
                    stage,
                );
            }
        }
    }
}

/// A stable aggregate key needs at most one merge, including bucket transitions.
fn update_stage_status<S, K>(
    stats: &mut AggregatedStatsMut<'_, '_>,
    key: K,
    before: Option<StageStatus>,
    after: Option<StageStatus>,
) where
    S: AggregatedStat<Aggregation = StageStatusGauge>,
    K: EncodeStatKey<S>,
{
    if before == after {
        return;
    }
    match (before, after) {
        (None, Some(new)) => stats.increment_stage_status::<S, _>(key, new),
        (Some(old), None) => stats.decrement_stage_status::<S, _>(key, old),
        (Some(old), Some(new)) => stats.transition_stage_status::<S, _>(key, old, new),
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
        EntryKey, EntryMetadata, EntryStateRef, ReadVQueueTable, Stage, Status, StoredEntryRef,
        WriteVQueueTable,
    };
    use restate_types::config::Configuration;
    use restate_types::partition_table::Partition;
    use restate_types::sharding::{KeyRange, PartitionId};
    use restate_types::vqueues::{EntryId, EntryKind, EntryTargetRef, HandlerRef, VQueueId};

    use restate_storage_api::vqueue_table::EntryChange;

    use crate::keys::KeyKind;
    use crate::keys::header::IndexScope;
    use crate::stats::StatValueCodec;
    use crate::{PartitionStoreManager, StorageRead};

    use super::*;

    fn read<S: Stat>(tx: &PartitionStoreTransaction<'_>, key: impl EncodeStatKey<S>) -> S::Value
    where
        S::Value: Default,
    {
        let scope = IndexScope {
            partition: tx.partition_id(),
            index: tx
                .maintained_indexes()
                .ready_index(S::DEFINITION, IndexKind::Aggregate)
                .unwrap(),
        };
        let mut encoded = Vec::new();
        S::encode_key(scope, key, &mut encoded);
        tx.get(KeyKind::Stats, encoded)
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
        let metadata = EntryMetadata {
            deployment: Some("dp_initial".into()),
            ..EntryMetadata::default()
        };
        let initial = EntryStateRef {
            stage: Stage::Inbox,
            status: Status::New,
            entry_key: &key,
            metadata: &metadata,
            stats: &stats,
        };
        let service_key =
            ServiceStatsKey::borrowed("counter", Some("increment"), EntryKind::Invocation);
        let object_key = VirtualObjectStatsKey::borrowed(
            "counter",
            Some("tenant"),
            "a",
            Some("increment"),
            EntryKind::Invocation,
            qid.partition_key(),
        );
        let deployment_a = DeploymentStatsKey::borrowed("dp_a", "counter");
        let deployment_b = DeploymentStatsKey::borrowed("dp_b", "counter");

        let mut config = Configuration::default();
        config.common.experimental.set_complete_status_gauges(true);
        config.worker.storage.enable_eager_migrations_on_startup = true;
        store
            .verify_and_run_migrations(Default::default(), &config)
            .await
            .unwrap();

        // Aborting a source write also discards its aggregate contributions.
        {
            let mut tx = store.transaction();
            tx.create_vqueue_entry_status(&context, initial);
        }
        let mut tx = store.transaction();
        assert!(read::<ServiceStats>(&tx, &service_key).is_empty());
        assert!(read::<VirtualObjectStats>(&tx, &object_key).is_empty());
        assert!(
            read::<DeploymentStats>(&tx, DeploymentStatsKey::borrowed("dp_initial", "counter"))
                .is_empty()
        );
        tx.create_vqueue_entry_status(&context, initial);
        assert_eq!(
            read::<ServiceStats>(&tx, &service_key)
                .iter()
                .collect::<Vec<_>>(),
            [(StageStatus::new(Stage::Inbox, Status::New), 1)]
        );
        assert_eq!(
            read::<VirtualObjectStats>(&tx, &object_key)
                .iter()
                .collect::<Vec<_>>(),
            [(StageStatus::new(Stage::Inbox, Status::New), 1)]
        );

        // Subsequent transitions must also work against a committed baseline.
        tx.commit().await.unwrap();
        drop(tx);
        let mut tx = store.transaction();

        // A status-only update must not be gated by stage or transition timestamp.
        let started = EntryStateRef {
            status: Status::Started,
            ..initial
        };
        let id = initial.base_entry_id(&context);
        let header = tx.get_vqueue_entry_status(&id).await.unwrap().unwrap();
        tx.update_vqueue_entry_status(&context, StoredEntryRef::from_header(&header), started);
        assert_eq!(
            read::<DeploymentStats>(&tx, DeploymentStatsKey::borrowed("dp_initial", "counter"))
                .iter()
                .collect::<Vec<_>>(),
            [(StageStatus::new(Stage::Inbox, Status::Started), 1)]
        );
        assert_eq!(
            read::<ServiceStats>(&tx, &service_key)
                .iter()
                .collect::<Vec<_>>(),
            [(StageStatus::new(Stage::Inbox, Status::Started), 1)]
        );
        assert_eq!(
            read::<VirtualObjectStats>(&tx, &object_key)
                .iter()
                .collect::<Vec<_>>(),
            [(StageStatus::new(Stage::Inbox, Status::Started), 1)]
        );

        let mut current = started;
        // Unpin, assign, repin, and remove a deployment without changing stage/status.
        let unpinned = EntryMetadata::default();
        let metadata_a = EntryMetadata {
            deployment: Some("dp_a".into()),
            ..metadata.clone()
        };
        let metadata_b = EntryMetadata {
            deployment: Some("dp_b".into()),
            ..metadata.clone()
        };
        for (next_metadata, expected_a, expected_b) in [
            (&unpinned, 0, 0),
            (&metadata_a, 1, 0),
            (&metadata_b, 0, 1),
            (&unpinned, 0, 0),
        ] {
            let next = EntryStateRef {
                metadata: next_metadata,
                ..current
            };
            let header = tx.get_vqueue_entry_status(&id).await.unwrap().unwrap();
            tx.update_vqueue_entry_status(&context, StoredEntryRef::from_header(&header), next);
            assert_eq!(
                read::<DeploymentStats>(&tx, &deployment_a)
                    .iter()
                    .map(|(_, count)| count)
                    .sum::<u64>(),
                expected_a
            );
            assert_eq!(
                read::<DeploymentStats>(&tx, &deployment_b)
                    .iter()
                    .map(|(_, count)| count)
                    .sum::<u64>(),
                expected_b
            );
            assert!(
                read::<DeploymentStats>(&tx, DeploymentStatsKey::borrowed("dp_initial", "counter"))
                    .is_empty()
            );
            for counts in [
                read::<ServiceStats>(&tx, &service_key),
                read::<VirtualObjectStats>(&tx, &object_key),
            ] {
                assert_eq!(
                    counts.iter().collect::<Vec<_>>(),
                    [(StageStatus::new(Stage::Inbox, Status::Started), 1)]
                );
            }
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
        let header = tx.get_vqueue_entry_status(&id).await.unwrap().unwrap();
        tx.maintained_indexes().apply(
            &mut tx,
            &context,
            &EntryChange::Update {
                before: StoredEntryRef::from_header(&header),
                after: newer,
            },
            // The indexes are Ready, so no walk cursor is consulted.
            &[],
        );
        assert_eq!(tx.estimated_size_in_bytes(), size);

        let finished = EntryStateRef {
            stage: Stage::Finished,
            status: Status::Succeeded,
            metadata: &metadata_a,
            stats: &newer_stats,
            ..current
        };
        tx.update_vqueue_entry_status(&context, StoredEntryRef::from_header(&header), finished);
        assert_eq!(
            read::<ServiceStats>(&tx, &service_key)
                .iter()
                .collect::<Vec<_>>(),
            [(StageStatus::new(Stage::Finished, Status::Succeeded), 1)]
        );
        assert_eq!(
            read::<VirtualObjectStats>(&tx, &object_key)
                .iter()
                .collect::<Vec<_>>(),
            [(StageStatus::new(Stage::Finished, Status::Succeeded), 1)]
        );
        assert_eq!(
            read::<DeploymentStats>(&tx, &deployment_a)
                .iter()
                .collect::<Vec<_>>(),
            [(StageStatus::new(Stage::Finished, Status::Succeeded), 1)]
        );
        let header = tx.get_vqueue_entry_status(&id).await.unwrap().unwrap();
        tx.delete_vqueue_entry_status(&context, StoredEntryRef::from_header(&header));
        assert!(read::<ServiceStats>(&tx, &service_key).is_empty());
        assert!(read::<VirtualObjectStats>(&tx, &object_key).is_empty());
        assert!(read::<DeploymentStats>(&tx, &deployment_a).is_empty());
        tx.commit().await.unwrap();
    }
}
