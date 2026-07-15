// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_bifrost::DataRecord;
use restate_partition_store::PartitionStoreTransaction;
use restate_storage_api::inbox_table::ReadInboxTable;
use restate_storage_api::invocation_status_table::ReadInvocationStatusTable;
use restate_types::SemanticRestateVersion;
use restate_types::partitions::features::PartitionFeatureChange;
use restate_types::sharding::KeyRange;
use restate_wal_protocol::control::VersionBarrierCommand;
use restate_wal_protocol::v2::{CommandScope, Envelope};

use super::{ApplyPartitionCommand, NextStep};
use crate::debug_if_leader;
use crate::partition::ProcessorError;
use crate::partition::processor::{
    FsmAccess, FsmMut, HasFsm, HasFsmMut, Processor, ProcessorRawContext,
};

pub struct VersionBarrierContext<'a, 'b> {
    pub txn: &'a mut PartitionStoreTransaction<'b>,
    pub processor: &'a mut ProcessorRawContext,
    pub is_leader: bool,
}

impl ApplyPartitionCommand<VersionBarrierCommand> for VersionBarrierContext<'_, '_> {
    async fn apply(
        &mut self,
        command: DataRecord<Envelope<VersionBarrierCommand>>,
    ) -> Result<NextStep, ProcessorError> {
        // We have versions in play:
        // - Our binary's version (this process)
        // - `min_restate_version` coming from the FSM
        // - `barrier.version` from bifrost.
        //
        // If we can process this command, we update the FSM.
        //
        // We can process this command if our own version is at or higher than the barrier
        // version as indicated by the message. We'll apply the change to the FSM only
        // if the new barrier version is higher than what the FSM already has.
        //
        // If we can't, then what?
        //
        // In v1.4 we crash the PP but tell a good message. This is not the best solution
        // but it'll make clear what's going on. The issue with this approach is that we
        // will probably continue restarting PP on the same node leading to unavailability.
        //
        // [todo] What's the ideal scenario?
        // - Ideal scenario is that we inform the operator (flare).
        // - We mark this node *generational* as a bad candidate (not to take leadership
        //   or run follower again).
        // - Through gossip, this node broadcasts its partition block-list so it won't be
        //   considered for leadership until a new generation pops up.
        //   Noting that the blocklist for a generational node can only increase/grow until
        //   the daemon is restarted (higher generation).
        // - Controller attempts to reconfigure or selects a different leader
        //   that's not blocking this partition if such replacement exists.
        // - Peers will not pick this node as leader candidate when performing
        //   adhoc failovers.
        let lsn = command.seq();
        let (header, barrier) = command.into_inner().split()?;

        if !SemanticRestateVersion::current().is_equal_or_newer_than(&barrier.version) {
            return Err(ProcessorError::VersionBarrier {
                required_min_version: barrier.version,
                barrier_reason: barrier.human_reason.unwrap_or_default(),
                feature_changes: barrier.feature_changes,
            });
        }

        // Defense-in-depth: every feature change ID carried by the barrier must be known to this
        // binary. A correctly behaving proposer also sets `barrier.version` >=
        // max(change.min_required_version()), so the version check at the dispatch site is the
        // primary gate; this check only fires if a proposer sent feature changes without bumping
        // the version accordingly.
        let mut unknown_ids = Vec::new();
        let mut known_changes = Vec::with_capacity(barrier.feature_changes.len());
        for id in barrier.feature_changes.iter() {
            match PartitionFeatureChange::from_repr(*id) {
                Some(change) => known_changes.push(change),
                None => unknown_ids.push(*id),
            }
        }
        if !unknown_ids.is_empty() {
            return Err(ProcessorError::UnknownFeatureFlags {
                unknown_ids,
                required_min_version: barrier.version,
                barrier_reason: barrier.human_reason.unwrap_or_default(),
            });
        }

        // Determine which known changes actually flip a feature off->on. Only those
        // need to run the migration probe; re-applying an already-enabled barrier
        // stays cheap and idempotent.
        let mut updated = *self.processor.enabled_features();
        let flip_on_changes: Vec<PartitionFeatureChange> = known_changes
            .iter()
            .copied()
            .filter(|change| change.apply_to(&mut updated))
            .collect();

        // Per-feature migration gate. Atomicity: if any flip-on change requires
        // migration, the whole barrier fails and the transaction rolls back so no
        // partial state (incl. min_restate_version) is persisted.
        let mut needs_migration = Vec::new();
        for &change in &flip_on_changes {
            if requires_migration_for(change, self.txn, self.processor.key_range()).await? {
                needs_migration.push(change);
            }
        }
        if !needs_migration.is_empty() {
            return Err(ProcessorError::MigrationRequired {
                features: needs_migration,
            });
        }

        if self
            .processor
            .fsm_mut()
            .set_min_restate_version(self.txn, barrier.version)
        {
            debug_if_leader!(
                self.is_leader,
                "Update a new minimum restate-server version barrier to {}",
                self.processor.fsm().min_restate_version()
            );
            // todo: Migrate invocations from journal v1 to journal v2 once bumping the min Restate version to v1.6.0
            //  if it is not prohibitively expensive
        }

        if self
            .processor
            .fsm_mut()
            .set_enabled_features(self.txn, updated)
        {
            debug_if_leader!(
                self.is_leader,
                "Applied state-machine feature changes {:?}; new feature set: {:?}",
                known_changes,
                self.processor.enabled_features()
            );
        }

        Ok(NextStep::AdvanceLastAppliedLsn {
            lsn,
            dedup: header.into_dedup(),
            scope: CommandScope::PartitionScoped,
        })
    }
}

/// Returns `true` if applying `change` to a partition that holds pre-existing
/// in-flight data would leave that data inconsistent and therefore requires a
/// migration step which is not provided by this binary.
async fn requires_migration_for<S>(
    change: PartitionFeatureChange,
    storage: &mut S,
    partition_key_range: KeyRange,
) -> Result<bool, restate_storage_api::StorageError>
where
    S: ReadInboxTable + ReadInvocationStatusTable,
{
    match change {
        PartitionFeatureChange::EnableVqueues => {
            // Inbox entries (invocations and state mutations) and any non-Completed
            // invocation status (which transitively covers held virtual-object locks
            // and scheduled-invocation timers via the `InvocationStatus::Scheduled`
            // source-of-truth) must be migrated to vqueue form before vqueues is
            // enabled. The 1.7.0 binary lacks the migration code; a later server
            // version provides it.
            if storage
                .any_inbox_entry_in_range(partition_key_range)
                .await?
            {
                return Ok(true);
            }
            if storage
                .any_non_completed_invocation_in_range(partition_key_range)
                .await?
            {
                return Ok(true);
            }
            Ok(false)
        }
        PartitionFeatureChange::EnableJournalV2 => Ok(false),
        // Flipping unique-random-seeds on only affects invocations created after the apply
        // point. Pre-existing invocations without a stored random seed keep working via the
        // `to_random_seed()` fallback in `invoker_storage_reader.rs`.
        PartitionFeatureChange::EnableUniqueRandomSeeds => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use googletest::prelude::*;

    use restate_bifrost::DataRecord;
    use restate_core::TaskCenter;
    use restate_partition_store::{PartitionStore, PartitionStoreManager};
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::Transaction;
    use restate_storage_api::fsm_table::ReadFsmTable;
    use restate_storage_api::inbox_table::{InboxEntry, WriteInboxTable};
    use restate_storage_api::invocation_status_table::{
        CompletedInvocation, InFlightInvocationMetadata, InvocationStatus,
        WriteInvocationStatusTable,
    };
    use restate_types::SemanticRestateVersion;
    use restate_types::identifiers::{InvocationId, PartitionId, PartitionKey, ServiceId};
    use restate_types::invocation::InvocationTarget;
    use restate_types::logs::{Keys, Lsn, SequenceNumber};
    use restate_types::partitions::Partition;
    use restate_types::partitions::features::{PartitionFeatureChange, PersistedFeatures};
    use restate_types::sharding::KeyRange;
    use restate_types::state_mut::ExternalStateMutation;
    use restate_types::time::NanosSinceEpoch;
    use restate_wal_protocol::control::VersionBarrierCommand;
    use restate_wal_protocol::v2::{self, Command, CommandScope};

    use super::{ApplyPartitionCommand, VersionBarrierContext};
    use crate::partition::ProcessorError;
    use crate::partition::processor::ProcessorRawContext;
    use crate::partition::processor::commands::NextStep;

    async fn open_store() -> PartitionStore {
        RocksDbManager::init();
        // The test harness shuts the node down after the body runs; hook RocksDB
        // teardown onto that instead of shutting the manager down in every test.
        TaskCenter::set_on_shutdown(Box::pin(async {
            RocksDbManager::get().shutdown().await;
        }));
        PartitionStoreManager::create(true)
            .await
            .unwrap()
            .open(&Partition::new(PartitionId::MIN, KeyRange::FULL), None)
            .await
            .unwrap()
    }

    fn processor(features: PersistedFeatures) -> ProcessorRawContext {
        ProcessorRawContext::new(
            Arc::new(Partition::new(PartitionId::MIN, KeyRange::FULL)),
            features,
        )
    }

    fn barrier(
        version: SemanticRestateVersion,
        feature_changes: Vec<u16>,
    ) -> VersionBarrierCommand {
        VersionBarrierCommand {
            version,
            human_reason: Some("testing".to_string()),
            partition_key_range: Keys::RangeInclusive(PartitionKey::MIN..=PartitionKey::MAX),
            feature_changes,
        }
    }

    /// Drives a `VersionBarrier` record through the partition-command handler.
    /// Mirrors the dispatcher: the transaction is committed on success and rolled
    /// back (dropped) on error.
    async fn apply_barrier(
        processor: &mut ProcessorRawContext,
        storage: &mut PartitionStore,
        command: VersionBarrierCommand,
    ) -> std::result::Result<(), ProcessorError> {
        let envelope = VersionBarrierCommand::test_envelope(command);
        let record = DataRecord::new(
            NanosSinceEpoch::UNIX_EPOCH,
            Keys::None,
            Lsn::OLDEST,
            envelope,
        );

        let mut txn = storage.transaction();
        let next_step = VersionBarrierContext {
            txn: &mut txn,
            processor,
            is_leader: false,
        }
        .apply(record.map(v2::Envelope::into_typed))
        .await?;

        assert_that!(
            next_step,
            pat!(NextStep::AdvanceLastAppliedLsn {
                lsn: eq(Lsn::OLDEST),
                dedup: eq(v2::Dedup::None),
                scope: pat!(CommandScope::PartitionScoped),
            })
        );

        txn.commit().await.unwrap();
        Ok(())
    }

    async fn seed_inbox_state_mutation(storage: &mut PartitionStore) {
        let mut tx = storage.transaction();
        tx.put_inbox_entry(
            0,
            &InboxEntry::StateMutation(ExternalStateMutation {
                service_id: ServiceId::new(None, "MyService", "MyKey"),
                version: None,
                state: HashMap::default(),
            }),
        )
        .unwrap();
        tx.commit().await.unwrap();
    }

    async fn seed_invoked_status(storage: &mut PartitionStore) {
        let mut tx = storage.transaction();
        tx.put_invocation_status(
            &InvocationId::mock_random(),
            &InvocationStatus::Invoked(InFlightInvocationMetadata::mock()),
        )
        .unwrap();
        tx.commit().await.unwrap();
    }

    async fn seed_completed_status(storage: &mut PartitionStore) {
        let invocation_id = InvocationId::generate(&InvocationTarget::mock_virtual_object(), None);
        let mut tx = storage.transaction();
        tx.put_invocation_status(
            &invocation_id,
            &InvocationStatus::Completed(CompletedInvocation::mock_neo()),
        )
        .unwrap();
        tx.commit().await.unwrap();
    }

    #[restate_core::test]
    async fn stop_at_version_barrier() {
        let mut storage = open_store().await;
        let mut processor = processor(PersistedFeatures::default());

        let unrealistic_future_version = SemanticRestateVersion::parse("99.0.0").unwrap();
        assert_that!(
            unrealistic_future_version.cmp_precedence(SemanticRestateVersion::current()),
            eq(std::cmp::Ordering::Greater)
        );

        let result = apply_barrier(
            &mut processor,
            &mut storage,
            barrier(unrealistic_future_version.clone(), Vec::new()),
        )
        .await;

        assert_that!(
            result,
            err(pat!(ProcessorError::VersionBarrier {
                required_min_version: eq(unrealistic_future_version),
                barrier_reason: eq("testing"),
            }))
        );
    }

    #[restate_core::test]
    async fn update_at_version_barrier() {
        let mut storage = open_store().await;
        let mut processor = processor(PersistedFeatures::default());

        apply_barrier(
            &mut processor,
            &mut storage,
            barrier(SemanticRestateVersion::current().clone(), Vec::new()),
        )
        .await
        .expect("current version applies");
        assert_that!(
            &storage.get_min_restate_version().await.unwrap(),
            eq(SemanticRestateVersion::current())
        );

        // Re-apply the same version: no-op.
        apply_barrier(
            &mut processor,
            &mut storage,
            barrier(SemanticRestateVersion::current().clone(), Vec::new()),
        )
        .await
        .expect("re-apply is a no-op");
        assert_that!(
            &storage.get_min_restate_version().await.unwrap(),
            eq(SemanticRestateVersion::current())
        );

        // Apply an older version: succeeds but the min version doesn't regress.
        apply_barrier(
            &mut processor,
            &mut storage,
            barrier(SemanticRestateVersion::parse("0.1.0").unwrap(), Vec::new()),
        )
        .await
        .expect("older version applies without effect");
        assert_that!(
            &storage.get_min_restate_version().await.unwrap(),
            eq(SemanticRestateVersion::current())
        );
    }

    #[restate_core::test]
    async fn apply_known_feature_change() {
        let mut storage = open_store().await;
        let mut processor = processor(PersistedFeatures::default());

        // PSF starts empty.
        assert_that!(
            storage.get_state_machine_features().await.unwrap().vqueues,
            eq(false)
        );

        // Enable vqueues, then re-apply: idempotent.
        for _ in 0..2 {
            apply_barrier(
                &mut processor,
                &mut storage,
                barrier(
                    SemanticRestateVersion::current().clone(),
                    vec![PartitionFeatureChange::EnableVqueues.id()],
                ),
            )
            .await
            .expect("enable vqueues");
            assert_that!(
                storage.get_state_machine_features().await.unwrap().vqueues,
                eq(true)
            );
        }
    }

    #[restate_core::test]
    async fn migration_required_when_inbox_non_empty() {
        let mut storage = open_store().await;
        let mut processor = processor(PersistedFeatures::default());
        seed_inbox_state_mutation(&mut storage).await;

        let result = apply_barrier(
            &mut processor,
            &mut storage,
            barrier(
                SemanticRestateVersion::current().clone(),
                vec![PartitionFeatureChange::EnableVqueues.id()],
            ),
        )
        .await;

        assert_that!(
            result,
            err(pat!(ProcessorError::MigrationRequired {
                features: eq(vec![PartitionFeatureChange::EnableVqueues]),
            }))
        );
        // The feature flag must remain off — the apply transaction rolled back.
        assert_that!(
            storage.get_state_machine_features().await.unwrap().vqueues,
            eq(false)
        );
    }

    #[restate_core::test]
    async fn migration_required_when_non_completed_invocation_present() {
        let mut storage = open_store().await;
        let mut processor = processor(PersistedFeatures::default());
        seed_invoked_status(&mut storage).await;

        let result = apply_barrier(
            &mut processor,
            &mut storage,
            barrier(
                SemanticRestateVersion::current().clone(),
                vec![PartitionFeatureChange::EnableVqueues.id()],
            ),
        )
        .await;

        assert_that!(
            result,
            err(pat!(ProcessorError::MigrationRequired {
                features: eq(vec![PartitionFeatureChange::EnableVqueues]),
            }))
        );
        assert_that!(
            storage.get_state_machine_features().await.unwrap().vqueues,
            eq(false)
        );
    }

    #[restate_core::test]
    async fn only_completed_invocation_does_not_block_enable() {
        let mut storage = open_store().await;
        let mut processor = processor(PersistedFeatures::default());
        seed_completed_status(&mut storage).await;

        apply_barrier(
            &mut processor,
            &mut storage,
            barrier(
                SemanticRestateVersion::current().clone(),
                vec![PartitionFeatureChange::EnableVqueues.id()],
            ),
        )
        .await
        .expect("completed invocations do not block enabling vqueues");
        assert_that!(
            storage.get_state_machine_features().await.unwrap().vqueues,
            eq(true)
        );
    }

    #[restate_core::test]
    async fn no_op_reapply_skips_probe() {
        // Start with vqueues already enabled, then seed an inbox entry that would
        // normally trip the gate. The barrier re-apply must succeed because the
        // change does not flip a feature off->on.
        let mut storage = open_store().await;
        let mut processor = processor(PersistedFeatures {
            journal_v2: false,
            vqueues: true,
            unique_random_seeds: false,
        });
        seed_inbox_state_mutation(&mut storage).await;

        apply_barrier(
            &mut processor,
            &mut storage,
            barrier(
                SemanticRestateVersion::current().clone(),
                vec![PartitionFeatureChange::EnableVqueues.id()],
            ),
        )
        .await
        .expect("already-enabled feature skips the migration probe");
    }

    #[restate_core::test]
    async fn reject_unknown_feature_change_id() {
        let mut storage = open_store().await;
        let mut processor = processor(PersistedFeatures::default());

        let result = apply_barrier(
            &mut processor,
            &mut storage,
            barrier(
                SemanticRestateVersion::current().clone(),
                vec![PartitionFeatureChange::EnableVqueues.id(), 9999],
            ),
        )
        .await;

        assert_that!(
            result,
            err(pat!(ProcessorError::UnknownFeatureFlags {
                unknown_ids: eq(vec![9999u16]),
                barrier_reason: eq("testing"),
            }))
        );
        // Nothing should have been persisted — PSF remains at default.
        assert_that!(
            storage.get_state_machine_features().await.unwrap().vqueues,
            eq(false)
        );
    }
}
