// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use restate_clock::UniqueTimestamp;
use restate_vqueues::context::HasVQueuesMut;
use tracing::debug;

use restate_bifrost::DataRecord;
use restate_core::cancellation_token;
use restate_partition_store::migrations::MigrationContext;
use restate_partition_store::{PartitionDb, PartitionStoreTransaction};
use restate_storage_api::Transaction;
use restate_types::SemanticRestateVersion;
use restate_types::partitions::features::PartitionFeatureChange;
use restate_types::protobuf::cluster::DetailedRunMode;
use restate_wal_protocol::control::VersionBarrierCommand;
use restate_wal_protocol::v2::{CommandScope, Envelope};

use super::{ApplyPartitionCommand, NextStep};
use crate::partition::processor::leadership::LeaderPromotion;
use crate::partition::processor::{
    FsmAccess, FsmMut, HasFsm, HasFsmMut, Processor, ProcessorRawContext,
};
use crate::partition::{NodeContext, ProcessorError};

pub struct VersionBarrierContext<'a, 'b, L> {
    pub txn: &'a mut PartitionStoreTransaction<'b>,
    pub node_ctx: &'a mut NodeContext,
    pub partition_db: PartitionDb,
    pub processor: &'a mut ProcessorRawContext,
    pub leadership: &'a mut L,
}

impl<L: LeaderPromotion> ApplyPartitionCommand<VersionBarrierCommand>
    for VersionBarrierContext<'_, '_, L>
{
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
        let created_at = command.created_at();
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

        if self
            .processor
            .fsm_mut()
            .set_min_restate_version(self.txn, barrier.version)
        {
            debug!(
                "Update a new minimum restate-server version barrier to {}",
                self.processor.fsm().min_restate_version()
            );
            // todo: Migrate invocations from journal v1 to journal v2 once bumping the min Restate version to v1.6.0
            //  if it is not prohibitively expensive
        }

        // Per-feature migration gate. Atomicity: if any flip-on change requires
        // migration, the whole barrier fails and the transaction rolls back so no
        // partial state (incl. min_restate_version) is persisted.
        let mut updated = *self.processor.enabled_features();

        if !known_changes.is_empty() {
            // Commit all in-flight changes before running any migrations
            self.txn.commit().await?;
        }

        // let's enable feature by feature
        for change in known_changes.iter() {
            if change.apply_to(&mut updated) {
                // Turn on the feature.
                match change {
                    // Inbox entries (invocations and state mutations) and any non-Completed
                    // invocation status (which transitively covers held virtual-object locks
                    // and scheduled-invocation timers via the `InvocationStatus::Scheduled`
                    // source-of-truth) must be migrated to vqueue form before vqueues is
                    // enabled.
                    PartitionFeatureChange::EnableVqueues => {
                        // if we are in "Leader" mode, then something is wrong!
                        assert_matches!(
                            self.leadership.current_mode(),
                            DetailedRunMode::Follower
                                | DetailedRunMode::BecomingLeader
                                | DetailedRunMode::Candidate
                        );
                        let config = self.node_ctx.config.live_load();
                        let skip_completed = config
                            .common
                            .experimental
                            .is_vqueues_migration_skip_completed_enabled();
                        let partition_db = &self.partition_db;
                        let mut ctx = MigrationContext::new(
                            config,
                            partition_db,
                            self.processor.key_range(),
                            cancellation_token(),
                        );

                        restate_vqueues::migrations::migrate_to_vqueues(
                            &mut ctx,
                            self.processor.vqueues_mut(),
                            UniqueTimestamp::from_unix_millis_unchecked(created_at.into()),
                            skip_completed,
                        )
                        .await?;
                    }
                    PartitionFeatureChange::EnableJournalV2 => {}
                    // Flipping unique-random-seeds on only affects invocations created after the apply
                    // point. Pre-existing invocations without a stored random seed keep working via the
                    // `to_random_seed()` fallback in `invoker_storage_reader.rs`.
                    PartitionFeatureChange::EnableUniqueRandomSeeds => {}
                }
            }
        }

        if self
            .processor
            .fsm_mut()
            .set_enabled_features(self.txn, updated)
        {
            debug!(
                "Applied state-machine feature changes {:?}; new feature set: {:?}",
                known_changes,
                self.processor.enabled_features()
            );
        }

        // Make sure we commit all changes in case we are becoming a leader.
        self.txn.commit().await?;
        // if we are in (becoming leader). Time to switch into a full leader.
        if matches!(
            self.leadership.current_mode(),
            DetailedRunMode::BecomingLeader
        ) {
            self.leadership
                .on_barrier_applied(self.node_ctx, &mut self.processor)
                .await?;
            assert!(matches!(
                self.leadership.current_mode(),
                DetailedRunMode::Leader
            ));
        }

        Ok(NextStep::AdvanceLastAppliedLsn {
            lsn,
            dedup: header.into_dedup(),
            scope: CommandScope::PartitionScoped,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use googletest::prelude::*;

    use restate_bifrost::{Bifrost, DataRecord};
    use restate_core::{TaskCenter, TestCoreEnv};
    use restate_partition_store::{PartitionStore, PartitionStoreManager};
    use restate_rocksdb::RocksDbManager;
    use restate_storage_api::Transaction;
    use restate_storage_api::fsm_table::ReadFsmTable;
    use restate_types::config::Configuration;
    use restate_types::identifiers::{PartitionId, PartitionKey};
    use restate_types::logs::{Keys, Lsn, SequenceNumber};
    use restate_types::partitions::Partition;
    use restate_types::partitions::features::{PartitionFeatureChange, PersistedFeatures};
    use restate_types::partitions::state::PartitionReplicaSetStates;
    use restate_types::protobuf::cluster::DetailedRunMode;
    use restate_types::sharding::KeyRange;
    use restate_types::time::NanosSinceEpoch;
    use restate_types::{GenerationalNodeId, SemanticRestateVersion};
    use restate_vqueues::context::HasVQueuesMut;
    use restate_wal_protocol::control::VersionBarrierCommand;
    use restate_wal_protocol::v2::{self, Command, CommandScope};
    use restate_worker_api::invoker::capacity::InvokerCapacity;
    use restate_worker_api::processor::Processor;

    use super::{ApplyPartitionCommand, VersionBarrierContext};
    use crate::RuleBookCacheHandle;
    use crate::partition::leadership::trim_queue::HasTrimQueue;
    use crate::partition::processor::ProcessorRawContext;
    use crate::partition::processor::commands::NextStep;
    use crate::partition::processor::leadership::LeaderPromotion;
    use crate::partition::{NodeContext, ProcessorError};
    use crate::partition_processor_manager::PartitionLeaderHandlesRegistry;

    /// No-op [`LeaderPromotion`]: the version-barrier command logic under test never
    /// depends on a real leadership transition. Reporting `Follower` keeps `apply` from
    /// attempting the become-leader step, so `on_barrier_applied` is never reached.
    struct NoLeadershipPromotion;

    impl LeaderPromotion for NoLeadershipPromotion {
        async fn on_barrier_applied(
            &mut self,
            _node_ctx: &mut NodeContext,
            _processor: impl Processor + HasTrimQueue + HasVQueuesMut,
        ) -> std::result::Result<(), ProcessorError> {
            Ok(())
        }

        fn current_mode(&self) -> DetailedRunMode {
            DetailedRunMode::Follower
        }
    }

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
        node_ctx: &mut NodeContext,
        processor: &mut ProcessorRawContext,
        storage: &mut PartitionStore,
        command: VersionBarrierCommand,
    ) -> std::result::Result<(), ProcessorError> {
        let envelope = VersionBarrierCommand::test_envelope(command);
        let record = DataRecord::new(
            NanosSinceEpoch::RESTATE_EPOCH,
            Keys::None,
            Lsn::OLDEST,
            envelope,
        );

        let partition_db = storage.partition_db().clone();
        let mut txn = storage.transaction();
        let mut leadership = NoLeadershipPromotion;
        let next_step = VersionBarrierContext {
            txn: &mut txn,
            partition_db,
            node_ctx,
            processor,
            leadership: &mut leadership,
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

    #[restate_core::test]
    async fn stop_at_version_barrier() {
        let env = TestCoreEnv::create_with_single_node(1, 0).await;
        let bifrost = Bifrost::init_in_memory(env.metadata_writer).await;

        let mut storage = open_store().await;
        let mut processor = processor(PersistedFeatures::default());

        let unrealistic_future_version = SemanticRestateVersion::parse("99.0.0").unwrap();
        assert_that!(
            unrealistic_future_version.cmp_precedence(SemanticRestateVersion::current()),
            eq(std::cmp::Ordering::Greater)
        );

        let mut node_ctx = NodeContext::new(
            GenerationalNodeId::new(1, 0),
            Configuration::live(),
            PartitionReplicaSetStates::default(),
            RuleBookCacheHandle::detached(),
            bifrost,
            InvokerCapacity::new_unlimited(),
            PartitionLeaderHandlesRegistry::default(),
        );

        let result = apply_barrier(
            &mut node_ctx,
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
        let env = TestCoreEnv::create_with_single_node(1, 0).await;
        let bifrost = Bifrost::init_in_memory(env.metadata_writer).await;
        let mut storage = open_store().await;
        let mut processor = processor(PersistedFeatures::default());

        let mut node_ctx = NodeContext::new(
            GenerationalNodeId::new(1, 0),
            Configuration::live(),
            PartitionReplicaSetStates::default(),
            RuleBookCacheHandle::detached(),
            bifrost,
            InvokerCapacity::new_unlimited(),
            PartitionLeaderHandlesRegistry::default(),
        );

        apply_barrier(
            &mut node_ctx,
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
            &mut node_ctx,
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
            &mut node_ctx,
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
        let env = TestCoreEnv::create_with_single_node(1, 0).await;
        let bifrost = Bifrost::init_in_memory(env.metadata_writer).await;
        let mut storage = open_store().await;
        let mut processor = processor(PersistedFeatures::default());

        let mut node_ctx = NodeContext::new(
            GenerationalNodeId::new(1, 0),
            Configuration::live(),
            PartitionReplicaSetStates::default(),
            RuleBookCacheHandle::detached(),
            bifrost,
            InvokerCapacity::new_unlimited(),
            PartitionLeaderHandlesRegistry::default(),
        );

        // PSF starts empty.
        assert_that!(
            storage.get_state_machine_features().await.unwrap().vqueues,
            eq(false)
        );

        // Enable vqueues, then re-apply: idempotent.
        for _ in 0..2 {
            apply_barrier(
                &mut node_ctx,
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
    async fn reject_unknown_feature_change_id() {
        let env = TestCoreEnv::create_with_single_node(1, 0).await;
        let bifrost = Bifrost::init_in_memory(env.metadata_writer).await;

        let mut storage = open_store().await;
        let mut processor = processor(PersistedFeatures::default());

        let mut node_ctx = NodeContext::new(
            GenerationalNodeId::new(1, 0),
            Configuration::live(),
            PartitionReplicaSetStates::default(),
            RuleBookCacheHandle::detached(),
            bifrost,
            InvokerCapacity::new_unlimited(),
            PartitionLeaderHandlesRegistry::default(),
        );

        let result = apply_barrier(
            &mut node_ctx,
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
