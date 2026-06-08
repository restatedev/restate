// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::fsm_table::WriteFsmTable;
use restate_storage_api::inbox_table::ReadInboxTable;
use restate_storage_api::invocation_status_table::ReadInvocationStatusTable;
use restate_types::partitions::features::PartitionFeatureChange;
use restate_types::sharding::KeyRange;
use restate_wal_protocol::control::VersionBarrierCommand;

use crate::debug_if_leader;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};

pub struct OnVersionBarrierCommand {
    pub barrier: VersionBarrierCommand,
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

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnVersionBarrierCommand
where
    S: WriteFsmTable + ReadInboxTable + ReadInvocationStatusTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        // Defense-in-depth: every feature change ID carried by the barrier must be known to this
        // binary. A correctly behaving proposer also sets `barrier.version` >=
        // max(change.min_required_version()), so the version check at the dispatch site is the
        // primary gate; this check only fires if a proposer sent feature changes without bumping
        // the version accordingly.
        let mut unknown_ids = Vec::new();
        let mut known_changes = Vec::with_capacity(self.barrier.feature_changes.len());
        for &id in &self.barrier.feature_changes {
            match PartitionFeatureChange::from_repr(id) {
                Some(change) => known_changes.push(change),
                None => unknown_ids.push(id),
            }
        }
        if !unknown_ids.is_empty() {
            return Err(Error::UnknownFeatureFlags {
                unknown_ids,
                required_min_version: self.barrier.version,
                barrier_reason: self.barrier.human_reason.unwrap_or_default(),
            });
        }

        // Determine which known changes actually flip a feature off->on. Only those
        // need to run the migration probe; re-applying an already-enabled barrier
        // stays cheap and idempotent.
        let mut updated = *ctx.enabled_features;
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
            if requires_migration_for(change, ctx.storage, ctx.partition_key_range).await? {
                needs_migration.push(change);
            }
        }
        if !needs_migration.is_empty() {
            return Err(Error::MigrationRequired {
                features: needs_migration,
            });
        }

        if matches!(
            self.barrier.version.cmp_precedence(ctx.min_restate_version),
            std::cmp::Ordering::Greater,
        ) {
            ctx.storage.put_min_restate_version(&self.barrier.version)?;
            *ctx.min_restate_version = self.barrier.version;
            debug_if_leader!(
                ctx.is_leader,
                "Update a new minimum restate-server version barrier to {}",
                ctx.min_restate_version
            );

            // todo: Migrate invocations from journal v1 to journal v2 once bumping the min Restate version to v1.6.0
            //  if it is not prohibitively expensive
        }

        if updated != *ctx.enabled_features {
            ctx.storage.put_state_machine_features(&updated)?;
            *ctx.enabled_features = updated;
            debug_if_leader!(
                ctx.is_leader,
                "Applied state-machine feature changes {:?}; new feature set: {:?}",
                known_changes,
                ctx.enabled_features
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use googletest::prelude::*;
    use restate_limiter::RuleBook;
    use restate_storage_api::Transaction;
    use restate_storage_api::fsm_table::ReadFsmTable;
    use restate_storage_api::inbox_table::{InboxEntry, WriteInboxTable};
    use restate_storage_api::invocation_status_table::{
        CompletedInvocation, InFlightInvocationMetadata, InvocationStatus,
        WriteInvocationStatusTable,
    };
    use restate_types::SemanticRestateVersion;
    use restate_types::identifiers::{InvocationId, PartitionKey, ServiceId};
    use restate_types::invocation::InvocationTarget;
    use restate_types::logs::Keys;
    use restate_types::partitions::features::{
        PartitionFeatureChange, PersistedStateMachineFeatures,
    };
    use restate_types::sharding::KeyRange;
    use restate_types::state_mut::ExternalStateMutation;
    use restate_wal_protocol::control::VersionBarrierCommand;
    use restate_wal_protocol::v2::{Command, commands};
    use std::collections::HashMap;

    use crate::partition::state_machine::StateMachine;
    use crate::partition::state_machine::tests::TestEnv;
    use crate::rule_book_cache::RuleBookCacheHandle;

    fn fresh_state_machine() -> StateMachine {
        StateMachine::new(
            0,    /* inbox_seq_number */
            0,    /* outbox_seq_number */
            None, /* outbox_head_seq_number */
            KeyRange::FULL,
            SemanticRestateVersion::unknown(),
            Default::default(), /* enabled_features */
            Default::default(), /* schema */
            std::sync::Arc::new(RuleBook::default()),
            RuleBookCacheHandle::detached(),
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

    #[restate_core::test]
    async fn stop_at_version_barrier() {
        let mut test_env = TestEnv::create_with_state_machine(fresh_state_machine()).await;

        // should fail
        let unrealistic_future_version = SemanticRestateVersion::parse("99.0.0").unwrap();
        assert_that!(
            unrealistic_future_version.cmp_precedence(SemanticRestateVersion::current()),
            eq(std::cmp::Ordering::Greater)
        );

        let result = test_env
            .apply_fallible(commands::VersionBarrierCommand::test_envelope(barrier(
                SemanticRestateVersion::parse("99.0.0").unwrap(),
                Vec::new(),
            )))
            .await;

        assert_that!(
            result,
            err(pat!(
                crate::partition::state_machine::Error::VersionBarrier {
                    required_min_version: eq(unrealistic_future_version),
                    barrier_reason: eq("testing"),
                }
            ))
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn update_at_version_barrier() {
        let mut test_env = TestEnv::create_with_state_machine(fresh_state_machine()).await;

        let result = test_env
            .apply_fallible(commands::VersionBarrierCommand::test_envelope(barrier(
                SemanticRestateVersion::current().clone(),
                Vec::new(),
            )))
            .await;

        assert_that!(result, ok(empty()));

        {
            let applied = test_env.storage().get_min_restate_version().await.unwrap();
            assert_that!(&applied, eq(SemanticRestateVersion::current()));
        }
        // re-apply the same version, no-op
        let result = test_env
            .apply_fallible(commands::VersionBarrierCommand::test_envelope(barrier(
                SemanticRestateVersion::current().clone(),
                Vec::new(),
            )))
            .await;

        assert_that!(result, ok(empty()));
        {
            let applied = test_env.storage().get_min_restate_version().await.unwrap();
            assert_that!(&applied, eq(SemanticRestateVersion::current()));
        }

        // apply an older version, success but without effect.
        let result = test_env
            .apply_fallible(commands::VersionBarrierCommand::test_envelope(barrier(
                SemanticRestateVersion::parse("0.1.0").unwrap(),
                Vec::new(),
            )))
            .await;

        assert_that!(result, ok(empty()));

        {
            let applied = test_env.storage().get_min_restate_version().await.unwrap();
            assert_that!(&applied, eq(SemanticRestateVersion::current()));
        }

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn apply_known_feature_change() {
        let mut test_env = TestEnv::create_with_state_machine(fresh_state_machine()).await;

        // PSF starts empty.
        {
            let psf = test_env
                .storage()
                .get_state_machine_features()
                .await
                .unwrap();
            assert_that!(psf.vqueues, eq(false));
        }

        // Enable vqueues.
        let result = test_env
            .apply_fallible(commands::VersionBarrierCommand::test_envelope(barrier(
                SemanticRestateVersion::current().clone(),
                vec![PartitionFeatureChange::EnableVqueues.id()],
            )))
            .await;
        assert_that!(result, ok(empty()));
        {
            let psf = test_env
                .storage()
                .get_state_machine_features()
                .await
                .unwrap();
            assert_that!(psf.vqueues, eq(true));
        }

        // Re-apply the same change: idempotent.
        let result = test_env
            .apply_fallible(commands::VersionBarrierCommand::test_envelope(barrier(
                SemanticRestateVersion::current().clone(),
                vec![PartitionFeatureChange::EnableVqueues.id()],
            )))
            .await;
        assert_that!(result, ok(empty()));
        {
            let psf = test_env
                .storage()
                .get_state_machine_features()
                .await
                .unwrap();
            assert_that!(psf.vqueues, eq(true));
        }

        test_env.shutdown().await;
    }

    async fn seed_inbox_state_mutation(test_env: &mut TestEnv) {
        let service_id = ServiceId::new(None, "MyService", "MyKey");
        let mut tx = test_env.storage().transaction();
        tx.put_inbox_entry(
            0,
            &InboxEntry::StateMutation(ExternalStateMutation {
                service_id,
                version: None,
                state: HashMap::default(),
            }),
        )
        .unwrap();
        tx.commit().await.unwrap();
    }

    async fn seed_invoked_status(test_env: &mut TestEnv) {
        let invocation_id = InvocationId::mock_random();
        let mut tx = test_env.storage().transaction();
        tx.put_invocation_status(
            &invocation_id,
            &InvocationStatus::Invoked(InFlightInvocationMetadata::mock()),
        )
        .unwrap();
        tx.commit().await.unwrap();
    }

    async fn seed_completed_status(test_env: &mut TestEnv) {
        let invocation_target = InvocationTarget::mock_virtual_object();
        let invocation_id = InvocationId::generate(&invocation_target, None);
        let mut tx = test_env.storage().transaction();
        tx.put_invocation_status(
            &invocation_id,
            &InvocationStatus::Completed(CompletedInvocation::mock_neo()),
        )
        .unwrap();
        tx.commit().await.unwrap();
    }

    #[restate_core::test]
    async fn migration_required_when_inbox_non_empty() {
        let mut test_env = TestEnv::create_with_state_machine(fresh_state_machine()).await;
        seed_inbox_state_mutation(&mut test_env).await;

        let result = test_env
            .apply_fallible(commands::VersionBarrierCommand::test_envelope(barrier(
                SemanticRestateVersion::current().clone(),
                vec![PartitionFeatureChange::EnableVqueues.id()],
            )))
            .await;

        assert_that!(
            result,
            err(pat!(
                crate::partition::state_machine::Error::MigrationRequired {
                    features: eq(vec![PartitionFeatureChange::EnableVqueues]),
                }
            ))
        );

        // The feature flag must remain off — the apply transaction rolled back.
        let psf = test_env
            .storage()
            .get_state_machine_features()
            .await
            .unwrap();
        assert_that!(psf.vqueues, eq(false));

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn migration_required_when_non_completed_invocation_present() {
        let mut test_env = TestEnv::create_with_state_machine(fresh_state_machine()).await;
        seed_invoked_status(&mut test_env).await;

        let result = test_env
            .apply_fallible(commands::VersionBarrierCommand::test_envelope(barrier(
                SemanticRestateVersion::current().clone(),
                vec![PartitionFeatureChange::EnableVqueues.id()],
            )))
            .await;

        assert_that!(
            result,
            err(pat!(
                crate::partition::state_machine::Error::MigrationRequired {
                    features: eq(vec![PartitionFeatureChange::EnableVqueues]),
                }
            ))
        );

        let psf = test_env
            .storage()
            .get_state_machine_features()
            .await
            .unwrap();
        assert_that!(psf.vqueues, eq(false));

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn only_completed_invocation_does_not_block_enable() {
        let mut test_env = TestEnv::create_with_state_machine(fresh_state_machine()).await;
        seed_completed_status(&mut test_env).await;

        let result = test_env
            .apply_fallible(commands::VersionBarrierCommand::test_envelope(barrier(
                SemanticRestateVersion::current().clone(),
                vec![PartitionFeatureChange::EnableVqueues.id()],
            )))
            .await;
        assert_that!(result, ok(empty()));

        let psf = test_env
            .storage()
            .get_state_machine_features()
            .await
            .unwrap();
        assert_that!(psf.vqueues, eq(true));

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn no_op_reapply_skips_probe() {
        // Start with vqueues already enabled in the state machine, then seed an
        // inbox entry that would normally trip the gate. The barrier re-apply must
        // succeed because the change does not flip a feature off->on.
        let state_machine = StateMachine::new(
            0,
            0,
            None,
            KeyRange::FULL,
            SemanticRestateVersion::unknown(),
            PersistedStateMachineFeatures {
                journal_v2: false,
                vqueues: true,
                unique_random_seeds: false,
            },
            Default::default(),
            std::sync::Arc::new(RuleBook::default()),
            RuleBookCacheHandle::detached(),
        );
        let mut test_env = TestEnv::create_with_state_machine(state_machine).await;
        seed_inbox_state_mutation(&mut test_env).await;

        let result = test_env
            .apply_fallible(commands::VersionBarrierCommand::test_envelope(barrier(
                SemanticRestateVersion::current().clone(),
                vec![PartitionFeatureChange::EnableVqueues.id()],
            )))
            .await;
        assert_that!(result, ok(empty()));

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn reject_unknown_feature_change_id() {
        let mut test_env = TestEnv::create_with_state_machine(fresh_state_machine()).await;

        let result = test_env
            .apply_fallible(commands::VersionBarrierCommand::test_envelope(barrier(
                SemanticRestateVersion::current().clone(),
                vec![PartitionFeatureChange::EnableVqueues.id(), 9999],
            )))
            .await;

        assert_that!(
            result,
            err(pat!(
                crate::partition::state_machine::Error::UnknownFeatureFlags {
                    unknown_ids: eq(vec![9999u16]),
                    barrier_reason: eq("testing"),
                }
            ))
        );

        // Nothing should have been persisted — PSF remains at default.
        let psf = test_env
            .storage()
            .get_state_machine_features()
            .await
            .unwrap();
        assert_that!(psf.vqueues, eq(false));

        test_env.shutdown().await;
    }
}
