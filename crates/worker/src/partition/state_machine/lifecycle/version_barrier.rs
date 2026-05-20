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
use restate_types::partitions::features::PartitionFeatureChange;
use restate_wal_protocol::control::VersionBarrierCommand;

use crate::debug_if_leader;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};

pub struct OnVersionBarrierCommand {
    pub barrier: VersionBarrierCommand,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnVersionBarrierCommand
where
    S: WriteFsmTable,
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

        if !known_changes.is_empty() {
            let mut updated = ctx.enabled_features.clone();
            for change in &known_changes {
                change.apply_to(&mut updated);
            }

            // todo(tillrohrmann) implement logic to enable currently supported features (vqueues)

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
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use googletest::prelude::*;
    use restate_limiter::RuleBook;
    use restate_storage_api::fsm_table::ReadFsmTable;
    use restate_types::SemanticRestateVersion;
    use restate_types::identifiers::PartitionKey;
    use restate_types::logs::Keys;
    use restate_types::partitions::features::PartitionFeatureChange;
    use restate_types::sharding::KeyRange;
    use restate_wal_protocol::control::VersionBarrierCommand;
    use restate_wal_protocol::v2::{Command, commands};

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
