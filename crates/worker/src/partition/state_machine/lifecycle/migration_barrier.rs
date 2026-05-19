// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::fsm_table::{LATEST_STORAGE_FORMAT, StorageFormatVersion, WriteFsmTable};
use restate_wal_protocol::control::MigrationBarrierCommand;

use crate::debug_if_leader;
use crate::partition::state_machine::actions::Action;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};

pub struct OnMigrationBarrierCommand {
    pub barrier: MigrationBarrierCommand,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnMigrationBarrierCommand
where
    S: WriteFsmTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let current = *ctx.storage_version;
        // Compare the wire `target_version` (raw u16) against bounds before converting
        // to the typed enum: an unknown future value must surface in the hard-fail error
        // verbatim, not collapse to `V1_5` via the lossy `From<u16>` fallback.
        let target_raw = self.barrier.target_version;
        let supported_raw = LATEST_STORAGE_FORMAT as u16;

        if target_raw <= current as u16 {
            return Ok(());
        }
        if target_raw > supported_raw {
            return Err(Error::MigrationBarrier {
                current,
                target: target_raw,
                hint: self.barrier.hint,
            });
        }
        let target = StorageFormatVersion::from(target_raw);
        let crossed_vqueues_threshold =
            current < StorageFormatVersion::Vqueues && target >= StorageFormatVersion::Vqueues;

        // todo(tillrohrmann) follow-up: run coordinated table migrations between
        //  `current` and `target` (inbox / invocation-status / state / promise /
        //  timer → vqueue layout).

        ctx.storage.put_storage_version(target)?;
        *ctx.storage_version = target;
        debug_if_leader!(
            ctx.is_leader,
            "Applied migration barrier: storage format {:?} -> {:?}",
            current,
            target
        );

        if crossed_vqueues_threshold {
            // Signal the leader to construct a fresh SchedulerService against the
            // now-migrated partition store. Followers receive the same action and
            // silently drop it via `LeadershipState::handle_actions`.
            ctx.action_collector
                .push(Action::InitializeVqueuesScheduler);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use googletest::prelude::*;
    use restate_limiter::RuleBook;
    use restate_storage_api::fsm_table::{
        LATEST_STORAGE_FORMAT, ReadFsmTable, StorageFormatVersion,
    };
    use restate_types::SemanticRestateVersion;
    use restate_types::sharding::KeyRange;
    use restate_wal_protocol::control::MigrationBarrierCommand;
    use restate_wal_protocol::v2::{Command, commands};

    use crate::partition::state_machine::StateMachine;
    use crate::partition::state_machine::actions::Action;
    use crate::partition::state_machine::tests::TestEnv;
    use crate::rule_book_cache::RuleBookCacheHandle;

    fn fresh_state_machine() -> StateMachine {
        // start at version `None` so tests exercise the bump path
        StateMachine::new(
            0,
            0,
            None,
            KeyRange::FULL,
            SemanticRestateVersion::unknown(),
            StorageFormatVersion::None,
            Default::default(),
            std::sync::Arc::new(RuleBook::default()),
            RuleBookCacheHandle::detached(),
        )
    }

    #[restate_core::test]
    async fn idempotent_when_target_at_or_below_current() {
        let mut test_env = TestEnv::create_with_state_machine(fresh_state_machine()).await;

        // Bump to LATEST_STORAGE_FORMAT first — crosses the Vqueues threshold, so we
        // expect exactly one InitializeVqueuesScheduler action.
        let result = test_env
            .apply_fallible(commands::MigrationBarrierCommand::test_envelope(
                MigrationBarrierCommand {
                    target_version: LATEST_STORAGE_FORMAT as u16,
                    hint: Some("initial".to_owned()),
                },
            ))
            .await;
        assert_that!(
            result,
            ok(elements_are![pat!(Action::InitializeVqueuesScheduler)])
        );
        assert_that!(
            test_env.storage().get_storage_version().await.unwrap(),
            eq(LATEST_STORAGE_FORMAT)
        );

        // Re-applying the same target is a no-op — no action emitted.
        let result = test_env
            .apply_fallible(commands::MigrationBarrierCommand::test_envelope(
                MigrationBarrierCommand {
                    target_version: LATEST_STORAGE_FORMAT as u16,
                    hint: None,
                },
            ))
            .await;
        assert_that!(result, ok(empty()));

        // A lower target is also a no-op — no action emitted.
        let lower = (LATEST_STORAGE_FORMAT as u16).saturating_sub(1);
        let result = test_env
            .apply_fallible(commands::MigrationBarrierCommand::test_envelope(
                MigrationBarrierCommand {
                    target_version: lower,
                    hint: None,
                },
            ))
            .await;
        assert_that!(result, ok(empty()));
        assert_that!(
            test_env.storage().get_storage_version().await.unwrap(),
            eq(LATEST_STORAGE_FORMAT)
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn hard_fails_when_target_exceeds_supported() {
        let mut test_env = TestEnv::create_with_state_machine(fresh_state_machine()).await;

        let future_target = (LATEST_STORAGE_FORMAT as u16) + 1;
        let result = test_env
            .apply_fallible(commands::MigrationBarrierCommand::test_envelope(
                MigrationBarrierCommand {
                    target_version: future_target,
                    hint: Some("upgrade required".to_owned()),
                },
            ))
            .await;

        assert_that!(
            result,
            err(pat!(
                crate::partition::state_machine::Error::MigrationBarrier {
                    target: eq(future_target),
                    hint: some(eq("upgrade required".to_owned())),
                }
            ))
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn advances_watermark_within_supported_range() {
        let mut test_env = TestEnv::create_with_state_machine(fresh_state_machine()).await;

        // Advance to Vqueues (the highest known version).
        let target = StorageFormatVersion::Vqueues;
        let result = test_env
            .apply_fallible(commands::MigrationBarrierCommand::test_envelope(
                MigrationBarrierCommand {
                    target_version: target as u16,
                    hint: None,
                },
            ))
            .await;

        assert_that!(
            result,
            ok(elements_are![pat!(Action::InitializeVqueuesScheduler)])
        );
        assert_that!(
            test_env.storage().get_storage_version().await.unwrap(),
            eq(target)
        );

        test_env.shutdown().await;
    }
}
