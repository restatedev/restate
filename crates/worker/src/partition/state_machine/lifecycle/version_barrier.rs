// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::fsm_table::FsmTable;
use restate_wal_protocol::control::VersionBarrier;

use crate::debug_if_leader;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};

pub struct OnVersionBarrierCommand {
    pub barrier: VersionBarrier,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnVersionBarrierCommand
where
    S: FsmTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        if matches!(
            self.barrier.version.cmp_precedence(ctx.min_restate_version),
            std::cmp::Ordering::Greater,
        ) {
            ctx.storage
                .put_min_restate_version(&self.barrier.version)
                .await?;
            *ctx.min_restate_version = self.barrier.version;
            debug_if_leader!(
                ctx.is_leader,
                "Update a new minimum restate-server version barrier to {}",
                ctx.min_restate_version
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use googletest::prelude::*;

    use restate_storage_api::fsm_table::ReadOnlyFsmTable;
    use restate_types::SemanticRestateVersion;
    use restate_types::identifiers::PartitionKey;
    use restate_types::logs::Keys;
    use restate_wal_protocol::Command;
    use restate_wal_protocol::control::VersionBarrier;

    use crate::partition::state_machine::tests::TestEnv;
    use crate::partition::state_machine::{Action, StateMachine};

    #[restate_core::test]
    async fn stop_at_version_barrier() {
        // starting at unknown version
        let state_machine = StateMachine::new(
            0,    /* inbox_seq_number */
            0,    /* outbox_seq_number */
            None, /* outbox_head_seq_number */
            PartitionKey::MIN..=PartitionKey::MAX,
            SemanticRestateVersion::unknown().clone(),
            Default::default(),
        );
        // this is fine as we are always above the unknown version (current > 0.0.0)
        let mut test_env = TestEnv::create_with_state_machine(state_machine).await;

        // should fail
        let unrealistic_future_version = SemanticRestateVersion::parse("99.0.0").unwrap();
        assert_that!(
            unrealistic_future_version.cmp_precedence(SemanticRestateVersion::current()),
            eq(std::cmp::Ordering::Greater)
        );

        let result = test_env
            .apply_fallible(Command::VersionBarrier(VersionBarrier {
                version: SemanticRestateVersion::parse("99.0.0").unwrap(),
                human_reason: Some("testing".to_string()),
                partition_key_range: Keys::RangeInclusive(PartitionKey::MIN..=PartitionKey::MAX),
            }))
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
        // starting at unknown version
        let state_machine = StateMachine::new(
            0,    /* inbox_seq_number */
            0,    /* outbox_seq_number */
            None, /* outbox_head_seq_number */
            PartitionKey::MIN..=PartitionKey::MAX,
            SemanticRestateVersion::unknown().clone(),
            Default::default(),
        );
        // this is fine as we are always above the unknown version (current > 0.0.0)
        let mut test_env = TestEnv::create_with_state_machine(state_machine).await;

        let result = test_env
            .apply_fallible(Command::VersionBarrier(VersionBarrier {
                version: SemanticRestateVersion::current().clone(),
                human_reason: Some("testing".to_string()),
                partition_key_range: Keys::RangeInclusive(PartitionKey::MIN..=PartitionKey::MAX),
            }))
            .await;

        assert_that!(result, ok(eq(Vec::<Action>::new())));

        {
            let applied = test_env.storage().get_min_restate_version().await.unwrap();
            assert_that!(&applied, eq(SemanticRestateVersion::current()));
        }
        // re-apply the same version, no-op
        let result = test_env
            .apply_fallible(Command::VersionBarrier(VersionBarrier {
                version: SemanticRestateVersion::current().clone(),
                human_reason: Some("testing".to_string()),
                partition_key_range: Keys::RangeInclusive(PartitionKey::MIN..=PartitionKey::MAX),
            }))
            .await;

        assert_that!(result, ok(eq(Vec::<Action>::new())));
        {
            let applied = test_env.storage().get_min_restate_version().await.unwrap();
            assert_that!(&applied, eq(SemanticRestateVersion::current()));
        }

        // apply an older version, success but without effect.
        let result = test_env
            .apply_fallible(Command::VersionBarrier(VersionBarrier {
                version: SemanticRestateVersion::parse("0.1.0").unwrap(),
                human_reason: Some("testing".to_string()),
                partition_key_range: Keys::RangeInclusive(PartitionKey::MIN..=PartitionKey::MAX),
            }))
            .await;

        assert_that!(result, ok(eq(Vec::<Action>::new())));

        {
            let applied = test_env.storage().get_min_restate_version().await.unwrap();
            assert_that!(&applied, eq(SemanticRestateVersion::current()));
        }

        test_env.shutdown().await;
    }
}
