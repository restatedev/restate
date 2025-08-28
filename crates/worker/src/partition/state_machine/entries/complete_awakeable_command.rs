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
use restate_storage_api::outbox_table::{OutboxMessage, OutboxTable};
use restate_storage_api::state_table::StateTable;
use restate_types::invocation::{NotifySignalRequest, ResponseResult};
use restate_types::journal_v2::{
    CompleteAwakeableCommand, CompleteAwakeableId, CompleteAwakeableResult, Signal, SignalResult,
};

use crate::partition::state_machine::entries::ApplyJournalCommandEffect;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};
use crate::partition::types::OutboxMessageExt;

pub(super) type ApplyCompleteAwakeableCommand<'e> =
    ApplyJournalCommandEffect<'e, CompleteAwakeableCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyCompleteAwakeableCommand<'e>
where
    S: StateTable + OutboxTable + FsmTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        ctx.handle_outgoing_message(match self.entry.id {
            CompleteAwakeableId::Old(old_awakeable_id) => {
                let (invocation_id, entry_index) = old_awakeable_id.into_inner();
                OutboxMessage::from_awakeable_completion(
                    invocation_id,
                    entry_index,
                    match self.entry.result {
                        CompleteAwakeableResult::Success(s) => ResponseResult::Success(s),
                        CompleteAwakeableResult::Failure(f) => ResponseResult::Failure(f.into()),
                    },
                )
            }
            CompleteAwakeableId::New(new_awakeable_id) => {
                let (invocation_id, signal_id) = new_awakeable_id.into_inner();
                OutboxMessage::NotifySignal(NotifySignalRequest {
                    invocation_id,
                    signal: Signal::new(
                        signal_id,
                        match self.entry.result {
                            CompleteAwakeableResult::Success(s) => SignalResult::Success(s),
                            CompleteAwakeableResult::Failure(f) => SignalResult::Failure(f),
                        },
                    ),
                })
            }
        })
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::partition::state_machine::tests::fixtures::invoker_entry_effect;
    use crate::partition::state_machine::tests::{TestEnv, fixtures, matchers};
    use bytes::Bytes;
    use googletest::prelude::{assert_that, contains, eq};
    use restate_types::identifiers::{AwakeableIdentifier, ExternalSignalIdentifier, InvocationId};
    use restate_types::invocation::ResponseResult;
    use restate_types::journal_v2::{
        CompleteAwakeableCommand, CompleteAwakeableId, CompleteAwakeableResult, Signal, SignalId,
        SignalResult,
    };

    #[restate_core::test]
    async fn complete_old_awakeable() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        let callee_invocation_id = InvocationId::mock_random();
        let callee_entry_index = 10;
        let awakeable_identifier =
            AwakeableIdentifier::new(callee_invocation_id, callee_entry_index);
        let result_value = Bytes::from_static(b"success");

        let actions = test_env
            .apply(invoker_entry_effect(
                invocation_id,
                CompleteAwakeableCommand {
                    id: CompleteAwakeableId::Old(awakeable_identifier),
                    result: CompleteAwakeableResult::Success(result_value.clone()),
                    name: Default::default(),
                },
            ))
            .await;
        assert_that!(
            actions,
            contains(
                matchers::actions::invocation_response_to_partition_processor(
                    callee_invocation_id,
                    callee_entry_index,
                    eq(ResponseResult::Success(result_value))
                )
            )
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn complete_new_awakeable() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        let callee_invocation_id = InvocationId::mock_random();
        let callee_signal_index = 10;
        let awakeable_identifier =
            ExternalSignalIdentifier::new(callee_invocation_id, callee_signal_index);
        let result_value = Bytes::from_static(b"success");

        let actions = test_env
            .apply(invoker_entry_effect(
                invocation_id,
                CompleteAwakeableCommand {
                    id: CompleteAwakeableId::New(awakeable_identifier),
                    result: CompleteAwakeableResult::Success(result_value.clone()),
                    name: Default::default(),
                },
            ))
            .await;
        assert_that!(
            actions,
            contains(matchers::actions::notify_signal(
                callee_invocation_id,
                Signal::new(
                    SignalId::for_index(callee_signal_index),
                    SignalResult::Success(result_value)
                )
            ))
        );

        test_env.shutdown().await;
    }
}
