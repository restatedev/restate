// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::state_machine::entries::ApplyJournalCommandEffect;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};
use restate_storage_api::fsm_table::WriteFsmTable;
use restate_storage_api::outbox_table::{OutboxMessage, WriteOutboxTable};
use restate_storage_api::timer_table::WriteTimerTable;
use restate_types::invocation::{AttachInvocationRequest, ServiceInvocationResponseSink};
use restate_types::journal_v2::AttachInvocationCommand;

pub(super) type ApplyAttachInvocationCommand<'e> =
    ApplyJournalCommandEffect<'e, AttachInvocationCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyAttachInvocationCommand<'e>
where
    S: WriteTimerTable + WriteOutboxTable + WriteFsmTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        ctx.handle_outgoing_message(OutboxMessage::AttachInvocation(AttachInvocationRequest {
            invocation_query: self.entry.target.into(),
            block_on_inflight: true,
            response_sink: ServiceInvocationResponseSink::partition_processor(
                self.invocation_id,
                self.entry.completion_id,
            ),
        }))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::partition::state_machine::Action;
    use crate::partition::state_machine::tests::fixtures::invoker_entry_effect;
    use crate::partition::state_machine::tests::{TestEnv, fixtures, matchers};
    use bytes::Bytes;
    use googletest::prelude::{all, assert_that, contains, eq, pat};
    use googletest::{elements_are, property};
    use restate_types::identifiers::{IdempotencyId, InvocationId, ServiceId};
    use restate_types::invocation::{
        InvocationQuery, InvocationResponse, JournalCompletionTarget, ResponseResult,
        ServiceInvocationResponseSink,
    };
    use restate_types::journal_v2::{
        AttachInvocationCommand, AttachInvocationCompletion, AttachInvocationResult,
        AttachInvocationTarget, CommandType, Entry, EntryMetadata, EntryType,
    };
    use restate_wal_protocol::Command;
    use rstest::rstest;

    #[rstest]
    #[restate_core::test]
    async fn attach_invocation(
        #[values(
            AttachInvocationTarget::InvocationId(InvocationId::mock_random()),
            AttachInvocationTarget::IdempotentRequest(IdempotencyId::mock_random()),
            AttachInvocationTarget::Workflow(ServiceId::mock_random())
        )]
        target: AttachInvocationTarget,
    ) {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        let completion_id = 1;
        let success_result = Bytes::from_static(b"success");

        let attach_invocation_command = AttachInvocationCommand {
            target: target.clone(),
            name: Default::default(),
            completion_id,
        };
        let actions = test_env
            .apply_multiple([
                invoker_entry_effect(invocation_id, attach_invocation_command.clone()),
                Command::InvocationResponse(InvocationResponse {
                    target: JournalCompletionTarget::from_parts(invocation_id, completion_id),
                    result: ResponseResult::Success(success_result.clone()),
                }),
            ])
            .await;

        let attach_invocation_completion = AttachInvocationCompletion {
            completion_id,
            result: AttachInvocationResult::Success(success_result),
        };
        assert_that!(
            actions,
            all![
                contains(pat!(Action::NewOutboxMessage {
                    message: pat!(
                        restate_storage_api::outbox_table::OutboxMessage::AttachInvocation(pat!(
                            restate_types::invocation::AttachInvocationRequest {
                                invocation_query: eq(InvocationQuery::from(target)),
                                block_on_inflight: eq(true),
                                response_sink: eq(
                                    ServiceInvocationResponseSink::partition_processor(
                                        invocation_id,
                                        completion_id,
                                    )
                                )
                            }
                        ))
                    )
                })),
                contains(matchers::actions::forward_notification(
                    invocation_id,
                    attach_invocation_completion.clone()
                ))
            ]
        );

        // Check journal
        assert_that!(
            test_env.read_journal_to_vec(invocation_id, 3).await,
            elements_are![
                property!(Entry.ty(), eq(EntryType::Command(CommandType::Input))),
                matchers::entry_eq(attach_invocation_command),
                matchers::entry_eq(attach_invocation_completion),
            ]
        );

        test_env.shutdown().await;
    }
}
