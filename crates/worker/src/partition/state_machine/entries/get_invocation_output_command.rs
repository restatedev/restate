// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use restate_types::journal_v2::GetInvocationOutputCommand;

pub(super) type ApplyGetInvocationOutputCommand<'e> =
    ApplyJournalCommandEffect<'e, GetInvocationOutputCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyGetInvocationOutputCommand<'e>
where
    S: WriteTimerTable + WriteOutboxTable + WriteFsmTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        ctx.handle_outgoing_message(OutboxMessage::AttachInvocation(AttachInvocationRequest {
            invocation_query: self.entry.target.into(),
            block_on_inflight: false,
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
    use restate_types::errors::NOT_READY_INVOCATION_ERROR;
    use restate_types::identifiers::{IdempotencyId, InvocationId, ServiceId};
    use restate_types::invocation::{
        GetInvocationOutputResponse, InvocationQuery, InvocationResponse, JournalCompletionTarget,
        ResponseResult, ServiceInvocationResponseSink,
    };
    use restate_types::journal_v2::{
        AttachInvocationTarget, CommandType, Entry, EntryMetadata, EntryType,
        GetInvocationOutputCommand, GetInvocationOutputCompletion, GetInvocationOutputResult,
    };
    use restate_wal_protocol::Command;
    use rstest::rstest;

    #[rstest]
    #[restate_core::test]
    async fn get_invocation_output(
        #[values(true, false)] complete_using_notify_get_invocation_output: bool,
        #[values(true, false)] complete_with_not_ready: bool,
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
        let expected_get_invocation_result = if complete_with_not_ready {
            GetInvocationOutputResult::Void
        } else {
            GetInvocationOutputResult::Success(success_result.clone())
        };

        let get_invocation_output_command = GetInvocationOutputCommand {
            target: target.clone(),
            name: Default::default(),
            completion_id,
        };
        let response_command = if complete_using_notify_get_invocation_output {
            Command::NotifyGetInvocationOutputResponse(GetInvocationOutputResponse {
                target: JournalCompletionTarget::from_parts(invocation_id, completion_id),
                result: expected_get_invocation_result.clone(),
            })
        } else {
            Command::InvocationResponse(InvocationResponse {
                target: JournalCompletionTarget::from_parts(invocation_id, completion_id),
                result: if complete_with_not_ready {
                    ResponseResult::Failure(NOT_READY_INVOCATION_ERROR)
                } else {
                    ResponseResult::Success(success_result.clone())
                },
            })
        };

        let actions = test_env
            .apply_multiple([
                invoker_entry_effect(invocation_id, get_invocation_output_command.clone()),
                response_command,
            ])
            .await;

        let get_invocation_output_completion = GetInvocationOutputCompletion {
            completion_id,
            result: expected_get_invocation_result,
        };
        assert_that!(
            actions,
            all![
                contains(pat!(Action::NewOutboxMessage {
                    message: pat!(
                        restate_storage_api::outbox_table::OutboxMessage::AttachInvocation(pat!(
                            restate_types::invocation::AttachInvocationRequest {
                                invocation_query: eq(InvocationQuery::from(target)),
                                block_on_inflight: eq(false),
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
                    get_invocation_output_completion.clone()
                ))
            ]
        );

        // Check journal
        assert_that!(
            test_env.read_journal_to_vec(invocation_id, 3).await,
            elements_are![
                property!(Entry.ty(), eq(EntryType::Command(CommandType::Input))),
                matchers::entry_eq(get_invocation_output_command),
                matchers::entry_eq(get_invocation_output_completion),
            ]
        );

        test_env.shutdown().await;
    }
}
