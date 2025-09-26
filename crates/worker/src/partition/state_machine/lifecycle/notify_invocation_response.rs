// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::debug_if_leader;
use crate::partition::state_machine::invocation_status_ext::InvocationStatusExt;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext, entries};
use restate_storage_api::fsm_table::FsmTable;
use restate_storage_api::invocation_status_table::{
    InvocationStatus, ReadInvocationStatusTable, WriteInvocationStatusTable,
};
use restate_storage_api::journal_table as journal_table_v1;
use restate_storage_api::journal_table_v2;
use restate_storage_api::outbox_table::WriteOutboxTable;
use restate_storage_api::promise_table::PromiseTable;
use restate_storage_api::state_table::StateTable;
use restate_storage_api::timer_table::TimerTable;
use restate_types::errors::NOT_READY_INVOCATION_ERROR;
use restate_types::identifiers::InvocationId;
use restate_types::invocation::{InvocationEpoch, ResponseResult};
use restate_types::journal_v2;
use restate_types::journal_v2::{
    AttachInvocationCompletion, AttachInvocationResult, CallCompletion, CallResult, CommandType,
    CompletionId, GetInvocationOutputCompletion, GetInvocationOutputResult, GetPromiseCompletion,
    GetPromiseResult, SleepCompletion,
};
use tracing::error;

pub struct OnNotifyInvocationResponse {
    pub invocation_id: InvocationId,
    pub invocation_epoch: InvocationEpoch,
    pub status: InvocationStatus,
    pub caller_completion_id: CompletionId,
    pub result: ResponseResult,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnNotifyInvocationResponse
where
    S: journal_table_v1::WriteJournalTable
        + journal_table_v1::ReadJournalTable
        + journal_table_v2::WriteJournalTable
        + journal_table_v2::ReadJournalTable
        + TimerTable
        + ReadInvocationStatusTable
        + WriteInvocationStatusTable
        + FsmTable
        + PromiseTable
        + StateTable
        + WriteOutboxTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let OnNotifyInvocationResponse {
            invocation_id,
            invocation_epoch: this_completion_invocation_epoch,
            status,
            caller_completion_id,
            result,
        } = self;

        // Verify that we need to ingest this
        if !status.should_accept_completion(this_completion_invocation_epoch, caller_completion_id)
        {
            debug_if_leader!(
                ctx.is_leader,
                "Ignoring InvocationResponse epoch {} completion id {}",
                this_completion_invocation_epoch,
                caller_completion_id
            );
            return Ok(());
        }

        // This code needs to be revisited once we remove Service Protocol <= V3, depending on what we still want to carry in InvocationResponse.
        //
        // For the time being we got the following completions carried using `InvocationResponse`:
        // * Promise completions: in theory we don't need another wal command at all, see comment in `ApplyCompletePromiseCommand` and https://github.com/restatedev/restate/pull/2656.
        // * Call and Attach completions: this is supposed to stay after we remove service protocol <= 3
        // * Sleep completions: we don't use it, it's there for defensive purpose.
        // * GetInvocationOutput completions: we have an ad-hoc command for this already, but we don't produce it yet. We can start producing it when removing service protocol <= 3

        let command = ctx
            .storage
            .get_command_by_completion_id(invocation_id, caller_completion_id)
            .await?
            .map(|(_, cmd)| cmd);

        if let Some(cmd) = command {
            let entry: journal_v2::Entry = match cmd.command_type() {
                CommandType::GetPromise => GetPromiseCompletion {
                    completion_id: caller_completion_id,
                    result: match result {
                        ResponseResult::Success(s) => GetPromiseResult::Success(s),
                        ResponseResult::Failure(err) => GetPromiseResult::Failure(err.into()),
                    },
                }
                .into(),
                CommandType::Sleep => SleepCompletion {
                    completion_id: caller_completion_id,
                }
                .into(),
                CommandType::Call => CallCompletion {
                    completion_id: caller_completion_id,
                    result: match result {
                        ResponseResult::Success(s) => CallResult::Success(s),
                        ResponseResult::Failure(err) => CallResult::Failure(err.into()),
                    },
                }
                .into(),
                CommandType::AttachInvocation => AttachInvocationCompletion {
                    completion_id: caller_completion_id,
                    result: match result {
                        ResponseResult::Success(s) => AttachInvocationResult::Success(s),
                        ResponseResult::Failure(err) => AttachInvocationResult::Failure(err.into()),
                    },
                }
                .into(),
                CommandType::GetInvocationOutput => {
                    GetInvocationOutputCompletion {
                        completion_id: caller_completion_id,
                        result: match result {
                            ResponseResult::Success(s) => GetInvocationOutputResult::Success(s),
                            ResponseResult::Failure(err) if err == NOT_READY_INVOCATION_ERROR => {
                                // Corner case with old journal/state machine
                                GetInvocationOutputResult::Void
                            }
                            ResponseResult::Failure(err) => {
                                GetInvocationOutputResult::Failure(err.into())
                            }
                        },
                    }
                    .into()
                }
                cmd_ty => {
                    error!(
                        "Got an invocation response, the command type {cmd_ty} is unexpected for completion index {}. This indicates storage corruption.",
                        caller_completion_id
                    );
                    return Err(Error::BadCommandTypeForInvocationResponse(
                        cmd_ty,
                        caller_completion_id,
                    ));
                }
            };

            entries::OnJournalEntryCommand::from_entry(invocation_id, status, entry)
                .apply(ctx)
                .await?;
        } else {
            error!(
                "Got an invocation response, but there is no corresponding command in the journal for completion index {}. This indicates storage corruption.",
                caller_completion_id
            );
            return Err(Error::MissingCommandForInvocationResponse(
                caller_completion_id,
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::partition::state_machine::tests::{TestEnv, fixtures, matchers};
    use googletest::prelude::*;
    use restate_types::errors::InvocationError;
    use restate_types::identifiers::ServiceId;
    use restate_types::invocation::{
        InvocationResponse, InvocationTarget, JournalCompletionTarget,
    };
    use restate_types::journal_v2::EntryMetadata;
    use restate_types::journal_v2::{
        CallCommand, CallInvocationIdCompletion, CallRequest, Entry, EntryType,
    };
    use restate_wal_protocol::Command;

    #[restate_core::test]
    async fn reply_to_call_with_failure_and_metadata() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        let invocation_id_completion_id = 1;
        let result_completion_id = 2;
        let callee_service_id = ServiceId::mock_random();
        let callee_invocation_target =
            InvocationTarget::mock_from_service_id(callee_service_id.clone());
        let callee_invocation_id = InvocationId::mock_generate(&callee_invocation_target);
        let expected_failure =
            InvocationError::new(512u16, "my custom error").with_metadata("mytype", "sometype");

        let call_command = CallCommand {
            request: CallRequest::mock(callee_invocation_id, callee_invocation_target.clone()),
            invocation_id_completion_id,
            result_completion_id,
            name: Default::default(),
        };
        let actions = test_env
            .apply_multiple([
                fixtures::invoker_entry_effect(invocation_id, call_command.clone()),
                Command::InvocationResponse(InvocationResponse {
                    target: JournalCompletionTarget::from_parts(
                        invocation_id,
                        result_completion_id,
                        0,
                    ),
                    result: ResponseResult::Failure(expected_failure.clone()),
                }),
            ])
            .await;

        let call_invocation_id_completion = CallInvocationIdCompletion {
            completion_id: invocation_id_completion_id,
            invocation_id: callee_invocation_id,
        };
        let call_completion = CallCompletion {
            completion_id: result_completion_id,
            result: CallResult::Failure(expected_failure.into()),
        };
        assert_that!(
            actions,
            all![
                contains(matchers::actions::forward_notification(
                    invocation_id,
                    call_invocation_id_completion.clone()
                )),
                contains(matchers::actions::forward_notification(
                    invocation_id,
                    call_completion.clone()
                ))
            ]
        );

        // Check journal
        assert_that!(
            test_env.read_journal_to_vec(invocation_id, 4).await,
            elements_are![
                property!(Entry.ty(), eq(EntryType::Command(CommandType::Input))),
                matchers::entry_eq(call_command),
                matchers::entry_eq(call_invocation_id_completion),
                matchers::entry_eq(call_completion),
            ]
        );

        test_env.shutdown().await;
    }
}
