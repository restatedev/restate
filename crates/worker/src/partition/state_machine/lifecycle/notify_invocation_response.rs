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
use restate_storage_api::invocation_status_table::{InvocationStatus, InvocationStatusTable};
use restate_storage_api::journal_table as journal_table_v1;
use restate_storage_api::journal_table_v2;
use restate_storage_api::outbox_table::OutboxTable;
use restate_storage_api::promise_table::PromiseTable;
use restate_storage_api::state_table::StateTable;
use restate_storage_api::timer_table::TimerTable;
use restate_types::errors::NOT_READY_INVOCATION_ERROR;
use restate_types::identifiers::InvocationId;
use restate_types::invocation::InvocationEpoch;
use restate_types::journal::{Completion, CompletionResult};
use restate_types::journal_v2;
use restate_types::journal_v2::{
    AttachInvocationCompletion, AttachInvocationResult, CallCompletion, CallResult, CommandType,
    GetInvocationOutputCompletion, GetInvocationOutputResult, GetPromiseCompletion,
    GetPromiseResult, SleepCompletion,
};
use tracing::error;

pub struct OnNotifyInvocationResponse {
    pub invocation_id: InvocationId,
    pub invocation_epoch: InvocationEpoch,
    pub status: InvocationStatus,
    pub completion: Completion,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnNotifyInvocationResponse
where
    S: journal_table_v1::JournalTable
        + journal_table_v2::JournalTable
        + InvocationStatusTable
        + TimerTable
        + FsmTable
        + PromiseTable
        + StateTable
        + OutboxTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let OnNotifyInvocationResponse {
            invocation_id,
            invocation_epoch: this_completion_invocation_epoch,
            status,
            completion,
        } = self;
        let invocation_status = ctx.get_invocation_status(&invocation_id).await?;

        // Verify that we need to ingest this
        if !invocation_status
            .should_accept_completion(this_completion_invocation_epoch, completion.entry_index)
        {
            debug_if_leader!(
                ctx.is_leader,
                "Ignoring InvocationResponse epoch {} completion id {}",
                this_completion_invocation_epoch,
                completion.entry_index
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
            .get_command_by_completion_id(invocation_id, completion.entry_index)
            .await?
            .map(|(_, cmd)| cmd);

        if let Some(cmd) = command {
            let entry: journal_v2::Entry = match cmd.command_type() {
                CommandType::GetPromise => GetPromiseCompletion {
                    completion_id: completion.entry_index,
                    result: match completion.result {
                        CompletionResult::Success(s) => GetPromiseResult::Success(s),
                        CompletionResult::Failure(code, message) => {
                            GetPromiseResult::Failure(journal_v2::Failure { code, message })
                        }
                        CompletionResult::Empty => {
                            return Err(Error::BadCompletionVariantForInvocationResponse(
                                CommandType::GetPromise,
                                completion.entry_index,
                                "Empty",
                            ));
                        }
                    },
                }
                .into(),
                CommandType::Sleep => SleepCompletion {
                    completion_id: completion.entry_index,
                }
                .into(),
                CommandType::Call => CallCompletion {
                    completion_id: completion.entry_index,
                    result: match completion.result {
                        CompletionResult::Success(s) => CallResult::Success(s),
                        CompletionResult::Failure(code, message) => {
                            CallResult::Failure(journal_v2::Failure { code, message })
                        }
                        CompletionResult::Empty => {
                            return Err(Error::BadCompletionVariantForInvocationResponse(
                                CommandType::Call,
                                completion.entry_index,
                                "Empty",
                            ));
                        }
                    },
                }
                .into(),
                CommandType::AttachInvocation => AttachInvocationCompletion {
                    completion_id: completion.entry_index,
                    result: match completion.result {
                        CompletionResult::Success(s) => AttachInvocationResult::Success(s),
                        CompletionResult::Failure(code, message) => {
                            AttachInvocationResult::Failure(journal_v2::Failure { code, message })
                        }
                        CompletionResult::Empty => {
                            return Err(Error::BadCompletionVariantForInvocationResponse(
                                CommandType::AttachInvocation,
                                completion.entry_index,
                                "Empty",
                            ));
                        }
                    },
                }
                .into(),
                CommandType::GetInvocationOutput => {
                    GetInvocationOutputCompletion {
                        completion_id: completion.entry_index,
                        result: match completion.result {
                            CompletionResult::Success(s) => GetInvocationOutputResult::Success(s),
                            failure @ CompletionResult::Failure(_, _)
                                if failure
                                    == CompletionResult::from(&NOT_READY_INVOCATION_ERROR) =>
                            {
                                // Corner case with old journal/state machine
                                GetInvocationOutputResult::Void
                            }
                            CompletionResult::Failure(code, message) => {
                                GetInvocationOutputResult::Failure(journal_v2::Failure {
                                    code,
                                    message,
                                })
                            }
                            CompletionResult::Empty => GetInvocationOutputResult::Void,
                        },
                    }
                    .into()
                }
                cmd_ty => {
                    error!(
                        "Got an invocation response, the command type {cmd_ty} is unexpected for completion index {}. This indicates storage corruption.",
                        completion.entry_index
                    );
                    return Err(Error::BadCommandTypeForInvocationResponse(
                        cmd_ty,
                        completion.entry_index,
                    ));
                }
            };

            entries::OnJournalEntryCommand::from_entry(invocation_id, status, entry)
                .apply(ctx)
                .await?;
        } else {
            error!(
                "Got an invocation response, but there is no corresponding command in the journal for completion index {}. This indicates storage corruption.",
                completion.entry_index
            );
            return Err(Error::MissingCommandForInvocationResponse(
                completion.entry_index,
            ));
        }

        Ok(())
    }
}
