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
use restate_storage_api::fsm_table::WriteFsmTable;
use restate_storage_api::invocation_status_table::{
    InvocationStatus, ReadInvocationStatusTable, WriteInvocationStatusTable,
};
use restate_storage_api::journal_table as journal_table_v1;
use restate_storage_api::journal_table_v2;
use restate_storage_api::outbox_table::WriteOutboxTable;
use restate_storage_api::promise_table::{ReadPromiseTable, WritePromiseTable};
use restate_storage_api::state_table::{ReadStateTable, WriteStateTable};
use restate_storage_api::timer_table::WriteTimerTable;
use restate_storage_api::vqueue_table::{ReadVQueueTable, WriteVQueueTable};
use restate_types::identifiers::InvocationId;
use restate_types::invocation::InvocationEpoch;
use restate_types::journal_v2::{CompletionId, SleepCompletion};

pub struct OnNotifySleepCompletionCommand {
    pub invocation_id: InvocationId,
    pub invocation_epoch: InvocationEpoch,
    pub status: InvocationStatus,
    pub completion_id: CompletionId,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnNotifySleepCompletionCommand
where
    S: journal_table_v1::WriteJournalTable
        + journal_table_v1::ReadJournalTable
        + journal_table_v2::WriteJournalTable
        + journal_table_v2::ReadJournalTable
        + ReadInvocationStatusTable
        + WriteInvocationStatusTable
        + WriteTimerTable
        + WriteFsmTable
        + ReadPromiseTable
        + WritePromiseTable
        + ReadStateTable
        + WriteStateTable
        + WriteOutboxTable
        + WriteVQueueTable
        + ReadVQueueTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let OnNotifySleepCompletionCommand {
            invocation_id,
            invocation_epoch: this_completion_invocation_epoch,
            status,
            completion_id,
        } = self;
        let invocation_status = ctx.get_invocation_status(&invocation_id).await?;

        // Verify that we need to ingest this
        if !invocation_status
            .should_accept_completion(this_completion_invocation_epoch, completion_id)
        {
            debug_if_leader!(
                ctx.is_leader,
                "Ignoring InvocationResponse epoch {} completion id {}",
                this_completion_invocation_epoch,
                completion_id
            );
            return Ok(());
        }

        entries::OnJournalEntryCommand::from_entry(
            invocation_id,
            status,
            SleepCompletion { completion_id }.into(),
        )
        .apply(ctx)
        .await
    }
}
