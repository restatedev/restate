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
    ReadInvocationStatusTable, WriteInvocationStatusTable,
};
use restate_storage_api::journal_table as journal_table_v1;
use restate_storage_api::journal_table_v2;
use restate_storage_api::outbox_table::WriteOutboxTable;
use restate_storage_api::promise_table::{ReadPromiseTable, WritePromiseTable};
use restate_storage_api::state_table::{ReadStateTable, WriteStateTable};
use restate_storage_api::timer_table::WriteTimerTable;
use restate_types::invocation::GetInvocationOutputResponse;
use restate_types::journal_v2::GetInvocationOutputCompletion;

pub struct OnNotifyGetInvocationOutputResponse(pub GetInvocationOutputResponse);

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnNotifyGetInvocationOutputResponse
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
        + WriteOutboxTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let invocation_status = ctx.get_invocation_status(&self.0.target.caller_id).await?;

        // Verify that we need to ingest this
        let this_completion_invocation_epoch = self.0.target.caller_invocation_epoch;
        let completion_id = self.0.target.caller_completion_id;
        if !invocation_status
            .should_accept_completion(this_completion_invocation_epoch, completion_id)
        {
            debug_if_leader!(
                ctx.is_leader,
                "Ignoring NotifyGetInvocationOutputResponse epoch {} completion id {}",
                this_completion_invocation_epoch,
                completion_id
            );
            return Ok(());
        }

        entries::OnJournalEntryCommand::from_entry(
            self.0.target.caller_id,
            ctx.get_invocation_status(&self.0.target.caller_id).await?,
            GetInvocationOutputCompletion {
                completion_id,
                result: self.0.result,
            }
            .into(),
        )
        .apply(ctx)
        .await
    }
}
