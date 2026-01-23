// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
use restate_types::journal_v2::{CompletionId, SleepCompletion};

pub struct OnNotifySleepCompletionCommand {
    pub invocation_id: InvocationId,
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
            status,
            completion_id,
        } = self;

        entries::OnJournalEntryCommand::from_entry(
            invocation_id,
            status,
            SleepCompletion { completion_id }.into(),
        )
        .apply(ctx)
        .await
    }
}
