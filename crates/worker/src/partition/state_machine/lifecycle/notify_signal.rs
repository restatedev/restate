// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::state_machine::entries::OnJournalEntryCommand;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};
use restate_storage_api::fsm_table::FsmTable;
use restate_storage_api::inbox_table::InboxTable;
use restate_storage_api::invocation_status_table::{InvocationStatus, InvocationStatusTable};
use restate_storage_api::journal_table;
use restate_storage_api::journal_table_v2::JournalTable;
use restate_storage_api::outbox_table::OutboxTable;
use restate_storage_api::promise_table::PromiseTable;
use restate_storage_api::service_status_table::VirtualObjectStatusTable;
use restate_storage_api::state_table::StateTable;
use restate_storage_api::timer_table::TimerTable;
use restate_types::identifiers::InvocationId;
use restate_types::journal_v2::{BuiltInSignal, Signal, SignalId};

pub struct OnNotifySignalCommand {
    pub invocation_id: InvocationId,
    pub invocation_status: InvocationStatus,
    pub signal: Signal,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnNotifySignalCommand
where
    S: JournalTable
        + InvocationStatusTable
        + InboxTable
        + FsmTable
        + StateTable
        + JournalTable
        + OutboxTable
        + journal_table::JournalTable
        + TimerTable
        + PromiseTable
        + VirtualObjectStatusTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let OnNotifySignalCommand {
            invocation_id,
            invocation_status,
            signal,
        } = self;

        if signal.id == SignalId::for_builtin_signal(BuiltInSignal::Cancel) {
            // Special handling to deal with inboxed and scheduled invocations
            // TODO this is the code when we'll get rid of protocol <= 3
            // OnCancelCommand {
            //     invocation_id,
            //     invocation_status,
            // }.apply(ctx).await
            ctx.on_cancel_invocation(invocation_id).await
        } else {
            // Normal handling, append to the journal.
            OnJournalEntryCommand::from_entry(invocation_id, invocation_status, signal.into())
                .apply(ctx)
                .await
        }
    }
}
