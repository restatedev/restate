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
use restate_storage_api::state_table::StateTable;
use restate_storage_api::timer_table::TimerTable;
use restate_types::identifiers::InvocationId;
use restate_types::invocation::TerminationFlavor;
use restate_types::journal_v2::CANCEL_SIGNAL;
use tracing::{debug, trace};

pub struct OnCancelCommand {
    pub invocation_id: InvocationId,
    pub invocation_status: InvocationStatus,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnCancelCommand
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
        + PromiseTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        match self.invocation_status {
            is @ InvocationStatus::Invoked(_) | is @ InvocationStatus::Suspended { .. } => {
                OnJournalEntryCommand::from_entry(self.invocation_id, is, CANCEL_SIGNAL.into())
                    .apply(ctx)
                    .await?;
            }
            InvocationStatus::Inboxed(inboxed) => {
                ctx.terminate_inboxed_invocation(
                    TerminationFlavor::Cancel,
                    self.invocation_id,
                    inboxed,
                )
                .await?
            }
            InvocationStatus::Scheduled(scheduled) => {
                ctx.terminate_scheduled_invocation(
                    TerminationFlavor::Cancel,
                    self.invocation_id,
                    scheduled,
                )
                .await?
            }
            InvocationStatus::Killed(_) => {
                trace!(
                    "Received cancel command for an already killed invocation '{}'.",
                    self.invocation_id
                );
                // Nothing to do here really, let's send again the abort signal to the invoker just in case
                ctx.send_abort_invocation_to_invoker(self.invocation_id, true);
            }
            InvocationStatus::Completed(_) => {
                debug!("Received cancel command for completed invocation '{}'. To cleanup the invocation after it's been completed, use the purge invocation command.", self.invocation_id);
            }
            InvocationStatus::Free => {
                trace!(
                    "Received cancel command for unknown invocation with id '{}'.",
                    self.invocation_id
                );
                // We still try to send the abort signal to the invoker,
                // as it might be the case that previously the user sent an abort signal
                // but some message was still between the invoker/PP queues.
                // This can happen because the invoke/resume and the abort invoker messages end up in different queues,
                // and the abort message can overtake the invoke/resume.
                // Consequently the invoker might have not received the abort and the user tried to send it again.
                ctx.send_abort_invocation_to_invoker(self.invocation_id, false);
            }
        };

        Ok(())
    }
}
