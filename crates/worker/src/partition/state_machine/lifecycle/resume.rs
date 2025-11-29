// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_invoker_api::InvokeInputJournal;
use restate_storage_api::invocation_status_table::InvocationStatus;
use restate_storage_api::vqueue_table::{ReadVQueueTable, WriteVQueueTable};
use restate_types::config::Configuration;
use restate_types::identifiers::InvocationId;

use crate::debug_if_leader;
use crate::partition::state_machine::{Action, CommandHandler, Error, StateMachineApplyContext};

pub struct ResumeInvocationCommand<'e> {
    pub invocation_id: InvocationId,
    pub invocation_status: &'e mut InvocationStatus,
}

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ResumeInvocationCommand<'e>
where
    S: WriteVQueueTable + ReadVQueueTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let Some(metadata) = self.invocation_status.get_invocation_metadata_mut() else {
            return Ok(());
        };

        debug_if_leader!(
            ctx.is_leader,
            restate.journal.length = metadata.journal_metadata.length,
            "Effect: Resume service"
        );
        let invocation_target = metadata.invocation_target.clone();

        metadata.timestamps.update(ctx.record_created_at);

        if Configuration::pinned().common.experimental_enable_vqueues {
            ctx.vqueue_move_invocation_to_inbox_stage(&self.invocation_id)
                .await?;
        } else {
            ctx.action_collector.push(Action::Invoke {
                invocation_id: self.invocation_id,
                invocation_target,
                invoke_input_journal: InvokeInputJournal::NoCachedJournal,
            });
        }

        *self.invocation_status = InvocationStatus::Invoked(metadata.clone());

        Ok(())
    }
}
