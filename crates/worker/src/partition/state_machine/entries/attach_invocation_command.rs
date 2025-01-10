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
use restate_storage_api::fsm_table::FsmTable;
use restate_storage_api::outbox_table::{OutboxMessage, OutboxTable};
use restate_storage_api::timer_table::TimerTable;
use restate_types::invocation::{AttachInvocationRequest, ServiceInvocationResponseSink};
use restate_types::journal_v2::AttachInvocationCommand;

pub(super) type ApplyAttachInvocationCommand<'e> =
    ApplyJournalCommandEffect<'e, AttachInvocationCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyAttachInvocationCommand<'e>
where
    S: TimerTable + OutboxTable + FsmTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        ctx.handle_outgoing_message(OutboxMessage::AttachInvocation(AttachInvocationRequest {
            invocation_query: self.entry.target.into(),
            block_on_inflight: true,
            response_sink: ServiceInvocationResponseSink::partition_processor(
                self.invocation_id,
                self.entry.completion_id,
            ),
        }))
        .await?;

        Ok(())
    }
}
