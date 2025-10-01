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
use restate_storage_api::fsm_table::WriteFsmTable;
use restate_storage_api::outbox_table::{OutboxMessage, WriteOutboxTable};
use restate_storage_api::state_table::WriteStateTable;
use restate_types::invocation::NotifySignalRequest;
use restate_types::journal_v2::{SendSignalCommand, Signal};

pub(super) type ApplySendSignalCommand<'e> = ApplyJournalCommandEffect<'e, SendSignalCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplySendSignalCommand<'e>
where
    S: WriteStateTable + WriteOutboxTable + WriteFsmTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        ctx.handle_outgoing_message(OutboxMessage::NotifySignal(NotifySignalRequest {
            invocation_id: self.entry.target_invocation_id,
            signal: Signal::new(self.entry.signal_id, self.entry.result),
        }))?;
        Ok(())
    }
}
