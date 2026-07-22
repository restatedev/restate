// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::timer_table::WriteTimerTable;
use restate_types::journal_v2::command::SleepCommand;
use restate_wal_protocol::timer::TimerKeyValue;

use crate::partition::processor::ProcessorContext;
use crate::partition::state_machine::entries::ApplyJournalCommandEffect;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};

pub(super) type ApplySleepCommand<'e> = ApplyJournalCommandEffect<'e, SleepCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S, P> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S, P>>
    for ApplySleepCommand<'e>
where
    S: WriteTimerTable,
    P: ProcessorContext,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S, P>) -> Result<(), Error> {
        let invocation_metadata = self
            .invocation_status
            .get_invocation_metadata()
            .expect("In-Flight invocation metadata must be present");

        ctx.register_timer(
            TimerKeyValue::complete_journal_entry(
                self.entry.wake_up_time,
                self.invocation_id,
                self.entry.completion_id,
            ),
            invocation_metadata.journal_metadata.span_context.clone(),
        )?;

        Ok(())
    }
}
