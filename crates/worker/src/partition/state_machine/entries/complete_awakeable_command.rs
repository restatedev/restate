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
use crate::partition::types::OutboxMessageExt;
use restate_storage_api::fsm_table::FsmTable;
use restate_storage_api::outbox_table::{OutboxMessage, OutboxTable};
use restate_storage_api::state_table::StateTable;
use restate_types::invocation::{NotifySignalRequest, ResponseResult};
use restate_types::journal_v2::{
    CompleteAwakeableCommand, CompleteAwakeableId, CompleteAwakeableResult, Signal, SignalResult,
};

pub(super) type ApplyCompleteAwakeableCommand<'e> =
    ApplyJournalCommandEffect<'e, CompleteAwakeableCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyCompleteAwakeableCommand<'e>
where
    S: StateTable + OutboxTable + FsmTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        ctx.handle_outgoing_message(match self.entry.id {
            CompleteAwakeableId::Old(old_awakeable_id) => {
                let (invocation_id, entry_index) = old_awakeable_id.into_inner();
                OutboxMessage::from_awakeable_completion(
                    invocation_id,
                    entry_index,
                    match self.entry.result {
                        CompleteAwakeableResult::Success(s) => ResponseResult::Success(s),
                        CompleteAwakeableResult::Failure(f) => ResponseResult::Failure(f.into()),
                    },
                )
            }
            CompleteAwakeableId::New(new_awakeable_id) => {
                let (invocation_id, signal_id) = new_awakeable_id.into_inner();
                OutboxMessage::NotifySignal(NotifySignalRequest {
                    invocation_id,
                    signal: Signal::new(
                        signal_id,
                        match self.entry.result {
                            CompleteAwakeableResult::Success(s) => SignalResult::Success(s),
                            CompleteAwakeableResult::Failure(f) => SignalResult::Failure(f),
                        },
                    ),
                })
            }
        })
        .await?;
        Ok(())
    }
}
