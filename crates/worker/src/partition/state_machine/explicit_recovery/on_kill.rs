// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tracing::debug;

use restate_storage_api::invocation_status_table::InvocationStatus;
use restate_types::errors::KILLED_INVOCATION_ERROR;
use restate_types::identifiers::InvocationId;
use restate_types::invocation::client::KillInvocationResponse;
use restate_types::invocation::{
    InvocationMutationResponseSink, ResponseResult, TerminationFlavor,
};
use restate_types::journal_events::AttemptOutcome;

use super::end_invocation::end_invocation;
use super::{AttemptEnd, ErEndStorage, event_failure, record_attempt_ended};
use crate::partition::processor::ProcessorContext;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};

/// A kill ends the invocation it names, and no other: the invocations it called go on (D33).
pub struct OnKill {
    pub invocation_id: InvocationId,
    pub status: InvocationStatus,
    pub response_sink: Option<InvocationMutationResponseSink>,
}

impl<'ctx, 's: 'ctx, S, P> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S, P>> for OnKill
where
    S: ErEndStorage,
    P: ProcessorContext,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S, P>) -> Result<(), Error> {
        let OnKill {
            invocation_id,
            status,
            response_sink,
        } = self;
        match status {
            InvocationStatus::Invoked(mut metadata) => {
                // A running attempt ends here; one backing off already recorded its end.
                if metadata
                    .explicit_recovery
                    .as_ref()
                    .is_some_and(|er| !er.attempt_ended)
                {
                    record_attempt_ended(
                        ctx,
                        &invocation_id,
                        &mut metadata,
                        AttemptEnd::new("killed", AttemptOutcome::Killed)
                            .failure(Some(event_failure(&KILLED_INVOCATION_ERROR))),
                        None,
                    )?;
                }
                end_invocation(
                    ctx,
                    invocation_id,
                    metadata,
                    Some(TerminationFlavor::Kill),
                    ResponseResult::Failure(KILLED_INVOCATION_ERROR),
                )
                .await?;
                ctx.send_abort_invocation_to_invoker(invocation_id);
                ctx.reply_to_kill(response_sink, KillInvocationResponse::Ok);
            }
            InvocationStatus::Suspended { metadata, .. } | InvocationStatus::Paused(metadata) => {
                end_invocation(
                    ctx,
                    invocation_id,
                    metadata,
                    Some(TerminationFlavor::Kill),
                    ResponseResult::Failure(KILLED_INVOCATION_ERROR),
                )
                .await?;
                ctx.send_abort_invocation_to_invoker(invocation_id);
                ctx.reply_to_kill(response_sink, KillInvocationResponse::Ok);
            }
            InvocationStatus::Inboxed(inboxed) => {
                ctx.terminate_inboxed_invocation(TerminationFlavor::Kill, invocation_id, inboxed)
                    .await?;
                ctx.reply_to_kill(response_sink, KillInvocationResponse::Ok);
            }
            InvocationStatus::Scheduled(scheduled) => {
                ctx.terminate_scheduled_invocation(
                    TerminationFlavor::Kill,
                    invocation_id,
                    scheduled,
                )
                .await?;
                ctx.reply_to_kill(response_sink, KillInvocationResponse::Ok);
            }
            InvocationStatus::Completed(_) => {
                debug!(
                    "Received kill command for completed invocation '{invocation_id}'. To cleanup the invocation after it's been completed, use the purge invocation command."
                );
                ctx.reply_to_kill(response_sink, KillInvocationResponse::AlreadyCompleted);
            }
            InvocationStatus::Free => {
                ctx.send_abort_invocation_to_invoker(invocation_id);
                ctx.reply_to_kill(response_sink, KillInvocationResponse::NotFound);
            }
        }
        Ok(())
    }
}
