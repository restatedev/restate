// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Operator pause and resume. Pause and wake are the vqueue plumbing both families share.

use tracing::warn;

use restate_storage_api::invocation_status_table::{
    InvocationStatus, ReadInvocationStatusTable, WriteInvocationStatusTable,
};
use restate_storage_api::journal_events::WriteJournalEventsTable;
use restate_storage_api::lock_table::WriteLockTable;
use restate_storage_api::vqueue_table::{ReadVQueueTable, WriteVQueueTable};
use restate_types::identifiers::InvocationId;
use restate_types::invocation::InvocationMutationResponseSink;
use restate_types::invocation::client::{PauseInvocationResponse, ResumeInvocationResponse};
use restate_types::journal_events::raw::RawEvent;
use restate_types::journal_events::{AttemptCause, AttemptOutcome, Event, PausedEvent};
use restate_types::vqueues::EntryTargetExt;

use super::{AttemptEnd, lsn, record_attempt_ended, set_wake_up};
use crate::partition::processor::ProcessorContext;
use crate::partition::state_machine::lifecycle::pause_invocation;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};

pub struct OnManualPause {
    pub invocation_id: InvocationId,
    pub status: InvocationStatus,
    pub response_sink: Option<InvocationMutationResponseSink>,
}

impl<'ctx, 's: 'ctx, S, P> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S, P>>
    for OnManualPause
where
    S: ReadInvocationStatusTable
        + WriteInvocationStatusTable
        + WriteJournalEventsTable
        + WriteVQueueTable
        + ReadVQueueTable
        + WriteLockTable,
    P: ProcessorContext,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S, P>) -> Result<(), Error> {
        let OnManualPause {
            invocation_id,
            status,
            response_sink,
        } = self;
        let paused_event = RawEvent::from(Event::Paused(PausedEvent {
            last_failure: None,
            lsn: lsn(ctx),
        }));
        let response = match status {
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
                        AttemptEnd::new("paused", AttemptOutcome::Paused),
                        None,
                    )?;
                }
                pause_invocation(ctx, &invocation_id, metadata, paused_event).await?;
                // The running attempt is aborted; what it applied stays.
                ctx.send_abort_invocation_to_invoker(invocation_id);
                PauseInvocationResponse::Accepted
            }
            InvocationStatus::Suspended { metadata, .. } => {
                pause_invocation(ctx, &invocation_id, metadata, paused_event).await?;
                PauseInvocationResponse::Accepted
            }
            InvocationStatus::Paused(_) => PauseInvocationResponse::AlreadyPaused,
            InvocationStatus::Scheduled(_)
            | InvocationStatus::Inboxed(_)
            | InvocationStatus::Completed(_) => PauseInvocationResponse::NotRunning,
            InvocationStatus::Free => PauseInvocationResponse::NotFound,
        };
        ctx.reply_to_pause_invocation(response_sink, response);
        Ok(())
    }
}

pub struct OnManualResume {
    pub invocation_id: InvocationId,
    pub status: InvocationStatus,
    pub patches_deployment: bool,
    pub response_sink: Option<InvocationMutationResponseSink>,
}

impl<'ctx, 's: 'ctx, S, P> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S, P>>
    for OnManualResume
where
    S: ReadInvocationStatusTable
        + WriteInvocationStatusTable
        + WriteVQueueTable
        + WriteLockTable
        + ReadVQueueTable,
    P: ProcessorContext,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S, P>) -> Result<(), Error> {
        let OnManualResume {
            invocation_id,
            status,
            patches_deployment,
            response_sink,
        } = self;
        if patches_deployment {
            warn!(
                restate.invocation.id = %invocation_id,
                "Ignoring the deployment patch of a resume: not supported for the explicit recovery protocol yet"
            );
        }
        let response = match status {
            InvocationStatus::Invoked(metadata) => {
                // Pull a backing-off attempt forward; a running attempt is left alone.
                if metadata.vqueue_id.is_some() {
                    ctx.vqueue_reschedule_invocation(
                        &invocation_id,
                        None,
                        &metadata.invocation_target.entry_target_ref(),
                        None,
                    )
                    .await?;
                }
                ResumeInvocationResponse::Ok
            }
            InvocationStatus::Suspended { mut metadata, .. }
            | InvocationStatus::Paused(mut metadata) => {
                set_wake_up(&mut metadata, AttemptCause::Resume, None);
                ctx.do_resume_service(invocation_id, metadata).await?;
                ResumeInvocationResponse::Ok
            }
            InvocationStatus::Scheduled(_) | InvocationStatus::Inboxed(_) => {
                ResumeInvocationResponse::NotStarted
            }
            InvocationStatus::Completed(_) => ResumeInvocationResponse::Completed,
            InvocationStatus::Free => ResumeInvocationResponse::NotFound,
        };
        ctx.reply_to_resume_invocation(response_sink, response);
        Ok(())
    }
}
