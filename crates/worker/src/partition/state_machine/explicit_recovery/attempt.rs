// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The start and the endings of an attempt that do not complete the invocation: failure,
//! restart (with the D63 loop bound), suspend (with the D34 count check).

use tracing::debug;

use restate_clock::UniqueTimestamp;
use restate_storage_api::invocation_status_table::{InFlightInvocationMetadata, InvocationStatus};
use restate_storage_api::vqueue_table::scheduler::YieldReason;
use restate_storage_api::vqueue_table::{EntryMetadata, EntryStatusHeader};
use restate_types::errors::InvocationErrorCode;
use restate_types::identifiers::{BaseEntryId, DeploymentId, InvocationId};
use restate_types::invocation::ResponseResult;
use restate_types::journal_events::raw::RawEvent;
use restate_types::journal_events::{
    AttemptCause, AttemptOutcome, AttemptStartedEvent, Event, EventFailure, OutstandingResult,
    PausedEvent, TransientErrorEvent,
};
use restate_types::journal_v2::{NotificationId, UnresolvedFuture};
use restate_types::vqueues::EntryTargetExt;
use restate_util_string::ToReString;
use restate_vqueues::VQueue;
use restate_worker_api::invoker::explicit_recovery::{ErFailureOutcome, RestartRule};

use super::end_invocation::end_invocation;
use super::{
    AttemptEnd, ErStorage, MAX_IDLE_RESTARTS, er_of, lsn, put_event, record_attempt_ended,
};
use crate::partition::processor::ProcessorContext;
use crate::partition::state_machine::lifecycle::{
    ApplyEventCommand, YieldInvocationCommand, pause_invocation,
};
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};

/// The attempt picked its deployment. The first attempt pins it (D7).
pub(super) async fn on_attempt_started<S, P>(
    ctx: &mut StateMachineApplyContext<'_, S, P>,
    invocation_id: InvocationId,
    mut metadata: InFlightInvocationMetadata,
    deployment_id: DeploymentId,
    protocol_version: u16,
) -> Result<(), Error>
where
    S: ErStorage,
    P: ProcessorContext,
{
    let er = er_of(&mut metadata);
    let attempt = er.attempt;
    let scheduled_at = er.run_at.map(|t| t.as_u64());
    // No cause noted: the first attempt, or a wake-up from outside this module.
    let (cause, woken_by) = er.cause.clone().unwrap_or(if attempt == 1 {
        (AttemptCause::First, None)
    } else {
        (AttemptCause::Resume, None)
    });
    if er.pinned_deployment_id.is_none() {
        er.pinned_deployment_id = Some(deployment_id);
        if let Some(header) = ctx
            .storage
            .get_vqueue_entry_status(&BaseEntryId::from(invocation_id))
            .await?
        {
            let mut entry_metadata = header.metadata().clone();
            entry_metadata.deployment = Some(deployment_id.to_restring());
            VQueue::get(
                header.vqueue_id(),
                ctx.storage,
                ctx.processor.vqueues_mut(),
                ctx.leader.is_some().then_some(ctx.action_collector),
            )
            .await?
            .expect("pinning in a non-existent vqueue")
            .update_entry_metadata(
                &header,
                &metadata.invocation_target.entry_target_ref(),
                &entry_metadata,
            );
        }
        metadata.timestamps.update(ctx.record_created_at);
    }

    let event = Event::AttemptStarted(AttemptStartedEvent {
        attempt,
        deployment_id: deployment_id.to_string(),
        protocol_version: u32::from(protocol_version),
        cause: Some(cause),
        woken_by: woken_by.filter(|_| cause == AttemptCause::WakeUp),
        lsn: lsn(ctx),
        scheduled_at,
    });
    put_event(ctx, &invocation_id, &metadata, event)?;
    ctx.storage
        .put_invocation_status(&invocation_id, &InvocationStatus::Invoked(metadata))?;
    Ok(())
}

/// The attempt failed, or an idle restart counts as a failure: retry, pause or fail.
pub(super) async fn on_failed<S, P>(
    ctx: &mut StateMachineApplyContext<'_, S, P>,
    invocation_id: InvocationId,
    metadata: InFlightInvocationMetadata,
    error_event: Option<RawEvent>,
    outcome: ErFailureOutcome,
) -> Result<(), Error>
where
    S: ErStorage,
    P: ProcessorContext,
{
    match outcome {
        ErFailureOutcome::Retry {
            resume_at,
            retry_attempts,
        } => {
            if let Some(event) = error_event {
                ApplyEventCommand {
                    invocation_id: &invocation_id,
                    invocation_status: &InvocationStatus::Invoked(metadata.clone()),
                    event,
                }
                .apply(ctx)
                .await?;
            }
            YieldInvocationCommand {
                invocation_id: &invocation_id,
                invocation_target: Some(&metadata.invocation_target),
                yield_reason: YieldReason::TransientError {
                    retry_attempts,
                    retry_count_since_last_stored_command: 0,
                },
                resume_at,
            }
            .apply(ctx)
            .await
        }
        ErFailureOutcome::Pause => {
            let last_failure = error_event.and_then(|event| match event.into_event_or_unknown() {
                Event::TransientError(failure) => Some(failure),
                _ => None,
            });
            pause_invocation(
                ctx,
                &invocation_id,
                metadata,
                RawEvent::from(Event::Paused(PausedEvent { last_failure })),
            )
            .await
        }
        ErFailureOutcome::Fail(error) => {
            end_invocation(
                ctx,
                invocation_id,
                metadata,
                None,
                ResponseResult::Failure(error),
            )
            .await
        }
    }
}

/// The attempt was shut down and the invocation starts a new attempt, unless it is the fourth
/// idle restart in a row (D63): then it is a failure.
pub(super) async fn on_restart<S, P>(
    ctx: &mut StateMachineApplyContext<'_, S, P>,
    invocation_id: InvocationId,
    mut metadata: InFlightInvocationMetadata,
    rule: RestartRule,
    idle_fallback: ErFailureOutcome,
) -> Result<(), Error>
where
    S: ErStorage,
    P: ProcessorContext,
{
    let er = er_of(&mut metadata);
    if rule.may_be_idle() && !er.action_applied {
        er.idle_restart_streak = er.idle_restart_streak.saturating_add(1);
        let streak = er.idle_restart_streak;
        if streak > MAX_IDLE_RESTARTS {
            debug!(
                restate.invocation.id = %invocation_id,
                "Idle restart {streak} in a row: treated as a failure"
            );
            let event = TransientErrorEvent {
                error_code: InvocationErrorCode::from(500u16),
                error_message: format!(
                    "the handler was restarted {streak} times in a row without applying any action. Handlers on the explicit recovery protocol must send heartbeats while they work"
                ),
                error_stacktrace: None,
                restate_doc_error_code: None,
                related_command_index: None,
                related_command_name: None,
                related_command_type: None,
            };
            let failure = EventFailure {
                code: u32::from(u16::from(event.error_code)),
                message: event.error_message.clone(),
            };
            let (end, next) = AttemptEnd::failed("restart loop", Some(failure), &idle_fallback);
            record_attempt_ended(ctx, &invocation_id, &mut metadata, end, next)?;
            return on_failed(
                ctx,
                invocation_id,
                metadata,
                Some(RawEvent::from(Event::TransientError(event))),
                idle_fallback,
            )
            .await;
        }
        return restart(ctx, invocation_id, metadata, rule).await;
    }
    restart(ctx, invocation_id, metadata, rule).await
}

async fn restart<S, P>(
    ctx: &mut StateMachineApplyContext<'_, S, P>,
    invocation_id: InvocationId,
    mut metadata: InFlightInvocationMetadata,
    rule: RestartRule,
) -> Result<(), Error>
where
    S: ErStorage,
    P: ProcessorContext,
{
    record_attempt_ended(
        ctx,
        &invocation_id,
        &mut metadata,
        AttemptEnd::new(
            format!("restart (rule {})", rule as u8),
            AttemptOutcome::Restarted,
        )
        .rule(rule as u32),
        Some(AttemptCause::Restart),
    )?;
    // A restart is not a failure: the yield resets the retry counters (D46).
    YieldInvocationCommand {
        invocation_id: &invocation_id,
        invocation_target: Some(&metadata.invocation_target),
        yield_reason: YieldReason::ExplicitRecoveryRestart,
        resume_at: None,
    }
    .apply(ctx)
    .await
}

/// The attempt was shut down while waiting. Suspends, unless a notification forwarded to the
/// attempt did not reach it (D34): then restarts.
pub(super) async fn on_suspend<S, P>(
    ctx: &mut StateMachineApplyContext<'_, S, P>,
    invocation_id: InvocationId,
    mut metadata: InFlightInvocationMetadata,
    received_notifications: u32,
    idle_fallback: ErFailureOutcome,
    outstanding: Vec<OutstandingResult>,
) -> Result<(), Error>
where
    S: ErStorage,
    P: ProcessorContext,
{
    let forwarded = er_of(&mut metadata).forwarded_notifications;
    if received_notifications != forwarded {
        debug!(
            restate.invocation.id = %invocation_id,
            "Restarting instead of suspending: the attempt received {received_notifications} notifications, {forwarded} were forwarded"
        );
        return on_restart(
            ctx,
            invocation_id,
            metadata,
            RestartRule::Unprocessed,
            idle_fallback,
        )
        .await;
    }

    // Why the next attempt starts is known once something wakes the invocation.
    let mut end = AttemptEnd::new("suspend", AttemptOutcome::Suspended).rule(6);
    end.outstanding = outstanding;
    record_attempt_ended(ctx, &invocation_id, &mut metadata, end, None)?;

    let entry_id = BaseEntryId::from(invocation_id);
    if let Some(header) = ctx.storage.get_vqueue_entry_status(&entry_id).await? {
        let now = UniqueTimestamp::from_unix_millis_unchecked(ctx.record_created_at);
        let target = metadata.invocation_target.entry_target_ref();
        VQueue::get(
            header.vqueue_id(),
            ctx.storage,
            ctx.processor.vqueues_mut(),
            ctx.leader.is_some().then_some(ctx.action_collector),
        )
        .await?
        .expect("suspending in a non-existent vqueue")
        .suspend_entry(now, &header, &target);

        // A suspend is not a failure: the retry counters start over (D46). Read the stored row
        // again: the suspend changed it in this transaction.
        let header = ctx
            .storage
            .get_vqueue_entry_status(&entry_id)
            .await?
            .expect("suspended entry exists");
        if header.metadata().retry_attempts != 0
            || header.metadata().retry_count_since_last_stored_command != 0
        {
            let entry_metadata = EntryMetadata {
                retry_attempts: 0,
                retry_count_since_last_stored_command: 0,
                ..header.metadata().clone()
            };
            VQueue::get(
                header.vqueue_id(),
                ctx.storage,
                ctx.processor.vqueues_mut(),
                ctx.leader.is_some().then_some(ctx.action_collector),
            )
            .await?
            .expect("suspending in a non-existent vqueue")
            .update_entry_metadata(&header, &target, &entry_metadata);
        }
        metadata.creation_seq.get_or_insert_with(|| header.seq());
    }

    metadata.timestamps.update(ctx.record_created_at);
    ctx.storage.put_invocation_status(
        &invocation_id,
        &InvocationStatus::Suspended {
            metadata,
            // Anything that arrives wakes the invocation: it waits on nothing in particular.
            awaiting_on: UnresolvedFuture::unknown_from_iter(std::iter::empty::<NotificationId>()),
        },
    )?;
    Ok(())
}
