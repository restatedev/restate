// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The partition processor side of the explicit recovery protocol family.
//!
//! Kept apart from the durable execution code on purpose: an invocation of this family has no
//! journal beyond its input, a status with an [`ExplicitRecoveryStatus`], and state and signals
//! in the explicit recovery table. Shared with the durable family: the outbox enqueue, and the
//! vqueue plumbing (yield, suspend, pause, wake).

mod actions;
mod attempt;
mod end_invocation;
mod on_arrival;
mod on_cancel;
mod on_kill;
mod on_manual;
mod run;

pub(super) use on_arrival::{OnResponse, OnSignal};
pub(super) use on_cancel::OnCancel;
pub(super) use on_kill::OnKill;
pub(super) use on_manual::{OnManualPause, OnManualResume};
pub(super) use run::OnRun;

use tracing::debug;

use restate_storage_api::explicit_recovery_table::{
    ReadExplicitRecoveryTable, WriteExplicitRecoveryTable,
};
use restate_storage_api::fsm_table::WriteFsmTable;
use restate_storage_api::inbox_table::{ReadInboxTable, WriteInboxTable};
use restate_storage_api::invocation_status_table::{
    InFlightInvocationMetadata, InvocationStatus, ReadInvocationStatusTable,
    WriteInvocationStatusTable,
};
use restate_storage_api::journal_events::{EventView, WriteJournalEventsTable};
use restate_storage_api::journal_table;
use restate_storage_api::journal_table_v2;
use restate_storage_api::lock_table::WriteLockTable;
use restate_storage_api::outbox_table::WriteOutboxTable;
use restate_storage_api::promise_table::{ReadPromiseTable, WritePromiseTable};
use restate_storage_api::service_status_table::{
    ReadVirtualObjectStatusTable, WriteVirtualObjectStatusTable,
};
use restate_storage_api::state_table::{ReadStateTable, WriteStateTable};
use restate_storage_api::timer_table::WriteTimerTable;
use restate_storage_api::vqueue_table::{ReadVQueueTable, WriteVQueueTable};
use restate_types::errors::InvocationError;
use restate_types::identifiers::InvocationId;
use restate_types::invocation::ResponseResult;
use restate_types::journal_events::raw::RawEvent;
use restate_types::journal_events::{
    AttemptCause, AttemptEndedEvent, AttemptOutcome, Event, EventFailure, OutstandingResult,
    WokenBy,
};
use restate_types::time::MillisSinceEpoch;
use restate_worker_api::invoker::explicit_recovery::{
    ErEffect, ErEffectKind, ErFailureOutcome, ErToInvoker,
};

use crate::partition::processor::ProcessorContext;
use crate::partition::state_machine::{Action, CommandHandler, Error, StateMachineApplyContext};

/// `next_result_id` moves in blocks, so that most actions don't rewrite the status.
pub(super) const RESULT_ID_BLOCK: u32 = 256;

/// Idle restarts in a row after which the next one counts as a failure (D63).
pub(super) const MAX_IDLE_RESTARTS: u32 = 3;

/// What ending an invocation of this family touches.
pub(crate) trait ErEndStorage:
    WriteInboxTable
    + ReadInvocationStatusTable
    + WriteInvocationStatusTable
    + WriteVirtualObjectStatusTable
    + journal_table::WriteJournalTable
    + journal_table::ReadJournalTable
    + WriteOutboxTable
    + WriteFsmTable
    + ReadStateTable
    + WriteStateTable
    + journal_table_v2::WriteJournalTable
    + journal_table_v2::ReadJournalTable
    + ReadVQueueTable
    + WriteVQueueTable
    + WriteLockTable
    + WriteJournalEventsTable
    + WriteTimerTable
    + WriteExplicitRecoveryTable
{
}

impl<T> ErEndStorage for T where
    T: WriteInboxTable
        + ReadInvocationStatusTable
        + WriteInvocationStatusTable
        + WriteVirtualObjectStatusTable
        + journal_table::WriteJournalTable
        + journal_table::ReadJournalTable
        + WriteOutboxTable
        + WriteFsmTable
        + ReadStateTable
        + WriteStateTable
        + journal_table_v2::WriteJournalTable
        + journal_table_v2::ReadJournalTable
        + ReadVQueueTable
        + WriteVQueueTable
        + WriteLockTable
        + WriteJournalEventsTable
        + WriteTimerTable
        + WriteExplicitRecoveryTable
{
}

/// Everything the handlers of this module read and write.
pub(crate) trait ErStorage:
    ErEndStorage
    + ReadExplicitRecoveryTable
    + ReadInboxTable
    + ReadVirtualObjectStatusTable
    + ReadPromiseTable
    + WritePromiseTable
{
}

impl<T> ErStorage for T where
    T: ErEndStorage
        + ReadExplicitRecoveryTable
        + ReadInboxTable
        + ReadVirtualObjectStatusTable
        + ReadPromiseTable
        + WritePromiseTable
{
}

/// Sends a message to the running attempt, through the leader's invoker.
pub(super) fn forward<S, P>(
    ctx: &mut StateMachineApplyContext<'_, S, P>,
    invocation_id: InvocationId,
    message: ErToInvoker,
) {
    if ctx.is_leader() {
        ctx.action_collector.push(Action::ForwardExplicitRecovery {
            invocation_id,
            message,
        });
    }
}

/// Writes an event of the attempt record (docs/14). One per type per log record: the journal
/// events table keys them by type, time and log position.
pub(super) fn put_event<S, P>(
    ctx: &mut StateMachineApplyContext<'_, S, P>,
    invocation_id: &InvocationId,
    metadata: &InFlightInvocationMetadata,
    event: Event,
) -> Result<(), Error>
where
    S: WriteJournalEventsTable,
{
    ctx.storage.put_journal_event(
        invocation_id,
        EventView::new(
            ctx.record_created_at,
            metadata.journal_metadata.length.saturating_sub(1),
            event,
        ),
        ctx.record_lsn.as_u64(),
    )?;
    Ok(())
}

/// The log position of the record being applied, for the events of the attempt record.
pub(super) fn lsn<S, P>(ctx: &StateMachineApplyContext<'_, S, P>) -> Option<u64> {
    Some(ctx.record_lsn.as_u64())
}

/// An event of durable execution written for an invocation of this family, with the log
/// position of its record, so that it can be ordered among the attempt record (docs/14).
pub(super) fn with_lsn<S, P>(
    ctx: &StateMachineApplyContext<'_, S, P>,
    event: RawEvent,
) -> RawEvent {
    let lsn = lsn(ctx);
    match Event::try_from(event.clone()) {
        Ok(Event::TransientError(mut e)) => {
            e.lsn = lsn;
            Event::TransientError(e).into()
        }
        Ok(Event::Paused(mut e)) => {
            e.lsn = lsn;
            Event::Paused(e).into()
        }
        _ => event,
    }
}

/// How an attempt ended, for its [`AttemptEndedEvent`].
pub(super) struct AttemptEnd {
    pub reason: String,
    pub outcome: AttemptOutcome,
    pub rule: Option<u32>,
    pub failure: Option<EventFailure>,
    pub outstanding: Vec<OutstandingResult>,
    pub retry_at: Option<u64>,
}

impl AttemptEnd {
    pub(super) fn new(reason: impl Into<String>, outcome: AttemptOutcome) -> Self {
        Self {
            reason: reason.into(),
            outcome,
            rule: None,
            failure: None,
            outstanding: vec![],
            retry_at: None,
        }
    }

    pub(super) fn rule(mut self, rule: u32) -> Self {
        self.rule = Some(rule);
        self
    }

    pub(super) fn failure(mut self, failure: Option<EventFailure>) -> Self {
        self.failure = failure;
        self
    }

    /// The end of an attempt that failed: the invocation retries, pauses or fails.
    pub(super) fn failed(
        reason: impl Into<String>,
        failure: Option<EventFailure>,
        outcome: &ErFailureOutcome,
    ) -> (Self, Option<AttemptCause>) {
        let (attempt_outcome, next) = match outcome {
            ErFailureOutcome::Retry { .. } => (AttemptOutcome::Failed, Some(AttemptCause::Retry)),
            ErFailureOutcome::Pause => (AttemptOutcome::Paused, None),
            ErFailureOutcome::Fail(_) => (AttemptOutcome::Failed, None),
        };
        let failure = failure.or_else(|| match outcome {
            ErFailureOutcome::Fail(error) => Some(event_failure(error)),
            _ => None,
        });
        let mut end = Self::new(reason, attempt_outcome).failure(failure);
        if let ErFailureOutcome::Retry {
            resume_at: Some(resume_at),
            ..
        } = outcome
        {
            end.retry_at = Some(MillisSinceEpoch::from(*resume_at).as_u64());
        }
        (end, next)
    }
}

pub(super) fn event_failure(error: &InvocationError) -> EventFailure {
    EventFailure {
        code: u32::from(u16::from(error.code())),
        message: error.message().to_owned(),
    }
}

/// Records that the current attempt ended, and why the next one will start (`next`, or set
/// when the invocation wakes up). Persists the status: callers that write it again pass the same
/// metadata on.
pub(super) fn record_attempt_ended<S, P>(
    ctx: &mut StateMachineApplyContext<'_, S, P>,
    invocation_id: &InvocationId,
    metadata: &mut InFlightInvocationMetadata,
    end: AttemptEnd,
    next: Option<AttemptCause>,
) -> Result<(), Error>
where
    S: WriteJournalEventsTable + WriteInvocationStatusTable,
{
    let er = er_of(metadata);
    er.attempt_ended = true;
    er.cause = next.map(|cause| (cause, None));
    let event = Event::AttemptEnded(AttemptEndedEvent {
        attempt: er.attempt,
        reason: end.reason,
        outcome: Some(end.outcome),
        rule: end.rule,
        failure: end.failure,
        outstanding: end.outstanding,
        lsn: lsn(ctx),
        retry_at: end.retry_at,
    });
    put_event(ctx, invocation_id, metadata, event)?;
    ctx.storage
        .put_invocation_status(invocation_id, &InvocationStatus::Invoked(metadata.clone()))?;
    Ok(())
}

/// A suspended or paused invocation is about to get a new attempt: why.
pub(super) fn set_wake_up(
    metadata: &mut InFlightInvocationMetadata,
    cause: AttemptCause,
    woken_by: Option<WokenBy>,
) {
    er_of(metadata).cause = Some((cause, woken_by));
}

/// An effect of an attempt, proposed by the invoker.
pub(super) struct OnExplicitRecoveryEffect {
    pub effect: ErEffect,
}

impl<'ctx, 's: 'ctx, S, P> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S, P>>
    for OnExplicitRecoveryEffect
where
    S: ErStorage,
    P: ProcessorContext,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S, P>) -> Result<(), Error> {
        let ErEffect {
            invocation_id,
            attempt,
            seq,
            kind,
        } = self.effect;
        let status = ctx.get_invocation_status(&invocation_id).await?;

        // Only the current attempt of an invoked invocation is applied (G4). The leader fences
        // stale attempts already; the attempt number fences what crossed a leader change.
        let metadata = match status {
            InvocationStatus::Invoked(metadata)
                if metadata
                    .explicit_recovery
                    .as_ref()
                    .is_some_and(|er| er.attempt == attempt) =>
            {
                metadata
            }
            InvocationStatus::Invoked(_) => {
                debug!(
                    restate.invocation.id = %invocation_id,
                    "Dropping explicit recovery effect of attempt {attempt}: not the current attempt"
                );
                return Ok(());
            }
            _ => {
                debug!(
                    restate.invocation.id = %invocation_id,
                    "Dropping explicit recovery effect of attempt {attempt}: the invocation is not invoked"
                );
                ctx.send_abort_invocation_to_invoker(invocation_id);
                return Ok(());
            }
        };

        match kind {
            ErEffectKind::AttemptStarted {
                deployment_id,
                protocol_version,
            } => {
                attempt::on_attempt_started(
                    ctx,
                    invocation_id,
                    metadata,
                    deployment_id,
                    protocol_version,
                )
                .await?;
                forward(ctx, invocation_id, ErToInvoker::Applied { attempt, seq });
            }
            ErEffectKind::Action {
                action,
                highest_result_id,
                record,
            } => {
                actions::on_action(
                    ctx,
                    invocation_id,
                    metadata,
                    action,
                    highest_result_id,
                    record,
                )
                .await?;
                forward(ctx, invocation_id, ErToInvoker::Applied { attempt, seq });
            }
            ErEffectKind::Record(record) => {
                actions::on_record_effect(ctx, &invocation_id, metadata, record)?;
                forward(ctx, invocation_id, ErToInvoker::Applied { attempt, seq });
            }
            ErEffectKind::End(output) => {
                let mut metadata = metadata;
                let failure = match &output {
                    ResponseResult::Success(_) => None,
                    ResponseResult::Failure(error) => Some(event_failure(error)),
                };
                record_attempt_ended(
                    ctx,
                    &invocation_id,
                    &mut metadata,
                    AttemptEnd::new("end", AttemptOutcome::Returned).failure(failure),
                    None,
                )?;
                end_invocation::end_invocation(ctx, invocation_id, metadata, None, output).await?;
            }
            ErEffectKind::Failed {
                error_event,
                outcome,
            } => {
                let mut metadata = metadata;
                let failure =
                    error_event
                        .clone()
                        .and_then(|event| match event.into_event_or_unknown() {
                            Event::TransientError(e) => Some(EventFailure {
                                code: u32::from(u16::from(e.error_code)),
                                message: e.error_message,
                            }),
                            _ => None,
                        });
                let (end, next) = AttemptEnd::failed("failed", failure, &outcome);
                record_attempt_ended(ctx, &invocation_id, &mut metadata, end, next)?;
                attempt::on_failed(ctx, invocation_id, metadata, error_event, outcome).await?;
            }
            ErEffectKind::Restart {
                rule,
                idle_fallback,
            } => {
                attempt::on_restart(ctx, invocation_id, metadata, rule, idle_fallback).await?;
            }
            ErEffectKind::Suspend {
                received_notifications,
                idle_fallback,
                outstanding,
            } => {
                attempt::on_suspend(
                    ctx,
                    invocation_id,
                    metadata,
                    received_notifications,
                    idle_fallback,
                    outstanding,
                )
                .await?;
            }
        }
        Ok(())
    }
}

/// The explicit recovery part of an in-flight status. Panics if the status is not on the family.
pub(super) fn er_of(
    metadata: &mut InFlightInvocationMetadata,
) -> &mut restate_storage_api::invocation_status_table::ExplicitRecoveryStatus {
    metadata
        .explicit_recovery
        .as_mut()
        .expect("explicit recovery invocation")
}
