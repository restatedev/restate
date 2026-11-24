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
use restate_storage_api::journal_events::WriteJournalEventsTable;
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
use restate_types::identifiers::InvocationId;
use restate_types::journal_events::raw::RawEvent;
use restate_types::journal_events::{AttemptEndedEvent, Event};
use restate_worker_api::invoker::explicit_recovery::{ErEffect, ErEffectKind, ErToInvoker};

use crate::partition::processor::ProcessorContext;
use crate::partition::state_machine::lifecycle::ApplyEventCommand;
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

/// Records in the journal events that the attempt ended, and why.
pub(super) async fn record_attempt_ended<S, P>(
    ctx: &mut StateMachineApplyContext<'_, S, P>,
    invocation_id: &InvocationId,
    status: &InvocationStatus,
    reason: impl Into<String>,
) -> Result<(), Error>
where
    S: WriteJournalEventsTable,
    P: ProcessorContext,
{
    let Some(attempt) = status.explicit_recovery().map(|er| er.attempt) else {
        return Ok(());
    };
    ApplyEventCommand {
        invocation_id,
        invocation_status: status,
        event: RawEvent::from(Event::AttemptEnded(AttemptEndedEvent {
            attempt,
            reason: reason.into(),
        })),
    }
    .apply(ctx)
    .await
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
            } => {
                actions::on_action(ctx, invocation_id, metadata, action, highest_result_id).await?;
                forward(ctx, invocation_id, ErToInvoker::Applied { attempt, seq });
            }
            ErEffectKind::End(output) => {
                record_attempt_ended(
                    ctx,
                    &invocation_id,
                    &InvocationStatus::Invoked(metadata.clone()),
                    "end",
                )
                .await?;
                end_invocation::end_invocation(ctx, invocation_id, metadata, None, output).await?;
            }
            ErEffectKind::Failed {
                error_event,
                outcome,
            } => {
                record_attempt_ended(
                    ctx,
                    &invocation_id,
                    &InvocationStatus::Invoked(metadata.clone()),
                    "failed",
                )
                .await?;
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
            } => {
                attempt::on_suspend(
                    ctx,
                    invocation_id,
                    metadata,
                    received_notifications,
                    idle_fallback,
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
