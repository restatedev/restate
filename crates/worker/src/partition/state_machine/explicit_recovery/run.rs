// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroU32;

use tracing::{debug, warn};

use restate_clock::UniqueTimestamp;
use restate_storage_api::invocation_status_table::{
    InFlightInvocationMetadata, InboxedInvocation, InvocationStatus, ReadInvocationStatusTable,
    ScheduledInvocation, WriteInvocationStatusTable,
};
use restate_storage_api::journal_events::WriteJournalEventsTable;
use restate_storage_api::journal_table;
use restate_storage_api::journal_table_v2;
use restate_storage_api::lock_table::WriteLockTable;
use restate_storage_api::vqueue_table::{
    self, EntryKey, EntryStatusHeader, ReadVQueueTable, WriteVQueueTable, stats::WaitStats,
};
use restate_tracing_instrumentation as instrumentation;
use restate_types::identifiers::InvocationId;
use restate_types::journal_events::{AttemptCause, AttemptOutcome};
use restate_types::vqueues::{EntryTargetExt, VQueueId};
use restate_util_string::ReString;
use restate_vqueues::VQueue;
use restate_worker_api::processor::{FsmAccess, PartitionFeatures};

use super::{AttemptEnd, er_of, record_attempt_ended};
use crate::partition::processor::ProcessorContext;
use crate::partition::state_machine::{Action, CommandHandler, Error, StateMachineApplyContext};

/// A scheduler decision to run an invocation of the family: starts a new attempt.
pub struct OnRun<'a, H> {
    pub qid: &'a VQueueId,
    pub entry_key: &'a EntryKey,
    pub header: &'a H,
    pub invocation_id: InvocationId,
    pub status: InvocationStatus,
    pub wait_stats: WaitStats,
}

impl<'a, 'ctx: 'a, 's: 'ctx, S, P, H> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S, P>>
    for OnRun<'a, H>
where
    S: ReadInvocationStatusTable
        + WriteInvocationStatusTable
        + journal_table::WriteJournalTable
        + journal_table_v2::WriteJournalTable
        + ReadVQueueTable
        + WriteVQueueTable
        + WriteLockTable
        + WriteJournalEventsTable,
    P: ProcessorContext,
    H: EntryStatusHeader,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S, P>) -> Result<(), Error> {
        let OnRun {
            qid,
            entry_key,
            header,
            invocation_id,
            status,
            wait_stats,
        } = self;
        let record_unique_ts = UniqueTimestamp::from_unix_millis_unchecked(ctx.record_created_at);

        let (mut metadata, run_status) = match status {
            // The first attempt: the invocation becomes in flight.
            InvocationStatus::Scheduled(ScheduledInvocation { mut metadata })
            | InvocationStatus::Inboxed(InboxedInvocation { mut metadata, .. }) => {
                metadata.creation_seq.get_or_insert_with(|| header.seq());
                let (mut in_flight, input) =
                    InFlightInvocationMetadata::from_pre_flight_invocation_metadata(
                        metadata,
                        ctx.record_created_at,
                    );
                if let Some(input) = input {
                    ctx.init_journal(&invocation_id, &mut in_flight, input)?;
                }
                if ctx.is_leader() {
                    let _start = instrumentation::create_invocation_start_span(
                        &invocation_id,
                        &in_flight.invocation_target,
                        &in_flight.journal_metadata.span_context,
                        in_flight.timestamps.creation_time(),
                    );
                }
                (in_flight, vqueue_table::Status::Started)
            }
            // A later attempt: after a restart, a retry, a wake-up or a resume.
            InvocationStatus::Invoked(metadata) => {
                let run_status = if ctx
                    .processor
                    .fsm()
                    .features()
                    .is_vq_lifecycle_refined_enabled()
                {
                    vqueue_table::Status::Started
                } else {
                    header.status()
                };
                (metadata, run_status)
            }
            other => {
                warn!(
                    restate.invocation.id = %invocation_id,
                    "Not running explicit recovery invocation in status {:?}",
                    other.discriminant()
                );
                return Ok(());
            }
        };

        let mut vqueue = VQueue::get(
            qid,
            ctx.storage,
            ctx.processor.vqueues_mut(),
            ctx.leader.is_some().then_some(ctx.action_collector),
        )
        .await?
        .expect("running in a non-existent vqueue");
        vqueue.run_entry(
            record_unique_ts,
            header,
            &metadata.invocation_target.entry_target_ref(),
            wait_stats,
            run_status,
        );
        let vq_handle = vqueue.handle();

        // An attempt whose end was never recorded was lost: a failover, a crash.
        let previous = er_of(&mut metadata);
        if previous.attempt > 0 && !previous.attempt_ended {
            record_attempt_ended(
                ctx,
                &invocation_id,
                &mut metadata,
                AttemptEnd::new("lost", AttemptOutcome::Lost),
                Some(AttemptCause::Restart),
            )?;
        }

        // A new attempt: what was counted for the previous one is discarded (D34, F14).
        let er = er_of(&mut metadata);
        er.attempt_ended = false;
        er.run_at = Some(ctx.record_created_at);
        er.attempt += 1;
        er.forwarded_notifications = 0;
        er.action_applied = false;
        let attempt = er.attempt;
        metadata.timestamps.update(ctx.record_created_at);
        // Only the leader logs: followers, and a replay after a restart, apply the same Run.
        if ctx.is_leader() {
            debug!(
                restate.invocation.id = %invocation_id,
                "Run explicit recovery attempt {attempt}"
            );
        }

        let invocation_target = metadata.invocation_target.clone();
        let idempotency_key = metadata.idempotency_key.as_deref().map(ReString::new);
        ctx.storage
            .put_invocation_status(&invocation_id, &InvocationStatus::Invoked(metadata))?;

        if ctx.is_leader() {
            ctx.action_collector.push(Action::VQInvoke {
                vq_handle,
                key: *entry_key,
                invocation_target,
                idempotency_key,
                explicit_recovery_attempt: NonZeroU32::new(attempt),
            });
        }
        Ok(())
    }
}
