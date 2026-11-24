// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Ends an invocation of the family with its output. A copy, for one case, of the durable
//! execution `end_invocation`: here the output is always given, as the family has no output
//! entry in a journal.

use restate_storage_api::invocation_status_table::{
    CompletedInvocation, InFlightInvocationMetadata, JournalRetentionPolicy,
};
use restate_storage_api::vqueue_table;
use restate_types::errors::codes;
use restate_types::identifiers::{BaseEntryId, InvocationId};
use restate_types::invocation::{ResponseResult, TerminationFlavor};
use restate_types::vqueues::EntryTargetExt;
use restate_vqueues::VQueue;

use super::ErEndStorage;
use crate::partition::processor::ProcessorContext;
use crate::partition::state_machine::{Error, StateMachineApplyContext};

pub(crate) async fn end_invocation<S, P>(
    ctx: &mut StateMachineApplyContext<'_, S, P>,
    invocation_id: InvocationId,
    invocation_metadata: InFlightInvocationMetadata,
    flavor: Option<TerminationFlavor>,
    response_result: ResponseResult,
) -> Result<(), Error>
where
    S: ErEndStorage,
    P: ProcessorContext,
{
    let invocation_target = invocation_metadata.invocation_target.clone();
    let journal_length = invocation_metadata.journal_metadata.length;
    let completion_retention = invocation_metadata.completion_retention_duration;
    let journal_retention = invocation_metadata.journal_retention_duration;
    let vqueue_id = invocation_metadata.vqueue_id.clone();
    let finished_at = ctx.vqueue_finished_at(&invocation_metadata.timestamps);

    let mut end_status = match &response_result {
        ResponseResult::Success(_) => vqueue_table::Status::Succeeded,
        ResponseResult::Failure(e) if e.code() == codes::ABORTED => vqueue_table::Status::Cancelled,
        ResponseResult::Failure(_) => vqueue_table::Status::Failed,
    };

    ctx.send_response_to_sinks(
        invocation_metadata.response_sinks.clone(),
        response_result.clone(),
        Some(invocation_id),
        None,
        Some(&invocation_metadata.invocation_target),
    )?;
    ctx.emit_invocation_end_span(
        &invocation_id,
        &invocation_metadata.invocation_target,
        &invocation_metadata.journal_metadata.span_context,
        match &response_result {
            ResponseResult::Success(_) => Ok(()),
            ResponseResult::Failure(err) => Err(err),
        },
    );

    if completion_retention.is_zero() {
        ctx.do_free_invocation(&invocation_id)?;
    } else {
        let completed_invocation = CompletedInvocation::from_in_flight_invocation_metadata(
            invocation_metadata,
            if journal_retention.is_zero() {
                JournalRetentionPolicy::Drop
            } else {
                JournalRetentionPolicy::Retain
            },
            response_result,
            ctx.record_created_at,
        );
        ctx.do_store_completed_invocation(invocation_id, completed_invocation)?;
    }

    if journal_retention.is_zero() {
        // Drops the explicit recovery table's rows too: invocation state and signals (D28). The
        // family has no sleep timers to delete.
        ctx.do_drop_journal(&invocation_id, journal_length, None, false)
            .await?;
    }

    if let Some(vqueue_id) = vqueue_id {
        let Some(entry_status) = ctx
            .storage
            .get_vqueue_entry_status(&BaseEntryId::from(invocation_id))
            .await?
        else {
            return Ok(());
        };
        end_status = match flavor {
            Some(TerminationFlavor::Cancel) => vqueue_table::Status::Cancelled,
            Some(TerminationFlavor::Kill) => vqueue_table::Status::Killed,
            None => end_status,
        };
        VQueue::get(
            &vqueue_id,
            ctx.storage,
            ctx.processor.vqueues_mut(),
            ctx.leader.is_some().then_some(ctx.action_collector),
        )
        .await?
        .expect("ending in a non-existent vqueue")
        .end(
            finished_at,
            &entry_status,
            &invocation_target.entry_target_ref(),
            end_status,
            completion_retention,
        );
    } else {
        ctx.consume_inbox(&invocation_target).await?;
    }

    Ok(())
}
