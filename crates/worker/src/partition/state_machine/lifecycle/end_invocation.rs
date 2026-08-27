// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tracing::warn;

use restate_clock::UniqueTimestamp;
use restate_storage_api::fsm_table::WriteFsmTable;
use restate_storage_api::inbox_table::WriteInboxTable;
use restate_storage_api::invocation_status_table::{
    CompletedInvocation, InFlightInvocationMetadata, JournalRetentionPolicy,
    ReadInvocationStatusTable, ResponseResultRef, WriteInvocationStatusTable,
};
use restate_storage_api::journal_events::WriteJournalEventsTable;
use restate_storage_api::journal_table::{ReadJournalTable, WriteJournalTable};
use restate_storage_api::journal_table_v2;
use restate_storage_api::lock_table::WriteLockTable;
use restate_storage_api::outbox_table::WriteOutboxTable;
use restate_storage_api::service_status_table::WriteVirtualObjectStatusTable;
use restate_storage_api::state_table::{ReadStateTable, WriteStateTable};
use restate_storage_api::vqueue_table::{self, ReadVQueueTable, WriteVQueueTable};
use restate_types::errors::{CANCELED_INVOCATION_ERROR, InvocationError, KILLED_INVOCATION_ERROR};
use restate_types::identifiers::InvocationId;
use restate_types::invocation::ResponseResult;
use restate_types::sharding::WithPartitionKey;
use restate_types::vqueues::EntryId;
use restate_vqueues::VQueue;
use restate_worker_api::processor::PartitionFeatures;

use crate::partition::processor::{FsmAccess, ProcessorContext};
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};

/// Terminal step of the invocation lifecycle: publishes the result, applies the retention
/// policies and releases whatever the invocation was holding (vqueue entry or inbox lock).
pub struct EndInvocationCommand {
    pub invocation_id: InvocationId,
    pub invocation_metadata: InFlightInvocationMetadata,
    pub reason: EndInvocationReason,
}

/// How the invocation ended.
pub enum EndInvocationReason {
    /// The invoker reported the invocation ran to completion. The result is read from the
    /// last Output entry in the journal.
    Completed,
    /// The invoker reported a terminal failure, which overrides any Output entry.
    Failed(InvocationError),
    /// The invocation was killed.
    Killed,
    /// The invocation was cancelled.
    ///
    /// Currently never constructed: cancellation travels through the invoker and arrives as
    /// [`Self::Failed`] with an `ABORTED` code, which is deduced back to
    /// [`vqueue_table::Status::Cancelled`] in [`EndInvocationCommand::apply`].
    #[allow(dead_code)]
    Cancelled,
}

impl EndInvocationReason {
    /// Result overriding any Output entry available in the journal table.
    fn into_response_result_override(self) -> Option<ResponseResult> {
        match self {
            Self::Completed => None,
            Self::Failed(e) => Some(ResponseResult::Failure(e)),
            Self::Killed => Some(ResponseResult::Failure(KILLED_INVOCATION_ERROR)),
            Self::Cancelled => Some(ResponseResult::Failure(CANCELED_INVOCATION_ERROR)),
        }
    }

    /// Terminal vqueue status forced by the reason. `None` means it has to be deduced from
    /// the response result.
    fn forced_end_status(&self) -> Option<vqueue_table::Status> {
        match self {
            Self::Completed | Self::Failed(_) => None,
            Self::Killed => Some(vqueue_table::Status::Killed),
            Self::Cancelled => Some(vqueue_table::Status::Cancelled),
        }
    }
}

impl<'ctx, 's: 'ctx, S, P> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S, P>>
    for EndInvocationCommand
where
    S: WriteInboxTable
        + ReadInvocationStatusTable
        + WriteInvocationStatusTable
        + WriteVirtualObjectStatusTable
        + WriteJournalTable
        + ReadJournalTable
        + WriteOutboxTable
        + WriteFsmTable
        + ReadStateTable
        + WriteStateTable
        + journal_table_v2::WriteJournalTable
        + journal_table_v2::ReadJournalTable
        + ReadVQueueTable
        + WriteVQueueTable
        + WriteLockTable
        + WriteJournalEventsTable,
    P: ProcessorContext,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S, P>) -> Result<(), Error> {
        let EndInvocationCommand {
            invocation_id,
            invocation_metadata,
            reason,
        } = self;

        let forced_end_status = reason.forced_end_status();
        let response_result_override = reason.into_response_result_override();

        let invocation_target = invocation_metadata.invocation_target.clone();
        let journal_length = invocation_metadata.journal_metadata.length;
        let completion_retention = invocation_metadata.completion_retention_duration;
        let journal_retention = invocation_metadata.journal_retention_duration;

        let pinned_service_protocol_version = invocation_metadata
            .pinned_deployment
            .as_ref()
            .map(|pd| pd.service_protocol_version);

        let vqueue_id = invocation_metadata.vqueue_id.clone();
        let mut end_status = vqueue_table::Status::Succeeded;
        // If there are any response sinks, or we need to store back the completed status,
        //  we need to find the latest output entry
        if !invocation_metadata.response_sinks.is_empty() || !completion_retention.is_zero() {
            //  output_index can be None if the output is overridden or because
            // read_last_output_entry detected protocol version <= V3.
            // In that case, the results is embedded in the ResponseResult and we
            // can't use Reference
            let (output_index, response_result) = if let Some(response_result) =
                response_result_override
            {
                // when the output is overridden, we have no way to reference
                // output journal, so we return None instead.

                (None, response_result)
            } else if let Some((output_index, response_result)) = ctx
                .read_last_output_entry_result(
                    &invocation_id,
                    journal_length,
                    invocation_metadata
                        .pinned_deployment
                        .as_ref()
                        .map(|pd| pd.service_protocol_version)
                        .unwrap_or_default(),
                )
                .await?
            {
                (output_index, response_result)
            } else {
                // We don't panic on this, although it indicates a bug at the moment.
                warn!("Invocation completed without an output entry. This is not supported yet.");
                return Ok(());
            };

            if let ResponseResult::Failure(e) = &response_result {
                if e.code() == restate_types::errors::codes::ABORTED {
                    // special handling for cancel/kill. Definitely not ideal, but the current
                    // design leaves me with no other options. In practice, to distinguish between
                    // cancel and kill, the reason will be used (in vqueues) to make the
                    // distinction.
                    //
                    // Kill is always known from the reason but cancel must be deduced from the
                    // aborted code.
                    end_status = vqueue_table::Status::Cancelled;
                } else {
                    end_status = vqueue_table::Status::Failed;
                }
            }

            // Send responses out
            ctx.send_response_to_sinks(
                invocation_metadata.response_sinks.clone(),
                response_result.clone(),
                Some(invocation_id),
                None,
                Some(&invocation_metadata.invocation_target),
            )?;

            // Notify invocation result
            ctx.emit_invocation_end_span(
                &invocation_id,
                &invocation_metadata.invocation_target,
                &invocation_metadata.journal_metadata.span_context,
                match &response_result {
                    ResponseResult::Success(_) => Ok(()),
                    ResponseResult::Failure(err) => Err(err),
                },
            );

            // Store the completed status, if needed
            if !completion_retention.is_zero() {
                // Only use `reference` if write-result-reference feature is enabled.
                let output_index = if ctx
                    .processor
                    .fsm()
                    .features()
                    .is_write_result_reference_enabled()
                {
                    output_index
                } else {
                    // force embed
                    None
                };

                let completed_invocation = CompletedInvocation::from_in_flight_invocation_metadata(
                    invocation_metadata,
                    if journal_retention.is_zero() {
                        JournalRetentionPolicy::Drop
                    } else {
                        JournalRetentionPolicy::Retain
                    },
                    ResponseResultRef::new(response_result, output_index),
                    ctx.record_created_at,
                );
                ctx.do_store_completed_invocation(invocation_id, completed_invocation)?;
            }
        } else {
            // Just notify Ok, no need to read the output entry
            ctx.emit_invocation_end_span(
                &invocation_id,
                &invocation_target,
                &invocation_metadata.journal_metadata.span_context,
                Ok(()),
            );
        }

        // If no retention, immediately cleanup the invocation status
        if completion_retention.is_zero() {
            ctx.do_free_invocation(&invocation_id)?;
        }

        if journal_retention.is_zero() {
            ctx.do_drop_journal(
                &invocation_id,
                journal_length,
                pinned_service_protocol_version,
            )
            .await?;
        }

        if let Some(vqueue_id) = vqueue_id {
            let Some(entry_status) = ctx
                .storage
                .get_vqueue_entry_status(
                    invocation_id.partition_key(),
                    &EntryId::from(invocation_id),
                )
                .await?
            else {
                // Invocation has been removed already!
                return Ok(());
            };
            let record_unique_ts =
                UniqueTimestamp::from_unix_millis_unchecked(ctx.record_created_at);

            // Make sure we report cancel/killed correctly.
            if let Some(forced_end_status) = forced_end_status {
                end_status = forced_end_status;
            }

            VQueue::get(
                &vqueue_id,
                ctx.storage,
                ctx.processor.vqueues_mut(),
                ctx.is_leader.then_some(ctx.action_collector),
            )
            .await?
            .expect("terminate expects vqueue to exist")
            .end(
                record_unique_ts,
                &entry_status,
                end_status,
                completion_retention,
            );
        } else {
            // Consume inbox and move on
            ctx.consume_inbox(&invocation_target).await?;
        }

        Ok(())
    }
}
