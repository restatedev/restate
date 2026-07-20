// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tracing::trace;

use restate_storage_api::fsm_table::WriteFsmTable;
use restate_storage_api::inbox_table::WriteInboxTable;
use restate_storage_api::invocation_status_table::{
    InvocationStatus, ReadInvocationStatusTable, WriteInvocationStatusTable,
};
use restate_storage_api::journal_events::WriteJournalEventsTable;
use restate_storage_api::lock_table::WriteLockTable;
use restate_storage_api::outbox_table::WriteOutboxTable;
use restate_storage_api::promise_table::{ReadPromiseTable, WritePromiseTable};
use restate_storage_api::service_status_table::WriteVirtualObjectStatusTable;
use restate_storage_api::state_table::{ReadStateTable, WriteStateTable};
use restate_storage_api::timer_table::WriteTimerTable;
use restate_storage_api::vqueue_table::{EntryStatusHeader, ReadVQueueTable, WriteVQueueTable};
use restate_storage_api::{journal_table as journal_table_v1, journal_table_v2};
use restate_types::deployment::PinnedDeployment;
use restate_types::identifiers::InvocationId;
use restate_types::service_protocol::ServiceProtocolVersion;
use restate_types::sharding::WithPartitionKey;
use restate_types::vqueues::EntryId;
use restate_util_string::ToReString;
use restate_vqueues::VQueue;

use super::VerifyOrMigrateJournalTableToV2Command;
use crate::debug_if_leader;
use crate::partition::processor::ProcessorContext;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};

pub struct OnPinnedDeploymentCommand {
    pub invocation_id: InvocationId,
    pub invocation_status: InvocationStatus,
    pub pinned_deployment: PinnedDeployment,
}

impl<'ctx, 's: 'ctx, S, P> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S, P>>
    for OnPinnedDeploymentCommand
where
    S: journal_table_v1::WriteJournalTable
        + journal_table_v1::ReadJournalTable
        + journal_table_v2::WriteJournalTable
        + journal_table_v2::ReadJournalTable
        + ReadInvocationStatusTable
        + WriteInvocationStatusTable
        + WriteOutboxTable
        + ReadStateTable
        + WriteStateTable
        + WriteFsmTable
        + WriteInboxTable
        + WriteVirtualObjectStatusTable
        + WriteJournalEventsTable
        + WriteTimerTable
        + ReadPromiseTable
        + ReadVQueueTable
        + WriteVQueueTable
        + WriteLockTable
        + WritePromiseTable,
    P: ProcessorContext,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S, P>) -> Result<(), Error> {
        let mut in_flight_invocation_metadata = self
            .invocation_status
            .into_invocation_metadata()
            .expect("Must be present unless status is killed or invoked");

        // We need to migrate the table to V2, if we have to.
        if self.pinned_deployment.service_protocol_version >= ServiceProtocolVersion::V4 {
            VerifyOrMigrateJournalTableToV2Command {
                invocation_id: self.invocation_id,
                metadata: &mut in_flight_invocation_metadata,
            }
            .apply(ctx)
            .await?;
        }

        let should_apply_cancellation_hotfix =
            in_flight_invocation_metadata.hotfix_apply_cancellation_after_deployment_is_pinned;
        in_flight_invocation_metadata.hotfix_apply_cancellation_after_deployment_is_pinned = false;

        debug_if_leader!(
            ctx.is_leader,
            restate.deployment.id = %self.pinned_deployment.deployment_id,
            restate.deployment.service_protocol_version = %self.pinned_deployment.service_protocol_version.as_repr(),
            "Store chosen deployment to storage"
        );

        if let Some(ref vqueue_id) = in_flight_invocation_metadata.vqueue_id {
            let entry_id = EntryId::from(&self.invocation_id);
            let Some(header) = ctx
                .storage
                .get_vqueue_entry_status(self.invocation_id.partition_key(), &entry_id)
                .await?
            else {
                panic!(
                    "Trying to update an invocation {}, in vqueue {vqueue_id} which does not have a vqueue entry!",
                    self.invocation_id
                );
            };
            let mut metadata = header.metadata().clone();
            metadata.deployment = Some(self.pinned_deployment.deployment_id.to_restring());

            VQueue::get(
                vqueue_id,
                ctx.storage,
                ctx.processor.vqueues_mut(),
                ctx.is_leader.then_some(ctx.action_collector),
            )
            .await?
            .expect("pinning in a non-existent vqueue")
            .update_entry_metadata(&header, &metadata);
        }

        in_flight_invocation_metadata
            .set_pinned_deployment(self.pinned_deployment, ctx.record_created_at);
        // We recreate the InvocationStatus in Invoked state as the invoker can notify the
        // chosen deployment_id only when the invocation is in-flight.
        ctx.storage
            .put_invocation_status(
                &self.invocation_id,
                &InvocationStatus::Invoked(in_flight_invocation_metadata),
            )
            .map_err(Error::Storage)?;

        if should_apply_cancellation_hotfix {
            trace!(
                "Applying hotfix for cancellation when invocation doesn't have a pinned service protocol"
            );
            // TODO this is the code when we'll get rid of protocol <= 3
            // OnCancelCommand {
            //     invocation_id,
            //     invocation_status,
            // }.apply(ctx).await
            ctx.on_cancel_invocation(self.invocation_id, None).await?;
        }

        Ok(())
    }
}
