// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::VerifyOrMigrateJournalTableToV2Command;

use crate::debug_if_leader;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};
use restate_storage_api::fsm_table::WriteFsmTable;
use restate_storage_api::inbox_table::WriteInboxTable;
use restate_storage_api::invocation_status_table::{
    InvocationStatus, ReadInvocationStatusTable, WriteInvocationStatusTable,
};
use restate_storage_api::journal_events::WriteJournalEventsTable;
use restate_storage_api::outbox_table::WriteOutboxTable;
use restate_storage_api::promise_table::{ReadPromiseTable, WritePromiseTable};
use restate_storage_api::service_status_table::WriteVirtualObjectStatusTable;
use restate_storage_api::state_table::{ReadStateTable, WriteStateTable};
use restate_storage_api::timer_table::WriteTimerTable;
use restate_storage_api::vqueue_table::{ReadVQueueTable, WriteVQueueTable};
use restate_storage_api::{journal_table as journal_table_v1, journal_table_v2};
use restate_types::deployment::PinnedDeployment;
use restate_types::identifiers::InvocationId;
use restate_types::service_protocol::ServiceProtocolVersion;
use tracing::trace;

pub struct OnPinnedDeploymentCommand {
    pub invocation_id: InvocationId,
    pub invocation_status: InvocationStatus,
    pub pinned_deployment: PinnedDeployment,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
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
        + WritePromiseTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
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
