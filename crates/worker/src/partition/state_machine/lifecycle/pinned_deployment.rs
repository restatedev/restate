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
use tracing::trace;

use crate::debug_if_leader;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};
use restate_storage_api::fsm_table::FsmTable;
use restate_storage_api::inbox_table::InboxTable;
use restate_storage_api::invocation_status_table::{InvocationStatus, InvocationStatusTable};
use restate_storage_api::outbox_table::OutboxTable;
use restate_storage_api::promise_table::PromiseTable;
use restate_storage_api::service_status_table::VirtualObjectStatusTable;
use restate_storage_api::state_table::StateTable;
use restate_storage_api::timer_table::TimerTable;
use restate_storage_api::{journal_table as journal_table_v1, journal_table_v2};
use restate_types::deployment::PinnedDeployment;
use restate_types::identifiers::InvocationId;
use restate_types::service_protocol::ServiceProtocolVersion;

pub struct OnPinnedDeploymentCommand {
    pub invocation_id: InvocationId,
    pub invocation_status: InvocationStatus,
    pub pinned_deployment: PinnedDeployment,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnPinnedDeploymentCommand
where
    S: journal_table_v1::JournalTable
        + journal_table_v2::JournalTable
        + InvocationStatusTable
        + OutboxTable
        + StateTable
        + FsmTable
        + InboxTable
        + VirtualObjectStatusTable
        + TimerTable
        + PromiseTable
        + OutboxTable,
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
                journal_length: in_flight_invocation_metadata.journal_metadata.length,
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
        in_flight_invocation_metadata.set_pinned_deployment(self.pinned_deployment);
        // We recreate the InvocationStatus in Invoked state as the invoker can notify the
        // chosen deployment_id only when the invocation is in-flight.
        ctx.storage
            .put_invocation_status(
                &self.invocation_id,
                &InvocationStatus::Invoked(in_flight_invocation_metadata),
            )
            .await
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
            ctx.on_cancel_invocation(self.invocation_id).await?;
        }

        Ok(())
    }
}
