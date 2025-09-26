// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};
use restate_storage_api::idempotency_table::IdempotencyTable;
use restate_storage_api::invocation_status_table::{
    CompletedInvocation, InvocationStatus, ReadInvocationStatusTable, WriteInvocationStatusTable,
};
use restate_storage_api::journal_events::JournalEventsTable;
use restate_storage_api::journal_table;
use restate_storage_api::journal_table_v2::WriteJournalTable;
use restate_storage_api::promise_table::WritePromiseTable;
use restate_storage_api::service_status_table::WriteVirtualObjectStatusTable;
use restate_storage_api::state_table::StateTable;
use restate_types::identifiers::{IdempotencyId, InvocationId};
use restate_types::invocation::client::PurgeInvocationResponse;
use restate_types::invocation::{
    InvocationMutationResponseSink, InvocationTargetType, WorkflowHandlerType,
};
use restate_types::service_protocol::ServiceProtocolVersion;
use tracing::trace;

pub struct OnPurgeCommand {
    pub invocation_id: InvocationId,
    pub response_sink: Option<InvocationMutationResponseSink>,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>> for OnPurgeCommand
where
    S: WriteJournalTable
        + ReadInvocationStatusTable
        + WriteInvocationStatusTable
        + StateTable
        + journal_table::WriteJournalTable
        + IdempotencyTable
        + WriteVirtualObjectStatusTable
        + WritePromiseTable
        + JournalEventsTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let OnPurgeCommand {
            invocation_id,
            response_sink,
        } = self;
        match ctx.get_invocation_status(&invocation_id).await? {
            InvocationStatus::Completed(CompletedInvocation {
                invocation_target,
                idempotency_key,
                journal_metadata,
                pinned_deployment,
                ..
            }) => {
                let should_remove_journal_table_v2 =
                    pinned_deployment.as_ref().is_some_and(|pinned_deployment| {
                        pinned_deployment.service_protocol_version >= ServiceProtocolVersion::V4
                    });

                ctx.do_free_invocation(invocation_id)?;

                // Also cleanup the associated idempotency key if any
                if let Some(idempotency_key) = idempotency_key {
                    ctx.do_delete_idempotency_id(IdempotencyId::combine(
                        invocation_id,
                        &invocation_target,
                        idempotency_key,
                    ))
                    .await?;
                }

                // For workflow, we should also clean up the service lock, associated state and promises.
                if invocation_target.invocation_target_ty()
                    == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
                {
                    let service_id = invocation_target
                        .as_keyed_service_id()
                        .expect("Workflow methods must have keyed service id");

                    ctx.do_unlock_service(service_id.clone()).await?;
                    ctx.do_clear_all_state(service_id.clone(), invocation_id)
                        .await?;
                    ctx.do_clear_all_promises(service_id).await?;
                }

                // If journal is not empty, clean it up
                if journal_metadata.length != 0 {
                    ctx.do_drop_journal(
                        invocation_id,
                        journal_metadata.length,
                        should_remove_journal_table_v2,
                    )
                    .await?;
                }
                ctx.reply_to_purge_invocation(response_sink, PurgeInvocationResponse::Ok);
            }
            InvocationStatus::Free => {
                trace!("Received purge command for unknown invocation with id '{invocation_id}'.");
                ctx.reply_to_purge_invocation(response_sink, PurgeInvocationResponse::NotFound);
            }
            _ => {
                trace!(
                    "Ignoring purge command as the invocation '{invocation_id}' is still ongoing."
                );
                ctx.reply_to_purge_invocation(response_sink, PurgeInvocationResponse::NotCompleted);
            }
        };

        Ok(())
    }
}
