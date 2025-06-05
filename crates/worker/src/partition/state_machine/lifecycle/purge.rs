// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::debug_if_leader;
use crate::partition::state_machine::{Action, CommandHandler, Error, StateMachineApplyContext};
use restate_storage_api::idempotency_table::IdempotencyTable;
use restate_storage_api::invocation_status_table::{
    CompletedInvocation, InvocationStatus, InvocationStatusTable,
};
use restate_storage_api::journal_table;
use restate_storage_api::journal_table_v2::JournalTable;
use restate_storage_api::promise_table::PromiseTable;
use restate_storage_api::service_status_table::VirtualObjectStatusTable;
use restate_storage_api::state_table::StateTable;
use restate_types::identifiers::{IdempotencyId, InvocationId};
use restate_types::invocation::client::PurgeInvocationResponse;
use restate_types::invocation::{
    IngressInvocationResponseSink, InvocationEpoch, InvocationMutationResponseSink,
    InvocationTargetType, WorkflowHandlerType,
};
use restate_types::service_protocol::ServiceProtocolVersion;
use tracing::trace;

pub struct OnPurgeCommand {
    pub invocation_id: InvocationId,
    pub response_sink: Option<InvocationMutationResponseSink>,
    pub invocation_epoch: InvocationEpoch,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>> for OnPurgeCommand
where
    S: JournalTable
        + InvocationStatusTable
        + StateTable
        + journal_table::JournalTable
        + IdempotencyTable
        + VirtualObjectStatusTable
        + PromiseTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let OnPurgeCommand {
            invocation_id,
            response_sink,
            invocation_epoch,
        } = self;

        let Some(latest_epoch) = ctx
            .storage
            .get_latest_epoch_for_invocation_status(&invocation_id)
            .await?
        else {
            trace!("Received purge command for unknown invocation with id '{invocation_id}'.");
            ctx.reply_to_purge_invocation(response_sink, PurgeInvocationResponse::NotFound);
            return Ok(());
        };

        match latest_epoch.cmp(&invocation_epoch) {
            std::cmp::Ordering::Equal => {
                // Purge all epochs
                for epoch in 0..=latest_epoch {
                    Self::purge_for_epoch(ctx, invocation_id, None, epoch).await?;
                }
                ctx.reply_to_purge_invocation(response_sink, PurgeInvocationResponse::Ok);
            }
            std::cmp::Ordering::Greater => {
                // Only purge my epoch
                Self::purge_for_epoch(ctx, invocation_id, response_sink, invocation_epoch).await?;
            }
            std::cmp::Ordering::Less => {
                trace!(
                    "Received purge command for invocation id {invocation_id} with unknown epoch {invocation_epoch}."
                );
                ctx.reply_to_purge_invocation(response_sink, PurgeInvocationResponse::NotFound);
            }
        }

        Ok(())
    }
}

impl OnPurgeCommand {
    async fn purge_for_epoch<'ctx, 's: 'ctx, S>(
        ctx: &'ctx mut StateMachineApplyContext<'s, S>,
        invocation_id: InvocationId,
        response_sink: Option<InvocationMutationResponseSink>,
        invocation_epoch: InvocationEpoch,
    ) -> Result<(), Error>
    where
        S: JournalTable
            + InvocationStatusTable
            + StateTable
            + journal_table::JournalTable
            + IdempotencyTable
            + VirtualObjectStatusTable
            + PromiseTable,
    {
        match ctx
            .get_invocation_status_for_epoch(&invocation_id, invocation_epoch)
            .await?
        {
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

                debug_if_leader!(
                    ctx.is_leader,
                    restate.invocation.id = %invocation_id,
                    "Effect: Delete invocation"
                );
                ctx.storage
                    .delete_invocation_status(&invocation_id, Some(invocation_epoch))
                    .await?;

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
                    ctx.do_clear_all_state(service_id.clone()).await?;
                    ctx.do_clear_all_promises(service_id).await?;
                }

                // If journal is not empty, clean it up
                if journal_metadata.length != 0 {
                    ctx.do_drop_journal(
                        invocation_id,
                        Some(invocation_epoch),
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

impl<S> StateMachineApplyContext<'_, S> {
    fn reply_to_purge_invocation(
        &mut self,
        response_sink: Option<InvocationMutationResponseSink>,
        response: PurgeInvocationResponse,
    ) {
        if response_sink.is_none() {
            return;
        }
        let InvocationMutationResponseSink::Ingress(IngressInvocationResponseSink { request_id }) =
            response_sink.unwrap();
        debug_if_leader!(
            self.is_leader,
            "Send purge response to request id '{:?}': {:?}",
            request_id,
            response
        );

        self.action_collector
            .push(Action::ForwardPurgeInvocationResponse {
                request_id,
                response,
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::partition::state_machine::Action;
    use crate::partition::state_machine::tests::TestEnv;
    use crate::partition::state_machine::tests::fixtures::{
        invoker_end_effect, invoker_end_effect_for_epoch, invoker_entry_effect,
        invoker_entry_effect_for_epoch, pinned_deployment, pinned_deployment_for_epoch,
    };
    use crate::partition::state_machine::tests::matchers::storage::{
        has_commands, has_journal_length, is_epoch, is_variant,
    };
    use bytes::Bytes;
    use googletest::prelude::*;
    use restate_storage_api::invocation_status_table::{
        InvocationStatusDiscriminants, ReadOnlyInvocationStatusTable,
    };
    use restate_storage_api::journal_table_v2::ReadOnlyJournalTable;
    use restate_types::identifiers::PartitionProcessorRpcRequestId;
    use restate_types::invocation::{
        InvocationTarget, PurgeInvocationRequest, ServiceInvocation, restart,
    };
    use restate_types::journal_v2::{CommandType, OutputCommand, OutputResult};
    use restate_types::service_protocol::ServiceProtocolVersion;
    use restate_wal_protocol::Command;
    use std::time::Duration;

    #[restate_core::test]
    async fn purge_restarted_invocation() {
        let mut test_env = TestEnv::create().await;

        let retention = Duration::from_secs(60) * 60 * 24;
        let invocation_target = InvocationTarget::mock_service();
        let invocation_id = InvocationId::mock_generate(&invocation_target);

        // Create a fresh invocation, complete it, then restart it
        let _ = test_env
            .apply_multiple([
                Command::Invoke(ServiceInvocation {
                    invocation_id,
                    invocation_target: invocation_target.clone(),
                    completion_retention_duration: retention,
                    journal_retention_duration: retention,
                    ..ServiceInvocation::mock()
                }),
                pinned_deployment(invocation_id, ServiceProtocolVersion::V5),
                invoker_entry_effect(
                    invocation_id,
                    OutputCommand {
                        result: OutputResult::Success(Bytes::from_static(b"123")),
                        name: Default::default(),
                    },
                ),
                invoker_end_effect(invocation_id),
                Command::RestartInvocation(restart::Request {
                    invocation_id,
                    if_running: Default::default(),
                    previous_attempt_retention: Default::default(),
                    apply_to_workflow_run: Default::default(),
                    response_sink: None,
                }),
            ])
            .await;

        // Verify we have the archived status and journal
        assert_that!(
            test_env
                .storage()
                .get_invocation_status_for_epoch(&invocation_id, 0)
                .await,
            ok(all!(
                is_variant(InvocationStatusDiscriminants::Completed),
                is_epoch(0)
            ))
        );
        test_env
            .verify_journal_components_for_epoch(
                invocation_id,
                0,
                [CommandType::Input.into(), CommandType::Output.into()],
            )
            .await;

        // Now purge the epoch 0
        let purge_request_id = PartitionProcessorRpcRequestId::default();
        let actions = test_env
            .apply(Command::PurgeInvocation(PurgeInvocationRequest {
                invocation_id,
                response_sink: Some(InvocationMutationResponseSink::Ingress(
                    IngressInvocationResponseSink {
                        request_id: purge_request_id,
                    },
                )),
                invocation_epoch: 0,
            }))
            .await;

        assert_that!(
            actions,
            contains(pat!(Action::ForwardPurgeInvocationResponse {
                request_id: eq(purge_request_id),
                response: eq(PurgeInvocationResponse::Ok)
            }))
        );

        // Nothing left for epoch 0
        assert_that!(
            test_env
                .storage()
                .get_invocation_status_for_epoch(&invocation_id, 0)
                .await,
            ok(eq(InvocationStatus::Free))
        );
        assert_that!(
            test_env
                .storage()
                .get_journal_entry_for_epoch(invocation_id, 0, 0)
                .await,
            ok(none())
        );

        // Latest/epoch 1 left untouched
        assert_that!(
            test_env
                .storage()
                .get_invocation_status(&invocation_id)
                .await,
            ok(all!(
                is_variant(InvocationStatusDiscriminants::Invoked),
                has_journal_length(1),
                has_commands(1),
                is_epoch(1)
            ))
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn purge_all_epochs_when_latest_provided() {
        let mut test_env = TestEnv::create().await;

        let retention = Duration::from_secs(60) * 60 * 24;
        let invocation_target = InvocationTarget::mock_service();
        let invocation_id = InvocationId::mock_generate(&invocation_target);

        // Create a fresh invocation, complete it, then restart it, then complete it
        let _ = test_env
            .apply_multiple([
                Command::Invoke(ServiceInvocation {
                    invocation_id,
                    invocation_target: invocation_target.clone(),
                    completion_retention_duration: retention,
                    journal_retention_duration: retention,
                    ..ServiceInvocation::mock()
                }),
                pinned_deployment(invocation_id, ServiceProtocolVersion::V5),
                invoker_entry_effect(
                    invocation_id,
                    OutputCommand {
                        result: OutputResult::Success(Bytes::from_static(b"123")),
                        name: Default::default(),
                    },
                ),
                invoker_end_effect(invocation_id),
                Command::RestartInvocation(restart::Request {
                    invocation_id,
                    if_running: Default::default(),
                    previous_attempt_retention: Default::default(),
                    apply_to_workflow_run: Default::default(),
                    response_sink: None,
                }),
                pinned_deployment_for_epoch(invocation_id, 1, ServiceProtocolVersion::V5),
                invoker_entry_effect_for_epoch(
                    invocation_id,
                    1,
                    OutputCommand {
                        result: OutputResult::Success(Bytes::from_static(b"456")),
                        name: Default::default(),
                    },
                ),
                invoker_end_effect_for_epoch(invocation_id, 1),
            ])
            .await;

        // Verify we have both invocations
        assert_that!(
            test_env
                .storage()
                .get_invocation_status_for_epoch(&invocation_id, 0)
                .await,
            ok(all!(
                is_variant(InvocationStatusDiscriminants::Completed),
                is_epoch(0)
            ))
        );
        test_env
            .verify_journal_components_for_epoch(
                invocation_id,
                0,
                [CommandType::Input.into(), CommandType::Output.into()],
            )
            .await;
        assert_that!(
            test_env
                .storage()
                .get_invocation_status(&invocation_id)
                .await,
            ok(all!(
                is_variant(InvocationStatusDiscriminants::Completed),
                is_epoch(1)
            ))
        );
        test_env
            .verify_journal_components(
                invocation_id,
                [CommandType::Input.into(), CommandType::Output.into()],
            )
            .await;

        // Now purge the epoch 1
        let purge_request_id = PartitionProcessorRpcRequestId::default();
        let actions = test_env
            .apply(Command::PurgeInvocation(PurgeInvocationRequest {
                invocation_id,
                response_sink: Some(InvocationMutationResponseSink::Ingress(
                    IngressInvocationResponseSink {
                        request_id: purge_request_id,
                    },
                )),
                invocation_epoch: 1,
            }))
            .await;

        assert_that!(
            actions,
            contains(pat!(Action::ForwardPurgeInvocationResponse {
                request_id: eq(purge_request_id),
                response: eq(PurgeInvocationResponse::Ok)
            }))
        );

        // Nothing left for epoch 0 neither 1
        assert_that!(
            test_env
                .storage()
                .get_invocation_status_for_epoch(&invocation_id, 0)
                .await,
            ok(eq(InvocationStatus::Free))
        );
        assert_that!(
            test_env
                .storage()
                .get_journal_entry_for_epoch(invocation_id, 0, 0)
                .await,
            ok(none())
        );
        assert_that!(
            test_env
                .storage()
                .get_invocation_status(&invocation_id)
                .await,
            ok(eq(InvocationStatus::Free))
        );
        assert_that!(
            test_env.storage().get_journal_entry(invocation_id, 0).await,
            ok(none())
        );

        test_env.shutdown().await;
    }
}
