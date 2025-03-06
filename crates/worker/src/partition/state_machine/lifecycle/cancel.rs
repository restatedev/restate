// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::state_machine::entries::OnJournalEntryCommand;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};
use restate_storage_api::fsm_table::FsmTable;
use restate_storage_api::inbox_table::InboxTable;
use restate_storage_api::invocation_status_table::{InvocationStatus, InvocationStatusTable};
use restate_storage_api::journal_table;
use restate_storage_api::journal_table_v2::JournalTable;
use restate_storage_api::outbox_table::OutboxTable;
use restate_storage_api::promise_table::PromiseTable;
use restate_storage_api::state_table::StateTable;
use restate_storage_api::timer_table::TimerTable;
use restate_types::identifiers::InvocationId;
use restate_types::invocation::TerminationFlavor;
use restate_types::journal_v2::CANCEL_SIGNAL;
use tracing::{debug, trace};

pub struct OnCancelCommand {
    pub invocation_id: InvocationId,
    pub invocation_status: InvocationStatus,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnCancelCommand
where
    S: JournalTable
        + InvocationStatusTable
        + InboxTable
        + FsmTable
        + StateTable
        + JournalTable
        + OutboxTable
        + journal_table::JournalTable
        + TimerTable
        + PromiseTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        match self.invocation_status {
            is @ InvocationStatus::Invoked(_) | is @ InvocationStatus::Suspended { .. } => {
                OnJournalEntryCommand::from_entry(self.invocation_id, is, CANCEL_SIGNAL.into())
                    .apply(ctx)
                    .await?;
            }
            InvocationStatus::Inboxed(inboxed) => {
                ctx.terminate_inboxed_invocation(
                    TerminationFlavor::Cancel,
                    self.invocation_id,
                    inboxed,
                )
                .await?
            }
            InvocationStatus::Scheduled(scheduled) => {
                ctx.terminate_scheduled_invocation(
                    TerminationFlavor::Cancel,
                    self.invocation_id,
                    scheduled,
                )
                .await?
            }
            InvocationStatus::Killed(_) => {
                trace!(
                    "Received cancel command for an already killed invocation '{}'.",
                    self.invocation_id
                );
                // Nothing to do here really, let's send again the abort signal to the invoker just in case
                ctx.send_abort_invocation_to_invoker(self.invocation_id, true);
            }
            InvocationStatus::Completed(_) => {
                debug!(
                    "Received cancel command for completed invocation '{}'. To cleanup the invocation after it's been completed, use the purge invocation command.",
                    self.invocation_id
                );
            }
            InvocationStatus::Free => {
                trace!(
                    "Received cancel command for unknown invocation with id '{}'.",
                    self.invocation_id
                );
                // We still try to send the abort signal to the invoker,
                // as it might be the case that previously the user sent an abort signal
                // but some message was still between the invoker/PP queues.
                // This can happen because the invoke/resume and the abort invoker messages end up in different queues,
                // and the abort message can overtake the invoke/resume.
                // Consequently the invoker might have not received the abort and the user tried to send it again.
                ctx.send_abort_invocation_to_invoker(self.invocation_id, false);
            }
        };

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::partition::state_machine::Action;
    use crate::partition::state_machine::tests::{TestEnv, fixtures, matchers};
    use crate::partition::types::{InvokerEffect, InvokerEffectKind};
    use assert2::assert;
    use googletest::pat;
    use googletest::prelude::{assert_that, contains, eq, ge, not, some};
    use restate_storage_api::invocation_status_table::{
        InvocationStatus, ReadOnlyInvocationStatusTable,
    };
    use restate_types::deployment::PinnedDeployment;
    use restate_types::errors::CANCELED_INVOCATION_ERROR;
    use restate_types::identifiers::{DeploymentId, InvocationId, PartitionProcessorRpcRequestId};
    use restate_types::invocation::{
        InvocationTarget, InvocationTermination, NotifySignalRequest, ResponseResult,
        ServiceInvocation, ServiceInvocationResponseSink,
    };
    use restate_types::journal_v2::CANCEL_SIGNAL;
    use restate_types::net::partition_processor::IngressResponseResult;
    use restate_types::service_protocol::ServiceProtocolVersion;
    use restate_types::time::MillisSinceEpoch;
    use restate_wal_protocol::Command;

    #[restate_core::test]
    async fn cancel_invoked_invocation() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v4(&mut test_env, invocation_id).await;

        // Send signal notification
        let actions = test_env
            .apply(Command::TerminateInvocation(InvocationTermination::cancel(
                invocation_id,
            )))
            .await;
        assert_that!(
            actions,
            contains(matchers::actions::forward_notification(
                invocation_id,
                CANCEL_SIGNAL.clone()
            ))
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn cancel_invoked_invocation_through_notify_signal() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v4(&mut test_env, invocation_id).await;

        // Send signal notification
        let actions = test_env
            .apply(Command::NotifySignal(NotifySignalRequest {
                invocation_id,
                signal: CANCEL_SIGNAL.try_into().unwrap(),
            }))
            .await;
        assert_that!(
            actions,
            contains(matchers::actions::forward_notification(
                invocation_id,
                CANCEL_SIGNAL.clone()
            ))
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn cancel_invoked_invocation_without_pinned_deployment() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;

        // Send signal notification before pinning the deployment
        let actions = test_env
            .apply(Command::NotifySignal(NotifySignalRequest {
                invocation_id,
                signal: CANCEL_SIGNAL.try_into().unwrap(),
            }))
            .await;
        assert_that!(
            actions,
            not(contains(matchers::actions::forward_notification(
                invocation_id,
                CANCEL_SIGNAL.clone()
            )))
        );

        // Now pin to protocol v4, this should apply the cancel notification
        let actions = test_env
            .apply(Command::InvokerEffect(InvokerEffect {
                invocation_id,
                kind: InvokerEffectKind::PinnedDeployment(PinnedDeployment {
                    deployment_id: DeploymentId::default(),
                    service_protocol_version: ServiceProtocolVersion::V4,
                }),
            }))
            .await;
        assert_that!(
            actions,
            contains(matchers::actions::forward_notification(
                invocation_id,
                CANCEL_SIGNAL.clone()
            ))
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn cancel_scheduled_invocation_through_notify_signal() -> anyhow::Result<()> {
        let mut test_env = TestEnv::create().await;

        let invocation_id = InvocationId::mock_random();
        let rpc_id = PartitionProcessorRpcRequestId::new();

        let _ = test_env
            .apply(Command::Invoke(ServiceInvocation {
                invocation_id,
                execution_time: Some(MillisSinceEpoch::MAX),
                response_sink: Some(ServiceInvocationResponseSink::ingress(rpc_id)),
                ..ServiceInvocation::mock()
            }))
            .await;

        // assert that scheduled invocation is in invocation_status
        let current_invocation_status = test_env
            .storage()
            .get_invocation_status(&invocation_id)
            .await?;
        assert!(let InvocationStatus::Scheduled(_) = current_invocation_status);

        let actions = test_env
            .apply(Command::NotifySignal(NotifySignalRequest {
                invocation_id,
                signal: CANCEL_SIGNAL.try_into().unwrap(),
            }))
            .await;
        assert_that!(
            actions,
            contains(pat!(Action::IngressResponse {
                request_id: eq(rpc_id),
                invocation_id: some(eq(invocation_id)),
                response: eq(IngressResponseResult::Failure(CANCELED_INVOCATION_ERROR))
            }))
        );

        // assert that invocation status was removed
        let current_invocation_status = test_env
            .storage()
            .get_invocation_status(&invocation_id)
            .await?;
        assert!(let InvocationStatus::Free = current_invocation_status);

        test_env.shutdown().await;
        Ok(())
    }

    #[restate_core::test]
    async fn cancel_inboxed_invocation_through_notify_signal() -> anyhow::Result<()> {
        let mut test_env = TestEnv::create().await;

        let invocation_target = InvocationTarget::mock_virtual_object();
        let invocation_id = InvocationId::mock_generate(&invocation_target);

        let inboxed_target = invocation_target.clone();
        let inboxed_id = InvocationId::mock_generate(&inboxed_target);

        let caller_id = InvocationId::mock_random();

        let _ = test_env
            .apply(Command::Invoke(ServiceInvocation {
                invocation_id,
                invocation_target: invocation_target.clone(),
                ..ServiceInvocation::mock()
            }))
            .await;

        let _ = test_env
            .apply(Command::Invoke(ServiceInvocation {
                invocation_id: inboxed_id,
                invocation_target: inboxed_target,
                response_sink: Some(ServiceInvocationResponseSink::PartitionProcessor {
                    caller: caller_id,
                    entry_index: 0,
                }),
                ..ServiceInvocation::mock()
            }))
            .await;

        let current_invocation_status = test_env
            .storage()
            .get_invocation_status(&inboxed_id)
            .await?;

        // assert that inboxed invocation is in invocation_status
        assert!(let InvocationStatus::Inboxed(_) = current_invocation_status);

        let actions = test_env
            .apply(Command::NotifySignal(NotifySignalRequest {
                invocation_id: inboxed_id,
                signal: CANCEL_SIGNAL.try_into().unwrap(),
            }))
            .await;

        let current_invocation_status = test_env
            .storage()
            .get_invocation_status(&inboxed_id)
            .await?;

        // assert that invocation status was removed
        assert!(let InvocationStatus::Free = current_invocation_status);

        assert_that!(
            actions,
            contains(
                matchers::actions::invocation_response_to_partition_processor(
                    caller_id,
                    0,
                    eq(ResponseResult::Failure(CANCELED_INVOCATION_ERROR))
                )
            )
        );

        let outbox_message = test_env.storage().get_next_outbox_message(0).await?;

        assert_that!(
            outbox_message,
            some((
                ge(0),
                matchers::outbox::invocation_response_to_partition_processor(
                    caller_id,
                    0,
                    eq(ResponseResult::Failure(CANCELED_INVOCATION_ERROR))
                )
            ))
        );

        test_env.shutdown().await;
        Ok(())
    }
}
