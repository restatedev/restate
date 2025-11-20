// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::state_machine::lifecycle::ResumeInvocationCommand;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};
use restate_storage_api::invocation_status_table::{
    InvocationStatus, ReadInvocationStatusTable, WriteInvocationStatusTable,
};
use restate_storage_api::vqueue_table::{ReadVQueueTable, WriteVQueueTable};
use restate_types::identifiers::{DeploymentId, InvocationId};
use restate_types::invocation::InvocationMutationResponseSink;
use restate_types::invocation::client::ResumeInvocationResponse;
use tracing::trace;

pub struct OnManualResumeCommand {
    pub invocation_id: InvocationId,
    pub update_pinned_deployment_id: Option<DeploymentId>,
    pub response_sink: Option<InvocationMutationResponseSink>,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnManualResumeCommand
where
    S: ReadInvocationStatusTable + WriteInvocationStatusTable + WriteVQueueTable + ReadVQueueTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let OnManualResumeCommand {
            invocation_id,
            update_pinned_deployment_id,
            response_sink,
        } = self;
        match ctx.get_invocation_status(&invocation_id).await? {
            InvocationStatus::Invoked(_) => {
                // The RPC command handler already dealt with it
                ctx.reply_to_resume_invocation(response_sink, ResumeInvocationResponse::Ok);
            }
            mut is @ InvocationStatus::Suspended { .. } | mut is @ InvocationStatus::Paused(_) => {
                if let Some(new_pinned_deployment_id) = update_pinned_deployment_id {
                    // Update pinned deployment only if set.
                    // This was already previously checked by the RPC handler.
                    if let Some(invocation_pinned_deploment) =
                        &mut is.get_invocation_metadata_mut().unwrap().pinned_deployment
                    {
                        trace!(
                            "Updating pinned deployment from {} to {} when resuming",
                            invocation_pinned_deploment.deployment_id, new_pinned_deployment_id
                        );
                        invocation_pinned_deploment.deployment_id = new_pinned_deployment_id;
                    }
                }

                // Resume
                ResumeInvocationCommand {
                    invocation_id,
                    invocation_status: &mut is,
                }
                .apply(ctx)
                .await?;

                // Update invocation status
                ctx.storage.put_invocation_status(&invocation_id, &is)?;

                ctx.reply_to_resume_invocation(response_sink, ResumeInvocationResponse::Ok);
            }
            InvocationStatus::Scheduled(_) | InvocationStatus::Inboxed(_) => {
                ctx.reply_to_resume_invocation(response_sink, ResumeInvocationResponse::NotStarted);
            }
            InvocationStatus::Completed(_) => {
                ctx.reply_to_resume_invocation(response_sink, ResumeInvocationResponse::Completed);
            }
            InvocationStatus::Free => {
                ctx.reply_to_resume_invocation(response_sink, ResumeInvocationResponse::NotFound);
            }
        };

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::partition::state_machine::Action;
    use crate::partition::state_machine::tests::fixtures::{
        invoker_entry_effect, invoker_suspended,
    };
    use crate::partition::state_machine::tests::{TestEnv, fixtures, matchers};
    use crate::partition::types::InvokerEffectKind;
    use googletest::prelude::{all, assert_that, contains, eq, pat};
    use restate_invoker_api::Effect;
    use restate_storage_api::invocation_status_table::{
        InvocationStatusDiscriminants, ReadInvocationStatusTable,
    };
    use restate_types::deployment::PinnedDeployment;
    use restate_types::identifiers::PartitionProcessorRpcRequestId;
    use restate_types::invocation::{IngressInvocationResponseSink, ResumeInvocationRequest};
    use restate_types::journal_v2::{NotificationId, SleepCommand};
    use restate_types::service_protocol::ServiceProtocolVersion;
    use restate_wal_protocol::Command;
    use std::time::{Duration, SystemTime};

    #[restate_core::test]
    async fn pause_then_resume() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        test_env
            .modify_invocation_status(invocation_id, |invocation_status| {
                // Mock the invocation to be paused
                *invocation_status = InvocationStatus::Paused(
                    invocation_status
                        .get_invocation_metadata_mut()
                        .unwrap()
                        .clone(),
                )
            })
            .await;

        // Now on manual resume, we should resume the suspended invocation
        let request_id = PartitionProcessorRpcRequestId::new();
        let actions = test_env
            .apply(Command::ResumeInvocation(ResumeInvocationRequest {
                invocation_id,
                update_pinned_deployment_id: None,
                response_sink: Some(InvocationMutationResponseSink::Ingress(
                    IngressInvocationResponseSink { request_id },
                )),
            }))
            .await;
        assert_that!(
            actions,
            all!(
                contains(matchers::actions::invoke_for_id(invocation_id)),
                contains(pat!(Action::ForwardResumeInvocationResponse {
                    request_id: eq(request_id),
                    response: eq(ResumeInvocationResponse::Ok)
                }))
            )
        );
        assert_that!(
            test_env
                .storage
                .get_invocation_status(&invocation_id)
                .await
                .unwrap(),
            matchers::storage::is_variant(InvocationStatusDiscriminants::Invoked)
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn pause_then_resume_update_pinned_deployment() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        let initial_deployment_id = DeploymentId::new();
        // Pin deployment
        let _ = test_env
            .apply(Command::InvokerEffect(Box::new(Effect {
                invocation_id,
                invocation_epoch: 0,
                kind: InvokerEffectKind::PinnedDeployment(PinnedDeployment {
                    deployment_id: initial_deployment_id,
                    service_protocol_version: ServiceProtocolVersion::V5,
                }),
            })))
            .await;
        // Mock paused
        test_env
            .modify_invocation_status(invocation_id, |invocation_status| {
                // Mock the invocation to be paused
                *invocation_status = InvocationStatus::Paused(
                    invocation_status
                        .get_invocation_metadata_mut()
                        .unwrap()
                        .clone(),
                )
            })
            .await;

        // Now on manual resume, we should resume the suspended invocation
        let request_id = PartitionProcessorRpcRequestId::new();
        let new_deployment_id = DeploymentId::new();
        let actions = test_env
            .apply(Command::ResumeInvocation(ResumeInvocationRequest {
                invocation_id,
                update_pinned_deployment_id: Some(new_deployment_id),
                response_sink: Some(InvocationMutationResponseSink::Ingress(
                    IngressInvocationResponseSink { request_id },
                )),
            }))
            .await;
        assert_that!(
            actions,
            all!(
                contains(matchers::actions::invoke_for_id(invocation_id)),
                contains(pat!(Action::ForwardResumeInvocationResponse {
                    request_id: eq(request_id),
                    response: eq(ResumeInvocationResponse::Ok)
                }))
            )
        );
        assert_that!(
            test_env
                .storage
                .get_invocation_status(&invocation_id)
                .await
                .unwrap(),
            all!(
                matchers::storage::is_variant(InvocationStatusDiscriminants::Invoked),
                matchers::storage::pinned_deployment_id_eq(new_deployment_id)
            )
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn sleep_then_suspend_then_manual_resume() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        // Let's suspend the invocation
        let completion_id = 1;
        let _ = test_env
            .apply_multiple([
                invoker_entry_effect(
                    invocation_id,
                    SleepCommand {
                        wake_up_time: (SystemTime::now() + Duration::from_secs(60)).into(),
                        name: Default::default(),
                        completion_id,
                    },
                ),
                invoker_suspended(
                    invocation_id,
                    [NotificationId::for_completion(completion_id)],
                ),
            ])
            .await;

        assert_that!(
            test_env
                .storage
                .get_invocation_status(&invocation_id)
                .await
                .unwrap(),
            matchers::storage::is_variant(InvocationStatusDiscriminants::Suspended)
        );

        // Now on manual resume, we should resume the suspended invocation
        let request_id = PartitionProcessorRpcRequestId::new();
        let actions = test_env
            .apply(Command::ResumeInvocation(ResumeInvocationRequest {
                invocation_id,
                update_pinned_deployment_id: None,
                response_sink: Some(InvocationMutationResponseSink::Ingress(
                    IngressInvocationResponseSink { request_id },
                )),
            }))
            .await;
        assert_that!(
            actions,
            all!(
                contains(matchers::actions::invoke_for_id(invocation_id)),
                contains(pat!(Action::ForwardResumeInvocationResponse {
                    request_id: eq(request_id),
                    response: eq(ResumeInvocationResponse::Ok)
                }))
            )
        );
        assert_that!(
            test_env
                .storage
                .get_invocation_status(&invocation_id)
                .await
                .unwrap(),
            matchers::storage::is_variant(InvocationStatusDiscriminants::Invoked)
        );

        test_env.shutdown().await;
    }
}
