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
use restate_storage_api::invocation_status_table::{InvocationStatus, InvocationStatusTable};
use restate_storage_api::journal_table;
use restate_storage_api::journal_table_v2::JournalTable;
use restate_types::identifiers::InvocationId;
use restate_types::invocation::client::PurgeInvocationResponse;
use restate_types::invocation::{
    IngressInvocationResponseSink, InvocationEpoch, InvocationMutationResponseSink,
};
use restate_types::service_protocol::ServiceProtocolVersion;
use tracing::trace;

pub struct OnPurgeJournalCommand {
    pub invocation_id: InvocationId,
    pub response_sink: Option<InvocationMutationResponseSink>,
    pub invocation_epoch: InvocationEpoch,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnPurgeJournalCommand
where
    S: JournalTable + InvocationStatusTable + journal_table::JournalTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let OnPurgeJournalCommand {
            invocation_id,
            response_sink,
            invocation_epoch,
        } = self;

        let Some(latest_epoch) = ctx
            .storage
            .get_latest_epoch_for_invocation_status(&invocation_id)
            .await?
        else {
            trace!(
                "Received purge journal command for unknown invocation with id '{invocation_id}'."
            );
            ctx.reply_to_purge_journal(response_sink, PurgeInvocationResponse::NotFound);
            return Ok(());
        };

        match latest_epoch.cmp(&invocation_epoch) {
            std::cmp::Ordering::Equal => {
                // Purge all epochs
                for epoch in 0..=latest_epoch {
                    Self::purge_for_epoch(ctx, invocation_id, None, epoch, epoch == latest_epoch)
                        .await?;
                }
                ctx.reply_to_purge_journal(response_sink, PurgeInvocationResponse::Ok);
            }
            std::cmp::Ordering::Greater => {
                // Only purge my epoch
                Self::purge_for_epoch(ctx, invocation_id, response_sink, invocation_epoch, false)
                    .await?;
            }
            std::cmp::Ordering::Less => {
                trace!(
                    "Received purge journal command for invocation id {invocation_id} with unknown epoch {invocation_epoch}."
                );
                ctx.reply_to_purge_journal(response_sink, PurgeInvocationResponse::NotFound);
            }
        }

        Ok(())
    }
}

impl OnPurgeJournalCommand {
    async fn purge_for_epoch<'ctx, 's: 'ctx, S>(
        ctx: &'ctx mut StateMachineApplyContext<'s, S>,
        invocation_id: InvocationId,
        response_sink: Option<InvocationMutationResponseSink>,
        invocation_epoch: InvocationEpoch,
        is_latest: bool,
    ) -> Result<(), Error>
    where
        S: JournalTable + InvocationStatusTable + journal_table::JournalTable,
    {
        match ctx
            .get_invocation_status_for_epoch(&invocation_id, invocation_epoch)
            .await?
        {
            InvocationStatus::Completed(mut completed) => {
                let should_remove_journal_table_v2 = completed
                    .pinned_deployment
                    .as_ref()
                    .is_some_and(|pinned_deployment| {
                        pinned_deployment.service_protocol_version >= ServiceProtocolVersion::V4
                    });

                // If journal is not empty, clean it up
                if completed.journal_metadata.length != 0 {
                    ctx.do_drop_journal(
                        invocation_id,
                        Some(invocation_epoch),
                        completed.journal_metadata.length,
                        should_remove_journal_table_v2,
                    )
                    .await?;
                }

                // Reset the length and commands, so journal won't be cleaned up again later
                completed.journal_metadata.length = 0;
                completed.journal_metadata.commands = 0;

                // Update invocation status
                if is_latest {
                    ctx.storage
                        .put_invocation_status(
                            &invocation_id,
                            &InvocationStatus::Completed(completed),
                        )
                        .await?;
                } else {
                    ctx.storage
                        .archive_invocation_status_to_epoch(
                            &invocation_id,
                            invocation_epoch,
                            &InvocationStatus::Completed(completed),
                        )
                        .await?;
                };
                ctx.reply_to_purge_journal(response_sink, PurgeInvocationResponse::Ok);
            }
            InvocationStatus::Free => {
                trace!(
                    "Received purge journal command for unknown invocation with id '{invocation_id}'."
                );
                ctx.reply_to_purge_journal(response_sink, PurgeInvocationResponse::NotFound);
            }
            _ => {
                trace!(
                    "Ignoring purge journal command as the invocation '{invocation_id}' is still ongoing."
                );
                ctx.reply_to_purge_journal(response_sink, PurgeInvocationResponse::NotCompleted);
            }
        };
        Ok(())
    }
}

impl<S> StateMachineApplyContext<'_, S> {
    fn reply_to_purge_journal(
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
            .push(Action::ForwardPurgeJournalResponse {
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
    use bytestring::ByteString;
    use googletest::prelude::{all, assert_that, contains, eq, none, ok, pat, some};
    use restate_storage_api::invocation_status_table::{
        InvocationStatusDiscriminants, ReadOnlyInvocationStatusTable,
    };
    use restate_storage_api::journal_table_v2::ReadOnlyJournalTable;
    use restate_types::identifiers::PartitionProcessorRpcRequestId;
    use restate_types::invocation::client::InvocationOutputResponse;
    use restate_types::invocation::{
        InvocationTarget, PurgeInvocationRequest, ServiceInvocation, ServiceInvocationResponseSink,
        restart,
    };
    use restate_types::journal_v2::{CommandType, OutputCommand, OutputResult};
    use restate_wal_protocol::Command;
    use std::time::Duration;

    #[restate_core::test]
    async fn purge_journal_then_invocation() {
        let mut test_env = TestEnv::create().await;

        let idempotency_key = ByteString::from_static("my-idempotency-key");
        let completion_retention = Duration::from_secs(60) * 60 * 24;
        let journal_retention = Duration::from_secs(60) * 60 * 2;
        let invocation_target = InvocationTarget::mock_virtual_object();
        let invocation_id = InvocationId::generate(&invocation_target, Some(&idempotency_key));
        let request_id = PartitionProcessorRpcRequestId::default();
        let response_bytes = Bytes::from_static(b"123");

        // Create and complete a fresh invocation
        let actions = test_env
            .apply_multiple([
                Command::Invoke(ServiceInvocation {
                    invocation_id,
                    invocation_target: invocation_target.clone(),
                    response_sink: Some(ServiceInvocationResponseSink::Ingress { request_id }),
                    idempotency_key: Some(idempotency_key.clone()),
                    completion_retention_duration: completion_retention,
                    journal_retention_duration: journal_retention,
                    ..ServiceInvocation::mock()
                }),
                pinned_deployment(invocation_id, ServiceProtocolVersion::V5),
                invoker_entry_effect(
                    invocation_id,
                    OutputCommand {
                        result: OutputResult::Success(response_bytes.clone()),
                        name: Default::default(),
                    },
                ),
                invoker_end_effect(invocation_id),
            ])
            .await;

        // Assert response
        assert_that!(
            actions,
            contains(pat!(Action::IngressResponse {
                request_id: eq(request_id),
                invocation_id: some(eq(invocation_id)),
                response: eq(InvocationOutputResponse::Success(
                    invocation_target.clone(),
                    response_bytes.clone()
                ))
            }))
        );

        // InvocationStatus contains completed
        assert_that!(
            test_env
                .storage()
                .get_invocation_status(&invocation_id)
                .await,
            ok(all!(
                is_variant(InvocationStatusDiscriminants::Completed),
                has_commands(2),
                has_journal_length(2)
            ))
        );

        // We also retain the journal here
        test_env
            .verify_journal_components(
                invocation_id,
                [CommandType::Input.into(), CommandType::Output.into()],
            )
            .await;

        // Now let's purge the journal
        test_env
            .apply(Command::PurgeJournal(PurgeInvocationRequest {
                invocation_id,
                response_sink: None,
                invocation_epoch: 0,
            }))
            .await;

        // At this point we should still be able to de-duplicate the invocation
        let request_id = PartitionProcessorRpcRequestId::default();
        let actions = test_env
            .apply(Command::Invoke(ServiceInvocation {
                invocation_id,
                invocation_target: invocation_target.clone(),
                response_sink: Some(ServiceInvocationResponseSink::Ingress { request_id }),
                idempotency_key: Some(idempotency_key),
                ..ServiceInvocation::mock()
            }))
            .await;
        assert_that!(
            actions,
            contains(pat!(Action::IngressResponse {
                request_id: eq(request_id),
                invocation_id: some(eq(invocation_id)),
                response: eq(InvocationOutputResponse::Success(
                    invocation_target.clone(),
                    response_bytes.clone()
                ))
            }))
        );

        // And InvocationStatus still contains completed, but without commands and length
        assert_that!(
            test_env
                .storage()
                .get_invocation_status(&invocation_id)
                .await,
            ok(all!(
                is_variant(InvocationStatusDiscriminants::Completed),
                has_commands(0),
                has_journal_length(0)
            ))
        );

        // Now purge completely
        test_env
            .apply(Command::PurgeInvocation(PurgeInvocationRequest {
                invocation_id,
                response_sink: None,
                invocation_epoch: 0,
            }))
            .await;

        // Nothing should be left
        assert_that!(
            test_env
                .storage()
                .get_invocation_status(&invocation_id)
                .await
                .unwrap(),
            pat!(InvocationStatus::Free)
        );
        assert_that!(
            test_env.storage().get_journal_entry(invocation_id, 0).await,
            ok(none())
        );

        test_env.shutdown().await;
    }

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

        // Now purge the journal epoch 0
        let purge_request_id = PartitionProcessorRpcRequestId::default();
        let actions = test_env
            .apply(Command::PurgeJournal(PurgeInvocationRequest {
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
            contains(pat!(Action::ForwardPurgeJournalResponse {
                request_id: eq(purge_request_id),
                response: eq(PurgeInvocationResponse::Ok)
            }))
        );

        // Still got status, but no journal anymore
        assert_that!(
            test_env
                .storage()
                .get_invocation_status_for_epoch(&invocation_id, 0)
                .await,
            ok(all!(
                is_variant(InvocationStatusDiscriminants::Completed),
                has_journal_length(0),
                has_commands(0),
                is_epoch(0)
            ))
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
            .apply(Command::PurgeJournal(PurgeInvocationRequest {
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
            contains(pat!(Action::ForwardPurgeJournalResponse {
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
            ok(all!(
                is_variant(InvocationStatusDiscriminants::Completed),
                has_journal_length(0),
                has_commands(0),
                is_epoch(0)
            ))
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
            ok(all!(
                is_variant(InvocationStatusDiscriminants::Completed),
                has_journal_length(0),
                has_commands(0),
                is_epoch(1)
            ))
        );
        assert_that!(
            test_env.storage().get_journal_entry(invocation_id, 0).await,
            ok(none())
        );

        test_env.shutdown().await;
    }
}
