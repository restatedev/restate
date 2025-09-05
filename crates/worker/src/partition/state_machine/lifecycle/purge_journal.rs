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
use restate_storage_api::invocation_status_table::{InvocationStatus, InvocationStatusTable};
use restate_storage_api::journal_events::JournalEventsTable;
use restate_storage_api::journal_table;
use restate_storage_api::journal_table_v2::JournalTable;
use restate_types::identifiers::InvocationId;
use restate_types::invocation::InvocationMutationResponseSink;
use restate_types::invocation::client::PurgeInvocationResponse;
use restate_types::service_protocol::ServiceProtocolVersion;
use tracing::trace;

pub struct OnPurgeJournalCommand {
    pub invocation_id: InvocationId,
    pub response_sink: Option<InvocationMutationResponseSink>,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnPurgeJournalCommand
where
    S: JournalTable + InvocationStatusTable + journal_table::JournalTable + JournalEventsTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let OnPurgeJournalCommand {
            invocation_id,
            response_sink,
        } = self;
        match ctx.get_invocation_status(&invocation_id).await? {
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
                        completed.journal_metadata.length,
                        should_remove_journal_table_v2,
                    )
                    .await?;
                }

                // Reset the length and commands, so journal won't be cleaned up again later
                completed.journal_metadata.length = 0;
                completed.journal_metadata.commands = 0;

                // Update invocation status
                ctx.storage
                    .put_invocation_status(&invocation_id, &InvocationStatus::Completed(completed))
                    .await?;
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

#[cfg(test)]
mod tests {
    use super::*;

    use crate::partition::state_machine::Action;
    use crate::partition::state_machine::tests::TestEnv;
    use crate::partition::state_machine::tests::fixtures::{
        invoker_end_effect, invoker_entry_effect, pinned_deployment,
    };
    use crate::partition::state_machine::tests::matchers::storage::{
        has_commands, has_journal_length, is_variant,
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
                Command::Invoke(Box::new(ServiceInvocation {
                    invocation_id,
                    invocation_target: invocation_target.clone(),
                    response_sink: Some(ServiceInvocationResponseSink::Ingress { request_id }),
                    idempotency_key: Some(idempotency_key.clone()),
                    completion_retention_duration: completion_retention,
                    journal_retention_duration: journal_retention,
                    ..ServiceInvocation::mock()
                })),
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
            }))
            .await;

        // At this point we should still be able to de-duplicate the invocation
        let request_id = PartitionProcessorRpcRequestId::default();
        let actions = test_env
            .apply(Command::Invoke(Box::new(ServiceInvocation {
                invocation_id,
                invocation_target: invocation_target.clone(),
                response_sink: Some(ServiceInvocationResponseSink::Ingress { request_id }),
                idempotency_key: Some(idempotency_key),
                ..ServiceInvocation::mock()
            })))
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
}
