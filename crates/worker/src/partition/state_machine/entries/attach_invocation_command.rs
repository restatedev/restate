// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::state_machine::entries::ApplyJournalCommandEffect;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};
use restate_storage_api::fsm_table::FsmTable;
use restate_storage_api::outbox_table::{OutboxMessage, OutboxTable};
use restate_storage_api::timer_table::TimerTable;
use restate_types::invocation::{AttachInvocationRequest, ServiceInvocationResponseSink};
use restate_types::journal_v2::AttachInvocationCommand;

pub(super) type ApplyAttachInvocationCommand<'e> =
    ApplyJournalCommandEffect<'e, AttachInvocationCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyAttachInvocationCommand<'e>
where
    S: TimerTable + OutboxTable + FsmTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        ctx.handle_outgoing_message(OutboxMessage::AttachInvocation(AttachInvocationRequest {
            invocation_query: self.entry.target.into(),
            block_on_inflight: true,
            response_sink: ServiceInvocationResponseSink::partition_processor(
                self.invocation_id,
                self.entry.completion_id,
            ),
        }))
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::partition::state_machine::tests::fixtures::invoker_entry_effect;
    use crate::partition::state_machine::tests::{fixtures, matchers, TestEnv, TestEnvBuilder};
    use crate::partition::state_machine::Action;
    use bytes::Bytes;
    use googletest::prelude::{all, assert_that, contains, eq, pat};
    use googletest::{elements_are, property};
    use restate_storage_api::invocation_status_table::{
        InFlightInvocationMetadata, InvocationStatus, InvocationStatusTable,
        ReadOnlyInvocationStatusTable,
    };
    use restate_storage_api::Transaction;
    use restate_types::identifiers::{IdempotencyId, InvocationId, PartitionKey, ServiceId};
    use restate_types::invocation::{
        InvocationQuery, InvocationResponse, InvocationTarget, ResponseResult,
        ServiceInvocationResponseSink, WorkflowHandlerType,
    };
    use restate_types::journal_v2::{
        AttachInvocationCommand, AttachInvocationCompletion, AttachInvocationResult,
        AttachInvocationTarget, CommandType, Entry, EntryMetadata, EntryType,
    };
    use restate_wal_protocol::Command;
    use rstest::rstest;

    #[rstest]
    #[restate_core::test]
    async fn attach_invocation(
        #[values(
            AttachInvocationTarget::InvocationId(InvocationId::mock_random()),
            AttachInvocationTarget::IdempotentRequest(IdempotencyId::mock_random()),
            AttachInvocationTarget::Workflow(ServiceId::mock_random())
        )]
        target: AttachInvocationTarget,
    ) {
        let mut test_env = TestEnv::create().await;
        let caller_invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v4(&mut test_env, caller_invocation_id).await;

        // Let's also mock the target invocation, no need to invoke it for real
        let target_invocation_id = InvocationQuery::from(target.clone()).to_invocation_id();
        let mut tx = test_env.transaction();
        let mut in_flight_invocation_metadata = InFlightInvocationMetadata::mock();
        match target {
            AttachInvocationTarget::IdempotentRequest(ref iid) => {
                in_flight_invocation_metadata.idempotency_key = Some(iid.idempotency_key.clone());
            }
            AttachInvocationTarget::Workflow(ref wf) => {
                in_flight_invocation_metadata.invocation_target = InvocationTarget::workflow(
                    wf.service_name.clone(),
                    wf.key.clone(),
                    "MyMethod",
                    WorkflowHandlerType::Workflow,
                );
            }
            _ => {
                // We need this to make sure it passes the check of having either idempotency key or workflow id
                in_flight_invocation_metadata.idempotency_key = Some("Some random key".into());
            }
        };
        tx.put_invocation_status(
            &target_invocation_id,
            &InvocationStatus::Invoked(in_flight_invocation_metadata),
        )
        .await;
        tx.commit().await.unwrap();

        let completion_id = 1;
        let success_result = Bytes::from_static(b"success");

        let attach_invocation_command = AttachInvocationCommand {
            target: target.clone(),
            name: Default::default(),
            completion_id,
        };

        // The target invocation should contain the response sink
        let _ = test_env
            .apply(invoker_entry_effect(
                caller_invocation_id,
                attach_invocation_command.clone(),
            ))
            .await;
        assert_that!(
            test_env
                .storage()
                .get_invocation_status(&target_invocation_id)
                .await
                .unwrap(),
            pat!(InvocationStatus::Invoked(pat!(
                InFlightInvocationMetadata {
                    response_sinks: contains(eq(
                        ServiceInvocationResponseSink::PartitionProcessor {
                            caller: caller_invocation_id,
                            entry_index: 1,
                        }
                    ))
                }
            )))
        );

        // Let's now apply the completion
        let actions = test_env
            .apply(Command::InvocationResponse(InvocationResponse {
                id: caller_invocation_id,
                entry_index: completion_id,
                result: ResponseResult::Success(success_result.clone()),
            }))
            .await;

        let attach_invocation_completion = AttachInvocationCompletion {
            completion_id,
            result: AttachInvocationResult::Success(success_result),
        };
        assert_that!(
            actions,
            all![contains(matchers::actions::forward_notification(
                caller_invocation_id,
                attach_invocation_completion.clone()
            ))]
        );

        // Check journal
        assert_that!(
            test_env.read_journal_to_vec(caller_invocation_id, 3).await,
            elements_are![
                property!(Entry.ty(), eq(EntryType::Command(CommandType::Input))),
                matchers::entry_eq(attach_invocation_command),
                matchers::entry_eq(attach_invocation_completion),
            ]
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn attach_invocation_in_another_pp() {
        let target_invocation_id = InvocationId::mock_with_partition_key(PartitionKey::MAX - 1);
        let caller_invocation_id = InvocationId::mock_with_partition_key(PartitionKey::MAX - 2);

        let mut test_env = TestEnvBuilder::new()
            .with_partition_key_range(PartitionKey::MIN..=(PartitionKey::MAX - 2))
            .build()
            .await;

        // Start caller invocation
        fixtures::mock_start_invocation_with_invocation_id(&mut test_env, caller_invocation_id)
            .await;
        fixtures::mock_pinned_deployment_v4(&mut test_env, caller_invocation_id).await;

        let completion_id = 1;
        let success_result = Bytes::from_static(b"success");

        let attach_invocation_command = AttachInvocationCommand {
            target: AttachInvocationTarget::InvocationId(target_invocation_id),
            name: Default::default(),
            completion_id,
        };
        // Simulate both creating the attach invocation and the response
        let actions = test_env
            .apply_multiple([
                invoker_entry_effect(caller_invocation_id, attach_invocation_command.clone()),
                Command::InvocationResponse(InvocationResponse {
                    id: caller_invocation_id,
                    entry_index: completion_id,
                    result: ResponseResult::Success(success_result.clone()),
                }),
            ])
            .await;

        let attach_invocation_completion = AttachInvocationCompletion {
            completion_id,
            result: AttachInvocationResult::Success(success_result),
        };
        assert_that!(
            actions,
            all![
                // Because this should go on a different PP, this should generate an outbox message
                contains(pat!(Action::NewOutboxMessage {
                    message: pat!(
                        restate_storage_api::outbox_table::OutboxMessage::AttachInvocation(pat!(
                            restate_types::invocation::AttachInvocationRequest {
                                invocation_query: eq(InvocationQuery::Invocation(
                                    target_invocation_id
                                )),
                                block_on_inflight: eq(true),
                                response_sink: eq(
                                    ServiceInvocationResponseSink::partition_processor(
                                        caller_invocation_id,
                                        completion_id
                                    )
                                )
                            }
                        ))
                    )
                })),
                contains(matchers::actions::forward_notification(
                    caller_invocation_id,
                    attach_invocation_completion.clone()
                ))
            ]
        );

        // Check journal
        assert_that!(
            test_env.read_journal_to_vec(caller_invocation_id, 3).await,
            elements_are![
                property!(Entry.ty(), eq(EntryType::Command(CommandType::Input))),
                matchers::entry_eq(attach_invocation_command),
                matchers::entry_eq(attach_invocation_completion),
            ]
        );

        test_env.shutdown().await;
    }
}
