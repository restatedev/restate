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
use restate_types::journal_v2::GetInvocationOutputCommand;

pub(super) type ApplyGetInvocationOutputCommand<'e> =
    ApplyJournalCommandEffect<'e, GetInvocationOutputCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyGetInvocationOutputCommand<'e>
where
    S: TimerTable + OutboxTable + FsmTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        ctx.handle_outgoing_message(OutboxMessage::AttachInvocation(AttachInvocationRequest {
            invocation_query: self.entry.target.into(),
            block_on_inflight: false,
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
        CompletedInvocation, InvocationStatus, InvocationStatusTable,
    };
    use restate_storage_api::Transaction;
    use restate_types::errors::NOT_READY_INVOCATION_ERROR;
    use restate_types::identifiers::{IdempotencyId, InvocationId, PartitionKey, ServiceId};
    use restate_types::invocation::{
        GetInvocationOutputResponse, InvocationQuery, InvocationResponse, InvocationTarget,
        ResponseResult, ServiceInvocationResponseSink, WorkflowHandlerType,
    };
    use restate_types::journal_v2::{
        AttachInvocationTarget, CommandType, Entry, EntryMetadata, EntryType,
        GetInvocationOutputCommand, GetInvocationOutputCompletion, GetInvocationOutputResult,
    };
    use restate_wal_protocol::Command;
    use rstest::rstest;

    #[rstest]
    #[restate_core::test]
    async fn get_invocation_output_in_same_pp(
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
        let success_result = Bytes::from_static(b"success");
        let mut completed_invocation = CompletedInvocation::mock_neo();
        completed_invocation.response_result = ResponseResult::Success(success_result.clone());
        let mut tx = test_env.transaction();
        match target {
            AttachInvocationTarget::IdempotentRequest(ref iid) => {
                completed_invocation.idempotency_key = Some(iid.idempotency_key.clone());
            }
            AttachInvocationTarget::Workflow(ref wf) => {
                completed_invocation.invocation_target = InvocationTarget::workflow(
                    wf.service_name.clone(),
                    wf.key.clone(),
                    "MyMethod",
                    WorkflowHandlerType::Workflow,
                );
            }
            _ => {
                // We need this to make sure it passes the check of having either idempotency key or workflow id
                completed_invocation.idempotency_key = Some("Some random key".into());
            }
        };
        tx.put_invocation_status(
            &target_invocation_id,
            &InvocationStatus::Completed(completed_invocation),
        )
        .await;
        tx.commit().await.unwrap();

        let completion_id = 1;
        let expected_get_invocation_result =
            GetInvocationOutputResult::Success(success_result.clone());

        let get_invocation_output_command = GetInvocationOutputCommand {
            target: target.clone(),
            name: Default::default(),
            completion_id,
        };

        let actions = test_env
            .apply(invoker_entry_effect(
                caller_invocation_id,
                get_invocation_output_command.clone(),
            ))
            .await;

        let get_invocation_output_completion = GetInvocationOutputCompletion {
            completion_id,
            result: expected_get_invocation_result,
        };
        // Because it's on the same PP, applying the invoker effect is enough to immediately get the notification
        assert_that!(
            actions,
            contains(matchers::actions::forward_notification(
                caller_invocation_id,
                get_invocation_output_completion.clone()
            ))
        );

        // Check journal
        assert_that!(
            test_env.read_journal_to_vec(caller_invocation_id, 3).await,
            elements_are![
                property!(Entry.ty(), eq(EntryType::Command(CommandType::Input))),
                matchers::entry_eq(get_invocation_output_command),
                matchers::entry_eq(get_invocation_output_completion),
            ]
        );

        test_env.shutdown().await;
    }

    #[rstest]
    #[restate_core::test]
    async fn get_invocation_output_to_another_pp(
        #[values(true, false)] complete_using_notify_get_invocation_output: bool,
        #[values(true, false)] complete_with_not_ready: bool,
    ) {
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
        let expected_get_invocation_result = if complete_with_not_ready {
            GetInvocationOutputResult::Void
        } else {
            GetInvocationOutputResult::Success(success_result.clone())
        };

        let get_invocation_output_command = GetInvocationOutputCommand {
            target: AttachInvocationTarget::InvocationId(target_invocation_id),
            name: Default::default(),
            completion_id,
        };
        let response_command = if complete_using_notify_get_invocation_output {
            Command::NotifyGetInvocationOutputResponse(GetInvocationOutputResponse {
                caller_id: caller_invocation_id,
                completion_id,
                result: expected_get_invocation_result.clone(),
            })
        } else {
            Command::InvocationResponse(InvocationResponse {
                id: caller_invocation_id,
                entry_index: completion_id,
                result: if complete_with_not_ready {
                    ResponseResult::Failure(NOT_READY_INVOCATION_ERROR)
                } else {
                    ResponseResult::Success(success_result.clone())
                },
            })
        };

        let actions = test_env
            .apply_multiple([
                invoker_entry_effect(caller_invocation_id, get_invocation_output_command.clone()),
                response_command,
            ])
            .await;

        let get_invocation_output_completion = GetInvocationOutputCompletion {
            completion_id,
            result: expected_get_invocation_result,
        };
        assert_that!(
            actions,
            all![
                contains(pat!(Action::NewOutboxMessage {
                    message: pat!(
                        restate_storage_api::outbox_table::OutboxMessage::AttachInvocation(pat!(
                            restate_types::invocation::AttachInvocationRequest {
                                invocation_query: eq(InvocationQuery::Invocation(
                                    target_invocation_id
                                )),
                                block_on_inflight: eq(false),
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
                    get_invocation_output_completion.clone()
                ))
            ]
        );

        // Check journal
        assert_that!(
            test_env.read_journal_to_vec(caller_invocation_id, 3).await,
            elements_are![
                property!(Entry.ty(), eq(EntryType::Command(CommandType::Input))),
                matchers::entry_eq(get_invocation_output_command),
                matchers::entry_eq(get_invocation_output_completion),
            ]
        );

        test_env.shutdown().await;
    }
}
