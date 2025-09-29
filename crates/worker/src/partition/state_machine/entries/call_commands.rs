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
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::fsm_table::FsmTable;
use restate_storage_api::invocation_status_table::InvocationStatus;
use restate_storage_api::outbox_table::{OutboxMessage, WriteOutboxTable};
use restate_types::identifiers::InvocationId;
use restate_types::invocation::{ServiceInvocation, ServiceInvocationResponseSink, Source};
use restate_types::journal_v2::command::{CallCommand, CallRequest, OneWayCallCommand};
use restate_types::journal_v2::raw::RawEntry;
use restate_types::journal_v2::{CallInvocationIdCompletion, CompletionId, Entry};
use restate_types::time::MillisSinceEpoch;
use std::collections::VecDeque;

pub(super) type ApplyCallCommand<'e> = ApplyJournalCommandEffect<'e, CallCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyJournalCommandEffect<'e, CallCommand>
where
    S: WriteOutboxTable + FsmTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        _ApplyCallCommand {
            caller_invocation_id: self.invocation_id,
            caller_invocation_status: self.invocation_status,
            request: self.entry.request,
            invocation_id_notification_idx: self.entry.invocation_id_completion_id,
            execution_time: None,
            result_notification_idx: Some(self.entry.result_completion_id),
            completions_to_process: self.completions_to_process,
        }
        .apply(ctx)
        .await
    }
}

pub(super) type ApplyOneWayCallCommand<'e> = ApplyJournalCommandEffect<'e, OneWayCallCommand>;

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyJournalCommandEffect<'e, OneWayCallCommand>
where
    S: WriteOutboxTable + FsmTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let execution_time = if self.entry.invoke_time == MillisSinceEpoch::UNIX_EPOCH {
            None
        } else {
            Some(self.entry.invoke_time)
        };
        _ApplyCallCommand {
            caller_invocation_id: self.invocation_id,
            caller_invocation_status: self.invocation_status,
            request: self.entry.request,
            invocation_id_notification_idx: self.entry.invocation_id_completion_id,
            execution_time,
            result_notification_idx: None,
            completions_to_process: self.completions_to_process,
        }
        .apply(ctx)
        .await
    }
}

struct _ApplyCallCommand<'e> {
    caller_invocation_id: InvocationId,
    caller_invocation_status: &'e InvocationStatus,
    request: CallRequest,
    invocation_id_notification_idx: CompletionId,
    execution_time: Option<MillisSinceEpoch>,
    result_notification_idx: Option<CompletionId>,
    completions_to_process: &'e mut VecDeque<RawEntry>,
}

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for _ApplyCallCommand<'e>
where
    S: WriteOutboxTable + FsmTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let caller_invocation_metadata = self
            .caller_invocation_status
            .get_invocation_metadata()
            .expect("In-Flight invocation metadata must be present");

        let CallRequest {
            invocation_id,
            invocation_target,
            span_context,
            parameter,
            headers,
            idempotency_key,
            completion_retention_duration,
            journal_retention_duration,
        } = self.request;

        // Prepare the service invocation to propose
        let service_invocation = ServiceInvocation {
            argument: parameter,
            headers,
            response_sink: self.result_notification_idx.map(|notification_idx| {
                ServiceInvocationResponseSink::partition_processor(
                    self.caller_invocation_id,
                    notification_idx,
                    caller_invocation_metadata.current_invocation_epoch,
                )
            }),
            span_context: span_context.clone(),
            execution_time: self.execution_time,
            completion_retention_duration,
            journal_retention_duration,
            idempotency_key,
            ..ServiceInvocation::initialize(
                invocation_id,
                invocation_target,
                Source::Service(
                    self.caller_invocation_id,
                    caller_invocation_metadata.invocation_target.clone(),
                ),
            )
        };

        ctx.handle_outgoing_message(OutboxMessage::ServiceInvocation(Box::new(
            service_invocation,
        )))
        .await?;

        // Notify the invocation id back
        self.completions_to_process.push_back(
            Entry::from(CallInvocationIdCompletion {
                completion_id: self.invocation_id_notification_idx,
                invocation_id,
            })
            .encode::<ServiceProtocolV4Codec>(),
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::partition::state_machine::Action;
    use crate::partition::state_machine::tests::fixtures::invoker_entry_effect;
    use crate::partition::state_machine::tests::{TestEnv, fixtures, matchers};
    use bytes::Bytes;
    use googletest::prelude::{all, assert_that, contains, eq, none, pat};
    use googletest::{elements_are, property};
    use restate_types::identifiers::{InvocationId, ServiceId};
    use restate_types::invocation::{
        Header, InvocationResponse, InvocationTarget, JournalCompletionTarget, ResponseResult,
        ServiceInvocationResponseSink,
    };
    use restate_types::journal_v2::{
        CallCommand, CallCompletion, CallInvocationIdCompletion, CallRequest, CallResult,
        CommandType, Entry, EntryMetadata, EntryType, OneWayCallCommand,
    };
    use restate_types::time::MillisSinceEpoch;
    use restate_wal_protocol::Command;
    use rstest::rstest;
    use std::time::{Duration, SystemTime};

    #[restate_core::test]
    async fn call_with_headers() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        let invocation_id_completion_id = 1;
        let result_completion_id = 2;
        let callee_service_id = ServiceId::mock_random();
        let callee_invocation_target =
            InvocationTarget::mock_from_service_id(callee_service_id.clone());
        let callee_invocation_id = InvocationId::mock_generate(&callee_invocation_target);
        let success_result = Bytes::from_static(b"success");

        let call_command = CallCommand {
            request: CallRequest {
                headers: vec![Header::new("foo", "bar")],
                ..CallRequest::mock(callee_invocation_id, callee_invocation_target.clone())
            },
            invocation_id_completion_id,
            result_completion_id,
            name: Default::default(),
        };
        let actions = test_env
            .apply_multiple([
                invoker_entry_effect(invocation_id, call_command.clone()),
                Command::InvocationResponse(InvocationResponse {
                    target: JournalCompletionTarget::from_parts(
                        invocation_id,
                        result_completion_id,
                        0,
                    ),
                    result: ResponseResult::Success(success_result.clone()),
                }),
            ])
            .await;

        let call_invocation_id_completion = CallInvocationIdCompletion {
            completion_id: invocation_id_completion_id,
            invocation_id: callee_invocation_id,
        };
        let call_completion = CallCompletion {
            completion_id: result_completion_id,
            result: CallResult::Success(success_result),
        };
        assert_that!(
            actions,
            all![
                contains(pat!(Action::NewOutboxMessage {
                    message: pat!(
                        restate_storage_api::outbox_table::OutboxMessage::ServiceInvocation(pat!(
                            restate_types::invocation::ServiceInvocation {
                                invocation_id: eq(callee_invocation_id),
                                invocation_target: eq(callee_invocation_target),
                                headers: eq(vec![Header::new("foo", "bar")]),
                                response_sink: eq(Some(
                                    ServiceInvocationResponseSink::partition_processor(
                                        invocation_id,
                                        result_completion_id,
                                        0
                                    )
                                ))
                            }
                        ))
                    )
                })),
                contains(matchers::actions::forward_notification(
                    invocation_id,
                    call_invocation_id_completion.clone()
                )),
                contains(matchers::actions::forward_notification(
                    invocation_id,
                    call_completion.clone()
                ))
            ]
        );

        // Check journal
        assert_that!(
            test_env.read_journal_to_vec(invocation_id, 4).await,
            elements_are![
                property!(Entry.ty(), eq(EntryType::Command(CommandType::Input))),
                matchers::entry_eq(call_command),
                matchers::entry_eq(call_invocation_id_completion),
                matchers::entry_eq(call_completion),
            ]
        );

        test_env.shutdown().await;
    }

    #[rstest]
    #[case(true)]
    #[case(false)]
    #[restate_core::test]
    async fn one_way_call(#[case] add_invoke_time: bool) {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        let invocation_id_completion_id = 1;
        let callee_service_id = ServiceId::mock_random();
        let callee_invocation_target =
            InvocationTarget::mock_from_service_id(callee_service_id.clone());
        let callee_invocation_id = InvocationId::mock_generate(&callee_invocation_target);
        let invoke_time = if add_invoke_time {
            MillisSinceEpoch::from(SystemTime::now() + Duration::from_secs(60))
        } else {
            MillisSinceEpoch::UNIX_EPOCH
        };

        let one_way_call_command = OneWayCallCommand {
            request: CallRequest::mock(callee_invocation_id, callee_invocation_target.clone()),
            invoke_time,
            invocation_id_completion_id,
            name: Default::default(),
        };
        let actions = test_env
            .apply_multiple([invoker_entry_effect(
                invocation_id,
                one_way_call_command.clone(),
            )])
            .await;

        let call_invocation_id_completion = CallInvocationIdCompletion {
            completion_id: invocation_id_completion_id,
            invocation_id: callee_invocation_id,
        };
        assert_that!(
            actions,
            all![
                contains(pat!(Action::NewOutboxMessage {
                    message: pat!(
                        restate_storage_api::outbox_table::OutboxMessage::ServiceInvocation(pat!(
                            restate_types::invocation::ServiceInvocation {
                                invocation_id: eq(callee_invocation_id),
                                invocation_target: eq(callee_invocation_target),
                                execution_time: eq(if add_invoke_time {
                                    Some(invoke_time)
                                } else {
                                    None
                                }),
                                response_sink: none()
                            }
                        ))
                    )
                })),
                contains(matchers::actions::forward_notification(
                    invocation_id,
                    call_invocation_id_completion.clone()
                ))
            ]
        );

        // Check journal
        assert_that!(
            test_env.read_journal_to_vec(invocation_id, 3).await,
            elements_are![
                property!(Entry.ty(), eq(EntryType::Command(CommandType::Input))),
                matchers::entry_eq(one_way_call_command),
                matchers::entry_eq(call_invocation_id_completion)
            ]
        );

        test_env.shutdown().await;
    }
}
