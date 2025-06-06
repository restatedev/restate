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
use crate::partition::state_machine::entries::write_entry::WriteJournalEntryCommand;
use crate::partition::state_machine::lifecycle::ArchiveInvocationCommand;
use crate::partition::state_machine::{
    Action, CommandHandler, Error, StateMachineApplyContext, should_use_journal_table_v2,
};
use restate_invoker_api::InvokeInputJournal;
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::fsm_table::FsmTable;
use restate_storage_api::invocation_status_table::{
    CompletedInvocation, InFlightInvocationMetadata, InvocationStatus, InvocationStatusTable,
    JournalRetentionPolicy, StatusTimestamps,
};
use restate_storage_api::journal_table as journal_table_v1;
use restate_storage_api::journal_table_v2::{JournalTable, ReadOnlyJournalTable};
use restate_storage_api::outbox_table::OutboxTable;
use restate_storage_api::promise_table::PromiseTable;
use restate_storage_api::state_table::StateTable;
use restate_types::errors::RESTARTED_INVOCATION_ERROR;
use restate_types::identifiers::InvocationId;
use restate_types::invocation::client::RestartInvocationResponse;
use restate_types::invocation::restart::{ApplyToWorkflowRun, IfRunning};
use restate_types::invocation::{
    IngressInvocationResponseSink, InvocationEpoch, InvocationMutationResponseSink,
    InvocationTargetType, ResponseResult, WorkflowHandlerType,
};
use restate_types::journal_v2;
use restate_types::journal_v2::{
    CommandType, Entry, EntryIndex, EntryMetadata, OutputCommand, OutputResult,
};
use std::time::Duration;
use tracing::trace;

pub struct OnRestartInvocationCommand {
    pub invocation_id: InvocationId,
    pub if_running: IfRunning,
    pub previous_attempt_retention: Option<Duration>,
    pub apply_to_workflow_run: ApplyToWorkflowRun,
    pub response_sink: Option<InvocationMutationResponseSink>,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnRestartInvocationCommand
where
    S: JournalTable
        + InvocationStatusTable
        + StateTable
        + PromiseTable
        + OutboxTable
        + FsmTable
        + journal_table_v1::ReadOnlyJournalTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let OnRestartInvocationCommand {
            invocation_id,
            if_running,
            previous_attempt_retention,
            apply_to_workflow_run,
            response_sink,
        } = self;

        let invocation_status = ctx.get_invocation_status(&invocation_id).await?;
        if invocation_status == InvocationStatus::Free {
            trace!("Received restart command for unknown invocation with id '{invocation_id}'.");
            ctx.reply_to_restart_invocation(response_sink, RestartInvocationResponse::NotFound);
            return Ok(());
        }

        if !should_use_journal_table_v2(&invocation_status) {
            trace!(
                "Received restart command for invocation using the old journal table, this is unsupported."
            );
            ctx.reply_to_restart_invocation(response_sink, RestartInvocationResponse::Unsupported);
            return Ok(());
        }

        let (mut invocation, input_entry_index, original_journal_length) = match invocation_status {
            InvocationStatus::Completed(completed_invocation) => {
                let Some(input_entry_index) = ctx
                    .find_input_entry(invocation_id, completed_invocation.journal_metadata.length)
                    .await?
                else {
                    trace!(
                        "Received restart command for a completed invocation, for which the journal/input entry was not retained."
                    );
                    ctx.reply_to_restart_invocation(
                        response_sink,
                        RestartInvocationResponse::MissingInput,
                    );
                    return Ok(());
                };

                let original_journal_length = completed_invocation.journal_metadata.length;
                (
                    completed_invocation,
                    input_entry_index,
                    original_journal_length,
                )
            }
            InvocationStatus::Invoked(mut metadata)
            | InvocationStatus::Suspended { mut metadata, .. } => {
                if if_running == IfRunning::Fail {
                    trace!(
                        "Received restart command for invocation that is not in completed state.",
                    );
                    ctx.reply_to_restart_invocation(
                        response_sink,
                        RestartInvocationResponse::StillRunning,
                    );
                    return Ok(());
                }

                let Some(input_entry_index) = ctx
                    .find_input_entry(invocation_id, metadata.journal_metadata.length)
                    .await?
                else {
                    trace!(
                        "Received restart command for a completed invocation, for which the journal/input entry was not retained."
                    );
                    ctx.reply_to_restart_invocation(
                        response_sink,
                        RestartInvocationResponse::MissingInput,
                    );
                    return Ok(());
                };

                // --- Kill the running invocation.

                // Kill children first
                ctx.kill_child_invocations(
                    &invocation_id,
                    metadata.journal_metadata.length,
                    &metadata,
                )
                .await?;

                // Send abort invocation to invoker
                ctx.do_send_abort_invocation_to_invoker(invocation_id, InvocationEpoch::MAX);

                // Write output entry
                let response_result = ResponseResult::Failure(RESTARTED_INVOCATION_ERROR);
                WriteJournalEntryCommand {
                    invocation_id: self.invocation_id,
                    journal_metadata: &mut metadata.journal_metadata,
                    invocation_epoch: metadata.current_invocation_epoch,
                    entry: Entry::from(OutputCommand {
                        result: OutputResult::Failure(RESTARTED_INVOCATION_ERROR.into()),
                        name: Default::default(),
                    })
                    .encode::<ServiceProtocolV4Codec>(),
                    related_completion_ids: Default::default(),
                }
                .apply(ctx)
                .await?;

                // Send responses out
                ctx.send_response_to_sinks(
                    metadata.response_sinks.clone(),
                    response_result.clone(),
                    Some(invocation_id),
                    None,
                    Some(&metadata.invocation_target),
                )
                .await?;

                // Notify invocation result
                ctx.notify_invocation_result(
                    invocation_id,
                    metadata.invocation_target.clone(),
                    metadata.journal_metadata.span_context.clone(),
                    // SAFETY: We use this field to send back the notification to ingress, and not as part of the PP deterministic logic.
                    unsafe { metadata.timestamps.creation_time() },
                    Err((
                        RESTARTED_INVOCATION_ERROR.code(),
                        RESTARTED_INVOCATION_ERROR.message().to_string(),
                    )),
                );

                // Prepare the completed status
                let original_journal_length = metadata.journal_metadata.length;
                let journal_retention_policy = if metadata.journal_retention_duration.is_zero() {
                    JournalRetentionPolicy::Drop
                } else {
                    JournalRetentionPolicy::Retain
                };
                let completed_invocation = CompletedInvocation::from_in_flight_invocation_metadata(
                    metadata,
                    journal_retention_policy,
                    response_result,
                );

                (
                    completed_invocation,
                    input_entry_index,
                    original_journal_length,
                )
            }
            is @ InvocationStatus::Scheduled(_) | is @ InvocationStatus::Inboxed(_) => {
                trace!(
                    "Received restart command for invocation that didn't start yet, current status {:?}. The command will be ignored.",
                    is.discriminant().unwrap()
                );
                ctx.reply_to_restart_invocation(
                    response_sink,
                    RestartInvocationResponse::NotStarted,
                );
                return Ok(());
            }
            InvocationStatus::Free => unreachable!(),
        };

        let new_epoch = invocation.invocation_epoch + 1;

        // If it's workflow run, we need some special logic
        if invocation.invocation_target.invocation_target_ty()
            == InvocationTargetType::Workflow(WorkflowHandlerType::Workflow)
        {
            let workflow_id = invocation.invocation_target.as_keyed_service_id().unwrap();
            match apply_to_workflow_run {
                ApplyToWorkflowRun::Nothing => {}
                ApplyToWorkflowRun::ClearOnlyPromises => {
                    ctx.do_clear_all_promises(workflow_id).await?;
                }
                ApplyToWorkflowRun::ClearOnlyState => {
                    ctx.do_clear_all_state(workflow_id).await?;
                }
                ApplyToWorkflowRun::ClearAllPromisesAndState => {
                    ctx.do_clear_all_promises(workflow_id.clone()).await?;
                    ctx.do_clear_all_state(workflow_id).await?;
                }
            };
        }

        // Archive the previous invocation
        ArchiveInvocationCommand {
            invocation_id,
            completed_invocation: invocation.clone(),
            previous_attempt_retention_override: previous_attempt_retention,
        }
        .apply(ctx)
        .await?;

        // Scan the journal to see where the input entry is: should always be first entry, but with journal v2 in future we might have entries interleaving.
        // Drop the suffix of the journal after the input entry.
        let delete_journal_from = input_entry_index + 1;
        let delete_journal_to = original_journal_length;
        debug_if_leader!(
            ctx.is_leader,
            "Deleting journal range {delete_journal_from} to {delete_journal_to}",
        );
        let notifications: Vec<_> = ctx
            .storage
            .get_notifications_index(invocation_id)
            .await?
            .into_keys()
            .collect();
        ctx.storage
            .delete_journal_range(
                invocation_id,
                delete_journal_from,
                delete_journal_to,
                &notifications,
            )
            .await?;
        ctx.storage
            .update_current_journal_epoch(&invocation_id, new_epoch, delete_journal_from)
            .await?;

        // --- Let's prepare the InFlightInvocationMetadata

        // Reset length and commands
        invocation.journal_metadata.length = input_entry_index + 1;
        invocation.journal_metadata.commands = 1 /* Only the input entry */;

        invocation
            .completion_range_epoch_map
            .add_trim_point(0, new_epoch);

        let in_flight_invocation_metadata = InFlightInvocationMetadata {
            invocation_target: invocation.invocation_target,
            source: invocation.source,
            execution_time: invocation.execution_time,
            idempotency_key: invocation.idempotency_key,
            current_invocation_epoch: new_epoch,
            completion_retention_duration: invocation.completion_retention_duration,
            journal_retention_duration: invocation.journal_retention_duration,
            completion_range_epoch_map: invocation.completion_range_epoch_map,
            journal_metadata: invocation.journal_metadata,
            hotfix_apply_cancellation_after_deployment_is_pinned: false,
            created_using_restate_version: invocation.created_using_restate_version,

            // Reset the pinned deployment
            pinned_deployment: None,
            // Reset timestamps
            timestamps: StatusTimestamps::init(),
            // Reset response sinks
            response_sinks: Default::default(),
        };

        // Finally, it's time to invoke again!
        ctx.invoke(
            invocation_id,
            in_flight_invocation_metadata,
            InvokeInputJournal::NoCachedJournal,
        )
        .await?;

        // Reply to the listener, restart went well
        ctx.reply_to_restart_invocation(response_sink, RestartInvocationResponse::Ok { new_epoch });

        Ok(())
    }
}

impl<S> StateMachineApplyContext<'_, S> {
    async fn find_input_entry(
        &mut self,
        invocation_id: InvocationId,
        length: EntryIndex,
    ) -> Result<Option<EntryIndex>, Error>
    where
        S: ReadOnlyJournalTable,
    {
        // Find input entry
        for i in 0..length {
            let Some(entry) = self.storage.get_journal_entry(invocation_id, i).await? else {
                return Ok(None);
            };
            if entry.ty() == journal_v2::EntryType::Command(CommandType::Input) {
                return Ok(Some(i));
            }
        }
        Ok(None)
    }

    fn reply_to_restart_invocation(
        &mut self,
        response_sink: Option<InvocationMutationResponseSink>,
        response: RestartInvocationResponse,
    ) {
        if response_sink.is_none() {
            return;
        }
        let InvocationMutationResponseSink::Ingress(IngressInvocationResponseSink { request_id }) =
            response_sink.unwrap();
        debug_if_leader!(
            self.is_leader,
            "Send restart response to request id '{:?}': {:?}",
            request_id,
            response
        );

        self.action_collector
            .push(Action::ForwardRestartInvocationResponse {
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
    use futures::TryStreamExt;
    use googletest::prelude::*;
    use restate_storage_api::invocation_status_table::{
        InvocationStatusDiscriminants, ReadOnlyInvocationStatusTable,
    };
    use restate_storage_api::promise_table::{
        Promise, PromiseResult, PromiseState, ReadOnlyPromiseTable,
    };
    use restate_storage_api::state_table::ReadOnlyStateTable;
    use restate_types::identifiers::PartitionProcessorRpcRequestId;
    use restate_types::invocation::client::InvocationOutputResponse;
    use restate_types::invocation::{
        InvocationTarget, ServiceInvocation, ServiceInvocationResponseSink, restart,
    };
    use restate_types::journal_v2::{
        CommandType, CompletePromiseCommand, CompletePromiseValue, CompletionType, OutputCommand,
        OutputResult, SetStateCommand, SleepCommand,
    };
    use restate_types::service_protocol::ServiceProtocolVersion;
    use restate_types::time::MillisSinceEpoch;
    use restate_wal_protocol::Command;
    use std::time::Duration;

    #[restate_core::test]
    async fn restart_completed_invocation() {
        let mut test_env = TestEnv::create().await;

        let retention = Duration::from_secs(60) * 60 * 24;
        let invocation_target = InvocationTarget::mock_service();
        let invocation_id = InvocationId::mock_generate(&invocation_target);
        let request_id = PartitionProcessorRpcRequestId::default();

        // Create and complete a fresh invocation
        let _ = test_env
            .apply_multiple([
                Command::Invoke(ServiceInvocation {
                    invocation_id,
                    invocation_target: invocation_target.clone(),
                    response_sink: Some(ServiceInvocationResponseSink::Ingress { request_id }),
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
            ])
            .await;

        // InvocationStatus contains completed
        assert_that!(
            test_env
                .storage()
                .get_invocation_status(&invocation_id)
                .await,
            ok(all!(
                is_variant(InvocationStatusDiscriminants::Completed),
                is_epoch(0)
            ))
        );

        // We also retain the journal here
        test_env
            .verify_journal_components(
                invocation_id,
                [CommandType::Input.into(), CommandType::Output.into()],
            )
            .await;

        // Now let's restart the invocation
        let restart_request_id = PartitionProcessorRpcRequestId::default();
        let actions = test_env
            .apply(Command::RestartInvocation(restart::Request {
                invocation_id,
                if_running: Default::default(),
                previous_attempt_retention: Default::default(),
                apply_to_workflow_run: Default::default(),
                response_sink: Some(InvocationMutationResponseSink::Ingress(
                    IngressInvocationResponseSink {
                        request_id: restart_request_id,
                    },
                )),
            }))
            .await;

        // Assert restart response
        assert_that!(
            actions,
            all!(
                contains(pat!(Action::ForwardRestartInvocationResponse {
                    request_id: eq(restart_request_id),
                    response: eq(RestartInvocationResponse::Ok { new_epoch: 1 })
                })),
                contains(pat!(Action::Invoke {
                    invocation_id: eq(invocation_id),
                }))
            )
        );

        // Verify the invocation is now in-flight
        assert_that!(
            test_env
                .storage()
                .get_invocation_status(&invocation_id)
                .await,
            ok(all!(
                is_variant(InvocationStatusDiscriminants::Invoked),
                is_epoch(1)
            ))
        );

        // Verify the journal contains only the input entry
        test_env
            .verify_journal_components(invocation_id, [CommandType::Input.into()])
            .await;

        // Verify we have the archived status and journal as well
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

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn restart_running_invocation() {
        let mut test_env = TestEnv::create().await;

        let retention = Duration::from_secs(60) * 60 * 24;
        let invocation_target = InvocationTarget::mock_service();
        let invocation_id = InvocationId::mock_generate(&invocation_target);
        let request_id = PartitionProcessorRpcRequestId::default();

        // Create an invocation but don't complete it
        let _ = test_env
            .apply_multiple([
                Command::Invoke(ServiceInvocation {
                    invocation_id,
                    invocation_target: invocation_target.clone(),
                    response_sink: Some(ServiceInvocationResponseSink::Ingress { request_id }),
                    completion_retention_duration: retention,
                    journal_retention_duration: retention,
                    ..ServiceInvocation::mock()
                }),
                pinned_deployment(invocation_id, ServiceProtocolVersion::V5),
                // Just add some entry here
                invoker_entry_effect(
                    invocation_id,
                    SleepCommand {
                        wake_up_time: MillisSinceEpoch::now(),
                        completion_id: 1,
                        name: Default::default(),
                    },
                ),
            ])
            .await;

        // InvocationStatus should be Invoked
        assert_that!(
            test_env
                .storage()
                .get_invocation_status(&invocation_id)
                .await,
            ok(all!(
                is_variant(InvocationStatusDiscriminants::Invoked),
                has_journal_length(2),
                has_commands(2),
                is_epoch(0)
            ))
        );

        // Now let's restart the invocation
        let restart_request_id = PartitionProcessorRpcRequestId::default();
        let actions = test_env
            .apply(Command::RestartInvocation(restart::Request {
                invocation_id,
                if_running: IfRunning::Kill,
                previous_attempt_retention: None,
                apply_to_workflow_run: ApplyToWorkflowRun::Nothing,
                response_sink: Some(InvocationMutationResponseSink::Ingress(
                    IngressInvocationResponseSink {
                        request_id: restart_request_id,
                    },
                )),
            }))
            .await;

        // Assert restart response
        assert_that!(
            actions,
            all!(
                // Verify the invocation gets killed (returning an error to ingress)
                contains(pat!(Action::IngressResponse {
                    request_id: eq(request_id),
                    invocation_id: some(eq(invocation_id)),
                    response: eq(InvocationOutputResponse::Failure(
                        RESTARTED_INVOCATION_ERROR
                    ))
                })),
                // Verify the restart response is sent
                contains(pat!(Action::ForwardRestartInvocationResponse {
                    request_id: eq(restart_request_id),
                    response: eq(RestartInvocationResponse::Ok { new_epoch: 1 })
                })),
                // Verify the invocation is restarted
                contains(pat!(Action::Invoke {
                    invocation_id: eq(invocation_id),
                }))
            )
        );

        // Verify the invocation is now in-flight with a new epoch, and contains only the input entry
        assert_that!(
            test_env
                .storage()
                .get_invocation_status(&invocation_id)
                .await,
            ok(all!(
                is_variant(InvocationStatusDiscriminants::Invoked),
                is_epoch(1)
            ))
        );
        test_env
            .verify_journal_components(invocation_id, [CommandType::Input.into()])
            .await;

        // Verify we have the archived status as well
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
                [
                    CommandType::Input.into(),
                    CommandType::Sleep.into(),
                    CommandType::Output.into(),
                ],
            )
            .await;

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn restart_workflow_invocation() {
        let mut test_env = TestEnv::create().await;

        let retention = Duration::from_secs(60) * 60 * 24;
        let invocation_target = InvocationTarget::mock_workflow();
        let invocation_id = InvocationId::mock_generate(&invocation_target);
        let request_id = PartitionProcessorRpcRequestId::default();
        let workflow_id = invocation_target.as_keyed_service_id().unwrap();

        let promise_key = ByteString::from("promisekey");
        let promise_value = Bytes::copy_from_slice(b"promisevalue");

        // Create and complete a workflow invocation
        let actions = test_env
            .apply_multiple([
                Command::Invoke(ServiceInvocation {
                    invocation_id,
                    invocation_target: invocation_target.clone(),
                    response_sink: Some(ServiceInvocationResponseSink::Ingress { request_id }),
                    completion_retention_duration: retention,
                    journal_retention_duration: retention,
                    ..ServiceInvocation::mock()
                }),
                pinned_deployment(invocation_id, ServiceProtocolVersion::V5),
                invoker_entry_effect(
                    invocation_id,
                    SetStateCommand {
                        key: ByteString::from("key"),
                        value: Bytes::copy_from_slice(b"value"),
                        name: Default::default(),
                    },
                ),
                invoker_entry_effect(
                    invocation_id,
                    CompletePromiseCommand {
                        key: promise_key.clone(),
                        value: CompletePromiseValue::Success(promise_value.clone()),
                        completion_id: 1,
                        name: Default::default(),
                    },
                ),
                invoker_entry_effect(
                    invocation_id,
                    OutputCommand {
                        result: OutputResult::Success(Bytes::from_static(b"workflow result")),
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
                    Bytes::from_static(b"workflow result")
                ))
            }))
        );

        // Verify state and promises exist
        let states: Vec<_> = test_env
            .storage()
            .get_all_user_states_for_service(&workflow_id)
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_that!(states, not(empty()));
        let promise = test_env
            .storage()
            .get_promise(&workflow_id, &promise_key)
            .await
            .unwrap();
        assert_that!(
            promise,
            some(pat!(Promise {
                state: eq(PromiseState::Completed(PromiseResult::Success(
                    promise_value
                )))
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
                is_epoch(0)
            ))
        );
        test_env
            .verify_journal_components(
                invocation_id,
                [
                    CommandType::Input.into(),
                    CommandType::SetState.into(),
                    CommandType::CompletePromise.into(),
                    CompletionType::CompletePromise.into(),
                    CommandType::Output.into(),
                ],
            )
            .await;

        // Now let's restart the workflow invocation with ClearAllPromisesAndState
        let restart_request_id = PartitionProcessorRpcRequestId::default();
        let actions = test_env
            .apply(Command::RestartInvocation(restart::Request {
                invocation_id,
                if_running: IfRunning::Kill,
                previous_attempt_retention: None,
                apply_to_workflow_run: ApplyToWorkflowRun::ClearAllPromisesAndState,
                response_sink: Some(InvocationMutationResponseSink::Ingress(
                    IngressInvocationResponseSink {
                        request_id: restart_request_id,
                    },
                )),
            }))
            .await;

        // Assert restart response
        assert_that!(
            actions,
            contains(pat!(Action::ForwardRestartInvocationResponse {
                request_id: eq(restart_request_id),
                response: eq(RestartInvocationResponse::Ok { new_epoch: 1 })
            }))
        );

        // Verify the invocation is now in-flight with a new epoch
        assert_that!(
            test_env
                .storage()
                .get_invocation_status(&invocation_id)
                .await,
            ok(all!(
                is_variant(InvocationStatusDiscriminants::Invoked),
                is_epoch(1)
            ))
        );
        test_env
            .verify_journal_components(invocation_id, [CommandType::Input.into()])
            .await;

        // Verify state has been cleared
        let states: Vec<_> = test_env
            .storage()
            .get_all_user_states_for_service(&workflow_id)
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_that!(states, empty());

        // Verify promises have been cleared
        let promise = test_env
            .storage()
            .get_promise(&workflow_id, &promise_key)
            .await
            .unwrap();
        assert_that!(promise, none());

        // Verify previous journal and status were correctly archived
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
                [
                    CommandType::Input.into(),
                    CommandType::SetState.into(),
                    CommandType::CompletePromise.into(),
                    CompletionType::CompletePromise.into(),
                    CommandType::Output.into(),
                ],
            )
            .await;

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn restart_running_invocation_without_retention() {
        let mut test_env = TestEnv::create().await;

        let invocation_target = InvocationTarget::mock_service();
        let invocation_id = InvocationId::mock_generate(&invocation_target);
        let request_id = PartitionProcessorRpcRequestId::default();

        // Create an invocation but don't complete it
        let _ = test_env
            .apply_multiple([
                Command::Invoke(ServiceInvocation {
                    invocation_id,
                    invocation_target: invocation_target.clone(),
                    response_sink: Some(ServiceInvocationResponseSink::Ingress { request_id }),
                    ..ServiceInvocation::mock()
                }),
                pinned_deployment(invocation_id, ServiceProtocolVersion::V5),
                // Just add some entry here
                invoker_entry_effect(
                    invocation_id,
                    SleepCommand {
                        wake_up_time: MillisSinceEpoch::now(),
                        completion_id: 1,
                        name: Default::default(),
                    },
                ),
            ])
            .await;

        // InvocationStatus should be Invoked
        assert_that!(
            test_env
                .storage()
                .get_invocation_status(&invocation_id)
                .await,
            ok(all!(
                is_variant(InvocationStatusDiscriminants::Invoked),
                has_journal_length(2),
                has_commands(2),
                is_epoch(0)
            ))
        );

        // Now let's restart the invocation
        let restart_request_id = PartitionProcessorRpcRequestId::default();
        let actions = test_env
            .apply(Command::RestartInvocation(restart::Request {
                invocation_id,
                if_running: IfRunning::Kill,
                previous_attempt_retention: None,
                apply_to_workflow_run: ApplyToWorkflowRun::Nothing,
                response_sink: Some(InvocationMutationResponseSink::Ingress(
                    IngressInvocationResponseSink {
                        request_id: restart_request_id,
                    },
                )),
            }))
            .await;

        // Assert restart response
        assert_that!(
            actions,
            all!(
                // Verify the invocation gets killed (returning an error to ingress)
                contains(pat!(Action::IngressResponse {
                    request_id: eq(request_id),
                    invocation_id: some(eq(invocation_id)),
                    response: eq(InvocationOutputResponse::Failure(
                        RESTARTED_INVOCATION_ERROR
                    ))
                })),
                // Verify the restart response is sent
                contains(pat!(Action::ForwardRestartInvocationResponse {
                    request_id: eq(restart_request_id),
                    response: eq(RestartInvocationResponse::Ok { new_epoch: 1 })
                })),
                // Verify the invocation is restarted
                contains(pat!(Action::Invoke {
                    invocation_id: eq(invocation_id),
                }))
            )
        );

        // Verify the invocation is now in-flight with a new epoch, and contains only the input entry
        assert_that!(
            test_env
                .storage()
                .get_invocation_status(&invocation_id)
                .await,
            ok(all!(
                is_variant(InvocationStatusDiscriminants::Invoked),
                is_epoch(1)
            ))
        );
        test_env
            .verify_journal_components(invocation_id, [CommandType::Input.into()])
            .await;

        // No archived status
        assert_that!(
            test_env
                .storage()
                .get_invocation_status_for_epoch(&invocation_id, 0)
                .await,
            ok(eq(InvocationStatus::Free))
        );

        // Complete the restarted invocation too
        let _ = test_env
            .apply_multiple([
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

        // Nothing left for this invocation
        assert_that!(
            test_env
                .storage()
                .get_invocation_status(&invocation_id)
                .await,
            ok(eq(InvocationStatus::Free))
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn restart_completed_invocation_without_journal_retention() {
        let mut test_env = TestEnv::create().await;

        let invocation_target = InvocationTarget::mock_service();
        let invocation_id = InvocationId::mock_generate(&invocation_target);
        let request_id = PartitionProcessorRpcRequestId::default();

        // Create and complete a fresh invocation
        let _ = test_env
            .apply_multiple([
                Command::Invoke(ServiceInvocation {
                    invocation_id,
                    invocation_target: invocation_target.clone(),
                    response_sink: Some(ServiceInvocationResponseSink::Ingress { request_id }),
                    completion_retention_duration: Duration::from_secs(60) * 60 * 24,
                    journal_retention_duration: Duration::ZERO,
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
            ])
            .await;

        // InvocationStatus contains completed
        assert_that!(
            test_env
                .storage()
                .get_invocation_status(&invocation_id)
                .await,
            ok(all!(
                is_variant(InvocationStatusDiscriminants::Completed),
                is_epoch(0),
                // No journal retained
                has_journal_length(0),
                has_commands(0),
            ))
        );

        // Now let's restart the invocation
        let restart_request_id = PartitionProcessorRpcRequestId::default();
        let actions = test_env
            .apply(Command::RestartInvocation(restart::Request {
                invocation_id,
                if_running: Default::default(),
                previous_attempt_retention: Default::default(),
                apply_to_workflow_run: Default::default(),
                response_sink: Some(InvocationMutationResponseSink::Ingress(
                    IngressInvocationResponseSink {
                        request_id: restart_request_id,
                    },
                )),
            }))
            .await;

        // Restart doesn't work
        assert_that!(
            actions,
            all!(
                contains(pat!(Action::ForwardRestartInvocationResponse {
                    request_id: eq(restart_request_id),
                    response: eq(RestartInvocationResponse::MissingInput)
                })),
                not(contains(pat!(Action::Invoke {
                    invocation_id: eq(invocation_id),
                })))
            )
        );

        // No mutations performed
        assert_that!(
            test_env
                .storage()
                .get_invocation_status(&invocation_id)
                .await,
            ok(all!(
                is_variant(InvocationStatusDiscriminants::Completed),
                is_epoch(0),
                // No journal retained
                has_journal_length(0),
                has_commands(0),
            ))
        );

        test_env.shutdown().await;
    }
}
