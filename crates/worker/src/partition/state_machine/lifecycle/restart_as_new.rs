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
use ahash::HashSet;
use opentelemetry::trace::Span;
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::fsm_table::WriteFsmTable;
use restate_storage_api::idempotency_table::IdempotencyTable;
use restate_storage_api::inbox_table::WriteInboxTable;
use restate_storage_api::invocation_status_table::{
    InvocationStatus, JournalMetadata, PreFlightInvocationArgument, PreFlightInvocationJournal,
    PreFlightInvocationMetadata, ReadInvocationStatusTable, StatusTimestamps,
    WriteInvocationStatusTable,
};
use restate_storage_api::journal_table_v2::ReadJournalTable;
use restate_storage_api::service_status_table::{
    ReadVirtualObjectStatusTable, WriteVirtualObjectStatusTable,
};
use restate_storage_api::timer_table::WriteTimerTable;
use restate_storage_api::{journal_table as journal_table_v1, journal_table_v2};
use restate_types::identifiers::{DeploymentId, EntryIndex, InvocationId};
use restate_types::invocation::client::RestartAsNewInvocationResponse;
use restate_types::invocation::{
    InvocationMutationResponseSink, ServiceInvocationSpanContext, Source, SpanRelation,
};
use restate_types::journal_v2;
use restate_types::journal_v2::{CommandMetadata, EntryMetadata, EntryType, NotificationId};

pub struct OnRestartAsNewInvocationCommand {
    pub invocation_id: InvocationId,
    pub new_invocation_id: InvocationId,
    pub copy_prefix_up_to_index_included: EntryIndex,
    pub patch_deployment_id: Option<DeploymentId>,
    pub response_sink: Option<InvocationMutationResponseSink>,
}

impl<'ctx, 's: 'ctx, S> StateMachineApplyContext<'s, S> {
    fn reply(
        &'ctx mut self,
        response_sink: Option<InvocationMutationResponseSink>,
        response: RestartAsNewInvocationResponse,
    ) {
        if let Some(InvocationMutationResponseSink::Ingress(sink)) = response_sink {
            debug_if_leader!(
                self.is_leader,
                "Send restart as new response to request id '{:?}': {:?}",
                sink.request_id,
                response
            );
            self.action_collector
                .push(Action::ForwardRestartAsNewInvocationResponse {
                    request_id: sink.request_id,
                    response,
                })
        }
    }
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnRestartAsNewInvocationCommand
where
    S: ReadJournalTable
        + IdempotencyTable
        + ReadInvocationStatusTable
        + WriteInvocationStatusTable
        + WriteFsmTable
        + ReadVirtualObjectStatusTable
        + WriteVirtualObjectStatusTable
        + WriteTimerTable
        + WriteInboxTable
        + journal_table_v1::WriteJournalTable
        + journal_table_v2::WriteJournalTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let OnRestartAsNewInvocationCommand {
            invocation_id,
            new_invocation_id,
            copy_prefix_up_to_index_included,
            patch_deployment_id,
            response_sink,
        } = self;
        // Retrieve completed status
        let completed_invocation = match ctx.get_invocation_status(&invocation_id).await? {
            InvocationStatus::Completed(completed_invocation) => completed_invocation,
            InvocationStatus::Free => {
                ctx.reply(response_sink, RestartAsNewInvocationResponse::NotFound);
                return Ok(());
            }
            InvocationStatus::Scheduled(_) | InvocationStatus::Inboxed(_) => {
                ctx.reply(response_sink, RestartAsNewInvocationResponse::NotStarted);
                return Ok(());
            }
            InvocationStatus::Invoked { .. }
            | InvocationStatus::Suspended { .. }
            | InvocationStatus::Paused { .. } => {
                ctx.reply(response_sink, RestartAsNewInvocationResponse::StillRunning);
                return Ok(());
            }
        };

        if copy_prefix_up_to_index_included >= completed_invocation.journal_metadata.length {
            ctx.reply(
                response_sink,
                RestartAsNewInvocationResponse::JournalIndexOutOfRange,
            );
            return Ok(());
        }

        // -- NOTE: This command assumes few checks have been already executed
        //     in the respective RPC handler. Please check that out before reading this through.

        // Action plan
        // - Copy over the journal prefix we're interested into
        // - Prep a new PreFlightInvocationStatus, applying new pinned deployment and keep journal info
        // - Pass it over to the on_pre_flight_invocation function, which deals with the rest

        // --- Let's start copying the journal
        let mut new_journal_index = 0;
        let mut new_journal_commands = 0;
        let mut missing_completions = HashSet::default();
        // Scan up to the index we want to copy (included)
        for i in 0..(copy_prefix_up_to_index_included + 1) {
            let Some(entry) =
                ReadJournalTable::get_journal_entry(ctx.storage, invocation_id, i).await?
            else {
                break;
            };

            match entry.ty() {
                EntryType::Command(_) => {
                    // If it's a command, figure out the completion ids and add them to the list of missing completions
                    let cmd = entry.decode::<ServiceProtocolV4Codec, journal_v2::Command>()?;
                    let related_completion_ids = cmd.related_completion_ids();
                    for completion_id in &related_completion_ids {
                        missing_completions.insert(*completion_id);
                    }
                    new_journal_commands += 1;

                    // Now copy to the new journal
                    journal_table_v2::WriteJournalTable::put_journal_entry(
                        ctx.storage,
                        new_invocation_id,
                        new_journal_index,
                        &entry,
                        &related_completion_ids,
                    )?;
                }
                EntryType::Notification(_) => {
                    let id = entry
                        .inner
                        .try_as_notification_ref()
                        .expect("storage corruption")
                        .id();
                    // If it's a completion, remove it from the list of missing completions
                    if let NotificationId::CompletionId(completion_id) = id {
                        missing_completions.remove(&completion_id);
                    }

                    // Now copy to the new journal
                    journal_table_v2::WriteJournalTable::put_journal_entry(
                        ctx.storage,
                        new_invocation_id,
                        new_journal_index,
                        &entry,
                        &[],
                    )?;
                }
            }

            // Increment journal index
            new_journal_index += 1;
        }

        // At this point, we need to continue iterating the journal
        // until we retrieve all the missing completions
        for i in
            (copy_prefix_up_to_index_included + 1)..completed_invocation.journal_metadata.length
        {
            if let Some(entry) =
                ReadJournalTable::get_journal_entry(ctx.storage, invocation_id, i).await?
                && let Some(notification) = entry.inner.try_as_notification_ref()
                && let NotificationId::CompletionId(completion_id) = notification.id()
                && missing_completions.remove(&completion_id)
            {
                // Copy over this notification
                journal_table_v2::WriteJournalTable::put_journal_entry(
                    ctx.storage,
                    new_invocation_id,
                    new_journal_index,
                    &entry,
                    &[],
                )?;

                // Increment journal index
                new_journal_index += 1;
            }
            if missing_completions.is_empty() {
                // We're good
                break;
            }
        }

        // --- Journal copied, let's roll

        // All the protocol version invariants should have been checked already by the RPC handler
        // so we just apply the new deployment here.
        let mut pinned_deployment = completed_invocation.pinned_deployment;
        if let Some(new_deployment_id) = patch_deployment_id
            && let Some(pinned_deployment) = &mut pinned_deployment
        {
            pinned_deployment.deployment_id = new_deployment_id;
        }

        // Prep tracing span
        let restart_as_new_span = restate_tracing_instrumentation::info_invocation_span!(
            relation = SpanRelation::Linked(
                completed_invocation
                    .journal_metadata
                    .span_context
                    .span_context()
                    .clone(),
            ),
            prefix = "restart-as-new",
            id = new_invocation_id,
            target = completed_invocation.invocation_target,
            tags = (restate.invocation.restart_as_new.original_invocation_id =
                invocation_id.to_string())
        );

        // Let's prep the PreFlightInvocationMetadata
        let pre_flight_invocation_metadata = PreFlightInvocationMetadata {
            timestamps: StatusTimestamps::init(ctx.record_created_at),
            invocation_target: completed_invocation.invocation_target,
            created_using_restate_version: completed_invocation.created_using_restate_version,
            input: PreFlightInvocationArgument::Journal(PreFlightInvocationJournal {
                journal_metadata: JournalMetadata::new(
                    new_journal_index,
                    new_journal_commands,
                    ServiceInvocationSpanContext::start(
                        &new_invocation_id,
                        SpanRelation::parent(restart_as_new_span.span_context()),
                    ),
                ),
                pinned_deployment,
            }),
            source: Source::RestartAsNew(invocation_id),
            completion_retention_duration: completed_invocation.completion_retention_duration,
            journal_retention_duration: completed_invocation.journal_retention_duration,
            random_seed: completed_invocation.random_seed,

            // We don't set those
            idempotency_key: None,
            execution_time: None,
            response_sinks: Default::default(),
        };

        // --- Invocation metadata ready, now go through the usual flow
        ctx.on_pre_flight_invocation(new_invocation_id, pre_flight_invocation_metadata, None)
            .await?;

        // --- Reply all good
        ctx.reply(
            response_sink,
            RestartAsNewInvocationResponse::Ok { new_invocation_id },
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::partition::state_machine::ExperimentalFeature;
    use crate::partition::state_machine::tests::{TestEnv, fixtures, matchers};
    use googletest::prelude::*;
    use restate_storage_api::invocation_status_table::{
        InFlightInvocationMetadata, InvocationStatusDiscriminants, ReadInvocationStatusTable,
    };
    use restate_types::identifiers::{
        DeploymentId, InvocationId, InvocationUuid, PartitionProcessorRpcRequestId,
        WithPartitionKey,
    };
    use restate_types::invocation::client::RestartAsNewInvocationResponse;
    use restate_types::invocation::{
        IngressInvocationResponseSink, InvocationTarget, InvocationTermination,
        NotifySignalRequest, RestartAsNewInvocationRequest, ServiceInvocation, TerminationFlavor,
    };
    use restate_types::journal_v2::{
        CommandType, CompletionType, NotificationType, OutputCommand, OutputResult, Signal,
        SignalId, SignalResult, SleepCommand,
    };
    use restate_types::service_protocol::ServiceProtocolVersion;
    use restate_types::time::MillisSinceEpoch;
    use restate_wal_protocol::Command;
    use restate_wal_protocol::timer::TimerKeyValue;
    use std::time::Duration;

    #[restate_core::test]
    async fn restart_missing_journal() {
        let mut test_env = TestEnv::create().await;

        // Start and complete an invocation with only the input entry
        let invocation_target = InvocationTarget::mock_virtual_object();
        let original_invocation_id = InvocationId::generate(&invocation_target, None);
        let _ = test_env
            .apply_multiple([
                Command::Invoke(Box::new(ServiceInvocation {
                    invocation_id: original_invocation_id,
                    invocation_target: invocation_target.clone(),
                    completion_retention_duration: Duration::from_secs(120),
                    journal_retention_duration: Duration::ZERO,
                    ..ServiceInvocation::mock()
                })),
                fixtures::pinned_deployment(original_invocation_id, ServiceProtocolVersion::V6),
                fixtures::invoker_entry_effect(
                    original_invocation_id,
                    OutputCommand {
                        result: OutputResult::Success(Default::default()),
                        name: Default::default(),
                    },
                ),
                fixtures::invoker_end_effect(original_invocation_id),
            ])
            .await;

        // Sanity check, journal should not be available
        assert_that!(
            test_env
                .storage
                .get_invocation_status(&original_invocation_id)
                .await
                .unwrap(),
            all!(
                matchers::storage::is_variant(InvocationStatusDiscriminants::Completed),
                matchers::storage::has_journal_length(0),
                matchers::storage::has_commands(0)
            )
        );

        // Restart as new with copy_prefix_up_to_index_included = 0
        let new_id = InvocationId::mock_generate(&InvocationTarget::mock_virtual_object());
        let request_id = PartitionProcessorRpcRequestId::new();
        let actions = test_env
            .apply(Command::RestartAsNewInvocation(
                RestartAsNewInvocationRequest {
                    invocation_id: original_invocation_id,
                    new_invocation_id: new_id,
                    copy_prefix_up_to_index_included: 0,
                    patch_deployment_id: None,
                    response_sink: Some(InvocationMutationResponseSink::Ingress(
                        IngressInvocationResponseSink { request_id },
                    )),
                },
            ))
            .await;

        // Didn't happen, because previous invocation was not retained!
        assert_that!(
            actions,
            all!(contains(pat!(
                Action::ForwardRestartAsNewInvocationResponse {
                    request_id: eq(request_id),
                    response: eq(RestartAsNewInvocationResponse::JournalIndexOutOfRange)
                }
            )))
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn restart_killed_invocation() {
        // This works only when using journal table v2 as default!
        // The corner case with journal table v1 is handled by the rpc handler instead.
        let mut test_env = TestEnv::create_with_experimental_features(
            ExperimentalFeature::UseJournalTableV2AsDefault.into(),
        )
        .await;

        // Start invocation, then kill it
        let invocation_target = InvocationTarget::mock_virtual_object();
        let original_invocation_id = InvocationId::generate(&invocation_target, None);
        let _ = test_env
            .apply_multiple([
                Command::Invoke(Box::new(ServiceInvocation {
                    invocation_id: original_invocation_id,
                    invocation_target: invocation_target.clone(),
                    completion_retention_duration: Duration::from_secs(120),
                    journal_retention_duration: Duration::from_secs(120),
                    ..ServiceInvocation::mock()
                })),
                Command::TerminateInvocation(InvocationTermination {
                    invocation_id: original_invocation_id,
                    flavor: TerminationFlavor::Kill,
                    response_sink: None,
                }),
            ])
            .await;

        // Restart as new with copy_prefix_up_to_index_included = 0
        let new_id = InvocationId::mock_generate(&invocation_target);
        let request_id = PartitionProcessorRpcRequestId::new();
        let actions = test_env
            .apply(Command::RestartAsNewInvocation(
                RestartAsNewInvocationRequest {
                    invocation_id: original_invocation_id,
                    new_invocation_id: new_id,
                    copy_prefix_up_to_index_included: 0,
                    patch_deployment_id: None,
                    response_sink: Some(InvocationMutationResponseSink::Ingress(
                        IngressInvocationResponseSink { request_id },
                    )),
                },
            ))
            .await;

        // We should invoke the new invocation and send OK back
        assert_that!(
            actions,
            all!(
                contains(matchers::actions::invoke_for_id(new_id)),
                contains(pat!(Action::ForwardRestartAsNewInvocationResponse {
                    request_id: eq(request_id),
                    response: eq(RestartAsNewInvocationResponse::Ok {
                        new_invocation_id: new_id
                    })
                }))
            )
        );

        assert_that!(
            test_env
                .storage
                .get_invocation_status(&new_id)
                .await
                .unwrap(),
            all!(
                matchers::storage::is_variant(InvocationStatusDiscriminants::Invoked),
                matchers::storage::has_journal_length(1),
                matchers::storage::has_commands(1)
            )
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn restart_copy_input_only() {
        let mut test_env = TestEnv::create().await;

        // Start and complete an invocation with only the input entry
        let invocation_target = InvocationTarget::mock_virtual_object();
        let original_invocation_id = InvocationId::generate(&invocation_target, None);
        let _ = test_env
            .apply_multiple([
                Command::Invoke(Box::new(ServiceInvocation {
                    invocation_id: original_invocation_id,
                    invocation_target: invocation_target.clone(),
                    completion_retention_duration: Duration::from_secs(120),
                    journal_retention_duration: Duration::from_secs(120),
                    ..ServiceInvocation::mock()
                })),
                fixtures::pinned_deployment(original_invocation_id, ServiceProtocolVersion::V6),
                fixtures::invoker_entry_effect(
                    original_invocation_id,
                    OutputCommand {
                        result: OutputResult::Success(Default::default()),
                        name: Default::default(),
                    },
                ),
                fixtures::invoker_end_effect(original_invocation_id),
            ])
            .await;

        // Restart as new with copy_prefix_up_to_index_included = 0
        let new_id = InvocationId::mock_generate(&invocation_target);
        let request_id = PartitionProcessorRpcRequestId::new();
        let actions = test_env
            .apply(Command::RestartAsNewInvocation(
                RestartAsNewInvocationRequest {
                    invocation_id: original_invocation_id,
                    new_invocation_id: new_id,
                    copy_prefix_up_to_index_included: 0,
                    patch_deployment_id: None,
                    response_sink: Some(InvocationMutationResponseSink::Ingress(
                        IngressInvocationResponseSink { request_id },
                    )),
                },
            ))
            .await;

        // We should invoke the new invocation and send OK back
        assert_that!(
            actions,
            all!(
                contains(matchers::actions::invoke_for_id(new_id)),
                contains(pat!(Action::ForwardRestartAsNewInvocationResponse {
                    request_id: eq(request_id),
                    response: eq(RestartAsNewInvocationResponse::Ok {
                        new_invocation_id: new_id
                    })
                }))
            )
        );

        assert_that!(
            test_env
                .storage
                .get_invocation_status(&new_id)
                .await
                .unwrap(),
            all!(
                matchers::storage::is_variant(InvocationStatusDiscriminants::Invoked),
                matchers::storage::has_journal_length(1),
                matchers::storage::has_commands(1)
            )
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn restart_for_virtual_object_gets_inboxed() {
        let mut test_env = TestEnv::create().await;

        let invocation_target = InvocationTarget::mock_virtual_object();

        // Mock the first invocation for the same target that we will restart
        let original_invocation_id = InvocationId::generate(&invocation_target, None);
        let _ = test_env
            .apply_multiple([
                Command::Invoke(Box::new(ServiceInvocation {
                    invocation_id: original_invocation_id,
                    invocation_target: invocation_target.clone(),
                    completion_retention_duration: Duration::from_secs(120),
                    journal_retention_duration: Duration::from_secs(120),
                    ..ServiceInvocation::mock()
                })),
                fixtures::pinned_deployment(original_invocation_id, ServiceProtocolVersion::V6),
                fixtures::invoker_entry_effect(
                    original_invocation_id,
                    OutputCommand {
                        result: OutputResult::Success(Default::default()),
                        name: Default::default(),
                    },
                ),
                fixtures::invoker_end_effect(original_invocation_id),
            ])
            .await;

        // Now before restarting the invocation, lock the VO
        let locker_id = fixtures::mock_start_invocation_with_invocation_target(
            &mut test_env,
            invocation_target.clone(),
        )
        .await;

        // Now restart original into a new invocation while VO is locked by locker_id
        let new_id = InvocationId::mock_generate(&invocation_target);
        let _ = test_env
            .apply(Command::RestartAsNewInvocation(
                RestartAsNewInvocationRequest {
                    invocation_id: original_invocation_id,
                    new_invocation_id: new_id,
                    copy_prefix_up_to_index_included: 0,
                    patch_deployment_id: None,
                    response_sink: None,
                },
            ))
            .await;

        // New invocation should be inboxed due to VO lock
        assert_that!(
            test_env
                .storage
                .get_invocation_status(&new_id)
                .await
                .unwrap(),
            matchers::storage::is_variant(InvocationStatusDiscriminants::Inboxed)
        );

        // Keep the locker alive to ensure it's indeed running
        assert_that!(
            test_env
                .storage
                .get_invocation_status(&locker_id)
                .await
                .unwrap(),
            matchers::storage::is_variant(InvocationStatusDiscriminants::Invoked)
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn restart_copies_journal_prefix() {
        let mut test_env = TestEnv::create().await;

        let invocation_target = InvocationTarget::mock_service();
        let original_invocation_id = InvocationId::generate(&invocation_target, None);

        // Build journal: input(0 already) + command(1) + signal() + completion(2)
        let wake_up_time = MillisSinceEpoch::now();
        let completion_id = 1u32;
        let _ = test_env
            .apply_multiple([
                Command::Invoke(Box::new(ServiceInvocation {
                    invocation_id: original_invocation_id,
                    invocation_target: invocation_target.clone(),
                    completion_retention_duration: Duration::from_secs(120),
                    journal_retention_duration: Duration::from_secs(120),
                    ..ServiceInvocation::mock()
                })),
                fixtures::pinned_deployment(original_invocation_id, ServiceProtocolVersion::V6),
                fixtures::invoker_entry_effect(
                    original_invocation_id,
                    SleepCommand {
                        wake_up_time,
                        name: Default::default(),
                        completion_id,
                    },
                ),
                Command::NotifySignal(NotifySignalRequest {
                    invocation_id: original_invocation_id,
                    signal: Signal::new(
                        SignalId::for_index(1),
                        SignalResult::Success(Default::default()),
                    ),
                }),
                fixtures::invoker_entry_effect(
                    original_invocation_id,
                    SleepCommand {
                        wake_up_time,
                        name: Default::default(),
                        completion_id: completion_id + 1,
                    },
                ),
                Command::Timer(TimerKeyValue::complete_journal_entry(
                    wake_up_time,
                    original_invocation_id,
                    completion_id,
                    0,
                )),
                fixtures::invoker_entry_effect(
                    original_invocation_id,
                    OutputCommand {
                        result: OutputResult::Success(Default::default()),
                        name: Default::default(),
                    },
                ),
                fixtures::invoker_end_effect(original_invocation_id),
            ])
            .await;

        // Capture original random seed to compare later
        let original_status = test_env
            .storage
            .get_invocation_status(&original_invocation_id)
            .await
            .unwrap()
            .try_as_completed()
            .unwrap();

        // Restart with copy_prefix_up_to_index_included = 1 and patch pinned deployment
        let new_invocation_id = InvocationId::from_parts(
            original_invocation_id.partition_key(),
            InvocationUuid::generate(&original_status.invocation_target, None),
        );
        let new_deployment_id = DeploymentId::new();
        let _ = test_env
            .apply(Command::RestartAsNewInvocation(
                RestartAsNewInvocationRequest {
                    invocation_id: original_invocation_id,
                    new_invocation_id,
                    copy_prefix_up_to_index_included: 2,
                    patch_deployment_id: Some(new_deployment_id),
                    response_sink: None,
                },
            ))
            .await;

        // Verify state of the new invocation
        assert_that!(
            test_env
                .storage
                .get_invocation_status(&new_invocation_id)
                .await
                .unwrap(),
            all!(
                matchers::storage::is_variant(InvocationStatusDiscriminants::Invoked),
                matchers::storage::has_journal_length(4),
                matchers::storage::has_commands(2),
                matchers::storage::pinned_deployment_id_eq(new_deployment_id),
                matchers::storage::in_flight_metadata(pat!(InFlightInvocationMetadata {
                    random_seed: eq(original_status.random_seed),
                    source: eq(Source::RestartAsNew(original_invocation_id))
                }))
            )
        );

        test_env
            .verify_journal_components(
                new_invocation_id,
                [
                    CommandType::Input.into(),
                    CommandType::Sleep.into(),
                    NotificationType::Signal.into(),
                    CompletionType::Sleep.into(),
                ],
            )
            .await;

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn restart_index_out_of_range_non_empty_journal() {
        let mut test_env = TestEnv::create().await;

        // Start and complete an invocation with the journal retained
        let invocation_target = InvocationTarget::mock_virtual_object();
        let original_invocation_id = InvocationId::generate(&invocation_target, None);
        let _ = test_env
            .apply_multiple([
                Command::Invoke(Box::new(ServiceInvocation {
                    invocation_id: original_invocation_id,
                    invocation_target: invocation_target.clone(),
                    completion_retention_duration: Duration::from_secs(120),
                    journal_retention_duration: Duration::from_secs(120),
                    ..ServiceInvocation::mock()
                })),
                fixtures::pinned_deployment(original_invocation_id, ServiceProtocolVersion::V6),
                // Complete with an output so the journal has at least input+output
                fixtures::invoker_entry_effect(
                    original_invocation_id,
                    OutputCommand {
                        result: OutputResult::Success(Default::default()),
                        name: Default::default(),
                    },
                ),
                fixtures::invoker_end_effect(original_invocation_id),
            ])
            .await;

        // Fetch completed status to get the exact journal length
        let original_status = test_env
            .storage
            .get_invocation_status(&original_invocation_id)
            .await
            .unwrap()
            .try_as_completed()
            .unwrap();
        let len = original_status.journal_metadata.length;
        assert!(len > 0, "expected non-empty journal for this test");

        // Using copy_prefix equal to the length must be out of range
        let new_invocation_id = InvocationId::mock_generate(&invocation_target);
        let request_id = PartitionProcessorRpcRequestId::new();
        let actions = test_env
            .apply(Command::RestartAsNewInvocation(
                RestartAsNewInvocationRequest {
                    invocation_id: original_invocation_id,
                    new_invocation_id,
                    copy_prefix_up_to_index_included: len,
                    patch_deployment_id: None,
                    response_sink: Some(InvocationMutationResponseSink::Ingress(
                        IngressInvocationResponseSink { request_id },
                    )),
                },
            ))
            .await;

        assert_that!(
            actions,
            contains(pat!(Action::ForwardRestartAsNewInvocationResponse {
                request_id: eq(request_id),
                response: eq(RestartAsNewInvocationResponse::JournalIndexOutOfRange)
            }))
        );

        test_env.shutdown().await;
    }
}
