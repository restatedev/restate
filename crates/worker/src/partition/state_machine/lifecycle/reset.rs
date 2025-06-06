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
use crate::partition::state_machine::lifecycle::ArchiveInvocationCommand;
use crate::partition::state_machine::{
    Action, CommandHandler, Error, StateMachineApplyContext, should_use_journal_table_v2,
};
use restate_invoker_api::InvokeInputJournal;
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::fsm_table::FsmTable;
use restate_storage_api::invocation_status_table::{
    CompletedInvocation, InvocationStatus, InvocationStatusTable,
};
use restate_storage_api::journal_table_v2::JournalTable;
use restate_storage_api::outbox_table::{OutboxMessage, OutboxTable};
use restate_types::errors::RESET_INVOCATION_ERROR;
use restate_types::identifiers::InvocationId;
use restate_types::invocation::client::ResetInvocationResponse;
use restate_types::invocation::reset::{ApplyToChildInvocations, ApplyToPinnedDeployment};
use restate_types::invocation::{
    IngressInvocationResponseSink, InvocationMutationResponseSink, InvocationTermination,
    ResponseResult, TerminationFlavor,
};
use restate_types::journal_v2::{
    Command, CommandMetadata, CompletionId, EntryIndex, EntryMetadata, EntryType, NotificationId,
    NotificationType,
};
use std::cmp;
use std::time::Duration;
use tracing::{trace, warn};

pub struct OnResetInvocationCommand {
    pub invocation_id: InvocationId,
    pub invocation_status: InvocationStatus,
    pub truncation_point_entry_index: EntryIndex,
    pub previous_attempt_retention: Option<Duration>,
    pub apply_to_child_calls: ApplyToChildInvocations,
    pub apply_to_pinned_deployment: ApplyToPinnedDeployment,
    pub response_sink: Option<InvocationMutationResponseSink>,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnResetInvocationCommand
where
    S: JournalTable + InvocationStatusTable + OutboxTable + FsmTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let OnResetInvocationCommand {
            invocation_id,
            invocation_status,
            truncation_point_entry_index,
            previous_attempt_retention,
            apply_to_child_calls,
            apply_to_pinned_deployment,
            response_sink,
        } = self;

        if !should_use_journal_table_v2(&invocation_status) {
            trace!(
                "Received restart command for invocation using the old journal table, this is unsupported."
            );
            ctx.reply_to_reset_invocation(response_sink, ResetInvocationResponse::Unsupported);
            return Ok(());
        }

        let is_invoked = matches!(invocation_status, InvocationStatus::Invoked(_));
        let Some(mut in_flight_invocation_metadata) = invocation_status.into_invocation_metadata()
        else {
            debug_if_leader!(
                ctx.is_leader,
                "Ignoring reset command because the invocation is not invoked nor suspended"
            );
            ctx.reply_to_reset_invocation(response_sink, ResetInvocationResponse::NotRunning);
            return Ok(());
        };

        // Validate the command first. The entry index must correspond to a command or signal index.
        if truncation_point_entry_index == 0 {
            debug_if_leader!(
                ctx.is_leader,
                "Ignoring reset command because truncation index is 0. You can't remove the input entry."
            );
            ctx.reply_to_reset_invocation(response_sink, ResetInvocationResponse::BadIndex);
            return Ok(());
        }
        let Some(truncation_point_entry) = ctx
            .storage
            .get_journal_entry(invocation_id, truncation_point_entry_index)
            .await?
        else {
            debug_if_leader!(
                ctx.is_leader,
                "Ignoring reset command because the given entry index doesn't exist"
            );
            ctx.reply_to_reset_invocation(response_sink, ResetInvocationResponse::BadIndex);
            return Ok(());
        };
        if !matches!(
            truncation_point_entry.ty(),
            EntryType::Command(_) | EntryType::Notification(NotificationType::Signal)
        ) {
            debug_if_leader!(
                ctx.is_leader,
                "Ignoring reset command because the given entry index doesn't correspond to a command entry, nor to a signal notification"
            );
            ctx.reply_to_reset_invocation(response_sink, ResetInvocationResponse::BadIndex);
            return Ok(());
        }

        // We need to send an abort signal to the invoker if the invocation was previously invoked
        if is_invoked {
            ctx.send_abort_invocation_to_invoker(
                invocation_id,
                in_flight_invocation_metadata.current_invocation_epoch,
            );
        }

        // Sanity checks done, let's archive the invocation
        ArchiveInvocationCommand {
            invocation_id,
            completed_invocation: CompletedInvocation::from_in_flight_invocation_metadata(
                in_flight_invocation_metadata.clone(),
                ResponseResult::Failure(RESET_INVOCATION_ERROR),
            ),
            previous_attempt_retention_override: previous_attempt_retention,
        }
        .apply(ctx)
        .await?;

        // Let's apply the mutations to pinned deployment
        match apply_to_pinned_deployment {
            ApplyToPinnedDeployment::Keep => {
                trace!("Will keep the pinned deployment");
            }
            ApplyToPinnedDeployment::Clear => {
                trace!("Clearing the pinned deployment");
                in_flight_invocation_metadata.pinned_deployment = None;
            }
        }

        debug_if_leader!(
            ctx.is_leader,
            "Rewriting journal starting from {}, index {}",
            truncation_point_entry.ty(),
            truncation_point_entry_index
        );

        let new_epoch = in_flight_invocation_metadata.current_invocation_epoch + 1;

        // We need to update the epochs of the journal prefix
        ctx.storage
            .update_current_journal_epoch(&invocation_id, new_epoch, truncation_point_entry_index)
            .await?;

        // Let's run the mark and compact algorithm on the part of the algorithm we wanna drop
        let mut minimum_completion_id_of_removed_commands = CompletionId::MAX;
        let mut notification_ids_to_forget = vec![];
        let mut commands_removed = 0;
        let mut truncated_journal_index = truncation_point_entry_index;
        for truncation_pointer in
            truncation_point_entry_index..in_flight_invocation_metadata.journal_metadata.length
        {
            let Some(entry) = ctx
                .storage
                .get_journal_entry(invocation_id, truncation_pointer)
                .await?
            else {
                warn!("Missing entry at index {truncation_pointer}, this is unexpected");
                return Ok(());
            };

            let keep_this_entry = match entry.ty() {
                EntryType::Command(_) => {
                    let cmd = entry.decode::<ServiceProtocolV4Codec, Command>()?;

                    // Let's make sure minimum_completion_id_of_removed_commands remains minimum
                    // We need to do this in the loop because the trim_point might be a non-completable entry.
                    minimum_completion_id_of_removed_commands = cmp::min(
                        minimum_completion_id_of_removed_commands,
                        cmd.related_completion_ids()
                            .into_iter()
                            .min()
                            .unwrap_or(CompletionId::MAX),
                    );

                    notification_ids_to_forget.extend(
                        cmd.related_completion_ids()
                            .into_iter()
                            .map(NotificationId::CompletionId),
                    );
                    commands_removed += 1;

                    // We remove the command
                    trace!("Removing {} at index {}", entry.ty(), truncation_pointer);

                    // If it's a call, we need to do something more
                    if let Command::Call(call_command) = cmd {
                        let child_invocation_id = call_command.request.invocation_id;
                        match apply_to_child_calls {
                            ApplyToChildInvocations::Nothing => {
                                trace!("Won't kill nor cancel {}", child_invocation_id);
                            }
                            ApplyToChildInvocations::Kill => {
                                trace!("Will kill {}", child_invocation_id);
                                ctx.handle_outgoing_message(OutboxMessage::InvocationTermination(
                                    InvocationTermination {
                                        invocation_id: child_invocation_id,
                                        flavor: TerminationFlavor::Kill,
                                        response_sink: None,
                                    },
                                ))
                                .await?;
                            }
                            ApplyToChildInvocations::Cancel => {
                                trace!("Will cancel {}", child_invocation_id);
                                ctx.handle_outgoing_message(OutboxMessage::InvocationTermination(
                                    InvocationTermination {
                                        invocation_id: child_invocation_id,
                                        flavor: TerminationFlavor::Cancel,
                                        response_sink: None,
                                    },
                                ))
                                .await?;
                            }
                        }
                    }

                    false
                }
                EntryType::Notification(NotificationType::Completion(completion_ty)) => {
                    let completion_id = entry
                        .inner
                        .try_as_notification_ref()
                        .expect("Entry type is notification!")
                        .id()
                        .try_as_completion_id()
                        .expect("Notification is completion id!");

                    if completion_id < minimum_completion_id_of_removed_commands {
                        // We copy this completion because it belongs to a command before the trim point.
                        trace!(
                            "Retaining Completion {} with id {} at index {}",
                            completion_ty, completion_id, truncation_pointer
                        );
                        true
                    } else {
                        // We remove this completion as it belongs to a command after (including) the trim point.
                        notification_ids_to_forget
                            .push(NotificationId::CompletionId(completion_id));
                        trace!(
                            "Removing Completion {} with id {} at index {}",
                            completion_ty, completion_id, truncation_pointer
                        );
                        false
                    }
                }
                EntryType::Notification(NotificationType::Signal) => {
                    // We remove this signal as it belongs to a command after (including) the trim point.
                    let notification_id = entry
                        .inner
                        .try_as_notification_ref()
                        .expect("Entry type is notification!")
                        .id();
                    trace!(
                        "Removing Notification Signal with id {} at index {}",
                        notification_id, truncation_pointer
                    );
                    notification_ids_to_forget.push(notification_id);
                    false
                }
                EntryType::Event => {
                    // We just remove events
                    trace!("Removing Event at index {}", truncation_pointer);
                    false
                }
            };

            if keep_this_entry {
                ctx
                    .storage
                    .put_journal_entry(invocation_id, new_epoch, truncated_journal_index, &entry, /* No need to fill this, we only ever re-add back notifications and not commands */&[])
                    .await?;
                truncated_journal_index += 1;
            }
        }

        // Time to drop the suffix we don't need we don't need
        ctx.storage
            .delete_journal_range(
                invocation_id,
                truncated_journal_index,
                in_flight_invocation_metadata.journal_metadata.length,
                &notification_ids_to_forget,
            )
            .await?;

        // Update the epoch and add the trim point to invocation status
        in_flight_invocation_metadata.current_invocation_epoch = new_epoch;
        in_flight_invocation_metadata
            .completion_range_epoch_map
            .add_truncation_point(minimum_completion_id_of_removed_commands, new_epoch);

        // Update journal length with the new length and the commands.
        in_flight_invocation_metadata.journal_metadata.length = truncated_journal_index;
        in_flight_invocation_metadata.journal_metadata.commands -= commands_removed;

        // Rewrite procedure done! We're now back in the game
        debug_if_leader!(
            ctx.is_leader,
            restate.journal.length = in_flight_invocation_metadata.journal_metadata.length,
            "Journal rewriting completed, resuming the invocation now"
        );
        ctx.invoke(
            invocation_id,
            in_flight_invocation_metadata,
            InvokeInputJournal::NoCachedJournal,
        )
        .await?;

        ctx.reply_to_reset_invocation(response_sink, ResetInvocationResponse::Ok { new_epoch });
        Ok(())
    }
}

impl<S> StateMachineApplyContext<'_, S> {
    fn reply_to_reset_invocation(
        &mut self,
        response_sink: Option<InvocationMutationResponseSink>,
        response: ResetInvocationResponse,
    ) {
        if response_sink.is_none() {
            return;
        }
        let InvocationMutationResponseSink::Ingress(IngressInvocationResponseSink { request_id }) =
            response_sink.unwrap();
        debug_if_leader!(
            self.is_leader,
            "Send reset response to request id '{:?}': {:?}",
            request_id,
            response
        );

        self.action_collector
            .push(Action::ForwardResetInvocationResponse {
                request_id,
                response,
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;

    use crate::partition::state_machine::tests::{TestEnv, fixtures, matchers};
    use googletest::{assert_that, prelude::*};
    use restate_storage_api::invocation_status_table::{
        CompletionRangeEpochMap, InFlightInvocationMetadata, InvocationStatusDiscriminants,
        ReadOnlyInvocationStatusTable,
    };
    use restate_storage_api::journal_table_v2::ReadOnlyJournalTable;
    use restate_types::identifiers::PartitionProcessorRpcRequestId;
    use restate_types::invocation::reset::TruncateFrom;
    use restate_types::invocation::{
        InvocationTarget, NotifySignalRequest, ServiceInvocation, reset,
    };
    use restate_types::journal_v2::raw::RawCommand;
    use restate_types::journal_v2::{
        ClearAllStateCommand, CommandType, CompletionType, Signal, SignalId, SignalResult,
        SleepCommand,
    };
    use restate_types::service_protocol::ServiceProtocolVersion;
    use restate_types::time::MillisSinceEpoch;
    use restate_wal_protocol::timer::TimerKeyValue;

    #[restate_core::test]
    async fn reset_with_empty_journal() {
        let mut test_env = TestEnv::create().await;

        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        for entry_index in 0..=2 {
            // None of these should cause any trim to happen, because either is journal out of bound, or tries to trim input entry, which is special cased.
            let actions = test_env
                .apply(restate_wal_protocol::Command::ResetInvocation(
                    reset::Request {
                        invocation_id,
                        truncate_from: TruncateFrom::EntryIndex { entry_index },
                        previous_attempt_retention: None,
                        apply_to_child_calls: Default::default(),
                        apply_to_pinned_deployment: Default::default(),
                        response_sink: None,
                    },
                ))
                .await;
            assert_that!(actions, empty());
            test_env
                .verify_journal_components(invocation_id, [CommandType::Input.into()])
                .await;
        }

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn reset_with_non_completable_entries() {
        let mut test_env = TestEnv::create().await;

        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        let _ = test_env
            .apply_multiple([
                fixtures::invoker_entry_effect_for_epoch(
                    invocation_id,
                    0,
                    // Any command will do fine
                    ClearAllStateCommand::default(),
                ),
                fixtures::invoker_entry_effect_for_epoch(
                    invocation_id,
                    0,
                    // Any command will do fine
                    ClearAllStateCommand::default(),
                ),
            ])
            .await;
        assert_that!(
            test_env.storage.get_invocation_status(&invocation_id).await,
            // [Input, ClearAllState, ClearAllState]
            ok(all!(
                matchers::storage::has_journal_length(3),
                matchers::storage::has_commands(3)
            ))
        );

        let reset_request_id = PartitionProcessorRpcRequestId::default();
        let actions = test_env
            .apply(restate_wal_protocol::Command::ResetInvocation(
                reset::Request {
                    invocation_id,
                    truncate_from: TruncateFrom::EntryIndex { entry_index: 2 },
                    previous_attempt_retention: None,
                    apply_to_child_calls: Default::default(),
                    apply_to_pinned_deployment: Default::default(),
                    response_sink: Some(InvocationMutationResponseSink::Ingress(
                        IngressInvocationResponseSink {
                            request_id: reset_request_id,
                        },
                    )),
                },
            ))
            .await;
        assert_that!(
            actions,
            all!(
                contains(matchers::actions::invoke_for_id_and_epoch(invocation_id, 1)),
                contains(pat!(Action::ForwardResetInvocationResponse {
                    request_id: eq(reset_request_id),
                    response: eq(ResetInvocationResponse::Ok { new_epoch: 1 })
                })),
            )
        );
        assert_that!(
            test_env.storage.get_invocation_status(&invocation_id).await,
            // Only Input entry and first clear state
            ok(all!(
                matchers::storage::status(InvocationStatusDiscriminants::Invoked),
                matchers::storage::in_flight_meta(pat!(InFlightInvocationMetadata {
                    current_invocation_epoch: eq(1),
                    // There were no completable entries among the trimmed ones, so this map should be unchanged.
                    completion_range_epoch_map: eq(CompletionRangeEpochMap::default())
                }))
            ))
        );
        test_env
            .verify_journal_components(
                invocation_id,
                [CommandType::Input.into(), CommandType::ClearAllState.into()],
            )
            .await;

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn reset_with_completable_entries() {
        let mut test_env = TestEnv::create().await;

        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        let wake_up_time = MillisSinceEpoch::now();

        let _ = test_env
            .apply_multiple([
                fixtures::invoker_entry_effect_for_epoch(
                    invocation_id,
                    0,
                    SleepCommand {
                        wake_up_time,
                        completion_id: 1,
                        name: Default::default(),
                    },
                ),
                fixtures::invoker_entry_effect_for_epoch(
                    invocation_id,
                    0,
                    SleepCommand {
                        wake_up_time: wake_up_time + Duration::from_secs(60),
                        completion_id: 2,
                        name: Default::default(),
                    },
                ),
            ])
            .await;
        assert_that!(
            test_env.storage.get_invocation_status(&invocation_id).await,
            // [Input, SleepCommand, SleepCommand]
            ok(matchers::storage::has_journal_length(3))
        );

        // Let's complete one of the sleeps
        let _ = test_env
            .apply(restate_wal_protocol::Command::Timer(
                TimerKeyValue::complete_journal_entry(wake_up_time, invocation_id, 1, 0),
            ))
            .await;
        test_env
            .verify_journal_components(
                invocation_id,
                [
                    CommandType::Input.into(),
                    CommandType::Sleep.into(),
                    CommandType::Sleep.into(),
                    CompletionType::Sleep.into(),
                ],
            )
            .await;

        let actions = test_env
            .apply(restate_wal_protocol::Command::ResetInvocation(
                reset::Request {
                    invocation_id,
                    truncate_from: TruncateFrom::EntryIndex { entry_index: 2 },
                    previous_attempt_retention: None,
                    apply_to_child_calls: Default::default(),
                    apply_to_pinned_deployment: Default::default(),
                    response_sink: None,
                },
            ))
            .await;
        assert_that!(
            actions,
            contains(matchers::actions::invoke_for_id_and_epoch(invocation_id, 1))
        );
        assert_that!(
            test_env.storage.get_invocation_status(&invocation_id).await,
            // Only Input entry and first clear state
            ok(all!(
                matchers::storage::status(InvocationStatusDiscriminants::Invoked),
                matchers::storage::in_flight_meta(pat!(InFlightInvocationMetadata {
                    current_invocation_epoch: eq(1),
                    // This should contain the trim point!
                    completion_range_epoch_map: eq(
                        CompletionRangeEpochMap::from_truncation_points([(2, 1)])
                    )
                }))
            ))
        );
        test_env
            .verify_journal_components(
                invocation_id,
                [
                    CommandType::Input.into(),
                    CommandType::Sleep.into(),
                    CompletionType::Sleep.into(),
                ],
            )
            .await;
        assert_that!(
            test_env
                .storage
                .get_command_by_completion_id(invocation_id, 2)
                .await,
            // This was the second Sleep
            ok(none())
        );
        assert_that!(
            test_env
                .storage
                .get_command_by_completion_id(invocation_id, 1)
                .await,
            // This was the first
            ok(some(property!(
                RawCommand.ty(),
                eq(EntryType::Command(CommandType::Sleep))
            )))
        );
        assert_that!(
            test_env
                .storage
                .get_notifications_index(invocation_id)
                .await,
            // First notification is there
            ok(eq(HashMap::from([(NotificationId::CompletionId(1), 2u32)])))
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn reset_from_signal_notification() {
        let mut test_env = TestEnv::create().await;

        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        let wake_up_time = MillisSinceEpoch::now();

        let _ = test_env
            .apply_multiple([
                fixtures::invoker_entry_effect_for_epoch(
                    invocation_id,
                    0,
                    SleepCommand {
                        wake_up_time,
                        completion_id: 1,
                        name: Default::default(),
                    },
                ),
                restate_wal_protocol::Command::NotifySignal(NotifySignalRequest {
                    invocation_id,
                    signal: Signal::new(SignalId::for_index(17), SignalResult::Void),
                }),
                fixtures::invoker_entry_effect_for_epoch(
                    invocation_id,
                    0,
                    SleepCommand {
                        wake_up_time: wake_up_time + Duration::from_secs(60),
                        completion_id: 2,
                        name: Default::default(),
                    },
                ),
            ])
            .await;
        test_env
            .verify_journal_components(
                invocation_id,
                [
                    CommandType::Input.into(),
                    CommandType::Sleep.into(),
                    NotificationType::Signal.into(),
                    CommandType::Sleep.into(),
                ],
            )
            .await;

        // Let's complete one of the sleeps
        let _ = test_env
            .apply(restate_wal_protocol::Command::Timer(
                TimerKeyValue::complete_journal_entry(wake_up_time, invocation_id, 1, 0),
            ))
            .await;
        test_env
            .verify_journal_components(
                invocation_id,
                [
                    CommandType::Input.into(),
                    CommandType::Sleep.into(),
                    NotificationType::Signal.into(),
                    CommandType::Sleep.into(),
                    CompletionType::Sleep.into(),
                ],
            )
            .await;

        let actions = test_env
            .apply(restate_wal_protocol::Command::ResetInvocation(
                reset::Request {
                    invocation_id,
                    truncate_from: TruncateFrom::EntryIndex { entry_index: 2 },
                    previous_attempt_retention: None,
                    apply_to_child_calls: Default::default(),
                    apply_to_pinned_deployment: Default::default(),
                    response_sink: None,
                },
            ))
            .await;
        assert_that!(
            actions,
            contains(matchers::actions::invoke_for_id_and_epoch(invocation_id, 1))
        );
        assert_that!(
            test_env.storage.get_invocation_status(&invocation_id).await,
            ok(all!(
                matchers::storage::status(InvocationStatusDiscriminants::Invoked),
                matchers::storage::in_flight_meta(pat!(InFlightInvocationMetadata {
                    current_invocation_epoch: eq(1),
                    // This should contain the trim point!
                    completion_range_epoch_map: eq(
                        CompletionRangeEpochMap::from_truncation_points([(2, 1)])
                    )
                }))
            ))
        );
        test_env
            .verify_journal_components(
                invocation_id,
                [
                    CommandType::Input.into(),
                    CommandType::Sleep.into(),
                    CompletionType::Sleep.into(),
                ],
            )
            .await;
        assert_that!(
            test_env
                .storage
                .get_command_by_completion_id(invocation_id, 2)
                .await,
            // This was the second Sleep
            ok(none())
        );
        assert_that!(
            test_env
                .storage
                .get_command_by_completion_id(invocation_id, 1)
                .await,
            // This was the first
            ok(some(property!(
                RawCommand.ty(),
                eq(EntryType::Command(CommandType::Sleep))
            )))
        );
        assert_that!(
            test_env
                .storage
                .get_notifications_index(invocation_id)
                .await,
            // First notification is there
            ok(eq(HashMap::from([(NotificationId::CompletionId(1), 2u32)])))
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn reset_with_retention() {
        let mut test_env = TestEnv::create().await;

        let retention = Duration::from_secs(60) * 60 * 24;
        let invocation_target = InvocationTarget::mock_service();
        let invocation_id = InvocationId::mock_generate(&invocation_target);

        let _ = test_env
            .apply(restate_wal_protocol::Command::Invoke(ServiceInvocation {
                invocation_id,
                invocation_target: invocation_target.clone(),
                completion_retention_duration: retention,
                journal_retention_duration: retention,
                ..ServiceInvocation::mock()
            }))
            .await;

        let _ = test_env
            .apply_multiple([
                fixtures::pinned_deployment(invocation_id, ServiceProtocolVersion::V5),
                fixtures::invoker_entry_effect_for_epoch(
                    invocation_id,
                    0,
                    // Any command will do fine
                    ClearAllStateCommand::default(),
                ),
                fixtures::invoker_entry_effect_for_epoch(
                    invocation_id,
                    0,
                    // Any command will do fine
                    ClearAllStateCommand::default(),
                ),
            ])
            .await;
        test_env
            .verify_journal_components(
                invocation_id,
                [
                    CommandType::Input.into(),
                    CommandType::ClearAllState.into(),
                    CommandType::ClearAllState.into(),
                ],
            )
            .await;

        let reset_request_id = PartitionProcessorRpcRequestId::default();
        let actions = test_env
            .apply(restate_wal_protocol::Command::ResetInvocation(
                reset::Request {
                    invocation_id,
                    truncate_from: TruncateFrom::EntryIndex { entry_index: 2 },
                    previous_attempt_retention: None,
                    apply_to_child_calls: Default::default(),
                    apply_to_pinned_deployment: Default::default(),
                    response_sink: Some(InvocationMutationResponseSink::Ingress(
                        IngressInvocationResponseSink {
                            request_id: reset_request_id,
                        },
                    )),
                },
            ))
            .await;

        // Verify the reset worked correctly
        assert_that!(
            actions,
            all!(
                contains(matchers::actions::invoke_for_id_and_epoch(invocation_id, 1)),
                contains(pat!(Action::ForwardResetInvocationResponse {
                    request_id: eq(reset_request_id),
                    response: eq(ResetInvocationResponse::Ok { new_epoch: 1 })
                })),
            )
        );
        assert_that!(
            test_env.storage.get_invocation_status(&invocation_id).await,
            // Only Input entry and first clear state
            ok(all!(
                matchers::storage::status(InvocationStatusDiscriminants::Invoked),
                matchers::storage::is_epoch(1),
                matchers::storage::in_flight_meta(pat!(InFlightInvocationMetadata {
                    // There were no completable entries among the trimmed ones, so this map should be unchanged.
                    completion_range_epoch_map: eq(CompletionRangeEpochMap::default())
                }))
            ))
        );
        test_env
            .verify_journal_components(
                invocation_id,
                [CommandType::Input.into(), CommandType::ClearAllState.into()],
            )
            .await;

        // Verify we have the archived invocation
        assert_that!(
            test_env
                .storage()
                .get_invocation_status_for_epoch(&invocation_id, 0)
                .await,
            ok(all!(
                matchers::storage::is_variant(InvocationStatusDiscriminants::Completed),
                matchers::storage::is_epoch(0),
            ))
        );
        test_env
            .verify_journal_components_for_epoch(
                invocation_id,
                0,
                [
                    CommandType::Input.into(),
                    CommandType::ClearAllState.into(),
                    CommandType::ClearAllState.into(),
                ],
            )
            .await;

        test_env.shutdown().await;
    }
}
