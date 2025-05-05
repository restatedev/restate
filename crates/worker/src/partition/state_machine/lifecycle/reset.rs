// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::state_machine::{Action, CommandHandler, Error, StateMachineApplyContext};
use crate::{debug_if_leader, info_if_leader};
use restate_invoker_api::InvokeInputJournal;
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::invocation_status_table::{InvocationStatus, InvocationStatusTable};
use restate_storage_api::journal_table_v2::JournalTable;
use restate_types::identifiers::InvocationId;
use restate_types::journal_v2::{
    Command, CommandMetadata, CompletionId, EntryIndex, EntryMetadata, EntryType, NotificationId,
    NotificationType,
};
use std::cmp;
use tracing::{trace, warn};

pub struct OnResetCommand {
    pub invocation_id: InvocationId,
    pub invocation_status: InvocationStatus,
    pub truncation_point_entry_index: EntryIndex,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>> for OnResetCommand
where
    S: JournalTable + InvocationStatusTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let OnResetCommand {
            invocation_id,
            invocation_status,
            truncation_point_entry_index,
        } = self;

        let is_invoked = matches!(invocation_status, InvocationStatus::Invoked(_));
        let Some(mut in_flight_invocation_metadata) = invocation_status.into_invocation_metadata()
        else {
            info_if_leader!(
                ctx.is_leader,
                "Ignoring reset command because the invocation is not invoked nor suspended"
            );
            return Ok(());
        };

        // Validate the command first. The entry index must correspond to a command or signal index.
        if truncation_point_entry_index == 0 {
            info_if_leader!(
                ctx.is_leader,
                "Ignoring reset command because truncation index is 0. You can't remove the input entry."
            );
            return Ok(());
        }
        let Some(truncation_point_entry) = ctx
            .storage
            .get_journal_entry(invocation_id, truncation_point_entry_index)
            .await?
        else {
            info_if_leader!(
                ctx.is_leader,
                "Ignoring reset command because the given entry index doesn't exist"
            );
            return Ok(());
        };
        if !matches!(
            truncation_point_entry.ty(),
            EntryType::Command(_) | EntryType::Notification(NotificationType::Signal)
        ) {
            info_if_leader!(
                ctx.is_leader,
                "Ignoring reset command because the given entry index doesn't correspond to a command entry, nor to a signal notification"
            );
            return Ok(());
        }
        debug_if_leader!(
            ctx.is_leader,
            "Rewriting journal starting from {}, index {}",
            truncation_point_entry.ty(),
            truncation_point_entry_index
        );

        // We need to send an abort signal to the invoker if the invocation was previously invoked
        if is_invoked {
            ctx.send_abort_invocation_to_invoker(
                invocation_id,
                in_flight_invocation_metadata.current_invocation_epoch,
            );
        }

        // Let's run the mark algorithm on the journal
        let mut minimum_completion_id_of_removed_commands = CompletionId::MAX;
        let mut notifications_to_retain = vec![];
        let mut notification_ids_to_forget = vec![];
        let mut commands_removed = 0;
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

            match entry.ty() {
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
                }
                EntryType::Notification(NotificationType::Completion(completion_ty)) => {
                    let completion_id = entry
                        .inner
                        .try_as_notification()
                        .expect("Entry type is notification!")
                        .id()
                        .try_as_completion_id()
                        .expect("Notification is completion id!");

                    if completion_id < minimum_completion_id_of_removed_commands {
                        // We copy this completion because it belongs to a command before the trim point.
                        notifications_to_retain.push(truncation_pointer);
                        trace!(
                            "Retaining Completion {} with id {} at index {}",
                            completion_ty, completion_id, truncation_pointer
                        );
                    } else {
                        // We remove this completion as it belongs to a command after (including) the trim point.
                        notification_ids_to_forget
                            .push(NotificationId::CompletionId(completion_id));
                        trace!(
                            "Removing Completion {} with id {} at index {}",
                            completion_ty, completion_id, truncation_pointer
                        );
                    }
                }
                EntryType::Notification(NotificationType::Signal) => {
                    // We remove this signal as it belongs to a command after (including) the trim point.
                    let notification_id = entry
                        .inner
                        .try_as_notification()
                        .expect("Entry type is notification!")
                        .id();
                    trace!(
                        "Removing Notification Signal with id {} at index {}",
                        notification_id, truncation_pointer
                    );
                    notification_ids_to_forget.push(notification_id);
                }
                EntryType::Event => {
                    // We just remove events
                    trace!("Removing Event at index {}", truncation_pointer);
                }
            }
        }

        // Now let's clean indexes, compact journal and remove remaining entries
        ctx.storage
            .rewrite_journal(
                invocation_id,
                truncation_point_entry_index,
                &notifications_to_retain,
                &notification_ids_to_forget,
                in_flight_invocation_metadata.journal_metadata.length,
            )
            .await?;

        // Update the epoch, and add the trim point to invocation status
        in_flight_invocation_metadata.current_invocation_epoch += 1;
        in_flight_invocation_metadata
            .completion_range_epoch_map
            .add_truncation_point(
                minimum_completion_id_of_removed_commands,
                in_flight_invocation_metadata.current_invocation_epoch,
            );

        // Update journal length with the new length and the commands.
        in_flight_invocation_metadata.journal_metadata.length =
            truncation_point_entry_index + (notifications_to_retain.len() as u32);
        in_flight_invocation_metadata.journal_metadata.commands -= commands_removed;

        // Rewrite procedure done! We're now back in the game
        debug_if_leader!(
            ctx.is_leader,
            restate.journal.length = in_flight_invocation_metadata.journal_metadata.length,
            "Journal rewriting completed, resuming the invocation now"
        );

        in_flight_invocation_metadata.timestamps.update();
        ctx.action_collector.push(Action::Invoke {
            invocation_id,
            invocation_epoch: in_flight_invocation_metadata.current_invocation_epoch,
            invocation_target: in_flight_invocation_metadata.invocation_target.clone(),
            invoke_input_journal: InvokeInputJournal::NoCachedJournal,
        });

        ctx.storage
            .put_invocation_status(
                &invocation_id,
                &InvocationStatus::Invoked(in_flight_invocation_metadata),
            )
            .await
            .map_err(Error::Storage)?;

        Ok(())
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
    use restate_types::invocation::{NotifySignalRequest, ResetInvocationRequest, TruncateFrom};
    use restate_types::journal_v2::raw::RawCommand;
    use restate_types::journal_v2::{
        ClearAllStateCommand, CommandType, CompletionType, Signal, SignalId, SignalResult,
        SleepCommand,
    };
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
                    ResetInvocationRequest {
                        invocation_id,
                        truncate_from: TruncateFrom::EntryIndex { entry_index },
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

        let actions = test_env
            .apply(restate_wal_protocol::Command::ResetInvocation(
                ResetInvocationRequest {
                    invocation_id,
                    truncate_from: TruncateFrom::EntryIndex { entry_index: 2 },
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
                ResetInvocationRequest {
                    invocation_id,
                    truncate_from: TruncateFrom::EntryIndex { entry_index: 2 },
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
                ResetInvocationRequest {
                    invocation_id,
                    truncate_from: TruncateFrom::EntryIndex { entry_index: 2 },
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
}
