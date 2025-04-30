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

pub struct OnTrimCommand {
    pub invocation_id: InvocationId,
    pub invocation_status: InvocationStatus,
    pub trim_point_command_entry_index: EntryIndex,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>> for OnTrimCommand
where
    S: JournalTable + InvocationStatusTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let OnTrimCommand {
            invocation_id,
            invocation_status,
            trim_point_command_entry_index,
        } = self;

        let is_invoked = matches!(invocation_status, InvocationStatus::Invoked(_));
        let Some(mut in_flight_invocation_metadata) = invocation_status.into_invocation_metadata()
        else {
            info_if_leader!(
                ctx.is_leader,
                "Ignoring trim command because the invocation is not invoked nor suspended"
            );
            return Ok(());
        };

        // Validate the command first. The entry index must correspond to a command index.
        if trim_point_command_entry_index == 0 {
            info_if_leader!(
                ctx.is_leader,
                "Ignoring trim command because index is 0. You can't remove the input entry."
            );
            return Ok(());
        }
        let Some(trim_point_entry) = ctx
            .storage
            .get_journal_entry(invocation_id, trim_point_command_entry_index)
            .await?
        else {
            info_if_leader!(
                ctx.is_leader,
                "Ignoring trim command because the given entry index doesn't exist"
            );
            return Ok(());
        };
        let Ok(trim_point_command) = trim_point_entry.decode::<ServiceProtocolV4Codec, Command>()
        else {
            info_if_leader!(
                ctx.is_leader,
                "Ignoring trim command because the given entry index doesn't correspond to a command entry"
            );
            return Ok(());
        };
        debug_if_leader!(
            ctx.is_leader,
            "Trimming journal starting from {}",
            trim_point_command.ty()
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
        for trim_pointer in
            trim_point_command_entry_index..in_flight_invocation_metadata.journal_metadata.length
        {
            let Some(entry) = ctx
                .storage
                .get_journal_entry(invocation_id, trim_pointer)
                .await?
            else {
                warn!("Missing entry at index {trim_pointer}, this is unexpected");
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

                    // We remove the command
                    trace!("Removing {} at index {}", entry.ty(), trim_pointer);
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
                        notifications_to_retain.push(trim_pointer);
                        trace!(
                            "Retaining Completion {} with id {} at index {}",
                            completion_ty, completion_id, trim_pointer
                        );
                    } else {
                        // We remove this completion as it belongs to a command after (including) the trim point.
                        notification_ids_to_forget
                            .push(NotificationId::CompletionId(completion_id));
                        trace!(
                            "Removing Completion {} with id {} at index {}",
                            completion_ty, completion_id, trim_pointer
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
                        notification_id, trim_pointer
                    );
                    notification_ids_to_forget.push(notification_id);
                }
                EntryType::Event => {
                    // We don't copy signals and events after the trim point!
                    trace!("Removing Event at index {}", trim_pointer);
                }
            }
        }

        // Now let's clean indexes, compact journal and remove remaining entries
        ctx.storage
            .rewrite_journal(
                invocation_id,
                trim_point_command_entry_index,
                &notifications_to_retain,
                &notification_ids_to_forget,
                in_flight_invocation_metadata.journal_metadata.length,
            )
            .await?;

        // Update the epoch, and add the trim point to invocation status
        in_flight_invocation_metadata.current_invocation_epoch += 1;
        if minimum_completion_id_of_removed_commands != CompletionId::MAX {
            in_flight_invocation_metadata
                .completion_range_epoch_map
                .add_trim_point(
                    minimum_completion_id_of_removed_commands,
                    in_flight_invocation_metadata.current_invocation_epoch,
                );
        }

        // Update journal length with the new length
        in_flight_invocation_metadata.journal_metadata.length =
            trim_point_command_entry_index + (notifications_to_retain.len() as u32);

        // Trim procedure done! We're now back in the game
        debug_if_leader!(
            ctx.is_leader,
            restate.journal.length = in_flight_invocation_metadata.journal_metadata.length,
            "Trim completed, resuming"
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
    use restate_types::invocation::{TrimBy, TrimInvocationRequest};
    use restate_types::journal_v2::raw::RawCommand;
    use restate_types::journal_v2::{
        ClearAllStateCommand, CommandType, CompletionType, Entry, SleepCommand,
    };
    use restate_types::time::MillisSinceEpoch;
    use restate_wal_protocol::timer::TimerKeyValue;

    #[restate_core::test]
    async fn trim_empty_journal() {
        let mut test_env = TestEnv::create().await;

        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        for entry_index in 0..=2 {
            // None of these should cause any trim to happen, because either is journal out of bound, or tries to trim input entry, which is special cased.
            let actions = test_env
                .apply(restate_wal_protocol::Command::TrimInvocation(
                    TrimInvocationRequest {
                        invocation_id,
                        trim_by: TrimBy::CommandEntryIndex { entry_index },
                    },
                ))
                .await;
            assert_that!(actions, empty());
        }

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn trim_with_non_completable_entries() {
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
            ok(matchers::storage::has_journal_length(3))
        );

        let actions = test_env
            .apply(restate_wal_protocol::Command::TrimInvocation(
                TrimInvocationRequest {
                    invocation_id,
                    trim_by: TrimBy::CommandEntryIndex { entry_index: 2 },
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
                matchers::storage::has_journal_length(2),
                matchers::storage::in_flight_meta(pat!(InFlightInvocationMetadata {
                    current_invocation_epoch: eq(1),
                    // There were no completable entries among the trimmed ones, so this map should be unchanged.
                    completion_range_epoch_map: eq(CompletionRangeEpochMap::default())
                }))
            ))
        );
        assert_that!(
            test_env.read_journal_to_vec(invocation_id, 2).await,
            elements_are![
                property!(Entry.ty(), eq(EntryType::Command(CommandType::Input))),
                property!(
                    Entry.ty(),
                    eq(EntryType::Command(CommandType::ClearAllState))
                ),
            ]
        );
        assert_that!(
            test_env.storage.get_journal_entry(invocation_id, 2).await,
            ok(none())
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn trim_with_completable_entries() {
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
            .apply(restate_wal_protocol::Command::TrimInvocation(
                TrimInvocationRequest {
                    invocation_id,
                    trim_by: TrimBy::CommandEntryIndex { entry_index: 2 },
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
                    completion_range_epoch_map: eq(CompletionRangeEpochMap::from_trim_points([(
                        2, 1
                    )]))
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
            test_env.storage.get_journal_entry(invocation_id, 3).await,
            ok(none())
        );
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
