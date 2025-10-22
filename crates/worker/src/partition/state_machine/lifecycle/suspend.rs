// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::state_machine::{CommandHandler, Error, ParkCause, StateMachineApplyContext};
use restate_storage_api::invocation_status_table::{InvocationStatus, WriteInvocationStatusTable};
use restate_storage_api::journal_table_v2::ReadJournalTable;
use restate_storage_api::vqueue_table::{ReadVQueueTable, WriteVQueueTable};
use restate_types::config::Configuration;
use restate_types::identifiers::InvocationId;
use restate_types::journal_v2::NotificationId;
use std::collections::HashSet;
use tracing::trace;

pub struct OnSuspendCommand {
    pub invocation_id: InvocationId,
    pub invocation_status: InvocationStatus,
    pub waiting_for_notifications: HashSet<NotificationId>,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnSuspendCommand
where
    S: ReadJournalTable + WriteInvocationStatusTable + WriteVQueueTable + ReadVQueueTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        debug_assert!(
            !self.waiting_for_notifications.is_empty(),
            "Expecting at least one entry on which the invocation {} is waiting.",
            self.invocation_id
        );

        // Notifications currently stored
        let available_notifications = ctx
            .storage
            .get_notifications_index(self.invocation_id)
            .await?
            .into_keys()
            .collect::<HashSet<_>>();

        // Find if any new notification is available of the ones the SDK is waiting on
        let mut any_completed = false;
        for notif in &self.waiting_for_notifications {
            if available_notifications.contains(notif) {
                any_completed = true;
                break;
            }
        }

        let mut invocation_status = self.invocation_status;
        if any_completed {
            trace!(
                "Resuming instead of suspending service because a notification is already available."
            );
            super::ResumeInvocationCommand {
                invocation_id: self.invocation_id,
                invocation_status: &mut invocation_status,
            }
            .apply(ctx)
            .await?;
        } else {
            // Let's transition to suspended
            let mut in_flight_invocation_metadata = invocation_status
                .into_invocation_metadata()
                .expect("Must be present unless status is killed or invoked");

            trace!(
                "Suspending invocation waiting for notifications {:?}",
                self.waiting_for_notifications
            );

            in_flight_invocation_metadata
                .timestamps
                .update(ctx.record_created_at);

            if Configuration::pinned().common.experimental_enable_vqueues {
                ctx.vqueue_park_invocation(
                    &self.invocation_id,
                    &in_flight_invocation_metadata.invocation_target,
                    ParkCause::Suspend,
                )
                .await?;
            }

            invocation_status = InvocationStatus::Suspended {
                metadata: in_flight_invocation_metadata,
                waiting_for_notifications: self.waiting_for_notifications,
            };
        }

        // Store invocation status
        ctx.storage
            .put_invocation_status(&self.invocation_id, &invocation_status)
            .map_err(Error::Storage)
    }
}

#[cfg(test)]
mod tests {
    use crate::partition::state_machine::tests::fixtures::{
        invoker_entry_effect, invoker_suspended,
    };
    use crate::partition::state_machine::tests::matchers::storage::{
        has_commands, has_journal_length, in_flight_metadata, is_variant,
    };
    use crate::partition::state_machine::tests::{TestEnv, fixtures, matchers};
    use crate::partition::state_machine::{Action, Feature};
    use enumset::EnumSet;
    use googletest::prelude::*;
    use restate_storage_api::invocation_status_table::{
        InFlightInvocationMetadata, InvocationStatusDiscriminants, ReadInvocationStatusTable,
    };
    use restate_types::invocation::NotifySignalRequest;
    use restate_types::journal_v2::{
        CommandType, Entry, EntryMetadata, EntryType, NotificationId, Signal, SignalId,
        SignalResult, SleepCommand, SleepCompletion,
    };
    use restate_types::time::MillisSinceEpoch;
    use restate_wal_protocol::Command;
    use restate_wal_protocol::timer::TimerKeyValue;
    use rstest::rstest;
    use std::time::{Duration, SystemTime};

    #[restate_core::test]
    async fn sleep_then_suspend_then_resume() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        let completion_id = 1;
        let wake_up_time: MillisSinceEpoch = (SystemTime::now() + Duration::from_secs(60)).into();

        let sleep_command = SleepCommand {
            wake_up_time,
            name: Default::default(),
            completion_id,
        };
        let timer_key_value =
            TimerKeyValue::complete_journal_entry(wake_up_time, invocation_id, completion_id);
        let actions = test_env
            .apply_multiple([
                invoker_entry_effect(invocation_id, sleep_command.clone()),
                invoker_suspended(
                    invocation_id,
                    [NotificationId::for_completion(completion_id)],
                ),
            ])
            .await;
        assert_that!(
            actions,
            contains(pat!(Action::RegisterTimer {
                timer_value: eq(timer_key_value.clone())
            }))
        );

        let actions = test_env.apply(Command::Timer(timer_key_value)).await;
        assert_that!(
            actions,
            contains(matchers::actions::invoke_for_id(invocation_id))
        );

        // Check journal
        let sleep_completion = SleepCompletion { completion_id };
        assert_that!(
            test_env.read_journal_to_vec(invocation_id, 3).await,
            elements_are![
                property!(Entry.ty(), eq(EntryType::Command(CommandType::Input))),
                matchers::entry_eq(sleep_command),
                matchers::entry_eq(sleep_completion),
            ]
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn suspend_with_already_completed_notifications() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        let completion_id = 1;
        let wake_up_time: MillisSinceEpoch = (SystemTime::now() + Duration::from_secs(60)).into();

        let sleep_command = SleepCommand {
            wake_up_time,
            name: Default::default(),
            completion_id,
        };
        let sleep_completion = SleepCompletion { completion_id };
        let timer_key_value =
            TimerKeyValue::complete_journal_entry(wake_up_time, invocation_id, completion_id);
        let actions = test_env
            .apply_multiple([
                invoker_entry_effect(invocation_id, sleep_command.clone()),
                Command::Timer(timer_key_value.clone()),
            ])
            .await;
        assert_that!(
            actions,
            all![
                contains(pat!(Action::RegisterTimer {
                    timer_value: eq(timer_key_value)
                })),
                contains(matchers::actions::forward_notification(
                    invocation_id,
                    sleep_completion.clone()
                ))
            ]
        );

        let actions = test_env
            .apply(invoker_suspended(
                invocation_id,
                [NotificationId::for_completion(completion_id)],
            ))
            .await;
        assert_that!(
            actions,
            contains(matchers::actions::invoke_for_id(invocation_id))
        );

        // Check journal
        assert_that!(
            test_env.read_journal_to_vec(invocation_id, 3).await,
            elements_are![
                property!(Entry.ty(), eq(EntryType::Command(CommandType::Input))),
                matchers::entry_eq(sleep_command),
                matchers::entry_eq(sleep_completion),
            ]
        );

        test_env.shutdown().await;
    }

    #[rstest]
    #[restate_core::test]
    async fn suspend_waiting_on_signal(
        #[values(Feature::UseJournalTableV2AsDefault.into(), EnumSet::empty())] features: EnumSet<
            Feature,
        >,
    ) {
        let mut test_env = TestEnv::create_with_features(features).await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        // We don't pin the deployment here, but this should work nevertheless.

        let _ = test_env
            .apply(invoker_suspended(
                invocation_id,
                [NotificationId::for_signal(SignalId::for_index(17))],
            ))
            .await;

        assert_that!(
            test_env
                .storage()
                .get_invocation_status(&invocation_id)
                .await,
            ok(all!(
                is_variant(InvocationStatusDiscriminants::Suspended),
                has_journal_length(1),
                in_flight_metadata(pat!(InFlightInvocationMetadata {
                    pinned_deployment: none()
                })),
            ))
        );

        // Let's notify the signal
        let signal = Signal {
            id: SignalId::for_index(17),
            result: SignalResult::Void,
        };
        let actions = test_env
            .apply(Command::NotifySignal(NotifySignalRequest {
                invocation_id,
                signal: signal.clone(),
            }))
            .await;
        assert_that!(
            actions,
            contains(matchers::actions::invoke_for_id(invocation_id))
        );

        assert_that!(
            test_env
                .storage()
                .get_invocation_status(&invocation_id)
                .await,
            ok(all!(
                is_variant(InvocationStatusDiscriminants::Invoked),
                has_journal_length(2),
                has_commands(1),
                in_flight_metadata(pat!(InFlightInvocationMetadata {
                    pinned_deployment: none()
                })),
            ))
        );

        // Check journal
        assert_that!(
            test_env.read_journal_to_vec(invocation_id, 2).await,
            elements_are![
                property!(Entry.ty(), eq(EntryType::Command(CommandType::Input))),
                matchers::entry_eq(signal),
            ]
        );

        test_env.shutdown().await;
    }
}
