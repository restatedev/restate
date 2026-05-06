use restate_storage_api::lock_table::WriteLockTable;
// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tracing::debug;

use restate_storage_api::invocation_status_table::InvocationStatus;
use restate_storage_api::vqueue_table::{ReadVQueueTable, WriteVQueueTable};
use restate_types::identifiers::{EntryIndex, InvocationId};
use restate_types::journal_v2::CANCEL_NOTIFICATION_ID;
use restate_types::journal_v2::raw::RawNotification;

use crate::partition::state_machine::lifecycle::ResumeInvocationCommand;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};

pub(super) struct ApplyNotificationCommand<'e> {
    pub(super) invocation_id: InvocationId,
    pub(super) invocation_status: &'e mut InvocationStatus,
    pub(super) entry: &'e RawNotification,
    pub(super) entry_index: EntryIndex,
}

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyNotificationCommand<'e>
where
    S: WriteVQueueTable + ReadVQueueTable + WriteLockTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        // If we're suspended, let's figure out if we need to resume
        match self.invocation_status {
            InvocationStatus::Suspended { awaiting_on, .. } => {
                debug!(
                    "Trying to resolve future '{awaiting_on:?}' with ({}, result: {:?})",
                    self.entry.id(),
                    self.entry.result_variant()
                );

                if awaiting_on.resolve(self.entry) {
                    ResumeInvocationCommand {
                        invocation_id: self.invocation_id,
                        invocation_status: self.invocation_status,
                    }
                    .apply(ctx)
                    .await?;
                }
            }
            InvocationStatus::Invoked(_) => {
                // Just forward the notification if we're invoked
                ctx.forward_notification(self.invocation_id, self.entry_index, self.entry.id());
            }
            InvocationStatus::Paused(_) => {
                // If we're paused, resume only if the notification was a cancellation signal.
                // In the other cases, the user manually retriggers it.
                if self.entry.id() == CANCEL_NOTIFICATION_ID {
                    ResumeInvocationCommand {
                        invocation_id: self.invocation_id,
                        invocation_status: self.invocation_status,
                    }
                    .apply(ctx)
                    .await?;
                }
            }
            InvocationStatus::Scheduled(_)
            | InvocationStatus::Inboxed(_)
            | InvocationStatus::Completed(_)
            | InvocationStatus::Free => {
                // In all the other cases, just move on, nothing to do here.
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::partition::state_machine::tests::{TestEnv, fixtures, matchers};
    use bytes::Bytes;
    use bytestring::ByteString;
    use googletest::prelude::*;
    use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
    use restate_storage_api::invocation_status_table::{
        InvocationStatus, InvocationStatusDiscriminants, ReadInvocationStatusTable,
    };
    use restate_storage_api::journal_table_v2::ReadJournalTable;
    use restate_types::invocation::{
        InvocationTermination, NotifySignalRequest, TerminationFlavor,
    };
    use restate_types::journal_v2::EntryMetadata;
    use restate_types::journal_v2::{
        BuiltInSignal, CommandType, Entry, EntryType, Failure, FailureMetadata, NotificationId,
        Signal, SignalId, SignalResult, SleepCommand, SleepCompletion,
    };
    use restate_types::time::MillisSinceEpoch;
    use restate_types::{RESTATE_VERSION_1_6_0, SemanticRestateVersion};
    use restate_wal_protocol::Command;
    use restate_wal_protocol::timer::TimerKeyValue;
    use rstest::rstest;
    use std::time::Duration;

    #[rstest]
    #[case(SignalResult::Void)]
    #[case(SignalResult::Success(Bytes::from_static(b"123")))]
    #[case(SignalResult::Failure(Failure { code: 512u16.into(), message: ByteString::from_static("my-error"), metadata: vec![FailureMetadata { key: ByteString::from_static("my-type"), value: ByteString::from_static("my-value") }] }))]
    #[restate_core::test]
    async fn notify_signal(#[case] signal_result: SignalResult) {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        // Send signal notification
        let signal = Signal::new(SignalId::for_index(17), signal_result);
        let actions = test_env
            .apply(Command::NotifySignal(NotifySignalRequest {
                invocation_id,
                signal: signal.clone(),
            }))
            .await;
        assert_that!(
            actions,
            contains(matchers::actions::forward_notification(
                invocation_id,
                1,
                NotificationId::SignalIndex(17),
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

    #[restate_core::test]
    async fn notify_signal_received_before_pinned_deployment() {
        run_notify_signal_received_before_pinned_deployment(SemanticRestateVersion::unknown())
            .await;
    }

    #[restate_core::test]
    async fn notify_signal_received_before_pinned_deployment_journal_v2_enabled() {
        run_notify_signal_received_before_pinned_deployment(RESTATE_VERSION_1_6_0.clone()).await;
    }

    async fn run_notify_signal_received_before_pinned_deployment(
        min_restate_version: SemanticRestateVersion,
    ) {
        let mut test_env = TestEnv::create_with_min_restate_version(min_restate_version).await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;

        // Send signal notification before pinned deployment
        let signal = Signal::new(SignalId::for_index(17), SignalResult::Void);
        let actions = test_env
            .apply(Command::NotifySignal(NotifySignalRequest {
                invocation_id,
                signal: signal.clone(),
            }))
            .await;
        assert_that!(
            actions,
            contains(matchers::actions::forward_notification(
                invocation_id,
                1,
                NotificationId::SignalIndex(17),
            ))
        );

        // At this point the journal table v2 should be filled, and journal table v1 empty
        let notification = test_env
            .storage
            .get_journal_entry(invocation_id, 1)
            .await
            .unwrap()
            .unwrap()
            .inner
            .try_as_notification()
            .unwrap();
        assert_eq!(
            notification
                .decode::<ServiceProtocolV4Codec, Signal>()
                .unwrap(),
            signal
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn notify_command_completion_while_paused_remains_paused_and_records_notification() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        // Add sleep command
        let completion_id = 1;
        let wake_up_time = MillisSinceEpoch::now() + Duration::from_secs(10000);
        let _ = test_env
            .apply(fixtures::invoker_entry_effect(
                invocation_id,
                SleepCommand {
                    wake_up_time,
                    completion_id,
                    name: Default::default(),
                },
            ))
            .await;

        // Pause the invocation (mock paused state)
        test_env
            .modify_invocation_status(invocation_id, |invocation_status| {
                *invocation_status = InvocationStatus::Paused(
                    invocation_status
                        .get_invocation_metadata_mut()
                        .unwrap()
                        .clone(),
                )
            })
            .await;

        // Send a completion notification for a command (e.g., Sleep) with completion_id = 1
        let completion_id = 1;
        let _ = test_env
            .apply(Command::Timer(TimerKeyValue::complete_journal_entry(
                wake_up_time,
                invocation_id,
                completion_id,
            )))
            .await;

        // The invocation should remain paused
        assert_that!(
            test_env
                .storage
                .get_invocation_status(&invocation_id)
                .await
                .unwrap(),
            matchers::storage::is_variant(InvocationStatusDiscriminants::Paused)
        );

        // The notification should be recorded in the journal table v2 at index 2
        let notification = test_env
            .read_journal_entry::<SleepCompletion>(invocation_id, 2)
            .await;
        // It must match a completion notification with the same id
        assert_eq!(notification.completion_id, completion_id);

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn cancel_signal_while_paused_resumes_invocation() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        // Pause the invocation (mock paused state)
        test_env
            .modify_invocation_status(invocation_id, |invocation_status| {
                *invocation_status = InvocationStatus::Paused(
                    invocation_status
                        .get_invocation_metadata_mut()
                        .unwrap()
                        .clone(),
                )
            })
            .await;

        // Apply the cancel signal notification
        let actions = test_env
            .apply(Command::TerminateInvocation(InvocationTermination {
                invocation_id,
                flavor: TerminationFlavor::Cancel,
                response_sink: None,
            }))
            .await;

        // The invocation should be resumed (invoke action dispatched)
        assert_that!(
            actions,
            contains(matchers::actions::invoke_for_id(invocation_id))
        );
        assert_that!(
            test_env
                .storage
                .get_invocation_status(&invocation_id)
                .await
                .unwrap(),
            matchers::storage::is_variant(InvocationStatusDiscriminants::Invoked)
        );

        let cancel_signal = test_env
            .read_journal_entry::<Signal>(invocation_id, 1)
            .await;
        assert_eq!(
            cancel_signal.id,
            SignalId::for_builtin_signal(BuiltInSignal::Cancel)
        );

        test_env.shutdown().await;
    }
}
