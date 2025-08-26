// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::state_machine::lifecycle::ResumeInvocationCommand;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};
use restate_storage_api::invocation_status_table::InvocationStatus;
use restate_storage_api::journal_table_v2::ReadOnlyJournalTable;
use restate_types::identifiers::InvocationId;
use restate_types::journal_v2::NotificationId;
use restate_types::journal_v2::raw::RawNotification;

pub(super) struct ApplyNotificationCommand<'e> {
    pub(super) invocation_id: InvocationId,
    pub(super) invocation_status: &'e mut InvocationStatus,
    pub(super) entry: &'e RawNotification,
}

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyNotificationCommand<'e>
where
    S: ReadOnlyJournalTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        if cfg!(debug_assertions)
            && let NotificationId::CompletionId(completion_id) = self.entry.id()
        {
            assert!(
                ctx.storage
                    .get_command_by_completion_id(self.invocation_id, completion_id)
                    .await?
                    .is_some(),
                "For given completion id {completion_id}, the corresponding command must be present already in the journal"
            )
        }

        // If we're suspended, let's figure out if we need to resume
        if let InvocationStatus::Suspended {
            waiting_for_notifications,
            ..
        } = self.invocation_status
        {
            if waiting_for_notifications.remove(&self.entry.id()) {
                ResumeInvocationCommand {
                    invocation_id: self.invocation_id,
                    invocation_status: self.invocation_status,
                }
                .apply(ctx)
                .await?;
            }
        } else if let InvocationStatus::Invoked(im) = self.invocation_status {
            // Just forward the notification if we're invoked
            ctx.forward_notification(
                self.invocation_id,
                im.current_invocation_epoch,
                self.entry.clone(),
            );
        }

        // In all the other cases, just move on.

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::partition::state_machine::tests::{TestEnv, fixtures, matchers};
    use googletest::prelude::{assert_that, contains};
    use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
    use restate_storage_api::journal_table_v2::ReadOnlyJournalTable;
    use restate_types::invocation::NotifySignalRequest;
    use restate_types::journal_v2::{Signal, SignalId, SignalResult};
    use restate_wal_protocol::Command;

    #[restate_core::test]
    async fn notify_signal() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        // Send signal notification
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
                signal.clone()
            ))
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn notify_signal_received_before_pinned_deployment() {
        let mut test_env = TestEnv::create().await;
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
                signal.clone()
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
}
