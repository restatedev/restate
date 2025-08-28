// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
use tracing::trace;

use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::invocation_status_table::{InFlightInvocationMetadata, InvocationStatus};
use restate_storage_api::journal_table_v2::ReadOnlyJournalTable;
use restate_tracing_instrumentation as instrumentation;
use restate_types::identifiers::InvocationId;
use restate_types::journal_v2::raw::RawNotification;
use restate_types::journal_v2::{Command, CommandType, CompletionId, NotificationId};

use crate::partition::state_machine::lifecycle::ResumeInvocationCommand;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};

pub(super) struct ApplyNotificationCommand<'e> {
    pub(super) invocation_id: InvocationId,
    pub(super) invocation_status: &'e mut InvocationStatus,
    pub(super) entry: &'e RawNotification,
}

impl<'e> ApplyNotificationCommand<'e> {
    async fn create_trace_span<'s, S>(
        &self,
        ctx: &'_ mut StateMachineApplyContext<'s, S>,
        completion_id: CompletionId,
        invocation_metadata: &InFlightInvocationMetadata,
    ) -> Result<(), Error>
    where
        S: ReadOnlyJournalTable,
    {
        let (header, command) = ctx.storage
                    .get_command_by_completion_id(self.invocation_id, completion_id)
                    .await?.expect("For given completion id, the corresponding command must be present already in the journal");

        if !instrumentation::is_service_tracing_enabled()
            || !invocation_metadata
                .journal_metadata
                .span_context
                .is_sampled()
            || !matches!(
                command.command_type(),
                CommandType::Run | CommandType::Sleep | CommandType::GetPromise
            )
        {
            return Ok(());
        }

        let cmd = command.decode::<ServiceProtocolV4Codec, Command>()?;

        trace!("Received notification for completion id {completion_id} of command {cmd:?}");

        match cmd {
            Command::Run(run) => {
                let _span = instrumentation::info_invocation_span!(
                    relation = invocation_metadata
                        .journal_metadata
                        .span_context
                        .as_parent(),
                    id = self.invocation_id,
                    name = format!("run ({})", run.name),
                    tags = (rpc.service = invocation_metadata
                        .invocation_target
                        .service_name()
                        .to_string()),
                    fields = (with_start_time = header.append_time)
                );
            }
            Command::Sleep(_) => {
                let _span = instrumentation::info_invocation_span!(
                    relation = invocation_metadata
                        .journal_metadata
                        .span_context
                        .as_parent(),
                    id = self.invocation_id,
                    name = "sleep",
                    tags = (rpc.service = invocation_metadata
                        .invocation_target
                        .service_name()
                        .to_string()),
                    fields = (with_start_time = header.append_time)
                );
            }
            Command::GetPromise(get_promise) => {
                let _span = instrumentation::info_invocation_span!(
                    relation = invocation_metadata
                        .journal_metadata
                        .span_context
                        .as_parent(),
                    id = self.invocation_id,
                    name = format!("get-promise ({})", get_promise.completion_id),
                    tags = (rpc.service = invocation_metadata
                        .invocation_target
                        .service_name()
                        .to_string()),
                    fields = (with_start_time = header.append_time)
                );
            }
            _ => unreachable!("Invalid matches! filter"),
        }

        Ok(())
    }
}

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for ApplyNotificationCommand<'e>
where
    S: ReadOnlyJournalTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        if ctx.is_leader {
            let invocation_metadata = self
                .invocation_status
                .get_invocation_metadata()
                .expect("In-Flight invocation metadata must be present");

            match self.entry.id() {
                NotificationId::CompletionId(completion_id) => {
                    self.create_trace_span(ctx, completion_id, invocation_metadata)
                        .await?;
                }
                NotificationId::SignalIndex(index) => {
                    let _span = instrumentation::info_invocation_span!(
                        relation = invocation_metadata
                            .journal_metadata
                            .span_context
                            .as_parent(),
                        id = self.invocation_id,
                        name = format!("awakeable ({})", index),
                        tags = (rpc.service = invocation_metadata
                            .invocation_target
                            .service_name()
                            .to_string())
                    );
                }
                NotificationId::SignalName(_name) => {
                    // todo(slinky): add span for signal name
                }
            }
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
