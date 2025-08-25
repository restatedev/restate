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
use crate::partition::state_machine::entries::OnJournalEntryCommand;
use crate::partition::state_machine::{
    CommandHandler, Error, StateMachineApplyContext, should_use_journal_table_v2,
};
use restate_storage_api::fsm_table::FsmTable;
use restate_storage_api::invocation_status_table::{InvocationStatus, InvocationStatusTable};
use restate_storage_api::journal_table as journal_table_v1;
use restate_storage_api::journal_table_v2::JournalTable;
use restate_storage_api::outbox_table::OutboxTable;
use restate_storage_api::promise_table::PromiseTable;
use restate_storage_api::state_table::StateTable;
use restate_storage_api::timer_table::TimerTable;
use restate_types::identifiers::InvocationId;
use restate_types::journal_v2;

pub struct OnPausedCommand {
    pub invocation_id: InvocationId,
    pub paused_event: journal_v2::raw::RawEvent,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnPausedCommand
where
    S: InvocationStatusTable
        + journal_table_v1::JournalTable
        + JournalTable
        + TimerTable
        + FsmTable
        + OutboxTable
        + PromiseTable
        + StateTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let OnPausedCommand {
            invocation_id,
            paused_event,
        } = self;
        let invoked_meta = match ctx.get_invocation_status(&invocation_id).await? {
            InvocationStatus::Invoked(meta) => meta,
            InvocationStatus::Suspended { .. }
            | InvocationStatus::Paused(_)
            | InvocationStatus::Scheduled(_)
            | InvocationStatus::Inboxed(_)
            | InvocationStatus::Completed(_)
            | InvocationStatus::Free => {
                // Nothing to do in these cases, pause gets processed only if the invocation was Invoked.
                return Ok(());
            }
        };

        // Invoker paused the invocation, let's record the event, then set the status to paused
        debug_if_leader!(ctx.is_leader, "Paused the invocation");
        let invocation_status = InvocationStatus::Paused(invoked_meta);

        if should_use_journal_table_v2(&invocation_status) {
            // The OnJournalEntryCommand will deal with writing the status out.
            OnJournalEntryCommand::from_raw_entry(
                invocation_id,
                invocation_status,
                paused_event.into(),
            )
            .apply(ctx)
            .await?;
        } else {
            // If the journal table is not v2, Events are not supported.
            // Simply write out the invocation status in paused, and that's it.
            ctx.storage
                .put_invocation_status(&self.invocation_id, &invocation_status)
                .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::partition::state_machine::tests::{TestEnv, fixtures, matchers};
    use googletest::prelude::*;
    use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
    use restate_storage_api::invocation_status_table::{
        InFlightInvocationMetadata, InvocationStatusDiscriminants, ReadOnlyInvocationStatusTable,
    };
    use restate_types::journal_v2::{Event, PausedEvent, TransientErrorEvent};
    use restate_wal_protocol::Command;

    #[restate_core::test]
    async fn paused_with_pinned_deployment() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        let paused_event = Event::from(PausedEvent {
            last_failure: Some(TransientErrorEvent {
                error_code: 501u16.into(),
                error_message: "my bad".to_string(),
                error_stacktrace: Some("something something".to_string()),
                restate_doc_error_code: Some("RT0001".to_string()),
                related_command_index: None,
                related_command_name: Some("my command".to_string()),
                related_command_type: None,
            }),
        });

        // Check we just pause
        let _ = test_env
            .apply(Command::InvokerEffect(Box::new(
                restate_invoker_api::Effect {
                    invocation_id,
                    invocation_epoch: 0,
                    kind: restate_invoker_api::EffectKind::Paused {
                        paused_event: paused_event
                            .clone()
                            .encode::<ServiceProtocolV4Codec>()
                            .try_as_event()
                            .unwrap(),
                    },
                },
            )))
            .await;
        assert_that!(
            test_env
                .storage
                .get_invocation_status(&invocation_id)
                .await
                .unwrap(),
            all!(
                matchers::storage::is_variant(InvocationStatusDiscriminants::Paused),
                matchers::storage::in_flight_metadata(field!(
                    InFlightInvocationMetadata.pinned_deployment,
                    some(anything())
                ))
            )
        );
        assert_eq!(
            test_env.read_journal_entry::<Event>(invocation_id, 1).await,
            paused_event
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn paused_when_deployment_version_not_set_yet() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;

        let paused_event = Event::from(PausedEvent {
            last_failure: Some(TransientErrorEvent {
                error_code: 501u16.into(),
                error_message: "my bad".to_string(),
                error_stacktrace: Some("something something".to_string()),
                restate_doc_error_code: Some("RT0001".to_string()),
                related_command_index: None,
                related_command_name: Some("my command".to_string()),
                related_command_type: None,
            }),
        })
        .encode::<ServiceProtocolV4Codec>()
        .try_as_event()
        .unwrap();

        // Check we just pause
        let _ = test_env
            .apply(Command::InvokerEffect(Box::new(
                restate_invoker_api::Effect {
                    invocation_id,
                    invocation_epoch: 0,
                    kind: restate_invoker_api::EffectKind::Paused { paused_event },
                },
            )))
            .await;
        assert_that!(
            test_env
                .storage
                .get_invocation_status(&invocation_id)
                .await
                .unwrap(),
            all!(
                matchers::storage::is_variant(InvocationStatusDiscriminants::Paused),
                matchers::storage::in_flight_metadata(field!(
                    InFlightInvocationMetadata.pinned_deployment,
                    none()
                ))
            )
        );

        test_env.shutdown().await;
    }
}
