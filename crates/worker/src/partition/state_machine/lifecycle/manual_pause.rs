// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_storage_api::invocation_status_table::{
    InvocationStatus, ReadInvocationStatusTable, WriteInvocationStatusTable,
};
use restate_storage_api::journal_events::WriteJournalEventsTable;
use restate_storage_api::lock_table::WriteLockTable;
use restate_storage_api::vqueue_table::{ReadVQueueTable, WriteVQueueTable};
use restate_types::identifiers::InvocationId;
use restate_types::invocation::InvocationMutationResponseSink;
use restate_types::invocation::client::PauseInvocationResponse;
use restate_types::journal_events::raw::RawEvent;
use restate_types::journal_events::{Event, PausedEvent};

use crate::partition::state_machine::lifecycle::pause_invocation;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};

/// Applies a user-requested pause that was proposed to the log as a
/// [`commands::PauseInvocationCommand`](restate_wal_protocol::v2::commands::PauseInvocationCommand).
///
/// Unlike the invoker-initiated [`OnPausedCommand`] (auto/error pause), this is driven by the
/// partition processor itself, so it works regardless of whether the invoker currently holds an
/// in-flight state machine. It classifies the current status, performs the pause for the cases it
/// supports, and replies to the originating RPC.
pub struct OnManualPauseCommand {
    pub invocation_id: InvocationId,
    pub response_sink: Option<InvocationMutationResponseSink>,
}

impl<'ctx, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for OnManualPauseCommand
where
    S: ReadInvocationStatusTable
        + WriteInvocationStatusTable
        + WriteJournalEventsTable
        + WriteVQueueTable
        + ReadVQueueTable
        + WriteLockTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let OnManualPauseCommand {
            invocation_id,
            response_sink,
        } = self;

        let response = match ctx.get_invocation_status(&invocation_id).await? {
            InvocationStatus::Invoked(metadata) => {
                // A user-requested pause carries no failure, so synthesize an empty PausedEvent.
                pause_invocation(
                    ctx,
                    &invocation_id,
                    metadata,
                    RawEvent::from(Event::Paused(PausedEvent { last_failure: None })),
                )
                .await?;

                // Unlike the invoker-initiated pause, the invoker may still be running this
                // attempt (it didn't drive the pause), so tell it to abort. Any straggler effect
                // from that attempt is fenced by the leader's write-time fencing-token check.
                ctx.send_abort_invocation_to_invoker(invocation_id);

                PauseInvocationResponse::Accepted
            }
            InvocationStatus::Suspended { metadata, .. } => {
                // A suspended invocation has no live invoker task, so there is nothing to abort.
                // `awaiting_on` is intentionally dropped: resuming a paused invocation re-invokes
                // it, replaying the journal so the SDK re-derives what it is waiting on.
                pause_invocation(
                    ctx,
                    &invocation_id,
                    metadata,
                    RawEvent::from(Event::Paused(PausedEvent { last_failure: None })),
                )
                .await?;

                PauseInvocationResponse::Accepted
            }
            InvocationStatus::Paused(_) => PauseInvocationResponse::AlreadyPaused,
            InvocationStatus::Scheduled(_)
            | InvocationStatus::Inboxed(_)
            | InvocationStatus::Completed(_) => PauseInvocationResponse::NotRunning,
            InvocationStatus::Free => PauseInvocationResponse::NotFound,
        };

        ctx.reply_to_pause_invocation(response_sink, response);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::partition::state_machine::Action;
    use crate::partition::state_machine::tests::fixtures::{
        invoker_entry_effect, invoker_suspended,
    };
    use crate::partition::state_machine::tests::{TestEnv, fixtures, matchers};
    use googletest::prelude::{all, assert_that, contains, eq, not, pat};
    use restate_storage_api::invocation_status_table::{
        InvocationStatusDiscriminants, ReadInvocationStatusTable,
    };
    use restate_types::identifiers::PartitionProcessorRpcRequestId;
    use restate_types::journal_v2::{NotificationId, SleepCommand};
    use restate_wal_protocol::v2::{Command, commands};
    use std::time::{Duration, SystemTime};

    fn pause_command(
        invocation_id: InvocationId,
        request_id: PartitionProcessorRpcRequestId,
    ) -> restate_wal_protocol::v2::Envelope<restate_wal_protocol::v2::Raw> {
        commands::PauseInvocationCommand::test_envelope(commands::PauseInvocationCommand {
            invocation_id,
            request_id: Some(request_id),
        })
    }

    #[restate_core::test]
    async fn pause_invoked_invocation() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;

        let request_id = PartitionProcessorRpcRequestId::new();
        let actions = test_env
            .apply(pause_command(invocation_id, request_id))
            .await;

        assert_that!(
            actions,
            all!(
                contains(pat!(Action::ForwardPauseInvocationResponse {
                    request_id: eq(request_id),
                    response: eq(PauseInvocationResponse::Accepted)
                })),
                // The invoker must be told to abort the still-running attempt.
                contains(matchers::actions::abort_for_id(invocation_id)),
            )
        );
        assert_that!(
            test_env
                .storage
                .get_invocation_status(&invocation_id)
                .await
                .unwrap(),
            matchers::storage::is_variant(InvocationStatusDiscriminants::Paused)
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn pause_suspended_invocation() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        // Suspend the invocation on a sleep.
        let completion_id = 1;
        let _ = test_env
            .apply_multiple([
                invoker_entry_effect(
                    invocation_id,
                    SleepCommand {
                        wake_up_time: (SystemTime::now() + Duration::from_secs(60)).into(),
                        name: Default::default(),
                        completion_id,
                    },
                ),
                invoker_suspended(invocation_id, NotificationId::for_completion(completion_id)),
            ])
            .await;
        assert_that!(
            test_env
                .storage
                .get_invocation_status(&invocation_id)
                .await
                .unwrap(),
            matchers::storage::is_variant(InvocationStatusDiscriminants::Suspended)
        );

        // Pausing a suspended invocation transitions it to Paused without aborting the invoker
        // (there is no live task to abort).
        let request_id = PartitionProcessorRpcRequestId::new();
        let actions = test_env
            .apply(pause_command(invocation_id, request_id))
            .await;
        assert_that!(
            actions,
            all!(
                contains(pat!(Action::ForwardPauseInvocationResponse {
                    request_id: eq(request_id),
                    response: eq(PauseInvocationResponse::Accepted)
                })),
                not(contains(matchers::actions::abort_for_id(invocation_id))),
            )
        );
        assert_that!(
            test_env
                .storage
                .get_invocation_status(&invocation_id)
                .await
                .unwrap(),
            matchers::storage::is_variant(InvocationStatusDiscriminants::Paused)
        );

        test_env.shutdown().await;
    }

    #[restate_core::test]
    async fn pause_unknown_invocation_replies_not_found() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = InvocationId::mock_random();

        let request_id = PartitionProcessorRpcRequestId::new();
        let actions = test_env
            .apply(pause_command(invocation_id, request_id))
            .await;

        assert_that!(
            actions,
            contains(pat!(Action::ForwardPauseInvocationResponse {
                request_id: eq(request_id),
                response: eq(PauseInvocationResponse::NotFound)
            }))
        );

        test_env.shutdown().await;
    }
}
