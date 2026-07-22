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

use restate_clock::UniqueTimestamp;
use restate_storage_api::invocation_status_table::{
    InFlightInvocationMetadata, InvocationStatus, ReadInvocationStatusTable,
    WriteInvocationStatusTable,
};
use restate_storage_api::journal_events::WriteJournalEventsTable;
use restate_storage_api::lock_table::WriteLockTable;
use restate_storage_api::vqueue_table::{EntryStatusHeader, ReadVQueueTable, WriteVQueueTable};
use restate_types::identifiers::{InvocationId, WithPartitionKey as _};
use restate_types::journal_events::raw::RawEvent;
use restate_types::vqueues::EntryId;
use restate_vqueues::VQueue;
use restate_vqueues::context::HasVQueuesMut;

use crate::debug_if_leader;
use crate::partition::processor::{Processor, ProcessorContext};
use crate::partition::state_machine::lifecycle::event::ApplyEventCommand;
use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};

pub struct OnPausedCommand<'a> {
    pub invocation_id: &'a InvocationId,
    pub paused_event: RawEvent,
}

impl<'ctx, 's: 'ctx, S, P> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S, P>>
    for OnPausedCommand<'_>
where
    S: ReadInvocationStatusTable
        + WriteInvocationStatusTable
        + WriteJournalEventsTable
        + WriteVQueueTable
        + WriteLockTable
        + ReadVQueueTable,
    P: ProcessorContext,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S, P>) -> Result<(), Error> {
        let OnPausedCommand {
            invocation_id,
            paused_event,
        } = self;
        let invoked_meta = match ctx.get_invocation_status(invocation_id).await? {
            InvocationStatus::Invoked(meta) => meta,
            InvocationStatus::Suspended { .. }
            | InvocationStatus::Paused(_)
            | InvocationStatus::Scheduled(_)
            | InvocationStatus::Inboxed(_)
            | InvocationStatus::Completed(_)
            | InvocationStatus::Free => {
                // Nothing to do in these cases, the invoker-driven pause is only processed if the
                // invocation was Invoked.
                return Ok(());
            }
        };

        // The invoker drove this pause, so it has already stopped working on the invocation; no
        // abort is needed (unlike the manual/persisted pause, see `OnManualPauseCommand`).
        pause_invocation(ctx, invocation_id, invoked_meta, paused_event).await
    }
}

/// Performs the pause transition for an in-flight invocation: parks the VQueue entry (if any),
/// records the paused event in the journal, and stores [`InvocationStatus::Paused`].
///
/// Shared by the invoker-initiated pause ([`OnPausedCommand`]) and the manual/persisted pause
/// ([`super::OnManualPauseCommand`]). It does **not** abort the invoker — callers that drive the
/// pause externally (the manual path) are responsible for that.
pub(crate) async fn pause_invocation<'ctx, 's: 'ctx, S, P>(
    ctx: &'ctx mut StateMachineApplyContext<'s, S, P>,
    invocation_id: &InvocationId,
    metadata: InFlightInvocationMetadata,
    paused_event: RawEvent,
) -> Result<(), Error>
where
    S: WriteInvocationStatusTable
        + WriteJournalEventsTable
        + WriteVQueueTable
        + WriteLockTable
        + ReadVQueueTable,
    P: Processor + HasVQueuesMut,
{
    debug_if_leader!(ctx.is_leader, "Paused the invocation");

    if metadata.vqueue_id.is_some() {
        let entry_id = EntryId::from(invocation_id);
        let Some(header) = ctx
            .storage
            .get_vqueue_entry_status(invocation_id.partition_key(), &entry_id)
            .await?
        else {
            // This is equivalent to InvocationStatus::Free.
            debug!(
                "Trying to pause invocation {invocation_id} which does not exist as a vqueue entry, will ignore."
            );
            return Ok(());
        };

        let at = UniqueTimestamp::from_unix_millis_unchecked(ctx.record_created_at);
        VQueue::get(
            header.vqueue_id(),
            ctx.storage,
            ctx.processor.vqueues_mut(),
            ctx.is_leader.then_some(ctx.action_collector),
        )
        .await?
        .expect("pausing in a non-existent vqueue")
        .pause_entry(at, &header);
    }

    let mut invocation_status = InvocationStatus::Paused(metadata);

    ApplyEventCommand {
        invocation_id,
        invocation_status: &invocation_status,
        event: paused_event,
    }
    .apply(ctx)
    .await?;

    // Update timestamps
    if let Some(timestamps) = invocation_status.get_timestamps_mut() {
        timestamps.update(ctx.record_created_at);
    }

    ctx.storage
        .put_invocation_status(invocation_id, &invocation_status)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use googletest::prelude::*;

    use restate_storage_api::invocation_status_table::{
        InFlightInvocationMetadata, InvocationStatusDiscriminants, ReadInvocationStatusTable,
    };
    use restate_types::journal_events::{Event, PausedEvent, TransientErrorEvent};
    use restate_wal_protocol::v2::{Command, commands};

    use crate::partition::state_machine::tests::{TestEnv, fixtures, matchers};
    use crate::partition::types::InvokerEffectKind;

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
            .apply(commands::InvokerEffectCommand::test_envelope(
                restate_worker_api::invoker::Effect {
                    invocation_id,
                    kind: InvokerEffectKind::Paused {
                        paused_event: paused_event.clone().into(),
                    },
                },
            ))
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
        assert_that!(
            test_env.read_journal_events(&invocation_id).await,
            elements_are![eq(paused_event)]
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
        .into();

        // Check we just pause
        let _ = test_env
            .apply(commands::InvokerEffectCommand::test_envelope(
                restate_worker_api::invoker::Effect {
                    invocation_id,
                    kind: InvokerEffectKind::Paused { paused_event },
                },
            ))
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
