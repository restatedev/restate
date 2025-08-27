// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};
use restate_storage_api::invocation_status_table::{
    InvocationStatus, InvocationStatusTable, JournalMetadata,
};
use restate_storage_api::journal_events::{JournalEventsTable, StoredEvent};
use restate_types::identifiers::InvocationId;
use restate_types::journal_events::raw::RawEvent;
use tracing::trace;

pub struct OnInvokerEventCommand {
    pub invocation_id: InvocationId,
    pub invocation_status: InvocationStatus,
    pub event: RawEvent,
}

impl<'ctx, 's: 'ctx, S: JournalEventsTable + InvocationStatusTable>
    CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>> for OnInvokerEventCommand
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let Self {
            invocation_id,
            mut invocation_status,
            event,
        } = self;
        ApplyEventCommand {
            invocation_id,
            invocation_status: &mut invocation_status,
            event,
        }
        .apply(ctx)
        .await?;

        // Store invocation status
        ctx.storage
            .put_invocation_status(&invocation_id, &invocation_status)
            .await
            .map_err(Error::Storage)?;

        Ok(())
    }
}

pub(super) struct ApplyEventCommand<'e> {
    pub(super) invocation_id: InvocationId,
    pub(super) invocation_status: &'e mut InvocationStatus,
    pub(super) event: RawEvent,
}

impl ApplyEventCommand<'_> {
    async fn should_store_event<'e, 's: 'e, S: JournalEventsTable>(
        journal_metadata: &JournalMetadata,
        invocation_id: InvocationId,
        new_event: &RawEvent,
        storage: &'s mut S,
    ) -> Result<bool, Error> {
        // --- Event deduplication logic
        // Events get duplicated to avoid storing too many duplicate events.
        if new_event.deduplication_hash().is_none() {
            return Ok(true);
        }
        if journal_metadata.events == 0 {
            return Ok(true);
        }

        let last_event_index = journal_metadata.events - 1;
        let Some(last_event) = storage
            .get_journal_event(invocation_id, last_event_index)
            .await?
        else {
            return Ok(true);
        };

        Ok(!(last_event.event.ty() == new_event.ty()
            && last_event.event.deduplication_hash() == new_event.deduplication_hash()))
    }
}
impl<'e, 'ctx: 'e, 's: 'ctx, S: JournalEventsTable>
    CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>> for ApplyEventCommand<'e>
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        let Some(journal_metadata) = self.invocation_status.get_journal_metadata_mut() else {
            // No journal, no events
            return Ok(());
        };

        if !Self::should_store_event(
            journal_metadata,
            self.invocation_id,
            &self.event,
            ctx.storage,
        )
        .await?
        {
            trace!(
                "Deduplicating event {} because the last entry in the journal is an event, and its deduplication hash matches with the current event",
                self.event.ty()
            );
            return Ok(());
        }

        // To store the event, we need to give it a total order wrt journal.
        let after_journal_entry_index = journal_metadata.length.checked_sub(1).unwrap_or_default();

        // Store event
        ctx.storage
            .put_journal_event(
                self.invocation_id,
                journal_metadata.events,
                &StoredEvent {
                    append_time: ctx.record_created_at,
                    after_journal_entry_index,
                    event: self.event,
                },
            )
            .await?;

        // Update events length
        journal_metadata.events += 1;

        // Update timestamps
        if let Some(timestamps) = self.invocation_status.get_timestamps_mut() {
            timestamps.update(ctx.record_created_at);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::partition::state_machine::tests::{TestEnv, fixtures, matchers};
    use crate::partition::types::InvokerEffectKind;
    use googletest::prelude::*;
    use restate_invoker_api::Effect;
    use restate_storage_api::invocation_status_table::ReadOnlyInvocationStatusTable;
    use restate_storage_api::journal_events::ReadOnlyJournalEventsTable;
    use restate_types::journal_events::raw::RawEvent;
    use restate_types::journal_events::{Event, TransientErrorEvent};
    use restate_wal_protocol::Command;

    #[restate_core::test]
    async fn store_event_then_get_deduplicated() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        let transient_error_event = TransientErrorEvent {
            error_code: 501u16.into(),
            error_message: "my bad".to_string(),
            error_stacktrace: Some("something something".to_string()),
            restate_doc_error_code: Some("RT0001".to_string()),
            related_command_index: None,
            related_command_name: Some("my command".to_string()),
            related_command_type: None,
        };

        let deduplication_hash = transient_error_event.deduplication_hash();
        let mut raw_event = RawEvent::from(Event::TransientError(transient_error_event.clone()));
        raw_event.set_deduplication_hash(deduplication_hash);

        let _ = test_env
            .apply(Command::InvokerEffect(Box::new(Effect {
                invocation_id,
                invocation_epoch: 0,
                kind: InvokerEffectKind::JournalEvent {
                    event: raw_event.clone(),
                },
            })))
            .await;

        assert_eq!(
            test_env.read_journal_event(invocation_id, 0).await,
            Event::TransientError(transient_error_event.clone())
        );

        // --- Applying the same event the second time will result in deduplication

        let _ = test_env
            .apply(Command::InvokerEffect(Box::new(Effect {
                invocation_id,
                invocation_epoch: 0,
                kind: InvokerEffectKind::JournalEvent { event: raw_event },
            })))
            .await;

        assert_that!(
            test_env.storage.get_invocation_status(&invocation_id).await,
            ok(matchers::storage::has_events(1))
        );
        assert_eq!(
            test_env.read_journal_event(invocation_id, 0).await,
            Event::TransientError(transient_error_event.clone())
        );
        assert_that!(
            test_env
                .storage
                .get_journal_event(invocation_id, 1)
                .await
                .unwrap(),
            none()
        );

        // --- Applying a new event will result in a new entry

        let another_transient_error_event = TransientErrorEvent {
            error_code: 502u16.into(),
            error_message: "my bad 2".to_string(),
            error_stacktrace: Some("something something".to_string()),
            restate_doc_error_code: Some("RT0001".to_string()),
            related_command_index: None,
            related_command_name: Some("my command 2".to_string()),
            related_command_type: None,
        };

        let deduplication_hash = another_transient_error_event.deduplication_hash();
        let mut another_raw_event =
            RawEvent::from(Event::TransientError(another_transient_error_event.clone()));
        another_raw_event.set_deduplication_hash(deduplication_hash);

        let _ = test_env
            .apply(Command::InvokerEffect(Box::new(Effect {
                invocation_id,
                invocation_epoch: 0,
                kind: InvokerEffectKind::JournalEvent {
                    event: another_raw_event,
                },
            })))
            .await;

        assert_that!(
            test_env.storage.get_invocation_status(&invocation_id).await,
            ok(matchers::storage::has_events(2))
        );
        // Previous event didn't change
        assert_eq!(
            test_env.read_journal_event(invocation_id, 0).await,
            Event::TransientError(transient_error_event)
        );
        assert_eq!(
            test_env.read_journal_event(invocation_id, 1).await,
            Event::TransientError(another_transient_error_event.clone())
        );

        test_env.shutdown().await;
    }
}
