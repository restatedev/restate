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
use restate_storage_api::invocation_status_table::InvocationStatus;
use restate_storage_api::journal_table_v2::ReadOnlyJournalTable;
use restate_types::identifiers::InvocationId;
use restate_types::journal_v2::raw::RawEntry;
use restate_types::journal_v2::{EntryMetadata, EntryType};
use tracing::trace;

pub(super) struct ApplyEventCommand<'e> {
    pub(super) invocation_id: InvocationId,
    pub(super) invocation_status: &'e mut InvocationStatus,
    pub(super) entry: &'e mut Option<RawEntry>,
}

impl<'e, 'ctx: 'e, 's: 'ctx, S: ReadOnlyJournalTable>
    CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>> for ApplyEventCommand<'e>
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        // --- Event deduplication logic
        // Some Events are merged together when duplicated, to avoid storing too many duplicate events.
        // This logic is completely optional and can fail silently.
        let Some(journal_metadata) = self.invocation_status.get_journal_metadata() else {
            return Ok(());
        };
        let last_entry_index = journal_metadata.length - 1;
        let Some(last_entry) = ctx
            .storage
            .get_journal_entry(self.invocation_id, last_entry_index)
            .await?
        else {
            return Ok(());
        };
        if last_entry.ty() != EntryType::Event {
            return Ok(());
        }

        let last_event = last_entry
            .inner
            .try_as_event_ref()
            .ok_or_else(|| Error::BadEntryVariant(EntryType::Event))?;
        let new_event = self
            .entry
            .as_ref()
            .unwrap()
            .try_as_event_ref()
            .ok_or_else(|| Error::BadEntryVariant(EntryType::Event))?;

        if last_event.event_type() == new_event.event_type()
            && last_event.deduplication_hash() == new_event.deduplication_hash()
        {
            trace!(
                "Deduplicating event {} because the last entry in the journal is an event, and its deduplication hash matches with the current event",
                last_event.event_type()
            );
            *self.entry = None;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::partition::state_machine::tests::{TestEnv, fixtures, matchers};
    use crate::partition::types::InvokerEffectKind;
    use googletest::prelude::*;
    use restate_invoker_api::Effect;
    use restate_storage_api::invocation_status_table::ReadOnlyInvocationStatusTable;
    use restate_types::journal_v2::raw::RawEvent;
    use restate_types::journal_v2::{Event, TransientErrorEvent};
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
        let raw_entry = RawEntry::Event(raw_event);

        let _ = test_env
            .apply(Command::InvokerEffect(Box::new(Effect {
                invocation_id,
                invocation_epoch: 0,
                kind: InvokerEffectKind::journal_entry(raw_entry.clone(), None),
            })))
            .await;

        assert_eq!(
            test_env.read_journal_entry::<Event>(invocation_id, 1).await,
            Event::TransientError(transient_error_event.clone())
        );

        // --- Applying the same event the second time will result in deduplication

        let _ = test_env
            .apply(Command::InvokerEffect(Box::new(Effect {
                invocation_id,
                invocation_epoch: 0,
                kind: InvokerEffectKind::journal_entry(raw_entry, None),
            })))
            .await;

        assert_that!(
            test_env.storage.get_invocation_status(&invocation_id).await,
            ok(matchers::storage::has_journal_length(2))
        );
        assert_eq!(
            test_env.read_journal_entry::<Event>(invocation_id, 1).await,
            Event::TransientError(transient_error_event.clone())
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
                kind: InvokerEffectKind::journal_entry(RawEntry::Event(another_raw_event), None),
            })))
            .await;

        assert_that!(
            test_env.storage.get_invocation_status(&invocation_id).await,
            ok(matchers::storage::has_journal_length(3))
        );
        // Previous event didn't change
        assert_eq!(
            test_env.read_journal_entry::<Event>(invocation_id, 1).await,
            Event::TransientError(transient_error_event)
        );
        assert_eq!(
            test_env.read_journal_entry::<Event>(invocation_id, 2).await,
            Event::TransientError(another_transient_error_event.clone())
        );

        test_env.shutdown().await;
    }
}
