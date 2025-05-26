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
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::invocation_status_table::InvocationStatus;
use restate_storage_api::journal_table_v2::JournalTable;
use restate_types::identifiers::InvocationId;
use restate_types::journal_v2::raw::RawEntry;
use restate_types::journal_v2::{EntryMetadata, EntryType, Event};

pub(super) struct ApplyEventCommand<'e> {
    pub(super) invocation_id: InvocationId,
    pub(super) invocation_status: &'e mut InvocationStatus,
    pub(super) entry: &'e mut Option<RawEntry>,
}

impl<'e, 'ctx: 'e, 's: 'ctx, S: JournalTable>
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

        let new_entry = self.entry.as_ref().unwrap();

        // If entry types don't match, there's nothing to deduplicate
        if last_entry.inner.try_as_event_ref().map(|e| e.event_type())
            != new_entry.inner.try_as_event_ref().map(|e| e.event_type())
        {
            return Ok(());
        }

        let new_event = new_entry.decode::<ServiceProtocolV4Codec, Event>()?;
        let last_stored_event = last_entry.decode::<ServiceProtocolV4Codec, Event>()?;

        match (last_stored_event, new_event) {
            (
                Event::TransientError(mut stored_transient_error_event),
                Event::TransientError(new_transient_error_event),
            ) if stored_transient_error_event.error_code
                == new_transient_error_event.error_code
                && stored_transient_error_event.error_message
                    == new_transient_error_event.error_message
                && stored_transient_error_event.restate_doc_error_code
                    == new_transient_error_event.restate_doc_error_code
                && stored_transient_error_event.related_command_type
                    == new_transient_error_event.related_command_type
                && stored_transient_error_event.related_command_name
                    == new_transient_error_event.related_command_name
                && stored_transient_error_event.related_command_index
                    == new_transient_error_event.related_command_index =>
            {
                // Override count and error_stacktrace
                stored_transient_error_event.count =
                    stored_transient_error_event.count.saturating_add(1);
                stored_transient_error_event.error_stacktrace =
                    new_transient_error_event.error_stacktrace;

                ctx.storage
                    .put_journal_entry(
                        self.invocation_id,
                        last_entry_index,
                        &Event::TransientError(stored_transient_error_event)
                            .encode::<ServiceProtocolV4Codec>(),
                        &[],
                    )
                    .await?;

                *self.entry = None;
            }
            _ => {}
        };

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU32;

    use crate::partition::state_machine::tests::{TestEnv, fixtures, matchers};
    use googletest::prelude::*;
    use restate_storage_api::invocation_status_table::ReadOnlyInvocationStatusTable;
    use restate_types::journal_v2::TransientErrorEvent;

    #[restate_core::test]
    async fn store_event_then_get_deduplicated() {
        let mut test_env = TestEnv::create().await;
        let invocation_id = fixtures::mock_start_invocation(&mut test_env).await;
        fixtures::mock_pinned_deployment_v5(&mut test_env, invocation_id).await;

        let mut transient_error_event = TransientErrorEvent {
            error_code: 501u16.into(),
            error_message: "my bad".to_string(),
            error_stacktrace: Some("something something".to_string()),
            restate_doc_error_code: Some("RT0001".to_string()),
            related_command_index: None,
            related_command_name: Some("my command".to_string()),
            related_command_type: None,
            count: NonZeroU32::new(1).unwrap(),
        };

        let _ = test_env
            .apply(fixtures::invoker_entry_effect(
                invocation_id,
                Event::TransientError(transient_error_event.clone()),
            ))
            .await;

        assert_eq!(
            test_env.read_journal_entry::<Event>(invocation_id, 1).await,
            Event::TransientError(transient_error_event.clone())
        );

        // --- Applying the same event the second time will result in bumping the count

        let _ = test_env
            .apply(fixtures::invoker_entry_effect(
                invocation_id,
                Event::TransientError(transient_error_event.clone()),
            ))
            .await;

        transient_error_event.count = transient_error_event.count.saturating_add(1);

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
            count: NonZeroU32::new(1).unwrap(),
        };

        let _ = test_env
            .apply(fixtures::invoker_entry_effect(
                invocation_id,
                Event::TransientError(another_transient_error_event.clone()),
            ))
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
