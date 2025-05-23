// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(dead_code)]

use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::invocation_status_table::InvocationStatus;
use restate_storage_api::journal_table_v2::JournalTable;
use restate_types::identifiers::InvocationId;
use restate_types::journal_v2::Event;
use restate_types::journal_v2::raw::RawEntry;

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
        let new_event = self
            .entry
            .as_ref()
            .unwrap()
            .decode::<ServiceProtocolV4Codec, Event>()?;
        let Some(last_entry) = ctx
            .storage
            .get_journal_entry(self.invocation_id, last_entry_index)
            .await?
        else {
            return Ok(());
        };
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
                stored_transient_error_event.count += 1;
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
