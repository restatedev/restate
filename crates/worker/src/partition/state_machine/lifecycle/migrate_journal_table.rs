// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::partition::state_machine::{CommandHandler, Error, StateMachineApplyContext};
use assert2::let_assert;
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::invocation_status_table::InFlightInvocationMetadata;
use restate_storage_api::{journal_table as journal_table_v1, journal_table_v2};
use restate_types::identifiers::InvocationId;
use restate_types::journal as journal_v1;
use restate_types::journal_v2::Entry;
use restate_types::journal_v2::command::InputCommand;
use restate_types::storage::{StoredRawEntry, StoredRawEntryHeader};

pub struct VerifyOrMigrateJournalTableToV2Command<'e> {
    pub invocation_id: InvocationId,
    pub metadata: &'e mut InFlightInvocationMetadata,
}

impl<'e, 'ctx: 'e, 's: 'ctx, S> CommandHandler<&'ctx mut StateMachineApplyContext<'s, S>>
    for VerifyOrMigrateJournalTableToV2Command<'e>
where
    S: journal_table_v1::WriteJournalTable
        + journal_table_v1::ReadJournalTable
        + journal_table_v2::WriteJournalTable
        + journal_table_v2::ReadJournalTable,
{
    async fn apply(self, ctx: &'ctx mut StateMachineApplyContext<'s, S>) -> Result<(), Error> {
        // Check if we need to perform journal table migrations!
        if self.metadata.journal_metadata.length == 1 {
            // This contains only the input entry, we can run a migration
            if let Some(old_journal_entry) = journal_table_v1::ReadJournalTable::get_journal_entry(
                ctx.storage,
                &self.invocation_id,
                0,
            )
            .await?
            {
                // Extract the old entry, it must be an input entry!
                let_assert!(journal_table_v1::JournalEntry::Entry(old_entry) = old_journal_entry);
                let_assert!(
                    journal_v1::Entry::Input(journal_v1::InputEntry { headers, value }) =
                        old_entry.deserialize_entry_ref::<ProtobufRawEntryCodec>()?
                );

                // Prepare the new entry
                let new_entry: Entry = InputCommand {
                    headers,
                    payload: value,
                    name: Default::default(),
                }
                .into();
                let new_raw_entry = new_entry.encode::<ServiceProtocolV4Codec>();

                // Now write the entry in the new table, and remove it from the old one
                journal_table_v2::WriteJournalTable::put_journal_entry(
                    ctx.storage,
                    self.invocation_id,
                    0,
                    &StoredRawEntry::new(
                        StoredRawEntryHeader::new(ctx.record_created_at),
                        new_raw_entry,
                    ),
                    &[],
                )?;
                journal_table_v1::WriteJournalTable::delete_journal(
                    ctx.storage,
                    &self.invocation_id,
                    1,
                )
                .map_err(Error::Storage)?;

                // Mutate the journal metadata commands
                self.metadata.journal_metadata.commands = 1;
            } else {
                // We're already in journal table v2, nothing to migrate!!!
            }
        } else if self.metadata.journal_metadata.length >= 1 {
            // We can just check corruption here.
            // Length can be greater than 1 when we have either Completions (in the old table) or Notifications (in the new table).
            // Because of the different Awakeable id format, we cannot incur in the situation where we write to the old table for a Completion arrived before the pinned deployment.
            assert!(
                journal_table_v2::ReadJournalTable::get_journal_entry(
                    ctx.storage,
                    self.invocation_id,
                    0,
                )
                .await?
                .is_some(),
                "We expect the JournalTable is V2, but it's currently V1 instead. This is a bug, please contact the developers."
            )
        }

        Ok(())
    }
}
