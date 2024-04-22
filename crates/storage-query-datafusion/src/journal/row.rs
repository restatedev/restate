// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::journal::schema::JournalBuilder;

use restate_service_protocol::codec::ProtobufRawEntryCodec;

use restate_storage_api::journal_table::JournalEntry;
use restate_storage_rocksdb::journal_table::OwnedJournalRow;
use restate_types::identifiers::WithPartitionKey;
use restate_types::journal::enriched::{EnrichedEntryHeader, EnrichedRawEntry};

use crate::table_util::format_using;
use restate_types::journal::{Entry, SleepEntry};

#[inline]
pub(crate) fn append_journal_row(
    builder: &mut JournalBuilder,
    output: &mut String,
    journal_row: OwnedJournalRow,
) {
    let mut row = builder.row();

    row.partition_key(journal_row.invocation_id.partition_key());
    if row.is_id_defined() {
        row.id(format_using(output, &journal_row.invocation_id));
    }

    row.index(journal_row.journal_index);

    match journal_row.journal_entry {
        JournalEntry::Entry(entry) => {
            row.entry_type(format_using(output, &entry.header().as_entry_type()));

            if let Some(completed) = entry.header().is_completed() {
                row.completed(completed);
            }

            if row.is_name_defined() {
                if let Some(name) = entry
                    .deserialize_name::<ProtobufRawEntryCodec>()
                    .expect("journal entry must deserialize")
                {
                    row.name(name);
                }
            }

            if row.is_raw_defined() {
                row.raw(entry.serialized_entry());
            }

            match &entry.header() {
                EnrichedEntryHeader::Invoke {
                    enrichment_result: Some(enrichment_result),
                    ..
                }
                | EnrichedEntryHeader::BackgroundInvoke {
                    enrichment_result, ..
                } => {
                    if row.is_invoked_id_defined() {
                        row.invoked_id(format_using(output, &enrichment_result.invocation_id));
                    }
                    if row.is_invoked_target_defined() {
                        row.invoked_target(format_using(
                            output,
                            &enrichment_result.invocation_target,
                        ));
                    }
                }
                EnrichedEntryHeader::Sleep { .. } => {
                    if row.is_sleep_wakeup_at_defined() {
                        if let Some(sleep_entry) = deserialize_sleep_entry(&entry) {
                            row.sleep_wakeup_at(sleep_entry.wake_up_time as i64);
                        }
                    }
                }
                _ => {}
            }
        }
        JournalEntry::Completion(_completion) => {
            row.entry_type("CompletionResult");
            row.completed(true);
        }
    };
}

fn deserialize_sleep_entry(entry: &EnrichedRawEntry) -> Option<SleepEntry> {
    let decoded_entry = entry
        .deserialize_entry_ref::<ProtobufRawEntryCodec>()
        .expect("journal entry must deserialize");

    debug_assert!(matches!(decoded_entry, Entry::Sleep(_)));
    match decoded_entry {
        Entry::Sleep(entry) => Some(entry),
        _ => None,
    }
}
