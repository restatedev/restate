// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::journal::schema::SysJournalBuilder;
use bytestring::ByteString;

use restate_service_protocol::codec::ProtobufRawEntryCodec;

use restate_storage_api::journal_table::JournalEntry;
use restate_types::identifiers::{JournalEntryId, WithInvocationId, WithPartitionKey};
use restate_types::journal::enriched::{EnrichedEntryHeader, EnrichedRawEntry};

use crate::table_util::format_using;
use restate_types::journal::{Entry, SleepEntry};

#[inline]
pub(crate) fn append_journal_row(
    builder: &mut SysJournalBuilder,
    output: &mut String,
    journal_entry_id: JournalEntryId,
    journal_entry: JournalEntry,
) {
    let mut row = builder.row();

    row.partition_key(journal_entry_id.partition_key());
    if row.is_id_defined() {
        row.id(format_using(output, &journal_entry_id.invocation_id()));
    }

    row.index(journal_entry_id.journal_index());

    match journal_entry {
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
                EnrichedEntryHeader::Call {
                    enrichment_result: Some(enrichment_result),
                    ..
                }
                | EnrichedEntryHeader::OneWayCall {
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
                EnrichedEntryHeader::GetPromise { .. }
                | EnrichedEntryHeader::PeekPromise { .. }
                | EnrichedEntryHeader::CompletePromise { .. } => {
                    if row.is_promise_name_defined() {
                        if let Some(promise_name) = get_promise_name(&entry) {
                            row.promise_name(promise_name);
                        }
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

fn get_promise_name(entry: &EnrichedRawEntry) -> Option<ByteString> {
    let decoded_entry = entry
        .deserialize_entry_ref::<ProtobufRawEntryCodec>()
        .expect("journal entry must deserialize");

    match decoded_entry {
        Entry::GetPromise(entry) => Some(entry.key),
        Entry::PeekPromise(entry) => Some(entry.key),
        Entry::CompletePromise(entry) => Some(entry.key),
        _ => None,
    }
}
