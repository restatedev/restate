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

use restate_service_protocol::codec::ProtobufRawEntryCodec;

use restate_storage_api::journal_table::JournalEntry;
use restate_types::identifiers::{JournalEntryId, WithInvocationId, WithPartitionKey};
use restate_types::journal::enriched::EnrichedEntryHeader;
use restate_types::journal::{CompletePromiseEntry, GetPromiseEntry, PeekPromiseEntry};

use crate::log_data_corruption_error;
use crate::table_util::format_using;
use restate_types::journal::Entry;

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
                match entry.deserialize_name::<ProtobufRawEntryCodec>() {
                    Ok(Some(name)) => row.name(name),
                    Err(e) => log_data_corruption_error!(
                        "sys_journal",
                        &journal_entry_id.invocation_id(),
                        "name",
                        e
                    ),
                    _ => {}
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
                        match entry
                            .deserialize_entry_ref::<ProtobufRawEntryCodec>() {
                            Ok(Entry::GetPromise(GetPromiseEntry { key, .. })) |
                            Ok(Entry::PeekPromise(PeekPromiseEntry { key, ..})) |
                            Ok(Entry::CompletePromise(CompletePromiseEntry { key, .. })) =>
                                row.promise_name(key),
                            Ok(_) => log_data_corruption_error!(
                                    "sys_journal",
                                    &journal_entry_id.invocation_id(),
                                    "promise_name",
                                    "The entry should be a GetPromise, PeekPromise or CompletePromise entry"
                                ),
                            Err(e) => log_data_corruption_error!(
                                    "sys_journal",
                                    &journal_entry_id.invocation_id(),
                                    "promise_name",
                                    e
                                )
                        };
                    }
                }
                EnrichedEntryHeader::Sleep { .. } => {
                    if row.is_sleep_wakeup_at_defined() {
                        match entry.deserialize_entry_ref::<ProtobufRawEntryCodec>() {
                            Ok(Entry::Sleep(entry)) => {
                                row.sleep_wakeup_at(entry.wake_up_time as i64)
                            }
                            Ok(_) => log_data_corruption_error!(
                                "sys_journal",
                                &journal_entry_id.invocation_id(),
                                "sleep_wakeup",
                                "The entry should be a Sleep entry"
                            ),
                            Err(e) => log_data_corruption_error!(
                                "sys_journal",
                                &journal_entry_id.invocation_id(),
                                "sleep_wakeup",
                                e
                            ),
                        };
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
