// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::journal::schema::SysJournalBuilder;
use crate::log_data_corruption_error;
use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::journal_table::JournalEntry;
use restate_types::identifiers::{JournalEntryId, WithInvocationId, WithPartitionKey};
use restate_types::journal::Entry;
use restate_types::journal::enriched::EnrichedEntryHeader;
use restate_types::journal::{CompletePromiseEntry, GetPromiseEntry, PeekPromiseEntry};
use restate_types::journal_v2;
use restate_types::journal_v2::EntryMetadata;
use restate_types::journal_v2::{CommandMetadata, Decoder};
use restate_types::storage::StoredRawEntry;

#[inline]
pub(crate) fn append_journal_row(
    builder: &mut SysJournalBuilder,
    journal_entry_id: JournalEntryId,
    journal_entry: JournalEntry,
) {
    let mut row = builder.row();
    row.version(1);

    row.partition_key(journal_entry_id.partition_key());
    if row.is_id_defined() {
        row.fmt_id(journal_entry_id.invocation_id());
    }

    row.index(journal_entry_id.journal_index());

    match journal_entry {
        JournalEntry::Entry(entry) => {
            row.fmt_entry_type(entry.header().as_entry_type());

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
                        row.fmt_invoked_id(enrichment_result.invocation_id);
                    }
                    if row.is_invoked_target_defined() {
                        row.fmt_invoked_target(&enrichment_result.invocation_target);
                    }
                }
                EnrichedEntryHeader::GetPromise { .. }
                | EnrichedEntryHeader::PeekPromise { .. }
                | EnrichedEntryHeader::CompletePromise { .. } => {
                    if row.is_promise_name_defined() {
                        match entry.deserialize_entry_ref::<ProtobufRawEntryCodec>() {
                            Ok(Entry::GetPromise(GetPromiseEntry { key, .. }))
                            | Ok(Entry::PeekPromise(PeekPromiseEntry { key, .. }))
                            | Ok(Entry::CompletePromise(CompletePromiseEntry { key, .. })) => {
                                row.promise_name(key)
                            }
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
                            ),
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

#[inline]
pub(crate) fn append_journal_row_v2(
    builder: &mut SysJournalBuilder,
    journal_entry_id: JournalEntryId,
    raw_entry: StoredRawEntry,
) {
    let mut row = builder.row();
    row.version(2);

    row.partition_key(journal_entry_id.partition_key());
    if row.is_id_defined() {
        row.fmt_id(journal_entry_id.invocation_id());
    }

    row.index(journal_entry_id.journal_index());
    if row.is_entry_type_defined() {
        row.fmt_entry_type(raw_entry.ty());
    }

    row.appended_at(raw_entry.header().append_time.as_u64() as i64);

    if row.is_entry_lite_json_defined() {
        // We need to parse the entry
        let Ok(entry_lite) = ServiceProtocolV4Codec::decode_entry_lite(&raw_entry.inner) else {
            log_data_corruption_error!(
                "sys_journal",
                &journal_entry_id.invocation_id(),
                "entry",
                "The entry should decode correctly"
            );
            return;
        };
        let Ok(entry_lite_json) = serde_json::to_string(&entry_lite) else {
            log_data_corruption_error!(
                "sys_journal",
                &journal_entry_id.invocation_id(),
                "entry",
                "The entry should decode correctly"
            );
            return;
        };
        row.entry_lite_json(entry_lite_json);
    }

    if row.is_entry_json_defined() || row.is_name_defined() {
        // We need to parse the entry
        let Ok(entry) = raw_entry.decode::<ServiceProtocolV4Codec, journal_v2::Entry>() else {
            log_data_corruption_error!(
                "sys_journal",
                &journal_entry_id.invocation_id(),
                "entry",
                "The entry should decode correctly"
            );
            return;
        };

        if row.is_entry_json_defined()
            && let Ok(json) = serde_json::to_string(&entry)
        {
            row.entry_json(json);
        }
        if row.is_name_defined()
            && let journal_v2::Entry::Command(cmd) = entry
        {
            row.name(cmd.name());
        }
    }
}
