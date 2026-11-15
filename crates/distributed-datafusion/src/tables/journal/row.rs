// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Journal rows, preserving V1's conversion. Undecodable derived values are logged
//! and left NULL rather than failing the query.

use std::fmt::Display;

use restate_service_protocol::codec::ProtobufRawEntryCodec;
use restate_service_protocol_v4::entry_codec::ServiceProtocolV4Codec;
use restate_storage_api::journal_table::JournalEntry;
use restate_storage_query_api::tables::journal::JournalRowWriter;
use restate_types::identifiers::{
    InvocationId, JournalEntryId, WithInvocationId, WithPartitionKey,
};
use restate_types::journal::enriched::EnrichedEntryHeader;
use restate_types::journal::{CompletePromiseEntry, Entry, GetPromiseEntry, PeekPromiseEntry};
use restate_types::journal_v2::{self, CommandMetadata, Decoder, EntryMetadata};
use restate_types::storage::StoredRawEntry;

fn log_corruption(id: &InvocationId, field: &str, error: impl Display) {
    tracing::error!(
        error = %error,
        "Cannot convert field '{field}' for 'sys_journal' table with row key {id:?}. This might indicate a data corruption problem."
    );
}

fn write_identity(row: &mut JournalRowWriter<'_>, id: JournalEntryId, version: u32) {
    row.version(version);
    row.partition_key(id.partition_key());
    if row.is_id_defined() {
        row.fmt_id(id.invocation_id());
    }
    row.index(id.journal_index());
}

/// An entry of service protocol versions below 4.
pub(super) fn write_v1(row: &mut JournalRowWriter<'_>, id: JournalEntryId, entry: &JournalEntry) {
    write_identity(row, id, 1);
    let invocation_id = id.invocation_id();
    let entry = match entry {
        JournalEntry::Entry(entry) => entry,
        JournalEntry::Completion(_) => {
            row.entry_type("CompletionResult");
            row.completed(true);
            return;
        }
    };
    row.fmt_entry_type(entry.header().as_entry_type());
    if let Some(completed) = entry.header().is_completed() {
        row.completed(completed);
    }
    if row.is_name_defined() {
        match entry.deserialize_name::<ProtobufRawEntryCodec>() {
            Ok(Some(name)) => row.name(name),
            Ok(None) => {}
            Err(error) => log_corruption(&invocation_id, "name", error),
        }
    }
    if row.is_raw_defined() {
        row.raw(entry.serialized_entry().as_ref());
    }
    match entry.header() {
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
        | EnrichedEntryHeader::CompletePromise { .. }
            if row.is_promise_name_defined() =>
        {
            match entry.deserialize_entry_ref::<ProtobufRawEntryCodec>() {
                Ok(Entry::GetPromise(GetPromiseEntry { key, .. }))
                | Ok(Entry::PeekPromise(PeekPromiseEntry { key, .. }))
                | Ok(Entry::CompletePromise(CompletePromiseEntry { key, .. })) => {
                    row.promise_name(key)
                }
                Ok(_) => log_corruption(
                    &invocation_id,
                    "promise_name",
                    "The entry should be a GetPromise, PeekPromise or CompletePromise entry",
                ),
                Err(error) => log_corruption(&invocation_id, "promise_name", error),
            }
        }
        EnrichedEntryHeader::Sleep { .. } if row.is_sleep_wakeup_at_defined() => {
            match entry.deserialize_entry_ref::<ProtobufRawEntryCodec>() {
                Ok(Entry::Sleep(entry)) => row.sleep_wakeup_at(entry.wake_up_time as i64),
                Ok(_) => log_corruption(
                    &invocation_id,
                    "sleep_wakeup",
                    "The entry should be a Sleep entry",
                ),
                Err(error) => log_corruption(&invocation_id, "sleep_wakeup", error),
            }
        }
        _ => {}
    }
}

/// An entry of service protocol version 4 and later.
pub(super) fn write_v2(row: &mut JournalRowWriter<'_>, id: JournalEntryId, entry: &StoredRawEntry) {
    write_identity(row, id, 2);
    let invocation_id = id.invocation_id();
    if row.is_entry_type_defined() {
        row.fmt_entry_type(entry.ty());
    }
    row.appended_at(entry.header.append_time.as_u64() as i64);
    if row.is_raw_defined() || row.is_raw_length_defined() {
        let serialized = entry.inner.serialized_content();
        if row.is_raw_defined() {
            row.raw(serialized.as_ref());
        }
        if row.is_raw_length_defined() {
            row.raw_length(serialized.len() as u64);
        }
    }
    // As in V1, an undecodable entry leaves its remaining derived columns NULL.
    if row.is_entry_lite_json_defined() {
        let Ok(entry_lite) = ServiceProtocolV4Codec::decode_entry_lite(&entry.inner) else {
            log_corruption(&invocation_id, "entry", "The entry should decode correctly");
            return;
        };
        let Ok(json) = serde_json::to_string(&entry_lite) else {
            log_corruption(&invocation_id, "entry", "The entry should decode correctly");
            return;
        };
        row.entry_lite_json(json);
    }
    if row.is_entry_json_defined() || row.is_name_defined() {
        let Ok(decoded) = entry.decode::<ServiceProtocolV4Codec, journal_v2::Entry>() else {
            log_corruption(&invocation_id, "entry", "The entry should decode correctly");
            return;
        };
        if row.is_entry_json_defined()
            && let Ok(json) = serde_json::to_string(&decoded)
        {
            row.entry_json(json);
        }
        if row.is_name_defined()
            && let journal_v2::Entry::Command(command) = decoded
        {
            row.name(command.name());
        }
    }
}
