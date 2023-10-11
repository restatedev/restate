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
use crate::udfs::restate_keys;
use std::cell::OnceCell;

use bytestring::ByteString;
use restate_schema_api::key::RestateKeyConverter;
use restate_service_protocol::codec::ProtobufRawEntryCodec;

use restate_storage_api::journal_table::JournalEntry;
use restate_storage_rocksdb::journal_table::OwnedJournalRow;
use restate_types::identifiers::{InvocationId, ServiceId, WithPartitionKey};
use restate_types::journal::enriched::EnrichedEntryHeader;
use restate_types::journal::raw::{EntryHeader, RawEntryCodec};





use crate::table_util::format_using;
use restate_types::journal::{BackgroundInvokeEntry, Entry, InvokeEntry};
use uuid::Uuid;

#[inline]
pub(crate) fn append_journal_row(
    builder: &mut JournalBuilder,
    output: &mut String,
    journal_row: OwnedJournalRow,
    resolver: impl RestateKeyConverter + Clone,
) {
    let mut row = builder.row();

    row.partition_key(journal_row.partition_key);
    row.service(&journal_row.service);
    row.service_key(&journal_row.service_key);
    if row.is_service_key_utf8_defined() {
        if let Some(utf8) = restate_keys::try_decode_restate_key_as_utf8(&journal_row.service_key) {
            row.service_key_utf8(utf8);
        }
    }
    if row.is_service_key_int32_defined() {
        if let Some(key) = restate_keys::try_decode_restate_key_as_int32(&journal_row.service_key) {
            row.service_key_int32(key);
        }
    }
    if row.is_service_key_uuid_defined() {
        let mut buffer = Uuid::encode_buffer();
        if let Some(key) =
            restate_keys::try_decode_restate_key_as_uuid(&journal_row.service_key, &mut buffer)
        {
            row.service_key_uuid(key);
        }
    }
    if row.is_service_key_json_defined() {
        if let Some(key) = restate_keys::try_decode_restate_key_as_json(
            &journal_row.service,
            &journal_row.service_key,
            output,
            resolver.clone(),
        ) {
            row.service_key_json(key);
        }
    }

    row.index(journal_row.journal_index);

    match journal_row.journal_entry {
        JournalEntry::Entry(entry) => {
            row.entry_type(format_using(output, &entry.header.to_entry_type()));

            if let Some(completed) = entry.header.is_completed() {
                row.completed(completed);
            }

            let decoded_entry = OnceCell::new();

            match &entry.header {
                EnrichedEntryHeader::Invoke {
                    resolution_result: Some(resolution_result),
                    ..
                }
                | EnrichedEntryHeader::BackgroundInvoke { resolution_result } => {
                    row.invoked_service_key(&resolution_result.service_key);

                    if row.is_invoked_id_defined() {
                        // we don't need to decode the entry for the service name to produce a partition key; use empty name
                        let partition_key = ServiceId::new(
                            ByteString::new(),
                            resolution_result.service_key.clone(),
                        )
                        .partition_key();

                        row.invoked_id(format_using(
                            output,
                            &InvocationId::new(partition_key, resolution_result.invocation_uuid),
                        ));
                    }

                    if row.is_invoked_service_defined()
                        || row.is_invoked_method_defined()
                            | row.is_invoked_service_key_json_defined()
                    {
                        let decoded_entry = decoded_entry.get_or_init(|| {
                            ProtobufRawEntryCodec::deserialize(&entry)
                                .expect("journal entry must deserialize")
                        });
                        debug_assert!(matches!(
                            decoded_entry,
                            Entry::Invoke(_) | Entry::BackgroundInvoke(_)
                        ));
                        match decoded_entry {
                            Entry::Invoke(InvokeEntry { request, .. })
                            | Entry::BackgroundInvoke(BackgroundInvokeEntry { request, .. }) => {
                                row.invoked_service(&request.service_name);
                                row.invoked_method(&request.method_name);

                                if row.is_invoked_service_key_json_defined() {
                                    if let Some(key) = restate_keys::try_decode_restate_key_as_json(
                                        &request.service_name,
                                        &resolution_result.service_key,
                                        output,
                                        resolver,
                                    ) {
                                        row.invoked_service_key_json(key)
                                    }
                                }
                            }
                            _ => {}
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
