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
use restate_types::identifiers::{InvocationId, ServiceId, WithPartitionKey};
use restate_types::journal::enriched::{EnrichedEntryHeader, EnrichedRawEntry};

use crate::table_util::format_using;
use restate_types::journal::{BackgroundInvokeEntry, Entry, InvokeEntry, InvokeRequest};

#[inline]
pub(crate) fn append_journal_row(
    builder: &mut JournalBuilder,
    output: &mut String,
    journal_row: OwnedJournalRow,
) {
    let mut row = builder.row();

    row.partition_key(journal_row.partition_key);
    row.service(&journal_row.service);
    row.service_key(
        std::str::from_utf8(&journal_row.service_key).expect("The key must be a string!"),
    );

    row.index(journal_row.journal_index);

    match journal_row.journal_entry {
        JournalEntry::Entry(entry) => {
            row.entry_type(format_using(output, &entry.header().as_entry_type()));

            if let Some(completed) = entry.header().is_completed() {
                row.completed(completed);
            }

            match &entry.header() {
                EnrichedEntryHeader::Invoke {
                    enrichment_result: Some(enrichment_result),
                    ..
                }
                | EnrichedEntryHeader::BackgroundInvoke {
                    enrichment_result, ..
                } => {
                    row.invoked_service_key(
                        std::str::from_utf8(&enrichment_result.service_key)
                            .expect("The key must be a string!"),
                    );

                    row.invoked_service(&enrichment_result.service_name);

                    if row.is_invoked_id_defined() {
                        let partition_key = ServiceId::new(
                            enrichment_result.service_name.clone(),
                            enrichment_result.service_key.clone(),
                        )
                        .partition_key();

                        row.invoked_id(format_using(
                            output,
                            &InvocationId::new(partition_key, enrichment_result.invocation_uuid),
                        ));
                    }

                    if row.is_invoked_method_defined() {
                        if let Some(request) = deserialize_invocation_request(&entry) {
                            row.invoked_method(&request.method_name);
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

fn deserialize_invocation_request(entry: &EnrichedRawEntry) -> Option<InvokeRequest> {
    let decoded_entry = entry
        .deserialize_entry_ref::<ProtobufRawEntryCodec>()
        .expect("journal entry must deserialize");

    debug_assert!(matches!(
        decoded_entry,
        Entry::Invoke(_) | Entry::BackgroundInvoke(_)
    ));
    match decoded_entry {
        Entry::Invoke(InvokeEntry { request, .. })
        | Entry::BackgroundInvoke(BackgroundInvokeEntry { request, .. }) => Some(request),
        _ => None,
    }
}
