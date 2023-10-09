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
use prost::Message;
use restate_schema_api::key::RestateKeyConverter;
use restate_service_protocol::pb::protocol;
use restate_storage_api::journal_table::JournalEntry;
use restate_storage_rocksdb::journal_table::OwnedJournalRow;
use restate_types::journal::enriched::EnrichedEntryHeader;
use restate_types::journal::raw::EntryHeader;
use restate_types::journal::CompletionResult;
use std::fmt;
use std::fmt::Write;
use uuid::Uuid;

#[inline]
pub(crate) fn append_journal_row(
    builder: &mut JournalBuilder,
    output: &mut String,
    journal_row: OwnedJournalRow,
    resolver: impl RestateKeyConverter,
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
            resolver,
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

                if completed {
                    match entry.header {
                        EnrichedEntryHeader::Custom { .. } => {
                            row.completion_result("ack");
                        }
                        _ => {
                            if row.is_completion_result_defined()
                                || row.is_completion_failure_code_defined()
                                || row.is_completion_failure_message_defined()
                            {
                                match Completion::decode(entry.entry).unwrap().result {
                                    Some(protocol::completion_message::Result::Value(_)) => {
                                        row.completion_result("success");
                                    }
                                    Some(protocol::completion_message::Result::Failure(
                                        protocol::Failure { code, message },
                                    )) => {
                                        row.completion_result("failure");
                                        row.completion_failure_code(code);
                                        row.completion_failure_message(message)
                                    }
                                    Some(protocol::completion_message::Result::Empty(())) => {
                                        row.completion_result("empty");
                                    }
                                    None => {}
                                }
                            }
                        }
                    }
                }
            }
        }
        JournalEntry::Completion(completion_result) => match completion_result {
            CompletionResult::Ack => {
                row.completion_result("ack");
            }
            CompletionResult::Empty => {
                row.completion_result("empty");
            }
            CompletionResult::Success(_) => {
                row.completion_result("success");
            }
            CompletionResult::Failure(code, message) => {
                row.completion_result("failure");
                row.completion_failure_code(code.into());
                row.completion_failure_message(message)
            }
        },
    };
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct Completion {
    #[prost(oneof = "protocol::completion_message::Result", tags = "13, 14, 15")]
    pub result: Option<protocol::completion_message::Result>,
}

#[inline]
fn format_using<'a>(output: &'a mut String, what: &impl fmt::Display) -> &'a str {
    output.clear();
    write!(output, "{}", what).expect("Error occurred while trying to write in String");
    output
}
