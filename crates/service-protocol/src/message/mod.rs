// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Module containing definitions of Protocol messages,
//! including encoding and decoding of headers and message payloads.

mod encoding;
mod header;

use std::time::Duration;

use bytes::Bytes;
use prost::Message;

use restate_types::journal::CompletionResult;
use restate_types::journal::raw::PlainRawEntry;
use restate_types::journal::{Completion, EntryIndex};
use restate_types::service_protocol;

pub use encoding::{Decoder, Encoder, EncodingError};
pub use header::{MessageHeader, MessageKind, MessageType};
pub use restate_types::service_protocol::start_message::StateEntry;

#[derive(Debug, Clone, PartialEq)]
pub enum ProtocolMessage {
    // Core
    Start(service_protocol::StartMessage),
    Completion(service_protocol::CompletionMessage),
    Suspension(service_protocol::SuspensionMessage),
    Error(service_protocol::ErrorMessage),
    End(service_protocol::EndMessage),
    EntryAck(service_protocol::EntryAckMessage),

    // Entries are not parsed at this point
    UnparsedEntry(PlainRawEntry),
}

impl ProtocolMessage {
    #[allow(clippy::too_many_arguments)]
    pub fn new_start_message(
        id: Bytes,
        debug_id: String,
        key: Option<Bytes>,
        known_entries: u32,
        partial_state: bool,
        state_map: Vec<service_protocol::start_message::StateEntry>,
        retry_count_since_last_stored_entry: u32,
        duration_since_last_stored_entry: Duration,
    ) -> Self {
        Self::Start(service_protocol::StartMessage {
            id,
            debug_id,
            known_entries,
            partial_state,
            state_map,
            key: key
                .and_then(|b| String::from_utf8(b.to_vec()).ok())
                .unwrap_or_default(),
            retry_count_since_last_stored_entry,
            duration_since_last_stored_entry: duration_since_last_stored_entry.as_millis() as u64,
        })
    }

    pub fn new_entry_ack(entry_index: EntryIndex) -> ProtocolMessage {
        Self::EntryAck(service_protocol::EntryAckMessage { entry_index })
    }

    pub(crate) fn encoded_len(&self) -> usize {
        match self {
            ProtocolMessage::Start(m) => m.encoded_len(),
            ProtocolMessage::Completion(m) => m.encoded_len(),
            ProtocolMessage::Suspension(m) => m.encoded_len(),
            ProtocolMessage::Error(m) => m.encoded_len(),
            ProtocolMessage::End(m) => m.encoded_len(),
            ProtocolMessage::EntryAck(m) => m.encoded_len(),
            ProtocolMessage::UnparsedEntry(entry) => entry.serialized_entry().len(),
        }
    }
}

impl From<Completion> for ProtocolMessage {
    fn from(completion: Completion) -> Self {
        match completion.result {
            CompletionResult::Empty => {
                ProtocolMessage::Completion(service_protocol::CompletionMessage {
                    entry_index: completion.entry_index,
                    result: Some(service_protocol::completion_message::Result::Empty(
                        service_protocol::Empty {},
                    )),
                })
            }
            CompletionResult::Success(b) => {
                ProtocolMessage::Completion(service_protocol::CompletionMessage {
                    entry_index: completion.entry_index,
                    result: Some(service_protocol::completion_message::Result::Value(b)),
                })
            }
            CompletionResult::Failure(code, message) => {
                ProtocolMessage::Completion(service_protocol::CompletionMessage {
                    entry_index: completion.entry_index,
                    result: Some(service_protocol::completion_message::Result::Failure(
                        service_protocol::Failure {
                            code: code.into(),
                            message: message.to_string(),
                        },
                    )),
                })
            }
        }
    }
}

impl From<PlainRawEntry> for ProtocolMessage {
    fn from(value: PlainRawEntry) -> Self {
        Self::UnparsedEntry(value)
    }
}
