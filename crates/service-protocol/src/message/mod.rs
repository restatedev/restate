// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
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

use super::pb;

use bytes::Bytes;
use prost::Message;
use restate_types::errors::InvocationError;
use restate_types::journal::raw::PlainRawEntry;
use restate_types::journal::CompletionResult;
use restate_types::journal::{Completion, EntryIndex};

mod encoding;
mod header;

pub use encoding::{Decoder, Encoder, EncodingError};
pub use header::{MessageHeader, MessageKind, MessageType};

#[derive(Debug, Clone, PartialEq)]
pub enum ProtocolMessage {
    // Core
    Start(pb::protocol::StartMessage),
    Completion(pb::protocol::CompletionMessage),
    Suspension(pb::protocol::SuspensionMessage),
    Error(pb::protocol::ErrorMessage),
    End(pb::protocol::EndMessage),
    EntryAck(pb::protocol::EntryAckMessage),

    // Entries are not parsed at this point
    UnparsedEntry(PlainRawEntry),
}

impl ProtocolMessage {
    pub fn new_start_message(
        id: Bytes,
        debug_id: String,
        known_entries: u32,
        partial_state: bool,
        state_map_entries: impl IntoIterator<Item = (Bytes, Bytes)>,
    ) -> Self {
        Self::Start(pb::protocol::StartMessage {
            id,
            debug_id,
            known_entries,
            partial_state,
            state_map: state_map_entries
                .into_iter()
                .map(|(key, value)| pb::protocol::start_message::StateEntry { key, value })
                .collect(),
        })
    }

    pub fn new_entry_ack(entry_index: EntryIndex) -> ProtocolMessage {
        Self::EntryAck(pb::protocol::EntryAckMessage { entry_index })
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
                ProtocolMessage::Completion(pb::protocol::CompletionMessage {
                    entry_index: completion.entry_index,
                    result: Some(pb::protocol::completion_message::Result::Empty(())),
                })
            }
            CompletionResult::Success(b) => {
                ProtocolMessage::Completion(pb::protocol::CompletionMessage {
                    entry_index: completion.entry_index,
                    result: Some(pb::protocol::completion_message::Result::Value(b)),
                })
            }
            CompletionResult::Failure(code, message) => {
                ProtocolMessage::Completion(pb::protocol::CompletionMessage {
                    entry_index: completion.entry_index,
                    result: Some(pb::protocol::completion_message::Result::Failure(
                        pb::protocol::Failure {
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

impl From<pb::protocol::ErrorMessage> for InvocationError {
    fn from(value: pb::protocol::ErrorMessage) -> Self {
        if value.description.is_empty() {
            InvocationError::new(value.code, value.message)
        } else {
            InvocationError::new(value.code, value.message).with_description(value.description)
        }
    }
}
