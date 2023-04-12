//! Module containing definitions of Protocol messages,
//! including encoding and decoding of headers and message payloads.

use super::pb;

use bytes::Bytes;
use common::types::CompletionResult;
use journal::raw::PlainRawEntry;
use journal::Completion;
use prost::Message;

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

    // Entries are not parsed at this point
    UnparsedEntry(PlainRawEntry),
}

impl ProtocolMessage {
    pub fn new_start_message(
        invocation_id: Bytes,
        instance_key: Bytes,
        known_entries: u32,
    ) -> Self {
        ProtocolMessage::Start(pb::protocol::StartMessage {
            invocation_id,
            instance_key,
            known_entries,
        })
    }
}

impl From<Completion> for ProtocolMessage {
    fn from(completion: Completion) -> Self {
        ProtocolMessage::Completion(pb::protocol::CompletionMessage {
            entry_index: completion.entry_index,
            result: match completion.result {
                CompletionResult::Ack => None,
                CompletionResult::Empty => {
                    Some(pb::protocol::completion_message::Result::Empty(()))
                }
                CompletionResult::Success(b) => {
                    Some(pb::protocol::completion_message::Result::Value(b))
                }
                CompletionResult::Failure(code, message) => Some(
                    pb::protocol::completion_message::Result::Failure(pb::protocol::Failure {
                        code,
                        message: message.to_string(),
                    }),
                ),
            },
        })
    }
}

impl From<PlainRawEntry> for ProtocolMessage {
    fn from(value: PlainRawEntry) -> Self {
        Self::UnparsedEntry(value)
    }
}
