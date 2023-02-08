// TODO remove this as soon as we start using this module in the invoker.
#![allow(dead_code)]

use bytes::Bytes;
use common::types::ServiceInvocationId;
use journal::raw::{RawEntry, RawEntryHeader};
use journal::{Completion, CompletionResult};
use prost::Message;
use service_protocol::pb;

mod encoding;
mod header;

pub use encoding::{Decoder, Encoder, EncodingError};
pub use header::{MessageHeader, MessageKind, MessageType};

#[derive(Debug, Clone, PartialEq)]
pub enum ProtocolMessage {
    // Core
    Start(pb::StartMessage),
    Completion(pb::CompletionMessage),

    // Entries are not parsed at this point
    UnparsedEntry(RawEntry),
}

impl ProtocolMessage {
    pub fn new_start_message(sid: &ServiceInvocationId, known_entries: u32) -> Self {
        ProtocolMessage::Start(pb::StartMessage {
            invocation_id: Bytes::copy_from_slice(sid.invocation_id.as_bytes()),
            instance_key: sid.service_id.key.clone(),

            // TODO https://github.com/restatedev/service-protocol/issues/10
            known_service_version: 0,
            known_entries,
        })
    }
}

impl From<Completion> for ProtocolMessage {
    fn from(jc: Completion) -> Self {
        ProtocolMessage::Completion(pb::CompletionMessage {
            entry_index: jc.entry_index,
            result: match jc.result {
                CompletionResult::Ack => None,
                CompletionResult::Empty => Some(pb::completion_message::Result::Empty(())),
                CompletionResult::Success(b) => Some(pb::completion_message::Result::Value(b)),
                CompletionResult::Failure(code, message) => {
                    Some(pb::completion_message::Result::Failure(pb::Failure {
                        code,
                        message: message.to_string(),
                    }))
                }
            },
        })
    }
}

impl From<RawEntry> for ProtocolMessage {
    fn from(value: RawEntry) -> Self {
        Self::UnparsedEntry(value)
    }
}
