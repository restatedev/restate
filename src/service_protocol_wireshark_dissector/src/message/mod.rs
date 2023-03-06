use bytes::Bytes;
use journal::raw::RawEntry;
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
    Suspension(pb::SuspensionMessage),

    // Entries are not parsed at this point
    UnparsedEntry(RawEntry),
}

impl ProtocolMessage {
    pub fn new_start_message(
        invocation_id: Bytes,
        instance_key: Bytes,
        known_entries: u32,
    ) -> Self {
        ProtocolMessage::Start(pb::StartMessage {
            invocation_id,
            instance_key,
            known_entries,
        })
    }
}

impl From<Completion> for ProtocolMessage {
    fn from(completion: Completion) -> Self {
        ProtocolMessage::Completion(pb::CompletionMessage {
            entry_index: completion.entry_index,
            result: match completion.result {
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
