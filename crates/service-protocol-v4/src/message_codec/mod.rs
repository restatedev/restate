// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
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

use bytes::Bytes;
use std::time::Duration;

mod encoding;
mod header;

pub use encoding::{Decoder, Encoder, EncodingError};
pub use header::MessageHeader;
use restate_types::journal_v2::CommandIndex;

/// Protobuf protocol messages
pub mod proto {
    pub use crate::proto::*;
}

const CUSTOM_MESSAGE_MASK: u16 = 0xFC00;

type MessageTypeId = u16;

#[derive(Debug, thiserror::Error)]
#[error("unknown protocol.message code {0:#x}")]
pub struct UnknownMessageType(u16);

// This macro generates:
//
// * the Message enum, containing the concrete protobuf messages, except for @noparse annotated variants.
// * the Message.encoded_len() method
// * the Message.encode() method
// * the Message.ty() method
// * the MessageType enum, containing only the type information.
// * the MessageType.decode() method
// * the conversions back and forth to MessageTypeId.
macro_rules! gen_message {
    (@gen_message_enum [] -> [$($body:tt)*]) => {
        #[derive(Clone, Debug, PartialEq)]
        pub enum Message {
            $($body)*
            Custom(u16, bytes::Bytes)
        }
    };
    (@gen_message_enum [$variant:ident noparse $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message!(@gen_message_enum [$($tail)*] -> [$variant(bytes::Bytes), $($body)*]); }
    };
    (@gen_message_enum [$variant:ident $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message!(@gen_message_enum [$($tail)*] -> [$variant(proto::[< $variant Message >]), $($body)*]); }
    };

    (@gen_message_enum_encoded_len [] -> [$($body:tt)*]) => {
        impl Message {
            pub(crate) fn encoded_len(&self) -> usize {
                match self {
                    $($body)*
                    Message::Custom(_, b) => b.len()
                }
            }
        }
    };
    (@gen_message_enum_encoded_len [$variant:ident noparse $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message!(@gen_message_enum_encoded_len [$($tail)*] -> [Message::$variant(b) => b.len(), $($body)*]);
    };
    (@gen_message_enum_encoded_len [$variant:ident $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message!(@gen_message_enum_encoded_len [$($tail)*] -> [Message::$variant(msg) => prost::Message::encoded_len(msg), $($body)*]);
    };

    (@gen_message_enum_ty [] -> [$($body:tt)*]) => {
        impl Message {
            pub fn ty(&self) -> MessageType {
                match self {
                    $($body)*
                    Message::Custom(ty, _) => MessageType::Custom(*ty)
                }
            }
        }
    };
    (@gen_message_enum_ty [$variant:ident $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message!(@gen_message_enum_ty [$($tail)*] -> [Message::$variant(_) => MessageType::$variant, $($body)*]);
    };

    (@gen_message_enum_encode [] -> [$($body:tt)*]) => {
        impl Message {
            pub(crate) fn encode(&self, buf: &mut impl bytes::BufMut) -> Result<(), prost::EncodeError> {
                match (self, buf) {
                    $($body)*
                    (Message::Custom(_, b), buf) => buf.put(b.clone())
                };
                Ok(())
            }
        }
    };
    (@gen_message_enum_encode [$variant:ident noparse $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message!(@gen_message_enum_encode [$($tail)*] -> [(Message::$variant(b), buf) => buf.put(b.clone()), $($body)*]);
    };
    (@gen_message_enum_encode [$variant:ident $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message!(@gen_message_enum_encode [$($tail)*] -> [(Message::$variant(msg), buf) => prost::Message::encode(msg, buf)?, $($body)*]);
    };

    (@gen_message_type_enum [] -> [$($body:tt)*]) => {
        #[derive(Copy, Clone, Debug, PartialEq, Eq)]
        pub enum MessageType {
            $($body)*
            Custom(u16)
        }
    };
    (@gen_message_type_enum [$variant:ident $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message!(@gen_message_type_enum [$($tail)*] -> [$variant, $($body)*]);
    };

    (@gen_message_type_enum_allows_ack [] -> [$($variants:tt)*]) => {
        impl MessageType {
            pub(crate) fn allows_ack(&self) -> bool {
                match self {
                    $($variants)* MessageType::Custom(_) => true,
                    _ => false
                }
            }
        }
    };
    (@gen_message_type_enum_allows_ack [$variant:ident $($noparse:ident)? allows_ack = $id:literal, $($tail:tt)*] -> [$($variants:tt)*]) => {
        gen_message!(@gen_message_type_enum_allows_ack [$($tail)*] -> [MessageType::$variant | $($variants)*]);
    };
    (@gen_message_type_enum_allows_ack [$variant:ident $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($variants:tt)*]) => {
        gen_message!(@gen_message_type_enum_allows_ack [$($tail)*] -> [$($variants)*]);
    };

    (@gen_message_type_enum_decode [] -> [$($body:tt)*]) => {
        impl MessageType {
            pub(crate) fn decode(&self, buf: impl bytes::Buf) -> Result<Message, prost::DecodeError> {
                match (self, buf) {
                    $($body)*
                    (MessageType::Custom(t), mut buf) => Ok(Message::Custom(*t, buf.copy_to_bytes(buf.remaining())))
                }
            }
        }
    };
    (@gen_message_type_enum_decode [$variant:ident noparse $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message!(@gen_message_type_enum_decode [$($tail)*] -> [(MessageType::$variant, mut buf) => Ok(Message::$variant(buf.copy_to_bytes(buf.remaining()))), $($body)*]);
    };
    (@gen_message_type_enum_decode [$variant:ident $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message!(@gen_message_type_enum_decode [$($tail)*] -> [(MessageType::$variant, buf) => Ok(Message::$variant(<proto::[< $variant Message >] as prost::Message>::decode(buf)?)), $($body)*]); }
    };

    (@gen_to_id [] -> [$($variant:ident, $id:literal,)*]) => {
        impl From<MessageType> for MessageTypeId {
            fn from(mt: MessageType) -> Self {
                match mt {
                    $(MessageType::$variant => $id,)*
                    MessageType::Custom(id) => id
                }
            }
        }
    };
    (@gen_to_id [$variant:ident $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message!(@gen_to_id [$($tail)*] -> [$variant, $id, $($body)*]);
    };

    (@gen_from_id [] -> [$($variant:ident, $id:literal,)*]) => {
        impl TryFrom<MessageTypeId> for MessageType {
            type Error = UnknownMessageType;

            fn try_from(value: MessageTypeId) -> Result<Self, UnknownMessageType> {
                match value {
                    $($id => Ok(MessageType::$variant),)*
                    v if (v & CUSTOM_MESSAGE_MASK) != 0 => Ok(MessageType::Custom(v)),
                    v => Err(UnknownMessageType(v)),
                }
            }
        }
    };
    (@gen_from_id [$variant:ident $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message!(@gen_from_id [$($tail)*] -> [$variant, $id, $($body)*]);
    };

    // Entrypoint of the macro
    ($($tokens:tt)*) => {
        gen_message!(@gen_message_enum [$($tokens)*] -> []);
        gen_message!(@gen_message_enum_encoded_len [$($tokens)*] -> []);
        gen_message!(@gen_message_enum_encode [$($tokens)*] -> []);
        gen_message!(@gen_message_enum_ty [$($tokens)*] -> []);
        gen_message!(@gen_message_type_enum [$($tokens)*] -> []);
        gen_message!(@gen_message_type_enum_allows_ack [$($tokens)*] -> []);
        gen_message!(@gen_message_type_enum_decode [$($tokens)*] -> []);
        gen_message!(@gen_to_id [$($tokens)*] -> []);
        gen_message!(@gen_from_id [$($tokens)*] -> []);
    };
}

gen_message!(
    Start = 0x0000,
    Suspension = 0x0002,
    Error = 0x0003,
    End = 0x0005,
    CommandAck = 0x0004,
    ProposeRunCompletion = 0x0006,

    Notification noparse = 0x0001,

    InputCommand noparse allows_ack = 0x0400,
    OutputCommand noparse allows_ack = 0x0401,

    GetLazyStateCommand noparse allows_ack = 0x0800,
    SetStateCommand noparse allows_ack = 0x0801,
    ClearStateCommand noparse allows_ack = 0x0802,
    ClearAllStateCommand noparse allows_ack = 0x0803,
    GetLazyStateKeysCommand noparse allows_ack = 0x0804,
    GetEagerStateCommand noparse allows_ack = 0x0805,
    GetEagerStateKeysCommand noparse allows_ack = 0x0806,

    GetPromiseCommand noparse allows_ack = 0x0808,
    PeekPromiseCommand noparse allows_ack = 0x0809,
    CompletePromiseCommand noparse allows_ack = 0x080A,

    SleepCommand noparse allows_ack = 0x0C00,
    CallCommand noparse allows_ack = 0x0C01,
    OneWayCallCommand noparse allows_ack = 0x0C02,

    SendNotificationCommand noparse allows_ack = 0x0C04,

    RunCommand noparse allows_ack = 0x0C05,

    AttachInvocationCommand noparse allows_ack = 0x0C08,
    GetInvocationOutputCommand noparse allows_ack = 0x0C09,
);

impl Message {
    #[allow(clippy::too_many_arguments)]
    pub fn new_start_message(
        id: Bytes,
        debug_id: String,
        key: Option<Bytes>,
        known_entries: u32,
        partial_state: bool,
        state_map_entries: impl IntoIterator<Item = (Bytes, Bytes)>,
        retry_count_since_last_stored_entry: u32,
        duration_since_last_stored_entry: Duration,
    ) -> Self {
        Self::Start(proto::StartMessage {
            id,
            debug_id,
            known_entries,
            partial_state,
            state_map: state_map_entries
                .into_iter()
                .map(|(key, value)| proto::start_message::StateEntry { key, value })
                .collect(),
            key: key
                .and_then(|b| String::from_utf8(b.to_vec()).ok())
                .unwrap_or_default(),
            retry_count_since_last_stored_entry,
            duration_since_last_stored_entry: duration_since_last_stored_entry.as_millis() as u64,
        })
    }

    pub fn new_command_ack(command_index: CommandIndex) -> Self {
        Self::CommandAck(proto::CommandAckMessage { command_index })
    }
}
