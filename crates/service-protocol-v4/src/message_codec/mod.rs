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
use restate_types::journal_v2::{
    CommandIndex, CommandType, CompletionType, EntryType, NotificationType,
};

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
    (@gen_message_enum [$variant:ident Control = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message!(@gen_message_enum [$($tail)*] -> [$variant(proto::[< $variant Message >]), $($body)*]); }
    };
    (@gen_message_enum [$variant:ident $ty:ident noparse $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message!(@gen_message_enum [$($tail)*] -> [[< $variant $ty >](bytes::Bytes), $($body)*]); }
    };
    (@gen_message_enum [$variant:ident $ty:ident $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message!(@gen_message_enum [$($tail)*] -> [[< $variant $ty >](proto::[< $variant $ty Message >]), $($body)*]); }
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
    (@gen_message_enum_encoded_len [$variant:ident Control = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message!(@gen_message_enum_encoded_len [$($tail)*] -> [Message::$variant(msg) => prost::Message::encoded_len(msg), $($body)*]);
    };
    (@gen_message_enum_encoded_len [$variant:ident $ty:ident noparse $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message!(@gen_message_enum_encoded_len [$($tail)*] -> [Message::[< $variant $ty >](b) => b.len(), $($body)*]); }
    };
    (@gen_message_enum_encoded_len [$variant:ident $ty:ident $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message!(@gen_message_enum_encoded_len [$($tail)*] -> [Message::[< $variant $ty >](msg) => prost::Message::encoded_len(msg), $($body)*]); }
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
    (@gen_message_enum_ty [$variant:ident Control $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message!(@gen_message_enum_ty [$($tail)*] -> [Message::$variant(_) => MessageType::$variant, $($body)*]);
    };
    (@gen_message_enum_ty [$variant:ident $ty:ident $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message!(@gen_message_enum_ty [$($tail)*] -> [Message::[< $variant $ty >](_) => MessageType::[< $variant $ty >], $($body)*]); }
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
    (@gen_message_enum_encode [$variant:ident Control = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message!(@gen_message_enum_encode [$($tail)*] -> [(Message::$variant(msg), buf) => prost::Message::encode(msg, buf)?, $($body)*]);
    };
    (@gen_message_enum_encode [$variant:ident $ty:ident noparse $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message!(@gen_message_enum_encode [$($tail)*] -> [(Message::[< $variant $ty >](b), buf) => buf.put(b.clone()), $($body)*]); }
    };
    (@gen_message_enum_encode [$variant:ident $ty:ident $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message!(@gen_message_enum_encode [$($tail)*] -> [(Message::[< $variant $ty >](msg), buf) => prost::Message::encode(msg, buf)?, $($body)*]); }
    };

    (@gen_message_enum_proto_debug [] -> [$($body:tt)*]) => {
        impl Message {
            pub fn proto_debug(&self) -> String {
                match self {
                    $($body)*
                    Message::Custom(_, b) => format!("{b:?}")
                }
            }
        }
    };
    (@gen_message_enum_proto_debug [$variant:ident Control = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message!(@gen_message_enum_proto_debug [$($tail)*] -> [Message::$variant(msg) => format!("{msg:?}"), $($body)*]);
    };
    (@gen_message_enum_proto_debug [$variant:ident $ty:ident noparse $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message!(@gen_message_enum_proto_debug [$($tail)*] -> [Message::[< $variant $ty >](b) => format!("{:?}", <proto::[< $variant $ty Message>] as prost::Message>::decode(&mut b.clone())), $($body)*]); }
    };
    (@gen_message_enum_proto_debug [$variant:ident $ty:ident $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message!(@gen_message_enum_proto_debug [$($tail)*] -> [Message::[< $variant $ty >](msg) => format!("{msg:?}"), $($body)*]); }
    };

    (@gen_message_type_enum [] -> [$($body:tt)*]) => {
        #[derive(Copy, Clone, Debug, PartialEq, Eq)]
        pub enum MessageType {
            $($body)*
            Custom(u16)
        }
    };
    (@gen_message_type_enum [$variant:ident Control $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message!(@gen_message_type_enum [$($tail)*] -> [$variant, $($body)*]);
    };
    (@gen_message_type_enum [$variant:ident $ty:ident $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message!(@gen_message_type_enum [$($tail)*] -> [[<$variant $ty>], $($body)*]); }
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
    (@gen_message_type_enum_allows_ack [$variant:ident Control = $id:literal, $($tail:tt)*] -> [$($variants:tt)*]) => {
        gen_message!(@gen_message_type_enum_allows_ack [$($tail)*] -> [$($variants)*]);
    };
    (@gen_message_type_enum_allows_ack [$variant:ident $ty:ident noparse allows_ack = $id:literal, $($tail:tt)*] -> [$($variants:tt)*]) => {
        paste::paste! { gen_message!(@gen_message_type_enum_allows_ack [$($tail)*] -> [MessageType::[<$variant $ty>] | $($variants)*]); }
    };
    (@gen_message_type_enum_allows_ack [$variant:ident $ty:ident allows_ack = $id:literal, $($tail:tt)*] -> [$($variants:tt)*]) => {
        paste::paste! { gen_message!(@gen_message_type_enum_allows_ack [$($tail)*] -> [MessageType::[<$variant $ty>] | $($variants)*]); }
    };
    (@gen_message_type_enum_allows_ack [$variant:ident $ty:ident $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($variants:tt)*]) => {
        paste::paste! { gen_message!(@gen_message_type_enum_allows_ack [$($tail)*] -> [$($variants)*]); }
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
    (@gen_message_type_enum_decode [$variant:ident Control = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message!(@gen_message_type_enum_decode [$($tail)*] -> [(MessageType::$variant, buf) => Ok(Message::$variant(<proto::[< $variant Message >] as prost::Message>::decode(buf)?)), $($body)*]); }
    };
    (@gen_message_type_enum_decode [$variant:ident $ty:ident noparse $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message!(@gen_message_type_enum_decode [$($tail)*] -> [(MessageType::[< $variant $ty >], mut buf) => Ok(Message::[< $variant $ty >](buf.copy_to_bytes(buf.remaining()))), $($body)*]); }
    };
    (@gen_message_type_enum_decode [$variant:ident $ty:ident $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message!(@gen_message_type_enum_decode [$($tail)*] -> [(MessageType::[< $variant $ty >], buf) => Ok(Message::[< $variant $ty >](<proto::[< $variant $ty Message >] as prost::Message>::decode(buf)?)), $($body)*]); }
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
    (@gen_to_id [$variant:ident Control $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message!(@gen_to_id [$($tail)*] -> [$variant, $id, $($body)*]);
    };
    (@gen_to_id [$variant:ident $ty:ident $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message!(@gen_to_id [$($tail)*] -> [[<$variant $ty >], $id, $($body)*]); }
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
    (@gen_from_id [$variant:ident Control $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message!(@gen_from_id [$($tail)*] -> [$variant, $id, $($body)*]);
    };
    (@gen_from_id [$variant:ident $ty:ident $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message!(@gen_from_id [$($tail)*] -> [[<$variant $ty>], $id, $($body)*]); }
    };

    (@gen_from_command_ty [] -> [$($variant:ident,)*]) => {
        impl From<CommandType> for MessageType {
            fn from(value: CommandType) -> Self {
                match value {
                    $(CommandType::$variant => paste::paste! { MessageType::[< $variant Command >] },)*
                }
            }
        }
    };
    (@gen_from_command_ty [$variant:ident Command $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message!(@gen_from_command_ty [$($tail)*] -> [$variant, $($body)*]);
    };
    (@gen_from_command_ty [$variant:ident $ty:ident $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message!(@gen_from_command_ty [$($tail)*] -> [$($body)*]);
    };

    (@gen_from_completion_ty [] -> [$($variant:ident,)*]) => {
        impl From<CompletionType> for MessageType {
            fn from(value: CompletionType) -> Self {
                match value {
                    $(CompletionType::$variant => paste::paste! { MessageType::[< $variant CompletionNotification >] },)*
                }
            }
        }
    };
    (@gen_from_completion_ty [$variant:ident CompletionNotification $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message!(@gen_from_completion_ty [$($tail)*] -> [$variant, $($body)*]);
    };
    (@gen_from_completion_ty [$variant:ident $ty:ident $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message!(@gen_from_completion_ty [$($tail)*] -> [$($body)*]);
    };

    (@gen_from_notification_ty [] -> [$($variant:ident,)*]) => {
        impl From<NotificationType> for MessageType {
            fn from(value: NotificationType) -> Self {
                match value {
                    $(NotificationType::$variant => paste::paste! { MessageType::[< $variant Notification >] },)*
                    NotificationType::Completion(completion) => completion.into()
                }
            }
        }
    };
    (@gen_from_notification_ty [$variant:ident Notification $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message!(@gen_from_notification_ty [$($tail)*] -> [$variant, $($body)*]);
    };
    (@gen_from_notification_ty [$variant:ident $ty:ident $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message!(@gen_from_notification_ty [$($tail)*] -> [$($body)*]);
    };

    (@gen_message_type_enum_entry_type [] -> [$($body:tt)*]) => {
        impl MessageType {
            pub fn entry_type(&self) -> Option<EntryType> {
                match self {
                    $($body)*
                    _ => None,
                }
            }
        }
    };
    (@gen_message_type_enum_entry_type [$variant:ident Command $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message!(@gen_message_type_enum_entry_type [$($tail)*] -> [MessageType::[< $variant Command >] => Some(EntryType::Command(CommandType::$variant)), $($body)*]); }
    };
    (@gen_message_type_enum_entry_type [$variant:ident CompletionNotification $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message!(@gen_message_type_enum_entry_type [$($tail)*] -> [MessageType::[< $variant CompletionNotification >] => Some(EntryType::Notification(NotificationType::Completion(CompletionType::$variant))), $($body)*]); }
    };
    (@gen_message_type_enum_entry_type [$variant:ident Notification $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message!(@gen_message_type_enum_entry_type [$($tail)*] -> [MessageType::[< $variant Notification >] => Some(EntryType::Notification(NotificationType::$variant)), $($body)*]); }
    };
    (@gen_message_type_enum_entry_type [$variant:ident $ty:ident $($ignore:ident)* = $id:literal, $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message!(@gen_message_type_enum_entry_type [$($tail)*] -> [$($body)*]);
    };

    // Entrypoint of the macro
    ($($tokens:tt)*) => {
        gen_message!(@gen_message_enum [$($tokens)*] -> []);
        gen_message!(@gen_message_enum_encoded_len [$($tokens)*] -> []);
        gen_message!(@gen_message_enum_encode [$($tokens)*] -> []);
        gen_message!(@gen_message_enum_ty [$($tokens)*] -> []);
        gen_message!(@gen_message_enum_proto_debug [$($tokens)*] -> []);
        gen_message!(@gen_message_type_enum [$($tokens)*] -> []);
        gen_message!(@gen_message_type_enum_allows_ack [$($tokens)*] -> []);
        gen_message!(@gen_message_type_enum_decode [$($tokens)*] -> []);
        gen_message!(@gen_message_type_enum_entry_type [$($tokens)*] -> []);
        gen_message!(@gen_to_id [$($tokens)*] -> []);
        gen_message!(@gen_from_id [$($tokens)*] -> []);
        gen_message!(@gen_from_command_ty [$($tokens)*] -> []);
        gen_message!(@gen_from_completion_ty [$($tokens)*] -> []);
        gen_message!(@gen_from_notification_ty [$($tokens)*] -> []);
    };
}

gen_message!(
    Start Control = 0x0000,
    Suspension Control = 0x0001,
    Error Control = 0x0002,
    End Control = 0x0003,
    CommandAck Control = 0x0004,
    ProposeRunCompletion Control = 0x0005,
    Awaiting Control = 0x0006,

    Input Command noparse allows_ack = 0x0400,
    Output Command noparse allows_ack = 0x0401,

    GetLazyState Command noparse allows_ack = 0x0402,
    GetLazyState CompletionNotification noparse = 0x8002,
    SetState Command noparse allows_ack = 0x0403,
    ClearState Command noparse allows_ack = 0x0404,
    ClearAllState Command noparse allows_ack = 0x0405,
    GetLazyStateKeys Command noparse allows_ack = 0x0406,
    GetLazyStateKeys CompletionNotification noparse = 0x8006,
    GetEagerState Command noparse allows_ack = 0x0407,
    GetEagerStateKeys Command noparse allows_ack = 0x0408,

    GetPromise Command noparse allows_ack = 0x0409,
    GetPromise CompletionNotification noparse = 0x8009,
    PeekPromise Command noparse allows_ack = 0x040A,
    PeekPromise CompletionNotification noparse = 0x800A,
    CompletePromise Command noparse allows_ack = 0x040B,
    CompletePromise CompletionNotification noparse = 0x800B,

    Sleep Command noparse allows_ack = 0x040C,
    Sleep CompletionNotification noparse = 0x800C,
    Call Command allows_ack = 0x040D,
    CallInvocationId CompletionNotification noparse = 0x800E,
    Call CompletionNotification noparse = 0x800D,
    OneWayCall Command allows_ack = 0x040E,

    SendSignal Command noparse allows_ack = 0x0410,

    Run Command noparse allows_ack = 0x0411,
    Run CompletionNotification noparse = 0x8011,

    AttachInvocation Command noparse allows_ack = 0x0412,
    AttachInvocation CompletionNotification noparse = 0x8012,
    GetInvocationOutput Command noparse allows_ack = 0x0413,
    GetInvocationOutput CompletionNotification noparse = 0x8013,

    CompleteAwakeable Command noparse allows_ack = 0x0414,

    Signal Notification noparse = 0xFBFF,
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
        random_seed: u64,
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
            random_seed,
        })
    }

    pub fn new_command_ack(command_index: CommandIndex) -> Self {
        Self::CommandAck(proto::CommandAckMessage { command_index })
    }
}
