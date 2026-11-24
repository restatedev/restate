// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{Buf, BufMut};
use prost::Message as _;

use super::MessageType;
use crate::proto;

macro_rules! messages {
    ($($variant:ident($ty:ident)),* $(,)?) => {
        /// A message of the protocol, with its payload.
        #[derive(Debug, Clone, PartialEq)]
        pub enum Message {
            $($variant(proto::$ty)),*
        }

        impl Message {
            pub fn ty(&self) -> MessageType {
                match self {
                    $(Message::$variant(_) => MessageType::$variant),*
                }
            }

            pub fn encoded_len(&self) -> usize {
                match self {
                    $(Message::$variant(m) => m.encoded_len()),*
                }
            }

            pub fn encode(&self, buf: &mut impl BufMut) {
                match self {
                    $(Message::$variant(m) => m.encode_raw(buf)),*
                }
            }

            pub fn decode(ty: MessageType, buf: impl Buf) -> Result<Self, prost::DecodeError> {
                Ok(match ty {
                    $(MessageType::$variant => Message::$variant(proto::$ty::decode(buf)?)),*
                })
            }
        }

        $(
            impl From<proto::$ty> for Message {
                fn from(value: proto::$ty) -> Self {
                    Message::$variant(value)
                }
            }
        )*
    };
}

messages! {
    Start(StartMessage),
    Shutdown(ShutdownMessage),
    Error(ErrorMessage),
    End(EndMessage),
    Heartbeat(HeartbeatMessage),
    GetStateAction(GetStateActionMessage),
    GetStateKeysAction(GetStateKeysActionMessage),
    SetStateAction(SetStateActionMessage),
    ClearStateAction(ClearStateActionMessage),
    ClearAllStateAction(ClearAllStateActionMessage),
    CallAction(CallActionMessage),
    AttachInvocationAction(AttachInvocationActionMessage),
    CancelInvocationAction(CancelInvocationActionMessage),
    ReadSignalAction(ReadSignalActionMessage),
    SendSignalAction(SendSignalActionMessage),
    FlushAction(FlushActionMessage),
    AtomicGroupAction(AtomicGroupActionMessage),
    SpanStartAction(SpanStartActionMessage),
    SpanEndAction(SpanEndActionMessage),
    ResultNotification(ResultNotificationMessage),
    SignalNotification(SignalNotificationMessage),
}
