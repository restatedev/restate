// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Who sends a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    RuntimeToSdk,
    SdkToRuntime,
}

macro_rules! message_types {
    ($($variant:ident = $code:literal),* $(,)?) => {
        /// The 21 message types of version 1. Any other code is unknown.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[repr(u16)]
        pub enum MessageType {
            $($variant = $code),*
        }

        impl TryFrom<u16> for MessageType {
            type Error = u16;

            fn try_from(value: u16) -> Result<Self, u16> {
                match value {
                    $($code => Ok(MessageType::$variant),)*
                    other => Err(other),
                }
            }
        }
    };
}

message_types! {
    Start = 0x0000,
    Shutdown = 0x0001,
    Error = 0x0002,
    End = 0x0003,
    Heartbeat = 0x0004,
    GetStateAction = 0x0410,
    GetStateKeysAction = 0x0411,
    SetStateAction = 0x0412,
    ClearStateAction = 0x0413,
    ClearAllStateAction = 0x0414,
    CallAction = 0x0420,
    AttachInvocationAction = 0x0421,
    CancelInvocationAction = 0x0422,
    ReadSignalAction = 0x0430,
    SendSignalAction = 0x0431,
    FlushAction = 0x0440,
    AtomicGroupAction = 0x0441,
    SpanStartAction = 0x0450,
    SpanEndAction = 0x0451,
    ResultNotification = 0x8000,
    SignalNotification = 0x8001,
}

impl MessageType {
    pub fn code(self) -> u16 {
        self as u16
    }

    pub fn direction(self) -> Direction {
        match self {
            MessageType::Start
            | MessageType::ResultNotification
            | MessageType::SignalNotification => Direction::RuntimeToSdk,
            _ => Direction::SdkToRuntime,
        }
    }
}

impl From<MessageType> for u16 {
    fn from(value: MessageType) -> Self {
        value.code()
    }
}
