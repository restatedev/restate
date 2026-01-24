// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{MessageType, UnknownMessageType};

const REQUIRES_ACK_MASK: u64 = 0x8000_0000_0000;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct MessageHeader {
    ty: MessageType,
    length: u32,

    // --- Flags
    requires_ack_flag: Option<bool>,
}

impl MessageHeader {
    #[inline]
    pub fn new(ty: MessageType, length: u32) -> Self {
        Self::_new(ty, None, length)
    }

    #[inline]
    fn _new(ty: MessageType, requires_ack_flag: Option<bool>, length: u32) -> Self {
        MessageHeader {
            ty,
            length,
            requires_ack_flag,
        }
    }

    #[inline]
    pub fn message_type(&self) -> MessageType {
        self.ty
    }

    #[inline]
    pub fn requires_ack(&self) -> Option<bool> {
        self.requires_ack_flag
    }

    #[inline]
    pub fn frame_length(&self) -> u32 {
        self.length
    }
}

macro_rules! read_flag_if {
    ($cond:expr, $value:expr, $mask:expr) => {
        if $cond {
            Some(($value & $mask) != 0)
        } else {
            None
        }
    };
}

impl TryFrom<u64> for MessageHeader {
    type Error = UnknownMessageType;

    /// Deserialize the protocol header.
    /// See https://github.com/restatedev/service-protocol/blob/main/service-invocation-protocol.md#message-header
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        let ty_code = (value >> 48) as u16;
        let ty: MessageType = ty_code.try_into()?;

        let requires_ack_flag = read_flag_if!(ty.allows_ack(), value, REQUIRES_ACK_MASK);
        let length = value as u32;

        Ok(MessageHeader::_new(ty, requires_ack_flag, length))
    }
}

macro_rules! write_flag {
    ($flag:expr, $value:expr, $mask:expr) => {
        if let Some(true) = $flag {
            *$value |= $mask;
        }
    };
}

impl From<MessageHeader> for u64 {
    /// Serialize the protocol header.
    /// See https://github.com/restatedev/service-protocol/blob/main/service-invocation-protocol.md#message-header
    fn from(message_header: crate::message_codec::MessageHeader) -> Self {
        let mut res =
            ((u16::from(message_header.ty) as u64) << 48) | (message_header.length as u64);

        write_flag!(
            message_header.requires_ack_flag,
            &mut res,
            REQUIRES_ACK_MASK
        );

        res
    }
}

#[cfg(test)]
mod tests {
    use super::{MessageType::*, *};

    macro_rules! roundtrip_test {
        ($test_name:ident, $header:expr, $ty:expr, $len:expr) => {
            roundtrip_test!($test_name, $header, $ty, $len, None);
        };
        ($test_name:ident, $header:expr, $ty:expr, $len:expr, requires_ack: $requires_ack:expr) => {
            roundtrip_test!($test_name, $header, $ty, $len, Some($requires_ack));
        };
        ($test_name:ident, $header:expr, $ty:expr, $len:expr, $requires_ack:expr) => {
            #[test]
            fn $test_name() {
                let serialized: u64 = $header.into();
                let header: MessageHeader = serialized.try_into().unwrap();

                assert_eq!(header.message_type(), $ty);
                assert_eq!(header.requires_ack(), $requires_ack);
                assert_eq!(header.frame_length(), $len);
            }
        };
    }

    roundtrip_test!(
        call_completion_notification,
        MessageHeader::new(CallCompletionNotification, 22),
        CallCompletionNotification,
        22
    );

    roundtrip_test!(
        get_state,
        MessageHeader::new(GetLazyStateCommand, 0),
        GetLazyStateCommand,
        0,
        requires_ack: false
    );

    roundtrip_test!(
        get_state_with_len,
        MessageHeader::new(GetLazyStateCommand, 10341),
        GetLazyStateCommand,
        10341,
        requires_ack: false
    );

    roundtrip_test!(
        set_state_with_requires_ack,
        MessageHeader::_new(SetStateCommand, Some(true), 10341),
        SetStateCommand,
        10341,
        requires_ack: true
    );

    roundtrip_test!(
        custom_entry,
        MessageHeader::new(Custom(0xFC00), 10341),
        Custom(0xFC00),
        10341,
        requires_ack: false
    );

    roundtrip_test!(
        custom_entry_with_requires_ack,
        MessageHeader::_new(Custom(0xFC00), Some(true), 10341),
        Custom(0xFC00),
        10341,
        requires_ack: true
    );
}
