// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use restate_types::journal::EntryType;

const CUSTOM_MESSAGE_MASK: u16 = 0xFC00;
const COMPLETED_MASK: u64 = 0x0001_0000_0000;
const REQUIRES_ACK_MASK: u64 = 0x8000_0000_0000;

type MessageTypeId = u16;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum MessageKind {
    Core,
    IO,
    State,
    Syscall,
    CustomEntry,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum MessageType {
    Start,
    Completion,
    Suspension,
    Error,
    End,
    EntryAck,
    InputEntry,
    OutputEntry,
    GetStateEntry,
    SetStateEntry,
    ClearStateEntry,
    GetStateKeysEntry,
    ClearAllStateEntry,
    SleepEntry,
    InvokeEntry,
    BackgroundInvokeEntry,
    AwakeableEntry,
    CompleteAwakeableEntry,
    SideEffectEntry,
    GetPromiseEntry,
    PeekPromiseEntry,
    CompletePromiseEntry,
    CancelInvocationEntry,
    GetCallInvocationIdEntry,
    AttachInvocationEntry,
    GetInvocationOutputEntry,
    CustomEntry(u16),
}

impl MessageType {
    fn kind(&self) -> MessageKind {
        match self {
            MessageType::Start => MessageKind::Core,
            MessageType::Completion => MessageKind::Core,
            MessageType::Suspension => MessageKind::Core,
            MessageType::Error => MessageKind::Core,
            MessageType::End => MessageKind::Core,
            MessageType::EntryAck => MessageKind::Core,
            MessageType::InputEntry => MessageKind::IO,
            MessageType::OutputEntry => MessageKind::IO,
            MessageType::GetStateEntry => MessageKind::State,
            MessageType::SetStateEntry => MessageKind::State,
            MessageType::ClearStateEntry => MessageKind::State,
            MessageType::GetStateKeysEntry => MessageKind::State,
            MessageType::ClearAllStateEntry => MessageKind::State,
            MessageType::SleepEntry => MessageKind::Syscall,
            MessageType::InvokeEntry => MessageKind::Syscall,
            MessageType::BackgroundInvokeEntry => MessageKind::Syscall,
            MessageType::AwakeableEntry => MessageKind::Syscall,
            MessageType::CompleteAwakeableEntry => MessageKind::Syscall,
            MessageType::SideEffectEntry => MessageKind::Syscall,
            MessageType::GetPromiseEntry => MessageKind::State,
            MessageType::PeekPromiseEntry => MessageKind::State,
            MessageType::CompletePromiseEntry => MessageKind::State,
            MessageType::CancelInvocationEntry => MessageKind::Syscall,
            MessageType::GetCallInvocationIdEntry => MessageKind::Syscall,
            MessageType::AttachInvocationEntry => MessageKind::Syscall,
            MessageType::GetInvocationOutputEntry => MessageKind::Syscall,
            MessageType::CustomEntry(_) => MessageKind::CustomEntry,
        }
    }

    fn has_completed_flag(&self) -> bool {
        matches!(
            self,
            MessageType::GetStateEntry
                | MessageType::GetStateKeysEntry
                | MessageType::SleepEntry
                | MessageType::InvokeEntry
                | MessageType::AwakeableEntry
                | MessageType::GetPromiseEntry
                | MessageType::PeekPromiseEntry
                | MessageType::CompletePromiseEntry
                | MessageType::GetCallInvocationIdEntry
                | MessageType::AttachInvocationEntry
                | MessageType::GetInvocationOutputEntry
        )
    }

    fn has_requires_ack_flag(&self) -> bool {
        matches!(
            self.kind(),
            MessageKind::State | MessageKind::IO | MessageKind::Syscall | MessageKind::CustomEntry
        )
    }
}

const START_MESSAGE_TYPE: u16 = 0x0000;
const COMPLETION_MESSAGE_TYPE: u16 = 0x0001;
const SUSPENSION_MESSAGE_TYPE: u16 = 0x0002;
const ERROR_MESSAGE_TYPE: u16 = 0x0003;
const ENTRY_ACK_MESSAGE_TYPE: u16 = 0x0004;
const END_MESSAGE_TYPE: u16 = 0x0005;
const INPUT_ENTRY_MESSAGE_TYPE: u16 = 0x0400;
const OUTPUT_ENTRY_MESSAGE_TYPE: u16 = 0x0401;
const GET_STATE_ENTRY_MESSAGE_TYPE: u16 = 0x0800;
const SET_STATE_ENTRY_MESSAGE_TYPE: u16 = 0x0801;
const CLEAR_STATE_ENTRY_MESSAGE_TYPE: u16 = 0x0802;
const CLEAR_ALL_STATE_ENTRY_MESSAGE_TYPE: u16 = 0x0803;
const GET_STATE_KEYS_ENTRY_MESSAGE_TYPE: u16 = 0x0804;
const GET_PROMISE_ENTRY_MESSAGE_TYPE: u16 = 0x0808;
const PEEK_PROMISE_ENTRY_MESSAGE_TYPE: u16 = 0x0809;
const COMPLETE_PROMISE_ENTRY_MESSAGE_TYPE: u16 = 0x080A;
const SLEEP_ENTRY_MESSAGE_TYPE: u16 = 0x0C00;
const INVOKE_ENTRY_MESSAGE_TYPE: u16 = 0x0C01;
const BACKGROUND_INVOKE_ENTRY_MESSAGE_TYPE: u16 = 0x0C02;
const AWAKEABLE_ENTRY_MESSAGE_TYPE: u16 = 0x0C03;
const COMPLETE_AWAKEABLE_ENTRY_MESSAGE_TYPE: u16 = 0x0C04;
const SIDE_EFFECT_ENTRY_MESSAGE_TYPE: u16 = 0x0C05;
const CANCEL_INVOCATION_ENTRY_MESSAGE_TYPE: u16 = 0x0C06;
const GET_CALL_INVOCATION_ID_ENTRY_MESSAGE_TYPE: u16 = 0x0C07;
const ATTACH_INVOCATION_ENTRY_MESSAGE_TYPE: u16 = 0x0C08;
const GET_INVOCATION_OUTPUT_ENTRY_MESSAGE_TYPE: u16 = 0x0C09;

impl From<MessageType> for MessageTypeId {
    fn from(mt: MessageType) -> Self {
        match mt {
            MessageType::Start => START_MESSAGE_TYPE,
            MessageType::Completion => COMPLETION_MESSAGE_TYPE,
            MessageType::Suspension => SUSPENSION_MESSAGE_TYPE,
            MessageType::Error => ERROR_MESSAGE_TYPE,
            MessageType::End => END_MESSAGE_TYPE,
            MessageType::EntryAck => ENTRY_ACK_MESSAGE_TYPE,
            MessageType::InputEntry => INPUT_ENTRY_MESSAGE_TYPE,
            MessageType::OutputEntry => OUTPUT_ENTRY_MESSAGE_TYPE,
            MessageType::GetStateEntry => GET_STATE_ENTRY_MESSAGE_TYPE,
            MessageType::SetStateEntry => SET_STATE_ENTRY_MESSAGE_TYPE,
            MessageType::ClearStateEntry => CLEAR_STATE_ENTRY_MESSAGE_TYPE,
            MessageType::ClearAllStateEntry => CLEAR_ALL_STATE_ENTRY_MESSAGE_TYPE,
            MessageType::GetStateKeysEntry => GET_STATE_KEYS_ENTRY_MESSAGE_TYPE,
            MessageType::SleepEntry => SLEEP_ENTRY_MESSAGE_TYPE,
            MessageType::InvokeEntry => INVOKE_ENTRY_MESSAGE_TYPE,
            MessageType::BackgroundInvokeEntry => BACKGROUND_INVOKE_ENTRY_MESSAGE_TYPE,
            MessageType::AwakeableEntry => AWAKEABLE_ENTRY_MESSAGE_TYPE,
            MessageType::CompleteAwakeableEntry => COMPLETE_AWAKEABLE_ENTRY_MESSAGE_TYPE,
            MessageType::SideEffectEntry => SIDE_EFFECT_ENTRY_MESSAGE_TYPE,
            MessageType::GetPromiseEntry => GET_PROMISE_ENTRY_MESSAGE_TYPE,
            MessageType::PeekPromiseEntry => PEEK_PROMISE_ENTRY_MESSAGE_TYPE,
            MessageType::CompletePromiseEntry => COMPLETE_PROMISE_ENTRY_MESSAGE_TYPE,
            MessageType::CancelInvocationEntry => CANCEL_INVOCATION_ENTRY_MESSAGE_TYPE,
            MessageType::GetCallInvocationIdEntry => GET_CALL_INVOCATION_ID_ENTRY_MESSAGE_TYPE,
            MessageType::AttachInvocationEntry => ATTACH_INVOCATION_ENTRY_MESSAGE_TYPE,
            MessageType::GetInvocationOutputEntry => GET_INVOCATION_OUTPUT_ENTRY_MESSAGE_TYPE,
            MessageType::CustomEntry(id) => id,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unknown message code {0:#x}")]
pub struct UnknownMessageType(u16);

impl TryFrom<MessageTypeId> for MessageType {
    type Error = UnknownMessageType;

    fn try_from(value: MessageTypeId) -> Result<Self, UnknownMessageType> {
        match value {
            START_MESSAGE_TYPE => Ok(MessageType::Start),
            COMPLETION_MESSAGE_TYPE => Ok(MessageType::Completion),
            SUSPENSION_MESSAGE_TYPE => Ok(MessageType::Suspension),
            ERROR_MESSAGE_TYPE => Ok(MessageType::Error),
            END_MESSAGE_TYPE => Ok(MessageType::End),
            ENTRY_ACK_MESSAGE_TYPE => Ok(MessageType::EntryAck),
            INPUT_ENTRY_MESSAGE_TYPE => Ok(MessageType::InputEntry),
            OUTPUT_ENTRY_MESSAGE_TYPE => Ok(MessageType::OutputEntry),
            GET_STATE_ENTRY_MESSAGE_TYPE => Ok(MessageType::GetStateEntry),
            SET_STATE_ENTRY_MESSAGE_TYPE => Ok(MessageType::SetStateEntry),
            CLEAR_STATE_ENTRY_MESSAGE_TYPE => Ok(MessageType::ClearStateEntry),
            GET_STATE_KEYS_ENTRY_MESSAGE_TYPE => Ok(MessageType::GetStateKeysEntry),
            CLEAR_ALL_STATE_ENTRY_MESSAGE_TYPE => Ok(MessageType::ClearAllStateEntry),
            SLEEP_ENTRY_MESSAGE_TYPE => Ok(MessageType::SleepEntry),
            INVOKE_ENTRY_MESSAGE_TYPE => Ok(MessageType::InvokeEntry),
            BACKGROUND_INVOKE_ENTRY_MESSAGE_TYPE => Ok(MessageType::BackgroundInvokeEntry),
            AWAKEABLE_ENTRY_MESSAGE_TYPE => Ok(MessageType::AwakeableEntry),
            COMPLETE_AWAKEABLE_ENTRY_MESSAGE_TYPE => Ok(MessageType::CompleteAwakeableEntry),
            GET_PROMISE_ENTRY_MESSAGE_TYPE => Ok(MessageType::GetPromiseEntry),
            PEEK_PROMISE_ENTRY_MESSAGE_TYPE => Ok(MessageType::PeekPromiseEntry),
            COMPLETE_PROMISE_ENTRY_MESSAGE_TYPE => Ok(MessageType::CompletePromiseEntry),
            SIDE_EFFECT_ENTRY_MESSAGE_TYPE => Ok(MessageType::SideEffectEntry),
            CANCEL_INVOCATION_ENTRY_MESSAGE_TYPE => Ok(MessageType::CancelInvocationEntry),
            GET_CALL_INVOCATION_ID_ENTRY_MESSAGE_TYPE => Ok(MessageType::GetCallInvocationIdEntry),
            ATTACH_INVOCATION_ENTRY_MESSAGE_TYPE => Ok(MessageType::AttachInvocationEntry),
            GET_INVOCATION_OUTPUT_ENTRY_MESSAGE_TYPE => Ok(MessageType::GetInvocationOutputEntry),
            v if ((v & CUSTOM_MESSAGE_MASK) != 0) => Ok(MessageType::CustomEntry(v)),
            v => Err(UnknownMessageType(v)),
        }
    }
}

impl TryFrom<MessageType> for EntryType {
    type Error = MessageType;

    fn try_from(value: MessageType) -> Result<Self, Self::Error> {
        match value {
            MessageType::InputEntry => Ok(EntryType::Input),
            MessageType::OutputEntry => Ok(EntryType::Output),
            MessageType::GetStateEntry => Ok(EntryType::GetState),
            MessageType::SetStateEntry => Ok(EntryType::SetState),
            MessageType::ClearStateEntry => Ok(EntryType::ClearState),
            MessageType::GetStateKeysEntry => Ok(EntryType::GetStateKeys),
            MessageType::ClearAllStateEntry => Ok(EntryType::ClearAllState),
            MessageType::SleepEntry => Ok(EntryType::Sleep),
            MessageType::InvokeEntry => Ok(EntryType::Call),
            MessageType::BackgroundInvokeEntry => Ok(EntryType::OneWayCall),
            MessageType::AwakeableEntry => Ok(EntryType::Awakeable),
            MessageType::CompleteAwakeableEntry => Ok(EntryType::CompleteAwakeable),
            MessageType::SideEffectEntry => Ok(EntryType::Run),
            MessageType::GetPromiseEntry => Ok(EntryType::GetPromise),
            MessageType::PeekPromiseEntry => Ok(EntryType::PeekPromise),
            MessageType::CompletePromiseEntry => Ok(EntryType::CompletePromise),
            MessageType::CancelInvocationEntry => Ok(EntryType::CancelInvocation),
            MessageType::GetCallInvocationIdEntry => Ok(EntryType::GetCallInvocationId),
            MessageType::AttachInvocationEntry => Ok(EntryType::AttachInvocation),
            MessageType::GetInvocationOutputEntry => Ok(EntryType::GetInvocationOutput),
            MessageType::CustomEntry(_) => Ok(EntryType::Custom),
            MessageType::Start
            | MessageType::Completion
            | MessageType::Suspension
            | MessageType::Error
            | MessageType::End
            | MessageType::EntryAck => Err(value),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageHeader {
    ty: MessageType,
    length: u32,

    // --- Flags
    /// Only `CompletableEntries` have completed flag. See [`MessageType#allows_completed_flag`].
    completed_flag: Option<bool>,
    /// All Entry messages may have requires ack flag.
    requires_ack_flag: Option<bool>,
}

impl MessageHeader {
    #[inline]
    pub fn new(ty: MessageType, length: u32) -> Self {
        Self::_new(ty, None, None, length)
    }

    #[inline]
    pub fn new_start(length: u32) -> Self {
        Self::_new(MessageType::Start, None, None, length)
    }

    #[inline]
    pub(super) fn new_entry_header(
        ty: MessageType,
        completed_flag: Option<bool>,
        length: u32,
    ) -> Self {
        debug_assert!(completed_flag.is_some() == ty.has_completed_flag());

        MessageHeader {
            ty,
            length,
            completed_flag,
            // It is always false when sending entries from the runtime
            requires_ack_flag: Some(false),
        }
    }

    #[inline]
    fn _new(
        ty: MessageType,
        completed_flag: Option<bool>,
        requires_ack_flag: Option<bool>,
        length: u32,
    ) -> Self {
        MessageHeader {
            ty,
            length,
            completed_flag,
            requires_ack_flag,
        }
    }

    #[inline]
    pub fn message_kind(&self) -> MessageKind {
        self.ty.kind()
    }

    #[inline]
    pub fn message_type(&self) -> MessageType {
        self.ty
    }

    #[inline]
    pub fn completed(&self) -> Option<bool> {
        self.completed_flag
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

        let completed_flag = read_flag_if!(ty.has_completed_flag(), value, COMPLETED_MASK);
        let requires_ack_flag = read_flag_if!(ty.has_requires_ack_flag(), value, REQUIRES_ACK_MASK);
        let length = value as u32;

        Ok(MessageHeader::_new(
            ty,
            completed_flag,
            requires_ack_flag,
            length,
        ))
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
    fn from(message_header: MessageHeader) -> Self {
        let mut res =
            ((u16::from(message_header.ty) as u64) << 48) | (message_header.length as u64);

        write_flag!(message_header.completed_flag, &mut res, COMPLETED_MASK);
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

    use super::{MessageKind::*, MessageType::*, *};

    impl MessageHeader {
        fn new_completable_entry(ty: MessageType, completed: bool, length: u32) -> Self {
            Self::new_entry_header(ty, Some(completed), length)
        }
    }

    macro_rules! roundtrip_test {
        ($test_name:ident, $header:expr, $ty:expr, $kind:expr, $len:expr) => {
            roundtrip_test!($test_name, $header, $ty, $kind, $len, None, None, None);
        };
        ($test_name:ident, $header:expr, $ty:expr, $kind:expr, $len:expr, version: $protocol_version:expr) => {
            roundtrip_test!(
                $test_name,
                $header,
                $ty,
                $kind,
                $len,
                None,
                Some($protocol_version),
                None
            );
        };
        ($test_name:ident, $header:expr, $ty:expr, $kind:expr, $len:expr, completed: $completed:expr) => {
            roundtrip_test!(
                $test_name,
                $header,
                $ty,
                $kind,
                $len,
                Some($completed),
                None,
                None
            );
        };
        ($test_name:ident, $header:expr, $ty:expr, $kind:expr, $len:expr, requires_ack: $requires_ack:expr) => {
            roundtrip_test!(
                $test_name,
                $header,
                $ty,
                $kind,
                $len,
                None,
                None,
                Some($requires_ack)
            );
        };
        ($test_name:ident, $header:expr, $ty:expr, $kind:expr, $len:expr, requires_ack: $requires_ack:expr, completed: $completed:expr) => {
            roundtrip_test!(
                $test_name,
                $header,
                $ty,
                $kind,
                $len,
                Some($completed),
                None,
                Some($requires_ack)
            );
        };
        ($test_name:ident, $header:expr, $ty:expr, $kind:expr, $len:expr, $completed:expr, $protocol_version:expr, $requires_ack:expr) => {
            #[test]
            fn $test_name() {
                let serialized: u64 = $header.into();
                let header: MessageHeader = serialized.try_into().unwrap();

                assert_eq!(header.message_type(), $ty);
                assert_eq!(header.message_kind(), $kind);
                assert_eq!(header.completed(), $completed);
                assert_eq!(header.requires_ack(), $requires_ack);
                assert_eq!(header.frame_length(), $len);
            }
        };
    }

    roundtrip_test!(
        start,
        MessageHeader::new_start(25),
        Start,
        Core,
        25,
        version: 1
    );

    roundtrip_test!(
        completion,
        MessageHeader::new(Completion, 22),
        Completion,
        Core,
        22
    );

    roundtrip_test!(
        completed_get_state,
        MessageHeader::new_completable_entry(GetStateEntry, true, 0),
        GetStateEntry,
        State,
        0,
        requires_ack: false,
        completed: true
    );

    roundtrip_test!(
        not_completed_get_state,
        MessageHeader::new_completable_entry(GetStateEntry, false, 0),
        GetStateEntry,
        State,
        0,
        requires_ack: false,
        completed: false
    );

    roundtrip_test!(
        completed_get_state_with_len,
        MessageHeader::new_completable_entry(GetStateEntry, true, 10341),
        GetStateEntry,
        State,
        10341,
        requires_ack: false,
        completed: true
    );

    roundtrip_test!(
        set_state_with_requires_ack,
        MessageHeader::_new(SetStateEntry, None, Some(true), 10341),
        SetStateEntry,
        State,
        10341,
        requires_ack: true
    );

    roundtrip_test!(
        custom_entry,
        MessageHeader::new(MessageType::CustomEntry(0xFC00), 10341),
        MessageType::CustomEntry(0xFC00),
        MessageKind::CustomEntry,
        10341,
        requires_ack: false
    );

    roundtrip_test!(
        custom_entry_with_requires_ack,
        MessageHeader::_new(MessageType::CustomEntry(0xFC00), None, Some(true), 10341),
        MessageType::CustomEntry(0xFC00),
        MessageKind::CustomEntry,
        10341,
        requires_ack: true
    );
}
