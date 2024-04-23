// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This crate contains the code-generated structs of [service-protocol](https://github.com/restatedev/service-protocol) and the codec to use them.

pub const RESTATE_SERVICE_PROTOCOL_VERSION: u16 = 2;

#[cfg(feature = "codec")]
pub mod codec;
#[cfg(feature = "discovery")]
pub mod discovery;
#[cfg(feature = "message")]
pub mod message;

#[cfg(feature = "awakeable-id")]
pub mod awakeable_id;

#[cfg(feature = "protocol")]
pub mod pb {
    pub mod protocol {
        #![allow(warnings)]
        #![allow(clippy::all)]
        #![allow(unknown_lints)]
        include!(concat!(env!("OUT_DIR"), "/dev.restate.service.protocol.rs"));
    }
}

/// This module implements conversions back and forth from proto messages to [`journal::Entry`] model.
/// These are used by the [`codec::ProtobufRawEntryCodec`].
#[cfg(feature = "codec")]
mod pb_into {
    use super::pb::protocol::*;

    use restate_types::journal::*;

    impl TryFrom<InputEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: InputEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::Input(InputEntry { value: msg.value }))
        }
    }

    impl TryFrom<OutputEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: OutputEntryMessage) -> Result<Self, Self::Error> {
            Ok(Entry::Output(OutputEntry {
                result: match msg.result.ok_or("result")? {
                    output_entry_message::Result::Value(r) => EntryResult::Success(r),
                    output_entry_message::Result::Failure(Failure { code, message }) => {
                        EntryResult::Failure(code.into(), message.into())
                    }
                },
            }))
        }
    }

    impl TryFrom<GetStateEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: GetStateEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::GetState(GetStateEntry {
                key: msg.key,
                value: msg.result.map(|v| match v {
                    get_state_entry_message::Result::Empty(_) => GetStateResult::Empty,
                    get_state_entry_message::Result::Value(b) => GetStateResult::Result(b),
                    get_state_entry_message::Result::Failure(failure) => {
                        GetStateResult::Failure(failure.code.into(), failure.message.into())
                    }
                }),
            }))
        }
    }

    impl TryFrom<SetStateEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: SetStateEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::SetState(SetStateEntry {
                key: msg.key,
                value: msg.value,
            }))
        }
    }

    impl TryFrom<ClearStateEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: ClearStateEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::ClearState(ClearStateEntry { key: msg.key }))
        }
    }

    impl TryFrom<GetStateKeysEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: GetStateKeysEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::GetStateKeys(GetStateKeysEntry {
                value: msg.result.map(|v| match v {
                    get_state_keys_entry_message::Result::Value(b) => {
                        GetStateKeysResult::Result(b.keys)
                    }
                    get_state_keys_entry_message::Result::Failure(failure) => {
                        GetStateKeysResult::Failure(failure.code.into(), failure.message.into())
                    }
                }),
            }))
        }
    }

    impl TryFrom<ClearAllStateEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(_: ClearAllStateEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::ClearAllState)
        }
    }

    impl TryFrom<SleepEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: SleepEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::Sleep(SleepEntry {
                wake_up_time: msg.wake_up_time,
                result: msg.result.map(|r| match r {
                    sleep_entry_message::Result::Empty(_) => SleepResult::Fired,
                    sleep_entry_message::Result::Failure(failure) => {
                        SleepResult::Failure(failure.code.into(), failure.message.into())
                    }
                }),
            }))
        }
    }

    impl TryFrom<CallEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: CallEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::Call(InvokeEntry {
                request: InvokeRequest {
                    service_name: msg.service_name.into(),
                    handler_name: msg.handler_name.into(),
                    parameter: msg.parameter,
                    key: msg.key.into(),
                },
                result: msg.result.map(|v| match v {
                    call_entry_message::Result::Value(r) => EntryResult::Success(r),
                    call_entry_message::Result::Failure(Failure { code, message }) => {
                        EntryResult::Failure(code.into(), message.into())
                    }
                }),
            }))
        }
    }

    impl TryFrom<OneWayCallEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: OneWayCallEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::OneWayCall(OneWayCallEntry {
                request: InvokeRequest {
                    service_name: msg.service_name.into(),
                    handler_name: msg.handler_name.into(),
                    parameter: msg.parameter,
                    key: msg.key.into(),
                },
                invoke_time: msg.invoke_time,
            }))
        }
    }

    impl TryFrom<AwakeableEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: AwakeableEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::Awakeable(AwakeableEntry {
                result: msg.result.map(|v| match v {
                    awakeable_entry_message::Result::Value(r) => EntryResult::Success(r),
                    awakeable_entry_message::Result::Failure(Failure { code, message }) => {
                        EntryResult::Failure(code.into(), message.into())
                    }
                }),
            }))
        }
    }

    impl TryFrom<CompleteAwakeableEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: CompleteAwakeableEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::CompleteAwakeable(CompleteAwakeableEntry {
                id: msg.id.into(),
                result: match msg.result.ok_or("result")? {
                    complete_awakeable_entry_message::Result::Value(r) => EntryResult::Success(r),
                    complete_awakeable_entry_message::Result::Failure(Failure {
                        code,
                        message,
                    }) => EntryResult::Failure(code.into(), message.into()),
                },
            }))
        }
    }

    impl TryFrom<RunEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: RunEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::Run(RunEntry {
                result: match msg.result.ok_or("result")? {
                    run_entry_message::Result::Value(r) => EntryResult::Success(r),
                    run_entry_message::Result::Failure(Failure { code, message }) => {
                        EntryResult::Failure(code.into(), message.into())
                    }
                },
            }))
        }
    }
}
