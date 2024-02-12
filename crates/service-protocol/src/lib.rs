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

    impl TryFrom<PollInputStreamEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: PollInputStreamEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::PollInputStream(PollInputStreamEntry {
                result: match msg.result.ok_or("result")? {
                    poll_input_stream_entry_message::Result::Value(r) => EntryResult::Success(r),
                    poll_input_stream_entry_message::Result::Failure(Failure { code, message }) => {
                        EntryResult::Failure(code.into(), message.into())
                    }
                },
            }))
        }
    }

    impl TryFrom<OutputStreamEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: OutputStreamEntryMessage) -> Result<Self, Self::Error> {
            Ok(Entry::OutputStream(OutputStreamEntry {
                result: match msg.result.ok_or("result")? {
                    output_stream_entry_message::Result::Value(r) => EntryResult::Success(r),
                    output_stream_entry_message::Result::Failure(Failure { code, message }) => {
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

    impl TryFrom<InvokeEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: InvokeEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::Invoke(InvokeEntry {
                request: InvokeRequest {
                    service_name: msg.service_name.into(),
                    method_name: msg.method_name.into(),
                    parameter: msg.parameter,
                },
                result: msg.result.map(|v| match v {
                    invoke_entry_message::Result::Value(r) => EntryResult::Success(r),
                    invoke_entry_message::Result::Failure(Failure { code, message }) => {
                        EntryResult::Failure(code.into(), message.into())
                    }
                }),
            }))
        }
    }

    impl TryFrom<BackgroundInvokeEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: BackgroundInvokeEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::BackgroundInvoke(BackgroundInvokeEntry {
                request: InvokeRequest {
                    service_name: msg.service_name.into(),
                    method_name: msg.method_name.into(),
                    parameter: msg.parameter,
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
}
