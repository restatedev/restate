//! This crate contains the code-generated structs of [service-protocol](https://github.com/restatedev/service-protocol) and the codec to use them.

pub mod codec;

pub mod pb {
    #![allow(warnings)]
    #![allow(clippy::all)]
    #![allow(unknown_lints)]
    include!(concat!(env!("OUT_DIR"), "/dev.restate.service.protocol.rs"));
}

/// This module implements conversions back and forth from proto messages to [`journal::Entry`] model.
/// These are used by the [`codec::ProtobufRawEntryCodec`].
mod pb_into {
    use journal::*;

    use super::pb::*;

    impl TryFrom<PollInputStreamEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: PollInputStreamEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::PollInputStream(PollInputStreamEntry {
                result: msg.value,
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
                        EntryResult::Failure(code, message.into())
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
                    get_state_entry_message::Result::Empty(_) => GetStateValue::Empty,
                    get_state_entry_message::Result::Value(b) => GetStateValue::Value(b),
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

    impl TryFrom<SleepEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: SleepEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::Sleep(SleepEntry {
                wake_up_time: msg.wake_up_time,
                fired: msg.result.is_some(),
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
                        EntryResult::Failure(code, message.into())
                    }
                }),
            }))
        }
    }

    impl TryFrom<BackgroundInvokeEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: BackgroundInvokeEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::BackgroundInvoke(BackgroundInvokeEntry(
                InvokeRequest {
                    service_name: msg.service_name.into(),
                    method_name: msg.method_name.into(),
                    parameter: msg.parameter,
                },
            )))
        }
    }

    impl TryFrom<AwakeableEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: AwakeableEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::Awakeable(AwakeableEntry {
                result: msg.result.map(|v| match v {
                    awakeable_entry_message::Result::Value(r) => EntryResult::Success(r),
                    awakeable_entry_message::Result::Failure(Failure { code, message }) => {
                        EntryResult::Failure(code, message.into())
                    }
                }),
            }))
        }
    }

    impl TryFrom<CompleteAwakeableEntryMessage> for Entry {
        type Error = &'static str;

        fn try_from(msg: CompleteAwakeableEntryMessage) -> Result<Self, Self::Error> {
            Ok(Self::CompleteAwakeable(CompleteAwakeableEntry {
                service_name: msg.service_name.into(),
                instance_key: msg.instance_key,
                invocation_id: msg.invocation_id,
                entry_index: msg.entry_index,
                result: EntryResult::Success(msg.payload),
            }))
        }
    }
}
