// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::pb::protocol;

use crate::pb::protocol::poll_input_stream_entry_message;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message;
use restate_types::journal::enriched::{EnrichedEntryHeader, EnrichedRawEntry};
use restate_types::journal::raw::*;
use restate_types::journal::{CompletionResult, Entry, EntryType};
use std::fmt::Debug;
use std::mem;

/// This macro generates the pattern matching with arms per entry.
/// For each entry it first executes `Message#decode` and then `try_into()`.
/// It expects that for each `{...}Entry` there is a valid `TryFrom<{...}Message>` implementation with `Error = &'static str`.
/// These implementations are available in [`super::pb_into`].
macro_rules! match_decode {
    ($ty:expr, $buf:expr, { $($variant:ident),* }) => {
        match $ty {
              $(EntryType::$variant { .. } => paste::paste! {
                  protocol::[<$variant EntryMessage>]::decode($buf)
                    .map_err(|e| RawEntryCodecError::new($ty.clone(), ErrorKind::Decode { source: Some(e.into()) }))
                    .and_then(|msg| msg.try_into().map_err(|f| RawEntryCodecError::new($ty.clone(), ErrorKind::MissingField(f))))
              },)*
             EntryType::Custom => Ok(Entry::Custom($buf.copy_to_bytes($buf.remaining()))),
        }
    };
}

#[derive(Debug, Default, Copy, Clone)]
pub struct ProtobufRawEntryCodec;

impl RawEntryCodec for ProtobufRawEntryCodec {
    fn serialize_as_unary_input_entry(value: Bytes) -> EnrichedRawEntry {
        RawEntry::new(
            EnrichedEntryHeader::PollInputStream { is_completed: true },
            protocol::PollInputStreamEntryMessage {
                result: Some(poll_input_stream_entry_message::Result::Value(value)),
            }
            .encode_to_vec()
            .into(),
        )
    }

    fn serialize_get_state_keys_completion(keys: Vec<Bytes>) -> CompletionResult {
        CompletionResult::Success(
            protocol::get_state_keys_entry_message::StateKeys { keys }
                .encode_to_vec()
                .into(),
        )
    }

    fn deserialize(
        entry_type: EntryType,
        mut entry_value: Bytes,
    ) -> Result<Entry, RawEntryCodecError> {
        // We clone the entry Bytes here to ensure that the generated Message::decode
        // invocation reuses the same underlying byte array.
        match_decode!(entry_type, entry_value, {
            PollInputStream,
            OutputStream,
            GetState,
            SetState,
            ClearState,
            ClearAllState,
            GetStateKeys,
            Sleep,
            Invoke,
            BackgroundInvoke,
            Awakeable,
            CompleteAwakeable
        })
    }

    fn write_completion<InvokeEnrichmentResult: Debug, AwakeableEnrichmentResult: Debug>(
        entry: &mut RawEntry<InvokeEnrichmentResult, AwakeableEnrichmentResult>,
        completion_result: CompletionResult,
    ) -> Result<(), RawEntryCodecError> {
        debug_assert_eq!(
            entry.header().is_completed(),
            Some(false),
            "Entry '{:?}' is already completed",
            entry
        );

        // Prepare the result to serialize in protobuf
        let completion_result_message = match completion_result {
            CompletionResult::Empty => protocol::completion_message::Result::Empty(()),
            CompletionResult::Success(b) => protocol::completion_message::Result::Value(b),
            CompletionResult::Failure(code, message) => {
                protocol::completion_message::Result::Failure(protocol::Failure {
                    code: code.into(),
                    message: message.to_string(),
                })
            }
        };

        // Prepare a buffer for the result
        // TODO perhaps use SegmentedBuf here to avoid allocating?
        let len = entry.serialized_entry().len() + completion_result_message.encoded_len();
        let mut result_buf = BytesMut::with_capacity(len);

        // Concatenate entry + result
        // The reason why encoding completion_message_result works is that by convention the tags
        // of completion message are the same used by completable entries.
        // See the service_protocol protobuf definition for more details.
        // https://protobuf.dev/programming-guides/encoding/#last-one-wins
        result_buf.put(mem::take(entry.serialized_entry_mut()));
        completion_result_message.encode(&mut result_buf);

        // Write back to the entry the new buffer and the completed flag
        *entry.serialized_entry_mut() = result_buf.freeze();
        entry.header_mut().mark_completed();

        Ok(())
    }
}

#[cfg(feature = "mocks")]
mod mocks {
    use std::str::FromStr;

    use super::*;

    use crate::awakeable_id::AwakeableIdentifier;
    use crate::pb::protocol::{
        awakeable_entry_message, complete_awakeable_entry_message, get_state_entry_message,
        get_state_keys_entry_message, invoke_entry_message, output_stream_entry_message,
        AwakeableEntryMessage, BackgroundInvokeEntryMessage, ClearAllStateEntryMessage,
        ClearStateEntryMessage, CompleteAwakeableEntryMessage, Failure, GetStateEntryMessage,
        GetStateKeysEntryMessage, InvokeEntryMessage, OutputStreamEntryMessage,
        PollInputStreamEntryMessage, SetStateEntryMessage,
    };
    use restate_types::journal::enriched::{
        AwakeableEnrichmentResult, EnrichedEntryHeader, EnrichedRawEntry,
    };
    use restate_types::journal::{
        AwakeableEntry, CompletableEntry, CompleteAwakeableEntry, EntryResult, GetStateKeysEntry,
        GetStateKeysResult, GetStateResult, PollInputStreamEntry,
    };

    impl ProtobufRawEntryCodec {
        pub fn serialize(entry: Entry) -> PlainRawEntry {
            match entry {
                Entry::PollInputStream(entry) => PlainRawEntry::new(
                    PlainEntryHeader::PollInputStream { is_completed: true },
                    Self::serialize_poll_input_stream_entry(entry),
                ),
                Entry::OutputStream(entry) => PlainRawEntry::new(
                    PlainEntryHeader::OutputStream {},
                    OutputStreamEntryMessage {
                        result: Some(match entry.result {
                            EntryResult::Success(s) => {
                                output_stream_entry_message::Result::Value(s)
                            }
                            EntryResult::Failure(code, msg) => {
                                output_stream_entry_message::Result::Failure(Failure {
                                    code: code.into(),
                                    message: msg.to_string(),
                                })
                            }
                        }),
                    }
                    .encode_to_vec()
                    .into(),
                ),
                Entry::GetState(entry) => PlainRawEntry::new(
                    PlainEntryHeader::GetState {
                        is_completed: entry.is_completed(),
                    },
                    GetStateEntryMessage {
                        key: entry.key,
                        result: entry.value.map(|value| match value {
                            GetStateResult::Empty => get_state_entry_message::Result::Empty(()),
                            GetStateResult::Result(v) => get_state_entry_message::Result::Value(v),
                            GetStateResult::Failure(code, reason) => {
                                get_state_entry_message::Result::Failure(Failure {
                                    code: code.into(),
                                    message: reason.to_string(),
                                })
                            }
                        }),
                    }
                    .encode_to_vec()
                    .into(),
                ),
                Entry::SetState(entry) => PlainRawEntry::new(
                    PlainEntryHeader::SetState {},
                    SetStateEntryMessage {
                        key: entry.key,
                        value: entry.value,
                    }
                    .encode_to_vec()
                    .into(),
                ),
                Entry::ClearState(entry) => PlainRawEntry::new(
                    PlainEntryHeader::ClearState {},
                    ClearStateEntryMessage { key: entry.key }
                        .encode_to_vec()
                        .into(),
                ),
                Entry::ClearAllState => PlainRawEntry::new(
                    PlainEntryHeader::ClearAllState {},
                    ClearAllStateEntryMessage {}.encode_to_vec().into(),
                ),
                Entry::Invoke(entry) => PlainRawEntry::new(
                    PlainEntryHeader::Invoke {
                        is_completed: entry.is_completed(),
                        enrichment_result: None,
                    },
                    InvokeEntryMessage {
                        service_name: entry.request.service_name.into(),
                        method_name: entry.request.method_name.into(),
                        parameter: entry.request.parameter,
                        result: entry.result.map(|r| match r {
                            EntryResult::Success(v) => invoke_entry_message::Result::Value(v),
                            EntryResult::Failure(code, msg) => {
                                invoke_entry_message::Result::Failure(Failure {
                                    code: code.into(),
                                    message: msg.to_string(),
                                })
                            }
                        }),
                    }
                    .encode_to_vec()
                    .into(),
                ),
                Entry::BackgroundInvoke(entry) => PlainRawEntry::new(
                    PlainEntryHeader::BackgroundInvoke {
                        enrichment_result: (),
                    },
                    BackgroundInvokeEntryMessage {
                        service_name: entry.request.service_name.into(),
                        method_name: entry.request.method_name.into(),
                        parameter: entry.request.parameter,
                        invoke_time: entry.invoke_time,
                    }
                    .encode_to_vec()
                    .into(),
                ),
                Entry::CompleteAwakeable(entry) => PlainRawEntry::new(
                    PlainEntryHeader::CompleteAwakeable {
                        enrichment_result: (),
                    },
                    Self::serialize_complete_awakeable_entry(entry),
                ),
                Entry::Awakeable(entry) => PlainRawEntry::new(
                    PlainEntryHeader::Awakeable {
                        is_completed: entry.is_completed(),
                    },
                    Self::serialize_awakeable_entry(entry),
                ),
                _ => unimplemented!(),
            }
        }

        pub fn serialize_enriched(entry: Entry) -> EnrichedRawEntry {
            match entry {
                Entry::PollInputStream(entry) => EnrichedRawEntry::new(
                    EnrichedEntryHeader::PollInputStream { is_completed: true },
                    Self::serialize_poll_input_stream_entry(entry),
                ),
                Entry::CompleteAwakeable(entry) => {
                    let (invocation_id, entry_index) = AwakeableIdentifier::from_str(&entry.id)
                        .unwrap()
                        .into_inner();

                    EnrichedRawEntry::new(
                        EnrichedEntryHeader::CompleteAwakeable {
                            enrichment_result: AwakeableEnrichmentResult {
                                invocation_id,
                                entry_index,
                            },
                        },
                        Self::serialize_complete_awakeable_entry(entry),
                    )
                }
                Entry::SetState(entry) => EnrichedRawEntry::new(
                    EnrichedEntryHeader::SetState {},
                    SetStateEntryMessage {
                        key: entry.key,
                        value: entry.value,
                    }
                    .encode_to_vec()
                    .into(),
                ),
                Entry::ClearState(entry) => EnrichedRawEntry::new(
                    EnrichedEntryHeader::ClearState {},
                    ClearStateEntryMessage { key: entry.key }
                        .encode_to_vec()
                        .into(),
                ),
                Entry::ClearAllState => EnrichedRawEntry::new(
                    EnrichedEntryHeader::ClearAllState {},
                    ClearAllStateEntryMessage {}.encode_to_vec().into(),
                ),
                Entry::GetStateKeys(entry) => EnrichedRawEntry::new(
                    EnrichedEntryHeader::GetStateKeys {
                        is_completed: entry.is_completed(),
                    },
                    Self::serialize_get_state_keys_entry(entry),
                ),
                Entry::Awakeable(entry) => EnrichedRawEntry::new(
                    EnrichedEntryHeader::Awakeable {
                        is_completed: entry.is_completed(),
                    },
                    Self::serialize_awakeable_entry(entry),
                ),
                _ => unimplemented!(),
            }
        }

        fn serialize_poll_input_stream_entry(
            PollInputStreamEntry { result }: PollInputStreamEntry,
        ) -> Bytes {
            PollInputStreamEntryMessage {
                result: Some(match result {
                    EntryResult::Success(value) => {
                        poll_input_stream_entry_message::Result::Value(value)
                    }
                    EntryResult::Failure(code, reason) => {
                        poll_input_stream_entry_message::Result::Failure(Failure {
                            code: code.into(),
                            message: reason.to_string(),
                        })
                    }
                }),
            }
            .encode_to_vec()
            .into()
        }

        fn serialize_get_state_keys_entry(GetStateKeysEntry { value }: GetStateKeysEntry) -> Bytes {
            GetStateKeysEntryMessage {
                result: value.map(|v| match v {
                    GetStateKeysResult::Result(keys) => {
                        get_state_keys_entry_message::Result::Value(
                            get_state_keys_entry_message::StateKeys { keys },
                        )
                    }
                    GetStateKeysResult::Failure(code, reason) => {
                        get_state_keys_entry_message::Result::Failure(Failure {
                            code: code.into(),
                            message: reason.to_string(),
                        })
                    }
                }),
            }
            .encode_to_vec()
            .into()
        }

        fn serialize_awakeable_entry(AwakeableEntry { result }: AwakeableEntry) -> Bytes {
            AwakeableEntryMessage {
                result: result.map(|r| match r {
                    EntryResult::Success(success) => {
                        awakeable_entry_message::Result::Value(success)
                    }
                    EntryResult::Failure(code, reason) => {
                        awakeable_entry_message::Result::Failure(Failure {
                            code: code.into(),
                            message: reason.to_string(),
                        })
                    }
                }),
            }
            .encode_to_vec()
            .into()
        }

        fn serialize_complete_awakeable_entry(
            CompleteAwakeableEntry { id, result }: CompleteAwakeableEntry,
        ) -> Bytes {
            CompleteAwakeableEntryMessage {
                id: id.to_string(),
                result: Some(match result {
                    EntryResult::Success(success) => {
                        complete_awakeable_entry_message::Result::Value(success)
                    }
                    EntryResult::Failure(code, reason) => {
                        complete_awakeable_entry_message::Result::Failure(Failure {
                            code: code.into(),
                            message: reason.to_string(),
                        })
                    }
                }),
            }
            .encode_to_vec()
            .into()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;
    use restate_types::journal::EntryResult;

    #[test]
    fn complete_invoke() {
        let invoke_result = Bytes::from_static(b"output");

        // Create an invoke entry
        let raw_entry: PlainRawEntry = RawEntry::new(
            PlainEntryHeader::Invoke {
                is_completed: false,
                enrichment_result: None,
            },
            protocol::InvokeEntryMessage {
                service_name: "MySvc".to_string(),
                method_name: "MyMethod".to_string(),

                parameter: Bytes::from_static(b"input"),
                ..protocol::InvokeEntryMessage::default()
            }
            .encode_to_vec()
            .into(),
        );

        // Complete the expected entry directly on the materialized model
        let mut expected_entry = raw_entry
            .deserialize_entry_ref::<ProtobufRawEntryCodec>()
            .unwrap();
        match &mut expected_entry {
            Entry::Invoke(invoke_entry_inner) => {
                invoke_entry_inner.result = Some(EntryResult::Success(invoke_result.clone()))
            }
            _ => unreachable!(),
        };

        // Complete the raw entry
        let mut actual_raw_entry = raw_entry;
        ProtobufRawEntryCodec::write_completion(
            &mut actual_raw_entry,
            CompletionResult::Success(invoke_result),
        )
        .unwrap();
        let actual_entry = actual_raw_entry
            .deserialize_entry_ref::<ProtobufRawEntryCodec>()
            .unwrap();

        assert_eq!(actual_raw_entry.header().is_completed(), Some(true));
        assert_eq!(actual_entry, expected_entry);
    }
}
