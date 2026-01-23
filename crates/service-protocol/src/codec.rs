// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
use std::mem;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message;

use restate_types::invocation::Header;
use restate_types::journal::enriched::{EnrichedEntryHeader, EnrichedRawEntry};
use restate_types::journal::raw::*;
use restate_types::journal::{CompletionResult, Entry, EntryType};
use restate_types::service_protocol;

/// This macro generates the pattern matching with arms per entry.
/// For each entry it first executes `Message#decode` and then `try_into()`.
/// It expects that for each `{...}Entry` there is a valid `TryFrom<{...}Message>` implementation with `Error = &'static str`.
/// These implementations are available in [`super::pb_into`].
macro_rules! match_decode {
    ($ty:expr, $buf:expr, { $($variant:ident),* }) => {
        match $ty {
              $(EntryType::$variant { .. } => paste::paste! {
                  service_protocol::[<$variant EntryMessage>]::decode($buf)
                    .map_err(|e| RawEntryCodecError::new($ty.clone(), ErrorKind::Decode { source: Some(e.into()) }))
                    .and_then(|msg| msg.try_into().map_err(|f| RawEntryCodecError::new($ty.clone(), ErrorKind::MissingField(f))))
              },)*
             EntryType::Custom => Ok(Entry::Custom($buf.copy_to_bytes($buf.remaining()))),
        }
    };
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct NamedEntryTemplate {
    // By spec the field `name` is always tag 12
    #[prost(string, optional, tag = "12")]
    name: Option<String>,
}

#[derive(Debug, Default, Copy, Clone)]
pub struct ProtobufRawEntryCodec;

impl RawEntryCodec for ProtobufRawEntryCodec {
    fn serialize_as_input_entry(headers: Vec<Header>, value: Bytes) -> EnrichedRawEntry {
        RawEntry::new(
            EnrichedEntryHeader::Input {},
            service_protocol::InputEntryMessage {
                headers: headers
                    .into_iter()
                    .map(|h| service_protocol::Header {
                        key: h.name.into(),
                        value: h.value.into(),
                    })
                    .collect(),
                value,
                ..Default::default()
            }
            .encode_to_vec()
            .into(),
        )
    }

    fn serialize_get_state_keys_completion(keys: Vec<Bytes>) -> CompletionResult {
        CompletionResult::Success(
            service_protocol::get_state_keys_entry_message::StateKeys { keys }
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
            Input,
            Output,
            GetState,
            SetState,
            ClearState,
            ClearAllState,
            GetStateKeys,
            GetPromise,
            PeekPromise,
            CompletePromise,
            Sleep,
            Call,
            OneWayCall,
            Awakeable,
            CompleteAwakeable,
            Run,
            CancelInvocation,
            GetCallInvocationId,
            AttachInvocation,
            GetInvocationOutput
        })
    }

    fn read_entry_name(
        entry_type: EntryType,
        entry_value: Bytes,
    ) -> Result<Option<String>, RawEntryCodecError> {
        Ok(NamedEntryTemplate::decode(entry_value)
            .map_err(|e| {
                RawEntryCodecError::new(
                    entry_type,
                    ErrorKind::Decode {
                        source: Some(e.into()),
                    },
                )
            })?
            .name)
    }

    fn write_completion<InvokeEnrichmentResult: Debug, AwakeableEnrichmentResult: Debug>(
        entry: &mut RawEntry<InvokeEnrichmentResult, AwakeableEnrichmentResult>,
        completion_result: CompletionResult,
    ) -> Result<(), RawEntryCodecError> {
        debug_assert_eq!(
            entry.header().is_completed(),
            Some(false),
            "Entry '{entry:?}' is already completed"
        );

        // Prepare the result to serialize in protobuf
        let completion_result_message = match completion_result {
            CompletionResult::Empty => {
                service_protocol::completion_message::Result::Empty(service_protocol::Empty {})
            }
            CompletionResult::Success(b) => service_protocol::completion_message::Result::Value(b),
            CompletionResult::Failure(code, message) => {
                service_protocol::completion_message::Result::Failure(service_protocol::Failure {
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

#[cfg(feature = "test-util")]
mod test_util {
    use std::str::FromStr;

    use super::*;

    use restate_types::identifiers::{AwakeableIdentifier, InvocationId};
    use restate_types::invocation::{InvocationTarget, VirtualObjectHandlerType};
    use restate_types::journal::enriched::{
        AwakeableEnrichmentResult, CallEnrichmentResult, EnrichedEntryHeader, EnrichedRawEntry,
    };
    use restate_types::journal::{
        AttachInvocationEntry, AttachInvocationTarget, AwakeableEntry, CancelInvocationEntry,
        CancelInvocationTarget, CompletableEntry, CompleteAwakeableEntry, EntryResult,
        GetCallInvocationIdEntry, GetCallInvocationIdResult, GetInvocationOutputEntry,
        GetStateKeysEntry, GetStateKeysResult, InputEntry, OutputEntry,
    };
    use restate_types::service_protocol::{
        AttachInvocationEntryMessage, AwakeableEntryMessage, CallEntryMessage,
        CancelInvocationEntryMessage, ClearAllStateEntryMessage, ClearStateEntryMessage,
        CompleteAwakeableEntryMessage, Failure, GetCallInvocationIdEntryMessage,
        GetInvocationOutputEntryMessage, GetStateEntryMessage, GetStateKeysEntryMessage,
        IdempotentRequestTarget, InputEntryMessage, OneWayCallEntryMessage, OutputEntryMessage,
        SetStateEntryMessage, WorkflowTarget, attach_invocation_entry_message,
        awakeable_entry_message, call_entry_message, cancel_invocation_entry_message,
        complete_awakeable_entry_message, get_call_invocation_id_entry_message,
        get_invocation_output_entry_message, get_state_entry_message, get_state_keys_entry_message,
        output_entry_message,
    };

    impl ProtobufRawEntryCodec {
        pub fn serialize(entry: Entry) -> PlainRawEntry {
            Self::serialize_enriched(entry).erase_enrichment()
        }

        pub fn serialize_enriched(entry: Entry) -> EnrichedRawEntry {
            match entry {
                Entry::Input(entry) => EnrichedRawEntry::new(
                    EnrichedEntryHeader::Input {},
                    Self::serialize_input_entry(entry),
                ),
                Entry::Output(entry) => EnrichedRawEntry::new(
                    EnrichedEntryHeader::Output {},
                    Self::serialize_output_entry(entry),
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
                Entry::GetState(entry) => EnrichedRawEntry::new(
                    EnrichedEntryHeader::GetState {
                        is_completed: entry.is_completed(),
                    },
                    GetStateEntryMessage {
                        key: entry.key,
                        result: entry.value.map(|value| match value {
                            CompletionResult::Empty => {
                                get_state_entry_message::Result::Empty(service_protocol::Empty {})
                            }
                            CompletionResult::Success(v) => {
                                get_state_entry_message::Result::Value(v)
                            }
                            CompletionResult::Failure(code, reason) => {
                                get_state_entry_message::Result::Failure(Failure {
                                    code: code.into(),
                                    message: reason.to_string(),
                                })
                            }
                        }),
                        ..Default::default()
                    }
                    .encode_to_vec()
                    .into(),
                ),
                Entry::Call(entry) => {
                    let invocation_id = InvocationId::mock_random();
                    EnrichedRawEntry::new(
                        EnrichedEntryHeader::Call {
                            is_completed: entry.is_completed(),
                            enrichment_result: Some(CallEnrichmentResult {
                                invocation_id,
                                invocation_target: InvocationTarget::VirtualObject {
                                    name: entry.request.service_name.clone(),
                                    key: entry.request.key.clone(),
                                    handler: entry.request.handler_name.clone(),
                                    handler_ty: VirtualObjectHandlerType::Exclusive,
                                },
                                completion_retention_time: None,
                                span_context: Default::default(),
                            }),
                        },
                        CallEntryMessage {
                            service_name: entry.request.service_name.into(),
                            handler_name: entry.request.handler_name.into(),
                            parameter: entry.request.parameter,
                            headers: entry.request.headers.into_iter().map(Into::into).collect(),
                            result: entry.result.map(|r| match r {
                                EntryResult::Success(v) => call_entry_message::Result::Value(v),
                                EntryResult::Failure(code, msg) => {
                                    call_entry_message::Result::Failure(Failure {
                                        code: code.into(),
                                        message: msg.to_string(),
                                    })
                                }
                            }),
                            ..Default::default()
                        }
                        .encode_to_vec()
                        .into(),
                    )
                }
                Entry::OneWayCall(entry) => {
                    let invocation_id = InvocationId::mock_random();

                    EnrichedRawEntry::new(
                        EnrichedEntryHeader::OneWayCall {
                            enrichment_result: CallEnrichmentResult {
                                invocation_id,
                                invocation_target: InvocationTarget::VirtualObject {
                                    name: entry.request.service_name.clone(),
                                    key: entry.request.key.clone(),
                                    handler: entry.request.handler_name.clone(),
                                    handler_ty: VirtualObjectHandlerType::Exclusive,
                                },
                                completion_retention_time: None,
                                span_context: Default::default(),
                            },
                        },
                        OneWayCallEntryMessage {
                            service_name: entry.request.service_name.into(),
                            handler_name: entry.request.handler_name.into(),
                            parameter: entry.request.parameter,
                            headers: entry.request.headers.into_iter().map(Into::into).collect(),
                            invoke_time: entry.invoke_time,
                            ..Default::default()
                        }
                        .encode_to_vec()
                        .into(),
                    )
                }
                Entry::SetState(entry) => EnrichedRawEntry::new(
                    EnrichedEntryHeader::SetState {},
                    SetStateEntryMessage {
                        key: entry.key,
                        value: entry.value,
                        ..Default::default()
                    }
                    .encode_to_vec()
                    .into(),
                ),
                Entry::ClearState(entry) => EnrichedRawEntry::new(
                    EnrichedEntryHeader::ClearState {},
                    ClearStateEntryMessage {
                        key: entry.key,
                        ..Default::default()
                    }
                    .encode_to_vec()
                    .into(),
                ),
                Entry::ClearAllState => EnrichedRawEntry::new(
                    EnrichedEntryHeader::ClearAllState {},
                    ClearAllStateEntryMessage {
                        ..Default::default()
                    }
                    .encode_to_vec()
                    .into(),
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
                Entry::CancelInvocation(entry) => EnrichedRawEntry::new(
                    EnrichedEntryHeader::CancelInvocation {},
                    Self::serialize_cancel_invocation_entry(entry),
                ),
                Entry::GetCallInvocationId(entry) => EnrichedRawEntry::new(
                    EnrichedEntryHeader::GetCallInvocationId {
                        is_completed: entry.is_completed(),
                    },
                    Self::serialize_get_call_invocation_id_entry(entry),
                ),
                Entry::AttachInvocation(entry) => EnrichedRawEntry::new(
                    EnrichedEntryHeader::AttachInvocation {
                        is_completed: entry.is_completed(),
                    },
                    Self::serialize_attach_invocation_entry(entry),
                ),
                Entry::GetInvocationOutput(entry) => EnrichedRawEntry::new(
                    EnrichedEntryHeader::GetInvocationOutput {
                        is_completed: entry.is_completed(),
                    },
                    Self::serialize_get_invocation_output_entry(entry),
                ),
                _ => unimplemented!(),
            }
        }

        fn serialize_input_entry(InputEntry { headers, value }: InputEntry) -> Bytes {
            InputEntryMessage {
                headers: headers.into_iter().map(Into::into).collect(),
                value,
                ..Default::default()
            }
            .encode_to_vec()
            .into()
        }

        fn serialize_output_entry(OutputEntry { result }: OutputEntry) -> Bytes {
            OutputEntryMessage {
                result: Some(match result {
                    EntryResult::Success(success) => output_entry_message::Result::Value(success),
                    EntryResult::Failure(code, reason) => {
                        output_entry_message::Result::Failure(Failure {
                            code: code.into(),
                            message: reason.to_string(),
                        })
                    }
                }),
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
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
                ..Default::default()
            }
            .encode_to_vec()
            .into()
        }

        fn serialize_cancel_invocation_entry(
            CancelInvocationEntry { target }: CancelInvocationEntry,
        ) -> Bytes {
            CancelInvocationEntryMessage {
                target: Some(match target {
                    CancelInvocationTarget::InvocationId(id) => {
                        cancel_invocation_entry_message::Target::InvocationId(id.to_string())
                    }
                    CancelInvocationTarget::CallEntryIndex(idx) => {
                        cancel_invocation_entry_message::Target::CallEntryIndex(idx)
                    }
                }),
                ..Default::default()
            }
            .encode_to_vec()
            .into()
        }

        fn serialize_get_call_invocation_id_entry(
            GetCallInvocationIdEntry {
                call_entry_index,
                result,
            }: GetCallInvocationIdEntry,
        ) -> Bytes {
            GetCallInvocationIdEntryMessage {
                call_entry_index,
                result: result.map(|res| match res {
                    GetCallInvocationIdResult::InvocationId(success) => {
                        get_call_invocation_id_entry_message::Result::Value(success)
                    }
                    GetCallInvocationIdResult::Failure(code, reason) => {
                        get_call_invocation_id_entry_message::Result::Failure(Failure {
                            code: code.into(),
                            message: reason.to_string(),
                        })
                    }
                }),
                ..Default::default()
            }
            .encode_to_vec()
            .into()
        }

        fn serialize_attach_invocation_entry(
            AttachInvocationEntry { target, result }: AttachInvocationEntry,
        ) -> Bytes {
            AttachInvocationEntryMessage {
                target: Some(match target {
                    AttachInvocationTarget::InvocationId(id) => {
                        attach_invocation_entry_message::Target::InvocationId(id.to_string())
                    }
                    AttachInvocationTarget::CallEntryIndex(idx) => {
                        attach_invocation_entry_message::Target::CallEntryIndex(idx)
                    }
                    AttachInvocationTarget::IdempotentRequest(id) => {
                        attach_invocation_entry_message::Target::IdempotentRequestTarget(
                            IdempotentRequestTarget {
                                service_name: id.service_name.into(),
                                service_key: id.service_key.map(Into::into),
                                handler_name: id.service_handler.into(),
                                idempotency_key: id.idempotency_key.into(),
                            },
                        )
                    }
                    AttachInvocationTarget::Workflow(id) => {
                        attach_invocation_entry_message::Target::WorkflowTarget(WorkflowTarget {
                            workflow_name: id.service_name.into(),
                            workflow_key: id.key.into(),
                        })
                    }
                }),
                result: result.map(|r| match r {
                    EntryResult::Success(success) => {
                        attach_invocation_entry_message::Result::Value(success)
                    }
                    EntryResult::Failure(code, reason) => {
                        attach_invocation_entry_message::Result::Failure(Failure {
                            code: code.into(),
                            message: reason.to_string(),
                        })
                    }
                }),
                ..Default::default()
            }
            .encode_to_vec()
            .into()
        }

        fn serialize_get_invocation_output_entry(
            GetInvocationOutputEntry { target, result }: GetInvocationOutputEntry,
        ) -> Bytes {
            GetInvocationOutputEntryMessage {
                target: Some(match target {
                    AttachInvocationTarget::InvocationId(id) => {
                        get_invocation_output_entry_message::Target::InvocationId(id.to_string())
                    }
                    AttachInvocationTarget::CallEntryIndex(idx) => {
                        get_invocation_output_entry_message::Target::CallEntryIndex(idx)
                    }
                    AttachInvocationTarget::IdempotentRequest(id) => {
                        get_invocation_output_entry_message::Target::IdempotentRequestTarget(
                            IdempotentRequestTarget {
                                service_name: id.service_name.into(),
                                service_key: id.service_key.map(Into::into),
                                handler_name: id.service_handler.into(),
                                idempotency_key: id.idempotency_key.into(),
                            },
                        )
                    }
                    AttachInvocationTarget::Workflow(id) => {
                        get_invocation_output_entry_message::Target::WorkflowTarget(
                            WorkflowTarget {
                                workflow_name: id.service_name.into(),
                                workflow_key: id.key.into(),
                            },
                        )
                    }
                }),
                result: result.map(|value| match value {
                    CompletionResult::Empty => get_invocation_output_entry_message::Result::Empty(
                        service_protocol::Empty {},
                    ),
                    CompletionResult::Success(v) => {
                        get_invocation_output_entry_message::Result::Value(v)
                    }
                    CompletionResult::Failure(code, reason) => {
                        get_invocation_output_entry_message::Result::Failure(Failure {
                            code: code.into(),
                            message: reason.to_string(),
                        })
                    }
                }),
                ..Default::default()
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
            PlainEntryHeader::Call {
                is_completed: false,
                enrichment_result: None,
            },
            service_protocol::CallEntryMessage {
                service_name: "MySvc".to_string(),
                handler_name: "MyMethod".to_string(),

                parameter: Bytes::from_static(b"input"),
                ..service_protocol::CallEntryMessage::default()
            }
            .encode_to_vec()
            .into(),
        );

        // Complete the expected entry directly on the materialized model
        let mut expected_entry = raw_entry
            .deserialize_entry_ref::<ProtobufRawEntryCodec>()
            .unwrap();
        match &mut expected_entry {
            Entry::Call(invoke_entry_inner) => {
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
