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

use bytes::{Buf, BufMut, Bytes, BytesMut};
use prost::Message;
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
    fn serialize_as_unary_input_entry(value: Bytes) -> PlainRawEntry {
        RawEntry::new(
            RawEntryHeader::PollInputStream { is_completed: true },
            protocol::PollInputStreamEntryMessage { value }
                .encode_to_vec()
                .into(),
        )
    }

    fn deserialize<H: EntryHeader>(raw_entry: &RawEntry<H>) -> Result<Entry, RawEntryCodecError> {
        // We clone the entry Bytes here to ensure that the generated Message::decode
        // invocation reuses the same underlying byte array.
        match_decode!(raw_entry.header.to_entry_type(), raw_entry.entry.clone(), {
            PollInputStream,
            OutputStream,
            GetState,
            SetState,
            ClearState,
            Sleep,
            Invoke,
            BackgroundInvoke,
            Awakeable,
            CompleteAwakeable
        })
    }

    fn write_completion<H: EntryHeader + Debug>(
        entry: &mut RawEntry<H>,
        completion_result: CompletionResult,
    ) -> Result<(), RawEntryCodecError> {
        debug_assert_eq!(
            entry.header.is_completed(),
            Some(false),
            "Entry '{:?}' is already completed",
            entry
        );

        // Prepare the result to serialize in protobuf
        let completion_result_message = match completion_result {
            CompletionResult::Ack => {
                // For acks we simply flag the entry as completed and return
                entry.header.mark_completed();
                return Ok(());
            }
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
        let len = entry.entry.len() + completion_result_message.encoded_len();
        let mut result_buf = BytesMut::with_capacity(len);

        // Concatenate entry + result
        // The reason why encoding completion_message_result works is that by convention the tags
        // of completion message are the same used by completable entries.
        // See the service_protocol protobuf definition for more details.
        // https://protobuf.dev/programming-guides/encoding/#last-one-wins
        result_buf.put(mem::take(&mut entry.entry));
        completion_result_message.encode(&mut result_buf);

        // Write back to the entry the new buffer and the completed flag
        entry.entry = result_buf.freeze();
        entry.header.mark_completed();

        Ok(())
    }
}

#[cfg(feature = "mocks")]
mod mocks {
    use super::*;

    use crate::pb::protocol::{
        complete_awakeable_entry_message, CompleteAwakeableEntryMessage, Failure,
        PollInputStreamEntryMessage,
    };
    use restate_types::journal::enriched::{EnrichedEntryHeader, EnrichedRawEntry};
    use restate_types::journal::{CompleteAwakeableEntry, EntryResult, PollInputStreamEntry};

    impl ProtobufRawEntryCodec {
        pub fn serialize(entry: Entry) -> PlainRawEntry {
            match entry {
                Entry::PollInputStream(entry) => PlainRawEntry::new(
                    RawEntryHeader::PollInputStream { is_completed: true },
                    Self::serialize_poll_input_stream_entry(entry),
                ),
                Entry::CompleteAwakeable(entry) => PlainRawEntry::new(
                    RawEntryHeader::CompleteAwakeable,
                    Self::serialize_complete_awakeable_entry(entry),
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
                Entry::CompleteAwakeable(entry) => EnrichedRawEntry::new(
                    EnrichedEntryHeader::CompleteAwakeable,
                    Self::serialize_complete_awakeable_entry(entry),
                ),
                _ => unimplemented!(),
            }
        }

        fn serialize_poll_input_stream_entry(
            PollInputStreamEntry { result }: PollInputStreamEntry,
        ) -> Bytes {
            PollInputStreamEntryMessage { value: result }
                .encode_to_vec()
                .into()
        }

        fn serialize_complete_awakeable_entry(
            CompleteAwakeableEntry {
                service_name,
                instance_key,
                invocation_id,
                entry_index,
                result,
            }: CompleteAwakeableEntry,
        ) -> Bytes {
            CompleteAwakeableEntryMessage {
                service_name: service_name.to_string(),
                instance_key,
                invocation_id,
                entry_index,
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
            RawEntryHeader::Invoke {
                is_completed: false,
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
        let mut expected_entry = ProtobufRawEntryCodec::deserialize(&raw_entry).unwrap();
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
        let actual_entry = ProtobufRawEntryCodec::deserialize(&actual_raw_entry).unwrap();

        assert_eq!(actual_raw_entry.header.is_completed(), Some(true));
        assert_eq!(actual_entry, expected_entry);
    }
}
