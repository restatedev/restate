// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::mem;
use std::num::NonZeroUsize;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytes_utils::SegmentedBuf;
use tracing::warn;

use restate_serde_util::ByteCount;
use restate_types::journal::raw::{PlainEntryHeader, RawEntry};
use restate_types::service_protocol::ServiceProtocolVersion;

use super::header::UnknownMessageType;
use super::*;

#[derive(Debug, codederror::CodedError, thiserror::Error)]
#[code(restate_errors::RT0012)]
pub enum EncodingError {
    #[error("cannot decode message type {0:?}. This looks like a bug of the SDK. Reason: {1:?}")]
    DecodeMessage(MessageType, #[source] prost::DecodeError),
    #[error(transparent)]
    UnknownMessageType(#[from] UnknownMessageType),
    #[error("hit message size limit: {0} >= {1}")]
    #[code(restate_errors::RT0003)]
    MessageSizeLimit(usize, NonZeroUsize),
}

// --- Input message encoder

// TODO: To reduce allocation overhead for small messages (completions, acks), we could
//  re-introduce a small bounded arena (e.g. 4-8 KiB) that is reused across encode calls.
//  The key constraint is that it must not grow unbounded — the previous arena retained the
//  high-water-mark capacity (up to 32 MiB) for the entire invocation lifetime, wasting
//  memory across thousands of concurrent long-lived invocations. See #4364.
pub struct Encoder;

impl Encoder {
    pub fn new(service_protocol_version: ServiceProtocolVersion) -> Self {
        assert_ne!(
            service_protocol_version,
            ServiceProtocolVersion::Unspecified,
            "A protocol version should be specified"
        );
        Self
    }

    /// Encodes a message to bytes.
    ///
    /// Each call allocates a right-sized buffer for the message. This avoids retaining a
    /// high-water-mark arena that would hold memory for the lifetime of the encoder — which
    /// matters when thousands of long-lived invocations each encoded one large message during
    /// replay but only send small completions/acks afterwards.
    // Todo: Once we merge thread-local buffer pools (https://github.com/restatedev/restate/pull/4366),
    //  we can consider passing in a reusable buffer.
    pub fn encode(&mut self, msg: ProtocolMessage) -> Bytes {
        let len = 8 + msg.encoded_len();
        let mut buf = BytesMut::with_capacity(len);
        let header = generate_header(&msg);
        buf.put_u64(header.into());
        encode_msg(&msg, &mut buf).expect(
            "Encoding messages should be infallible, \
            this error indicates a bug in the invoker code. \
            Please contact the Restate developers.",
        );
        buf.freeze()
    }
}

#[inline(always)]
fn generate_header(msg: &ProtocolMessage) -> MessageHeader {
    let len: u32 = msg
        .encoded_len()
        .try_into()
        .expect("Protocol messages can't be larger than u32");
    match msg {
        ProtocolMessage::Start(_) => MessageHeader::new_start(len),
        ProtocolMessage::Completion(_) => MessageHeader::new(MessageType::Completion, len),
        ProtocolMessage::Suspension(_) => MessageHeader::new(MessageType::Suspension, len),
        ProtocolMessage::Error(_) => MessageHeader::new(MessageType::Error, len),
        ProtocolMessage::End(_) => MessageHeader::new(MessageType::End, len),
        ProtocolMessage::EntryAck(_) => MessageHeader::new(MessageType::EntryAck, len),
        ProtocolMessage::UnparsedEntry(entry) => {
            let completed_flag = entry.header().is_completed();
            MessageHeader::new_entry_header(
                raw_header_to_message_type(entry.header()),
                completed_flag,
                len,
            )
        }
    }
}

#[inline(always)]
fn encode_msg(msg: &ProtocolMessage, buf: &mut impl BufMut) -> Result<(), prost::EncodeError> {
    match msg {
        ProtocolMessage::Start(m) => m.encode(buf),
        ProtocolMessage::Completion(m) => m.encode(buf),
        ProtocolMessage::Suspension(m) => m.encode(buf),
        ProtocolMessage::Error(m) => m.encode(buf),
        ProtocolMessage::End(m) => m.encode(buf),
        ProtocolMessage::EntryAck(m) => m.encode(buf),
        ProtocolMessage::UnparsedEntry(entry) => {
            buf.put(entry.serialized_entry().clone());
            Ok(())
        }
    }
}

// --- Input message decoder

/// Stateful decoder to decode [`ProtocolMessage`]
pub struct Decoder {
    buf: SegmentedBuf<Bytes>,
    state: DecoderState,
    message_size_warning: NonZeroUsize,
    message_size_limit: NonZeroUsize,
}

impl Decoder {
    pub fn new(
        service_protocol_version: ServiceProtocolVersion,
        message_size_warning: NonZeroUsize,
        message_size_limit: NonZeroUsize,
    ) -> Self {
        assert_ne!(
            service_protocol_version,
            ServiceProtocolVersion::Unspecified,
            "A protocol version should be specified"
        );
        Self {
            buf: SegmentedBuf::new(),
            state: DecoderState::WaitingHeader,
            message_size_warning,
            message_size_limit,
        }
    }

    pub fn has_remaining(&self) -> bool {
        self.buf.has_remaining()
    }

    /// Concatenate a new chunk in the internal buffer.
    pub fn push(&mut self, buf: Bytes) {
        self.buf.push(buf)
    }

    /// Try to consume the next message in the internal buffer.
    ///
    /// Returns `(header, message, payload_size)` where `payload_size` is not counting
    /// the header length.
    pub fn consume_next(
        &mut self,
    ) -> Result<Option<(MessageHeader, ProtocolMessage, usize)>, EncodingError> {
        loop {
            let remaining = self.buf.remaining();

            if remaining < self.state.needs_bytes() {
                return Ok(None);
            }

            if let Some(res) = self.state.decode(
                &mut self.buf,
                self.message_size_warning,
                self.message_size_limit,
            )? {
                return Ok(Some(res));
            }
        }
    }
}

#[derive(Default)]
enum DecoderState {
    #[default]
    WaitingHeader,
    WaitingPayload(MessageHeader),
}

impl DecoderState {
    fn needs_bytes(&self) -> usize {
        match self {
            DecoderState::WaitingHeader => 8,
            DecoderState::WaitingPayload(h) => h.frame_length() as usize,
        }
    }

    fn decode(
        &mut self,
        mut buf: impl Buf,
        message_size_warning: NonZeroUsize,
        message_size_limit: NonZeroUsize,
    ) -> Result<Option<(MessageHeader, ProtocolMessage, usize)>, EncodingError> {
        let mut res = None;

        *self = match mem::take(self) {
            DecoderState::WaitingHeader => {
                let header: MessageHeader = buf.get_u64().try_into()?;
                let message_length =
                    usize::try_from(header.frame_length()).expect("u32 must convert into usize");

                if message_length >= message_size_warning.get() {
                    warn!(
                        "Message size warning for '{:?}': {} >= {}. \
                    Generating very large messages can make the system unstable if configured with too little memory. \
                    You can increase the threshold to avoid this warning by changing the worker.invoker.message_size_warning config option",
                        header.message_type(),
                        ByteCount::from(message_length),
                        ByteCount::from(message_size_warning),
                    );
                }
                if message_length >= message_size_limit.get() {
                    return Err(EncodingError::MessageSizeLimit(
                        message_length,
                        message_size_limit,
                    ));
                }

                DecoderState::WaitingPayload(header)
            }
            DecoderState::WaitingPayload(h) => {
                let payload_size = h.frame_length() as usize;
                let msg = decode_protocol_message(&h, buf.take(payload_size))
                    .map_err(|e| EncodingError::DecodeMessage(h.message_type(), e))?;
                res = Some((h, msg, payload_size));
                DecoderState::WaitingHeader
            }
        };

        Ok(res)
    }
}

fn decode_protocol_message(
    header: &MessageHeader,
    mut buf: impl Buf,
) -> Result<ProtocolMessage, prost::DecodeError> {
    Ok(match header.message_type() {
        MessageType::Start => ProtocolMessage::Start(service_protocol::StartMessage::decode(buf)?),
        MessageType::Completion => {
            ProtocolMessage::Completion(service_protocol::CompletionMessage::decode(buf)?)
        }
        MessageType::Suspension => {
            ProtocolMessage::Suspension(service_protocol::SuspensionMessage::decode(buf)?)
        }
        MessageType::Error => ProtocolMessage::Error(service_protocol::ErrorMessage::decode(buf)?),
        MessageType::End => ProtocolMessage::End(service_protocol::EndMessage::decode(buf)?),
        MessageType::EntryAck => {
            ProtocolMessage::EntryAck(service_protocol::EntryAckMessage::decode(buf)?)
        }
        _ => ProtocolMessage::UnparsedEntry(RawEntry::new(
            message_header_to_raw_header(header),
            // NOTE: This is a no-op copy if the Buf is instance of Bytes.
            // In case of SegmentedBuf, this doesn't copy if the whole message is contained
            // in a single Bytes instance.
            buf.copy_to_bytes(buf.remaining()),
        )),
    })
}

macro_rules! expect_flag {
    ($message_header:expr, $name:ident) => {
        MessageHeader::$name($message_header)
            .expect(concat!(stringify!($name), " flag being present"))
    };
}

fn message_header_to_raw_header(message_header: &MessageHeader) -> PlainEntryHeader {
    debug_assert!(
        !matches!(
            message_header.message_type(),
            MessageType::Start
                | MessageType::Completion
                | MessageType::Suspension
                | MessageType::EntryAck
                | MessageType::Error
                | MessageType::End
        ),
        "Message is not an entry type. This is a Restate bug. Please contact the developers."
    );
    match message_header.message_type() {
        MessageType::Start => unreachable!(),
        MessageType::Completion => unreachable!(),
        MessageType::Suspension => unreachable!(),
        MessageType::Error => unreachable!(),
        MessageType::End => unreachable!(),
        MessageType::EntryAck => unreachable!(),

        MessageType::InputEntry => PlainEntryHeader::Input {},
        MessageType::OutputEntry => PlainEntryHeader::Output {},
        MessageType::GetStateEntry => PlainEntryHeader::GetState {
            is_completed: expect_flag!(message_header, completed),
        },
        MessageType::SetStateEntry => PlainEntryHeader::SetState {},
        MessageType::ClearStateEntry => PlainEntryHeader::ClearState {},
        MessageType::GetStateKeysEntry => PlainEntryHeader::GetStateKeys {
            is_completed: expect_flag!(message_header, completed),
        },
        MessageType::ClearAllStateEntry => PlainEntryHeader::ClearAllState {},
        MessageType::GetPromiseEntry => PlainEntryHeader::GetPromise {
            is_completed: expect_flag!(message_header, completed),
        },
        MessageType::PeekPromiseEntry => PlainEntryHeader::PeekPromise {
            is_completed: expect_flag!(message_header, completed),
        },
        MessageType::CompletePromiseEntry => PlainEntryHeader::CompletePromise {
            is_completed: expect_flag!(message_header, completed),
        },
        MessageType::SleepEntry => PlainEntryHeader::Sleep {
            is_completed: expect_flag!(message_header, completed),
        },
        MessageType::InvokeEntry => PlainEntryHeader::Call {
            is_completed: expect_flag!(message_header, completed),
            enrichment_result: None,
        },
        MessageType::BackgroundInvokeEntry => PlainEntryHeader::OneWayCall {
            enrichment_result: (),
        },
        MessageType::AwakeableEntry => PlainEntryHeader::Awakeable {
            is_completed: expect_flag!(message_header, completed),
        },
        MessageType::CompleteAwakeableEntry => PlainEntryHeader::CompleteAwakeable {
            enrichment_result: (),
        },
        MessageType::SideEffectEntry => PlainEntryHeader::Run {},
        MessageType::CancelInvocationEntry => PlainEntryHeader::CancelInvocation {},
        MessageType::GetCallInvocationIdEntry => PlainEntryHeader::GetCallInvocationId {
            is_completed: expect_flag!(message_header, completed),
        },
        MessageType::AttachInvocationEntry => PlainEntryHeader::AttachInvocation {
            is_completed: expect_flag!(message_header, completed),
        },
        MessageType::GetInvocationOutputEntry => PlainEntryHeader::GetInvocationOutput {
            is_completed: expect_flag!(message_header, completed),
        },
        MessageType::CustomEntry(code) => PlainEntryHeader::Custom { code },
    }
}

fn raw_header_to_message_type(entry_header: &PlainEntryHeader) -> MessageType {
    match entry_header {
        PlainEntryHeader::Input { .. } => MessageType::InputEntry,
        PlainEntryHeader::Output { .. } => MessageType::OutputEntry,
        PlainEntryHeader::GetState { .. } => MessageType::GetStateEntry,
        PlainEntryHeader::SetState { .. } => MessageType::SetStateEntry,
        PlainEntryHeader::ClearState { .. } => MessageType::ClearStateEntry,
        PlainEntryHeader::GetStateKeys { .. } => MessageType::GetStateKeysEntry,
        PlainEntryHeader::ClearAllState { .. } => MessageType::ClearAllStateEntry,
        PlainEntryHeader::GetPromise { .. } => MessageType::GetPromiseEntry,
        PlainEntryHeader::PeekPromise { .. } => MessageType::PeekPromiseEntry,
        PlainEntryHeader::CompletePromise { .. } => MessageType::CompletePromiseEntry,
        PlainEntryHeader::Sleep { .. } => MessageType::SleepEntry,
        PlainEntryHeader::Call { .. } => MessageType::InvokeEntry,
        PlainEntryHeader::OneWayCall { .. } => MessageType::BackgroundInvokeEntry,
        PlainEntryHeader::Awakeable { .. } => MessageType::AwakeableEntry,
        PlainEntryHeader::CompleteAwakeable { .. } => MessageType::CompleteAwakeableEntry,
        PlainEntryHeader::Run { .. } => MessageType::SideEffectEntry,
        PlainEntryHeader::CancelInvocation => MessageType::CancelInvocationEntry,
        PlainEntryHeader::GetCallInvocationId { .. } => MessageType::GetCallInvocationIdEntry,
        PlainEntryHeader::AttachInvocation { .. } => MessageType::AttachInvocationEntry,
        PlainEntryHeader::GetInvocationOutput { .. } => MessageType::GetInvocationOutputEntry,
        PlainEntryHeader::Custom { code, .. } => MessageType::CustomEntry(*code),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::codec::ProtobufRawEntryCodec;
    use restate_test_util::{assert, assert_eq, let_assert};
    use restate_types::journal::raw::RawEntryCodec;

    #[test]
    fn fill_decoder_with_several_messages() {
        let mut encoder = Encoder::new(ServiceProtocolVersion::V1);
        let mut decoder = Decoder::new(
            ServiceProtocolVersion::V1,
            NonZeroUsize::MAX,
            NonZeroUsize::MAX,
        );

        let expected_msg_0 = ProtocolMessage::new_start_message(
            "key".into(),
            "key".into(),
            Some("key".into()),
            1,
            true,
            vec![],
            10,
            Duration::ZERO,
        );

        let expected_msg_1: ProtocolMessage = ProtobufRawEntryCodec::serialize_as_input_entry(
            vec![],
            Bytes::from_static("input".as_bytes()),
        )
        .erase_enrichment()
        .into();
        let expected_msg_2: ProtocolMessage = Completion {
            entry_index: 1,
            result: CompletionResult::Empty,
        }
        .into();

        decoder.push(encoder.encode(expected_msg_0.clone()));
        decoder.push(encoder.encode(expected_msg_1.clone()));
        decoder.push(encoder.encode(expected_msg_2.clone()));

        let (actual_msg_header_0, actual_msg_0, _) = decoder.consume_next().unwrap().unwrap();
        assert_eq!(actual_msg_header_0.message_type(), MessageType::Start);
        assert_eq!(actual_msg_0, expected_msg_0);

        let (actual_msg_header_1, actual_msg_1, _) = decoder.consume_next().unwrap().unwrap();
        assert_eq!(actual_msg_header_1.message_type(), MessageType::InputEntry);
        assert_eq!(actual_msg_header_1.completed(), None);
        assert_eq!(actual_msg_1, expected_msg_1);

        let (actual_msg_header_2, actual_msg_2, _) = decoder.consume_next().unwrap().unwrap();
        assert_eq!(actual_msg_header_2.message_type(), MessageType::Completion);
        assert_eq!(actual_msg_2, expected_msg_2);

        assert!(decoder.consume_next().unwrap().is_none());
    }

    #[test]
    fn fill_decoder_with_partial_header() {
        partial_decoding_test(4)
    }

    #[test]
    fn fill_decoder_with_partial_body() {
        partial_decoding_test(10)
    }

    fn partial_decoding_test(split_index: usize) {
        let mut encoder = Encoder::new(ServiceProtocolVersion::V1);
        let mut decoder = Decoder::new(
            ServiceProtocolVersion::V1,
            NonZeroUsize::MAX,
            NonZeroUsize::MAX,
        );

        let expected_msg: ProtocolMessage = ProtobufRawEntryCodec::serialize_as_input_entry(
            vec![],
            Bytes::from_static("input".as_bytes()),
        )
        .erase_enrichment()
        .into();
        let expected_msg_encoded = encoder.encode(expected_msg.clone());

        decoder.push(expected_msg_encoded.slice(0..split_index));
        assert!(decoder.consume_next().unwrap().is_none());

        decoder.push(expected_msg_encoded.slice(split_index..));

        let (actual_msg_header, actual_msg, _) = decoder.consume_next().unwrap().unwrap();
        assert_eq!(actual_msg_header.message_type(), MessageType::InputEntry);
        assert_eq!(actual_msg_header.completed(), None);
        assert_eq!(actual_msg, expected_msg);

        assert!(decoder.consume_next().unwrap().is_none());
    }

    #[test]
    fn hit_message_size_limit() {
        let mut decoder = Decoder::new(
            ServiceProtocolVersion::V1,
            NonZeroUsize::new((u8::MAX / 2) as usize).unwrap(),
            NonZeroUsize::new(u8::MAX as usize).unwrap(),
        );

        let mut encoder = Encoder::new(ServiceProtocolVersion::V1);
        let message = ProtocolMessage::from(
            ProtobufRawEntryCodec::serialize_as_input_entry(
                vec![],
                (0..=u8::MAX).collect::<Vec<_>>().into(),
            )
            .erase_enrichment(),
        );
        let expected_msg_size = message.encoded_len();
        let msg = encoder.encode(message);

        decoder.push(msg.clone());
        let_assert!(
            EncodingError::MessageSizeLimit(msg_size, limit) = decoder.consume_next().unwrap_err()
        );
        assert_eq!(msg_size, expected_msg_size);
        assert_eq!(limit, NonZeroUsize::new(u8::MAX as usize).unwrap())
    }
}
