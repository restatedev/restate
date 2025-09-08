// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::UnknownMessageType;
use super::*;

use std::mem;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytes_utils::SegmentedBuf;
use restate_types::service_protocol::ServiceProtocolVersion;
use size::Size;
use tracing::warn;

#[derive(Debug, codederror::CodedError, thiserror::Error)]
#[code(restate_errors::RT0012)]
pub enum EncodingError {
    #[error("cannot decode message type {0:?}. This looks like a bug of the SDK. Reason: {1:?}")]
    DecodeMessage(MessageType, #[source] prost::DecodeError),
    #[error(transparent)]
    UnknownMessageType(#[from] UnknownMessageType),
    #[error("hit message size limit: {0} >= {1}")]
    #[code(restate_errors::RT0003)]
    MessageSizeLimit(usize, usize),
}

// --- Input message encoder

pub struct Encoder {}

impl Encoder {
    pub fn new(service_protocol_version: ServiceProtocolVersion) -> Self {
        assert_ne!(
            service_protocol_version,
            ServiceProtocolVersion::Unspecified,
            "A protocol version should be specified"
        );
        Self {}
    }

    /// Encodes a message to bytes
    pub fn encode(&self, msg: Message) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.encoded_len(&msg));
        self.encode_to_buf_mut(&mut buf, msg).expect(
            "Encoding messages should be infallible, \
            this error indicates a bug in the invoker code. \
            Please contact the Restate developers.",
        );
        buf.freeze()
    }

    /// Encodes a raw message to bytes
    pub fn encode_raw(&self, msg_ty: MessageType, content: Bytes) -> Bytes {
        let mut buf = BytesMut::with_capacity(8 + content.len());
        let len: u32 = content
            .len()
            .try_into()
            .expect("Protocol messages can't be larger than u32");
        buf.put_u64(MessageHeader::new(msg_ty, len).into());
        buf.put(content);
        buf.freeze()
    }

    /// Includes header len
    fn encoded_len(&self, msg: &Message) -> usize {
        8 + msg.encoded_len()
    }

    fn encode_to_buf_mut(
        &self,
        mut buf: impl BufMut,
        msg: Message,
    ) -> Result<(), prost::EncodeError> {
        let header = generate_header(&msg);
        buf.put_u64(header.into());

        // Note:
        // prost::EncodeError can be triggered only by a buffer smaller than required,
        // but because we create the buffer a couple of lines above using the size computed by prost,
        // this can happen only if there is a very bad bug in prost.
        msg.encode(&mut buf)
    }
}

fn generate_header(msg: &Message) -> MessageHeader {
    let len: u32 = msg
        .encoded_len()
        .try_into()
        .expect("Protocol messages can't be larger than u32");
    let ty = msg.ty();
    MessageHeader::new(ty, len)
}

// --- Input message decoder

/// Stateful decoder to decode [`ProtocolMessage`]
pub struct Decoder {
    buf: SegmentedBuf<Bytes>,
    state: DecoderState,
    message_size_warning: usize,
    message_size_limit: usize,
}

impl Decoder {
    pub fn new(
        service_protocol_version: ServiceProtocolVersion,
        message_size_warning: usize,
        message_size_limit: Option<usize>,
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
            message_size_limit: message_size_limit.unwrap_or(usize::MAX),
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
    pub fn consume_next(&mut self) -> Result<Option<(MessageHeader, Message)>, EncodingError> {
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
        message_size_warning: usize,
        message_size_limit: usize,
    ) -> Result<Option<(MessageHeader, Message)>, EncodingError> {
        let mut res = None;

        *self = match mem::take(self) {
            DecoderState::WaitingHeader => {
                let header: MessageHeader = buf.get_u64().try_into()?;
                let message_length =
                    usize::try_from(header.frame_length()).expect("u32 must convert into usize");

                if message_length >= message_size_warning {
                    warn!(
                        "Message size warning for '{:?}': {} >= {}. \
                    Generating very large messages can make the system unstable if configured with too little memory. \
                    You can increase the threshold to avoid this warning by changing the worker.invoker.message_size_warning config option",
                        header.message_type(),
                        Size::from_bytes(message_length),
                        Size::from_bytes(message_size_warning),
                    );
                }
                if message_length >= message_size_limit {
                    return Err(EncodingError::MessageSizeLimit(
                        message_length,
                        message_size_limit,
                    ));
                }

                DecoderState::WaitingPayload(header)
            }
            DecoderState::WaitingPayload(h) => {
                let msg = h
                    .message_type()
                    .decode(buf.take(h.frame_length() as usize))
                    .map_err(|e| EncodingError::DecodeMessage(h.message_type(), e))?;
                res = Some((h, msg));
                DecoderState::WaitingHeader
            }
        };

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use restate_test_util::{assert, assert_eq, let_assert};

    #[test]
    fn fill_decoder_with_several_messages() {
        let encoder = Encoder::new(ServiceProtocolVersion::V1);
        let mut decoder = Decoder::new(ServiceProtocolVersion::V1, usize::MAX, None);

        let expected_msg_0 = Message::new_start_message(
            "key".into(),
            "key".into(),
            Some("key".into()),
            1,
            true,
            vec![],
            10,
            Duration::ZERO,
            10,
        );

        let expected_msg_1 = Message::InputCommand(Bytes::from_static(b"123"));
        let expected_msg_2 = Message::CallCompletionNotification(Bytes::from_static(b"456"));

        decoder.push(encoder.encode(expected_msg_0.clone()));
        decoder.push(encoder.encode(expected_msg_1.clone()));
        decoder.push(encoder.encode(expected_msg_2.clone()));

        let (actual_msg_header_0, actual_msg_0) = decoder.consume_next().unwrap().unwrap();
        assert_eq!(actual_msg_header_0.message_type(), MessageType::Start);
        assert_eq!(actual_msg_0, expected_msg_0);

        let (actual_msg_header_1, actual_msg_1) = decoder.consume_next().unwrap().unwrap();
        assert_eq!(
            actual_msg_header_1.message_type(),
            MessageType::InputCommand
        );
        assert_eq!(actual_msg_1, expected_msg_1);

        let (actual_msg_header_2, actual_msg_2) = decoder.consume_next().unwrap().unwrap();
        assert_eq!(
            actual_msg_header_2.message_type(),
            MessageType::CallCompletionNotification
        );
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
        let encoder = Encoder::new(ServiceProtocolVersion::V1);
        let mut decoder = Decoder::new(ServiceProtocolVersion::V1, usize::MAX, None);

        let expected_msg = Message::InputCommand(Bytes::from_static(b"123"));
        let expected_msg_encoded = encoder.encode(expected_msg.clone());

        decoder.push(expected_msg_encoded.slice(0..split_index));
        assert!(decoder.consume_next().unwrap().is_none());

        decoder.push(expected_msg_encoded.slice(split_index..));

        let (actual_msg_header, actual_msg) = decoder.consume_next().unwrap().unwrap();
        assert_eq!(actual_msg_header.message_type(), MessageType::InputCommand);
        assert_eq!(actual_msg, expected_msg);

        assert!(decoder.consume_next().unwrap().is_none());
    }

    #[test]
    fn hit_message_size_limit() {
        let mut decoder = Decoder::new(
            ServiceProtocolVersion::V1,
            (u8::MAX / 2) as usize,
            Some(u8::MAX as usize),
        );

        let encoder = Encoder::new(ServiceProtocolVersion::V1);
        let message = Message::InputCommand((0..=u8::MAX).collect::<Vec<_>>().into());
        let expected_msg_size = message.encoded_len();
        let msg = encoder.encode(message);

        decoder.push(msg.clone());
        let_assert!(
            EncodingError::MessageSizeLimit(msg_size, limit) = decoder.consume_next().unwrap_err()
        );
        assert_eq!(msg_size, expected_msg_size);
        assert_eq!(limit, u8::MAX as usize)
    }
}
