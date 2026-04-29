// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
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
use std::num::NonZeroUsize;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytes_utils::SegmentedBuf;
use tracing::warn;

use restate_serde_util::ByteCount;
use restate_types::{errors::GenericError, service_protocol::ServiceProtocolVersion};

#[derive(Debug, codederror::CodedError, thiserror::Error)]
#[code(restate_errors::RT0012)]
pub enum EncodingError {
    #[error("cannot decode message type {0:?}. Reason: {1:?}")]
    MessageEncoding(MessageType, #[source] MessageEncodingError),
    #[error(transparent)]
    UnknownMessageType(#[from] UnknownMessageType),
    #[error("hit message size limit: {0} >= {1}")]
    #[code(restate_errors::RT0003)]
    MessageSizeLimit(usize, NonZeroUsize),
}

#[derive(Debug, thiserror::Error)]
pub enum MessageEncodingError {
    #[error("unknown service protocol version {0:?}")]
    UnknownServiceProtocolVersion(ServiceProtocolVersion),
    #[error("cannot encode message {0}")]
    Encoding(
        #[source]
        #[from]
        prost::EncodeError,
    ),
    #[error("cannot decode message {0}. This looks like a bug of the SDK.")]
    Decoding(
        #[source]
        #[from]
        prost::DecodeError,
    ),
    #[error(transparent)]
    Generic(GenericError),
}

pub trait ServiceWireEncoder {
    fn encode(
        &self,
        buf: &mut impl bytes::BufMut,
        service_protocol_version: ServiceProtocolVersion,
    ) -> Result<(), MessageEncodingError>;

    fn encoded_len(&self, service_protocol_version: ServiceProtocolVersion) -> usize;
}

pub trait ServiceWireDecoder: Sized {
    fn decode(
        buf: impl bytes::Buf,
        service_protocol_version: ServiceProtocolVersion,
    ) -> Result<Self, MessageEncodingError>;
}

/// Implements [`ServiceWireEncoder`] and [`ServiceWireDecoder`] for the given protobuf message
/// types by delegating directly to [`prost::Message`], ignoring the `service_protocol_version`.
///
/// Types that need version-specific encoding/decoding should implement the traits manually instead.
macro_rules! default_encode_decode {
    ($ty:ident) => {
        impl crate::message_codec::ServiceWireEncoder for $ty {
            fn encode(
                &self,
                buf: &mut impl bytes::BufMut,
                _service_protocol_version: restate_types::service_protocol::ServiceProtocolVersion,
            ) -> Result<(), crate::message_codec::MessageEncodingError> {
                prost::Message::encode(self, buf).map_err(Into::into)
            }

            fn encoded_len(&self, _service_protocol_version: restate_types::service_protocol::ServiceProtocolVersion) -> usize {
                prost::Message::encoded_len(self)
            }
        }

        impl crate::message_codec::ServiceWireDecoder for $ty {
            fn decode(
                buf: impl bytes::Buf,
                _service_protocol_version: restate_types::service_protocol::ServiceProtocolVersion,
            ) -> Result<Self, crate::message_codec::MessageEncodingError> {
                prost::Message::decode(buf).map_err(Into::into)
            }
        }
    };
    ($($ty:ident),*) => {
        $($crate::message_codec::default_encode_decode!($ty);)*
    }
}

pub(crate) use default_encode_decode;
// --- Input message encoder

// TODO: To reduce allocation overhead for small messages (completions, acks), we could
//  re-introduce a small bounded arena (e.g. 4-8 KiB) that is reused across encode calls.
//  The key constraint is that it must not grow unbounded — the previous arena retained the
//  high-water-mark capacity (up to 32 MiB) for the entire invocation lifetime, wasting
//  memory across thousands of concurrent long-lived invocations. See #4364.
pub struct Encoder {
    service_protocol_version: ServiceProtocolVersion,
}

impl Encoder {
    pub fn new(service_protocol_version: ServiceProtocolVersion) -> Self {
        assert_ne!(
            service_protocol_version,
            ServiceProtocolVersion::Unspecified,
            "A protocol version should be specified"
        );
        Self {
            service_protocol_version,
        }
    }

    /// Encodes a message to bytes.
    ///
    /// Each call allocates a right-sized buffer for the message. This avoids retaining a
    /// high-water-mark arena that would hold memory for the lifetime of the encoder — which
    /// matters when thousands of long-lived invocations each encoded one large message during
    /// replay but only send small completions/acks afterwards.
    // Todo: Once we merge thread-local buffer pools (https://github.com/restatedev/restate/pull/4366),
    //  we can consider passing in a reusable buffer.
    pub fn encode(&mut self, msg: Message) -> Bytes {
        let len = 8 + msg.encoded_len(self.service_protocol_version);
        let mut buf = BytesMut::with_capacity(len);
        let header = generate_header(&msg, self.service_protocol_version);
        buf.put_u64(header.into());
        msg.encode(&mut buf, self.service_protocol_version).expect(
            "Encoding messages should be infallible, \
            this error indicates a bug in the invoker code. \
            Please contact the Restate developers.",
        );
        buf.freeze()
    }

    /// Encodes a raw message to bytes.
    ///
    /// See [`Self::encode`] for why we allocate per call.
    pub fn encode_raw(&mut self, msg_ty: MessageType, content: Bytes) -> Bytes {
        let len: u32 = content
            .len()
            .try_into()
            .expect("Protocol messages can't be larger than u32");
        let mut buf = BytesMut::with_capacity(8 + content.len());
        buf.put_u64(MessageHeader::new(msg_ty, len).into());
        buf.put(content);
        buf.freeze()
    }
}

#[inline(always)]
fn generate_header(
    msg: &Message,
    service_protocol_version: ServiceProtocolVersion,
) -> MessageHeader {
    let len: u32 = msg
        .encoded_len(service_protocol_version)
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
    service_protocol_version: ServiceProtocolVersion,
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
            service_protocol_version,
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
    pub fn consume_next(&mut self) -> Result<Option<(MessageHeader, Message)>, EncodingError> {
        loop {
            let remaining = self.buf.remaining();

            if remaining < self.state.needs_bytes() {
                return Ok(None);
            }

            if let Some(res) = self.state.decode(
                &mut self.buf,
                self.service_protocol_version,
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
        service_protocol_version: ServiceProtocolVersion,
        message_size_warning: NonZeroUsize,
        message_size_limit: NonZeroUsize,
    ) -> Result<Option<(MessageHeader, Message)>, EncodingError> {
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
                let msg = h
                    .message_type()
                    .decode(
                        buf.take(h.frame_length() as usize),
                        service_protocol_version,
                    )
                    .map_err(|e| EncodingError::MessageEncoding(h.message_type(), e))?;
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
        let mut encoder = Encoder::new(ServiceProtocolVersion::V1);
        let mut decoder = Decoder::new(
            ServiceProtocolVersion::V1,
            NonZeroUsize::MAX,
            NonZeroUsize::MAX,
        );

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
            None,
            &LimitKey::None,
            None,
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
        let mut encoder = Encoder::new(ServiceProtocolVersion::V1);
        let mut decoder = Decoder::new(
            ServiceProtocolVersion::V1,
            NonZeroUsize::MAX,
            NonZeroUsize::MAX,
        );

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
            NonZeroUsize::new((u8::MAX / 2) as usize).unwrap(),
            NonZeroUsize::new(u8::MAX as usize).unwrap(),
        );

        let mut encoder = Encoder::new(ServiceProtocolVersion::V1);
        let message = Message::InputCommand((0..=u8::MAX).collect::<Vec<_>>().into());
        let expected_msg_size = message.encoded_len(ServiceProtocolVersion::V1);
        let msg = encoder.encode(message);

        decoder.push(msg.clone());
        let_assert!(
            EncodingError::MessageSizeLimit(msg_size, limit) = decoder.consume_next().unwrap_err()
        );
        assert_eq!(msg_size, expected_msg_size);
        assert_eq!(limit, NonZeroUsize::new(u8::MAX as usize).unwrap())
    }
}
