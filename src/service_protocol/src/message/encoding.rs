use super::header::UnknownMessageType;
use super::*;

use std::mem;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use bytes_utils::SegmentedBuf;
use common::types::RawEntry;
use journal::raw::RawEntryHeader;

#[derive(Debug, thiserror::Error)]
pub enum EncodingError {
    #[error("cannot decode message type {0:?}. This looks like a bug of the SDK. Reason: {1:?}")]
    DecodeMessage(MessageType, #[source] prost::DecodeError),
    #[error(transparent)]
    UnknownFrameType(#[from] UnknownMessageType),
}

// --- Input message encoder

pub struct Encoder {
    protocol_version: u16,
}

impl Encoder {
    pub fn new(protocol_version: u16) -> Self {
        Self { protocol_version }
    }

    /// Encodes a message to bytes
    pub fn encode(&self, msg: ProtocolMessage) -> Bytes {
        let header = generate_header(&msg, self.protocol_version);

        let mut buf = BytesMut::with_capacity((header.frame_length() + 8) as usize);
        buf.put_u64(header.into());

        // Note:
        // prost::EncodeError can be triggered only by a buffer smaller than required,
        // but because we create the buffer a couple of lines above using the size computed by prost,
        // this can happen only if there is a very bad bug in prost.
        encode_msg(&msg, &mut buf).expect(
            "Encoding messages should be infallible, \
            this error indicates a bug in the invoker code. \
            Please contact the Restate developers.",
        );

        buf.freeze()
    }
}

fn generate_header(msg: &ProtocolMessage, protocol_version: u16) -> MessageHeader {
    match msg {
        ProtocolMessage::Start(m) => MessageHeader::new_start(
            protocol_version,
            m.encoded_len()
                .try_into()
                .expect("Protocol messages can't be larger than u32"),
        ),
        ProtocolMessage::Completion(m) => MessageHeader::new(
            MessageType::Completion,
            m.encoded_len()
                .try_into()
                .expect("Protocol messages can't be larger than u32"),
        ),
        ProtocolMessage::Suspension(m) => MessageHeader::new(
            MessageType::Suspension,
            m.encoded_len()
                .try_into()
                .expect("Protocol messages can't be larger than u32"),
        ),
        ProtocolMessage::UnparsedEntry(entry) => match entry.header.is_completed() {
            Some(completed_flag) => MessageHeader::new_completable_entry(
                raw_header_to_message_type(&entry.header),
                completed_flag,
                entry.entry.len() as u32,
            ),
            None => MessageHeader::new(
                raw_header_to_message_type(&entry.header),
                entry.entry.len() as u32,
            ),
        },
    }
}

fn encode_msg(msg: &ProtocolMessage, buf: &mut impl BufMut) -> Result<(), prost::EncodeError> {
    match msg {
        ProtocolMessage::Start(m) => m.encode(buf),
        ProtocolMessage::Completion(m) => m.encode(buf),
        ProtocolMessage::Suspension(m) => m.encode(buf),
        ProtocolMessage::UnparsedEntry(entry) => {
            buf.put(entry.entry.clone());
            Ok(())
        }
    }
}

// --- Input message decoder

/// Stateful decoder to decode [`ProtocolMessage`]
pub struct Decoder {
    buf: SegmentedBuf<Bytes>,
    state: DecoderState,
}

impl Default for Decoder {
    fn default() -> Self {
        Self {
            buf: SegmentedBuf::new(),
            state: DecoderState::WaitingHeader,
        }
    }
}

impl Decoder {
    /// Concatenate a new chunk in the internal buffer.
    pub fn push(&mut self, buf: Bytes) {
        self.buf.push(buf)
    }

    /// Try to consume the next message in the internal buffer.
    pub fn consume_next(
        &mut self,
    ) -> Result<Option<(MessageHeader, ProtocolMessage)>, EncodingError> {
        loop {
            let remaining = self.buf.remaining();

            if remaining < self.state.needs_bytes() {
                return Ok(None);
            }

            if let Some(res) = self.state.decode(&mut self.buf)? {
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
    ) -> Result<Option<(MessageHeader, ProtocolMessage)>, EncodingError> {
        let mut res = None;

        *self = match mem::take(self) {
            DecoderState::WaitingHeader => DecoderState::WaitingPayload(buf.get_u64().try_into()?),
            DecoderState::WaitingPayload(h) => {
                let msg = decode_protocol_message(&h, buf.take(h.frame_length() as usize))
                    .map_err(|e| EncodingError::DecodeMessage(h.message_type(), e))?;
                res = Some((h, msg));
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
        MessageType::Start => ProtocolMessage::Start(pb::protocol::StartMessage::decode(buf)?),
        MessageType::Completion => {
            ProtocolMessage::Completion(pb::protocol::CompletionMessage::decode(buf)?)
        }
        MessageType::Suspension => {
            ProtocolMessage::Suspension(pb::protocol::SuspensionMessage::decode(buf)?)
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

fn message_header_to_raw_header(message_header: &MessageHeader) -> RawEntryHeader {
    debug_assert!(
        !matches!(
            message_header.message_type(),
            MessageType::Start | MessageType::Completion | MessageType::Suspension
        ),
        "Message is not an entry type. This is a Restate bug. Please contact the developers."
    );
    match message_header.message_type() {
        MessageType::Start => unreachable!(),
        MessageType::Completion => unreachable!(),
        MessageType::Suspension => unreachable!(),
        MessageType::PollInputStreamEntry => RawEntryHeader::PollInputStream {
            is_completed: message_header
                .completed()
                .expect("completed flag being present"),
        },
        MessageType::OutputStreamEntry => RawEntryHeader::OutputStream,
        MessageType::GetStateEntry => RawEntryHeader::GetState {
            is_completed: message_header
                .completed()
                .expect("completed flag being present"),
        },
        MessageType::SetStateEntry => RawEntryHeader::SetState,
        MessageType::ClearStateEntry => RawEntryHeader::ClearState,
        MessageType::SleepEntry => RawEntryHeader::Sleep {
            is_completed: message_header
                .completed()
                .expect("completed flag being present"),
        },
        MessageType::InvokeEntry => RawEntryHeader::Invoke {
            is_completed: message_header
                .completed()
                .expect("completed flag being present"),
        },
        MessageType::BackgroundInvokeEntry => RawEntryHeader::BackgroundInvoke,
        MessageType::AwakeableEntry => RawEntryHeader::Awakeable {
            is_completed: message_header
                .completed()
                .expect("completed flag being present"),
        },
        MessageType::CompleteAwakeableEntry => RawEntryHeader::CompleteAwakeable,
        MessageType::Custom(code) => RawEntryHeader::Custom {
            code,
            requires_ack: message_header
                .requires_ack()
                .expect("requires ack flag begin present"),
        },
    }
}

fn raw_header_to_message_type(entry_header: &RawEntryHeader) -> MessageType {
    match entry_header {
        RawEntryHeader::PollInputStream { .. } => MessageType::PollInputStreamEntry,
        RawEntryHeader::OutputStream => MessageType::OutputStreamEntry,
        RawEntryHeader::GetState { .. } => MessageType::GetStateEntry,
        RawEntryHeader::SetState => MessageType::SetStateEntry,
        RawEntryHeader::ClearState => MessageType::ClearStateEntry,
        RawEntryHeader::Sleep { .. } => MessageType::SleepEntry,
        RawEntryHeader::Invoke { .. } => MessageType::InvokeEntry,
        RawEntryHeader::BackgroundInvoke => MessageType::BackgroundInvokeEntry,
        RawEntryHeader::Awakeable { .. } => MessageType::AwakeableEntry,
        RawEntryHeader::CompleteAwakeable => MessageType::CompleteAwakeableEntry,
        RawEntryHeader::Custom { code, .. } => MessageType::Custom(*code),
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use crate::pb;
    use journal::raw::RawEntryHeader;

    #[test]
    fn fill_decoder_with_several_messages() {
        let protocol_version = 1;
        let encoder = Encoder::new(protocol_version);
        let mut decoder = Decoder::default();

        let expected_msg_0 = ProtocolMessage::new_start_message(
            "key".into(),
            Bytes::copy_from_slice(uuid::Uuid::now_v7().as_bytes()),
            1,
        );
        let expected_msg_1: ProtocolMessage = RawEntry::new(
            RawEntryHeader::PollInputStream { is_completed: true },
            pb::protocol::PollInputStreamEntryMessage {
                value: Bytes::from_static("input".as_bytes()),
            }
            .encode_to_vec()
            .into(),
        )
        .into();
        let expected_msg_2: ProtocolMessage = Completion {
            entry_index: 1,
            result: CompletionResult::Empty,
        }
        .into();

        decoder.push(encoder.encode(expected_msg_0.clone()));
        decoder.push(encoder.encode(expected_msg_1.clone()));
        decoder.push(encoder.encode(expected_msg_2.clone()));

        let (actual_msg_header_0, actual_msg_0) = decoder.consume_next().unwrap().unwrap();
        assert_eq!(
            actual_msg_header_0.protocol_version(),
            Some(protocol_version)
        );
        assert_eq!(actual_msg_header_0.message_type(), MessageType::Start);
        assert_eq!(actual_msg_0, expected_msg_0);

        let (actual_msg_header_1, actual_msg_1) = decoder.consume_next().unwrap().unwrap();
        assert_eq!(
            actual_msg_header_1.message_type(),
            MessageType::PollInputStreamEntry
        );
        assert_eq!(actual_msg_header_1.completed(), Some(true));
        assert_eq!(actual_msg_1, expected_msg_1);

        let (actual_msg_header_2, actual_msg_2) = decoder.consume_next().unwrap().unwrap();
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
        let encoder = Encoder::new(0);
        let mut decoder = Decoder::default();

        let expected_msg: ProtocolMessage = RawEntry::new(
            RawEntryHeader::PollInputStream { is_completed: true },
            pb::protocol::PollInputStreamEntryMessage {
                value: Bytes::from_static("input".as_bytes()),
            }
            .encode_to_vec()
            .into(),
        )
        .into();
        let expected_msg_encoded = encoder.encode(expected_msg.clone());

        decoder.push(expected_msg_encoded.slice(0..split_index));
        assert!(decoder.consume_next().unwrap().is_none());

        decoder.push(expected_msg_encoded.slice(split_index..));

        let (actual_msg_header, actual_msg) = decoder.consume_next().unwrap().unwrap();
        assert_eq!(
            actual_msg_header.message_type(),
            MessageType::PollInputStreamEntry
        );
        assert_eq!(actual_msg_header.completed(), Some(true));
        assert_eq!(actual_msg, expected_msg);

        assert!(decoder.consume_next().unwrap().is_none());
    }
}
