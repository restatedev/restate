// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::{Buf, Bytes};
use bytes_utils::SegmentedBuf;

use super::{HeaderError, Message, MessageHeader, MessageType};

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error(transparent)]
    Header(#[from] HeaderError),
    #[error("message of {size} bytes is above the limit of {limit} bytes")]
    TooLarge { size: usize, limit: usize },
    #[error("cannot decode message {ty:?}: {source}")]
    Protobuf {
        ty: MessageType,
        #[source]
        source: prost::DecodeError,
    },
}

/// Stateful decoder: push chunks as they arrive, take messages once complete.
pub struct Decoder {
    buf: SegmentedBuf<Bytes>,
    header: Option<MessageHeader>,
    message_size_limit: usize,
}

impl Decoder {
    pub fn new(message_size_limit: usize) -> Self {
        Self {
            buf: SegmentedBuf::new(),
            header: None,
            message_size_limit,
        }
    }

    pub fn push(&mut self, buf: Bytes) {
        self.buf.push(buf)
    }

    /// True when the decoder holds the bytes of a message that is not complete.
    pub fn has_partial(&self) -> bool {
        self.header.is_some() || self.buf.has_remaining()
    }

    pub fn consume_next(&mut self) -> Result<Option<Message>, DecodeError> {
        let header = match self.header {
            Some(header) => header,
            None => {
                if self.buf.remaining() < 8 {
                    return Ok(None);
                }
                let header = MessageHeader::try_from(self.buf.get_u64())?;
                let size = header.frame_length() as usize;
                if size > self.message_size_limit {
                    return Err(DecodeError::TooLarge {
                        size,
                        limit: self.message_size_limit,
                    });
                }
                self.header = Some(header);
                header
            }
        };

        let len = header.frame_length() as usize;
        if self.buf.remaining() < len {
            return Ok(None);
        }
        self.header = None;
        let ty = header.message_type();
        Message::decode(ty, (&mut self.buf).take(len))
            .map(Some)
            .map_err(|source| DecodeError::Protobuf { ty, source })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::codec::encode;
    use crate::proto;

    fn roundtrip(messages: Vec<Message>) {
        let mut decoder = Decoder::new(usize::MAX);
        for msg in &messages {
            decoder.push(encode(msg));
        }
        for expected in messages {
            assert_eq!(decoder.consume_next().unwrap(), Some(expected));
        }
        assert_eq!(decoder.consume_next().unwrap(), None);
        assert!(!decoder.has_partial());
    }

    #[test]
    fn roundtrip_every_group() {
        let value = |s: &'static str| proto::Value {
            content: Bytes::from_static(s.as_bytes()),
        };
        roundtrip(vec![
            // control
            proto::StartMessage {
                id: Bytes::from_static(b"id"),
                debug_id: "inv_1".into(),
                attempt: 3,
                next_result_id: 17,
                cancelled: true,
                state_map: vec![proto::start_message::StateEntry {
                    scope: proto::StateScope::Invocation.into(),
                    key: Bytes::from_static(b"k"),
                    value: Some(value("v")),
                }],
                is_object_scope_partial_state: true,
                ..Default::default()
            }
            .into(),
            proto::ShutdownMessage {
                processed_notifications: 4,
            }
            .into(),
            proto::HeartbeatMessage {}.into(),
            proto::EndMessage {
                output: Some(proto::end_message::Output::Value(value("out"))),
            }
            .into(),
            // actions
            proto::AtomicGroupActionMessage {
                actions: vec![proto::atomic_group_action_message::Action {
                    action: Some(
                        proto::atomic_group_action_message::action::Action::SetState(
                            proto::SetStateActionMessage {
                                scope: proto::StateScope::Object.into(),
                                key: Bytes::from_static(b"k"),
                                value: Some(value("v")),
                                result_id: 5,
                                ..Default::default()
                            },
                        ),
                    ),
                }],
                result_id: 6,
                ..Default::default()
            }
            .into(),
            proto::CallActionMessage {
                service_name: "svc".into(),
                handler_name: "h".into(),
                invocation_id_result_id: 7,
                result_id: 8,
                ..Default::default()
            }
            .into(),
            // notifications
            proto::ResultNotificationMessage {
                result_id: 7,
                result: Some(proto::result_notification_message::Result::InvocationId(
                    "inv_2".into(),
                )),
            }
            .into(),
            proto::SignalNotificationMessage {
                signal_id: proto::BuiltInSignal::Cancel.into(),
            }
            .into(),
        ]);
    }

    #[test]
    fn header_errors() {
        // unknown type
        let mut decoder = Decoder::new(usize::MAX);
        decoder.push(Bytes::copy_from_slice(&(0x0405u64 << 48).to_be_bytes()));
        assert!(matches!(
            decoder.consume_next(),
            Err(DecodeError::Header(HeaderError::UnknownType(0x0405)))
        ));

        // reserved bit
        let mut decoder = Decoder::new(usize::MAX);
        decoder.push(Bytes::copy_from_slice(
            &((0x0004u64 << 48) | (1 << 32)).to_be_bytes(),
        ));
        assert!(matches!(
            decoder.consume_next(),
            Err(DecodeError::Header(HeaderError::ReservedBits(1)))
        ));

        // too large
        let mut decoder = Decoder::new(10);
        decoder.push(Bytes::copy_from_slice(
            &((0x0004u64 << 48) | 11).to_be_bytes(),
        ));
        assert!(matches!(
            decoder.consume_next(),
            Err(DecodeError::TooLarge {
                size: 11,
                limit: 10
            })
        ));
    }

    #[test]
    fn message_split_across_chunks() {
        let msg: Message = proto::SetStateActionMessage {
            scope: proto::StateScope::Object.into(),
            key: Bytes::from_static(b"key"),
            value: Some(proto::Value {
                content: Bytes::from_static(b"some value"),
            }),
            result_id: 1,
            ..Default::default()
        }
        .into();
        let bytes = encode(&msg);

        let mut decoder = Decoder::new(usize::MAX);
        assert!(!decoder.has_partial());
        for i in 0..bytes.len() - 1 {
            decoder.push(bytes.slice(i..i + 1));
            assert_eq!(decoder.consume_next().unwrap(), None);
            assert!(decoder.has_partial(), "partial after byte {i}");
        }
        decoder.push(bytes.slice(bytes.len() - 1..));
        assert_eq!(decoder.consume_next().unwrap(), Some(msg));
        assert!(!decoder.has_partial());
    }
}
