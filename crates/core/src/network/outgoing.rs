// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bytes::BufMut;
use prost::Message as _;
use prost::encoding::{self, WireType};

use restate_types::net::codec::EncodedPayload;

use super::protobuf::network::{Datagram, Header, Message, RpcCall};

/// Outbound-only representation of the existing Prost envelope. The receiver
/// continues to use the generated `Message` and contiguous payloads.
#[derive(Debug)]
pub struct OutgoingMessage(Inner);

#[derive(Debug)]
enum Inner {
    Message(Message),
    RpcCall {
        header: Option<Header>,
        // Generated metadata encoding; payload is always empty here.
        call: RpcCall,
        payload: EncodedPayload,
    },
}

impl From<Message> for OutgoingMessage {
    fn from(message: Message) -> Self {
        Self(Inner::Message(message))
    }
}

impl OutgoingMessage {
    pub(crate) fn rpc_call(header: Option<Header>, call: RpcCall, payload: EncodedPayload) -> Self {
        assert!(call.payload.is_empty());
        match payload.try_into_bytes() {
            Ok(payload) => {
                let call = RpcCall { payload, ..call };
                Message {
                    header,
                    body: Some(
                        Datagram {
                            datagram: Some(call.into()),
                        }
                        .into(),
                    ),
                }
                .into()
            }
            Err(payload) => Self(Inner::RpcCall {
                header,
                call,
                payload,
            }),
        }
    }

    /// Adapter for transports that bypass serialization (loopback and mocks).
    pub fn into_message(self) -> Message {
        match self.0 {
            Inner::Message(message) => message,
            Inner::RpcCall {
                header,
                mut call,
                payload,
            } => {
                call.payload = payload.into_bytes();
                Message {
                    header,
                    body: Some(
                        Datagram {
                            datagram: Some(call.into()),
                        }
                        .into(),
                    ),
                }
            }
        }
    }

    pub(crate) fn encoded_len(&self) -> usize {
        match &self.0 {
            Inner::Message(message) => message.encoded_len(),
            Inner::RpcCall {
                header,
                call,
                payload,
            } => {
                let rpc_len = call.encoded_len() + payload_field_len(payload);
                header
                    .as_ref()
                    .map_or(0, |h| encoding::message::encoded_len(1, h))
                    + field_len(5, field_len(1, rpc_len))
            }
        }
    }

    /// Encode the normal Prost envelope directly into the transport buffer.
    /// Tags below mirror Message.body, Datagram.datagram and RpcCall.payload in
    /// protobuf/restate/network.proto; metadata is encoded by generated Prost.
    pub(crate) fn encode_into(&self, dst: &mut impl BufMut) {
        match &self.0 {
            Inner::Message(message) => message.encode_raw(dst),
            Inner::RpcCall {
                header,
                call,
                payload,
            } => {
                let rpc_len = call.encoded_len() + payload_field_len(payload);
                if let Some(header) = header {
                    encoding::message::encode(1, header, dst);
                }
                field_header(5, field_len(1, rpc_len), dst);
                field_header(1, rpc_len, dst);
                call.encode_raw(dst);
                if !payload.is_empty() {
                    field_header(100, payload.len(), dst);
                    payload.encode_into(dst);
                }
            }
        }
    }
}

fn payload_field_len(payload: &EncodedPayload) -> usize {
    if payload.is_empty() {
        0
    } else {
        field_len(100, payload.len())
    }
}

fn field_len(tag: u32, len: usize) -> usize {
    encoding::key_len(tag) + encoding::encoded_len_varint(len as u64) + len
}

fn field_header(tag: u32, len: usize, dst: &mut impl BufMut) {
    encoding::encode_key(tag, WireType::LengthDelimited, dst);
    encoding::encode_varint(len as u64, dst);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;

    use restate_types::GenerationalNodeId;
    use restate_types::logs::{Keys, LogletId, LogletOffset, Record, SequenceNumber};
    use restate_types::net::ProtocolVersion;
    use restate_types::net::codec::{WireDecode, WireEncode};
    use restate_types::net::log_server::{
        LogServerRequestHeader, PreparedPayloads, PreparedStore, Store, StoreFlags,
    };
    use restate_types::storage::PolyBytes;
    use restate_types::time::{MillisSinceEpoch, NanosSinceEpoch};

    use super::*;

    #[test]
    fn prepared_envelope_matches_generated_prost() {
        for (count, size) in [
            (0, 0),
            (1, 0),
            (1, 95),
            (1, 127),
            (1, 128),
            (1, 8191),
            (1, 8192),
            (1, 16340),
            (1, 16383),
            (1, 16384),
            (64, 4096),
        ] {
            let records: Arc<[Record]> = (0..count)
                .map(|_| {
                    Record::from_parts(
                        NanosSinceEpoch::new(42),
                        Keys::None,
                        PolyBytes::Bytes(Bytes::from(vec![7; size])),
                    )
                })
                .collect();
            let prepared = PreparedPayloads::new(records.clone());
            for version in [
                ProtocolVersion::V3,
                ProtocolVersion::V4,
                ProtocolVersion::V6,
            ] {
                for attempt in 0..6 {
                    let request = PreparedStore {
                        header: LogServerRequestHeader::new(
                            LogletId::new_unchecked(123),
                            LogletOffset::from(attempt),
                        ),
                        timeout_at: match attempt {
                            0 => None,
                            1 => Some(MillisSinceEpoch::new(0)),
                            _ => Some(MillisSinceEpoch::new(123_456)),
                        },
                        flags: if attempt == 0 {
                            StoreFlags::empty()
                        } else {
                            StoreFlags::IgnoreSeal
                        },
                        first_offset: LogletOffset::OLDEST,
                        sequencer: GenerationalNodeId::new(1, 1),
                        known_archived: LogletOffset::from(attempt),
                        payloads: prepared.clone(),
                    };
                    let payload = request.encode_to_bytes(version).unwrap();
                    let ordinary = Store::try_decode(payload.clone(), version).unwrap();
                    assert_eq!(ordinary.payloads.len(), count);
                    assert_eq!(ordinary.encode_to_bytes(version).unwrap(), payload);
                    for header in [
                        None,
                        Some(Header::default()),
                        Some(Header {
                            my_nodes_config_version: u32::MAX,
                            ..Default::default()
                        }),
                    ] {
                        let call = if attempt == 0 {
                            RpcCall::default()
                        } else {
                            RpcCall {
                                id: u64::MAX,
                                service: 1,
                                msg_type: "Store".into(),
                                sort_code: Some(0),
                                ..Default::default()
                            }
                        };
                        let outgoing = OutgoingMessage::rpc_call(
                            header,
                            call.clone(),
                            request.encode_payload(version).unwrap(),
                        );
                        let expected = Message {
                            header,
                            body: Some(
                                Datagram {
                                    datagram: Some(
                                        RpcCall {
                                            payload: payload.clone(),
                                            ..call
                                        }
                                        .into(),
                                    ),
                                }
                                .into(),
                            ),
                        };
                        let len = outgoing.encoded_len();
                        // An exact-sized slice detects any length underestimate/overwrite.
                        let mut dst = vec![0; len];
                        let mut remaining = dst.as_mut_slice();
                        outgoing.encode_into(&mut remaining);
                        assert!(remaining.is_empty());
                        assert_eq!(dst, expected.encode_to_vec());
                        assert_eq!(Message::decode(dst.as_slice()).unwrap(), expected);
                        assert_eq!(outgoing.into_message(), expected);
                    }
                }
            }
        }
        // Empty payloads and non-RPC bodies preserve normal Prost omission rules.
        for outgoing in [
            OutgoingMessage::from(Message::default()),
            OutgoingMessage::rpc_call(None, RpcCall::default(), Bytes::new().into()),
        ] {
            let mut dst = Vec::with_capacity(outgoing.encoded_len());
            outgoing.encode_into(&mut dst);
            assert_eq!(dst, outgoing.into_message().encode_to_vec());
        }
    }
}
