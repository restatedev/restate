// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Reuse the encoded record batch across Store requests while retaining the
//! existing contiguous transport and receiver-side Store representation.

use std::sync::Arc;

use bytes::Bytes;

use crate::GenerationalNodeId;
use crate::logs::{LogletOffset, Record};
use crate::net::codec::{EncodeError, EncodedPayload, WireDecode, WireEncode, encode_as_bilrost};
use crate::net::{ProtocolVersion, RpcRequest};
use crate::time::MillisSinceEpoch;

use super::{LogServerRequestHeader, Payloads, Store, StoreFlags};

/// Immutable records and their prepared current-protocol encoding.
/// Clones share both allocations, including across replicas and retry waves.
/// Original records are retained for V2 peers, which use the compatibility encoder.
#[derive(Debug, Clone)]
pub struct PreparedPayloads {
    payloads: Payloads,
    encoded: Bytes,
}

impl PreparedPayloads {
    /// Encode a batch once before handing it to appenders or replica tasks.
    pub fn new(records: Arc<[Record]>) -> Self {
        let payloads = records.into();
        let encoded = encode_as_bilrost(&payloads);
        Self { payloads, encoded }
    }

    /// Borrow the original records retained for bookkeeping and V2 peers.
    pub fn records(&self) -> &[Record] {
        &self.payloads
    }
}

/// A Store request with independently refreshed metadata and shared record
/// encoding. Uses the same RPC identity and wire format as [`Store`].
#[derive(Debug, Clone)]
pub struct PreparedStore {
    pub header: LogServerRequestHeader,
    pub timeout_at: Option<MillisSinceEpoch>,
    pub flags: StoreFlags,
    pub first_offset: LogletOffset,
    pub sequencer: GenerationalNodeId,
    pub known_archived: LogletOffset,
    pub payloads: PreparedPayloads,
}

// Keep tags aligned with Store. A nested Payloads message and opaque Bytes
// have the same length-delimited representation, including empty payloads.
#[derive(Debug, bilrost::Message)]
struct EncodedStore {
    #[bilrost(1)]
    header: LogServerRequestHeader,
    #[bilrost(2)]
    timeout_at: Option<MillisSinceEpoch>,
    #[bilrost(3)]
    flags: StoreFlags,
    #[bilrost(4)]
    first_offset: LogletOffset,
    #[bilrost(5)]
    sequencer: GenerationalNodeId,
    #[bilrost(6)]
    known_archived: LogletOffset,
    #[bilrost(7)]
    payloads: Bytes,
}

impl PreparedStore {
    fn encoded(&self) -> EncodedStore {
        EncodedStore {
            header: self.header.clone(),
            timeout_at: self.timeout_at,
            flags: self.flags.clone(),
            first_offset: self.first_offset,
            sequencer: self.sequencer,
            known_archived: self.known_archived,
            payloads: self.payloads.encoded.clone(),
        }
    }
}

impl WireEncode for PreparedStore {
    fn encode_to_bytes(&self, protocol_version: ProtocolVersion) -> Result<Bytes, EncodeError> {
        match protocol_version {
            ProtocolVersion::Unknown | ProtocolVersion::V2 => Store {
                header: self.header.clone(),
                timeout_at: self.timeout_at,
                flags: self.flags.clone(),
                first_offset: self.first_offset,
                sequencer: self.sequencer,
                known_archived: self.known_archived,
                payloads: self.payloads.payloads.clone(),
            }
            .encode_to_bytes(protocol_version),
            ProtocolVersion::V3 | ProtocolVersion::V4 | ProtocolVersion::V6 => {
                Ok(bilrost::Message::encode_to_bytes(&self.encoded()))
            }
        }
    }

    fn encode_payload(
        &self,
        protocol_version: ProtocolVersion,
    ) -> Result<EncodedPayload, EncodeError> {
        match protocol_version {
            ProtocolVersion::V3 | ProtocolVersion::V4 | ProtocolVersion::V6
                if self.payloads.encoded.len() >= 8 * 1024 =>
            {
                Ok(EncodedPayload::from_bilrost(self.encoded()))
            }
            // For small payloads, exact-sized encoding is cheaper than boxing
            // and invoking the deferred encoder. Retain V2's compatibility path.
            _ => self
                .encode_to_bytes(protocol_version)
                .map(EncodedPayload::from),
        }
    }
}

impl WireDecode for PreparedStore {
    type Error = anyhow::Error;

    fn try_decode(
        buf: impl bytes::Buf,
        protocol_version: ProtocolVersion,
    ) -> Result<Self, Self::Error> {
        let store = Store::try_decode(buf, protocol_version)?;
        Ok(Self {
            header: store.header,
            timeout_at: store.timeout_at,
            flags: store.flags,
            first_offset: store.first_offset,
            sequencer: store.sequencer,
            known_archived: store.known_archived,
            payloads: PreparedPayloads::new(store.payloads.into()),
        })
    }
}

impl RpcRequest for PreparedStore {
    const TYPE: &str = <Store as RpcRequest>::TYPE;
    type Service = <Store as RpcRequest>::Service;
    type Response = <Store as RpcRequest>::Response;
}

#[cfg(test)]
mod tests {
    use restate_clock::time::NanosSinceEpoch;

    use crate::logs::{Keys, LogletId, SequenceNumber};
    use crate::storage::PolyBytes;

    use super::*;

    #[test]
    fn prepared_store_preserves_wire_format_and_reuses_records() {
        for record_count in [0, 1, 64] {
            let records: Arc<[Record]> = (0..record_count)
                .map(|_| {
                    Record::from_parts(
                        NanosSinceEpoch::now(),
                        Keys::None,
                        PolyBytes::Bytes(Bytes::from(vec![42; 4096])),
                    )
                })
                .collect();
            let payloads = PreparedPayloads::new(records.clone());
            assert_eq!(payloads.records().as_ptr(), records.as_ptr());
            assert_eq!(payloads.encoded, encode_as_bilrost(&payloads.payloads));
            let mut request = PreparedStore {
                header: LogServerRequestHeader::new(
                    LogletId::new_unchecked(123),
                    LogletOffset::INVALID,
                ),
                timeout_at: None,
                flags: StoreFlags::empty(),
                first_offset: LogletOffset::OLDEST,
                sequencer: GenerationalNodeId::new(1, 1),
                known_archived: LogletOffset::INVALID,
                payloads: payloads.clone(),
            };
            assert!(request.encode_to_bytes(ProtocolVersion::Unknown).is_err());
            assert!(request.encode_payload(ProtocolVersion::Unknown).is_err());
            for version in [
                ProtocolVersion::V2,
                ProtocolVersion::V3,
                ProtocolVersion::V4,
                ProtocolVersion::V6,
            ] {
                // Distinct replicas and subsequent attempts share one batch but
                // must retain their own metadata, including omitted/default fields.
                for attempt in 0..6 {
                    request.payloads = payloads.clone();
                    request.timeout_at = match attempt {
                        0 => None,
                        1 => Some(MillisSinceEpoch::new(0)),
                        _ => Some(MillisSinceEpoch::new(123_456)),
                    };
                    request.header.known_global_tail = LogletOffset::from(attempt);
                    request.known_archived = LogletOffset::from(attempt);
                    request.flags = if attempt == 0 {
                        StoreFlags::empty()
                    } else {
                        StoreFlags::IgnoreSeal
                    };
                    let ordinary = Store {
                        header: request.header.clone(),
                        timeout_at: request.timeout_at,
                        flags: request.flags.clone(),
                        first_offset: request.first_offset,
                        sequencer: request.sequencer,
                        known_archived: request.known_archived,
                        payloads: records.clone().into(),
                    };
                    let encoded = request.encode_to_bytes(version).unwrap();
                    let deferred = request.encode_payload(version).unwrap();
                    assert_eq!(deferred.len(), encoded.len());
                    assert_eq!(deferred.into_bytes(), encoded);
                    let boxed: Box<dyn WireEncode + Send> = Box::new(request.clone());
                    let deferred = boxed.encode_payload(version).unwrap();
                    assert_eq!(
                        deferred.try_into_bytes().is_ok(),
                        version == ProtocolVersion::V2 || payloads.encoded.len() < 8 * 1024
                    );
                    assert_eq!(encoded, ordinary.encode_to_bytes(version).unwrap());
                    let decoded = Store::try_decode(encoded.clone(), version).unwrap();
                    assert_eq!(decoded.payloads.len(), record_count);
                    assert_eq!(decoded.encode_to_bytes(version).unwrap(), encoded);
                    let decoded = PreparedStore::try_decode(encoded.clone(), version).unwrap();
                    assert_eq!(decoded.encode_to_bytes(version).unwrap(), encoded);
                    assert_eq!(request.payloads.encoded.as_ptr(), payloads.encoded.as_ptr());
                    assert_eq!(request.payloads.records().as_ptr(), records.as_ptr());
                }
            }
            assert_eq!(PreparedStore::TYPE, Store::TYPE);
        }
    }
}
