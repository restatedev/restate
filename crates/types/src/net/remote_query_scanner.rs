// Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::ServiceTag;
use crate::identifiers::{PartitionId, PartitionKey};
use crate::net::{bilrost_wire_codec_with_v1_fallback, define_rpc, define_service};
use crate::{GenerationalNodeId, NetRangeInclusive};
use restate_encoding::BilrostAs;
use std::fmt::{Display, Formatter};

pub struct RemoteDataFusionService;
define_service! {
    @service = RemoteDataFusionService,
    @tag = ServiceTag::RemoteDataFusionService,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize, bilrost::Message,
)]
pub struct ScannerId(pub GenerationalNodeId, pub u64);

impl Display for ScannerId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("ScannerId({}, {})", self.0, self.1))
    }
}

// ----- open scanner -----

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, bilrost::Message)]
pub struct RemoteQueryScannerOpen {
    #[bilrost(1)]
    pub partition_id: PartitionId,
    #[bilrost(2)]
    pub range: NetRangeInclusive<PartitionKey>,
    #[bilrost(3)]
    pub table: String,
    #[bilrost(tag = 4, encoding = "plainbytes")]
    pub projection_schema_bytes: Vec<u8>,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    bilrost::Oneof,
    bilrost::Message,
)]
pub enum RemoteQueryScannerOpened {
    #[bilrost(1)]
    Success {
        scanner_id: ScannerId,
    },

    Failure,
}

// ----- next batch -----

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, bilrost::Message)]
pub struct RemoteQueryScannerNext {
    #[bilrost(1)]
    pub scanner_id: ScannerId,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default, BilrostAs)]
#[bilrost_as(dto::RemoteQueryScannerNextResult)]
pub enum RemoteQueryScannerNextResultShadow {
    #[default]
    Unknown,

    NextBatch {
        scanner_id: ScannerId,
        record_batch: Vec<u8>,
    },
    Failure {
        scanner_id: ScannerId,
        message: String,
    },
    NoMoreRecords(ScannerId),
    NoSuchScanner(ScannerId),
}

#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default, bilrost::Message,
)]
pub struct RemoteQueryScannerNextResult(pub RemoteQueryScannerNextResultShadow);

// ----- close scanner -----

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, bilrost::Message)]
pub struct RemoteQueryScannerClose {
    #[bilrost(1)]
    pub scanner_id: ScannerId,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, bilrost::Message)]
pub struct RemoteQueryScannerClosed {
    #[bilrost(1)]
    pub scanner_id: ScannerId,
}

// ----- RemoteScanner API API -----

// Scan
define_rpc! {
    @request = RemoteQueryScannerOpen,
    @response = RemoteQueryScannerOpened,
    @service = RemoteDataFusionService,
}
bilrost_wire_codec_with_v1_fallback!(RemoteQueryScannerOpen);
bilrost_wire_codec_with_v1_fallback!(RemoteQueryScannerOpened);

define_rpc! {
    @request = RemoteQueryScannerNext,
    @response = RemoteQueryScannerNextResult,
    @service = RemoteDataFusionService,
}
bilrost_wire_codec_with_v1_fallback!(RemoteQueryScannerNext);
bilrost_wire_codec_with_v1_fallback!(RemoteQueryScannerNextResult);

define_rpc! {
    @request = RemoteQueryScannerClose,
    @response = RemoteQueryScannerClosed,
    @service = RemoteDataFusionService,
}
bilrost_wire_codec_with_v1_fallback!(RemoteQueryScannerClose);
bilrost_wire_codec_with_v1_fallback!(RemoteQueryScannerClosed);

mod dto {
    use super::ScannerId;

    #[derive(bilrost::Message)]
    pub struct RemoteQueryScannerNextResultNextBatch {
        #[bilrost(1)]
        pub scanner_id: ScannerId,
        #[bilrost(tag = 2, encoding = "plainbytes")]
        pub record_batch: Vec<u8>,
    }

    #[derive(bilrost::Message, bilrost::Oneof)]
    pub enum RemoteQueryScannerNextResult {
        Unknown,
        #[bilrost(2)]
        NextBatch(RemoteQueryScannerNextResultNextBatch),
        #[bilrost(3)]
        Failure((ScannerId, String)),
        #[bilrost(4)]
        NoMoreRecords(ScannerId),
        #[bilrost(5)]
        NoSuchScanner(ScannerId),
    }

    impl From<&super::RemoteQueryScannerNextResultShadow> for RemoteQueryScannerNextResult {
        fn from(value: &super::RemoteQueryScannerNextResultShadow) -> Self {
            match value {
                super::RemoteQueryScannerNextResultShadow::Unknown => Self::Unknown,
                super::RemoteQueryScannerNextResultShadow::NextBatch {
                    scanner_id,
                    record_batch,
                } => Self::NextBatch(RemoteQueryScannerNextResultNextBatch {
                    scanner_id: *scanner_id,
                    record_batch: record_batch.clone(),
                }),
                super::RemoteQueryScannerNextResultShadow::Failure {
                    scanner_id,
                    message,
                } => Self::Failure((*scanner_id, message.clone())),
                super::RemoteQueryScannerNextResultShadow::NoMoreRecords(scanner_id) => {
                    Self::NoMoreRecords(*scanner_id)
                }
                super::RemoteQueryScannerNextResultShadow::NoSuchScanner(scanner_id) => {
                    Self::NoSuchScanner(*scanner_id)
                }
            }
        }
    }

    impl From<RemoteQueryScannerNextResult> for super::RemoteQueryScannerNextResultShadow {
        fn from(value: RemoteQueryScannerNextResult) -> Self {
            match value {
                RemoteQueryScannerNextResult::Unknown => Self::Unknown,
                RemoteQueryScannerNextResult::NextBatch(next_batch) => Self::NextBatch {
                    scanner_id: next_batch.scanner_id,
                    record_batch: next_batch.record_batch,
                },
                RemoteQueryScannerNextResult::Failure((scanner_id, message)) => Self::Failure {
                    scanner_id,
                    message,
                },
                RemoteQueryScannerNextResult::NoMoreRecords(scanner_id) => {
                    Self::NoMoreRecords(scanner_id)
                }
                RemoteQueryScannerNextResult::NoSuchScanner(scanner_id) => {
                    Self::NoSuchScanner(scanner_id)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remote_query_scanner_next_result_compatibility() {
        let v = RemoteQueryScannerNextResultShadow::Failure {
            scanner_id: ScannerId(GenerationalNodeId::new(1, 2), 3),
            message: "test".to_string(),
        };
        let buf = crate::net::codec::encode_as_flexbuffers(&v);

        let v2: RemoteQueryScannerNextResult = crate::net::codec::decode_as_flexbuffers(
            ::bytes::Bytes::from(buf),
            crate::net::ProtocolVersion::V1,
        )
        .expect("decode successful");
        assert_eq!(v, v2.0);

        let v = RemoteQueryScannerNextResult(RemoteQueryScannerNextResultShadow::NoMoreRecords(
            ScannerId(GenerationalNodeId::new(1, 2), 3),
        ));
        let buf = crate::net::codec::encode_as_flexbuffers(&v);

        let v2: RemoteQueryScannerNextResultShadow = crate::net::codec::decode_as_flexbuffers(
            ::bytes::Bytes::from(buf),
            crate::net::ProtocolVersion::V1,
        )
        .expect("decode successfule");
        assert_eq!(v.0, v2);
    }
}
