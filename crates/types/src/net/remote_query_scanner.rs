// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{Display, Formatter};
use std::ops::RangeInclusive;

use restate_encoding::RestateEncoding;

use super::ServiceTag;
use crate::GenerationalNodeId;
use crate::identifiers::{PartitionId, PartitionKey};
use crate::net::{bilrost_wire_codec, define_rpc, define_service};

pub struct RemoteDataFusionService;
define_service! {
    @service = RemoteDataFusionService,
    @tag = ServiceTag::RemoteDataFusionService,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize, bilrost::Message,
)]
pub struct ScannerId(#[bilrost(1)] pub GenerationalNodeId, #[bilrost(2)] pub u64);

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
    #[bilrost(tag(2), encoding(RestateEncoding))]
    pub range: RangeInclusive<PartitionKey>,
    #[bilrost(3)]
    pub table: String,
    #[bilrost(tag(4), encoding(plainbytes))]
    pub projection_schema_bytes: Vec<u8>,
    #[bilrost(tag(5))]
    #[serde(default)]
    pub limit: Option<u64>,
    #[bilrost(tag(6))]
    #[serde(default = "default_batch_size")]
    pub batch_size: u64,
    #[bilrost(tag(7))]
    #[serde(default)]
    pub predicate: Option<RemoteQueryScannerPredicate>,
}

fn default_batch_size() -> u64 {
    64
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, bilrost::Message)]
pub struct RemoteQueryScannerPredicate {
    // We ship the expression passed to scan() over the wire to filter records before sending
    // them back
    // see `encode_expr` / `decode_expr` in storage-query-datafusion/src/lib.rs
    #[bilrost(tag(1), encoding(plainbytes))]
    pub serialized_physical_expression: Vec<u8>,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    bilrost::Message,
    bilrost::Oneof,
)]
pub enum RemoteQueryScannerOpened {
    Failure,
    #[bilrost(1)]
    Success {
        scanner_id: ScannerId,
    },
}

// ----- next batch -----

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, bilrost::Message)]
pub struct RemoteQueryScannerNext {
    #[bilrost(1)]
    pub scanner_id: ScannerId,
    #[bilrost(tag(2))]
    #[serde(default)]
    pub next_predicate: Option<RemoteQueryScannerPredicate>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, bilrost::Message)]
pub struct ScannerBatch {
    #[bilrost(1)]
    pub scanner_id: ScannerId,
    #[bilrost(tag(2), encoding(plainbytes))]
    pub record_batch: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, bilrost::Message)]
pub struct ScannerFailure {
    #[bilrost(1)]
    pub scanner_id: ScannerId,
    #[bilrost(2)]
    pub message: String,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    bilrost::Message,
    bilrost::Oneof,
)]
pub enum RemoteQueryScannerNextResult {
    Unknown,
    #[bilrost(1)]
    NextBatch(ScannerBatch),
    #[bilrost(2)]
    Failure(ScannerFailure),
    #[bilrost(3)]
    NoMoreRecords(ScannerId),
    #[bilrost(4)]
    NoSuchScanner(ScannerId),
}

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

bilrost_wire_codec!(RemoteQueryScannerOpen);
bilrost_wire_codec!(RemoteQueryScannerOpened);

define_rpc! {
    @request = RemoteQueryScannerNext,
    @response = RemoteQueryScannerNextResult,
    @service = RemoteDataFusionService,
}
bilrost_wire_codec!(RemoteQueryScannerNext);
bilrost_wire_codec!(RemoteQueryScannerNextResult);

define_rpc! {
    @request = RemoteQueryScannerClose,
    @response = RemoteQueryScannerClosed,
    @service = RemoteDataFusionService,
}
bilrost_wire_codec!(RemoteQueryScannerClose);
bilrost_wire_codec!(RemoteQueryScannerClosed);

#[cfg(test)]
mod test {

    use serde::Deserialize;

    use crate::{
        GenerationalNodeId,
        net::remote_query_scanner::{ScannerBatch, ScannerFailure, ScannerId},
    };

    // V1/flexbuffers scanner next result type.
    #[derive(Deserialize)]
    #[allow(dead_code)]
    pub enum RemoteQueryScannerNextResult {
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

    #[test]
    fn backward_compatibility() {
        let scanner_id = ScannerId(GenerationalNodeId::new(10, 20), 100);
        let batch = vec![1, 2, 3, 4];
        let v2 = super::RemoteQueryScannerNextResult::NextBatch(ScannerBatch {
            scanner_id,
            record_batch: batch.clone(),
        });

        let bytes = flexbuffers::to_vec(&v2).expect("to serialize");

        let v1: RemoteQueryScannerNextResult =
            flexbuffers::from_slice(&bytes).expect("to deserialize");

        assert!(
            matches!(v1, RemoteQueryScannerNextResult::NextBatch { scanner_id: id, record_batch} if id == scanner_id && batch == record_batch )
        );

        let v2 = super::RemoteQueryScannerNextResult::Failure(ScannerFailure {
            scanner_id,
            message: "scanner failed successfully!".to_string(),
        });

        let bytes = flexbuffers::to_vec(&v2).expect("to serialize");

        let v1: RemoteQueryScannerNextResult =
            flexbuffers::from_slice(&bytes).expect("to deserialize");

        assert!(
            matches!(v1, RemoteQueryScannerNextResult::Failure { scanner_id: id, message} if id == scanner_id && message == "scanner failed successfully!" )
        )
    }
}
